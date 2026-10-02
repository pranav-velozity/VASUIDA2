/* ── VelOzity Pinpoint — Terminal49 integration v1 ──
   The carrier tells us when the ship left and when it arrived, so nobody has to type it.

   What it can and cannot do, stated plainly because it decides the whole design:

     · vessel_departed  → the ACTUAL departure. The carrier observed it; that is a fact.
     · vessel_arrived   → the ACTUAL arrival. Same.
     · estimated.arrival → the PLANNED arrival only. A revised ETA is a carrier's opinion
       about the future, not an observation, and writing it as an actual would fabricate a
       milestone that has not happened.

   It never touches the quoted transit time. If a carrier's revision could rewrite the quote,
   a carrier could erase its own slip by re-quoting, and the one number that holds them to
   account would be the one they control.

   Origin clearance, destination clearance and FC receipt stay with your team. Terminal49 has
   no view of them, and a stage nobody can observe must not be auto-confirmed.

   Mount:
     app.use('/t49', require('./t49_routes')({ express, db, authenticateRequest, requireRole,
       auditLog, curClient, refreshPlanned }));

   Environment:
     T49_API_KEY         the Terminal49 key
     T49_WEBHOOK_SECRET  a secret of your choosing, included in the webhook path
*/
'use strict';

const crypto = require('crypto');

module.exports = function mountT49(deps) {
  const { express, db, authenticateRequest, requireRole, auditLog, curClient, refreshPlanned } = deps;
  // Optional, from the consignments module: the estimate history and the ports. Absent (an
  // older server.js), tracking behaves exactly as before.
  const { logEstimate, recordPorts, addViaPort } = deps;
  const router = express.Router();

  const API = 'https://api.terminal49.com/v2';

  db.exec(`
    -- Which consignment a Terminal49 shipment belongs to. Keyed on their id AND on the
    -- container number, because events arrive carrying one or the other.
    CREATE TABLE IF NOT EXISTS t49_link (
      client_id        TEXT NOT NULL,
      consignment_uid  TEXT NOT NULL,
      t49_shipment_id  TEXT,
      container_number TEXT,
      request_number   TEXT,
      request_type     TEXT,
      scac             TEXT,
      state            TEXT NOT NULL DEFAULT 'requested',  -- requested | tracking | failed | stopped
      failed_reason    TEXT,
      -- Terminal facts that are not milestones. Demurrage starts at the last free day, so a
      -- container sitting past it costs money every day nobody notices.
      pickup_lfd       TEXT,
      holds            TEXT,            -- JSON: the holds the terminal is reporting
      available_at     TEXT,
      pod_terminal     TEXT,
      last_context     TEXT,            -- the most recent non-milestone event, in words
      created_at       TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at       TEXT,
      PRIMARY KEY (client_id, consignment_uid)
    );
    CREATE INDEX IF NOT EXISTS idx_t49_ship ON t49_link(t49_shipment_id);
    CREATE INDEX IF NOT EXISTS idx_t49_cont ON t49_link(container_number);

    -- Every notification, kept raw. When a date looks wrong, the question is always "what did
    -- they actually send", and a parsed summary cannot answer it.
    CREATE TABLE IF NOT EXISTS t49_event (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      received_at TEXT NOT NULL DEFAULT (datetime('now')),
      event       TEXT,
      shipment_id TEXT,
      container   TEXT,
      applied_to  TEXT,                -- consignment_uid, or null when nothing matched
      outcome     TEXT,                -- applied | ignored | unmatched | error
      note        TEXT,
      payload     TEXT
    );
  `);

  // The same trap: t49_link gained columns after it first existed.
  function addColumn(table, column, decl) {
    try {
      const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
      if (!cols.includes(column)) {
        db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${decl}`);
        console.log(`[t49] added ${table}.${column}`);
      }
    } catch (e) { console.warn(`[t49] could not add ${table}.${column}:`, e.message); }
  }
  for (const [col, decl] of [['pickup_lfd', 'TEXT'], ['holds', 'TEXT'], ['available_at', 'TEXT'],
                             ['pod_terminal', 'TEXT'], ['last_context', 'TEXT']]) {
    addColumn('t49_link', col, decl);
  }
  addColumn('consignment', 'scac', 'TEXT');

  // ── Their vocabulary, mapped onto ours ──
  // Observations only: each of these is something the carrier or terminal saw happen.
  const ACTUAL_EVENTS = {
    'container.transport.vessel_departed': 'departed',
    'container.transport.vessel_arrived': 'arrived',
    // Available for pickup means the terminal has released it, which in practice means
    // customs and the line have both let go. It is the closest observable thing to our
    // "destination cleared", and it is recorded as the carrier's reading rather than a
    // person's — if your clearance means something narrower, amend it and the feed defers.
    'container.transport.available': 'dest_cleared',
  };

  // Estimates move the plan for the stage they concern. A later estimate for a stage that has
  // already happened is ignored rather than applied backwards.
  const ESTIMATE_EVENTS = {
    'container.transport.estimated.vessel_departed': 'departed',
    'container.transport.estimated.vessel_arrived': 'arrived',
    'shipment.estimated.arrival': 'arrived',
  };

  // Not milestones of ours, but facts worth keeping: they explain a long transit, or they
  // cost money if nobody acts.
  const CONTEXT_EVENTS = {
    'container.transport.full_out': 'gated out of the port',
    'container.transport.empty_in': 'empty returned',
    'container.transport.vessel_discharged': 'discharged at the port of discharge',
    'container.transport.transshipment_arrived': 'arrived at a transshipment port',
    'container.transport.transshipment_departed': 'departed the transshipment port',
    'container.transport.feeder_departed': 'feeder departed',
    'container.transport.feeder_arrived': 'feeder arrived',
    'container.transport.not_available': 'no longer available for pickup',
  };

  const log = (row) => {
    try {
      db.prepare(`INSERT INTO t49_event (event, shipment_id, container, applied_to, outcome, note, payload)
                  VALUES (?,?,?,?,?,?,?)`)
        .run(row.event || null, row.shipment_id || null, row.container || null,
             row.applied_to || null, row.outcome, row.note || null,
             row.payload ? JSON.stringify(row.payload).slice(0, 20000) : null);
    } catch (e) { console.warn('[t49] could not log event:', e.message); }
  };

  const dayOf = (v) => v ? String(v).slice(0, 10) : null;

  // ── Ports, read from the shipment ──
  // The shipment carries the port of loading and the port of discharge. Field names are read
  // defensively — several spellings are accepted — and nothing is written when none is found,
  // so a payload of a different shape degrades to "port not yet known", never to a wrong port.
  const firstOf = (...v) => { for (const x of v) if (x != null && String(x).trim() !== '') return String(x).trim(); return null; };
  function portsFromShipment(sa) {
    const a = sa || {};
    return {
      origin: firstOf(a.port_of_lading_locode, a.pol_locode, a.port_of_lading_code),
      origin_name: firstOf(a.port_of_lading_name, a.pol_name),
      dest: firstOf(a.port_of_discharge_locode, a.pod_locode, a.port_of_discharge_code),
      dest_name: firstOf(a.port_of_discharge_name, a.pod_name),
    };
  }
  function savePorts(uid, sa) {
    if (typeof recordPorts !== 'function' || !uid || !sa) return false;
    try { return recordPorts(uid, portsFromShipment(sa)); } catch (_) { return false; }
  }
  // A transshipment event names where it happened either on the event itself or through a
  // related location in `included`.
  function viaFromEvent(transport, included) {
    const a = (transport && transport.attributes) || {};
    let code = firstOf(a.location_locode, a.port_locode, a.locode);
    let name = firstOf(a.location_name, a.port_name);
    const rel = transport && transport.relationships && transport.relationships.location && transport.relationships.location.data;
    if (rel && Array.isArray(included)) {
      const loc = included.find(x => x && x.id === rel.id && x.type === rel.type);
      const la = (loc && loc.attributes) || {};
      code = code || firstOf(la.locode, la.code, la.un_locode);
      name = name || firstOf(la.name, la.city);
    }
    return (code || name) ? { code, name } : null;
  }

  // ── Subscribing ──
  router.post('/subscribe', authenticateRequest, requireRole(['admin', 'supplier']),
    auditLog('t49_subscribe'), async (req, res) => {
    try {
      // The consignment is checked before the credentials: an air movement is never going to
      // be tracked here whatever the key says, and "key not set" would be the wrong reason.
      const uid = String((req.body && req.body.consignment_uid) || '').trim();
      const client = curClient();
      const c = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ? AND client_id = ?')
        .get(uid, client);
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });
      if (c.mode === 'Air') {
        // Terminal49 is ocean only. Saying so is better than a failed request they have to
        // interpret.
        return res.status(400).json({ ok: false, error: 'air_not_supported',
          message: 'Terminal49 tracks ocean freight. Air milestones stay manual.' });
      }

      const key = process.env.T49_API_KEY;
      if (!key) return res.status(503).json({ ok: false, error: 'T49_API_KEY is not set.' });

      // Master bill first: it covers the whole movement. A container number works too, but it
      // only starts tracking once the carrier has manifested it.
      const number = String(c.mbl || req.body.request_number || c.reference || '').trim();
      const type = c.mbl ? 'bill_of_lading' : 'container';
      if (!number) {
        return res.status(400).json({ ok: false, error: 'nothing_to_track',
          message: 'Enter the MBL or the container number before subscribing.' });
      }

      // Terminal49 requires either a carrier SCAC or an explicit instruction to work it out.
      // An omitted `scac` is not the same as asking them to infer it: JSON.stringify drops an
      // undefined key, so the first version of this sent neither and was refused for a blank
      // SCAC it never knowingly sent.
      const scac = String(req.body.scac || c.scac || '').trim().toUpperCase();

      const ask = async (attrs) => {
        const r = await fetch(API + '/tracking_requests', {
          method: 'POST',
          headers: { 'Authorization': 'Token ' + key,
                     'Content-Type': 'application/vnd.api+json',
                     'Accept': 'application/vnd.api+json' },
          body: JSON.stringify({ data: { type: 'tracking_request', attributes: Object.assign({
            request_type: type, request_number: number,
            ref_numbers: [c.reference, c.shipment_ref].filter(Boolean),
          }, attrs) } }),
        });
        const text = await r.text();
        let json = null; try { json = JSON.parse(text); } catch (_) {}
        return { ok: r.ok, status: r.status, json, text };
      };

      // With a SCAC, ask directly. Without one, ask them to infer it — and if their account
      // does not have inference, say so in words rather than passing their error through.
      let out = scac ? await ask({ scac }) : await ask({ auto_detect_vocc_scac: true });

      if (!out.ok && !scac) {
        const errs = (out.json && out.json.errors) || [];
        const aboutScac = errs.some(e => /scac/i.test(JSON.stringify(e)));
        if (aboutScac) {
          return res.status(400).json({ ok: false, error: 'scac_required',
            message: 'Terminal49 needs the carrier code (SCAC) for this shipment and could not '
                   + 'work it out from the number. Add it in the consignment details — it is on '
                   + 'the bill of lading, and Evergreen is EGLV, Maersk MAEU, CMA CGM CMDU, ONE ONEY.',
            detail: errs });
        }
      }

      if (!out.ok) {
        const errs = (out.json && out.json.errors) || [];
        // Their message, surfaced in full. A caller who cannot see why it failed has to guess.
        return res.status(502).json({ ok: false, error: 'terminal49_refused',
          status: out.status,
          message: errs.map(e => e.detail || e.title).filter(Boolean).join('; ')
                   || String(out.text || '').slice(0, 300),
          detail: errs.length ? errs : out.text.slice(0, 500) });
      }

      const json = out.json;
      // Whatever carrier they settled on, keep it: the next subscription for this box should
      // not have to be inferred again.
      const used = scac || (json && json.data && json.data.attributes && json.data.attributes.scac) || null;
      if (used) {
        try { db.prepare('UPDATE consignment SET scac = ? WHERE consignment_uid = ?').run(used, uid); }
        catch (_) {}
      }

      db.prepare(`INSERT INTO t49_link (client_id, consignment_uid, container_number,
                    request_number, request_type, scac, state, updated_at)
                  VALUES (@c,@u,@cont,@num,@type,@scac,'requested',@now)
                  ON CONFLICT(client_id, consignment_uid) DO UPDATE SET
                    container_number=@cont, request_number=@num, request_type=@type,
                    scac=@scac, state='requested', failed_reason=NULL, updated_at=@now`)
        .run({ c: client, u: uid, cont: c.reference || null, num: number, type,
               scac: used || null, now: new Date().toISOString() });

      // Nothing is tracked yet — the carrier has to find it first, and that arrives by webhook.
      res.json({ ok: true, state: 'requested', request_type: type, request_number: number,
        scac: used || '(being inferred)',
        note: 'Terminal49 is looking for this shipment. Tracking starts when it confirms.' });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Receiving ──
  // Terminal49 signs every delivery: HMAC-SHA256 of the raw body, hex, in
  // X-T49-Webhook-Signature. That is strictly better than a secret in the URL — a signature
  // proves the payload came from them AND that nobody altered it, where a path only proves
  // somebody knew the path.
  //
  // Both are accepted. The signature is the real check; the path secret remains so an
  // endpoint registered the old way keeps working.
  const rawJson = express.json({
    type: ['application/json', 'application/vnd.api+json'],
    // The signature is over the exact bytes sent. Re-serialising the parsed object would
    // produce different bytes and a digest that never matches.
    verify: (req, _res, buf) => { req.rawBody = buf; },
  });

  function signatureOk(req) {
    const secret = process.env.T49_SIGNING_SECRET || '';
    if (!secret) return null;                       // not configured: cannot judge
    const given = String(req.get('X-T49-Webhook-Signature') || '');
    if (!given || !req.rawBody) return false;
    const mine = crypto.createHmac('sha256', secret).update(req.rawBody).digest('hex');
    const a = Buffer.from(given), b = Buffer.from(mine);
    return a.length === b.length && crypto.timingSafeEqual(a, b);
  }

  async function handle(req, res) {
    const signed = signatureOk(req);
    const pathSecret = process.env.T49_WEBHOOK_SECRET || '';
    const given = String(req.params.secret || '');
    const pathOk = !!pathSecret && given.length === pathSecret.length && given === pathSecret;

    if (signed === false) {
      log({ outcome: 'error', note: 'signature did not verify' });
      return res.status(404).end();
    }
    // Nothing to authenticate with at all: neither a valid signature nor a matching path.
    if (!signed && !pathOk) {
      log({ outcome: 'error',
            note: given ? 'bad webhook path secret and no valid signature'
                        : 'no signature and no path secret — set T49_SIGNING_SECRET' });
      return res.status(404).end();        // 404, not 403: a prober learns nothing
    }

    // Acknowledge immediately. Terminal49 retries on failure, and a slow handler turns a
    // burst of events into a queue of retries.
    res.status(202).json({ ok: true });

    try {
      const body = req.body || {};
      const data = body.data || {};
      const event = (data.attributes && data.attributes.event) || null;
      const included = Array.isArray(body.included) ? body.included : [];

      const find = (t) => included.find(x => x.type === t) || null;
      const shipment = find('shipment');
      const container = find('container');
      const transport = find('transport_event');
      const estimate = find('estimated_event');

      const shipId = shipment && shipment.id;
      const contNo = container && container.attributes && container.attributes.number;

      // Match on their shipment id, falling back to the container number — events carry one
      // or the other depending on the type.
      let link = null;
      if (shipId) link = db.prepare('SELECT * FROM t49_link WHERE t49_shipment_id = ?').get(shipId);
      if (!link && contNo) link = db.prepare('SELECT * FROM t49_link WHERE container_number = ?').get(contNo);
      if (!link && shipment) {
        // First contact: the tracking request succeeded and now has a shipment id.
        const refs = (shipment.attributes && shipment.attributes.ref_numbers) || [];
        for (const ref of refs) {
          const cand = db.prepare('SELECT * FROM t49_link WHERE request_number = ? OR container_number = ?')
            .get(ref, ref);
          if (cand) { link = cand; break; }
        }
      }

      if (!link) {
        log({ event, shipment_id: shipId, container: contNo, outcome: 'unmatched',
              note: 'no consignment is subscribed to this shipment', payload: body });
        return;
      }

      const now = new Date().toISOString();
      if (shipment && shipment.attributes) savePorts(link.consignment_uid, shipment.attributes);
      const firstContact = shipId && link.t49_shipment_id !== shipId;
      if (firstContact) {
        db.prepare('UPDATE t49_link SET t49_shipment_id = ?, state = ?, updated_at = ? WHERE client_id = ? AND consignment_uid = ?')
          .run(shipId, 'tracking', now, link.client_id, link.consignment_uid);

        // Tracking has just started, so everything before this moment is history that will
        // never be delivered. Fetch it once, now.
        backfill(link.consignment_uid)
          .then(r => console.log('[t49] backfill on first contact:', r && r.note))
          .catch(e => console.warn('[t49] backfill failed:', e.message));
      }

      if (event === 'tracking_request.failed' || event === 'tracking_request.awaiting_manifest') {
        const reason = (data.attributes && data.attributes.failed_reason) || event;
        db.prepare('UPDATE t49_link SET state = ?, failed_reason = ?, updated_at = ? WHERE client_id = ? AND consignment_uid = ?')
          .run(event === 'tracking_request.failed' ? 'failed' : 'requested', reason, now,
               link.client_id, link.consignment_uid);
        log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
              outcome: 'applied', note: reason, payload: body });
        return;
      }

      // ── An observation: write the actual ──
      const stage = ACTUAL_EVENTS[event];
      if (stage) {
        const when = dayOf(transport && transport.attributes &&
          (transport.attributes.timestamp || transport.attributes.actual_at));
        if (!when) {
          log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
                outcome: 'ignored', note: 'event carried no timestamp', payload: body });
          return;
        }
        const existing = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ? AND stage = ?')
          .get(link.consignment_uid, stage);

        // A person's confirmation outranks the carrier's feed. Somebody looked; the feed did
        // not know they had.
        if (existing && existing.state !== 'assumed' && existing.state !== 'carrier') {
          log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
                outcome: 'ignored', note: `already ${existing.state} by a person`, payload: body });
          return;
        }

        db.prepare(`INSERT INTO consignment_milestone
                      (consignment_uid, stage, planned_at, actual_at, state, source_user, source_detail, recorded_at)
                    VALUES (@uid,@stage,@planned,@actual,'carrier','terminal49',@detail,@at)
                    ON CONFLICT(consignment_uid, stage) DO UPDATE SET
                      actual_at=@actual, state='carrier', source_user='terminal49',
                      source_detail=@detail, recorded_at=@at`)
          .run({ uid: link.consignment_uid, stage,
                 planned: existing ? existing.planned_at : null,
                 actual: when, detail: event, at: now });

        // A real departure moves everything downstream that nobody has spoken for.
        try { if (typeof refreshPlanned === 'function') refreshPlanned(link.consignment_uid); } catch (_) {}

        log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
              outcome: 'applied', note: `${stage} = ${when}`, payload: body });
        return;
      }

      // ── An opinion about the future: move the plan, never the quote ──
      const estStage = ESTIMATE_EVENTS[event];
      if (estStage) {
        const eta = dayOf((estimate && estimate.attributes &&
            (estimate.attributes.estimated_at || estimate.attributes.timestamp))
          || (transport && transport.attributes &&
            (transport.attributes.estimated_at || transport.attributes.timestamp)));
        if (!eta) {
          log({ event, shipment_id: shipId, outcome: 'ignored', note: 'no estimate in payload', payload: body });
          return;
        }
        const row = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ? AND stage = ?')
          .get(link.consignment_uid, estStage);
        if (row && row.actual_at) {
          log({ event, shipment_id: shipId, applied_to: link.consignment_uid, outcome: 'ignored',
                note: `${estStage} already happened; a revised estimate is moot`, payload: body });
          return;
        }
        // Stored on the consignment, not into the milestone's planned date: the plan is
        // recomputed whenever anything changes, and a date written straight into it was being
        // erased moments later by that recompute.
        const col = estStage === 'departed' ? 'carrier_etd' : 'carrier_eta';
        // The value being replaced is only readable before the update.
        if (typeof logEstimate === 'function') { try { logEstimate(link.consignment_uid, col, eta, 'tracking'); } catch (_) {} }
        db.prepare(`UPDATE consignment SET ${col} = ?, carrier_est_at = ? WHERE consignment_uid = ?`)
          .run(eta, now, link.consignment_uid);

        try { if (typeof refreshPlanned === 'function') refreshPlanned(link.consignment_uid); } catch (_) {}
        log({ event, shipment_id: shipId, applied_to: link.consignment_uid, outcome: 'applied',
              note: `planned ${estStage} = ${eta} (quote untouched)`, payload: body });
        return;
      }

      // ── Terminal facts: holds, last free day, availability ──
      // Not milestones, but the holds feed the customs-hold count people already read, and the
      // last free day is where demurrage starts.
      if (event === 'container.updated' || event.startsWith('container.pickup_lfd')
          || event === 'container.pod_terminal_changed') {
        const a = (container && container.attributes) || {};
        const dl = a.import_deadlines || {};
        const lfd = dayOf(a.pickup_lfd || dl.pickup_lfd || dl.pickup_lfd_terminal || dl.pickup_lfd_line);
        const holds = a.holds_at_pod_terminal || null;
        db.prepare(`UPDATE t49_link SET pickup_lfd = COALESCE(@lfd, pickup_lfd),
                      holds = COALESCE(@holds, holds),
                      available_at = COALESCE(@avail, available_at),
                      pod_terminal = COALESCE(@term, pod_terminal),
                      updated_at = @now
                    WHERE client_id = @c AND consignment_uid = @u`)
          .run({ lfd, holds: holds ? JSON.stringify(holds) : null,
                 avail: dayOf(a.available_for_pickup_at), term: a.pod_terminal_name || null,
                 now, c: link.client_id, u: link.consignment_uid });
        log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
              outcome: 'applied',
              note: [lfd ? 'LFD ' + lfd : null,
                     holds && holds.length ? holds.length + ' hold(s)' : null].filter(Boolean).join(' · ')
                    || 'attributes updated', payload: body });
        return;
      }

      // ── Context: explains a long transit, or starts the last mile ──
      const ctx = CONTEXT_EVENTS[event];
      if (ctx && /transshipment/.test(event) && typeof addViaPort === 'function') {
        try { const via = viaFromEvent(transport, included); if (via) addViaPort(link.consignment_uid, via); } catch (_) {}
      }
      if (ctx) {
        const when = dayOf(transport && transport.attributes &&
          (transport.attributes.timestamp || transport.attributes.actual_at));
        db.prepare('UPDATE t49_link SET last_context = ?, updated_at = ? WHERE client_id = ? AND consignment_uid = ?')
          .run(`${ctx}${when ? ' ' + when : ''}`, now, link.client_id, link.consignment_uid);
        log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
              outcome: 'applied', note: ctx + (when ? ' on ' + when : ''), payload: body });
        return;
      }

      log({ event, shipment_id: shipId, container: contNo, applied_to: link.consignment_uid,
            outcome: 'ignored', note: 'event not mapped', payload: body });
    } catch (e) {
      console.error('[t49] webhook handling failed:', e);
      log({ outcome: 'error', note: String(e.message || e) });
    }
  }

  // Signed deliveries land here; the path-secret form is kept for an endpoint already
  // registered that way.
  router.post('/webhook', rawJson, handle);
  router.post('/webhook/:secret', rawJson, handle);

  // ── Catching up on what already happened ──
  // Webhooks only carry events that fire AFTER you subscribe. A container subscribed
  // mid-voyage has a departure, possibly an arrival, and an ETA already sitting in
  // Terminal49 — none of which will ever be delivered. Without this, those stages stay empty
  // and nobody can tell "did not happen" from "happened before we were watching", which is
  // the exact ambiguity this project exists to remove.
  async function backfill(uid) {
    const key = process.env.T49_API_KEY;
    if (!key) return { ok: false, error: 'T49_API_KEY is not set.' };

    const link = db.prepare('SELECT * FROM t49_link WHERE consignment_uid = ?').get(uid);
    if (!link) return { ok: false, error: 'This consignment is not subscribed.' };

    const get = async (path) => {
      const r = await fetch(API + path, {
        headers: { 'Authorization': 'Token ' + key, 'Accept': 'application/vnd.api+json' },
      });
      const text = await r.text();
      let json = null; try { json = JSON.parse(text); } catch (_) {}
      return { ok: r.ok, status: r.status, json, text };
    };

    // Find the shipment: by the id we already hold, or by searching their records for the
    // number we subscribed with.
    let shipment = null, containers = [];
    if (link.t49_shipment_id) {
      const r = await get(`/shipments/${encodeURIComponent(link.t49_shipment_id)}?include=containers`);
      if (r.ok && r.json) { shipment = r.json.data; containers = (r.json.included || []).filter(x => x.type === 'container'); }
    }
    if (!shipment) {
      const q = await get(`/shipments?q=${encodeURIComponent(link.request_number || link.container_number || '')}&include=containers`);
      // A list endpoint returns an array; a lookup that resolves to one returns an object.
      // Accept either rather than silently finding nothing.
      const found = q.ok && q.json
        ? (Array.isArray(q.json.data) ? q.json.data[0] : q.json.data)
        : null;
      if (found && found.id) {
        shipment = found;
        containers = (q.json.included || []).filter(x => x.type === 'container');
      }
    }
    if (!shipment) {
      return { ok: false, error: 'not_found',
        message: 'Terminal49 has no shipment for this number yet. If the request is still '
               + 'awaiting manifest, try again once it has been found.' };
    }

    if (shipment.id && link.t49_shipment_id !== shipment.id) {
      db.prepare(`UPDATE t49_link SET t49_shipment_id = ?, state = 'tracking', updated_at = ?
                   WHERE consignment_uid = ?`).run(shipment.id, new Date().toISOString(), uid);
    }

    // Prefer our own container where the shipment carries several.
    const mine = containers.find(c => c.attributes && c.attributes.number === link.container_number)
              || containers[0] || null;
    const sa = shipment.attributes || {};
    const ca = (mine && mine.attributes) || {};

    const applied = [], skipped = [];
    const now = new Date().toISOString();

    // Their field names for the things we treat as observations.
    const facts = [
      ['departed', ca.pod_vessel_departed_at || sa.pol_vessel_departed_at || ca.pol_vessel_departed_at],
      ['arrived', ca.pod_vessel_arrived_at || sa.pod_vessel_arrived_at],
      ['dest_cleared', ca.available_for_pickup_at || ca.pod_full_out_at],
    ];

    for (const [stage, raw] of facts) {
      const when = dayOf(raw);
      if (!when) { skipped.push({ stage, why: 'Terminal49 has no date for this yet' }); continue; }
      const existing = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ? AND stage = ?')
        .get(uid, stage);
      // A person's record is never overwritten by a catch-up.
      if (existing && existing.state !== 'assumed' && existing.state !== 'carrier') {
        skipped.push({ stage, why: `already ${existing.state} by a person` });
        continue;
      }
      db.prepare(`INSERT INTO consignment_milestone
                    (consignment_uid, stage, planned_at, actual_at, state, source_user, source_detail, recorded_at)
                  VALUES (@uid,@stage,@planned,@actual,'carrier','terminal49','backfill',@at)
                  ON CONFLICT(consignment_uid, stage) DO UPDATE SET
                    actual_at=@actual, state='carrier', source_user='terminal49',
                    source_detail='backfill', recorded_at=@at`)
        .run({ uid, stage, planned: existing ? existing.planned_at : null, actual: when, at: now });
      applied.push({ stage, date: when });
    }

    // And their current expectations, which move the plan but never a quote.
    const etd = dayOf(ca.pol_etd_at || sa.pol_etd_at);
    const eta = dayOf(ca.pod_eta_at || sa.pod_eta_at);
    savePorts(uid, sa);
    if (typeof logEstimate === 'function') {
      try { if (etd) logEstimate(uid, 'carrier_etd', etd, 'tracking'); if (eta) logEstimate(uid, 'carrier_eta', eta, 'tracking'); } catch (_) {}
    }
    if (etd || eta) {
      db.prepare(`UPDATE consignment SET carrier_etd = COALESCE(?, carrier_etd),
                    carrier_eta = COALESCE(?, carrier_eta), carrier_est_at = ?
                  WHERE consignment_uid = ?`).run(etd, eta, now, uid);
    }

    // Terminal facts worth having now rather than at the next update event.
    const dl = ca.import_deadlines || {};
    db.prepare(`UPDATE t49_link SET pickup_lfd = COALESCE(?, pickup_lfd),
                  holds = COALESCE(?, holds), pod_terminal = COALESCE(?, pod_terminal), updated_at = ?
                WHERE consignment_uid = ?`)
      .run(dayOf(ca.pickup_lfd || dl.pickup_lfd || dl.pickup_lfd_terminal),
           ca.holds_at_pod_terminal ? JSON.stringify(ca.holds_at_pod_terminal) : null,
           ca.pod_terminal_name || null, now, uid);

    try { if (typeof refreshPlanned === 'function') refreshPlanned(uid); } catch (_) {}

    log({ event: 'backfill', shipment_id: shipment.id, container: ca.number || link.container_number,
          applied_to: uid, outcome: 'applied',
          note: applied.length ? applied.map(a => `${a.stage}=${a.date}`).join(' · ') : 'nothing to apply',
          payload: { shipment: sa, container: ca } });

    return { ok: true, shipment_id: shipment.id, container: ca.number || link.container_number,
             applied, skipped,
             carrier_etd: etd || null, carrier_eta: eta || null,
             pickup_lfd: dayOf(ca.pickup_lfd || dl.pickup_lfd || dl.pickup_lfd_terminal) || null,
             note: applied.length
               ? 'Recorded from what Terminal49 already holds. Future milestones arrive by webhook.'
               : 'Terminal49 has no completed milestones for this shipment yet.' };
  }

  router.post('/backfill', authenticateRequest, requireRole(['admin', 'supplier']),
    auditLog('t49_backfill'), async (req, res) => {
    try {
      const uid = String((req.body && req.body.consignment_uid) || '').trim();
      const c = db.prepare('SELECT consignment_uid FROM consignment WHERE consignment_uid = ? AND client_id = ?')
        .get(uid, curClient());
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });
      const out = await backfill(uid);
      res.status(out.ok ? 200 : 400).json(out);
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Looking at what happened ──
  // ── Ports for consignments tracked before ports were kept ──
  // Every backfill stored the shipment it read, and every webhook stored its payload. Reading
  // those back fills the ports without calling the provider. Idempotent: it only touches
  // consignments that have no port yet, and never overwrites one.
  function portsFromStored() {
    if (typeof recordPorts !== 'function') return { scanned: 0, filled: 0, via: 0 };
    let scanned = 0, filled = 0, via = 0;
    try {
      const need = new Set(db.prepare(`SELECT consignment_uid FROM consignment
                                         WHERE origin_port IS NULL AND dest_port IS NULL AND mode = 'Sea'`).all().map(r => r.consignment_uid));
      const rows = db.prepare(`SELECT applied_to, event, payload FROM t49_event
                                WHERE applied_to IS NOT NULL AND payload IS NOT NULL
                                  AND (event = 'backfill' OR payload LIKE '%port_of_%' OR event LIKE '%transshipment%')
                                ORDER BY id`).all();
      for (const r of rows) {
        scanned++;
        let body; try { body = JSON.parse(r.payload); } catch (_) { continue; }
        if (need.has(r.applied_to)) {
          const sa = r.event === 'backfill'
            ? (body && body.shipment)
            : ((Array.isArray(body && body.included) ? body.included : []).find(x => x && x.type === 'shipment') || {}).attributes;
          if (sa && savePorts(r.applied_to, sa)) { filled++; need.delete(r.applied_to); }
        }
        if (/transshipment/.test(String(r.event || '')) && typeof addViaPort === 'function') {
          const inc = Array.isArray(body && body.included) ? body.included : [];
          const tr = inc.find(x => x && x.type === 'transport_event');
          const v = viaFromEvent(tr, inc);
          if (v && addViaPort(r.applied_to, v)) via++;
        }
      }
    } catch (e) { console.warn('[t49] ports from stored payloads:', e.message); }
    return { scanned, filled, via };
  }
  try {
    const r = portsFromStored();
    if (r.scanned) console.log(`[t49] ports from stored payloads: ${r.filled} consignment(s) filled, ${r.via} transshipment port(s), ${r.scanned} payload(s) read`);
  } catch (_) {}

  // Read-only: what the stored shipments actually call their port fields. Run once after
  // deploy to confirm the names before trusting the route lines.
  router.get('/ports-check', authenticateRequest, requireRole(['admin']), (req, res) => {
    try {
      const row = db.prepare(`SELECT payload FROM t49_event WHERE event = 'backfill' AND payload IS NOT NULL ORDER BY id DESC LIMIT 1`).get();
      let keys = [];
      if (row) { try { const sa = (JSON.parse(row.payload) || {}).shipment || {}; keys = Object.keys(sa).filter(k => /port|pol|pod|locode|destination/i.test(k)); } catch (_) {} }
      const counts = db.prepare(`SELECT COUNT(*) total, SUM(CASE WHEN origin_port IS NOT NULL OR dest_port IS NOT NULL THEN 1 ELSE 0 END) with_ports
                                   FROM consignment WHERE client_id = ? AND mode = 'Sea'`).get(curClient());
      res.json({ ok: true, port_fields_seen: keys, sea_consignments: counts.total || 0, with_ports: counts.with_ports || 0,
                 rerun: portsFromStored() });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  router.get('/status', authenticateRequest, (req, res) => {
    const client = curClient();
    const links = db.prepare('SELECT * FROM t49_link WHERE client_id = ? ORDER BY updated_at DESC').all(client);
    const recent = db.prepare(`SELECT id, received_at, event, container, applied_to, outcome, note
                                 FROM t49_event ORDER BY id DESC LIMIT 50`).all();
    res.json({ ok: true,
      configured: !!process.env.T49_API_KEY,
      webhook_configured: !!(process.env.T49_SIGNING_SECRET || process.env.T49_WEBHOOK_SECRET),
      signature_verification: !!process.env.T49_SIGNING_SECRET,
      webhook_url: process.env.T49_SIGNING_SECRET
        ? '/t49/webhook   (signed — no secret in the path)'
        : '/t49/webhook/<T49_WEBHOOK_SECRET>',
      subscriptions: links, recent_events: recent });
  });

  console.log('[t49] integration v1 mounted');
  return router;
};
