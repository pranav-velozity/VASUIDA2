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

module.exports = function mountT49(deps) {
  const { express, db, authenticateRequest, requireRole, auditLog, curClient, refreshPlanned } = deps;
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

      const body = { data: { type: 'tracking_request', attributes: {
        request_type: type, request_number: number,
        scac: req.body.scac || undefined,          // omitted: Terminal49 infers the carrier
        ref_numbers: [c.reference, c.shipment_ref].filter(Boolean),
      } } };

      const r = await fetch(API + '/tracking_requests', {
        method: 'POST',
        headers: { 'Authorization': 'Token ' + key,
                   'Content-Type': 'application/vnd.api+json',
                   'Accept': 'application/vnd.api+json' },
        body: JSON.stringify(body),
      });
      const text = await r.text();
      let json = null; try { json = JSON.parse(text); } catch (_) {}
      if (!r.ok) {
        return res.status(502).json({ ok: false, error: 'terminal49_refused',
          status: r.status, detail: (json && json.errors) || text.slice(0, 300) });
      }

      db.prepare(`INSERT INTO t49_link (client_id, consignment_uid, container_number,
                    request_number, request_type, scac, state, updated_at)
                  VALUES (@c,@u,@cont,@num,@type,@scac,'requested',@now)
                  ON CONFLICT(client_id, consignment_uid) DO UPDATE SET
                    container_number=@cont, request_number=@num, request_type=@type,
                    scac=@scac, state='requested', failed_reason=NULL, updated_at=@now`)
        .run({ c: client, u: uid, cont: c.reference || null, num: number, type,
               scac: req.body.scac || null, now: new Date().toISOString() });

      // Nothing is tracked yet — the carrier has to find it first, and that arrives by webhook.
      res.json({ ok: true, state: 'requested', request_type: type, request_number: number,
        note: 'Terminal49 is looking for this shipment. Tracking starts when it confirms.' });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Receiving ──
  // The secret is in the path rather than a header: Terminal49 posts a plain JSON:API document
  // with no signature of its own, so an unguessable URL is what stands between this endpoint
  // and anyone who finds it. Compared as a whole string, not a prefix.
  router.post('/webhook/:secret', express.json({ type: ['application/json', 'application/vnd.api+json'] }),
    async (req, res) => {
    const expected = process.env.T49_WEBHOOK_SECRET || '';
    const given = String(req.params.secret || '');
    if (!expected || given.length !== expected.length || given !== expected) {
      log({ outcome: 'error', note: 'bad webhook secret' });
      return res.status(404).end();        // 404, not 403: an attacker learns nothing
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
      if (shipId && link.t49_shipment_id !== shipId) {
        db.prepare('UPDATE t49_link SET t49_shipment_id = ?, state = ?, updated_at = ? WHERE client_id = ? AND consignment_uid = ?')
          .run(shipId, 'tracking', now, link.client_id, link.consignment_uid);
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
  });

  // ── Looking at what happened ──
  router.get('/status', authenticateRequest, (req, res) => {
    const client = curClient();
    const links = db.prepare('SELECT * FROM t49_link WHERE client_id = ? ORDER BY updated_at DESC').all(client);
    const recent = db.prepare(`SELECT id, received_at, event, container, applied_to, outcome, note
                                 FROM t49_event ORDER BY id DESC LIMIT 50`).all();
    res.json({ ok: true,
      configured: !!process.env.T49_API_KEY,
      webhook_configured: !!process.env.T49_WEBHOOK_SECRET,
      subscriptions: links, recent_events: recent });
  });

  console.log('[t49] integration v1 mounted');
  return router;
};
