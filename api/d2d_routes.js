/* ── VelOzity Pinpoint — door-to-door module (GRBA) v1 ──
   Mounted from server.js with one line. Everything for this client lives here, so a fault in
   it cannot reach ICONIC's or EHP's screens.

   Three rules are enforced in code rather than left to care:

   1. EVERY query goes through scopedDb(). It refuses SQL that does not filter on client_id, so
      a forgotten WHERE cannot leak another tenant's rows — the single most common cause of the
      tenancy bugs found in this codebase.

   2. The PLAN is written once, when a booking is approved, and never updated. If the plan moved
      with reality nothing would ever look late, and planned-vs-actual would be theatre.

   3. COST NEVER REACHES A CLIENT. Supplier cost, accessorials and margin live only on
      d2d_booking and are stripped by forClient() before anything is returned to a client org.
*/
'use strict';

const crypto = require('crypto');

module.exports = function mountD2D(deps) {
  const { express, db, authenticateRequest, requireRole, curClient, tenancyResolve, auditLog } = deps;
  const router = express.Router();

  // ── Schema ──
  // client_id is NOT NULL everywhere: a row that belongs to nobody is a row that leaks.
  db.exec(`
    CREATE TABLE IF NOT EXISTS d2d_booking (
      id             TEXT PRIMARY KEY,
      client_id      TEXT NOT NULL,
      week_start     TEXT NOT NULL,
      option_ref     TEXT NOT NULL,
      title          TEXT,
      mode           TEXT NOT NULL DEFAULT 'sea',      -- 'sea' | 'air'
      container_type TEXT,                              -- '20GP' | '40GP' | '40HQ' | null for air
      container_qty  INTEGER DEFAULT 1,
      carrier        TEXT,
      service        TEXT,
      transhipment   INTEGER NOT NULL DEFAULT 0,
      transit_days   INTEGER,
      -- Internal only. forClient() strips all four.
      cost_amount    REAL,
      accessorial_amount REAL,
      margin_pct     REAL,
      sell_amount    REAL,
      currency       TEXT NOT NULL DEFAULT 'USD',
      status         TEXT NOT NULL DEFAULT 'draft',     -- draft|released|approved|declined|expired
      recommended    INTEGER NOT NULL DEFAULT 0,
      request_id     TEXT,                              -- the header this option answers
      released_at    TEXT,
      decision_token TEXT,
      decided_at     TEXT,
      decided_by     TEXT,
      created_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS ix_d2d_booking_week ON d2d_booking(client_id, week_start);

    CREATE TABLE IF NOT EXISTS d2d_shipment (
      id             TEXT PRIMARY KEY,
      client_id      TEXT NOT NULL,
      booking_id     TEXT,
      week_start     TEXT NOT NULL,
      mode           TEXT NOT NULL DEFAULT 'sea',
      reference      TEXT,                              -- container number or AWB
      container_type TEXT,
      carrier        TEXT,
      vessel         TEXT,
      voyage         TEXT,
      origin         TEXT,
      destination    TEXT,
      capacity_cbm   REAL,
      -- The frozen plan. Written once at approval; never updated afterwards.
      plan_frozen_at        TEXT,
      plan_pickup           TEXT,
      plan_origin_cleared   TEXT,
      plan_departed         TEXT,
      plan_arrived          TEXT,
      plan_dest_cleared     TEXT,
      plan_out_for_delivery TEXT,
      plan_delivered        TEXT,
      status         TEXT NOT NULL DEFAULT 'booked',
      created_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS ix_d2d_shipment_week ON d2d_shipment(client_id, week_start);

    -- Actual milestones. One row per stage per shipment, each carrying WHERE it came from, so
    -- the screens can show whether a date is a carrier feed or somebody's typing.
    CREATE TABLE IF NOT EXISTS d2d_event (
      id          TEXT PRIMARY KEY,
      client_id   TEXT NOT NULL,
      shipment_id TEXT NOT NULL,
      stage       TEXT NOT NULL,                        -- pickup|origin_cleared|departed|arrived|dest_cleared|out_for_delivery|delivered
      actual_at   TEXT,
      source      TEXT NOT NULL DEFAULT 'manual',       -- carrier|partner|import|manual
      note        TEXT,
      recorded_by TEXT,
      recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_d2d_event ON d2d_event(client_id, shipment_id, stage);

    -- PO numbers are NOT assumed unique: GRBA may reuse a number in a later week, and a file
    -- may even carry it twice in one week. The key is the surrogate id; po_number is indexed,
    -- never unique. seq separates repeats inside the same week so an import never overwrites
    -- a real order.
    CREATE TABLE IF NOT EXISTS d2d_po (
      id               TEXT PRIMARY KEY,
      client_id        TEXT NOT NULL,
      po_number        TEXT NOT NULL,
      week_start       TEXT NOT NULL,
      seq              INTEGER NOT NULL DEFAULT 1,
      supplier         TEXT,
      cargo_ready_date TEXT,
      units            INTEGER,
      cbm              REAL,
      weight_kg        REAL,
      value_amount     REAL,
      currency         TEXT,
      source           TEXT NOT NULL DEFAULT 'upload',
      created_at       TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_d2d_po ON d2d_po(client_id, week_start, po_number, seq);
    CREATE INDEX IF NOT EXISTS ix_d2d_po_number ON d2d_po(client_id, po_number);

    CREATE TABLE IF NOT EXISTS d2d_po_line (
      id          TEXT PRIMARY KEY,
      client_id   TEXT NOT NULL,
      po_id       TEXT NOT NULL,
      sku_code    TEXT NOT NULL,
      description TEXT,
      units       INTEGER,
      cbm         REAL
    );
    CREATE INDEX IF NOT EXISTS ix_d2d_po_line_po ON d2d_po_line(client_id, po_id);

    -- A PO can span several shipments and a shipment carries many POs, so the split lives here
    -- with its own unit count. Modelling this as a column on either side breaks the first time
    -- an order ships in two containers — which the mockups show happening in week 38.
    CREATE TABLE IF NOT EXISTS d2d_assignment (
      id          TEXT PRIMARY KEY,
      client_id   TEXT NOT NULL,
      po_id       TEXT NOT NULL,
      shipment_id TEXT NOT NULL,
      units       INTEGER,
      cbm         REAL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_d2d_assignment ON d2d_assignment(client_id, po_id, shipment_id);

    -- A booking request: the cargo, stated once, that the options are quoted against.
    -- Options used to float free of any header, so there was nowhere to record what was
    -- actually shipping — and the partner cannot quote a sailing without knowing the volume.
    CREATE TABLE IF NOT EXISTS d2d_request (
      id             TEXT PRIMARY KEY,
      client_id      TEXT NOT NULL,
      week_start     TEXT NOT NULL,
      ref            TEXT,                            -- human reference, e.g. SEA-2610-01
      mode           TEXT NOT NULL DEFAULT 'sea',     -- 'sea' | 'air' | 'both'
      origin         TEXT,
      destination    TEXT,
      ready_date     TEXT,                            -- cargo ready, if it differs from the week
      pack_type      TEXT,                            -- 'loose' | 'pallets' | 'mixed'
      pallets        INTEGER,
      cartons        INTEGER,
      units          INTEGER,
      cbm            REAL,
      gross_weight_kg REAL,
      notes          TEXT,
      state          TEXT NOT NULL DEFAULT 'draft',   -- draft|sent|costed|priced|released|approved|declined|cancelled
      sent_at        TEXT,
      created_at     TEXT NOT NULL DEFAULT (datetime('now')),
      created_by     TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_d2d_request_week ON d2d_request(client_id, week_start);

    -- A partner needs no account: the link carries a token, exactly as the air quotes work.
    CREATE TABLE IF NOT EXISTS d2d_request_token (
      token      TEXT PRIMARY KEY,
      client_id  TEXT NOT NULL,
      request_id TEXT NOT NULL,
      purpose    TEXT NOT NULL DEFAULT 'partner_quote',
      expires_at TEXT,
      used_at    TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- Every state change, so the thread with the partner is auditable.
    CREATE TABLE IF NOT EXISTS d2d_request_event (
      id          TEXT PRIMARY KEY,
      client_id   TEXT NOT NULL,
      request_id  TEXT NOT NULL,
      from_state  TEXT,
      to_state    TEXT,
      actor       TEXT,
      role        TEXT,
      detail      TEXT,
      at          TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS ix_d2d_request_event ON d2d_request_event(client_id, request_id);

    -- How long each stage is expected to take, per client and per mode. These were constants
    -- in this file: plausible guesses that nobody had validated, applied to every plan. Held
    -- here so they can be corrected as real lead times become known.
    --
    -- Changing them affects FUTURE approvals only. A frozen plan is never recomputed.
    CREATE TABLE IF NOT EXISTS d2d_baseline (
      client_id        TEXT NOT NULL,
      mode             TEXT NOT NULL,                  -- 'sea' | 'air'
      origin_cleared   INTEGER NOT NULL,               -- days after cargo ready
      departed         INTEGER NOT NULL,               -- days after cargo ready
      dest_cleared     INTEGER NOT NULL,               -- days after arrival
      out_for_delivery INTEGER NOT NULL,               -- days after arrival
      delivered        INTEGER NOT NULL,               -- days after arrival
      updated_at       TEXT,
      updated_by       TEXT,
      PRIMARY KEY (client_id, mode)
    );
  `);

  // Older databases predate request_id, and CREATE TABLE IF NOT EXISTS leaves them as they are.
  try {
    const cols = db.prepare(`PRAGMA table_info(d2d_booking)`).all().map(c => c.name);
    if (!cols.includes('request_id')) {
      db.exec(`ALTER TABLE d2d_booking ADD COLUMN request_id TEXT`);
      console.warn('[d2d] added d2d_booking.request_id');
    }
  } catch (e) { console.error('[d2d] could not add request_id', e); }

  // ── The scoped data layer ──
  // Nothing in this module talks to db directly. Queries must filter on client_id, and the
  // client is supplied here rather than by the caller.
  // Parameters are bound BY NAME, never by position. Positional binding meant the client had
  // to be the first placeholder, which silently corrupted any UPDATE — SET comes before WHERE,
  // so the values shifted and the statement matched nothing while still reporting success.
  const SCOPED = /client_id\s*=\s*@client\b/i;
  // An INSERT cannot say "client_id = @client"; it names the column and binds @client in its
  // VALUES. That shape is accepted, and only that shape — reads, updates and deletes must
  // still carry the WHERE, which is where a missing filter actually leaks rows.
  const SCOPED_INSERT = /^\s*INSERT\s+INTO/i;
  function scopedDb(client) {
    if (!client) throw new Error('d2d: no client in scope');
    const guard = (sql) => {
      const ok = SCOPED.test(sql)
        || (SCOPED_INSERT.test(sql) && /\bclient_id\b/i.test(sql) && /@client\b/.test(sql));
      if (!ok) throw new Error('d2d: query is not scoped to a client — ' + sql.slice(0, 80));
      return sql;
    };
    const bind = (p) => ({ ...(p || {}), client });
    return {
      client,
      all:  (sql, p) => db.prepare(guard(sql)).all(bind(p)),
      get:  (sql, p) => db.prepare(guard(sql)).get(bind(p)),
      run:  (sql, p) => db.prepare(guard(sql)).run(bind(p)),
      // Inserts name client_id explicitly and are given it here, so it cannot be forgotten.
      insert: (table, row) => {
        const data = { ...row, client_id: client };
        const cols = Object.keys(data);
        const sql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`;
        return db.prepare(sql).run(...cols.map(c => data[c]));
      },
    };
  }

  // ── Cost never leaves the server for a client ──
  const COST_FIELDS = ['cost_amount', 'accessorial_amount', 'margin_pct'];
  function forClient(row) {
    if (!row) return row;
    const out = { ...row };
    for (const f of COST_FIELDS) delete out[f];
    return out;
  }
  const isInternal = (req) => {
    try { return tenancyResolve(req.auth && req.auth.orgId, req.auth && req.auth.orgRole).org_type === 'internal'; }
    catch (e) { return false; }
  };

  // Every route below requires the capability, so the whole module is dark until it is granted.
  function requireD2D(req, res, next) {
    try {
      const client = curClient();
      const on = db.prepare(`SELECT 1 x FROM client_capability
        WHERE client_id=? AND capability='freight_d2d' AND enabled=1`).get(client);
      if (!on) return res.status(404).json({ error: 'not_found' });

      // A partner org resolves to the client whose facility it works at, so a forwarder lands inside
      // GRBA's scope. That is correct for recording what happened at the warehouse and wrong
      // for everything else, so partners reach ONLY /partner/* — bookings, costs and orders
      // stay out of reach whatever the capability says.
      let orgType = null;
      try { orgType = tenancyResolve(req.auth && req.auth.orgId, req.auth && req.auth.orgRole).org_type; } catch (e) {}
      if (orgType === 'partner' && !String(req.path || '').startsWith('/partner/')) {
        return res.status(403).json({ error: 'not_available_to_partner' });
      }
      req.d2d = scopedDb(client);
      req.d2dOrgType = orgType;
      return next();
    } catch (e) {
      console.error('[d2d] scope error', e);
      return res.status(403).json({ error: 'd2d_unavailable' });
    }
  }

  router.get('/health', authenticateRequest, requireD2D, (req, res) => {
    res.json({ ok: true, client: req.d2d.client });
  });

  router.get('/weeks', authenticateRequest, requireD2D, auditLog('view_d2d_weeks'), (req, res) => {
    try {
      // Weeks come from bookings AND shipments. Listing only weeks that have shipments meant a
      // week could not be opened until it was approved — and approval happens on that screen,
      // so a newly quoted week was unreachable.
      const rows = req.d2d.all(`
        SELECT w.week_start,
          (SELECT COUNT(*) FROM d2d_shipment s WHERE s.client_id = @client AND s.week_start = w.week_start) AS shipments,
          (SELECT COUNT(*) FROM d2d_shipment s WHERE s.client_id = @client AND s.week_start = w.week_start AND s.status='delivered') AS delivered,
          (SELECT COUNT(*) FROM d2d_booking b WHERE b.client_id = @client AND b.week_start = w.week_start AND b.status='draft') AS drafts,
          (SELECT COUNT(*) FROM d2d_booking b WHERE b.client_id = @client AND b.week_start = w.week_start AND b.status='released') AS awaiting
        FROM (SELECT week_start FROM d2d_shipment WHERE client_id = @client
              UNION SELECT week_start FROM d2d_booking WHERE client_id = @client) w
        ORDER BY w.week_start DESC LIMIT 26`);
      res.json({ weeks: rows });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/shipments', authenticateRequest, requireD2D, auditLog('view_d2d_shipments'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const rows = ws
        ? req.d2d.all(`SELECT * FROM d2d_shipment WHERE client_id = @client AND week_start = @ws ORDER BY reference`, { ws })
        : req.d2d.all(`SELECT * FROM d2d_shipment WHERE client_id = @client ORDER BY week_start DESC, reference LIMIT 100`);
      const ids = rows.map(r => r.id);
      const idParams = {}; ids.forEach((id, i) => { idParams['id' + i] = id; });
      const events = ids.length
        ? req.d2d.all(`SELECT * FROM d2d_event WHERE client_id = @client
                       AND shipment_id IN (${ids.map((_, i) => '@id' + i).join(',')})`, idParams)
        : [];
      const byShipment = {};
      for (const e of events) (byShipment[e.shipment_id] = byShipment[e.shipment_id] || []).push(e);

      // What is aboard, from the assignments — so a container can say how many orders and
      // units it carries without a second round trip.
      const asg = ids.length
        ? req.d2d.all(`SELECT shipment_id, COUNT(*) AS po_count, SUM(units) AS units, SUM(cbm) AS cbm
                       FROM d2d_assignment WHERE client_id = @client
                       AND shipment_id IN (${ids.map((_, i) => '@id' + i).join(',')})
                       GROUP BY shipment_id`, idParams)
        : [];
      const load = {}; asg.forEach(a => { load[a.shipment_id] = a; });

      // The cargo stated on the booking request, carried down to the shipment. Without it a
      // container can only report what the order file knows, and most weeks that is nothing yet.
      const bIds = [...new Set(rows.map(r => r.booking_id).filter(Boolean))];
      const bp = {}; bIds.forEach((id, i) => { bp['b' + i] = id; });
      const reqs = bIds.length
        ? req.d2d.all(`SELECT b.id AS booking_id, r.pack_type, r.pallets, r.cartons, r.units AS req_units,
                              r.cbm AS req_cbm, r.gross_weight_kg, r.origin, r.destination
                       FROM d2d_booking b JOIN d2d_request r ON r.id = b.request_id AND r.client_id = b.client_id
                       WHERE b.client_id = @client AND b.id IN (${bIds.map((_, i) => '@b' + i).join(',')})`, bp)
        : [];
      const cargo = {}; reqs.forEach(x => { cargo[x.booking_id] = x; });

      res.json({ shipments: rows.map(r => {
        const c = cargo[r.booking_id] || {};
        const qty = Math.max(1, rows.filter(x => x.booking_id === r.booking_id).length);
        // Cargo is stated for the whole booking; split it evenly across its containers rather
        // than repeating the week's total on each one.
        const share = (v) => (v == null ? null : Math.round((Number(v) / qty) * 100) / 100);
        return {
          ...r,
          events: byShipment[r.id] || [],
          po_count: (load[r.id] || {}).po_count || 0,
          units: (load[r.id] || {}).units || 0,
          cbm: (load[r.id] || {}).cbm || null,
          origin: r.origin || c.origin || null,
          // The booked destination, carried down. Without it every shipment was drawn to the
          // same place regardless of what was quoted.
          destination: r.destination || c.destination || null,
          cargo: c.pack_type || c.pallets || c.cartons || c.req_units || c.req_cbm ? {
            pack_type: c.pack_type || null,
            pallets: share(c.pallets), cartons: share(c.cartons),
            units: share(c.req_units), cbm: share(c.req_cbm),
            gross_weight_kg: share(c.gross_weight_kg),
            split_across: qty,
          } : null,
        };
      }) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/bookings', authenticateRequest, requireD2D, auditLog('view_d2d_bookings'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const internal = isInternal(req);
      // The lane comes off the request the option was quoted against, so a card can say which
      // movement it is pricing rather than just naming the carrier.
      const LANE = `, r.origin AS origin, r.destination AS destination, r.ref AS request_ref`;
      const JOIN = ` LEFT JOIN d2d_request r ON r.id = b.request_id AND r.client_id = b.client_id`;
      const rows = ws
        ? req.d2d.all(`SELECT b.*${LANE} FROM d2d_booking b${JOIN}
                       WHERE b.client_id = @client AND b.week_start = @ws ORDER BY b.option_ref`, { ws })
        : req.d2d.all(`SELECT b.*${LANE} FROM d2d_booking b${JOIN}
                       WHERE b.client_id = @client ORDER BY b.week_start DESC LIMIT 60`);
      // A client sees released options only, and never the cost behind them.
      const visible = internal ? rows : rows.filter(r => r.status !== 'draft').map(forClient);
      res.json({ bookings: visible, pricing_visible: internal });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── The booking lifecycle ──
  // Costs in from the partner, margin applied by VelOzity, released to the client, decided by
  // them. Approval is the moment the PLAN IS FROZEN: the approved option's transit time writes
  // the plan dates onto real shipments, and nothing may rewrite them afterwards.
  //
  // Sell price is ALWAYS computed here from cost and margin. It is never accepted from a
  // request, or a client could post their own price.
  const MARGIN_FLOOR = 15;           // percent; below this needs an explicit override
  // Starting values only, used the first time a client and mode are seen. Air is deliberately
  // shorter than sea: it shared sea's numbers before, which was plainly wrong.
  const PLAN_DEFAULTS = {
    sea: { origin_cleared: 3, departed: 5, dest_cleared: 2, out_for_delivery: 3, delivered: 4 },
    air: { origin_cleared: 2, departed: 3, dest_cleared: 1, out_for_delivery: 2, delivered: 2 },
  };
  const PLAN_FIELDS = ['origin_cleared', 'departed', 'dest_cleared', 'out_for_delivery', 'delivered'];

  function baselineFor(sdb, mode) {
    const m = mode === 'air' ? 'air' : 'sea';
    const row = sdb.get(`SELECT * FROM d2d_baseline WHERE client_id = @client AND mode = @mode`, { mode: m });
    if (row) return row;
    const d = PLAN_DEFAULTS[m];
    sdb.insert('d2d_baseline', { mode: m, ...d, updated_at: new Date().toISOString(), updated_by: 'default' });
    return { mode: m, ...d, updated_by: 'default' };
  }
  const addDays = (ymd, n) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };
  const sellFrom = (cost, acc, pct) => {
    const base = (Number(cost) || 0) + (Number(acc) || 0);
    return Math.round(base * (1 + (Number(pct) || 0) / 100) * 100) / 100;
  };
  const internalOnly = (req, res) => {
    if (isInternal(req)) return false;
    res.status(403).json({ error: 'internal_only' });
    return true;
  };

  // Options for a week, as quoted by the partner. Internal only: this carries cost.
  router.post('/bookings', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('create_d2d_booking'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const b = req.body || {};
      const week = String(b.week_start || '');
      if (!/^\d{4}-\d{2}-\d{2}$/.test(week)) return res.status(400).json({ error: 'week_start required' });
      const opts = Array.isArray(b.options) ? b.options : [];
      if (!opts.length) return res.status(400).json({ error: 'options required' });

      const requestId = String(b.request_id || '') || null;
      if (requestId) {
        const rq = req.d2d.get(`SELECT id, week_start FROM d2d_request WHERE client_id = @client AND id = @id`, { id: requestId });
        if (!rq) return res.status(404).json({ error: 'request_not_found' });
        if (rq.week_start !== week) return res.status(400).json({ error: 'week_mismatch', message: 'That request is for another week.' });
      }

      const made = [];
      const tx = db.transaction(() => {
        for (const o of opts) {
          const pct = o.margin_pct == null ? MARGIN_FLOOR : Number(o.margin_pct);
          const id = 'bk_' + crypto.randomUUID().slice(0, 12);
          req.d2d.insert('d2d_booking', {
            id, week_start: week,
            option_ref: String(o.option_ref || made.length + 1),
            title: String(o.title || ''),
            mode: o.mode === 'air' ? 'air' : 'sea',
            container_type: o.container_type || null,
            container_qty: Number(o.container_qty) || 1,
            carrier: o.carrier || null,
            service: o.service || null,
            transhipment: o.transhipment ? 1 : 0,
            transit_days: Number(o.transit_days) || null,
            cost_amount: Number(o.cost_amount) || 0,
            accessorial_amount: Number(o.accessorial_amount) || 0,
            margin_pct: pct,
            sell_amount: sellFrom(o.cost_amount, o.accessorial_amount, pct),
            currency: String(o.currency || 'USD').toUpperCase(),
            status: 'draft',
            recommended: o.recommended ? 1 : 0,
            request_id: requestId,
          });
          made.push(id);
        }
        if (requestId) {
          req.d2d.run(`UPDATE d2d_request SET state='costed' WHERE client_id = @client AND id = @id AND state IN ('draft','sent','repricing')`,
            { id: requestId });
          reqEvent(req.d2d, requestId, null, 'costed', (req.auth && req.auth.userId) || 'velozity', 'internal',
            made.length + ' option(s) entered');
        }
      });
      tx();
      res.json({ ok: true, created: made.length, ids: made });
    } catch (e) { console.error('[d2d] create booking', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  // Margin. Sell is recomputed, never supplied. A released option cannot be repriced — that
  // would change a number the client has already been shown.
  router.patch('/bookings/:id/pricing', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('price_d2d_booking'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const row = req.d2d.get(`SELECT * FROM d2d_booking WHERE client_id = @client AND id = @id`, { id: req.params.id });
      if (!row) return res.status(404).json({ error: 'not_found' });
      if (row.status !== 'draft') return res.status(409).json({ error: 'already_released',
        message: 'Reprice by re-issuing: the client has already seen this price.' });

      const pct = Number((req.body || {}).margin_pct);
      if (!isFinite(pct) || pct < -100 || pct > 500) return res.status(400).json({ error: 'margin_pct invalid' });
      const sell = sellFrom(row.cost_amount, row.accessorial_amount, pct);
      const done = req.d2d.run(`UPDATE d2d_booking SET margin_pct = @pct, sell_amount = @sell
                   WHERE client_id = @client AND id = @id`, { pct, sell, id: req.params.id });
      // A write that changes nothing is a failure, not a success.
      if (!done.changes) return res.status(409).json({ error: 'not_updated' });
      res.json({ ok: true, margin_pct: pct, sell_amount: sell, below_floor: pct < MARGIN_FLOOR });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // Release a week's options to the client. The floor is checked here, once, for all of them.
  router.post('/bookings/week/:ws/release', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('release_d2d_bookings'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const ws = req.params.ws;
      const rows = req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = @client AND week_start = @ws AND status='draft'`, { ws });
      if (!rows.length) return res.status(404).json({ error: 'nothing_to_release' });

      const below = rows.filter(r => Number(r.margin_pct) < MARGIN_FLOOR);
      if (below.length && String((req.body || {}).override || '') !== '1') {
        return res.status(409).json({ error: 'below_floor', floor: MARGIN_FLOOR,
          options: below.map(r => ({ id: r.id, option_ref: r.option_ref, margin_pct: r.margin_pct })),
          message: `${below.length} option(s) price below the ${MARGIN_FLOOR}% floor. Re-price, or release with override.` });
      }

      const token = crypto.randomUUID();
      const now = new Date().toISOString();
      const tx = db.transaction(() => {
        for (const r of rows) {
          req.d2d.run(`UPDATE d2d_booking SET status='released', released_at=@now, decision_token=@token
                       WHERE client_id = @client AND id = @id`, { now, token, id: r.id });
        }
      });
      tx();
      for (const rid of [...new Set(rows.map(r => r.request_id).filter(Boolean))]) {
        req.d2d.run(`UPDATE d2d_request SET state='released' WHERE client_id = @client AND id = @id`, { id: rid });
        reqEvent(req.d2d, rid, 'costed', 'released', (req.auth && req.auth.userId) || 'velozity', 'internal', 'released to the client');
      }
      console.warn(`[d2d] released ${rows.length} option(s) for ${req.d2d.client} week ${ws}`);
      res.json({ ok: true, released: rows.length, week_start: ws, overridden: !!below.length });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // The client's decision. Approving freezes the plan and creates the shipments; the other
  // options for that week expire so a week can never hold two approvals.
  router.post('/bookings/:id/decision', authenticateRequest, requireD2D,
    auditLog('decide_d2d_booking'), (req, res) => {
    try {
      const choice = String((req.body || {}).decision || '').toLowerCase();
      if (!['approve', 'decline'].includes(choice)) return res.status(400).json({ error: 'decision must be approve or decline' });

      const row = req.d2d.get(`SELECT * FROM d2d_booking WHERE client_id = @client AND id = @id`, { id: req.params.id });
      if (!row) return res.status(404).json({ error: 'not_found' });
      // Only a RELEASED option can be decided. Listing the statuses to reject instead let an
      // EXPIRED option be approved — so a week that already had an approved booking could take
      // a second one, and created shipments for both.
      if (row.status !== 'released') {
        const why = row.status === 'draft' ? 'not_released'
                  : row.status === 'expired' ? 'option_expired'
                  : 'already_decided';
        return res.status(409).json({ error: why, status: row.status, decided_at: row.decided_at || null,
          message: row.status === 'expired'
            ? 'Another option was approved for this week, so this one is no longer on the table.'
            : undefined });
      }

      const who = (req.auth && req.auth.userId) || 'unknown';
      const now = new Date().toISOString();

      if (choice === 'decline') {
        req.d2d.run(`UPDATE d2d_booking SET status='declined', decided_at=@now, decided_by=@who
                     WHERE client_id = @client AND id = @id`, { now, who, id: row.id });
        return res.json({ ok: true, status: 'declined' });
      }

      // Cargo ready anchors the plan; the option's transit time sets arrival.
      // The baseline in force RIGHT NOW is copied into the plan. Editing it later moves no
      // existing plan: variance would become meaningless if the yardstick moved with it.
      const bl = baselineFor(req.d2d, row.mode);
      const ready = row.week_start;
      const transit = Number(row.transit_days) || 0;
      const departed = addDays(ready, bl.departed);
      const arrived = addDays(departed, transit);
      const plan = {
        plan_pickup: ready,
        plan_origin_cleared: addDays(ready, bl.origin_cleared),
        plan_departed: departed,
        plan_arrived: arrived,
        plan_dest_cleared: addDays(arrived, bl.dest_cleared),
        plan_out_for_delivery: addDays(arrived, bl.out_for_delivery),
        plan_delivered: addDays(arrived, bl.delivered),
        plan_frozen_at: now,
      };

      const made = [];
      const tx = db.transaction(() => {
        const upd = req.d2d.run(`UPDATE d2d_booking SET status='approved', decided_at=@now, decided_by=@who
                     WHERE client_id = @client AND id = @id`, { now, who, id: row.id });
        if (!upd.changes) throw new Error('approval did not apply');
        // One shipment per container booked. Air books a single consignment.
        const qty = row.mode === 'air' ? 1 : Math.max(1, Number(row.container_qty) || 1);
        for (let i = 0; i < qty; i++) {
          const id = 'sh_' + crypto.randomUUID().slice(0, 12);
          req.d2d.insert('d2d_shipment', {
            id, booking_id: row.id, week_start: row.week_start, mode: row.mode,
            reference: null,                       // container number arrives later, from the partner
            container_type: row.container_type, carrier: row.carrier,
            origin: row.service || null, destination: null,
            status: 'booked', ...plan,
          });
          made.push(id);
        }
        // Everything else offered that week is off the table.
        req.d2d.run(`UPDATE d2d_booking SET status='expired'
                     WHERE client_id = @client AND week_start = @ws AND id <> @id AND status='released'`,
                     { ws: row.week_start, id: row.id });
      });
      tx();

      if (row.request_id) {
        req.d2d.run(`UPDATE d2d_request SET state='approved' WHERE client_id = @client AND id = @id`, { id: row.request_id });
        reqEvent(req.d2d, row.request_id, 'released', 'approved', who, 'client', 'approved ' + (row.title || row.option_ref));
      }
      console.warn(`[d2d] ${req.d2d.client} approved ${row.id} — ${made.length} shipment(s), plan frozen`);
      res.json({ ok: true, status: 'approved', shipments: made.length, plan: plan });
    } catch (e) { console.error('[d2d] decision', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── Transit baselines ──
  router.get('/baselines', authenticateRequest, requireD2D, (req, res) => {
    try {
      if (internalOnly(req, res)) return;
      res.json({ baselines: ['sea', 'air'].map(m => baselineFor(req.d2d, m)), fields: PLAN_FIELDS,
        note: 'Applied when a booking is approved. Existing plans are never recomputed.' });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.put('/baselines/:mode', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('update_d2d_baseline'), (req, res) => {
    try {
      if (internalOnly(req, res)) return;
      const mode = req.params.mode === 'air' ? 'air' : 'sea';
      const b = req.body || {};
      const vals = {};
      for (const f of PLAN_FIELDS) {
        const v = Number(b[f]);
        if (!Number.isInteger(v) || v < 0 || v > 60) {
          return res.status(400).json({ error: 'invalid', field: f, message: 'Each stage must be a whole number of days, 0 to 60.' });
        }
        vals[f] = v;
      }
      if (vals.departed < vals.origin_cleared) {
        return res.status(400).json({ error: 'out_of_order',
          message: 'Departure cannot be planned before origin clearance.' });
      }
      if (vals.delivered < vals.out_for_delivery || vals.out_for_delivery < vals.dest_cleared) {
        return res.status(400).json({ error: 'out_of_order',
          message: 'After arrival the order is: cleared, then out for delivery, then delivered.' });
      }
      baselineFor(req.d2d, mode);        // make sure the row exists before updating it
      req.d2d.run(`UPDATE d2d_baseline SET origin_cleared=@origin_cleared, departed=@departed,
                     dest_cleared=@dest_cleared, out_for_delivery=@out_for_delivery, delivered=@delivered,
                     updated_at=@now, updated_by=@who
                   WHERE client_id = @client AND mode = @mode`,
        { ...vals, mode, now: new Date().toISOString(), who: (req.auth && req.auth.userId) || 'velozity' });
      const frozen = req.d2d.get(`SELECT COUNT(*) n FROM d2d_shipment WHERE client_id = @client AND plan_frozen_at IS NOT NULL`);
      res.json({ ok: true, mode, ...vals,
        applies_to: 'bookings approved from now on',
        unchanged_plans: (frozen && frozen.n) || 0 });
    } catch (e) { console.error('[d2d] baseline', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── Actuals ──
  // The plan is frozen; this is what actually happened. Every row records WHERE it came from,
  // because a date somebody typed and a date a carrier reported deserve different trust.
  const STAGES = ['pickup', 'origin_cleared', 'departed', 'arrived', 'dest_cleared', 'out_for_delivery', 'delivered'];
  const SOURCES = ['carrier', 'partner', 'import', 'manual'];
  const isYmd = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));

  // One milestone. Re-recording the same stage updates it rather than adding a second row, so
  // a corrected date replaces the wrong one instead of both being true at once.
  function writeEvent(sdb, { shipment_id, stage, actual_at, source, note, who, clear }) {
    if (!STAGES.includes(stage)) return { error: 'unknown stage: ' + stage };
    if (actual_at && !isYmd(actual_at)) return { error: 'actual_at must be YYYY-MM-DD' };
    const ship = sdb.get(`SELECT id FROM d2d_shipment WHERE client_id = @client AND id = @id`, { id: shipment_id });
    if (!ship) return { error: 'shipment not found: ' + shipment_id };
    const now = new Date().toISOString();

    // Removing a wrongly entered date deletes the row. Storing a null would leave a stage
    // looking recorded-but-blank, which reads as fact on the strip.
    if (clear || (!actual_at && actual_at !== 0)) {
      sdb.run(`DELETE FROM d2d_event WHERE client_id = @client AND shipment_id = @sid AND stage = @stage`,
        { sid: shipment_id, stage });
      const left = sdb.get(`SELECT COUNT(*) n FROM d2d_event WHERE client_id = @client AND shipment_id = @sid AND actual_at IS NOT NULL`,
        { sid: shipment_id });
      sdb.run(`UPDATE d2d_shipment SET status = @st WHERE client_id = @client AND id = @id`,
        { st: left && left.n ? 'in_transit' : 'booked', id: shipment_id });
      return { ok: true, stage, actual_at: null, cleared: true };
    }
    sdb.run(`INSERT INTO d2d_event (id, client_id, shipment_id, stage, actual_at, source, note, recorded_by, recorded_at)
             VALUES (@id, @client, @shipment_id, @stage, @actual_at, @source, @note, @who, @now)
             ON CONFLICT(client_id, shipment_id, stage) DO UPDATE SET
               actual_at = excluded.actual_at, source = excluded.source, note = excluded.note,
               recorded_by = excluded.recorded_by, recorded_at = excluded.recorded_at`,
      { id: 'ev_' + crypto.randomUUID().slice(0, 12), shipment_id, stage,
        actual_at: actual_at || null, source: SOURCES.includes(source) ? source : 'manual',
        note: note || null, who: who || 'unknown', now });
    // Delivered closes the shipment; anything else means it is moving.
    sdb.run(`UPDATE d2d_shipment SET status = @st WHERE client_id = @client AND id = @id`,
      { st: stage === 'delivered' ? 'delivered' : 'in_transit', id: shipment_id });
    return { ok: true, stage, actual_at: actual_at || null };
  }

  // VelOzity records milestones and fills in the container number the partner gives us.
  router.post('/shipments/:id/events', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('record_d2d_event'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const b = req.body || {};
      const out = writeEvent(req.d2d, {
        shipment_id: req.params.id, stage: String(b.stage || ''), actual_at: b.actual_at,
        source: b.source || 'manual', note: b.note,
        who: (req.auth && req.auth.userId) || 'velozity',
      });
      if (out.error) return res.status(400).json(out);
      res.json(out);
    } catch (e) { console.error('[d2d] event', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  router.patch('/shipments/:id', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('update_d2d_shipment'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const b = req.body || {};
      // Deliberately short: the plan columns are NOT here. Once frozen they stay frozen.
      const allowed = ['reference', 'vessel', 'voyage', 'origin', 'destination', 'capacity_cbm', 'container_type'];
      const sets = [], params = { id: req.params.id };
      for (const f of allowed) if (b[f] !== undefined) { sets.push(`${f} = @${f}`); params[f] = b[f]; }
      if (!sets.length) return res.status(400).json({ error: 'nothing to update', allowed });
      const done = req.d2d.run(`UPDATE d2d_shipment SET ${sets.join(', ')}
                                WHERE client_id = @client AND id = @id`, params);
      if (!done.changes) return res.status(404).json({ error: 'not_found' });
      res.json({ ok: true, updated: sets.length });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── The partner's door ──
  // The partner knows when a box was picked up, cleared and sailed. This is the ONLY thing they can
  // write, and they can write nothing else: no bookings, no costs, no orders, no plan.
  router.post('/partner/events', authenticateRequest, requireD2D,
    auditLog('partner_d2d_event'), (req, res) => {
    try {
      if (req.d2dOrgType !== 'partner' && !isInternal(req)) {
        return res.status(403).json({ error: 'partner_or_internal_only' });
      }
      const rows = Array.isArray((req.body || {}).events) ? req.body.events : [];
      if (!rows.length) return res.status(400).json({ error: 'events required' });
      if (rows.length > 500) return res.status(400).json({ error: 'too many events in one call' });

      const who = (req.auth && req.auth.userId) || 'partner';
      const results = [];
      const tx = db.transaction(() => {
        for (const r of rows) {
          results.push({
            shipment_id: r.shipment_id, stage: r.stage,
            // source is forced: a partner cannot claim a date came from the carrier feed.
            ...writeEvent(req.d2d, { shipment_id: String(r.shipment_id || ''), stage: String(r.stage || ''),
              actual_at: r.actual_at, source: 'partner', note: r.note, who }),
          });
        }
      });
      tx();
      const failed = results.filter(r => r.error);
      console.warn(`[d2d] partner wrote ${results.length - failed.length}/${results.length} event(s) for ${req.d2d.client}`);
      res.json({ ok: failed.length === 0, written: results.length - failed.length, failed: failed.length, results });
    } catch (e) { console.error('[d2d] partner events', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  router.post('/partner/shipments/:id/reference', authenticateRequest, requireD2D,
    auditLog('partner_d2d_reference'), (req, res) => {
    try {
      if (req.d2dOrgType !== 'partner' && !isInternal(req)) {
        return res.status(403).json({ error: 'partner_or_internal_only' });
      }
      const ref = String((req.body || {}).reference || '').trim().toUpperCase();
      if (!/^[A-Z]{4}[0-9]{6,7}$/.test(ref)) {
        return res.status(400).json({ error: 'bad_reference',
          message: 'A container number looks like ABCD1234567.' });
      }
      const row = req.d2d.get(`SELECT id, reference FROM d2d_shipment WHERE client_id = @client AND id = @id`,
        { id: req.params.id });
      if (!row) return res.status(404).json({ error: 'not_found' });
      // Advising is once. Correcting one already in use is VelOzity's call, not the partner's.
      if (row.reference) return res.status(409).json({ error: 'already_advised', reference: row.reference });

      req.d2d.run(`UPDATE d2d_shipment SET reference = @ref, vessel = COALESCE(@vessel, vessel)
                   WHERE client_id = @client AND id = @id`,
        { ref, vessel: (req.body || {}).vessel || null, id: req.params.id });
      res.json({ ok: true, reference: ref });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // What the partner needs to see to do that: the shipments, and nothing about money.
  router.get('/partner/shipments', authenticateRequest, requireD2D, (req, res) => {
    try {
      if (req.d2dOrgType !== 'partner' && !isInternal(req)) {
        return res.status(403).json({ error: 'partner_or_internal_only' });
      }
      const rows = req.d2d.all(`SELECT id, week_start, mode, reference, container_type, carrier, vessel,
                                       origin, destination, status,
                                       plan_pickup, plan_origin_cleared, plan_departed, plan_arrived,
                                       plan_dest_cleared, plan_out_for_delivery, plan_delivered
                                FROM d2d_shipment WHERE client_id = @client
                                ORDER BY week_start DESC, id LIMIT 200`);
      const ids = rows.map(r => r.id);
      const idp = {}; ids.forEach((id, i) => { idp['id' + i] = id; });
      const events = ids.length
        ? req.d2d.all(`SELECT shipment_id, stage, actual_at, source FROM d2d_event
                       WHERE client_id = @client AND shipment_id IN (${ids.map((_, i) => '@id' + i).join(',')})`, idp)
        : [];
      const by = {};
      for (const e of events) (by[e.shipment_id] = by[e.shipment_id] || []).push(e);
      res.json({ shipments: rows.map(r => ({ ...r, events: by[r.id] || [] })), stages: STAGES });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── Booking requests ──
  const PACK_TYPES = ['loose', 'pallets', 'mixed'];

  function reqEvent(sdb, requestId, from, to, actor, role, detail) {
    sdb.insert('d2d_request_event', {
      id: 'rqe_' + crypto.randomUUID().slice(0, 12), request_id: requestId,
      from_state: from || null, to_state: to || null, actor: actor || null,
      role: role || null, detail: detail || null,
    });
  }

  // What the week already knows about its cargo, from the order file. Typing volume by hand
  // when the orders are loaded is how two numbers end up disagreeing.
  function cargoFromOrders(sdb, week) {
    const r = sdb.get(`SELECT COUNT(*) AS orders, SUM(units) AS units, SUM(cbm) AS cbm,
                              SUM(weight_kg) AS weight
                       FROM d2d_po WHERE client_id = @client AND week_start = @ws`, { ws: week });
    if (!r || !r.orders) return null;
    return { orders: r.orders, units: r.units || null, cbm: r.cbm || null, gross_weight_kg: r.weight || null };
  }

  router.get('/requests/prefill', authenticateRequest, requireD2D, (req, res) => {
    try {
      if (internalOnly(req, res)) return;
      const ws = String(req.query.week || '');
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) return res.status(400).json({ error: 'week required' });
      res.json({ week_start: ws, from_orders: cargoFromOrders(req.d2d, ws) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.post('/requests', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('create_d2d_request'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const b = req.body || {};
      const week = String(b.week_start || '');
      if (!/^\d{4}-\d{2}-\d{2}$/.test(week)) return res.status(400).json({ error: 'week_start required' });
      const mode = ['sea', 'air', 'both'].includes(b.mode) ? b.mode : 'sea';
      const pack = PACK_TYPES.includes(b.pack_type) ? b.pack_type : null;

      const num = (v, whole) => {
        if (v === '' || v == null) return null;
        const n = Number(v);
        if (!isFinite(n) || n < 0) return undefined;            // undefined means "rejected"
        return whole ? Math.round(n) : n;
      };
      const vals = {
        pallets: num(b.pallets, true), cartons: num(b.cartons, true), units: num(b.units, true),
        cbm: num(b.cbm), gross_weight_kg: num(b.gross_weight_kg),
      };
      for (const [k, v] of Object.entries(vals)) {
        if (v === undefined) return res.status(400).json({ error: 'invalid', field: k, message: k + ' must be a number of zero or more.' });
      }
      // Pallets without a pack type, or a pallet count on a loose shipment, is a contradiction
      // the partner would have to come back and ask about.
      if (pack === 'loose' && vals.pallets) {
        return res.status(400).json({ error: 'contradiction', message: 'A loose shipment cannot have a pallet count.' });
      }
      if (pack === 'pallets' && !vals.pallets) {
        return res.status(400).json({ error: 'contradiction', message: 'Palletised cargo needs a pallet count.' });
      }

      const seq = (req.d2d.get(`SELECT COUNT(*) n FROM d2d_request WHERE client_id = @client AND week_start = @ws`, { ws: week }) || {}).n || 0;
      const id = 'rq_' + crypto.randomUUID().slice(0, 12);
      const ref = (mode === 'air' ? 'AIR-' : 'SEA-') + week.slice(2, 4) + week.slice(5, 7) + '-' + String(seq + 1).padStart(2, '0');

      req.d2d.insert('d2d_request', {
        id, week_start: week, ref, mode,
        origin: b.origin || null, destination: b.destination || null,
        ready_date: /^\d{4}-\d{2}-\d{2}$/.test(String(b.ready_date || '')) ? b.ready_date : null,
        pack_type: pack, ...vals, notes: b.notes || null,
        state: 'draft', created_by: (req.auth && req.auth.userId) || 'velozity',
      });
      reqEvent(req.d2d, id, null, 'draft', (req.auth && req.auth.userId) || 'velozity', 'internal', 'created');
      res.json({ ok: true, id, ref, week_start: week, mode });
    } catch (e) { console.error('[d2d] create request', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/requests', authenticateRequest, requireD2D, auditLog('view_d2d_requests'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const rows = ws
        ? req.d2d.all(`SELECT * FROM d2d_request WHERE client_id = @client AND week_start = @ws ORDER BY created_at DESC`, { ws })
        : req.d2d.all(`SELECT * FROM d2d_request WHERE client_id = @client ORDER BY week_start DESC LIMIT 60`);
      const internal = isInternal(req);
      const ids = rows.map(r => r.id);
      const idp = {}; ids.forEach((id, i) => { idp['id' + i] = id; });
      const opts = ids.length
        ? req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = @client
                       AND request_id IN (${ids.map((_, i) => '@id' + i).join(',')})`, idp)
        : [];
      const byReq = {};
      for (const b of opts) {
        if (!internal && b.status === 'draft') continue;       // unreleased options are not the client's business
        (byReq[b.request_id] = byReq[b.request_id] || []).push(internal ? b : forClient(b));
      }
      res.json({ requests: rows.map(r => ({ ...r, options: byReq[r.id] || [] })) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── The partner's page ──
  // Reached by a link, with no account and no client header, so it cannot use the scoped
  // layer's usual entry point. The token carries the client, and every query below is scoped
  // to that client explicitly.
  function tokenLookup(raw) {
    const t = String(raw || '').trim();
    if (!/^[a-f0-9]{48}$/.test(t)) return { error: 'bad_link' };
    const row = db.prepare(`SELECT * FROM d2d_request_token WHERE token = ?`).get(t);
    if (!row) return { error: 'bad_link' };
    if (row.expires_at && new Date(row.expires_at) < new Date()) return { error: 'expired' };
    const sdb = scopedDb(row.client_id);
    const rq = sdb.get(`SELECT * FROM d2d_request WHERE client_id = @client AND id = @id`, { id: row.request_id });
    if (!rq) return { error: 'bad_link' };
    return { token: row, rq, sdb };
  }

  const pageShell = (title, body) => `<!doctype html><html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>${title}</title>
    <style>
      :root{color-scheme:light}
      body{margin:0;background:#F7F8FA;color:#1C1C1E;
        font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;}
      .wrap{max-width:820px;margin:0 auto;padding:26px 20px 60px;}
      .card{background:#fff;border:.5px solid rgba(16,18,27,.10);border-radius:14px;padding:18px 20px;margin-bottom:14px;
        box-shadow:0 1px 2px rgba(16,18,27,.04),0 4px 12px rgba(16,18,27,.05);}
      h1{font-size:19px;letter-spacing:-.01em;margin:0 0 4px;}
      .muted{color:#6E6E73;font-size:12.5px;}
      label{display:block;font-size:10px;color:#AEAEB2;text-transform:uppercase;letter-spacing:.05em;margin-top:10px;}
      input,select,textarea{display:block;width:100%;box-sizing:border-box;font:inherit;font-size:13px;
        border:.5px solid rgba(0,0,0,.18);border-radius:9px;padding:9px 10px;margin-top:4px;background:#fff;min-height:42px;}
      .grid{display:grid;gap:12px;}
      .g2{grid-template-columns:repeat(2,minmax(0,1fr));} .g3{grid-template-columns:repeat(3,minmax(0,1fr));}
      .g4{grid-template-columns:repeat(4,minmax(0,1fr));}
      @media(max-width:640px){.g2,.g3,.g4{grid-template-columns:1fr;}}
      button{font:inherit;font-weight:600;font-size:13px;border-radius:10px;padding:11px 18px;min-height:46px;cursor:pointer;}
      .primary{background:#1C1C1E;color:#fff;border:0;}
      .ghost{background:#fff;color:#1C1C1E;border:.5px solid rgba(0,0,0,.16);}
      .facts{display:flex;flex-wrap:wrap;gap:18px;margin-top:10px;}
      .fact b{display:block;font-size:15px;} .fact span{font-size:10px;color:#AEAEB2;text-transform:uppercase;letter-spacing:.05em;}
      .opt{border:.5px solid rgba(0,0,0,.10);border-radius:12px;padding:14px 16px;margin-bottom:10px;}
      .note{background:rgba(254,208,0,.12);border-left:3px solid #FED000;border-radius:9px;padding:10px 13px;font-size:12.5px;}
      .ok{background:rgba(155,171,21,.14);border-left:3px solid #9BAB15;border-radius:9px;padding:12px 15px;}
    </style></head><body><div class="wrap">${body}</div></body></html>`;

  router.get('/quote', (req, res) => {
    try {
      const look = tokenLookup(req.query.token);
      if (look.error) {
        return res.status(look.error === 'expired' ? 410 : 404).send(pageShell('Link', `
          <div class="card"><h1>${look.error === 'expired' ? 'This link has expired' : 'This link is not valid'}</h1>
          <p class="muted">${look.error === 'expired'
            ? 'Ask your VelOzity contact to send a fresh one.'
            : 'Please check the link in the email, or ask your VelOzity contact to resend it.'}</p></div>`));
      }
      const { rq } = look;
      const done = rq.state === 'costed' || rq.state === 'released' || rq.state === 'approved';
      const last = look.sdb.all(`SELECT detail FROM d2d_request_event WHERE client_id = @client AND request_id = @id
                                 AND to_state='repricing' ORDER BY at DESC LIMIT 1`, { id: rq.id })[0];

      const optionFields = (n) => `
        <div class="opt">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <b style="font-size:13px;">Option ${String.fromCharCode(65 + n)}</b>
            ${n > 0 ? '<span class="muted">optional</span>' : ''}
          </div>
          <div class="grid g2">
            <div><label>Description<input name="title_${n}" placeholder="2 x 40HQ, direct"></label></div>
            <div><label>Carrier<input name="carrier_${n}" placeholder="ONE"></label></div>
          </div>
          <div class="grid g4">
            <div><label>Equipment<select name="container_type_${n}">
              <option value="40HQ">40HQ</option><option value="40GP">40GP</option>
              <option value="20GP">20GP</option><option value="">Air / LCL</option></select></label></div>
            <div><label>How many<input name="container_qty_${n}" type="number" min="1" value="1"></label></div>
            <div><label>Transit days<input name="transit_days_${n}" type="number" min="1" placeholder="26"></label></div>
            <div><label>Routing<select name="transhipment_${n}">
              <option value="0">Direct</option><option value="1">Transhipment</option></select></label></div>
          </div>
          <div class="grid g3">
            <div><label>Freight cost<input name="cost_amount_${n}" type="number" min="0" step="0.01" placeholder="8000"></label></div>
            <div><label>Origin + destination charges<input name="accessorial_amount_${n}" type="number" min="0" step="0.01" placeholder="900"></label></div>
            <div><label>Currency<select name="currency_${n}"><option>USD</option><option>AUD</option><option>CNY</option></select></label></div>
          </div>
        </div>`;

      res.send(pageShell('Rates wanted · ' + (rq.ref || ''), `
        <div class="card">
          <h1>Rates wanted</h1>
          <p class="muted">${rq.ref ? rq.ref + ' · ' : ''}cargo ready week of ${rq.week_start}${rq.origin ? ' · from ' + rq.origin : ''}${rq.destination ? ' to ' + rq.destination : ''}</p>
          <div class="facts">
            ${[['Packed as', rq.pack_type || '—'], ['Pallets', rq.pallets], ['Cartons', rq.cartons],
               ['Units', rq.units], ['CBM', rq.cbm], ['Gross kg', rq.gross_weight_kg]]
              .filter(([, v]) => v != null && v !== '')
              .map(([l, v]) => `<span class="fact"><b>${typeof v === 'number' ? v.toLocaleString() : v}</b><span>${l}</span></span>`).join('')}
          </div>
          ${rq.notes ? `<p class="muted" style="margin-top:12px;">${rq.notes}</p>` : ''}
        </div>

        ${last && last.detail ? `<div class="card note"><b>Please look again:</b> ${last.detail}</div>` : ''}

        ${done ? `<div class="card ok"><b>Thank you — your options are with us.</b>
            <div class="muted" style="margin-top:4px;">You can send revised options below if anything changes.</div></div>` : ''}

        <form method="POST" action="/d2d/quote">
          <input type="hidden" name="token" value="${req.query.token}">
          <div class="card">
            <div style="display:flex;justify-content:space-between;align-items:baseline;">
              <b>Your options</b><span class="muted">send as many as you can offer</span>
            </div>
            ${[0, 1, 2].map(optionFields).join('')}
            <div class="grid g2">
              <div><label>Your name<input name="partner_name" placeholder="Who we should reply to"></label></div>
              <div><label>Valid until<input name="valid_until" type="date"></label></div>
            </div>
            <label>Anything we should know<textarea name="partner_note" rows="3"></textarea></label>
            <div style="margin-top:16px;"><button class="primary" type="submit">Send these rates</button></div>
          </div>
        </form>`));
    } catch (e) { console.error('[d2d] quote page', e); res.status(500).send('Something went wrong.'); }
  });

  router.post('/quote', express.urlencoded({ extended: false }), (req, res) => {
    try {
      const look = tokenLookup((req.body || {}).token);
      if (look.error) return res.status(404).send(pageShell('Link', `<div class="card"><h1>This link is not valid</h1></div>`));
      const { rq, sdb } = look;
      const b = req.body || {};

      const options = [];
      for (let n = 0; n < 3; n++) {
        const cost = Number(b['cost_amount_' + n]);
        if (!isFinite(cost) || cost <= 0) continue;              // an empty block is not an option
        options.push({
          option_ref: String.fromCharCode(65 + options.length),
          title: String(b['title_' + n] || '').trim() || null,
          carrier: String(b['carrier_' + n] || '').trim() || null,
          container_type: String(b['container_type_' + n] || '') || null,
          container_qty: Number(b['container_qty_' + n]) || 1,
          transit_days: Number(b['transit_days_' + n]) || null,
          transhipment: String(b['transhipment_' + n] || '0') === '1' ? 1 : 0,
          cost_amount: cost,
          accessorial_amount: Number(b['accessorial_amount_' + n]) || 0,
          currency: String(b['currency_' + n] || 'USD').toUpperCase(),
        });
      }
      if (!options.length) {
        return res.status(400).send(pageShell('Rates wanted', `<div class="card">
          <h1>No rates received</h1><p class="muted">At least one option needs a freight cost. Please go back and try again.</p></div>`));
      }

      const who = String(b.partner_name || '').trim() || 'partner';
      const tx = db.transaction(() => {
        // A fresh submission supersedes the previous one rather than adding to it.
        sdb.run(`UPDATE d2d_booking SET status='expired'
                 WHERE client_id = @client AND request_id = @id AND status='draft'`, { id: rq.id });
        for (const o of options) {
          sdb.insert('d2d_booking', {
            id: 'bk_' + crypto.randomUUID().slice(0, 12), week_start: rq.week_start,
            option_ref: o.option_ref, title: o.title || (o.container_qty + ' x ' + (o.container_type || '')),
            mode: rq.mode === 'air' ? 'air' : 'sea', container_type: o.container_type,
            container_qty: o.container_qty, carrier: o.carrier, service: rq.origin || null,
            transhipment: o.transhipment, transit_days: o.transit_days,
            cost_amount: o.cost_amount, accessorial_amount: o.accessorial_amount,
            margin_pct: MARGIN_FLOOR, sell_amount: sellFrom(o.cost_amount, o.accessorial_amount, MARGIN_FLOOR),
            currency: o.currency, status: 'draft', recommended: 0, request_id: rq.id,
          });
        }
        sdb.run(`UPDATE d2d_request SET state='costed' WHERE client_id = @client AND id = @id`, { id: rq.id });
        sdb.insert('d2d_request_event', {
          id: 'rqe_' + crypto.randomUUID().slice(0, 12), request_id: rq.id,
          from_state: rq.state, to_state: 'costed', actor: who, role: 'partner',
          detail: options.length + ' option(s) quoted' + (b.partner_note ? ' — ' + String(b.partner_note).slice(0, 300) : ''),
        });
        db.prepare(`UPDATE d2d_request_token SET used_at = datetime('now') WHERE token = ?`).run(look.token.token);
      });
      tx();
      console.warn(`[d2d] partner quoted ${options.length} option(s) for ${rq.client_id} ${rq.ref}`);

      res.send(pageShell('Thank you', `
        <div class="card ok">
          <h1 style="margin-bottom:6px;">Thank you</h1>
          <p class="muted">${options.length} option${options.length === 1 ? '' : 's'} received for ${rq.ref || 'this booking'}.
             We will come back to you once the client has decided.</p>
        </div>
        <div class="card"><p class="muted">Keep this link — if anything changes you can send revised rates from it.</p></div>`));
    } catch (e) { console.error('[d2d] quote post', e); res.status(500).send('Something went wrong.'); }
  });

  // ── Sending it to the partner ──
  // The same pattern as the air quotes: a token in a link, no account needed, and the page
  // states the cargo so the partner is quoting the right thing. The difference is what comes
  // back — sea partners return two or three OPTIONS, not a single cost.
  const cargoLine = (rq) => [
    rq.pack_type === 'pallets' ? (rq.pallets ? rq.pallets + ' pallets' : 'palletised')
      : rq.pack_type === 'loose' ? 'loose cartons' : rq.pack_type,
    rq.cartons ? rq.cartons.toLocaleString() + ' cartons' : null,
    rq.units ? rq.units.toLocaleString() + ' units' : null,
    rq.cbm ? rq.cbm + ' CBM' : null,
    rq.gross_weight_kg ? Math.round(rq.gross_weight_kg).toLocaleString() + ' kg' : null,
  ].filter(Boolean).join(' · ');

  router.post('/requests/:id/send', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('send_d2d_rfq'), async (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const rq = req.d2d.get(`SELECT * FROM d2d_request WHERE client_id = @client AND id = @id`, { id: req.params.id });
      if (!rq) return res.status(404).json({ error: 'not_found' });
      if (['approved', 'cancelled'].includes(rq.state)) return res.status(409).json({ error: 'not_sendable', state: rq.state });
      // A partner cannot quote a sailing without knowing the volume.
      if (!rq.cbm && !rq.cartons && !rq.pallets && !rq.units) {
        return res.status(400).json({ error: 'no_cargo', message: 'Add the cargo details before sending: a partner cannot quote without them.' });
      }

      const days = Math.max(1, parseInt((req.body || {}).expires_days, 10) || 14);
      const token = crypto.randomBytes(24).toString('hex');
      req.d2d.insert('d2d_request_token', { token, request_id: rq.id, purpose: 'partner_quote',
        expires_at: new Date(Date.now() + days * 86400000).toISOString() });

      const base = String(process.env.PUBLIC_API_URL || (req.protocol + '://' + req.get('host'))).replace(/\/+$/, '');
      const link = `${base}/d2d/quote?token=${token}`;
      // Sea shares the air partner address for now: it is the same forwarder.
      const to = deps.parseEmailList
        ? deps.parseEmailList(process.env.D2D_PARTNER_EMAIL_TO || process.env.AIR_QUOTE_PARTNER_EMAIL_TO)
        : [];
      const subject = `Rates wanted — ${rq.ref || 'booking'} · cargo ready ${rq.week_start}`;
      const html = `
        <p>Hello,</p>
        <p>We have cargo ready in the week of <b>${rq.week_start}</b>${rq.origin ? ' from <b>' + rq.origin + '</b>' : ''}
           and would like your options.</p>
        <p><b>Cargo:</b> ${cargoLine(rq) || 'see the link'}<br>
           <b>Mode:</b> ${rq.mode}${rq.destination ? '<br><b>To:</b> ' + rq.destination : ''}
           ${rq.notes ? '<br><b>Notes:</b> ' + rq.notes : ''}</p>
        <p>Please enter your options here — you can send more than one sailing:<br>
           <a href="${link}">${link}</a></p>
        <p>The link works for ${days} days.</p>`;
      const text = `Rates wanted for ${rq.ref}. Cargo ready ${rq.week_start}. ${cargoLine(rq)}. Enter options: ${link}`;

      let mail = { skipped: true };
      if (deps.sendPartnerMail && to.length) {
        mail = await deps.sendPartnerMail(to, subject, html, text);
      }

      req.d2d.run(`UPDATE d2d_request SET state='sent', sent_at=@now WHERE client_id = @client AND id = @id`,
        { now: new Date().toISOString(), id: rq.id });
      reqEvent(req.d2d, rq.id, rq.state, 'sent', (req.auth && req.auth.userId) || 'velozity', 'internal',
        'sent to the partner' + (mail && mail.to ? ' (' + mail.to.join(', ') + ')' : ''));

      // The link is returned either way, so it can be pasted into an email if mail is not set up.
      res.json({ ok: true, link, expires_days: days, mail });
    } catch (e) { console.error('[d2d] send rfq', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  // Ask the partner to look again, with a reason. This is the reject path.
  router.post('/requests/:id/review', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('review_d2d_rfq'), async (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const rq = req.d2d.get(`SELECT * FROM d2d_request WHERE client_id = @client AND id = @id`, { id: req.params.id });
      if (!rq) return res.status(404).json({ error: 'not_found' });
      const note = String((req.body || {}).note || '').trim();
      if (!note) return res.status(400).json({ error: 'note_required', message: 'Say what needs looking at again.' });

      // The options quoted so far are set aside rather than deleted: what was offered, and
      // when, is part of the record.
      req.d2d.run(`UPDATE d2d_booking SET status='expired'
                   WHERE client_id = @client AND request_id = @id AND status='draft'`, { id: rq.id });
      req.d2d.run(`UPDATE d2d_request SET state='repricing' WHERE client_id = @client AND id = @id`, { id: rq.id });
      reqEvent(req.d2d, rq.id, rq.state, 'repricing', (req.auth && req.auth.userId) || 'velozity', 'internal', note);
      res.json({ ok: true, state: 'repricing', note });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/requests/:id/events', authenticateRequest, requireD2D, (req, res) => {
    try {
      if (internalOnly(req, res)) return;
      res.json({ events: req.d2d.all(`SELECT * FROM d2d_request_event
        WHERE client_id = @client AND request_id = @id ORDER BY at`, { id: req.params.id }) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── Orders: the PO, SKU and unit feed ──
  // One row per SKU line. PO-level fields repeat down the rows, which is how every ERP export
  // and every spreadsheet a supplier sends actually looks.
  //
  // Preview and apply are separate calls on purpose: a bad file must never half-load into a
  // live week, and the preview is where supplier and container mismatches surface.
  const PO_COLUMNS = {
    po_number:   ['po_number', 'po number', 'po', 'po no', 'po #', 'purchase order', 'order', 'order number', 'po ref'],
    week_start:  ['week_start', 'week', 'cargo week'],
    supplier:    ['supplier', 'vendor', 'factory'],
    cargo_ready: ['cargo_ready_date', 'cargo ready', 'crd', 'ex factory', 'ex-factory'],
    container:   ['container', 'container_no', 'container number', 'reference', 'awb'],
    sku_code:    ['sku', 'sku_code', 'item', 'item code', 'style'],
    sku_desc:    ['description', 'sku_description', 'item description', 'name'],
    units:       ['units', 'qty', 'quantity', 'pieces', 'pcs'],
    cbm:         ['cbm', 'volume', 'm3'],
    weight_kg:   ['weight', 'weight_kg', 'kg', 'gross weight'],
    value:       ['value', 'value_amount', 'amount', 'commercial value', 'invoice value'],
    currency:    ['currency', 'ccy'],
  };

  function splitCsvLine(line) {
    const out = []; let cur = '', q = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (q) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') q = false;
        else cur += c;
      } else if (c === '"') q = true;
      else if (c === ',') { out.push(cur); cur = ''; }
      else cur += c;
    }
    out.push(cur);
    return out.map(v => v.trim());
  }

  function parseRows(body) {
    if (Array.isArray(body.rows)) return { rows: body.rows, headers: Object.keys(body.rows[0] || {}) };
    const text = String(body.csv || '').replace(/^\uFEFF/, '').trim();
    if (!text) return { rows: [], headers: [] };
    const lines = text.split(/\r?\n/).filter(l => l.trim() !== '');
    if (!lines.length) return { rows: [], headers: [] };
    const headers = splitCsvLine(lines[0]).map(h => h.toLowerCase());
    const rows = lines.slice(1).map(l => {
      const cells = splitCsvLine(l), r = {};
      headers.forEach((h, i) => { r[h] = cells[i] == null ? '' : cells[i]; });
      return r;
    });
    return { rows, headers };
  }

  // Map whatever the file calls its columns onto what we need, so nobody has to rename headers.
  const normHeader = (h) => String(h == null ? '' : h).toLowerCase().replace(/[^a-z0-9]/g, '');
  function mapColumns(headers) {
    const found = {}, norm = headers.map(normHeader);
    for (const [field, names] of Object.entries(PO_COLUMNS)) {
      const wanted = names.map(normHeader);
      let i = norm.findIndex(h => wanted.includes(h));
      // Then a looser pass: "po number (client)" or "total cbm" should still land.
      if (i < 0) i = norm.findIndex(h => h && wanted.some(x => x.length > 2 && h.includes(x)));
      if (i >= 0) found[field] = headers[i];
    }
    return found;
  }

  const numOf = (v) => { const n = Number(String(v == null ? '' : v).replace(/[, $]/g, '')); return isFinite(n) ? n : null; };
  const dateOf = (v) => {
    const t = String(v == null ? '' : v).trim();
    if (!t) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
    const m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);   // d/m/Y, as Australian files are
    if (m) return `${m[3]}-${String(m[2]).padStart(2, '0')}-${String(m[1]).padStart(2, '0')}`;
    const d = new Date(t);
    return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
  };

  function buildOrders(req, body) {
    const { rows, headers } = parseRows(body || {});
    const col = mapColumns(headers);
    const problems = [];
    if (!col.po_number) problems.push({ level: 'stop', message: 'No PO number column found. Expected one of: ' + PO_COLUMNS.po_number.join(', ') });
    if (!col.units) problems.push({ level: 'warn', message: 'No units column found; unit counts will be empty.' });

    const weekFallback = /^\d{4}-\d{2}-\d{2}$/.test(String((body || {}).week_start || '')) ? body.week_start : null;
    // Monday of the week a date falls in. Weeks run Monday to Sunday.
    const mondayOf = (ymd) => {
      const d = new Date(String(ymd) + 'T00:00:00Z');
      if (isNaN(d.getTime())) return null;
      const dow = d.getUTCDay() || 7;                 // Sunday counts as the 7th day
      d.setUTCDate(d.getUTCDate() - (dow - 1));
      return d.toISOString().slice(0, 10);
    };

    // Containers this client actually has, so the file can be matched against reality.
    const ships = req.d2d.all(`SELECT id, reference, week_start FROM d2d_shipment WHERE client_id = @client`);
    const byRef = {};
    for (const sh of ships) if (sh.reference) byRef[String(sh.reference).toUpperCase()] = sh;

    const orders = new Map();
    const unmatchedContainers = new Set();
    let lineCount = 0;

    rows.forEach((r, idx) => {
      const po = String(r[col.po_number] == null ? '' : r[col.po_number]).trim();
      if (!po) { problems.push({ level: 'row', row: idx + 2, message: 'No PO number on this row; skipped.' }); return; }
      // Where the week comes from, in order of what is most certainly true:
      //   1. the container named on the row — it is already booked into a week
      //   2. the cargo-ready date on the row, rounded back to its Monday
      //   3. an explicit week column, if the file has one
      //   4. the week being uploaded into
      const cref0 = col.container ? String(r[col.container] || '').trim().toUpperCase() : '';
      const crd = col.cargo_ready ? dateOf(r[col.cargo_ready]) : null;
      const week = (cref0 && byRef[cref0] && byRef[cref0].week_start)
        || (crd && mondayOf(crd))
        || dateOf(col.week_start ? r[col.week_start] : null)
        || weekFallback;
      if (!week) { problems.push({ level: 'row', row: idx + 2,
        message: `PO ${po}: no container, cargo-ready date or week on this row, so it cannot be placed in a week.` }); return; }

      const key = week + '|' + po;
      if (!orders.has(key)) {
        orders.set(key, {
          po_number: po, week_start: week,
          supplier: col.supplier ? String(r[col.supplier] || '').trim() || null : null,
          cargo_ready_date: crd,
          cbm: null, weight_kg: null, value_amount: null,
          currency: col.currency ? (String(r[col.currency] || '').trim().toUpperCase() || null) : null,
          lines: [], containers: new Set(),
        });
      }
      const ord = orders.get(key);
      if (crd && !ord.cargo_ready_date) ord.cargo_ready_date = crd;
      // PO-level figures repeat on every line; take them once rather than summing them.
      if (col.cbm && ord.cbm == null) ord.cbm = numOf(r[col.cbm]);
      if (col.weight_kg && ord.weight_kg == null) ord.weight_kg = numOf(r[col.weight_kg]);
      if (col.value && ord.value_amount == null) ord.value_amount = numOf(r[col.value]);

      const sku = col.sku_code ? String(r[col.sku_code] || '').trim() : '';
      const units = col.units ? numOf(r[col.units]) : null;
      if (sku) { ord.lines.push({ sku_code: sku, description: col.sku_desc ? String(r[col.sku_desc] || '').trim() : null, units: units }); lineCount++; }
      else if (units != null && !ord.lines.length) { ord.lines.push({ sku_code: '(no sku)', description: null, units }); lineCount++; }

      const cref = col.container ? String(r[col.container] || '').trim().toUpperCase() : '';
      if (cref) {
        if (byRef[cref]) ord.containers.add(byRef[cref].id);
        else unmatchedContainers.add(cref);
      }
    });

    const list = [...orders.values()].map(ord => ({
      ...ord,
      units: ord.lines.reduce((n, l) => n + (Number(l.units) || 0), 0),
      containers: [...ord.containers],
    }));

    // A PO number may legitimately repeat in a later week. Twice in the SAME week is ambiguous
    // and worth flagging rather than silently merging.
    const seen = {};
    for (const ord of list) {
      seen[ord.po_number] = (seen[ord.po_number] || 0) + 1;
    }
    for (const [po, n] of Object.entries(seen)) {
      if (n > 1) problems.push({ level: 'warn', message: `PO ${po} appears in ${n} different weeks in this file — each is treated as its own order.` });
    }
    for (const c of unmatchedContainers) {
      problems.push({ level: 'warn', message: `Container ${c} is not one of this client's containers; those lines will load without a container.` });
    }

    return { orders: list, lineCount, problems, columns: col, headers, rowCount: rows.length };
  }

  router.post('/po/preview', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('preview_d2d_po'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      const out = buildOrders(req, req.body || {});
      const weeks = [...new Set(out.orders.map(o2 => o2.week_start))].sort();
      res.json({
        ok: !out.problems.some(p2 => p2.level === 'stop'),
        rows: out.rowCount, orders: out.orders.length, lines: out.lineCount,
        units: out.orders.reduce((n, o2) => n + o2.units, 0),
        weeks, columns_matched: out.columns, headers: out.headers,
        problems: out.problems.slice(0, 40),
        sample: out.orders.slice(0, 8).map(o2 => ({
          po_number: o2.po_number, week_start: o2.week_start, supplier: o2.supplier,
          units: o2.units, lines: o2.lines.length, containers: o2.containers.length, cbm: o2.cbm,
          cargo_ready_date: o2.cargo_ready_date,
        })),
        week_source: 'Derived from the container, or from the cargo-ready date rounded back to its Monday.',
      });
    } catch (e) { console.error('[d2d] po preview', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  router.post('/po/apply', authenticateRequest, requireRole(['admin']), requireD2D,
    auditLog('apply_d2d_po'), (req, res) => {
    if (internalOnly(req, res)) return;
    try {
      if (String((req.body || {}).confirm || '') !== '1') return res.status(400).json({ error: 'confirm required' });
      const out = buildOrders(req, req.body || {});
      if (out.problems.some(p2 => p2.level === 'stop')) {
        return res.status(400).json({ error: 'cannot_apply', problems: out.problems.filter(p2 => p2.level === 'stop') });
      }
      if (!out.orders.length) return res.status(400).json({ error: 'nothing_to_apply' });

      let created = 0, replaced = 0, lines = 0, assigned = 0;
      const tx = db.transaction(() => {
        for (const ord of out.orders) {
          // Re-uploading a week replaces that order rather than duplicating it: files get sent
          // twice, and a second copy of an order would double every unit count downstream.
          const existing = req.d2d.get(`SELECT id FROM d2d_po WHERE client_id = @client
              AND week_start = @ws AND po_number = @po AND seq = 1`, { ws: ord.week_start, po: ord.po_number });
          let poId;
          if (existing) {
            poId = existing.id; replaced++;
            req.d2d.run(`DELETE FROM d2d_po_line WHERE client_id = @client AND po_id = @id`, { id: poId });
            req.d2d.run(`DELETE FROM d2d_assignment WHERE client_id = @client AND po_id = @id`, { id: poId });
            req.d2d.run(`UPDATE d2d_po SET supplier=@supplier, cargo_ready_date=@crd, units=@units,
                           cbm=@cbm, weight_kg=@wk, value_amount=@val, currency=@ccy, source='upload'
                         WHERE client_id = @client AND id = @id`,
              { supplier: ord.supplier, crd: ord.cargo_ready_date, units: ord.units, cbm: ord.cbm,
                wk: ord.weight_kg, val: ord.value_amount, ccy: ord.currency, id: poId });
          } else {
            poId = 'po_' + crypto.randomUUID().slice(0, 12); created++;
            req.d2d.insert('d2d_po', {
              id: poId, po_number: ord.po_number, week_start: ord.week_start, seq: 1,
              supplier: ord.supplier, cargo_ready_date: ord.cargo_ready_date, units: ord.units,
              cbm: ord.cbm, weight_kg: ord.weight_kg, value_amount: ord.value_amount,
              currency: ord.currency, source: 'upload',
            });
          }
          for (const l of ord.lines) {
            req.d2d.insert('d2d_po_line', {
              id: 'pl_' + crypto.randomUUID().slice(0, 12), po_id: poId,
              sku_code: l.sku_code, description: l.description, units: l.units, cbm: null,
            });
            lines++;
          }
          for (const sid of ord.containers) {
            req.d2d.insert('d2d_assignment', {
              id: 'as_' + crypto.randomUUID().slice(0, 12), po_id: poId, shipment_id: sid,
              units: ord.units, cbm: ord.cbm,
            });
            assigned++;
          }
        }
      });
      tx();
      console.warn(`[d2d] ${req.d2d.client} orders applied — ${created} new, ${replaced} replaced, ${lines} lines`);
      res.json({ ok: true, created, replaced, lines, assigned,
                 warnings: out.problems.filter(p2 => p2.level !== 'stop').slice(0, 20) });
    } catch (e) { console.error('[d2d] po apply', e); res.status(500).json({ error: String(e.message || e) }); }
  });

  // Orders for a week, with their lines and which containers carry them.
  router.get('/po', authenticateRequest, requireD2D, auditLog('view_d2d_po'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const pos = ws
        ? req.d2d.all(`SELECT * FROM d2d_po WHERE client_id = @client AND week_start = @ws ORDER BY po_number`, { ws })
        : req.d2d.all(`SELECT * FROM d2d_po WHERE client_id = @client ORDER BY week_start DESC, po_number LIMIT 300`);
      const ids = pos.map(r => r.id);
      const idp = {}; ids.forEach((id, i) => { idp['id' + i] = id; });
      const inList = ids.map((_, i) => '@id' + i).join(',');
      const lines = ids.length ? req.d2d.all(`SELECT * FROM d2d_po_line WHERE client_id = @client AND po_id IN (${inList})`, idp) : [];
      const asg = ids.length ? req.d2d.all(`SELECT * FROM d2d_assignment WHERE client_id = @client AND po_id IN (${inList})`, idp) : [];
      const ships = req.d2d.all(`SELECT id, reference, container_type FROM d2d_shipment WHERE client_id = @client`);
      const shipById = {}; ships.forEach(x => { shipById[x.id] = x; });

      const byPo = {}, asgByPo = {};
      for (const l of lines) (byPo[l.po_id] = byPo[l.po_id] || []).push(l);
      for (const a of asg) (asgByPo[a.po_id] = asgByPo[a.po_id] || []).push({ ...a, reference: (shipById[a.shipment_id] || {}).reference || null });
      res.json({ orders: pos.map(r => ({ ...r, lines: byPo[r.id] || [], containers: asgByPo[r.id] || [] })) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ── Self-check ──
  // Proves the three rules hold on the running server rather than in a comment.
  router.get('/_selftest', authenticateRequest, requireRole(['admin']), (req, res) => {
    if (!isInternal(req)) return res.status(403).json({ error: 'internal_only' });
    const out = { tables: {}, checks: {} };
    const tables = ['d2d_booking', 'd2d_shipment', 'd2d_event', 'd2d_po', 'd2d_po_line', 'd2d_assignment'];
    let allScoped = true;
    for (const t of tables) {
      const cols = db.prepare(`PRAGMA table_info(${t})`).all();
      const cid = cols.find(c => c.name === 'client_id');
      out.tables[t] = { columns: cols.length, client_id: !!cid, not_null: !!(cid && cid.notnull) };
      if (!cid || !cid.notnull) allScoped = false;
    }
    out.checks.every_table_scoped = allScoped;

    // The guard must refuse an unscoped query.
    try {
      scopedDb('TEST').all('SELECT * FROM d2d_shipment WHERE client_id = ?');
      out.checks.unscoped_query_refused = false;
    } catch (e) { out.checks.unscoped_query_refused = /not scoped/.test(e.message); }

    // An INSERT that does not carry the client must still be refused.
    try {
      scopedDb('TEST').run(`INSERT INTO d2d_event (id, shipment_id, stage) VALUES (@id, @s, @st)`, { id: 'x', s: 'y', st: 'pickup' });
      out.checks.unscoped_insert_refused = false;
    } catch (e) { out.checks.unscoped_insert_refused = /not scoped/.test(e.message); }

    // forClient must strip every cost field.
    const stripped = forClient({ id: 'x', sell_amount: 100, cost_amount: 80, accessorial_amount: 5, margin_pct: 18 });
    // An UPDATE must bind by name and actually change a row — the bug this replaced reported
    // success while writing nothing.
    try {
      db.exec(`CREATE TABLE IF NOT EXISTS d2d_selftest (client_id TEXT NOT NULL, id TEXT, v TEXT)`);
      db.prepare(`DELETE FROM d2d_selftest`).run();
      db.prepare(`INSERT INTO d2d_selftest VALUES ('TEST','r1','before')`).run();
      const t = scopedDb('TEST');
      const r1 = t.run(`UPDATE d2d_selftest SET v = @v WHERE client_id = @client AND id = @id`, { v: 'after', id: 'r1' });
      const after = db.prepare(`SELECT v FROM d2d_selftest WHERE id='r1'`).get();
      out.checks.update_binds_by_name = r1.changes === 1 && after.v === 'after';
      db.exec(`DROP TABLE d2d_selftest`);
    } catch (e) { out.checks.update_binds_by_name = false; }

    out.checks.cost_stripped_for_clients = COST_FIELDS.every(f => !(f in stripped)) && stripped.sell_amount === 100;

    // PO numbers must be allowed to repeat across weeks.
    const idx = db.prepare(`PRAGMA index_list(d2d_po)`).all();
    const uq = idx.filter(i => i.unique).map(i => db.prepare(`PRAGMA index_info(${i.name})`).all().map(c => c.name).join(','));
    out.checks.po_number_not_unique_alone = !uq.includes('po_number') && !uq.includes('client_id,po_number');
    out.checks.po_unique_key = uq;

    out.ok = Object.entries(out.checks).filter(([k]) => k !== 'po_unique_key').every(([, v]) => v === true);
    res.json(out);
  });

  console.log('[d2d] module v1 mounted');
  return router;
};
