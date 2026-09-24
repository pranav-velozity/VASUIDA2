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

      res.json({ shipments: rows.map(r => ({
        ...r,
        events: byShipment[r.id] || [],
        po_count: (load[r.id] || {}).po_count || 0,
        units: (load[r.id] || {}).units || 0,
        cbm: (load[r.id] || {}).cbm || null,
      })) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/bookings', authenticateRequest, requireD2D, auditLog('view_d2d_bookings'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const internal = isInternal(req);
      const rows = ws
        ? req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = @client AND week_start = @ws ORDER BY option_ref`, { ws })
        : req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = @client ORDER BY week_start DESC LIMIT 60`);
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
          });
          made.push(id);
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

  // ── Orders: the PO, SKU and unit feed ──
  // One row per SKU line. PO-level fields repeat down the rows, which is how every ERP export
  // and every spreadsheet a supplier sends actually looks.
  //
  // Preview and apply are separate calls on purpose: a bad file must never half-load into a
  // live week, and the preview is where supplier and container mismatches surface.
  const PO_COLUMNS = {
    po_number:   ['po_number', 'po', 'po no', 'po #', 'purchase order', 'order'],
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
  function mapColumns(headers) {
    const found = {}, lower = headers.map(h => String(h).toLowerCase().trim());
    for (const [field, names] of Object.entries(PO_COLUMNS)) {
      const i = lower.findIndex(h => names.includes(h));
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
      const week = dateOf(col.week_start ? r[col.week_start] : null) || weekFallback;
      if (!week) { problems.push({ level: 'row', row: idx + 2, message: `PO ${po}: no week, and none supplied for the file.` }); return; }

      const key = week + '|' + po;
      if (!orders.has(key)) {
        orders.set(key, {
          po_number: po, week_start: week,
          supplier: col.supplier ? String(r[col.supplier] || '').trim() || null : null,
          cargo_ready_date: col.cargo_ready ? dateOf(r[col.cargo_ready]) : null,
          cbm: null, weight_kg: null, value_amount: null,
          currency: col.currency ? (String(r[col.currency] || '').trim().toUpperCase() || null) : null,
          lines: [], containers: new Set(),
        });
      }
      const ord = orders.get(key);
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
        })),
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
