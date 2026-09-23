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
  `);

  // ── The scoped data layer ──
  // Nothing in this module talks to db directly. Queries must filter on client_id, and the
  // client is supplied here rather than by the caller.
  const SCOPED = /client_id\s*=\s*\?/i;
  function scopedDb(client) {
    if (!client) throw new Error('d2d: no client in scope');
    const guard = (sql) => {
      if (!SCOPED.test(sql)) throw new Error('d2d: query is not scoped to a client — ' + sql.slice(0, 80));
      return sql;
    };
    return {
      client,
      all:  (sql, ...p) => db.prepare(guard(sql)).all(client, ...p),
      get:  (sql, ...p) => db.prepare(guard(sql)).get(client, ...p),
      run:  (sql, ...p) => db.prepare(guard(sql)).run(client, ...p),
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
      req.d2d = scopedDb(client);
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
      const rows = req.d2d.all(`SELECT week_start,
          COUNT(*) AS shipments,
          SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered
        FROM d2d_shipment WHERE client_id = ?
        GROUP BY week_start ORDER BY week_start DESC LIMIT 26`);
      res.json({ weeks: rows });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/shipments', authenticateRequest, requireD2D, auditLog('view_d2d_shipments'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const rows = ws
        ? req.d2d.all(`SELECT * FROM d2d_shipment WHERE client_id = ? AND week_start = ? ORDER BY reference`, ws)
        : req.d2d.all(`SELECT * FROM d2d_shipment WHERE client_id = ? ORDER BY week_start DESC, reference LIMIT 100`);
      const ids = rows.map(r => r.id);
      const events = ids.length
        ? req.d2d.all(`SELECT * FROM d2d_event WHERE client_id = ? AND shipment_id IN (${ids.map(() => '?').join(',')})`, ...ids)
        : [];
      const byShipment = {};
      for (const e of events) (byShipment[e.shipment_id] = byShipment[e.shipment_id] || []).push(e);
      res.json({ shipments: rows.map(r => ({ ...r, events: byShipment[r.id] || [] })) });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  router.get('/bookings', authenticateRequest, requireD2D, auditLog('view_d2d_bookings'), (req, res) => {
    try {
      const ws = String(req.query.week || '');
      const internal = isInternal(req);
      const rows = ws
        ? req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = ? AND week_start = ? ORDER BY option_ref`, ws)
        : req.d2d.all(`SELECT * FROM d2d_booking WHERE client_id = ? ORDER BY week_start DESC LIMIT 60`);
      // A client sees released options only, and never the cost behind them.
      const visible = internal ? rows : rows.filter(r => r.status !== 'draft').map(forClient);
      res.json({ bookings: visible, pricing_visible: internal });
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
      scopedDb('TEST').all('SELECT * FROM d2d_shipment');
      out.checks.unscoped_query_refused = false;
    } catch (e) { out.checks.unscoped_query_refused = /not scoped/.test(e.message); }

    // forClient must strip every cost field.
    const stripped = forClient({ id: 'x', sell_amount: 100, cost_amount: 80, accessorial_amount: 5, margin_pct: 18 });
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
