// server.js — VelOzity UID Ops Backend (Express + SQLite + SSE)

const fs = require('fs');
const path = require('path');
const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');

// ── Anthropic client (lazy-init so server starts even if key not set) ──
let _anthropic = null;
function getAnthropic() {
  if (!_anthropic) {
    const key = process.env.ANTHROPIC_API_KEY;
    if (!key) throw new Error('ANTHROPIC_API_KEY env var not set');
    _anthropic = new Anthropic({ apiKey: key });
  }
  return _anthropic;
}
const cors = require('cors');
const ExcelJS = require('exceljs');
const Database = require('better-sqlite3');
const { randomUUID } = require('crypto');

// 🔐 Security Middleware
const { authenticateRequest, requireRole, autoFilterResponse, optionalAuth, authenticateApiKey } = require('./middleware/auth');
const { apiLimiter, writeOpLimiter, uploadLimiter, aiLimiter } = require('./middleware/rateLimiter');
const { validateRecordInput, validateBulkInput } = require('./middleware/validation');
const { auditLog } = require('./middleware/auditLog');

// ---- Config ----
const PORT = process.env.PORT || 4000;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*'; // set to your frontend origin(s) in prod
const DB_DIR = process.env.DB_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DB_DIR, { recursive: true });
const DB_FILE = process.env.DB_FILE || path.join(DB_DIR, 'uid_ops.sqlite');

// ---- App ----
const app = express();

// --- Global CORS (single source of truth) ---
const allowList = (ALLOWED_ORIGIN || '*')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const corsOptions = {
  origin: (origin, cb) => {
    if (!origin) return cb(null, true); // same-origin/curl
    if (allowList.includes('*') || allowList.includes(origin)) return cb(null, true);
    // allow Netlify preview subdomains if a wildcard *.netlify.app was provided
    if (allowList.some(a => a.endsWith('.netlify.app') && origin.endsWith('.netlify.app'))) {
      return cb(null, true);
    }
    return cb(new Error('Not allowed by CORS: ' + origin));
  },
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions));
app.use(express.json({ limit: '100mb' }));

// 🔐 Rate Limiting
app.use(apiLimiter);

/* ===== BEGIN: /api alias -> root endpoints =====
   This lets /api/plan, /api/records, /api/bins, /api/flow/... hit the same handlers as
   /plan, /records, /bins, /flow/... without duplicating code.
   Place ABOVE all your app.get('/...') routes.
*/
app.use((req, _res, next) => {
  if (req.url === '/api' || req.url === '/api/') {
    req.url = '/';
    return next();
  }
  if (req.url.startsWith('/api/')) {
    req.url = req.url.slice(4) || '/'; // drop leading "/api"
  }
  next();
});
/* ===== END: /api alias ===== */


// ---- Summary cache (in-memory, additive) ----
const _summaryCache = new Map();
const SUMMARY_TTL_MS = 30 * 1000; // 30s

function _summaryKey(q) {
  const from = String(q.from || q.weekStart || '').trim();
  const to = String(q.to || q.weekEnd || '').trim();
  const status = String(q.status || 'complete').trim();
  return `${from}|${to}|${status}`;
}



// --- Time helpers (America/Chicago) ---
function chicagoISOFromDate(d = new Date()) {
  try {
    const fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit'
    });
    const parts = fmt.formatToParts(d);
    const y = parts.find(p => p.type === 'year')?.value;
    const m = parts.find(p => p.type === 'month')?.value;
    const dd = parts.find(p => p.type === 'day')?.value;
    if (y && m && dd) return `${y}-${m}-${dd}`;
  } catch {}
  return d.toISOString().slice(0, 10);
}
const todayChicagoISO = () => chicagoISOFromDate(new Date());

function toISODate(v) {
  if (v == null) return '';
  if (typeof v === 'number' && isFinite(v)) { // Excel serial
    const base = new Date(Date.UTC(1899, 11, 30));
    const ms = Math.round(v * 86400000);
    const d = new Date(base.getTime() + ms);
    return d.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  if (!s) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const d1 = new Date(s);
  if (!isNaN(d1)) return d1.toISOString().slice(0, 10);
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
  if (m) {
    const dd = m[1].padStart(2, '0');
    const mm = m[2].padStart(2, '0');
    const yyyy = (m[3].length === 2 ? '20' + m[3] : m[3]);
    const d2 = new Date(`${yyyy}-${mm}-${dd}T00:00:00Z`);
    if (!isNaN(d2)) return d2.toISOString().slice(0, 10);
  }
  return '';
}

// --- DB setup ---
const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.exec(`
CREATE TABLE IF NOT EXISTS records (
  id TEXT PRIMARY KEY,
  date_local TEXT,
  mobile_bin TEXT,
  sscc_label TEXT,
  po_number TEXT,
  sku_code TEXT,
  uid TEXT,
  status TEXT DEFAULT 'draft',
  completed_at TEXT,
  sync_state TEXT DEFAULT 'unknown'
);
CREATE INDEX IF NOT EXISTS idx_records_date ON records(date_local);
CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);
CREATE INDEX IF NOT EXISTS idx_records_completed_at ON records(completed_at);
CREATE INDEX IF NOT EXISTS idx_records_status_completed_at ON records(status, completed_at);
CREATE INDEX IF NOT EXISTS idx_records_po ON records(po_number);
CREATE INDEX IF NOT EXISTS idx_records_po_status ON records(po_number, status);
CREATE INDEX IF NOT EXISTS idx_records_po_mobile_bin ON records(po_number, mobile_bin);
CREATE INDEX IF NOT EXISTS idx_records_sku ON records(sku_code);
CREATE INDEX IF NOT EXISTS idx_records_uid ON records(uid);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_po_sku_uid ON records(po_number, sku_code, uid);

CREATE TABLE IF NOT EXISTS plans (
  week_start TEXT PRIMARY KEY,
  data TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
`);
db.exec(`
CREATE TABLE IF NOT EXISTS receiving(
  week_start TEXT NOT NULL,
  po_number TEXT NOT NULL,
  supplier_name TEXT,
  facility_name TEXT,
  received_at_utc TEXT,
  received_at_local TEXT,
  received_tz TEXT,
  cartons_received INTEGER DEFAULT 0,
  cartons_damaged INTEGER DEFAULT 0,
  cartons_noncompliant INTEGER DEFAULT 0,
  cartons_replaced INTEGER DEFAULT 0,
  updated_at TEXT,
  PRIMARY KEY(week_start, po_number)
);
CREATE INDEX IF NOT EXISTS idx_receiving_week ON receiving(week_start);
CREATE INDEX IF NOT EXISTS idx_receiving_supplier ON receiving(week_start, supplier_name);
`);

// ---- Flow week persistence (facility-scoped) ----
db.exec(`
CREATE TABLE IF NOT EXISTS flow_week (
  facility   TEXT NOT NULL,
  week_start TEXT NOT NULL,
  data       TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (facility, week_start)
);
CREATE INDEX IF NOT EXISTS idx_flow_week_ws ON flow_week(week_start);
CREATE INDEX IF NOT EXISTS idx_flow_week_fac ON flow_week(facility);
`);

// ── Finance tables ──
db.exec(`
CREATE TABLE IF NOT EXISTS fin_invoices (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL CHECK(type IN ('VAS','SEA','AIR')),
  week_start  TEXT NOT NULL,
  ref_number  TEXT NOT NULL UNIQUE,
  status      TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','sent','paid','overdue')),
  invoice_date TEXT,
  due_date    TEXT,
  subtotal    REAL DEFAULT 0,
  gst         REAL DEFAULT 0,
  customs     REAL DEFAULT 0,
  misc_total  REAL DEFAULT 0,
  total       REAL DEFAULT 0,
  currency    TEXT DEFAULT 'USD',
  notes       TEXT,
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fin_inv_week ON fin_invoices(week_start);
CREATE INDEX IF NOT EXISTS idx_fin_inv_type ON fin_invoices(type);
CREATE INDEX IF NOT EXISTS idx_fin_inv_status ON fin_invoices(status);

CREATE TABLE IF NOT EXISTS fin_invoice_lines (
  id          TEXT PRIMARY KEY,
  invoice_id  TEXT NOT NULL REFERENCES fin_invoices(id) ON DELETE CASCADE,
  sort_order  INTEGER DEFAULT 0,
  description TEXT NOT NULL,
  unit_label  TEXT,
  rate        REAL DEFAULT 0,
  quantity    REAL DEFAULT 0,
  total       REAL DEFAULT 0,
  gst_free    INTEGER DEFAULT 0,
  is_misc     INTEGER DEFAULT 0,
  created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fin_lines_inv ON fin_invoice_lines(invoice_id);

CREATE TABLE IF NOT EXISTS fin_expenses (
  id          TEXT PRIMARY KEY,
  category    TEXT NOT NULL,
  description TEXT NOT NULL,
  amount      REAL NOT NULL,
  currency    TEXT DEFAULT 'USD',
  expense_date TEXT NOT NULL,
  month_key   TEXT NOT NULL,
  is_recurring INTEGER DEFAULT 0,
  recur_freq  TEXT CHECK(recur_freq IN ('monthly','quarterly','annually',NULL)),
  recur_end   TEXT,
  parent_id   TEXT,
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fin_exp_month ON fin_expenses(month_key);
CREATE INDEX IF NOT EXISTS idx_fin_exp_cat ON fin_expenses(category);
CREATE INDEX IF NOT EXISTS idx_fin_exp_recur ON fin_expenses(is_recurring);

CREATE TABLE IF NOT EXISTS fin_fx_rates (
  id          TEXT PRIMARY KEY,
  from_curr   TEXT NOT NULL,
  to_curr     TEXT NOT NULL,
  rate        REAL NOT NULL,
  source      TEXT DEFAULT 'manual',
  fetched_at  TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(from_curr, to_curr)
);
`);





const selectRecordById = db.prepare('SELECT * FROM records WHERE id = ?');
const selectByComposite = db.prepare('SELECT id FROM records WHERE po_number = ? AND sku_code = ? AND uid = ?');
const deleteById = db.prepare('DELETE FROM records WHERE id = ?');
const deleteBySkuUid = db.prepare('DELETE FROM records WHERE uid = ? AND sku_code = ?');
const deleteByUid = db.prepare('DELETE FROM records WHERE uid = ?');

const upsertByComposite = db.prepare(`
INSERT INTO records (id, date_local, mobile_bin, sscc_label, po_number, sku_code, uid, status, completed_at, sync_state)
VALUES (@id, @date_local, @mobile_bin, @sscc_label, @po_number, @sku_code, @uid, @status, @completed_at, @sync_state)
ON CONFLICT(po_number, sku_code, uid) DO UPDATE SET
  date_local   = COALESCE(excluded.date_local, records.date_local),
  mobile_bin   = COALESCE(excluded.mobile_bin, records.mobile_bin),
  sscc_label   = COALESCE(excluded.sscc_label, records.sscc_label),
  status       = CASE WHEN excluded.status='complete' THEN 'complete' ELSE records.status END,
  completed_at = COALESCE(records.completed_at, excluded.completed_at),
  sync_state   = 'synced'
`);

// --- Completion rule (SSCC optional) ---
function isComplete(row) {
  return Boolean(
    (row?.date_local || '').trim() &&
    (row?.mobile_bin || '').trim() &&
    (row?.po_number || '').trim() &&
    (row?.sku_code  || '').trim() &&
    (row?.uid       || '').trim()
  );
}

// --- SSE hub for ops pulse ---
const clients = new Set();
function emitScan(ts = new Date()) {
  const payload = JSON.stringify({ ts: ts.toISOString() });
  for (const res of clients) {
    try { res.write(`data: ${payload}\n\n`); } catch {}
  }
}

app.get('/events/scan', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': allowList.includes('*') ? '*' : (req.headers.origin || allowList[0] || '*')
  });
  res.write('\n');
  clients.add(res);
  req.on('close', () => { clients.delete(res); });
});

// --- Health ---
app.get('/health', (req, res) => res.json({ ok: true, now: new Date().toISOString() }));

// --- Debug: show flow_week facilities and week_starts ---
app.get('/debug/flow_weeks', authenticateRequest, (req, res) => {
  const rows = db.prepare('SELECT facility, week_start, updated_at, length(data) as data_len FROM flow_week ORDER BY week_start DESC LIMIT 20').all();
  // Also show a sample of what lanes/containers exist
  const detail = rows.slice(0,5).map(r => {
    const fw = db.prepare('SELECT data FROM flow_week WHERE facility=? AND week_start=?').get(r.facility, r.week_start);
    const d = safeJsonParse(fw?.data, {}) || {};
    const lanes = Object.keys(d.intl_lanes || {}).length;
    const wc = d.intl_weekcontainers;
    const conts = Array.isArray(wc) ? wc.length : (Array.isArray(wc?.containers) ? wc.containers.length : 0);
    return { ...r, lanes, containers: conts };
  });
  res.json({ rows: detail });
});

// ── Pulse AI endpoints moved to /ai section below ──





// ===== AI / Pulse Endpoints =====

// AI endpoints use the existing writeOpLimiter

// ── GET /pulse/context — full 12-week context for PULSE session ──
// Called once per session on first PULSE open. Returns everything Claude needs.
app.get('/pulse/context',
  authenticateRequest,
  aiLimiter,
  async (req, res) => {
  try {
    const facilityHint = normFacility(req.query.facility || '');

    // ── Resolve facility + find last 4 week-starts ──
    const allPlans = db.prepare('SELECT week_start, data FROM plans ORDER BY week_start DESC LIMIT 6').all();
    let facility = facilityHint;
    if (!facility) {
      for (const row of allPlans) {
        const rows = safeJsonParse(row.data, []) || [];
        const fac = rows.map(p => String(p.facility_name||p.facility||'').trim()).find(Boolean);
        if (fac) { facility = fac; break; }
      }
    }

    // Get 4 most recent weeks that have plan data
    const weeks = [];
    for (const row of allPlans) {
      const rows = safeJsonParse(row.data, []) || [];
      if (rows.length > 0) {
        weeks.push(row.week_start);
        if (weeks.length >= 4) break;
      }
    }
    weeks.reverse(); // chronological order

    const weekData = [];

    for (const ws of weeks) {
      const wsDate = new Date(ws + 'T00:00:00Z');
      const weDate = new Date(wsDate); weDate.setUTCDate(weDate.getUTCDate() + 6);
      const we = weDate.toISOString().slice(0, 10);

      // ── Plan rows ──
      const planRow = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(ws);
      const planRows = safeJsonParse(planRow?.data, []) || [];

      // ── Applied by PO ──
      const appliedByPO = new Map();
      const recRows = db.prepare(
        `SELECT po_number, COUNT(*) as n FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete' GROUP BY po_number`
      ).all(ws, we);
      for (const r of recRows) appliedByPO.set(String(r.po_number||'').trim(), r.n);

      // ── Receiving ──
      const recvRows = db.prepare('SELECT * FROM receiving WHERE week_start = ?').all(ws);

      // ── Bins (mobile bins / cartons out) ──
      // bins table stores one row per mobile_bin with total_units and weight
      // Join against records to get PO association
      const binRows = db.prepare(`
        SELECT b.mobile_bin, b.total_units, b.weight_kg,
               r.po_number
        FROM bins b
        LEFT JOIN (
          SELECT TRIM(mobile_bin) as mobile_bin,
                 po_number,
                 COUNT(*) as scan_count
          FROM records
          WHERE date_local >= ? AND date_local <= ?
            AND TRIM(COALESCE(mobile_bin,'')) <> ''
            AND TRIM(COALESCE(po_number,'')) <> ''
          GROUP BY TRIM(mobile_bin), po_number
        ) r ON TRIM(b.mobile_bin) = r.mobile_bin
        WHERE b.week_start = ?
        ORDER BY r.scan_count DESC
      `).all(ws, we, ws);

      // Build per-PO bin counts
      const binsByPO = new Map(); // po → { bin_count, total_units, weight_kg }
      const seenBins = new Set();
      let totalBins = 0;
      let totalBinUnits = 0;
      let totalBinWeight = 0;
      for (const b of binRows) {
        const bin = String(b.mobile_bin||'').trim();
        if(!bin || seenBins.has(bin)) continue;
        seenBins.add(bin);
        totalBins++;
        totalBinUnits += Number(b.total_units||0);
        totalBinWeight += Number(b.weight_kg||0);
        const po = String(b.po_number||'').trim();
        if(po) {
          if(!binsByPO.has(po)) binsByPO.set(po, { bin_count:0, total_units:0, weight_kg:0 });
          const entry = binsByPO.get(po);
          entry.bin_count++;
          entry.total_units += Number(b.total_units||0);
          entry.weight_kg   += Number(b.weight_kg||0);
        }
      }

      // ── Flow week (lanes + containers) ──
      // Fetch ALL rows for this week and merge them — data may be split across
      // multiple rows with different facility keys (prebook, intl_lanes etc. saved separately)
      const allFlowRows = db.prepare('SELECT facility, data FROM flow_week WHERE week_start = ?').all(ws);
      const flowData = {};
      for (const row of allFlowRows) {
        const d = safeJsonParse(row.data, {}) || {};
        // Merge all rows — intl_lanes gets deep-merged, everything else overwrites
        for (const [k, v] of Object.entries(d)) {
          if (k === 'intl_lanes' && v && typeof v === 'object' && !Array.isArray(v)) {
            flowData.intl_lanes = Object.assign({}, flowData.intl_lanes || {}, v);
          } else {
            flowData[k] = v;
          }
        }
      }
      const laneCount = Object.keys((flowData.intl_lanes && typeof flowData.intl_lanes === 'object') ? flowData.intl_lanes : {}).length;
      const contArr2 = (() => { const wc = flowData.intl_weekcontainers; return Array.isArray(wc) ? wc : (Array.isArray(wc?.containers) ? wc.containers : []); })();
      console.log('[pulse/context] week', ws, '- merged', allFlowRows.length, 'flow_week rows | lanes:', laneCount, '| containers:', contArr2.length);


      // ── Build PO summary (join plan + applied + receiving) ──
      const poMap = new Map();
      for (const p of planRows) {
        const po = String(p.po_number||'').trim();
        const sku = String(p.sku_code||'').trim();
        if (!po) continue;
        if (!poMap.has(po)) poMap.set(po, {
          po,
          supplier: p.supplier_name || '',
          freight: p.freight_type || '',
          zendesk: p.zendesk_ticket || '',
          due_date: p.due_date || '',
          skus: [],
          planned: 0,
          applied: appliedByPO.get(po) || 0,  // set ONCE — records are per-PO not per-SKU
          received: false,
          received_date: null,
          cartons_received: 0,
          mobile_bins: 0,
          mobile_bin_units: 0,
        });
        const entry = poMap.get(po);
        entry.planned += Number(p.target_qty || 0) || 0;
        if (sku) entry.skus.push({ sku, planned: Number(p.target_qty||0)||0 });
      }
      // Merge receiving
      for (const r of recvRows) {
        const po = String(r.po_number||'').trim();
        if (poMap.has(po)) {
          const entry = poMap.get(po);
          entry.received = true;
          entry.received_date = r.received_at_local || null;
          entry.cartons_received = Number(r.cartons_received||0)||0;
        }
      }
      // Merge bins
      for (const [po, binData] of binsByPO.entries()) {
        if (poMap.has(po)) {
          const entry = poMap.get(po);
          entry.mobile_bins      = binData.bin_count;
          entry.mobile_bin_units = binData.total_units;
        }
      }
      const pos = Array.from(poMap.values());

      // ── Lane summary ──
      const lanes = [];
      const intl_lanes = (flowData.intl_lanes && typeof flowData.intl_lanes === 'object') ? flowData.intl_lanes : {};
      for (const [laneKey, manual] of Object.entries(intl_lanes)) {
        if (!manual || typeof manual !== 'object') continue;
        const parts = laneKey.split('||');
        lanes.push({
          supplier: parts[0] || '',
          zendesk: parts[1] || '',
          freight: parts[2] || '',
          packing_list_ready: manual.packing_list_ready_at || null,
          origin_customs_cleared: manual.origin_customs_cleared_at || null,
          departed: manual.departed_at || null,
          arrived: manual.arrived_at || null,
          dest_customs_cleared: manual.dest_customs_cleared_at || null,
          eta_fc: manual.eta_fc || null,
          latest_arrival_date: manual.latest_arrival_date || null,
          customs_hold: manual.customs_hold || false,
          shipment_number: manual.shipmentNumber || manual.shipment || null,
          hbl: manual.hbl || null,
          mbl: manual.mbl || null,
        });
      }

      // ── Container summary ──
      const containers = [];
      const wc = flowData.intl_weekcontainers;
      const contArr = Array.isArray(wc) ? wc : (Array.isArray(wc?.containers) ? wc.containers : []);
      for (const c of contArr) {
        const cid = String(c.container_id||c.container||'').trim();
        if (!cid) continue;
        containers.push({
          container_id: cid,
          vessel: c.vessel || '',
          size_ft: c.size_ft || '40',
          pos: String(c.pos||'').split(',').map(p=>p.trim()).filter(Boolean),
          lane_keys: Array.isArray(c.lane_keys) ? c.lane_keys : [],
        });
      }

      // ── Week totals ──
      const planned_total = pos.reduce((s,p)=>s+p.planned,0);
      const applied_total = pos.reduce((s,p)=>s+p.applied,0);
      const received_pos  = pos.filter(p=>p.received).length;
      const late_pos      = pos.filter(p=>p.received && p.due_date && p.received_date && p.received_date.slice(0,10) > p.due_date).length;

      console.log('[pulse/context] week', ws,
        '| facility:', facility,
        '| flow rows merged:', allFlowRows.length,
        '| lanes:', lanes.length,
        '| containers:', containers.length,
        '| pos:', pos.length
      );

      weekData.push({
        week_start: ws,
        week_end: we,
        planned_total,
        applied_total,
        completion_pct: planned_total > 0 ? Math.round(applied_total/planned_total*100) : 0,
        received_pos,
        late_pos,
        total_mobile_bins: totalBins,
        total_bin_units:   totalBinUnits,
        total_bin_weight_kg: Math.round(totalBinWeight * 10) / 10,
        pos,       // full PO detail (now includes mobile_bins + mobile_bin_units per PO)
        lanes,     // all transit lanes with dates
        containers // all containers with PO assignments
      });
    }

    return res.json({ facility, weeks: weekData, generated_at: new Date().toISOString() });

  } catch(e) {
    console.error('[/pulse/context]', e);
    res.status(500).json({ error: 'Failed to build pulse context: ' + (e.message||e) });
  }
});

// ── Helper: build ops context snapshot (used by /pulse/chat as fallback) ──
function buildOpsContext(facility, weekStart) {
  try {
    // Current week plan
    const planRow = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(weekStart);
    const planRows = planRow ? (safeJsonParse(planRow.data, []) || []) : [];

    const planned_units  = planRows.reduce((s,p) => s + (Number(p.target_qty)||0), 0);
    const unique_pos     = new Set(planRows.map(p=>p.po_number).filter(Boolean)).size;
    const unique_sups    = new Set(planRows.map(p=>p.supplier_name).filter(Boolean)).size;
    const air_planned    = planRows.filter(p=>(p.freight_type||'').toLowerCase()==='air').reduce((s,p)=>s+(Number(p.target_qty)||0),0);
    const sea_planned    = planRows.filter(p=>(p.freight_type||'').toLowerCase()==='sea').reduce((s,p)=>s+(Number(p.target_qty)||0),0);

    // Applied this week
    const wsDate = new Date(weekStart+'T00:00:00Z');
    const weDate = new Date(wsDate); weDate.setUTCDate(weDate.getUTCDate()+6);
    const we = weDate.toISOString().slice(0,10);
    const applied_units = db.prepare(`SELECT COUNT(*) as n FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete'`).get(weekStart, we)?.n || 0;
    const unique_bins   = db.prepare(`SELECT COUNT(DISTINCT mobile_bin) as n FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete'`).get(weekStart, we)?.n || 0;

    // Receiving
    const recvRows = db.prepare('SELECT po_number, received_at_local FROM receiving WHERE week_start = ?').all(weekStart);
    const received_pos = recvRows.length;
    let late_pos = 0;
    const dueDateByPO = new Map(planRows.filter(p=>p.po_number&&p.due_date).map(p=>[String(p.po_number).trim(),String(p.due_date)]));
    for (const r of recvRows) {
      const po = String(r.po_number||'').trim();
      const due = dueDateByPO.get(po);
      if (r.received_at_local && due && r.received_at_local.slice(0,10) > due.slice(0,10)) late_pos++;
    }

    // Supplier breakdown
    const supMap = new Map();
    for (const p of planRows) {
      const s = String(p.supplier_name||'').trim()||'Unknown';
      if (!supMap.has(s)) supMap.set(s, {planned:0,applied:0});
      supMap.get(s).planned += Number(p.target_qty)||0;
    }
    const recRows2 = db.prepare(`SELECT po_number FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete'`).all(weekStart, we);
    const poToSup  = new Map(planRows.map(p=>[String(p.po_number||'').trim(), String(p.supplier_name||'').trim()||'Unknown']));
    for (const r of recRows2) {
      const sup = poToSup.get(String(r.po_number||'').trim());
      if (sup && supMap.has(sup)) supMap.get(sup).applied++;
    }
    const suppliers = Array.from(supMap.entries()).map(([name,d])=>({
      name, planned:d.planned, applied:d.applied,
      pct: d.planned>0 ? Math.round(d.applied/d.planned*100) : 0
    })).sort((a,b)=>b.planned-a.planned);

    // Flow week (transit/container data)
    const flowRow = db.prepare('SELECT data FROM flow_week WHERE facility=? AND week_start=?').get(facility, weekStart);
    const flowData = flowRow ? (safeJsonParse(flowRow.data,{})||{}) : {};
    const intl_lanes = flowData.intl_lanes || {};
    const laneCount  = Object.keys(intl_lanes).length;
    const etaDates   = Object.values(intl_lanes).map(l=>l&&l.eta_fc).filter(Boolean).sort();
    const nextEtaFC  = etaDates.pop() || null;

    const completion_pct = planned_units > 0 ? Math.round(applied_units/planned_units*100) : 0;
    const otr_pct = unique_pos > 0 ? Math.round((received_pos - late_pos) / unique_pos * 100) : null;

    return {
      facility, week_start: weekStart, week_end: we,
      planned_units, applied_units, completion_pct,
      unique_pos, unique_sups, received_pos, late_pos,
      otr_pct, unique_bins,
      air_planned, sea_planned,
      suppliers: suppliers.slice(0,8),
      active_lanes: laneCount,
      next_eta_fc: nextEtaFC,
    };
  } catch(e) {
    console.error('[buildOpsContext]', e);
    return { facility, week_start: weekStart };
  }
}

// POST /pulse/chat — full-fidelity conversational assistant
app.post('/pulse/chat',
  authenticateRequest,
  aiLimiter,
  async (req, res) => {
  try {
    const { messages, pulseContext, currentWeek } = req.body || {};
    if (!messages || !Array.isArray(messages) || !messages.length) {
      return res.status(400).json({ error: 'messages array required' });
    }

    const userRole = req.auth?.orgRole || '';
    const roleLabel = {
      'org:admin_auth':    'Admin',
      'org:supplier_auth': 'Facility',
      'org:client_auth':   'Client',
      'org:api_auth':      'API'
    }[userRole] || 'User';

    const facility = pulseContext?.facility || currentWeek?.facility || '';
    const weeks    = Array.isArray(pulseContext?.weeks) ? pulseContext.weeks : [];
    const currWk   = currentWeek?.week_start || (weeks.length ? weeks[weeks.length-1].week_start : '');

    // ── Build the system prompt from full context ──
    const lines = [];
    lines.push('You are Pulse, the AI operations assistant for VelOzity Pinpoint — a real-time warehouse UID operations platform.');
    lines.push('');
    lines.push('## Your role');
    lines.push('You help the VelOzity operations team understand warehouse performance across receiving, VAS processing, transit, and FC delivery.');
    lines.push('You have FULL access to the last 4 weeks of detailed operations data below. Use it to answer any question specifically and accurately.');
    lines.push('Be concise and direct. Lead with numbers. Do not hedge when the data is clear.');
    lines.push('You are read-only — guide users to the UI for any changes (e.g. "Update that in Week Hub → Transit & Clearing").');
    lines.push('');

    // ── Finance context (admin only) ──
    if (userRole === 'org:admin_auth') {
      try {
        const finSum = db.prepare(`
          SELECT
            (SELECT COUNT(*) FROM fin_invoices WHERE status IN ('draft','sent','overdue')) as outstanding_n,
            (SELECT COALESCE(SUM(total),0) FROM fin_invoices WHERE status IN ('draft','sent','overdue')) as outstanding_total,
            (SELECT COALESCE(SUM(total),0) FROM fin_invoices WHERE status='paid' AND week_start >= ?) as revenue_ytd,
            (SELECT COALESCE(SUM(amount),0) FROM fin_expenses WHERE month_key >= ?) as expenses_ytd
        `).get(`${new Date().getUTCFullYear()}-01-01`, `${new Date().getUTCFullYear()}-01`);
        if (finSum) {
          const net = finSum.revenue_ytd - finSum.expenses_ytd;
          const margin = finSum.revenue_ytd > 0 ? Math.round(net/finSum.revenue_ytd*100) : 0;
          lines.push('## Finance summary (YTD)');
          lines.push(`- Revenue YTD: USD ${finSum.revenue_ytd.toLocaleString()}`);
          lines.push(`- Expenses YTD: USD ${finSum.expenses_ytd.toLocaleString()}`);
          lines.push(`- Net: USD ${net.toLocaleString()} (${margin}% margin)`);
          lines.push(`- Outstanding invoices: ${finSum.outstanding_n} (USD ${finSum.outstanding_total.toLocaleString()})`);
          lines.push('');
        }
      } catch(e) { /* Finance tables may not exist yet */ }
    }
    lines.push('## User context');
    lines.push('- Role: ' + roleLabel);
    lines.push('- Facility: ' + (facility || 'not set'));
    lines.push('- Currently viewing week: ' + (currWk || 'not set'));
    lines.push('');

    if (weeks.length > 0) {
      lines.push('## Operations data — last ' + weeks.length + ' weeks (' + weeks[0].week_start + ' to ' + weeks[weeks.length-1].week_start + ') [most recent 4 weeks]');
      lines.push('');

      for (const w of weeks) {
        lines.push('### Week ' + w.week_start + ' (ends ' + w.week_end + ')');
        lines.push('Planned: ' + (w.planned_total||0).toLocaleString() + ' units | Applied: ' + (w.applied_total||0).toLocaleString() + ' (' + (w.completion_pct||0) + '%) | Received POs: ' + (w.received_pos||0) + ' | Late POs: ' + (w.late_pos||0) + ' | Mobile bins (cartons out): ' + (w.total_mobile_bins||0) + ' | Bin units: ' + (w.total_bin_units||0).toLocaleString() + (w.total_bin_weight_kg ? ' | Bin weight: ' + w.total_bin_weight_kg + ' kg' : ''));
        lines.push('');

        // PO detail
        if (Array.isArray(w.pos) && w.pos.length > 0) {
          lines.push('POs this week:');
          for (const p of w.pos) {
            const recvStr = p.received ? ('received ' + (p.received_date ? p.received_date.slice(0,10) : 'yes') + (p.cartons_received ? ' (' + p.cartons_received + ' cartons in)' : '')) : 'NOT received';
            const binStr  = p.mobile_bins ? (' | ' + p.mobile_bins + ' mobile bins (' + (p.mobile_bin_units||0) + ' units out)') : '';
            const lateFlag = p.received && p.due_date && p.received_date && p.received_date.slice(0,10) > p.due_date ? ' [LATE]' : '';
            lines.push('  PO ' + p.po + ' | ' + p.supplier + ' | ' + (p.freight||'') + ' | Zendesk: ' + (p.zendesk||'—') + ' | Due: ' + (p.due_date||'—') + ' | Planned: ' + p.planned + ' | Applied: ' + p.applied + ' | ' + recvStr + binStr + lateFlag);
          }
          lines.push('');
        }

        // Lanes/transit
        if (Array.isArray(w.lanes) && w.lanes.length > 0) {
          lines.push('Transit lanes (' + w.lanes.length + ' total):');
          for (const l of w.lanes) {
            const dates = [
              l.departed      ? 'departed ' + l.departed.slice(0,10)         : null,
              l.arrived       ? 'arrived ' + l.arrived.slice(0,10)           : null,
              l.eta_fc        ? 'ETA FC ' + l.eta_fc.slice(0,10)             : null,
              l.dest_customs_cleared ? 'customs cleared ' + l.dest_customs_cleared.slice(0,10) : null,
              l.customs_hold  ? 'CUSTOMS HOLD'                               : null,
            ].filter(Boolean).join(' | ');
            lines.push('  Lane: ' + l.supplier + ' | Zendesk: ' + (l.zendesk||'—') + ' | ' + (l.freight||'') + (l.shipment_number ? ' | Shipment: '+l.shipment_number : '') + (l.hbl ? ' | HBL: '+l.hbl : '') + (dates ? ' | ' + dates : ' | no transit dates yet'));
          }
          lines.push('');
        } else {
          lines.push('Transit lanes: none recorded for this week');
          lines.push('');
        }

        // Containers
        if (Array.isArray(w.containers) && w.containers.length > 0) {
          lines.push('Containers (' + w.containers.length + ' total):');
          for (const c of w.containers) {
            lines.push('  Container: ' + c.container_id + ' | Size: ' + (c.size_ft||'40') + 'ft | Vessel: ' + (c.vessel||'—') + ' | POs assigned: ' + (c.pos.join(', ')||'none') + ' | Lane keys: ' + (c.lane_keys||[]).join('; '));
          }
          lines.push('');
        } else {
          lines.push('Containers: none recorded for this week');
          lines.push('');
        }
      }
    } else {
      lines.push('## No operations data available');
      lines.push('The client has not loaded context yet. Ask the user to navigate to Week Hub first.');
    }

    lines.push('## Guidelines');
    lines.push('- Answer questions about specific POs, containers, dates, units directly from the data above');
    lines.push('- For changes: guide user to the correct UI location (e.g. "Update ETA FC in Week Hub → Transit & Clearing for Zendesk 77634")');
    lines.push('- For downloads: guide user to Reports & Downloads page and the specific report');
    lines.push('- If asked about something not in the 12-week window, say so clearly');
    lines.push('- Keep responses under 200 words unless user asks for detail. Use plain text, no markdown symbols.');

    // Split prompt: static ops data (cacheable) + dynamic context (not cached)
    // Cache only kicks in when content is identical across requests.
    // The ops data block is static for the whole session — perfect for caching.
    // The role/facility/week line is tiny and dynamic — sent uncached each time.

    // Find where the ops data starts (after the user context lines)
    const opsDataStartIdx = lines.findIndex(l => l.startsWith('## Operations data'));
    const staticLines  = opsDataStartIdx >= 0 ? lines.slice(opsDataStartIdx) : lines;
    const dynamicLines = opsDataStartIdx >= 0 ? lines.slice(0, opsDataStartIdx) : [];

    const staticBlock  = staticLines.join('\n');
    const dynamicBlock = dynamicLines.join('\n');

    const anthropic = getAnthropic();
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 800,
      system: [
        // Static ops data — cached for the session (content identical across turns)
        {
          type: 'text',
          text: staticBlock,
          cache_control: { type: 'ephemeral' }
        },
        // Dynamic context — small, changes per request, not cached
        {
          type: 'text',
          text: dynamicBlock,
        }
      ],
      messages: messages.map(m => ({ role: m.role, content: m.content })),
    }, {
      headers: { 'anthropic-beta': 'prompt-caching-2024-07-31' }
    });

    const reply = response.content.find(b => b.type === 'text')?.text || 'No response.';
    res.json({ reply });

  } catch(e) {
    console.error('[/pulse/chat]', e);
    if (e.message?.includes('ANTHROPIC_API_KEY')) return res.status(503).json({ error: 'AI service not configured' });
    res.status(500).json({ error: 'AI service error: ' + (e.message||e) });
  }
});

// POST /ai/pulse — generate exec Improvement Intelligence insights via Claude
app.post('/ai/pulse',
  authenticateRequest,
  aiLimiter,
  async (req, res) => {
  try {
    const { weeks, facility } = req.body || {};
    if (!weeks || !Array.isArray(weeks) || !weeks.length) {
      return res.status(400).json({ error: 'weeks array required' });
    }

    // Compute baseline (top 25% weeks)
    const daysArr = weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null).sort((a,b)=>a-b);
    const otrArr  = weeks.map(w=>w.on_time_receiving_pct).filter(v=>v!=null).sort((a,b)=>b-a);
    const thruArr = weeks.map(w=>w.planned_units>0?w.applied_units/w.planned_units*100:0).sort((a,b)=>b-a);
    const q = arr => arr.length ? arr[Math.floor(arr.length*0.25)] : null;
    const baseline = {
      avg_days_to_apply:     q(daysArr),
      on_time_receiving_pct: q(otrArr),
      throughput_pct:        q(thruArr),
    };

    const totPl   = weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp   = weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const totAir  = weeks.reduce((s,w)=>s+(w.air_units||0),0);
    const totSea  = weeks.reduce((s,w)=>s+(w.sea_units||0),0);
    const avgDays = daysArr.length ? daysArr.reduce((a,b)=>a+b,0)/daysArr.length : null;
    const avgOTR  = otrArr.length  ? otrArr.reduce((a,b)=>a+b,0)/otrArr.length   : null;
    const lateArr = weeks.map(w=>w.late_pos||0);
    const avgLate = lateArr.reduce((a,b)=>a+b,0)/weeks.length;

    // Supplier aggregates
    const supMap = new Map();
    weeks.forEach(w=>(w.suppliers||[]).forEach(s=>{
      if(!supMap.has(s.supplier)) supMap.set(s.supplier,{planned:0,applied:0});
      const m=supMap.get(s.supplier); m.planned+=(s.planned||0); m.applied+=(s.applied||0);
    }));
    const supRows = Array.from(supMap.entries())
      .map(([name,d])=>({name, pct:d.planned>0?Math.min(100,Math.round(d.applied/d.planned*100)):0, planned:d.planned}))
      .filter(s=>s.planned>200).sort((a,b)=>b.planned-a.planned);

    // Build data prompt safely without nested template literals
    const weekLines = weeks.map(w => {
      const pct = w.planned_units>0 ? Math.round(w.applied_units/w.planned_units*100) : 0;
      const days = w.avg_days_to_apply!=null ? w.avg_days_to_apply.toFixed(1)+'d' : '--';
      const otr  = w.on_time_receiving_pct!=null ? Math.round(w.on_time_receiving_pct)+'%' : '--';
      return '- '+w.week_start+': planned '+(w.planned_units||0).toLocaleString()+', applied '+(w.applied_units||0).toLocaleString()+' ('+pct+'%), recv->VAS '+days+', on-time '+otr+', late POs '+(w.late_pos||0);
    }).join('\n');
    const supLines = supRows.map(s => '- '+s.name+': '+s.pct+'% completion ('+s.planned.toLocaleString()+' units planned)').join('\n') || 'No manufacturer data';
    const airPct2  = (totAir+totSea)>0 ? Math.round(totAir/(totAir+totSea)*100) : 0;
    const dataPrompt = [
      'You are analysing VelOzity Pinpoint operations data for facility '+facility+' over '+weeks.length+' weeks.',
      '',
      '## Aggregate performance',
      '- Total planned units: '+totPl.toLocaleString(),
      '- Total applied units: '+totAp.toLocaleString()+' ('+(totPl>0?Math.round(totAp/totPl*100):0)+'% completion)',
      '- Avg days receiving -> VAS complete: '+(avgDays!=null?avgDays.toFixed(1)+'d':'no data'),
      '- Best-week baseline (top 25%): '+(baseline.avg_days_to_apply!=null?baseline.avg_days_to_apply.toFixed(1)+'d':'no data'),
      '- On-time receiving avg: '+(avgOTR!=null?Math.round(avgOTR)+'%':'no data')+' (best-week: '+(baseline.on_time_receiving_pct!=null?Math.round(baseline.on_time_receiving_pct)+'%':'no data')+')',
      '- Avg late POs per week: '+avgLate.toFixed(1),
      '- Air freight: '+totAir.toLocaleString()+' units ('+airPct2+'%) | Sea: '+totSea.toLocaleString()+' units',
      '',
      '## Week-by-week',
      weekLines,
      '',
      '## Manufacturer performance',
      supLines,
      '',
      'Generate exactly 5 operational improvement insights for VelOzity warehouse operations team. These insights are about VelOzity internal operations, not blame on manufacturers. Manufacturers are external partners whose patterns affect VelOzity throughput.',
      '',
      'For each insight return a JSON object with these exact fields:',
      '- category: one of "time-to-live", "throughput", or "risk"',
      '- priority: one of "high", "medium", or "low"',
      '- title: short punchy title (max 8 words)',
      '- observation: what the data shows (1-2 sentences, use specific numbers)',
      '- impact: the operational consequence if unaddressed (1 sentence, start with a number or %)',
      '- action: concrete next step for VelOzity ops team (1-2 sentences)',
      '',
      'Respond ONLY with a JSON array of 5 objects. No markdown, no explanation, just the array.',
    ].join('\n');

    const anthropic = getAnthropic();
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1200,
      messages: [
        {
          role: 'user',
          content: [
            {
              type: 'text',
              text: dataPrompt,
              cache_control: { type: 'ephemeral' }
            }
          ]
        }
      ],
    }, {
      headers: { 'anthropic-beta': 'prompt-caching-2024-07-31' }
    });

    const raw = response.content.find(b=>b.type==='text')?.text || '[]';
    let insights;
    try {
      const clean = raw.replace(/^```json\s*/,'').replace(/```\s*$/,'').trim();
      insights = JSON.parse(clean);
    } catch(e) {
      console.error('[/ai/pulse] JSON parse failed:', raw.slice(0,200));
      return res.status(500).json({ error: 'Failed to parse AI response' });
    }

    res.json({ insights });

  } catch(e) {
    console.error('[/ai/pulse]', e);
    if (e.message?.includes('ANTHROPIC_API_KEY')) {
      return res.status(503).json({ error: 'AI service not configured' });
    }
    res.status(500).json({ error: 'AI service error: '+(e.message||e) });
  }
});


// ===== Executive Summary API =====
// GET /exec/summary?from=2026-01-06&to=2026-04-14&facility=VOZ_UL
// Returns per-week aggregated data for the Executive dashboard.
// Joins: plans + records + receiving + bins + flow_week
app.get('/exec/summary',
  authenticateRequest,
  auditLog('view_exec_summary'),
  (req, res) => {
  const facility = normFacility(req.query.facility);
  if (!facility) return res.status(400).json({ error: 'facility required' });

  const fromRaw = String(req.query.from || '').trim();
  const toRaw   = String(req.query.to   || '').trim();
  if (!fromRaw || !toRaw) return res.status(400).json({ error: 'from and to required' });

  // Normalise to Monday-aligned week starts within range
  const fromDate = new Date(fromRaw + 'T00:00:00Z');
  const toDate   = new Date(toRaw   + 'T00:00:00Z');
  if (isNaN(fromDate) || isNaN(toDate)) return res.status(400).json({ error: 'invalid date range' });

  // Collect all Monday week-starts in range
  const weeks = [];
  const cursor = new Date(fromDate);
  // Snap to Monday
  const dow = cursor.getUTCDay();
  cursor.setUTCDate(cursor.getUTCDate() + (dow === 0 ? -6 : 1 - dow));
  while (cursor <= toDate) {
    weeks.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 7);
  }
  if (!weeks.length) return res.json({ weeks: [] });

  const result = [];

  for (const ws of weeks) {
    // Week end (Sunday)
    const wsDate = new Date(ws + 'T00:00:00Z');
    const weDate = new Date(wsDate); weDate.setUTCDate(weDate.getUTCDate() + 6);
    const we = weDate.toISOString().slice(0, 10);

    // ── 1. Plan rows (JSON blob) ──
    const planRow = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(ws);
    const planRows = planRow ? (safeJsonParse(planRow.data, []) || []) : [];

    let planned_units = 0;
    let air_units = 0;
    let sea_units = 0;
    const plannedPOSet = new Set();
    const supplierMap = new Map(); // supplier -> { planned, air, sea, pos: Set }

    for (const p of planRows) {
      const qty = Number(p.target_qty || 0) || 0;
      const freight = String(p.freight_type || '').trim().toLowerCase();
      const supplier = String(p.supplier_name || '').trim() || 'Unknown';
      const po = String(p.po_number || '').trim();

      planned_units += qty;
      if (freight === 'air') air_units += qty;
      else sea_units += qty;
      if (po) plannedPOSet.add(po);

      if (!supplierMap.has(supplier)) supplierMap.set(supplier, { planned: 0, applied: 0, air: 0, sea: 0, pos: new Set(), receivedAt: null, vasAt: null });
      const s = supplierMap.get(supplier);
      s.planned += qty;
      if (freight === 'air') s.air += qty; else s.sea += qty;
      if (po) s.pos.add(po);
    }

    // ── 2. Records (applied units + VAS completion dates) ──
    const recRows = db.prepare(
      `SELECT po_number, mobile_bin, completed_at FROM records WHERE date_local >= ? AND date_local <= ? AND status = 'complete'`
    ).all(ws, we);

    let applied_units = 0;
    const appliedPOSet = new Set();
    const vasDateByPO = new Map();   // po -> latest completed_at
    const mobileBinSet = new Set();  // unique bins = cartons out

    for (const r of recRows) {
      const po = String(r.po_number || '').trim();
      applied_units++;
      if (po) {
        appliedPOSet.add(po);
        const prev = vasDateByPO.get(po) || '';
        const cur  = String(r.completed_at || '').trim();
        if (cur > prev) vasDateByPO.set(po, cur);
      }
      const mb = String(r.mobile_bin || '').trim();
      if (mb) mobileBinSet.add(mb);
    }

    // Per-supplier applied units
    for (const p of planRows) {
      const supplier = String(p.supplier_name || '').trim() || 'Unknown';
      const po = String(p.po_number || '').trim();
      if (!po || !supplierMap.has(supplier)) continue;
      // Count applied from records for this PO
      const poApplied = recRows.filter(r => String(r.po_number||'').trim() === po).length;
      supplierMap.get(supplier).applied += poApplied;
    }

    // ── 3. Receiving rows ──
    const recvRows = db.prepare(
      `SELECT po_number, supplier_name, received_at_local, cartons_received FROM receiving WHERE week_start = ?`
    ).all(ws);

    const receivedPOSet = new Set();
    const recvDateByPO = new Map(); // po -> received_at_local
    let cartons_in = 0;

    for (const r of recvRows) {
      const po = String(r.po_number || '').trim();
      if (po) {
        receivedPOSet.add(po);
        if (r.received_at_local) recvDateByPO.set(po, String(r.received_at_local));
      }
      cartons_in += Number(r.cartons_received || 0) || 0;
    }

    // Late POs: received but after plan due_date
    let late_pos = 0;
    const dueDateByPO = new Map();
    for (const p of planRows) {
      const po = String(p.po_number || '').trim();
      if (po && p.due_date) dueDateByPO.set(po, String(p.due_date));
    }
    for (const po of receivedPOSet) {
      const recvDate = recvDateByPO.get(po);
      const dueDate = dueDateByPO.get(po);
      if (recvDate && dueDate && recvDate.slice(0, 10) > dueDate.slice(0, 10)) late_pos++;
    }

    // ── 4. Bins (weight) ──
    const binRows = db.prepare(
      `SELECT mobile_bin, weight_kg, total_units FROM bins WHERE week_start = ?`
    ).all(ws);

    let total_weight_kg = 0;
    let bin_count = 0;
    for (const b of binRows) {
      if (b.weight_kg != null) { total_weight_kg += Number(b.weight_kg) || 0; bin_count++; }
    }
    const avg_weight_kg = bin_count > 0 ? Math.round((total_weight_kg / bin_count) * 100) / 100 : 0;

    // ── 5. Flow week (transit dates + containers + last mile) ──
    // Must fetch ALL rows for this week and merge — data is split across multiple
    // facility keys (intl_lanes under one key, intl_weekcontainers under another)
    const allExecFlowRows = db.prepare(
      `SELECT facility, data FROM flow_week WHERE week_start = ?`
    ).all(ws);
    const flowData = {};
    for (const row of allExecFlowRows) {
      const d = safeJsonParse(row.data, {}) || {};
      for (const [k, v] of Object.entries(d)) {
        if (k === 'intl_lanes' && v && typeof v === 'object' && !Array.isArray(v)) {
          flowData.intl_lanes = Object.assign({}, flowData.intl_lanes || {}, v);
        } else {
          flowData[k] = v;
        }
      }
    }

    // ── Flow week: intl_lanes + containers ──
    const intl_lanes = (flowData.intl_lanes && typeof flowData.intl_lanes === 'object') ? flowData.intl_lanes : {};
    const etaDates = [];
    const vasToEtaList = []; // Segment 2: VAS complete → ETA FC

    // Latest VAS completion across all applied POs this week
    const allVasDates = Array.from(vasDateByPO.values()).filter(Boolean).sort();
    const latestVasDate = allVasDates.length ? allVasDates[allVasDates.length - 1] : null;

    for (const [laneKey, manual] of Object.entries(intl_lanes)) {
      if (!manual || typeof manual !== 'object') continue;
      if (manual.eta_fc) {
        etaDates.push(String(manual.eta_fc));
        // Segment 2: VAS complete → ETA FC per lane
        if (latestVasDate) {
          try {
            const vd = new Date(latestVasDate.slice(0, 10) + 'T00:00:00Z');
            const ed = new Date(String(manual.eta_fc).slice(0, 10) + 'T00:00:00Z');
            const days = Math.round((ed - vd) / (1000 * 60 * 60 * 24));
            if (days > 0 && days < 120) vasToEtaList.push(days);
          } catch {}
        }
      }
    }

    // Segment 3: ETA FC → FC Delivery (from last mile container delivery dates)
    const etaToDeliveryList = [];
    const containers = (() => {
      const wc = flowData.intl_weekcontainers;
      if (!wc) return [];
      return Array.isArray(wc) ? wc : (Array.isArray(wc.containers) ? wc.containers : []);
    })();

    // Container utilisation: units per container by size
    const cont20 = [], cont40 = [];
    for (const c of containers) {
      const size = String(c.size_ft || '40').trim();
      // Get POs on this container
      const cPOs = String(c.pos || '').split(',').map(p => p.trim().toUpperCase()).filter(Boolean);
      // Also check lane_keys for POs via plan
      const laneKeys = Array.isArray(c.lane_keys) ? c.lane_keys : [];
      const allPOs = new Set(cPOs);
      // Add POs from plan that match this container's lane keys
      for (const p of planRows) {
        const po = String(p.po_number || '').trim();
        if (!po) continue;
        const supplier = String(p.supplier_name || '').trim();
        const zendesk  = String(p.zendesk_ticket || '').trim();
        const freight  = String(p.freight_type || '').trim().toLowerCase();
        const lk = `${supplier}||${zendesk}||${freight}`;
        // Case-insensitive match — lane keys may use Sea/Air or SEA/AIR
        if (laneKeys.some(k => k.includes(zendesk) && k.toLowerCase().includes(freight))) {
          allPOs.add(po);
        }
      }
      // Sum planned units for these POs
      let units = 0;
      for (const po of allPOs) {
        for (const p of planRows) {
          if (String(p.po_number || '').trim() === po) {
            units += Number(p.target_qty || 0) || 0;
          }
        }
      }
      // Fallback: use applied units from records for these POs
      if (units === 0) {
        for (const po of allPOs) {
          const recCount = recRows.filter(r => String(r.po_number||'').trim() === po).length;
          units += recCount;
        }
      }
      if (size === '20') cont20.push(units);
      else cont40.push(units);
    }

    const avg_units_per_20ft = cont20.length
      ? Math.round(cont20.reduce((a,b)=>a+b,0) / cont20.length)
      : null;
    const avg_units_per_40ft = cont40.length
      ? Math.round(cont40.reduce((a,b)=>a+b,0) / cont40.length)
      : null;
    const count_20ft = cont20.length;
    const count_40ft = cont40.length;

    const etaFcDate = etaDates.sort().pop() || null;
    for (const c of containers) {
      // Last mile delivery date stored as delivery_local or delivered_at
      const delivDate = c.delivery_local || c.delivered_at || c.delivery_date || null;
      if (delivDate && etaFcDate) {
        try {
          const eta = new Date(String(etaFcDate).slice(0, 10) + 'T00:00:00Z');
          const del = new Date(String(delivDate).slice(0, 10) + 'T00:00:00Z');
          const days = Math.round((del - eta) / (1000 * 60 * 60 * 24));
          if (days >= 0 && days < 60) etaToDeliveryList.push(days);
        } catch {}
      }
    }

    // Segment 1: Receiving → VAS complete
    const daysToApplyList = [];
    for (const po of appliedPOSet) {
      const recvDate = recvDateByPO.get(po);
      const vasDate  = vasDateByPO.get(po);
      if (recvDate && vasDate) {
        try {
          const rv = new Date(recvDate.slice(0, 10) + 'T00:00:00Z');
          const vd = new Date(vasDate.slice(0, 10) + 'T00:00:00Z');
          const days = Math.round((vd - rv) / (1000 * 60 * 60 * 24));
          if (days >= 0 && days < 60) daysToApplyList.push(days);
        } catch {}
      }
    }

    const avg_days_to_apply = daysToApplyList.length
      ? Math.round((daysToApplyList.reduce((a, b) => a + b, 0) / daysToApplyList.length) * 10) / 10
      : null;

    const avg_days_vas_to_eta = vasToEtaList.length
      ? Math.round((vasToEtaList.reduce((a, b) => a + b, 0) / vasToEtaList.length) * 10) / 10
      : null;

    const avg_days_eta_to_delivery = etaToDeliveryList.length
      ? Math.round((etaToDeliveryList.reduce((a, b) => a + b, 0) / etaToDeliveryList.length) * 10) / 10
      : null;

    // Total end-to-end days (only if all 3 segments available)
    const avg_days_end_to_end = (avg_days_to_apply != null && avg_days_vas_to_eta != null)
      ? Math.round(((avg_days_to_apply + avg_days_vas_to_eta + (avg_days_eta_to_delivery || 0)) * 10)) / 10
      : null;

    const avg_transit_days = avg_days_vas_to_eta; // keep for backward compat
    const eta_fc = etaFcDate;

    // On-time receiving % this week
    const on_time_receiving_pct = plannedPOSet.size > 0
      ? Math.round(((receivedPOSet.size - late_pos) / plannedPOSet.size) * 100)
      : null;

    // Supplier breakdown
    const suppliers = Array.from(supplierMap.entries()).map(([name, s]) => ({
      supplier: name,
      planned: s.planned,
      applied: s.applied,
      air: s.air,
      sea: s.sea,
      po_count: s.pos.size,
      avg_days_to_apply: null, // computed client-side from per-PO data if needed
    }));

    result.push({
      week_start: ws,
      week_end: we,
      planned_units,
      applied_units,
      planned_pos: plannedPOSet.size,
      received_pos: receivedPOSet.size,
      late_pos,
      on_time_receiving_pct,
      cartons_in,
      cartons_out: mobileBinSet.size,
      avg_weight_kg,
      total_weight_kg: Math.round(total_weight_kg * 100) / 100,
      air_units,
      sea_units,
      avg_days_to_apply,
      avg_days_vas_to_eta,
      avg_days_eta_to_delivery,
      avg_days_end_to_end,
      avg_transit_days,
      eta_fc,
      avg_units_per_20ft,
      avg_units_per_40ft,
      count_20ft,
      count_40ft,
      suppliers,
    });
  }

  return res.json({ facility, from: fromRaw, to: toRaw, weeks: result });
});


// ===== Flow Week API (facility-scoped, week-scoped) =====

// GET /flow/week/:weekStart?facility=LKWF
app.get('/flow/week/:weekStart',
  authenticateRequest,
  auditLog('view_flow'),
  (req, res) => {
  const wsIn = String(req.params.weekStart || '').trim();
  const facility = normFacility(req.query.facility);
  if (!facility) return res.status(400).json({ error: 'facility required' });

  const monday = mondayOfLoose(wsIn);
  if (!monday) return res.status(400).json({ error: 'invalid weekStart' });

  const row = flowWeekGet.get(facility, monday);
  if (!row) return res.json({ facility, week_start: monday, data: null, updated_at: null });

  return res.json({
    facility,
    week_start: monday,
    data: safeJsonParse(row.data, null),
    updated_at: row.updated_at || null
  });
});

// GET /flow/week/:weekStart/all   (full view across facilities)
app.get('/flow/week/:weekStart/all',
  authenticateRequest,
  auditLog('view_flow_all'),
  (req, res) => {
  const wsIn = String(req.params.weekStart || '').trim();
  const monday = mondayOfLoose(wsIn);
  if (!monday) return res.status(400).json({ error: 'invalid weekStart' });

  const rows = flowWeekAllForWeek.all(monday);
  const facilities = {};
  for (const r of rows) {
    facilities[r.facility] = {
      data: safeJsonParse(r.data, null),
      updated_at: r.updated_at || null
    };
  }
  return res.json({ week_start: monday, facilities });
});

// POST /flow/week/:weekStart?facility=LKWF   body: { ...patch }
app.post('/flow/week/:weekStart',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'client', 'api']),
  writeOpLimiter,
  auditLog('edit_flow'),
  (req, res) => {
  const wsIn = String(req.params.weekStart || '').trim();
  const facility = normFacility(req.query.facility);
  if (!facility) return res.status(400).json({ error: 'facility required' });

  const monday = mondayOfLoose(wsIn);
  if (!monday) return res.status(400).json({ error: 'invalid weekStart' });

  const patch = (req.body && typeof req.body === 'object') ? req.body : null;
  if (!patch) return res.status(400).json({ error: 'patch object required' });

  const existingRow = flowWeekGet.get(facility, monday);
  const existing = existingRow ? (safeJsonParse(existingRow.data, {}) || {}) : {};
  const merged = (function mergeFlowWeek(existingObj, patchObj) {
    const out = { ...(existingObj || {}) };
    for (const [k, v] of Object.entries(patchObj || {})) {
      if (k === 'intl_lanes' && v && typeof v === 'object' && !Array.isArray(v)) {
        const prev = (existingObj && existingObj.intl_lanes && typeof existingObj.intl_lanes === 'object' && !Array.isArray(existingObj.intl_lanes))
          ? existingObj.intl_lanes
          : {};
        out.intl_lanes = { ...prev, ...v };
        continue;
      }
      out[k] = v;
    }
    return out;
  })(existing, patch);

  flowWeekUpsert.run(facility, monday, JSON.stringify(merged));

  return res.json({ ok: true, facility, week_start: monday, data: merged });
});



// --- Inline cell patch from Intake table ---
app.patch('/records/:id',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  validateRecordInput,
  auditLog('patch_record'),
  (req, res) => {
  const id = String(req.params.id);
  const { field, value } = req.body || {};
  if (!id || !field) return res.status(400).json({ error: 'id and field required' });

  const allowed = new Set(['date_local','mobile_bin','sscc_label','po_number','sku_code','uid']);
  if (!allowed.has(field)) return res.status(400).json({ error: `Invalid field: ${field}` });

  let row = selectRecordById.get(id);
  let createdNow = false;
  if (!row) {
    db.prepare(`INSERT INTO records(id, date_local, status, sync_state) VALUES(?, ?, 'draft', 'pending')`)
      .run(id, todayChicagoISO());
    row = selectRecordById.get(id);
    createdNow = true;
  }

  const next = { ...row, [field]: String(value ?? '') };
  const completed = isComplete(next);

  db.prepare(`
    UPDATE records SET
      date_local=?, mobile_bin=?, sscc_label=?, po_number=?, sku_code=?, uid=?,
      status=?, completed_at=?, sync_state=? WHERE id=?
  `).run(
    next.date_local || row.date_local || todayChicagoISO(),
    next.mobile_bin ?? row.mobile_bin ?? '',
    next.sscc_label ?? row.sscc_label ?? '',
    next.po_number  ?? row.po_number  ?? '',
    next.sku_code   ?? row.sku_code   ?? '',
    next.uid        ?? row.uid        ?? '',
    completed ? 'complete' : 'draft',
    completed ? new Date().toISOString() : row.completed_at,
    completed ? 'synced' : 'pending',
    id
  );

  const after = selectRecordById.get(id);

  // if just completed, pulse
  if (completed && row.status !== 'complete') emitScan(new Date(after.completed_at));

  // if we created a new shell but it duplicates an existing composite, drop the shell
  if (createdNow && after.po_number && after.sku_code && after.uid) {
    const ex = selectByComposite.get(after.po_number, after.sku_code, after.uid);
    if (ex && ex.id && ex.id !== id) { try { deleteById.run(id); } catch {} }
  }

  return res.json({ ok: true, record: after });
});

// --- Create record (used by UI once a row is complete) ---
app.post('/records',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  validateRecordInput,
  writeOpLimiter,
  auditLog('create_record'),
  (req, res) => {
  const b = req.body || {};
  const rec = {
    id: b.id || randomUUID(),
    date_local: toISODate(b.date_local) || todayChicagoISO(),
    mobile_bin: String(b.mobile_bin ?? ''),
    sscc_label: String(b.sscc_label ?? ''), // optional
    po_number:  String(b.po_number  ?? ''),
    sku_code:   String(b.sku_code   ?? ''),
    uid:        String(b.uid        ?? ''),
    status:     'complete',
    completed_at: new Date().toISOString(),
    sync_state: 'synced'
  };

  if (!rec.date_local || !rec.mobile_bin || !rec.po_number || !rec.sku_code || !rec.uid) {
    return res.status(400).json({ error: 'date_local, mobile_bin, po_number, sku_code, uid are required' });
  }

  try {
    upsertByComposite.run(rec);
    emitScan(new Date(rec.completed_at));
    const row = selectByComposite.get(rec.po_number, rec.sku_code, rec.uid);
    const saved = row ? selectRecordById.get(row.id) : selectRecordById.get(rec.id);
    return res.json({ ok: true, record: saved });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// --- Import (kept for compatibility; UI can still call it if desired) ---
function normalizeUploadRow(row) {
  const norm = {};
  for (const k in row) norm[String(k).toLowerCase().trim()] = row[k];
  const pick = (...names) => {
    for (const n of names) {
      const v = norm[n.toLowerCase().trim()];
      if (v != null && String(v).trim() !== '') return String(v);
    }
    return '';
  };
  return {
    id: randomUUID(),
    date_local: toISODate(pick('date_local', 'date')) || todayChicagoISO(),
    mobile_bin: String(pick('mobile_bin', 'mobile bin (box)') || ''),
    sscc_label: String(pick('sscc_label', 'sscc label (box)', 'sscc') || ''),
    po_number:  String(pick('po_number', 'po_number', 'po', 'po#', 'po number') || ''),
    sku_code:   String(pick('sku_code', 'sku_code', 'sku', 'sku code') || ''),
    uid:        String(pick('uid', 'uid', 'u_id', 'u id') || ''), // verbatim
    status: 'complete',
    completed_at: new Date().toISOString(),
    sync_state: 'synced'
  };
}

app.post('/records/import',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  validateBulkInput,
  uploadLimiter,
  auditLog('import_records'),
  (req, res) => {
  const arr = Array.isArray(req.body) ? req.body : [];
  if (!arr.length) return res.status(400).json({ error: 'array of rows required' });

  try {
    const normalized = arr.map(normalizeUploadRow);

    const payload = [];
    const rejected = [];

    normalized.forEach((r, index) => {
      const missing = [];
      if (!r.date_local) missing.push('date_local');
      if (!r.po_number) missing.push('po_number');
      if (!r.sku_code)  missing.push('sku_code');
      if (!r.uid)       missing.push('uid');

      if (missing.length) {
        rejected.push({
          index,
          po_number: r.po_number,
          sku_code:  r.sku_code,
          uid:       r.uid,
          reason:    'Missing ' + missing.join(', ')
        });
      } else {
        // NOTE: mobile_bin is allowed to be empty on import; can be fixed later in intake UI
        payload.push(r);
      }
    });

    if (!payload.length) {
      return res.json({
        ok:       true,
        inserted: 0,
        total:    arr.length,
        rejected: rejected.length,
        errors:   rejected
      });
    }

    const trx = db.transaction(rows => {
      for (const r of rows) upsertByComposite.run(r);
    });
    trx(payload);

    if (payload.length) {
      emitScan(new Date(payload[payload.length - 1].completed_at));
    }

    return res.json({
      ok:       true,
      inserted: payload.length,
      total:    arr.length,
      rejected: rejected.length,
      errors:   rejected
    });
  } catch (e) {
    console.error('Import failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


// --- Fetch records ---
app.get('/records',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_records'),
  (req, res) => {
  // Accept either from/to OR weekStart/weekEnd (we translate weekStart/weekEnd to from/to)
  const weekStart = req.query.weekStart ? String(req.query.weekStart) : '';
  const weekEnd   = req.query.weekEnd   ? String(req.query.weekEnd)   : '';
  const fromQ     = req.query.from      ? String(req.query.from)      : '';
  const toQ       = req.query.to        ? String(req.query.to)        : '';

  const fromRaw = fromQ || weekStart || '';
  const toRaw   = toQ   || weekEnd   || '';

  // Normalize ISO timestamps (e.g. 2026-02-02T00:00:00.000Z) to YYYY-MM-DD for date_local filtering.
  const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
  const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;
  const status = req.query.status ? String(req.query.status) : '';
  const limit  = req.query.limit  ? Number(req.query.limit)  : undefined;

  const params = [];
  let sql = 'SELECT * FROM records WHERE 1=1';
  if (from)   { sql += ' AND date_local >= ?'; params.push(from); }
  if (to)     { sql += ' AND date_local <= ?'; params.push(to); }
  if (status) { sql += ' AND status = ?';      params.push(status); }
  sql += ' ORDER BY completed_at DESC';
  if (limit)  { sql += ' LIMIT ?';             params.push(limit); }

  const rows = db.prepare(sql).all(...params);
  res.json({ records: rows });
});


// --- Paginated records (cursor-based; for drilldowns only) ---
// Cursor format: "<completed_at>|<id>" (both URL-encoded by the client). Results are ordered DESC.
app.get('/records/page',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_records_page'),
  (req, res) => {
  try {
    const weekStart = req.query.weekStart ? String(req.query.weekStart) : '';
    const weekEnd   = req.query.weekEnd   ? String(req.query.weekEnd)   : '';
    const fromQ     = req.query.from      ? String(req.query.from)      : '';
    const toQ       = req.query.to        ? String(req.query.to)        : '';

    const fromRaw = fromQ || weekStart || '';
    const toRaw   = toQ   || weekEnd   || '';

    const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
    const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;
    const status = req.query.status ? String(req.query.status) : '';

    const limitRaw = req.query.limit ? Number(req.query.limit) : 5000;
    const limit = Math.max(1, Math.min(20000, Number.isFinite(limitRaw) ? limitRaw : 5000));

    const cursor = req.query.cursor ? String(req.query.cursor) : '';
    let cursorCompletedAt = '';
    let cursorId = '';
    if (cursor.includes('|')) {
      const parts = cursor.split('|');
      cursorCompletedAt = parts[0] || '';
      cursorId = parts[1] || '';
    }

    const params = [];
    let sql = 'SELECT * FROM records WHERE 1=1';
    if (from)   { sql += ' AND date_local >= ?'; params.push(from); }
    if (to)     { sql += ' AND date_local <= ?'; params.push(to); }
    if (status) { sql += ' AND status = ?';      params.push(status); }

    // keyset pagination: (completed_at, id) DESC
    if (cursorCompletedAt && cursorId) {
      sql += ' AND (completed_at < ? OR (completed_at = ? AND id < ?))';
      params.push(cursorCompletedAt, cursorCompletedAt, cursorId);
    } else if (cursorCompletedAt) {
      sql += ' AND completed_at < ?';
      params.push(cursorCompletedAt);
    }

    sql += ' ORDER BY completed_at DESC, id DESC';
    sql += ' LIMIT ?';
    params.push(limit);

    const rows = db.prepare(sql).all(...params);

    let next_cursor = null;
    if (rows.length) {
      const last = rows[rows.length - 1];
      if (last?.completed_at && last?.id) {
        next_cursor = `${last.completed_at}|${last.id}`;
      }
    }

    return res.json({ records: rows, next_cursor });
  } catch (e) {
    console.error('GET /records/page failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


// --- Ops quick stats (tiny payload; safe to call frequently) ---
app.get('/summary/ops',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_summary_ops'),
  (req, res) => {
  try {
    const now = new Date();
    const nowISO = now.toISOString();
    const hourAgoISO = new Date(now.getTime() - 3600e3).toISOString();
    const halfAgoISO = new Date(now.getTime() - 1800e3).toISOString();
    const todayYMD = todayChicagoISO();

    const scansToday = db.prepare(
      `SELECT COUNT(*) AS n FROM records WHERE status='complete' AND date_local = ?`
    ).get(todayYMD)?.n || 0;

    const lastHour = db.prepare(
      `SELECT COUNT(*) AS n FROM records WHERE status='complete' AND completed_at >= ?`
    ).get(hourAgoISO)?.n || 0;

    const last30 = db.prepare(
      `SELECT COUNT(*) AS n FROM records WHERE status='complete' AND completed_at >= ?`
    ).get(halfAgoISO)?.n || 0;

    const drafts = db.prepare(
      `SELECT COUNT(*) AS n FROM records WHERE status <> 'complete'`
    ).get()?.n || 0;

    // Duplicate pairs among completed rows for today (sku_code + uid)
    const dupeRow = db.prepare(
      `SELECT
        (COUNT(*) - COUNT(DISTINCT COALESCE(TRIM(sku_code),'') || '|' || COALESCE(TRIM(uid),''))) AS dupes
       FROM records
       WHERE status='complete' AND date_local = ? AND TRIM(COALESCE(sku_code,''))<>'' AND TRIM(COALESCE(uid,''))<>''`
    ).get(todayYMD);
    const dupes = Math.max(0, Number(dupeRow?.dupes || 0));

    const syncRows = db.prepare(
      `SELECT COALESCE(sync_state,'unknown') AS sync_state, COUNT(*) AS n
       FROM records
       GROUP BY COALESCE(sync_state,'unknown')`
    ).all();
    const sync_counts = {};
    for (const r of syncRows) sync_counts[r.sync_state] = Number(r.n || 0);

    const lastCompleted = db.prepare(
      `SELECT MAX(completed_at) AS ts FROM records WHERE status='complete'`
    ).get()?.ts || null;

    return res.json({
      now: nowISO,
      today: todayYMD,
      scans_today: Number(scansToday),
      last_hour: Number(lastHour),
      last_30m: Number(last30),
      drafts: Number(drafts),
      dupes: Number(dupes),
      sync_counts,
      last_completed_at: lastCompleted
    });
  } catch (e) {
    console.error('GET /summary/ops failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});



// --- Fetch records summary (totals + trends; avoids pulling huge record sets to client) ---
app.get('/records/summary',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_records_summary'),
  (req, res) => {
  try {
    // Accept either from/to OR weekStart/weekEnd (same pattern as /records)
    const weekStart = req.query.weekStart ? String(req.query.weekStart) : '';
    const weekEnd   = req.query.weekEnd   ? String(req.query.weekEnd)   : '';
    const fromQ     = req.query.from      ? String(req.query.from)      : '';
    const toQ       = req.query.to        ? String(req.query.to)        : '';

    const fromRaw = fromQ || weekStart || '';
    const toRaw   = toQ   || weekEnd   || '';

    // Normalize ISO timestamps (e.g. 2026-02-02T00:00:00.000Z) to YYYY-MM-DD for date_local filtering.
    const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
    const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;

    const status = req.query.status ? String(req.query.status) : 'complete';

    // cache
    const key = _summaryKey({ from, to, status });
    const cached = _summaryCache.get(key);
    if (cached && (Date.now() - cached.ts) < SUMMARY_TTL_MS) {
      return res.json(cached.data);
    }

    const params = [];
    let where = 'WHERE 1=1';
    if (from)   { where += ' AND date_local >= ?'; params.push(from); }
    if (to)     { where += ' AND date_local <= ?'; params.push(to); }
    if (status) { where += ' AND status = ?';      params.push(status); }

    // total units (count of records)
    const totalRow = db.prepare(`
      SELECT COUNT(*) AS total_units
      FROM records
      ${where}
    `).get(...params);

    // trend: units by day
    const byDayRows = db.prepare(`
      SELECT date_local AS ymd, COUNT(*) AS units
      FROM records
      ${where}
      GROUP BY date_local
      ORDER BY date_local
    `).all(...params);

    // optional: PO-level totals + cartons_out (= distinct mobile_bin)
    const byPoRows = db.prepare(`
      SELECT
        po_number AS po,
        COUNT(*) AS units,
        COUNT(DISTINCT NULLIF(TRIM(mobile_bin), '')) AS cartons_out
      FROM records
      ${where}
        AND TRIM(COALESCE(po_number,'')) <> ''
      GROUP BY po_number
      ORDER BY po_number
    `).all(...params);

    const data = {
      from: from || null,
      to: to || null,
      status,
      total_units: Number(totalRow?.total_units || 0),
      by_day: byDayRows.map(r => ({ ymd: r.ymd, units: Number(r.units || 0) })),
      by_po: byPoRows.map(r => ({ po: r.po, units: Number(r.units || 0), cartons_out: Number(r.cartons_out || 0) }))
    };

    _summaryCache.set(key, { ts: Date.now(), data });
    return res.json(data);
  } catch (e) {
    console.error('GET /records/summary failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


// --- Summary: PO+SKU rollup (for discrepancies without pulling raw records) ---
app.get('/summary/po_sku',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_summary_po_sku'),
  (req, res) => {
  try {
    const fromRaw = req.query.from ? String(req.query.from) : '';
    const toRaw   = req.query.to   ? String(req.query.to)   : '';
    const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
    const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;
    const status = req.query.status ? String(req.query.status) : 'complete';

    const params = [];
    let where = 'WHERE 1=1';
    if (from)   { where += ' AND date_local >= ?'; params.push(from); }
    if (to)     { where += ' AND date_local <= ?'; params.push(to); }
    if (status) { where += ' AND status = ?';      params.push(status); }

    const rows = db.prepare(`
      SELECT
        po_number AS po,
        sku_code  AS sku,
        COUNT(*) AS units
      FROM records
      ${where}
        AND TRIM(COALESCE(po_number,'')) <> ''
        AND TRIM(COALESCE(sku_code,'')) <> ''
      GROUP BY po_number, sku_code
      ORDER BY po_number, sku_code
    `).all(...params);

    return res.json({ from: from || null, to: to || null, status, rows: rows.map(r => ({ po: r.po, sku: r.sku, units: Number(r.units || 0) })) });
  } catch (e) {
    console.error('GET /summary/po_sku failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// --- Summary: SKU rollup ---
app.get('/summary/sku',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_summary_sku'),
  (req, res) => {
  try {
    const fromRaw = req.query.from ? String(req.query.from) : '';
    const toRaw   = req.query.to   ? String(req.query.to)   : '';
    const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
    const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;
    const status = req.query.status ? String(req.query.status) : 'complete';

    const params = [];
    let where = 'WHERE 1=1';
    if (from)   { where += ' AND date_local >= ?'; params.push(from); }
    if (to)     { where += ' AND date_local <= ?'; params.push(to); }
    if (status) { where += ' AND status = ?';      params.push(status); }

    const rows = db.prepare(`
      SELECT sku_code AS sku, COUNT(*) AS units
      FROM records
      ${where}
        AND TRIM(COALESCE(sku_code,'')) <> ''
      GROUP BY sku_code
      ORDER BY sku_code
    `).all(...params);

    return res.json({ from: from || null, to: to || null, status, rows: rows.map(r => ({ sku: r.sku, units: Number(r.units || 0) })) });
  } catch (e) {
    console.error('GET /summary/sku failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


// --- Shipment reports (summary + detail) ---
// Used for Operations downloads without pulling raw record datasets to the browser.
// Week-scoped (weekStart -> weekStart+6). Grouping is derived from the uploaded plan JSON.
function _normStr(v) {
  const s = String(v ?? '').trim();
  return s ? s : '(Unspecified)';
}

function _weekEndISO(ws) {
  const d = new Date(String(ws) + 'T00:00:00');
  d.setDate(d.getDate() + 6);
  return d.toISOString().slice(0, 10);
}

function _getPlanRowsForWeek(ws) {
  const row = db.prepare(`SELECT data FROM plans WHERE week_start = ?`).get(ws);
  if (!row?.data) return [];
  try {
    const parsed = JSON.parse(row.data);
    return Array.isArray(parsed) ? parsed : (Array.isArray(parsed?.rows) ? parsed.rows : []);
  } catch {
    return [];
  }
}

function _getBinsForWeek(ws) {
  return db.prepare(`SELECT week_start, mobile_bin, total_units, weight_kg, date_local FROM bins WHERE week_start = ?`).all(ws);
}

app.get('/summary/shipment_summary',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_summary_shipment'),
  (req, res) => {
  try {
    const ws = String(req.query.weekStart || '').slice(0, 10);
    if (!ws) return res.status(400).json({ error: 'weekStart is required (YYYY-MM-DD)' });
    const we = _weekEndISO(ws);

    const plan = _getPlanRowsForWeek(ws);
    const metaByPO = new Map();
    for (const p of plan) {
      const po = String(p?.po_number || '').trim();
      if (!po || metaByPO.has(po)) continue;
      metaByPO.set(po, {
        supplier: _normStr(p?.supplier_name),
        zendesk: _normStr(p?.zendesk_ticket ?? p?.zendesk_ticket_number ?? p?.zendesk),
        freight: _normStr(p?.freight_type),
        facility: _normStr(p?.facility_name),
      });
    }

    // applied units by PO (week scoped)
    const poUnits = db.prepare(`
      SELECT po_number AS po, COUNT(*) AS units
      FROM records
      WHERE status='complete'
        AND date_local >= ? AND date_local <= ?
        AND TRIM(COALESCE(po_number,'')) <> ''
      GROUP BY po_number
    `).all(ws, we);

    // distinct bins by PO (week scoped)
    const poBins = db.prepare(`
      SELECT po_number AS po, TRIM(mobile_bin) AS mobile_bin
      FROM records
      WHERE status='complete'
        AND date_local >= ? AND date_local <= ?
        AND TRIM(COALESCE(po_number,'')) <> ''
        AND TRIM(COALESCE(mobile_bin,'')) <> ''
      GROUP BY po_number, TRIM(mobile_bin)
    `).all(ws, we);

    const bins = _getBinsForWeek(ws);
    const binWeight = new Map();
    for (const b of bins) {
      const mb = String(b?.mobile_bin ?? '').trim();
      if (!mb) continue;
      binWeight.set(mb, Number(b?.weight_kg || 0) || 0);
    }

    const binsByPO = new Map();
    for (const r of poBins) {
      const po = String(r.po || '').trim();
      const mb = String(r.mobile_bin || '').trim();
      if (!po || !mb) continue;
      if (!binsByPO.has(po)) binsByPO.set(po, new Set());
      binsByPO.get(po).add(mb);
    }

    const groups = new Map(); // gkey -> agg
    const binAssigned = new Map();

    for (const r of poUnits) {
      const po = String(r.po || '').trim();
      if (!po) continue;
      const m = metaByPO.get(po) || { supplier: '(Unspecified)', zendesk: '(Unspecified)', freight: '(Unspecified)', facility: '(Unspecified)' };
      const gkey = `${m.supplier}|||${m.zendesk}|||${m.freight}|||${m.facility}`;
      if (!groups.has(gkey)) {
        groups.set(gkey, {
          'Supplier Name': m.supplier,
          'Zendesk Ticket #': m.zendesk,
          'Freight Type': m.freight,
          'Facility Name': m.facility,
          _poSet: new Set(),
          _binSet: new Set(),
          'Unique PO Count': 0,
          'Total Units Applied': 0,
          'Total Mobile Bins': 0,
          'Gross Weight': 0,
          'CBM': 0,
        });
      }
      const g = groups.get(gkey);
      g['Total Units Applied'] += Number(r.units || 0) || 0;
      g._poSet.add(po);

      const poBinSet = binsByPO.get(po) || new Set();
      for (const mb of poBinSet) {
        g._binSet.add(mb);
        if (!binAssigned.has(mb)) {
          binAssigned.set(mb, gkey);
          g['Gross Weight'] += binWeight.get(mb) || 0;
        }
      }
    }

    const out = [];
    for (const g of groups.values()) {
      const binCount = g._binSet.size;
      out.push({
        'Supplier Name': g['Supplier Name'],
        'Zendesk Ticket #': g['Zendesk Ticket #'],
        'Freight Type': g['Freight Type'],
        'Facility Name': g['Facility Name'],
        'Unique PO Count': g._poSet.size,
        'Total Units Applied': Number(g['Total Units Applied'] || 0),
        'Total Mobile Bins': binCount,
        'Gross Weight': Math.round((Number(g['Gross Weight'] || 0) + Number.EPSILON) * 100) / 100,
        'CBM': Math.round((binCount * 0.046 + Number.EPSILON) * 1000) / 1000,
      });
    }

    out.sort((a, b) =>
      String(a['Supplier Name']).localeCompare(String(b['Supplier Name'])) ||
      String(a['Zendesk Ticket #']).localeCompare(String(b['Zendesk Ticket #'])) ||
      String(a['Freight Type']).localeCompare(String(b['Freight Type'])) ||
      String(a['Facility Name']).localeCompare(String(b['Facility Name']))
    );

    return res.json({ weekStart: ws, weekEnd: we, rows: out });
  } catch (e) {
    console.error('GET /summary/shipment_summary failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get('/summary/shipment_detail',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_summary_shipment_detail'),
  (req, res) => {
  try {
    const ws = String(req.query.weekStart || '').slice(0, 10);
    if (!ws) return res.status(400).json({ error: 'weekStart is required (YYYY-MM-DD)' });
    const we = _weekEndISO(ws);

    const plan = _getPlanRowsForWeek(ws);
    const metaByPO = new Map();
    for (const p of plan) {
      const po = String(p?.po_number || '').trim();
      if (!po || metaByPO.has(po)) continue;
      metaByPO.set(po, {
        supplier: _normStr(p?.supplier_name),
        zendesk: _normStr(p?.zendesk_ticket ?? p?.zendesk_ticket_number ?? p?.zendesk),
        freight: _normStr(p?.freight_type),
        facility: _normStr(p?.facility_name),
      });
    }

    const poUnits = db.prepare(`
      SELECT po_number AS po, COUNT(*) AS units
      FROM records
      WHERE status='complete'
        AND date_local >= ? AND date_local <= ?
        AND TRIM(COALESCE(po_number,'')) <> ''
      GROUP BY po_number
    `).all(ws, we);

    const poBins = db.prepare(`
      SELECT po_number AS po, TRIM(mobile_bin) AS mobile_bin
      FROM records
      WHERE status='complete'
        AND date_local >= ? AND date_local <= ?
        AND TRIM(COALESCE(po_number,'')) <> ''
        AND TRIM(COALESCE(mobile_bin,'')) <> ''
      GROUP BY po_number, TRIM(mobile_bin)
    `).all(ws, we);

    const bins = _getBinsForWeek(ws);
    const binWeight = new Map();
    for (const b of bins) {
      const mb = String(b?.mobile_bin ?? '').trim();
      if (!mb) continue;
      binWeight.set(mb, Number(b?.weight_kg || 0) || 0);
    }

    const binsByPO = new Map();
    for (const r of poBins) {
      const po = String(r.po || '').trim();
      const mb = String(r.mobile_bin || '').trim();
      if (!po || !mb) continue;
      if (!binsByPO.has(po)) binsByPO.set(po, new Set());
      binsByPO.get(po).add(mb);
    }

    const groups = new Map();
    const binAssigned = new Map();

    for (const r of poUnits) {
      const po = String(r.po || '').trim();
      if (!po) continue;
      const m = metaByPO.get(po) || { supplier: '(Unspecified)', zendesk: '(Unspecified)', freight: '(Unspecified)', facility: '(Unspecified)' };
      const gkey = `${m.supplier}|||${m.zendesk}|||${m.freight}|||${m.facility}|||${po}`;

      if (!groups.has(gkey)) {
        groups.set(gkey, {
          'Supplier Name': m.supplier,
          'Zendesk Ticket #': m.zendesk,
          'Freight Type': m.freight,
          'Facility Name': m.facility,
          'PO': po,
          _binSet: new Set(),
          'Total Units Applied': 0,
          'Total Mobile Bins': 0,
          'Gross Weight': 0,
          'CBM': 0,
        });
      }

      const g = groups.get(gkey);
      g['Total Units Applied'] += Number(r.units || 0) || 0;

      const poBinSet = binsByPO.get(po) || new Set();
      for (const mb of poBinSet) {
        g._binSet.add(mb);
        if (!binAssigned.has(mb)) {
          binAssigned.set(mb, gkey);
          g['Gross Weight'] += binWeight.get(mb) || 0;
        }
      }
    }

    const out = [];
    for (const g of groups.values()) {
      const binCount = g._binSet.size;
      out.push({
        'Supplier Name': g['Supplier Name'],
        'Zendesk Ticket #': g['Zendesk Ticket #'],
        'Freight Type': g['Freight Type'],
        'Facility Name': g['Facility Name'],
        'PO': g['PO'],
        'Total Units Applied': Number(g['Total Units Applied'] || 0),
        'Total Mobile Bins': binCount,
        'Gross Weight': Math.round((Number(g['Gross Weight'] || 0) + Number.EPSILON) * 100) / 100,
        'CBM': Math.round((binCount * 0.046 + Number.EPSILON) * 1000) / 1000,
      });
    }

    out.sort((a, b) =>
      String(a['Supplier Name']).localeCompare(String(b['Supplier Name'])) ||
      String(a['Zendesk Ticket #']).localeCompare(String(b['Zendesk Ticket #'])) ||
      String(a['Freight Type']).localeCompare(String(b['Freight Type'])) ||
      String(a['Facility Name']).localeCompare(String(b['Facility Name'])) ||
      String(a['PO']).localeCompare(String(b['PO']))
    );

    return res.json({ weekStart: ws, weekEnd: we, rows: out });
  } catch (e) {
    console.error('GET /summary/shipment_detail failed:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


// --- Export: applied UIDs (CSV stream) ---
// For large weeks, do NOT materialize the full dataset in the browser.
app.get('/export/applied',
  authenticateRequest,
  autoFilterResponse,
  auditLog('export_applied'),
  (req, res) => {
  try {
    const fromRaw = req.query.from ? String(req.query.from) : '';
    const toRaw   = req.query.to   ? String(req.query.to)   : '';
    const from = fromRaw && fromRaw.includes('T') ? fromRaw.slice(0, 10) : fromRaw;
    const to   = toRaw   && toRaw.includes('T')   ? toRaw.slice(0, 10)   : toRaw;
    if (!from || !to) return res.status(400).send('from and to are required (YYYY-MM-DD)');

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="Applied_UIDs_${from}_to_${to}.csv"`);

    // UTF-8 BOM for Excel compatibility
    res.write('\ufeff');
    res.write('Date Applied,Mobile Bin,SSCC Label,PO Number,SKU Code,UID\r\n');

    const stmt = db.prepare(`
      SELECT date_local, mobile_bin, sscc_label, po_number, sku_code, uid
      FROM records
      WHERE status='complete'
        AND date_local >= ? AND date_local <= ?
      ORDER BY date_local, po_number, sku_code
    `);

    const esc = (v) => {
      const s = String(v ?? '');
      return /[",\n\r]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    };

    for (const r of stmt.iterate(from, to)) {
      res.write([
        esc(r.date_local),
        esc(r.mobile_bin),
        esc(r.sscc_label),
        esc(r.po_number),
        esc(r.sku_code),
        esc(r.uid)
      ].join(',') + '\r\n');
    }
    return res.end();
  } catch (e) {
    console.error('GET /export/applied failed:', e);
    return res.status(500).send(String(e?.message || e));
  }
});
// --- Export XLSX ---
app.get('/export/xlsx',
  authenticateRequest,
  autoFilterResponse,
  auditLog('export_xlsx'),
  async (req, res) => {
  const date = String(req.query.date || todayChicagoISO());
  const rows = db.prepare('SELECT * FROM records WHERE date_local = ? ORDER BY completed_at DESC').all(date);
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('UIDs');
  ws.columns = [
    { header: 'Date', key: 'date_local', width: 12 },
    { header: 'Mobile Bin (BOX)', key: 'mobile_bin', width: 16 },
    { header: 'SSCC Label (BOX)', key: 'sscc_label', width: 18 },
    { header: 'PO_Number', key: 'po_number', width: 14 },
    { header: 'SKU_Code', key: 'sku_code', width: 14 },
    { header: 'UID', key: 'uid', width: 22 },
    { header: 'Status', key: 'status', width: 10 },
    { header: 'Completed At', key: 'completed_at', width: 22 },
  ];
  rows.forEach(r => ws.addRow(r));
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="uids_${date}.xlsx"`);
  await wb.xlsx.write(res);
  res.end();
});

// --- Deletions ---
app.delete('/records',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  validateBulkInput,
  auditLog('bulk_delete_records'),
  (req, res) => {
  const uid = String(req.query.uid || '').trim();
  const sku = String(req.query.sku_code || '').trim();

  if (!uid) return res.status(400).json({ error: 'uid required' });

  const info = sku
    ? deleteBySkuUid.run(uid, sku)  // original behavior
    : deleteByUid.run(uid);         // NEW: delete all rows with this UID

  return res.json({ ok: true, deleted: info.changes });
});

app.post('/records/delete',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  validateBulkInput,
  auditLog('delete_records'),
  (req, res) => {
  const input = req.body;

  // Normalize input to a list of objects with { uid, sku_code? }
  let items = [];
  if (Array.isArray(input)) {
    items = input.map(x => {
      if (typeof x === 'string') return { uid: String(x).trim(), sku_code: '' };
      return { uid: String(x?.uid || '').trim(), sku_code: String(x?.sku_code || '').trim() };
    });
  } else if (input && typeof input === 'object') {
    items = [{ uid: String(input.uid || '').trim(), sku_code: String(input.sku_code || '').trim() }];
  }

  if (!items.length) {
    return res.status(400).json({ error: 'Body must be array or object containing uid (and optional sku_code)' });
  }

  const results = [];
  const trx = db.transaction(list => {
    for (const it of list) {
      const uid = it.uid;
      const sku = it.sku_code;

      if (!uid) {
        results.push({ uid, sku_code: sku, deleted: 0, error: 'missing uid' });
        continue;
      }

      const info = sku
        ? deleteBySkuUid.run(uid, sku) // original precise delete
        : deleteByUid.run(uid);        // NEW: delete all rows with this UID

      results.push({ uid, sku_code: sku, deleted: info.changes });
    }
  });

  try { trx(items); }
  catch (e) { return res.status(500).json({ error: String(e?.message || e) }); }

  const total = results.reduce((s, r) => s + (r.deleted || 0), 0);
  return res.json({ ok: true, total_deleted: total, results });
});

// Simple helper to compute Monday for a given date (kept consistent with your existing mondayOf)
function mondayOfLoose(ymd) {
  if (!ymd) return '';
  try {
    const d = new Date(String(ymd).trim() + 'T00:00:00Z');
    const day = d.getUTCDay();               // 0..6, Sunday=0
    const diff = (day === 0 ? -6 : (1 - day));
    d.setUTCDate(d.getUTCDate() + diff);
    return d.toISOString().slice(0,10);
  } catch { return ''; }
}


// ---- Flow week helpers / statements ----
function normFacility(v) {
  return String(v || '').trim();
}
function safeJsonParse(s, fallback) {
  try { return JSON.parse(s); } catch { return fallback; }
}

const flowWeekGet = db.prepare(`
  SELECT data, updated_at
  FROM flow_week
  WHERE facility = ? AND week_start = ?
`);

const flowWeekUpsert = db.prepare(`
  INSERT INTO flow_week(facility, week_start, data, updated_at)
  VALUES (?, ?, ?, datetime('now'))
  ON CONFLICT(facility, week_start) DO UPDATE SET
    data = excluded.data,
    updated_at = excluded.updated_at
`);

const flowWeekAllForWeek = db.prepare(`
  SELECT facility, data, updated_at
  FROM flow_week
  WHERE week_start = ?
  ORDER BY facility
`);

/* ===== BEGIN: /plan?weekStart=YYYY-MM-DD alias =====
   Returns the same payload as GET /plan/weeks/:mondayISO
   (works for /api/plan too thanks to the /api alias above)
*/
app.get('/plan',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_plan'),
  (req, res) => {
  const ws = String(req.query.weekStart || req.query.ws || '').trim();
  if (!ws) return res.status(400).json({ error: 'weekStart required' });
  const monday = mondayOfLoose(ws);
  if (!monday) return res.status(400).json({ error: 'invalid weekStart' });

  const row = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(monday);
  if (!row) return res.json([]);
  try { return res.json(JSON.parse(row.data) || []); }
  catch { return res.json([]); }
});
/* ===== END: /plan alias ===== */


// ---------- Weekly Plan API (kept) ----------
function normalizePlanArray(body, fallbackStart) {
  if (!Array.isArray(body)) return [];
  const norm = body.map(r => {
    // Core fields
    const item = {
      po_number:  String(r?.po_number ?? '').trim(),
      sku_code:   String(r?.sku_code  ?? '').trim(),
      start_date: String(r?.start_date ?? '').trim() || fallbackStart || '',
      due_date:   String(r?.due_date   ?? '').trim(),
      target_qty: Number(r?.target_qty ?? 0) || 0,
    };

    // Optional fields — only include if present
    const optionals = [
      'style', 'vendor_sku', 'supplier_name', 'supplier_contact', 
      'supplier_contact_email', 'supplier_contact_phone',
      'vendor_code', 'vendor_item_no', 'facility_name',
      'freight_type', 'zendesk_ticket', 'item_description', 'item_color',
      'item_size', 'department_code', 'brand_name', 'season', 'ean',
      'cost', 'priority', 'notes',
    ];
    for (const key of optionals) {
      if (r?.[key] !== undefined && r[key] !== null && r[key] !== '') {
        item[key] = key === 'cost' ? (Number(r[key]) || 0) : String(r[key]).trim();
      }
    }

    return item;
  }).filter(r => r.po_number && r.sku_code);  // due_date is storage-only, not required

  return norm;
}

app.get('/plan/weeks/:mondayISO',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_plan_week'),
  (req, res) => {
  const monday = String(req.params.mondayISO);
  const row = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(monday);
  if (!row) return res.json([]);
  try {
    return res.json(JSON.parse(row.data) || []);
  } catch {
    return res.json([]);
  }
});

app.put('/plan/weeks/:mondayISO',
  authenticateRequest,
  requireRole(['admin', 'client', 'api']),
  uploadLimiter,
  auditLog('upload_plan'),
  (req, res) => {
  const monday = String(req.params.mondayISO);
  const arr = normalizePlanArray(req.body, monday);
  db.prepare(`
    INSERT INTO plans(week_start, data, updated_at)
    VALUES(?, ?, datetime('now'))
    ON CONFLICT(week_start) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
  `).run(monday, JSON.stringify(arr));
  return res.json(arr);
});

app.post('/plan/weeks/:mondayISO/zero',
  authenticateRequest,
  requireRole(['admin', 'client', 'api']),
  auditLog('zero_plan'),
  (req, res) => {
  const monday = String(req.params.mondayISO);
  db.prepare(`
    INSERT INTO plans(week_start, data, updated_at)
    VALUES(?, '[]', datetime('now'))
    ON CONFLICT(week_start) DO UPDATE SET data='[]', updated_at=datetime('now')
  `).run(monday);
  return res.json({ ok: true, week_start: monday, rows: 0 });
});

app.get('/plan/weeks',
  authenticateRequest,
  auditLog('list_plan_weeks'),
  (req, res) => {
  const rows = db.prepare(`SELECT week_start, updated_at FROM plans ORDER BY week_start DESC LIMIT 52`).all();
  res.json(rows);
});

// --- bins.routes.js ---
const binsRouter = express.Router();

// Store: use your DB (SQL/NoSQL). Here we assume a generic DAL with upsertMany/getByWeek.
// --- Bins DAL (SQLite, inline) ---
db.exec(`
CREATE TABLE IF NOT EXISTS bins (
  week_start  TEXT NOT NULL,
  mobile_bin  TEXT NOT NULL,
  total_units INTEGER,
  weight_kg   REAL,
  date_local  TEXT,
  PRIMARY KEY (week_start, mobile_bin)
);
CREATE INDEX IF NOT EXISTS idx_bins_week ON bins(week_start);
`);

const Bins = {
  upsertMany: db.transaction((rows) => {
    const stmt = db.prepare(`
      INSERT INTO bins (week_start, mobile_bin, total_units, weight_kg, date_local)
      VALUES (@week_start, @mobile_bin, @total_units, @weight_kg, @date_local)
      ON CONFLICT(week_start, mobile_bin) DO UPDATE SET
        total_units = excluded.total_units,
        weight_kg   = excluded.weight_kg,
        date_local  = excluded.date_local
    `);
    let n = 0;
    for (const r of rows) { stmt.run(r); n++; }
    return n;
  }),
  getByWeek: (ws) => {
    return db.prepare(`
      SELECT week_start, mobile_bin, total_units, weight_kg, date_local
      FROM bins
      WHERE week_start = ?
      ORDER BY mobile_bin
    `).all(ws);
  }
};

// Helper: Monday anchor computed in business TZ on the server if you store server-side
function mondayOf(ymd) {
  const d = new Date(ymd + 'T00:00:00Z');
  const day = d.getUTCDay();
  const diff = (day === 0 ? -6 : (1 - day));
  d.setUTCDate(d.getUTCDate() + diff);
  return d.toISOString().slice(0,10);
}

// PUT /bins/weeks/:ws    body: [{mobile_bin, total_units?, weight_kg?, date_local?}, ...]
binsRouter.put('/weeks/:ws',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  writeOpLimiter,
  validateBulkInput,
  auditLog('edit_bins'),
  async (req, res) => {
  try {
    const ws = req.params.ws; // YYYY-MM-DD (business Monday from client)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) return res.status(400).send('Invalid week start');

    const rows = Array.isArray(req.body) ? req.body : [];
    if (!rows.length) return res.json({ ok: true, upserted: 0 });

    const clean = [];
    const seen = new Set();
    const errors = [];

    for (const r of rows) {
      const bin = String(r.mobile_bin || '').trim();
      const units = (r.total_units == null || r.total_units === '') ? null : Number(r.total_units);
      const weight = (r.weight_kg == null || r.weight_kg === '') ? null : Number(r.weight_kg);
      const dateLocal = String(r.date_local || ws);

      if (!bin) { errors.push({row:r, reason:'missing mobile_bin'}); continue; }
      if (units != null && (!Number.isFinite(units) || units < 0)) { errors.push({row:r, reason:'invalid total_units'}); continue; }
      if (weight != null && (!Number.isFinite(weight) || weight < 0)) { errors.push({row:r, reason:'invalid weight_kg'}); continue; }

      // de-dupe per (ws,bin). If multiple entries present, keep last one.
      const key = ws + '|' + bin;
      if (seen.has(key)) clean.pop();
      seen.add(key);

      clean.push({
        week_start: ws,
        mobile_bin: bin,
        total_units: units,
        weight_kg: weight,
        date_local: dateLocal
      });
    }

    if (!clean.length) return res.status(400).json({ ok:false, errors });

    const upserted = await Bins.upsertMany(clean); // implement in DAL
    return res.json({ ok:true, upserted, rejected: errors.length, errors });
  } catch (e) {
    console.error(e);
    return res.status(500).send('Failed to upsert bins');
  }
});

// GET /bins/weeks/:ws
// Returns bins enriched with po_number and supplier_name by joining against
// the records table (UID scans) and the stored plan (for supplier lookup).
// This is required so the client can build the Mobile Bin Report with PO/Supplier
// columns, and so autoFilterResponse can correctly scope data for supplier/client roles.
binsRouter.get('/weeks/:ws',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_bins'),
  async (req, res) => {
  try {
    const ws = req.params.ws;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) return res.status(400).send('Invalid week start');

    const bins = await Bins.getByWeek(ws); // [{mobile_bin, total_units, weight_kg, date_local, week_start}]
    if (!bins.length) return res.json([]);

    const we = _weekEndISO(ws);

    // Join: for each mobile_bin find the PO(s) it was associated with in records this week.
    // Use the PRIMARY po_number (most scans wins) to keep one row per bin.
    const binPORows = db.prepare(`
      SELECT
        TRIM(mobile_bin) AS mobile_bin,
        po_number,
        COUNT(*) AS scan_count
      FROM records
      WHERE date_local >= ? AND date_local <= ?
        AND TRIM(COALESCE(mobile_bin, '')) <> ''
        AND TRIM(COALESCE(po_number, '')) <> ''
      GROUP BY TRIM(mobile_bin), po_number
    `).all(ws, we);

    // Build map: mobile_bin -> po_number (highest scan_count wins)
    const binToPO = new Map();
    for (const r of binPORows) {
      const mb = String(r.mobile_bin || '').trim();
      if (!mb) continue;
      const existing = binToPO.get(mb);
      if (!existing || r.scan_count > existing.scan_count) {
        binToPO.set(mb, { po_number: String(r.po_number || '').trim(), scan_count: r.scan_count });
      }
    }

    // Build supplier lookup from the week's plan
    const planRows = _getPlanRowsForWeek(ws);
    const poToSupplier = new Map();
    for (const p of planRows) {
      const po = String(p && p.po_number || '').trim();
      if (!po || poToSupplier.has(po)) continue;
      poToSupplier.set(po, String(p && (p.supplier_name || p.supplier) || '').trim());
    }

    // Enrich each bin row with po_number and supplier_name
    const enriched = bins.map(b => {
      const mb = String(b.mobile_bin || '').trim();
      const poEntry = binToPO.get(mb);
      const po_number = poEntry ? poEntry.po_number : '';
      const supplier_name = po_number ? (poToSupplier.get(po_number) || '') : '';
      return Object.assign({}, b, { po_number: po_number, supplier_name: supplier_name });
    });

    return res.json(enriched);
  } catch (e) {
    console.error('GET /bins/weeks failed:', e);
    return res.status(500).send('Failed to fetch bins');
  }
});

app.use('/bins', binsRouter);

const receivingRouter = express.Router();

function normalizeReceivingArray(body, ws) {
  if (!Array.isArray(body)) return [];
  return body.map(r => ({
    week_start: ws,
    po_number: String(r?.po_number ?? r?.po ?? '').trim(),
    supplier_name: r?.supplier_name ? String(r.supplier_name).trim() : (r?.supplier ? String(r.supplier).trim() : ''),
    facility_name: r?.facility_name ? String(r.facility_name).trim() : (r?.facility ? String(r.facility).trim() : ''),
    received_at_utc: r?.received_at_utc ? String(r.received_at_utc).trim() : '',
    received_at_local: r?.received_at_local ? String(r.received_at_local).trim() : '',
    received_tz: r?.received_tz ? String(r.received_tz).trim() : '',
    cartons_received: Number(r?.cartons_received ?? r?.cartons_in ?? 0) || 0,
    cartons_damaged: Number(r?.cartons_damaged ?? r?.damaged ?? 0) || 0,
    cartons_noncompliant: Number(r?.cartons_noncompliant ?? r?.noncompliant ?? r?.non_compliant ?? 0) || 0,
    cartons_replaced: Number(r?.cartons_replaced ?? r?.replaced ?? 0) || 0,
    updated_at: new Date().toISOString(),
  })).filter(x => x.po_number);
}

// GET /receiving/weeks/:ws
receivingRouter.get('/weeks/:ws',
  authenticateRequest,
  autoFilterResponse,
  auditLog('view_receiving'),
  (req, res) => {
  const ws = req.params.ws;
  const rows = db.prepare(`SELECT * FROM receiving WHERE week_start=? ORDER BY supplier_name, po_number`).all(ws);
  res.json(rows);
});

// PUT /receiving/weeks/:ws  (UPSERT array)
receivingRouter.put('/weeks/:ws',
  authenticateRequest,
  requireRole(['admin', 'supplier', 'api']),
  writeOpLimiter,
  validateBulkInput,
  auditLog('edit_receiving'),
  (req, res) => {
  const ws = req.params.ws;
  const rows = normalizeReceivingArray(req.body, ws);

  const stmt = db.prepare(`
    INSERT INTO receiving(
      week_start, po_number, supplier_name, facility_name,
      received_at_utc, received_at_local, received_tz,
      cartons_received, cartons_damaged, cartons_noncompliant, cartons_replaced,
      updated_at
    ) VALUES (
      @week_start, @po_number, @supplier_name, @facility_name,
      @received_at_utc, @received_at_local, @received_tz,
      @cartons_received, @cartons_damaged, @cartons_noncompliant, @cartons_replaced,
      @updated_at
    )
    ON CONFLICT(week_start, po_number) DO UPDATE SET
      supplier_name=excluded.supplier_name,
      facility_name=excluded.facility_name,
      received_at_utc=excluded.received_at_utc,
      received_at_local=excluded.received_at_local,
      received_tz=excluded.received_tz,
      cartons_received=excluded.cartons_received,
      cartons_damaged=excluded.cartons_damaged,
      cartons_noncompliant=excluded.cartons_noncompliant,
      cartons_replaced=excluded.cartons_replaced,
      updated_at=excluded.updated_at
  `);

  const tx = db.transaction((arr) => {
    for (const r of arr) stmt.run(r);
  });

  tx(rows);
  res.json({ ok: true, week_start: ws, rows: rows.length });
});

// Alias GET /receiving?weekStart=YYYY-MM-DD  (like bins/plan)
receivingRouter.get('/',
  authenticateRequest,
  autoFilterResponse,
  auditLog('query_receiving'),
  (req, res) => {
  const ws = String(req.query.weekStart || '').trim();
  if (!ws) return res.status(400).json({ error: 'weekStart required' });
  const rows = db.prepare(`SELECT * FROM receiving WHERE week_start=? ORDER BY supplier_name, po_number`).all(ws);
  res.json(rows);
});

app.use('/receiving', receivingRouter);


/* ===== BEGIN: /bins?weekStart=YYYY-MM-DD alias =====
   Returns the same as GET /bins/weeks/:ws
   (works for /api/bins too thanks to the /api alias above)
*/
app.get('/bins',
  authenticateRequest,
  autoFilterResponse,
  auditLog('query_bins'),
  (req, res) => {
  const ws = String(req.query.weekStart || req.query.ws || '').trim();
  if (!ws) return res.status(400).json({ error: 'weekStart required' });
  const monday = mondayOfLoose(ws);
  if (!monday) return res.status(400).json({ error: 'invalid weekStart' });

  try {
    const rows = Bins.getByWeek(monday);
    return res.json(rows);
  } catch (e) {
    console.error(e);
    return res.status(500).send('Failed to fetch bins');
  }
});
/* ===== END: /bins alias ===== */


// 🔐 AUDIT LOG ENDPOINT (Admin Only)
const { getAuditLogs } = require('./middleware/auditLog');

app.get('/admin/audit-logs',
  authenticateRequest,
  requireRole(['admin']),
  (req, res) => {
    try {
      const filters = {
        userId: req.query.user_id,
        orgId: req.query.org_id,
        action: req.query.action,
        startDate: req.query.start_date,
        endDate: req.query.end_date,
        limit: parseInt(req.query.limit) || 100
      };

      const logs = getAuditLogs(filters);
      res.json(logs);
    } catch (error) {
      console.error('Failed to fetch audit logs:', error);
      res.status(500).json({ error: 'Failed to fetch audit logs' });
    }
  }
);



// ═══════════════════════════════════════════════════════════════
// ── FINANCE MODULE ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════

const { v4: uuidv4 } = require('uuid');

// ── Helper: generate invoice reference number ──
function genInvoiceRef(type, weekStart, existingCount) {
  const d = new Date(weekStart + 'T00:00:00Z');
  const yyyy = d.getUTCFullYear();

  if (type === 'VAS') {
    // INVVAS027_2026 — global counter per year
    const pattern = `INVVAS%_${yyyy}`;
    const existing = db.prepare(
      `SELECT ref_number FROM fin_invoices WHERE type = 'VAS' AND ref_number LIKE ? ORDER BY ref_number DESC LIMIT 1`
    ).get(pattern);
    let seq = 1;
    if (existing) {
      const match = existing.ref_number.match(/INVVAS(\d+)_\d{4}$/);
      if (match) seq = parseInt(match[1]) + 1;
    }
    return `INVVAS${String(seq).padStart(3,'0')}_${yyyy}`;
  }

  // SEA: VOZ_INSD2D_W132026-1  AIR: VOZ_INAD2D_W132026-1
  // Counter resets per week — increment within same week only
  const wk = String(getISOWeek(d)).padStart(2, '0');
  const prefix = type === 'SEA' ? 'VOZ_INSD2D' : 'VOZ_INAD2D';
  const weekCode = `W${wk}${yyyy}`;
  const pattern = `${prefix}_${weekCode}-%`;
  const existing = db.prepare(
    `SELECT ref_number FROM fin_invoices WHERE type = ? AND ref_number LIKE ? ORDER BY ref_number DESC LIMIT 1`
  ).get(type, pattern);
  let seq = 1;
  if (existing) {
    const match = existing.ref_number.match(/-(\d+)$/);
    if (match) seq = parseInt(match[1]) + 1;
  }
  return `${prefix}_${weekCode}-${seq}`;
}
function getISOWeek(d) {
  const tmp = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = tmp.getUTCDay() || 7;
  tmp.setUTCDate(tmp.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(tmp.getUTCFullYear(), 0, 1));
  return Math.ceil((((tmp - yearStart) / 86400000) + 1) / 7);
}

// ── GET /finance/invoices — list invoices ──
app.get('/finance/invoices', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { week_start, type, status } = req.query;
    let where = 'WHERE 1=1';
    const params = [];
    if (week_start) { where += ' AND week_start = ?'; params.push(week_start); }
    if (type)       { where += ' AND type = ?';       params.push(type); }
    if (status)     { where += ' AND status = ?';     params.push(status); }
    const invoices = db.prepare(`SELECT * FROM fin_invoices ${where} ORDER BY week_start DESC, type`).all(...params);
    // Attach line items
    const result = invoices.map(inv => ({
      ...inv,
      lines: db.prepare('SELECT * FROM fin_invoice_lines WHERE invoice_id = ? ORDER BY sort_order').all(inv.id)
    }));
    res.json(result);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── GET /finance/invoices/:id ──
app.get('/finance/invoices/:id', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const inv = db.prepare('SELECT * FROM fin_invoices WHERE id = ?').get(req.params.id);
    if (!inv) return res.status(404).json({ error: 'Not found' });
    inv.lines = db.prepare('SELECT * FROM fin_invoice_lines WHERE invoice_id = ? ORDER BY sort_order').all(inv.id);
    res.json(inv);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── POST /finance/invoices — create invoice ──
app.post('/finance/invoices', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { type, week_start, lines = [], invoice_date, due_date, notes, customs, misc_total, ref_override, status } = req.body;
    if (!type || !week_start) return res.status(400).json({ error: 'type and week_start required' });
    const existing = db.prepare('SELECT COUNT(*) as n FROM fin_invoices WHERE type = ? AND week_start = ?').get(type, week_start);
    const id = uuidv4();
    const ref = (ref_override && ref_override.trim()) ? ref_override.trim() : genInvoiceRef(type, week_start, existing.n);
    // Calculate totals — exclude gst_free lines from taxable subtotal
    const taxableLines = lines.filter(l => !l.gst_free);
    const subtotal = taxableLines.reduce((s, l) => s + (parseFloat(l.total)||0), 0);
    const gst = Math.round(subtotal * 0.10 * 100) / 100;
    const customsAmt = parseFloat(customs) || 0;
    const miscAmt = parseFloat(misc_total) || 0;
    // misc lines are already included in subtotal (taxableLines includes is_misc lines)
    // so total = subtotal + gst + customs only — miscAmt is stored for reference but not added again
    const total = Math.round((subtotal + gst + customsAmt) * 100) / 100;
    const invStatus = status || 'draft';
    db.prepare(`INSERT INTO fin_invoices (id,type,week_start,ref_number,status,invoice_date,due_date,subtotal,gst,customs,misc_total,total,notes)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(id, type, week_start, ref, invStatus, invoice_date||null, due_date||null, subtotal, gst, customsAmt, miscAmt, total, notes||null);
    // Insert lines
    lines.forEach((l, i) => {
      db.prepare(`INSERT INTO fin_invoice_lines (id,invoice_id,sort_order,description,unit_label,rate,quantity,total,gst_free,is_misc)
        VALUES (?,?,?,?,?,?,?,?,?,?)`).run(uuidv4(), id, i, l.description||'', l.unit_label||'', parseFloat(l.rate)||0, parseFloat(l.quantity)||0, parseFloat(l.total)||0, l.gst_free?1:0, l.is_misc?1:0);
    });
    const inv = db.prepare('SELECT * FROM fin_invoices WHERE id = ?').get(id);
    inv.lines = db.prepare('SELECT * FROM fin_invoice_lines WHERE invoice_id = ? ORDER BY sort_order').all(id);
    res.json(inv);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── PATCH /finance/invoices/:id — update invoice ──
app.patch('/finance/invoices/:id', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const inv = db.prepare('SELECT * FROM fin_invoices WHERE id = ?').get(req.params.id);
    if (!inv) return res.status(404).json({ error: 'Not found' });
    const { lines, status, invoice_date, due_date, notes, customs, misc_total, ref_override } = req.body;
    // Update ref_number if override provided
    if (ref_override && ref_override.trim()) {
      db.prepare(`UPDATE fin_invoices SET ref_number=?,updated_at=datetime('now') WHERE id=?`).run(ref_override.trim(), inv.id);
    }
    // Recalculate if lines provided
    if (lines) {
      db.prepare('DELETE FROM fin_invoice_lines WHERE invoice_id = ?').run(inv.id);
      lines.forEach((l, i) => {
        db.prepare(`INSERT INTO fin_invoice_lines (id,invoice_id,sort_order,description,unit_label,rate,quantity,total,gst_free,is_misc)
          VALUES (?,?,?,?,?,?,?,?,?,?)`).run(uuidv4(), inv.id, i, l.description||'', l.unit_label||'', parseFloat(l.rate)||0, parseFloat(l.quantity)||0, parseFloat(l.total)||0, l.gst_free?1:0, l.is_misc?1:0);
      });
      const subtotal = lines.filter(l=>!l.gst_free).reduce((s, l) => s + (parseFloat(l.total)||0), 0);
      const gst = Math.round(subtotal * 0.10 * 100) / 100;
      const customsAmt = customs !== undefined ? parseFloat(customs)||0 : inv.customs;
      const miscAmt = misc_total !== undefined ? parseFloat(misc_total)||0 : inv.misc_total;
      // misc lines already in subtotal — don't add miscAmt to total again
      const total = Math.round((subtotal + gst + customsAmt) * 100) / 100;
      db.prepare(`UPDATE fin_invoices SET subtotal=?,gst=?,customs=?,misc_total=?,total=?,updated_at=datetime('now') WHERE id=?`).run(subtotal, gst, customsAmt, miscAmt, total, inv.id);
    }
    // Always update these fields if provided — use explicit !== undefined check so 'draft' isn't falsy-skipped
    if (status !== undefined)        db.prepare(`UPDATE fin_invoices SET status=?,updated_at=datetime('now') WHERE id=?`).run(status, inv.id);
    if (invoice_date !== undefined)  db.prepare(`UPDATE fin_invoices SET invoice_date=?,updated_at=datetime('now') WHERE id=?`).run(invoice_date, inv.id);
    if (due_date !== undefined)      db.prepare(`UPDATE fin_invoices SET due_date=?,updated_at=datetime('now') WHERE id=?`).run(due_date, inv.id);
    if (notes !== undefined)         db.prepare(`UPDATE fin_invoices SET notes=?,updated_at=datetime('now') WHERE id=?`).run(notes, inv.id);
    if (customs !== undefined && !lines) {
      const cur = db.prepare('SELECT * FROM fin_invoices WHERE id=?').get(inv.id);
      const total = Math.round((cur.subtotal + cur.gst + parseFloat(customs) + cur.misc_total) * 100) / 100;
      db.prepare(`UPDATE fin_invoices SET customs=?,total=?,updated_at=datetime('now') WHERE id=?`).run(parseFloat(customs)||0, total, inv.id);
    }
    const updated = db.prepare('SELECT * FROM fin_invoices WHERE id = ?').get(inv.id);
    updated.lines = db.prepare('SELECT * FROM fin_invoice_lines WHERE invoice_id = ? ORDER BY sort_order').all(inv.id);
    res.json(updated);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── DELETE /finance/invoices/:id ──
app.delete('/finance/invoices/:id', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    db.prepare('DELETE FROM fin_invoice_lines WHERE invoice_id = ?').run(req.params.id);
    db.prepare('DELETE FROM fin_invoices WHERE id = ?').run(req.params.id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── GET /finance/prefill/:type/:week_start — auto-populate invoice data from Pinpoint ──
app.get('/finance/prefill/:type/:week_start', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { type, week_start } = req.params;
    const weDate = new Date(week_start + 'T00:00:00Z'); weDate.setUTCDate(weDate.getUTCDate() + 6);
    const we = weDate.toISOString().slice(0,10);

    if (type === 'VAS') {
      // Units applied this week
      const units = db.prepare(`SELECT COUNT(*) as n FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete'`).get(week_start, we)?.n || 0;
      // Cartons out (distinct mobile bins with status complete)
      const cartonsOut = db.prepare(`SELECT COUNT(DISTINCT NULLIF(TRIM(mobile_bin),'')) as n FROM records WHERE date_local >= ? AND date_local <= ? AND status='complete'`).get(week_start, we)?.n || 0;
      // Cartons in from receiving table
      const recvRows = db.prepare(`SELECT SUM(cartons_received) as total FROM receiving WHERE week_start = ?`).get(week_start);
      const cartonsIn = recvRows?.total || 0;
      const cartonDelta = Math.max(0, cartonsOut - cartonsIn);
      // Standard VAS rates
      const lines = [
        { sort_order:0, description:'VAS Base Processing',         unit_label:'Per Unit',        rate:0.21, quantity:units,              total:Math.round(0.21*units*100)/100,              gst_free:0, is_misc:0 },
        { sort_order:1, description:'Outbound Activities',         unit_label:'Per Unit',        rate:0.05, quantity:units,              total:Math.round(0.05*units*100)/100,              gst_free:0, is_misc:0 },
        { sort_order:2, description:'Additional Labelling',        unit_label:'Per Unit',        rate:0.01, quantity:units*3,            total:Math.round(0.01*units*3*100)/100,            gst_free:0, is_misc:0 },
        { sort_order:3, description:'Polybagging',                 unit_label:'Per Unit',        rate:0.05, quantity:0,                 total:0,                                           gst_free:0, is_misc:0 },
        { sort_order:4, description:'Storage post-processing',     unit_label:'Per Unit Per Day', rate:0.01, quantity:0,                 total:0,                                           gst_free:0, is_misc:0 },
        { sort_order:5, description:'Carton Replacement - labour only', unit_label:'Per Carton', rate:1.10, quantity:cartonDelta*2,      total:Math.round(1.10*cartonDelta*2*100)/100,      gst_free:0, is_misc:0 },
        { sort_order:6, description:'',                            unit_label:'',                rate:0,    quantity:0,                 total:0,                                           gst_free:0, is_misc:1 },
        { sort_order:7, description:'',                            unit_label:'',                rate:0,    quantity:0,                 total:0,                                           gst_free:0, is_misc:1 },
      ];
      const subtotal = lines.slice(0,6).reduce((s,l)=>s+l.total,0);
      const gst = Math.round(subtotal*0.10*100)/100;
      const customs = 0; // VAS has no customs
      const total = Math.round((subtotal+gst+customs)*100)/100;
      return res.json({ type:'VAS', week_start, units, cartonsIn, cartonsOut, cartonDelta, lines, subtotal, gst, customs, total });
    }

    if (type === 'SEA' || type === 'AIR') {
      // Load flow/container data
      const flowRows = db.prepare('SELECT data FROM flow_week WHERE week_start = ?').all(week_start);
      const containers = [];
      for (const row of flowRows) {
        try {
          const d = JSON.parse(row.data || '{}');
          const wc = d.intl_weekcontainers;
          const conts = Array.isArray(wc) ? wc : (Array.isArray(wc?.containers) ? wc.containers : []);
          for (const c of conts) {
            const vesselLower = String(c.vessel||'').toLowerCase();
            const containerLower = String(c.container_id||'').toLowerCase();
            const isAirContainer = c.is_air ||
              vesselLower.includes('air') ||
              containerLower.includes('airway') ||
              containerLower.startsWith('ca') ||  // CA prefix = common air waybill format
              (c.lane_keys||[]).some(k => k.split('||')[2]?.toLowerCase() === 'air');
            if (type === 'SEA' && c.vessel && !isAirContainer) containers.push(c);
            if (type === 'AIR' && isAirContainer) containers.push(c);
          }
        } catch {}
      }
      // Also get intl_lanes for zendesk/supplier info
      const lanes = [];
      for (const row of flowRows) {
        try {
          const d = JSON.parse(row.data || '{}');
          const il = d.intl_lanes || {};
          for (const [key, manual] of Object.entries(il)) {
            const parts = key.split('||');
            lanes.push({ key, supplier: parts[0]||'', zendesk: parts[1]||'', freight: parts[2]||'', manual: manual||{} });
          }
        } catch {}
      }
      if (type === 'SEA') {
        const lines = containers.map((c, i) => ({
          sort_order: i,
          description: `Container ${c.container_id||'—'}`,
          unit_label: c.size_ft ? `${c.size_ft}' HC` : '40\' HC',
          container_id: c.container_id || '',
          container_type: c.size_ft ? `${c.size_ft}' HC` : '40\' HC',
          vessel: c.vessel || '',
          zendesks: c.lane_keys ? c.lane_keys.map(k=>k.split('||')[1]).filter(Boolean) : [],
          rate: 0, // User enters rate per container
          quantity: 1,
          total: 0,
          gst_free: 0,
          is_misc: 0
        }));
        // Add customs lines
        const customsLines = containers.map((c, i) => ({
          sort_order: containers.length + i,
          description: `Customs Clearance - ${c.container_id||'—'}`,
          unit_label: 'Flat Fee per Container',
          container_id: c.container_id || '',
          rate: 158,
          quantity: 1,
          total: 158,
          gst_free: 1,
          is_misc: 0
        }));
        // Misc lines
        lines.push({ sort_order: lines.length + customsLines.length, description:'', unit_label:'', rate:0, quantity:0, total:0, gst_free:0, is_misc:1 });
        lines.push({ sort_order: lines.length + customsLines.length + 1, description:'', unit_label:'', rate:0, quantity:0, total:0, gst_free:0, is_misc:1 });
        const customs = customsLines.reduce((s,l)=>s+l.total, 0);
        return res.json({ type:'SEA', week_start, containers, lanes, lines: [...lines, ...customsLines], customs, subtotal:0, gst:0, total:customs });
      }
      if (type === 'AIR') {
        // Group by zendesk
        const airLanes = lanes.filter(l => l.freight.toLowerCase() === 'air');
        const lines = airLanes.map((l, i) => ({
          sort_order: i,
          description: `Air Freight - ${l.supplier||'—'}`,
          unit_label: 'Per KG',
          zendesk: l.zendesk,
          supplier: l.supplier,
          rate: 0, // User enters rate
          quantity: 0, // User enters KG
          total: 0,
          gst_free: 0,
          is_misc: 0
        }));
        lines.push({ sort_order: lines.length, description:'Customs Processing', unit_label:'Flat Fee', rate:141, quantity:airLanes.length||1, total:141*(airLanes.length||1), gst_free:1, is_misc:0 });
        lines.push({ sort_order: lines.length+1, description:'', unit_label:'', rate:0, quantity:0, total:0, gst_free:0, is_misc:1 });
        lines.push({ sort_order: lines.length+2, description:'', unit_label:'', rate:0, quantity:0, total:0, gst_free:0, is_misc:1 });
        const customs = 141 * (airLanes.length||1);
        return res.json({ type:'AIR', week_start, lanes: airLanes, lines, customs, subtotal:0, gst:0, total:customs });
      }
    }
    res.status(400).json({ error: 'Unknown type' });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── GET /finance/invoice/:id/pdf — generate PDF (pure Node, no Python) ──
app.get('/finance/invoice/:id/pdf', async (req, res) => {
  try {
    // Accept token from query param (for direct browser downloads)
    if (req.query._token) {
      req.headers['authorization'] = 'Bearer ' + req.query._token;
    }
    // Auth check via existing middleware pattern
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.replace('Bearer ', '').trim();
    if (!token) return res.status(401).json({ error: 'No token' });

    const inv = db.prepare('SELECT * FROM fin_invoices WHERE id = ?').get(req.params.id);
    if (!inv) return res.status(404).json({ error: 'Not found' });
    inv.lines = db.prepare('SELECT * FROM fin_invoice_lines WHERE invoice_id = ? ORDER BY sort_order').all(inv.id);

    const PDFDocument = require('pdfkit');
    const doc = new PDFDocument({ size: 'A4', margin: 50 });

    const chunks = [];
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => {
      const pdf = Buffer.concat(chunks);
      const filename = (inv.ref_number || 'invoice').replace(/[^a-zA-Z0-9\-_]/g, '_') + '.pdf';
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Content-Length', pdf.length);
      res.send(pdf);
    });

    const BRAND = '#990033';
    const DARK  = '#1C1C1E';
    const MID   = '#6E6E73';
    const LIGHT = '#AEAEB2';

    function fmtUSD(v) {
      const f = parseFloat(v) || 0;
      return 'USD ' + f.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    function fmtDate(s) {
      if (!s) return '—';
      try { return new Date(s.slice(0,10) + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' }); } catch { return s; }
    }

    const W = 595 - 100; // usable width (A4 = 595, margins 50 each side)
    const type = inv.type;
    const lines = inv.lines || [];
    const mainLines = lines.filter(l => !l.gst_free && !l.is_misc);
    const customsLines = lines.filter(l => l.gst_free && !l.is_misc);
    const miscLines = lines.filter(l => l.is_misc && l.description);

    // ── Header ──
    // ── Brand name: velOzity>> with mixed styling ──
    // "vel" lowercase regular, "Oz" bold brand color, "ity" lowercase regular, ">>" brand arrows
    const brandY = 50;
    doc.fontSize(22).font('Helvetica').fillColor('#3A3A3C').text('vel', 50, brandY, { continued: true });
    doc.fontSize(22).font('Helvetica-Bold').fillColor(BRAND).text('Oz', { continued: true });
    doc.fontSize(22).font('Helvetica').fillColor('#3A3A3C').text('ity', { continued: true });
    doc.fontSize(16).font('Helvetica-Bold').fillColor(BRAND).text('»', { continued: false });
    doc.fontSize(16).font('Helvetica-Bold').fillColor(DARK).text('TAX INVOICE', 400, 50, { align: 'right', width: 145 });

    // Rule
    doc.moveTo(50, 80).lineTo(545, 80).lineWidth(2).strokeColor(BRAND).stroke();

    // Company info
    doc.fontSize(9).font('Helvetica-Bold').fillColor(DARK).text('Ogeo Pty Ltd.', 50, 92);
    doc.fontSize(8).font('Helvetica').fillColor(MID)
      .text('ABN: 96 670 485 499', 50, 104)
      .text('9 Aquamarine Street, Quakers Hill NSW 2763', 50, 114)
      .text('shuch@velozity.au  |  +61-449-701-751', 50, 124);

    // Invoice meta (right side)
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text('Invoice No:', 370, 92);
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(inv.ref_number || '—', 440, 92);
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text('Invoice Date:', 370, 104);
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(fmtDate(inv.invoice_date), 440, 104);
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text('Due Date:', 370, 114);
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(fmtDate(inv.due_date), 440, 114);
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text('Week:', 370, 124);
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(inv.week_start || '—', 440, 124);
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text('Payment Terms:', 370, 134);
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(type === 'VAS' ? '30 Days' : '7 Days', 440, 134);

    // Thin rule
    doc.moveTo(50, 148).lineTo(545, 148).lineWidth(0.5).strokeColor('#E5E5EA').stroke();

    // Bill To
    let y = 158;
    doc.fontSize(8).font('Helvetica-Bold').fillColor(BRAND).text('BILL TO', 50, y);
    y += 12;
    doc.fontSize(9).font('Helvetica-Bold').fillColor(DARK).text('The Iconic [ABN 50 152 631 082]', 50, y);
    y += 12;
    doc.fontSize(8).font('Helvetica').fillColor(MID).text('Level 18, Tower Two, International Towers, 200 Barangaroo Avenue, Barangaroo NSW 2000', 50, y);
    y += 14;

    // Description
    doc.moveTo(50, y).lineTo(545, y).lineWidth(0.5).strokeColor('#E5E5EA').stroke();
    y += 10;
    doc.fontSize(8).font('Helvetica-Bold').fillColor(BRAND).text('DESCRIPTION OF SERVICES', 50, y);
    y += 11;
    const descText = type === 'VAS'
      ? 'Services provided to The Iconic by VelOzity: VAS base processing, outbound activities, additional labelling, and carton replacement labour as detailed below.'
      : type === 'SEA'
      ? 'Services provided to The Iconic by VelOzity: transportation from warehouse to port, origin customs clearing and declaration, sea freight, destination customs declaration and clearing, and transportation from port to FC Yennora.'
      : 'Services provided to The Iconic by VelOzity: transportation from warehouse to airport, origin customs clearing and declaration, air freight, destination customs declaration and clearing, and transportation from airport to FC Yennora.';
    doc.fontSize(8).font('Helvetica').fillColor(DARK).text(descText, 50, y, { width: W });
    y += 28;

    // ── Line items table ──
    doc.fontSize(8).font('Helvetica-Bold').fillColor(BRAND).text('DETAILS OF CHARGES (USD)', 50, y);
    y += 10;

    // Table header
    doc.rect(50, y, W, 16).fillColor('#F5F5F7').fill();
    doc.fontSize(8).font('Helvetica-Bold').fillColor(MID);
    if (type === 'VAS') {
      doc.text('Service', 56, y + 4, { width: 160 });
      doc.text('Unit', 220, y + 4, { width: 90 });
      doc.text('Rate', 315, y + 4, { width: 50, align: 'right' });
      doc.text('Qty', 370, y + 4, { width: 60, align: 'right' });
      doc.text('Total', 435, y + 4, { width: 104, align: 'right' });
    } else {
      doc.text('Description', 56, y + 4, { width: 210 });
      doc.text('Rate (USD)', 270, y + 4, { width: 90, align: 'right' });
      doc.text('Qty', 365, y + 4, { width: 60, align: 'right' });
      doc.text('Total (USD)', 430, y + 4, { width: 109, align: 'right' });
    }
    y += 16;

    // Draw line rows
    function drawLine(l, idx) {
      const rowH = 18;
      if (idx % 2 === 1) { doc.rect(50, y, W, rowH).fillColor('#FAFAFA').fill(); }
      doc.moveTo(50, y + rowH).lineTo(545, y + rowH).lineWidth(0.3).strokeColor('#E5E5EA').stroke();
      doc.fontSize(8).font('Helvetica').fillColor(DARK);
      if (type === 'VAS') {
        doc.text(l.description || '', 56, y + 5, { width: 160 });
        doc.fontSize(7).fillColor(MID).text(l.unit_label || '', 220, y + 6, { width: 90 });
        doc.fontSize(8).fillColor(DARK).text(String(parseFloat(l.rate||0).toFixed(2)), 315, y + 5, { width: 50, align: 'right' });
        doc.text(String(parseFloat(l.quantity||0)), 370, y + 5, { width: 60, align: 'right' });
        doc.font('Helvetica-Bold').text(fmtUSD(l.total), 435, y + 5, { width: 104, align: 'right' });
      } else {
        doc.text(l.description || '', 56, y + 5, { width: 210 });
        doc.text(fmtUSD(l.rate||0), 270, y + 5, { width: 90, align: 'right' });
        doc.text(String(parseFloat(l.quantity||0)), 365, y + 5, { width: 60, align: 'right' });
        doc.font('Helvetica-Bold').text(fmtUSD(l.total), 430, y + 5, { width: 109, align: 'right' });
      }
      y += rowH;
    }

    mainLines.forEach((l, i) => drawLine(l, i));
    if (miscLines.length) {
      doc.rect(50, y, W, 14).fillColor('#F5F5F7').fill();
      doc.fontSize(7).font('Helvetica-Bold').fillColor(LIGHT).text('MISCELLANEOUS', 56, y + 4);
      y += 14;
      miscLines.forEach((l, i) => drawLine(l, mainLines.length + i));
    }
    if (customsLines.length) {
      doc.rect(50, y, W, 14).fillColor('#F5F5F7').fill();
      doc.fontSize(7).font('Helvetica-Bold').fillColor(LIGHT).text('CUSTOMS / GST-FREE', 56, y + 4);
      y += 14;
      customsLines.forEach((l, i) => drawLine(l, mainLines.length + miscLines.length + i));
    }

    y += 8;

    // ── Totals ──
    doc.moveTo(50, y).lineTo(545, y).lineWidth(0.5).strokeColor('#E5E5EA').stroke();
    y += 8;
    const subtotal = parseFloat(inv.subtotal) || 0;
    const gst      = parseFloat(inv.gst) || 0;
    const customs  = parseFloat(inv.customs) || 0;
    const total    = parseFloat(inv.total) || 0;

    function totRow(label, value, bold = false) {
      doc.fontSize(8).font(bold ? 'Helvetica-Bold' : 'Helvetica').fillColor(bold ? DARK : MID)
        .text(label, 350, y, { width: 130 })
        .text(value, 480, y, { width: 65, align: 'right' });
      y += 13;
    }
    if (subtotal) totRow('Subtotal (excl. GST)', fmtUSD(subtotal));
    if (gst)      totRow('GST at 10%', fmtUSD(gst));
    if (customs)  totRow('Customs Clearance (GST-free)', fmtUSD(customs));

    // Total payable highlighted box
    y += 2;
    doc.rect(350, y, 195, 20).fillColor(BRAND).fill();
    doc.fontSize(9).font('Helvetica-Bold').fillColor('#FFFFFF')
      .text('Total Payable', 356, y + 5, { width: 90 })
      .text(fmtUSD(total), 356, y + 5, { width: 183, align: 'right' });
    y += 28;

    // Notes
    if (inv.notes) {
      doc.moveTo(50, y).lineTo(545, y).lineWidth(0.5).strokeColor('#E5E5EA').stroke();
      y += 8;
      doc.fontSize(8).font('Helvetica-Bold').fillColor(BRAND).text('NOTES', 50, y);
      y += 11;
      doc.fontSize(8).font('Helvetica').fillColor(DARK).text(inv.notes, 50, y, { width: W });
      y += 20;
    }

    // ── Remittance ──
    y = Math.max(y, 680); // push to bottom area
    doc.moveTo(50, y).lineTo(545, y).lineWidth(1.5).strokeColor(BRAND).stroke();
    y += 10;
    doc.fontSize(8).font('Helvetica-Bold').fillColor(BRAND).text('REMITTANCE INFORMATION', 50, y);
    y += 12;
    const remit = [
      ['Account Beneficiary Name', 'OGEO PTY LTD'],
      ['Bank Name', 'COMMONWEALTH BANK'],
      ['Bank Address', '2 Sentry Dr, Stanhope Gardens NSW 2768, Australia'],
      ['Bank Account Number', '10199366'],
      ['SWIFT Code', 'CTBAAU2S'],
      ['BSB / IBAN', '062-704'],
    ];
    remit.forEach(([label, value]) => {
      doc.fontSize(8).font('Helvetica-Bold').fillColor(MID).text(label + ':', 50, y, { width: 140 });
      doc.fontSize(8).font('Helvetica').fillColor(DARK).text(value, 195, y, { width: 350 });
      y += 12;
    });

    doc.end();
  } catch(e) {
    console.error('[finance/pdf]', e);
    if (!res.headersSent) res.status(500).json({ error: String(e.message||e) });
  }
});

// ── GET /finance/expenses — list expenses ──
app.get('/finance/expenses', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { month_key, category } = req.query;
    let where = 'WHERE 1=1';
    const params = [];
    if (month_key) { where += ' AND month_key = ?'; params.push(month_key); }
    if (category)  { where += ' AND category = ?';  params.push(category); }
    const rows = db.prepare(`SELECT * FROM fin_expenses ${where} ORDER BY expense_date DESC`).all(...params);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── POST /finance/expenses ──
app.post('/finance/expenses', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { category, description, amount, currency, expense_date, is_recurring, recur_freq, recur_end } = req.body;
    if (!category || !description || !amount || !expense_date) return res.status(400).json({ error: 'Missing required fields' });
    const month_key = expense_date.slice(0, 7);
    const id = uuidv4();
    db.prepare(`INSERT INTO fin_expenses (id,category,description,amount,currency,expense_date,month_key,is_recurring,recur_freq,recur_end)
      VALUES (?,?,?,?,?,?,?,?,?,?)`).run(id, category, description, parseFloat(amount), currency||'USD', expense_date, month_key, is_recurring?1:0, recur_freq||null, recur_end||null);
    // If recurring, generate forward entries up to 12 months
    if (is_recurring && recur_freq === 'monthly') {
      const startDate = new Date(expense_date + 'T00:00:00Z');
      for (let i = 1; i <= 11; i++) {
        const nextDate = new Date(startDate);
        nextDate.setUTCMonth(nextDate.getUTCMonth() + i);
        const nextStr = nextDate.toISOString().slice(0,10);
        if (recur_end && nextStr > recur_end) break;
        const nextMonth = nextStr.slice(0,7);
        db.prepare(`INSERT INTO fin_expenses (id,category,description,amount,currency,expense_date,month_key,is_recurring,recur_freq,recur_end,parent_id)
          VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(uuidv4(), category, description, parseFloat(amount), currency||'USD', nextStr, nextMonth, 1, recur_freq, recur_end||null, id);
      }
    }
    res.json(db.prepare('SELECT * FROM fin_expenses WHERE id = ?').get(id));
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── PATCH /finance/expenses/:id ──
app.patch('/finance/expenses/:id', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { category, description, amount, currency, expense_date } = req.body;
    const exp = db.prepare('SELECT * FROM fin_expenses WHERE id = ?').get(req.params.id);
    if (!exp) return res.status(404).json({ error: 'Not found' });
    const upd = {
      category: category ?? exp.category,
      description: description ?? exp.description,
      amount: amount !== undefined ? parseFloat(amount) : exp.amount,
      currency: currency ?? exp.currency,
      expense_date: expense_date ?? exp.expense_date,
    };
    upd.month_key = upd.expense_date.slice(0,7);
    db.prepare(`UPDATE fin_expenses SET category=?,description=?,amount=?,currency=?,expense_date=?,month_key=?,updated_at=datetime('now') WHERE id=?`)
      .run(upd.category, upd.description, upd.amount, upd.currency, upd.expense_date, upd.month_key, exp.id);
    res.json(db.prepare('SELECT * FROM fin_expenses WHERE id = ?').get(exp.id));
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── DELETE /finance/expenses/:id ──
app.delete('/finance/expenses/:id', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    db.prepare('DELETE FROM fin_expenses WHERE id = ? OR parent_id = ?').run(req.params.id, req.params.id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── GET /finance/pl — P&L summary by month with unit economics ──
app.get('/finance/pl', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { year } = req.query;
    const y = year || new Date().getUTCFullYear();

    // ── 1. Invoices by month and type — include draft so P&L shows accrual view ──
    const invRows = db.prepare(`
      SELECT type, week_start, invoice_date, total, ref_number, status
      FROM fin_invoices
      WHERE status IN ('draft','sent','paid') AND week_start >= ? AND week_start <= ?
      ORDER BY week_start
    `).all(`${y}-01-01`, `${y}-12-31`);

    // ── 2. Expenses by month and category ──
    const expRows = db.prepare(`
      SELECT * FROM fin_expenses WHERE month_key >= ? AND month_key <= ? ORDER BY expense_date
    `).all(`${y}-01`, `${y}-12`);

    // ── 3. VAS applied units by month from records ──
    const vasUnitRows = db.prepare(`
      SELECT substr(date_local, 1, 7) as month_key, COUNT(*) as units
      FROM records
      WHERE status='complete' AND date_local >= ? AND date_local <= ?
      GROUP BY substr(date_local, 1, 7)
    `).all(`${y}-01-01`, `${y}-12-31`);
    const vasUnitsByMonth = {};
    for (const r of vasUnitRows) vasUnitsByMonth[r.month_key] = r.units;

    // ── 4. Sea + Air ACTUAL units — join records (applied) to plans (freight_type) by PO ──
    // This gives actuals: units actually applied against sea/air POs, by month of application
    const planWeeks = db.prepare(`
      SELECT week_start, data FROM plans WHERE week_start >= ? AND week_start <= ?
    `).all(`${y}-01-01`, `${y}-12-31`);

    // Build PO → freight_type map from all plan weeks in the year
    const poFreightMap = new Map(); // po_number → 'sea' | 'air'
    for (const pw of planWeeks) {
      try {
        const rows = JSON.parse(pw.data || '[]');
        for (const p of rows) {
          const po = String(p.po_number || '').trim();
          const ft = String(p.freight_type || '').toLowerCase();
          if (po && (ft === 'sea' || ft === 'air')) {
            poFreightMap.set(po, ft);
          }
        }
      } catch {}
    }

    // Count applied records by freight type, grouped by month (date_local)
    // Each record row = 1 applied unit (records are UID-level)
    const appliedRecords = db.prepare(`
      SELECT po_number, substr(date_local, 1, 7) as month_key
      FROM records
      WHERE status='complete' AND date_local >= ? AND date_local <= ?
    `).all(`${y}-01-01`, `${y}-12-31`);

    const seaUnitsByMonth = {}, airUnitsByMonth = {};
    for (const r of appliedRecords) {
      const ft = poFreightMap.get(String(r.po_number || '').trim());
      if (!ft || !r.month_key) continue;
      if (ft === 'sea') seaUnitsByMonth[r.month_key] = (seaUnitsByMonth[r.month_key] || 0) + 1;
      if (ft === 'air') airUnitsByMonth[r.month_key] = (airUnitsByMonth[r.month_key] || 0) + 1;
    }

    // ── 5. Build month objects ──
    const months = {};
    for (let m = 1; m <= 12; m++) {
      const mk = `${y}-${String(m).padStart(2,'0')}`;
      months[mk] = {
        month_key: mk,
        revenue: 0, rev_vas: 0, rev_sea: 0, rev_air: 0,
        expenses: 0, exp_labour: 0, exp_freight: 0, exp_overhead: 0,
        units_vas: vasUnitsByMonth[mk] || 0,
        units_sea: seaUnitsByMonth[mk] || 0,
        units_air: airUnitsByMonth[mk] || 0,
        net: 0, margin_pct: 0,
        invoices: [], expense_rows: [],
      };
    }

    // ── 6. Allocate invoice revenue by type ──
    // Use invoice_date for month bucketing when set (avoids week-boundary cross-month issues).
    // Fall back to week_start if invoice_date is absent.
    for (const inv of invRows) {
      const dateForBucket = (inv.invoice_date && inv.invoice_date.length >= 7) ? inv.invoice_date : inv.week_start;
      const mk = dateForBucket.slice(0, 7);
      if (!months[mk]) continue;
      months[mk].revenue += inv.total;
      months[mk].invoices.push({ ref: inv.ref_number||inv.id, type: inv.type, amount: inv.total, status: inv.status });
      if (inv.type === 'VAS') months[mk].rev_vas += inv.total;
      else if (inv.type === 'SEA') months[mk].rev_sea += inv.total;
      else if (inv.type === 'AIR') months[mk].rev_air += inv.total;
    }

    // ── 7. Allocate expenses to channel pools by category ──
    // Direct Labour → VAS cost pool (scanning/labelling/processing ops only)
    // Labour → overhead/blended (admin, management, non-VAS staff)
    // Freight Cost + Duties → split between Sea and Air by revenue ratio
    // Everything else → overhead (blended)
    const LABOUR_CATS   = new Set(['Direct Labour', 'VAS Cost']);
    const FREIGHT_CATS  = new Set(['Freight Cost', 'Sea Freight Cost', 'Air Freight Cost', 'Duties & Customs']);
    const SEA_CATS      = new Set(['Sea Freight Cost']);
    const AIR_CATS      = new Set(['Air Freight Cost']);
    for (const exp of expRows) {
      const mk = exp.month_key;
      if (!months[mk]) continue;
      const amt = parseFloat(exp.amount) || 0;
      months[mk].expenses += amt;
      months[mk].expense_rows.push(exp);
      if (LABOUR_CATS.has(exp.category))        months[mk].exp_labour   += amt;
      else if (SEA_CATS.has(exp.category))      { months[mk].exp_freight += amt; months[mk].exp_freight_sea_direct = (months[mk].exp_freight_sea_direct||0) + amt; }
      else if (AIR_CATS.has(exp.category))      { months[mk].exp_freight += amt; months[mk].exp_freight_air_direct = (months[mk].exp_freight_air_direct||0) + amt; }
      else if (FREIGHT_CATS.has(exp.category))  months[mk].exp_freight  += amt;
      else                                       months[mk].exp_overhead += amt;
    }

    // ── 8. Compute net, margins, unit economics ──
    for (const mk of Object.keys(months)) {
      const m = months[mk];
      // Round all money fields
      ['revenue','rev_vas','rev_sea','rev_air','expenses','exp_labour','exp_freight','exp_overhead'].forEach(k => {
        m[k] = Math.round(m[k] * 100) / 100;
      });
      m.net = Math.round((m.revenue - m.expenses) * 100) / 100;
      m.margin_pct = m.revenue > 0 ? Math.round(m.net / m.revenue * 100) : 0;

      // VAS unit economics — labour cost pool
      m.vas_rev_pu     = m.units_vas > 0 ? Math.round(m.rev_vas / m.units_vas * 100) / 100 : null;
      m.vas_cost_pu    = m.units_vas > 0 ? Math.round(m.exp_labour / m.units_vas * 100) / 100 : null;
      m.vas_margin_pu  = (m.vas_rev_pu !== null && m.vas_cost_pu !== null)
        ? Math.round((m.vas_rev_pu - m.vas_cost_pu) * 100) / 100 : null;

      // Freight cost split — use direct Sea/Air category amounts when available,
      // fall back to revenue-ratio split for generic 'Freight Cost' entries
      const directSea = m.exp_freight_sea_direct || 0;
      const directAir = m.exp_freight_air_direct || 0;
      const directTotal = directSea + directAir;
      const undirected = Math.max(0, m.exp_freight - directTotal);
      const freightTotal = m.rev_sea + m.rev_air;
      const seaFrac = freightTotal > 0 ? m.rev_sea / freightTotal : 0.5;
      const airFrac = freightTotal > 0 ? m.rev_air / freightTotal : 0.5;
      m.exp_freight_sea = Math.round((directSea + undirected * seaFrac) * 100) / 100;
      m.exp_freight_air = Math.round((directAir + undirected * airFrac) * 100) / 100;

      // SEA unit economics
      m.sea_rev_pu    = m.units_sea > 0 ? Math.round(m.rev_sea / m.units_sea * 100) / 100 : null;
      m.sea_cost_pu   = m.units_sea > 0 ? Math.round(m.exp_freight_sea / m.units_sea * 100) / 100 : null;
      m.sea_margin_pu = (m.sea_rev_pu !== null && m.sea_cost_pu !== null)
        ? Math.round((m.sea_rev_pu - m.sea_cost_pu) * 100) / 100 : null;

      // AIR unit economics
      m.air_rev_pu    = m.units_air > 0 ? Math.round(m.rev_air / m.units_air * 100) / 100 : null;
      m.air_cost_pu   = m.units_air > 0 ? Math.round(m.exp_freight_air / m.units_air * 100) / 100 : null;
      m.air_margin_pu = (m.air_rev_pu !== null && m.air_cost_pu !== null)
        ? Math.round((m.air_rev_pu - m.air_cost_pu) * 100) / 100 : null;

      // Blended (all revenue / all VAS units processed)
      m.blended_rev_pu    = m.units_vas > 0 ? Math.round(m.revenue / m.units_vas * 100) / 100 : null;
      m.blended_cost_pu   = m.units_vas > 0 ? Math.round(m.expenses / m.units_vas * 100) / 100 : null;
      m.blended_margin_pu = (m.blended_rev_pu !== null && m.blended_cost_pu !== null)
        ? Math.round((m.blended_rev_pu - m.blended_cost_pu) * 100) / 100 : null;
    }

    // ── 9. YTD totals ──
    const ytd = Object.values(months).reduce((acc, m) => ({
      revenue: acc.revenue + m.revenue, rev_vas: acc.rev_vas + m.rev_vas,
      rev_sea: acc.rev_sea + m.rev_sea, rev_air: acc.rev_air + m.rev_air,
      expenses: acc.expenses + m.expenses, exp_labour: acc.exp_labour + m.exp_labour,
      exp_freight: acc.exp_freight + m.exp_freight, exp_overhead: acc.exp_overhead + m.exp_overhead,
      net: acc.net + m.net,
      units_vas: acc.units_vas + m.units_vas,
      units_sea: acc.units_sea + m.units_sea,
      units_air: acc.units_air + m.units_air,
    }), { revenue:0,rev_vas:0,rev_sea:0,rev_air:0,expenses:0,exp_labour:0,exp_freight:0,exp_overhead:0,net:0,units_vas:0,units_sea:0,units_air:0 });

    ytd.margin_pct      = ytd.revenue > 0 ? Math.round(ytd.net / ytd.revenue * 100) : 0;
    ytd.vas_rev_pu      = ytd.units_vas > 0 ? Math.round(ytd.rev_vas / ytd.units_vas * 100) / 100 : null;
    ytd.vas_cost_pu     = ytd.units_vas > 0 ? Math.round(ytd.exp_labour / ytd.units_vas * 100) / 100 : null;
    const ytdFreight    = ytd.rev_sea + ytd.rev_air || 1;
    ytd.sea_rev_pu      = ytd.units_sea > 0 ? Math.round(ytd.rev_sea / ytd.units_sea * 100) / 100 : null;
    ytd.sea_cost_pu     = ytd.units_sea > 0 ? Math.round(ytd.exp_freight * (ytd.rev_sea / ytdFreight) / ytd.units_sea * 100) / 100 : null;
    ytd.air_rev_pu      = ytd.units_air > 0 ? Math.round(ytd.rev_air / ytd.units_air * 100) / 100 : null;
    ytd.air_cost_pu     = ytd.units_air > 0 ? Math.round(ytd.exp_freight * (ytd.rev_air / ytdFreight) / ytd.units_air * 100) / 100 : null;
    ytd.blended_rev_pu  = ytd.units_vas > 0 ? Math.round(ytd.revenue / ytd.units_vas * 100) / 100 : null;
    ytd.blended_cost_pu = ytd.units_vas > 0 ? Math.round(ytd.expenses / ytd.units_vas * 100) / 100 : null;

    const outstanding = db.prepare(`SELECT COUNT(*) as n, COALESCE(SUM(total),0) as total FROM fin_invoices WHERE status IN ('draft','sent','overdue')`).get();
    res.json({ year: y, months: Object.values(months), ytd, outstanding });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});


// ── GET/POST /finance/fx — FX rates ──
app.get('/finance/fx', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const rates = db.prepare('SELECT * FROM fin_fx_rates ORDER BY from_curr, to_curr').all();
    res.json(rates);
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

app.post('/finance/fx', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const { from_curr, to_curr, rate, source } = req.body;
    if (!from_curr || !to_curr || !rate) return res.status(400).json({ error: 'Missing fields' });
    db.prepare(`INSERT INTO fin_fx_rates (id,from_curr,to_curr,rate,source,fetched_at) VALUES (?,?,?,?,?,datetime('now'))
      ON CONFLICT(from_curr,to_curr) DO UPDATE SET rate=excluded.rate, source=excluded.source, fetched_at=datetime('now')`)
      .run(uuidv4(), from_curr.toUpperCase(), to_curr.toUpperCase(), parseFloat(rate), source||'manual');
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── GET /finance/summary — for PULSE context ──
app.get('/finance/summary', authenticateRequest, requireRole(['admin']), (req, res) => {
  try {
    const outstanding = db.prepare(`SELECT COUNT(*) as n, COALESCE(SUM(total),0) as total FROM fin_invoices WHERE status IN ('draft','sent','overdue')`).get();
    const paid_ytd = db.prepare(`SELECT COALESCE(SUM(total),0) as total FROM fin_invoices WHERE status='paid' AND week_start >= ?`).get(`${new Date().getUTCFullYear()}-01-01`);
    const expenses_ytd = db.prepare(`SELECT COALESCE(SUM(amount),0) as total FROM fin_expenses WHERE month_key >= ?`).get(`${new Date().getUTCFullYear()}-01`);
    const last_invoice = db.prepare(`SELECT * FROM fin_invoices ORDER BY created_at DESC LIMIT 1`).get();
    const by_type = db.prepare(`SELECT type, COUNT(*) as n, COALESCE(SUM(total),0) as total FROM fin_invoices WHERE status='paid' GROUP BY type`).all();
    res.json({ outstanding, paid_ytd, expenses_ytd, last_invoice, by_type });
  } catch(e) { res.status(500).json({ error: String(e.message||e) }); }
});

// ── POST /finance/insights — AI-powered P&L analysis ──
app.post('/finance/insights', authenticateRequest, requireRole(['admin']), aiLimiter, async (req, res) => {
  try {
    const { pl_data } = req.body || {};
    if (!pl_data) return res.status(400).json({ error: 'pl_data required' });
    const anthropic = getAnthropic();
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1200,
      system: 'You are a financial analyst for VelOzity, a 3PL/VAS company. Revenue channels: VAS (warehouse labelling/processing, billed per unit, Labour=direct VAS cost), Sea Freight, Air Freight, Overhead (Software/Office/Storage/Marketing/Other). Analyse P&L and return exactly 5 specific actionable insights. Use real numbers. Return ONLY a valid JSON array, no markdown. Each object: title (3-5 words), insight (1-2 sentences with numbers), action (one concrete next step), impact (High/Medium/Low), channel (VAS/Sea/Air/Overall).',
      messages: [{ role: 'user', content: 'Analyse this P&L and return 5 insights as JSON array:\n' + JSON.stringify(pl_data) }]
    });
    const text = response.content.find(b => b.type === 'text')?.text || '[]';
    const clean = text.replace(/^```json\s*/, '').replace(/```\s*$/, '').trim();
    let insights;
    try { insights = JSON.parse(clean); }
    catch { insights = [{ title: 'Analysis complete', insight: text.slice(0,300), action: 'Review data', impact: 'Medium', channel: 'Overall' }]; }
    res.json({ insights });
  } catch(e) {
    console.error('[/finance/insights]', e);
    res.status(500).json({ error: String(e.message||e) });
  }
});

// ── END FINANCE MODULE ──────────────────────────────────────────

// ---- Start ----
app.listen(PORT, () => {
  console.log(`UID Ops backend listening on http://localhost:${PORT}`);
  console.log(`DB file: ${DB_FILE}`);
  console.log(`CORS origin(s): ${allowList.join(', ')}`);
});

