// server.js — UID Ops Backend (Express + SQLite + SSE + Weekly Plan persistence)
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const ExcelJS = require('exceljs');
const Database = require('better-sqlite3');

// ---- Config ----
const PORT = process.env.PORT || 4000;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*'; // set to your Netlify domain in production
const DB_DIR = process.env.DB_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DB_DIR, { recursive: true });
const DB_FILE = process.env.DB_FILE || path.join(DB_DIR, 'uid_ops.sqlite');

// ---- App ----
const app = express();
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOWED_ORIGIN === '*' || origin === ALLOWED_ORIGIN) return cb(null, true);
    // allow common Netlify preview subdomains: set ALLOWED_ORIGIN like https://*.netlify.app to wildcard
    if (ALLOWED_ORIGIN.endsWith('.netlify.app') && origin.endsWith('.netlify.app')) return cb(null, true);
    return cb(new Error('CORS blocked: ' + origin));
  }
}));
app.use(express.json({ limit: '10mb' }));


// ---- TZ helpers ----
function ymdInTZ(date = new Date(), tz = 'America/Chicago') {
  const p = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(date);
  const y = p.find(x=>x.type==='year').value;
  const m = p.find(x=>x.type==='month').value;
  const d = p.find(x=>x.type==='day').value;
  return `${y}-${m}-${d}`; // YYYY-MM-DD in that tz
}
function weekdayInTZ(date = new Date(), tz = 'America/Chicago') {
  // Build a Date from the Y-M-D label (as UTC) and read getUTCDay
  const ymd = ymdInTZ(date, tz);
  const dt = new Date(ymd + 'T00:00:00Z');
  return dt.getUTCDay(); // 0=Sun..6=Sat representing that local date
}
function mondayISOInTZ(date = new Date(), tz = 'America/Chicago') {
  const ymd = ymdInTZ(date, tz);
  let dt = new Date(ymd + 'T00:00:00Z');
  const dow = dt.getUTCDay();
  const delta = (dow === 0 ? -6 : 1 - dow); // move to Monday
  dt.setUTCDate(dt.getUTCDate() + delta);
  return dt.toISOString().slice(0,10);
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
CREATE INDEX IF NOT EXISTS idx_records_po ON records(po_number);
CREATE INDEX IF NOT EXISTS idx_records_sku ON records(sku_code);


-- New table for per-timezone plans (backward compatible)
CREATE TABLE IF NOT EXISTS plans_by_tz (
  tz TEXT NOT NULL,
  week_start TEXT NOT NULL,
  data TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (tz, week_start)
);

CREATE TABLE IF NOT EXISTS plans (
  week_start TEXT PRIMARY KEY,
  data TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
`);


// Plans by TZ
const getPlanTZ = db.prepare('SELECT data FROM plans_by_tz WHERE tz = ? AND week_start = ?');
const putPlanTZ = db.prepare('INSERT INTO plans_by_tz (tz, week_start, data, updated_at) VALUES (?, ?, ?, datetime(\'now\')) ON CONFLICT(tz, week_start) DO UPDATE SET data=excluded.data, updated_at=datetime(\'now\')');
const listWeeksTZ = db.prepare('SELECT week_start FROM plans_by_tz WHERE tz = ? ORDER BY week_start DESC LIMIT 32');
const selectRecordById = db.prepare('SELECT * FROM records WHERE id = ?');
const insertRecordBase = db.prepare(
  `INSERT INTO records (id, date_local, status, sync_state)
   VALUES (?, ?, 'draft', 'pending')`
);
const updateRecordFields = db.prepare(
  `UPDATE records SET
     date_local   = COALESCE(?, date_local),
     mobile_bin   = COALESCE(?, mobile_bin),
     sscc_label   = COALESCE(?, sscc_label),
     po_number    = COALESCE(?, po_number),
     sku_code     = COALESCE(?, sku_code),
     uid          = COALESCE(?, uid),
     status       = COALESCE(?, status),
     completed_at = COALESCE(?, completed_at),
     sync_state   = COALESCE(?, sync_state)
   WHERE id = ?`
);

function isComplete(row) {
  return Boolean(
    (row?.po_number || '').trim() &&
    (row?.sku_code  || '').trim() &&
    (row?.uid       || '').trim() &&
    (row?.mobile_bin|| '').trim()
  );
}

// --- SSE hub for cardio ---
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
    'Access-Control-Allow-Origin': ALLOWED_ORIGIN
  });
  res.write('\n');
  clients.add(res);
  req.on('close', () => { clients.delete(res); });
});

// --- Health ---
app.get('/health', (req, res) => res.json({ ok: true, now: new Date().toISOString() }));

// --- Records API ---
app.patch('/records/:id', (req, res) => {
  const id = String(req.params.id);
  const { field, value } = req.body || {};
  if (!id || !field) return res.status(400).json({ error: 'id and field required' });

  const allowed = new Set(['date_local', 'mobile_bin', 'sscc_label', 'po_number', 'sku_code', 'uid']);
  if (!allowed.has(field)) return res.status(400).json({ error: `Invalid field: ${field}` });

  let row = selectRecordById.get(id);
  if (!row) {
    insertRecordBase.run(id, todayChicagoISO());
    row = selectRecordById.get(id);
  }

  const next = { ...row };
  if (field === 'date_local') next.date_local = String(value || '') || todayChicagoISO();
  if (field === 'mobile_bin') next.mobile_bin = String(value || '');
  if (field === 'sscc_label') next.sscc_label = String(value || '');
  if (field === 'po_number')  next.po_number  = String(value || '');
  if (field === 'sku_code')   next.sku_code   = String(value || '');
  if (field === 'uid')        next.uid        = String(value || '');

  let completed_at = row.completed_at;
  let status = row.status;
  let sync_state = 'pending';

  const willBeComplete = isComplete({ ...row, [field]: value });
  if (willBeComplete && row.status !== 'complete') {
    status = 'complete';
    completed_at = new Date().toISOString();
    emitScan(new Date(completed_at));
  }

  updateRecordFields.run(
    next.date_local || row.date_local || todayChicagoISO(),
    next.mobile_bin ?? null,
    next.sscc_label ?? null,
    next.po_number  ?? null,
    next.sku_code   ?? null,
    next.uid        ?? null,
    status,
    completed_at    ?? null,
    sync_state,
    id
  );

  const after = selectRecordById.get(id);
  return res.json({ ok: true, record: after });
});

app.get('/records', (req, res) => {
  const { from, to, status, limit } = req.query;
  const params = [];
  let sql = 'SELECT * FROM records WHERE 1=1';
  if (from)   { sql += ' AND date_local >= ?'; params.push(String(from)); }
  if (to)     { sql += ' AND date_local <= ?'; params.push(String(to)); }
  if (status) { sql += ' AND status = ?';      params.push(String(status)); }
  sql += ' ORDER BY completed_at DESC';
  if (limit)  { sql += ' LIMIT ?';             params.push(Number(limit)); }
  const rows = db.prepare(sql).all(...params);
  res.json({ records: rows });
});

app.get('/export/xlsx', async (req, res) => {
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

// ---------- Weekly Plan API ----------
function normalizePlanArray(body, fallbackStart) {
  if (!Array.isArray(body)) return [];
  const norm = body.map(r => ({
    po_number: String(r?.po_number ?? '').trim(),
    sku_code:  String(r?.sku_code  ?? '').trim(),
    start_date: (String(r?.start_date ?? '').trim()) || fallbackStart || '',
    due_date:   String(r?.due_date   ?? '').trim(),
    target_qty: Number(r?.target_qty ?? 0) || 0,
    priority:   r?.priority ? String(r.priority).trim() : undefined,
    notes:      r?.notes ? String(r.notes).trim() : undefined,
  })).filter(r => r.po_number && r.sku_code && r.due_date);
  return norm;
}

app.get('/plan/weeks/:mondayISO', (req, res) => {
  const monday = String(req.params.mondayISO);
  const row = db.prepare('SELECT data FROM plans WHERE week_start = ?').get(monday);
  if (!row) return res.json([]);
  try {
    const data = JSON.parse(row.data);
    return res.json(Array.isArray(data) ? data : []);
  } catch {
    return res.json([]);
  }
});

app.put('/plan/weeks/:mondayISO', (req, res) => {
  const monday = String(req.params.mondayISO);
  const arr = normalizePlanArray(req.body, monday);
  const json = JSON.stringify(arr);
  db.prepare(`
    INSERT INTO plans(week_start, data, updated_at)
    VALUES(?, ?, datetime('now'))
    ON CONFLICT(week_start) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
  `).run(monday, json);
  return res.json(arr);
});

app.get('/plan/weeks', (req, res) => {
  const rows = db.prepare(`SELECT week_start, updated_at FROM plans ORDER BY week_start DESC LIMIT 52`).all();
  res.json(rows);
});

// ---- Start ----

// Convenience: current active week start for a timezone
app.get('/plan/active_monday', (req, res) => {
  const tz = String(req.query.tz || 'America/Chicago');
  return res.json({ ok: true, tz, week_start: mondayISOInTZ(new Date(), tz) });
});

app.listen(PORT, () => {
  console.log(`UID Ops backend listening on http://localhost:${PORT}`);
  console.log(`DB file: ${DB_FILE}`);
  console.log(`CORS origin: ${ALLOWED_ORIGIN}`);
});
