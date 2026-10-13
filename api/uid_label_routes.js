/* ── VelOzity Pinpoint — UID labels: ledger, delta issuance, renderer, print queue ──
   Replaces the two standalone label generators (velozityfastgen — 76×36 standard labels;
   portrait3676uid — 36×76 footwear labels). Both minted random UIDs in the browser and kept
   no record, which made "print only what changed" impossible. Here the UID is minted by the
   server into a ledger and everything else is derived from it.

   What this file insists on:

     · a UID is issued once and never again. Format exactly as before — "TIBC" + 9 characters
       from [0-9a-z]. Minted inside a write transaction against a UNIQUE index, and checked
       against every UID the scanners have already seen in `records`, so nothing printed before
       Pinpoint owned the ledger can collide either.

     · issuance is keyed (client, week, PO, SKU) against the ledger — never against the
       previous plan file. Teams re-upload the whole plan with changes; the delta is
       planned − issued. Same file twice prints nothing. A reduction prints nothing and shows
       as over-issued. Extra labels (torn, lost) are new UIDs, requested on the page.

     · a PO that leaves one week's plan and appears in another within MOVE_WINDOW weeks is a
       move, not a repeat: its labels travel with it in the ledger and nothing prints. The
       warehouse chooses on the page — download the same PDF again, or issue new.

     · one PDF per PO per run, named by PO, rendered on demand from the ledger and cached.
       Render's disk is ephemeral; the ledger is what persists. Vector PDFs: DataMatrix and
       Code128 are drawn as rectangles from bwip-js module data, not embedded bitmaps.

     · layouts are the two originals, constant for constant. Standard = the canvas renderer
       (300 dpi, Helvetica regular); footwear = the reportlab renderer (Helvetica-Bold,
       DataMatrix flush to the corners, dark-grey Code128 of the EAN). Nothing was redesigned.

     · footwear is decided by the department code — the first four characters of the PO —
       from a table the team maintains on the page. A code the table has never seen prints as
       standard and is flagged New, so a print is never blocked.

   Mount:
     app.use('/uid-labels', require('./uid_label_routes')({ express, db, authenticateRequest,
       requireRole, requireInternalOrg, auditLog, curClient, sendViaResend, writeOpLimiter, logger }));
     and after a plan is saved: uidLabels.onPlanSaved(clientId, monday, rows, who)
*/
'use strict';

const crypto = require('crypto');
const PDFDocument = require('pdfkit');
let bwipjs = null;
try { bwipjs = require('bwip-js'); } catch (e) { bwipjs = null; }

module.exports = function mountUidLabels(deps) {
  const { express, db, authenticateRequest, requireRole, requireInternalOrg, auditLog, curClient,
          sendViaResend, writeOpLimiter, logger } = deps;
  const router = express.Router();
  const log = logger || console;
  const passthru = (req, res, next) => next();
  const limiter = writeOpLimiter || passthru;
  const internalOnly = requireInternalOrg || passthru;

  const MOVE_WINDOW_WEEKS = Number(process.env.UID_LABEL_MOVE_WINDOW_WEEKS || 4);
  const APP_URL = String(process.env.ELEVATE_APP_URL || process.env.APP_URL || 'https://pinpoint.velozity.com.au').replace(/\/+$/, '');
  const PAGE_URL = `${APP_URL}/#uid-labels`;

  // ── Schema ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS uid_label_run (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id   TEXT NOT NULL,
      week_start  TEXT NOT NULL,
      kind        TEXT NOT NULL,              -- plan | extra
      trigger_by  TEXT,                       -- who / what caused it
      created_at  TEXT NOT NULL DEFAULT (datetime('now')),
      labels      INTEGER NOT NULL DEFAULT 0,
      summary     TEXT NOT NULL DEFAULT '{}', -- JSON: pos, moved, new_codes, over, note
      emailed_at  TEXT,
      email_error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ulr_week ON uid_label_run(client_id, week_start, id);

    CREATE TABLE IF NOT EXISTS uid_label (
      uid         TEXT PRIMARY KEY,           -- TIBC + 9 × [0-9a-z]
      client_id   TEXT NOT NULL,
      week_start  TEXT NOT NULL,
      po_number   TEXT NOT NULL,
      sku_code    TEXT NOT NULL,
      style       TEXT NOT NULL DEFAULT '',
      ean         TEXT NOT NULL DEFAULT '',
      product     TEXT NOT NULL DEFAULT '',
      color       TEXT NOT NULL DEFAULT '',
      size        TEXT NOT NULL DEFAULT '',
      dept_code   TEXT NOT NULL DEFAULT '',
      format      TEXT NOT NULL,              -- standard | footwear
      run_id      INTEGER NOT NULL,
      seq         INTEGER NOT NULL,           -- order within the run, for stable PDFs
      issued_at   TEXT NOT NULL DEFAULT (datetime('now')),
      moved_from  TEXT                        -- week this label was first issued for, if moved
    );
    CREATE INDEX IF NOT EXISTS idx_ul_key ON uid_label(client_id, week_start, po_number, sku_code);
    CREATE INDEX IF NOT EXISTS idx_ul_run ON uid_label(run_id, po_number, seq);
    CREATE INDEX IF NOT EXISTS idx_ul_po  ON uid_label(client_id, po_number, sku_code);

    CREATE TABLE IF NOT EXISTS uid_label_format (
      client_id   TEXT NOT NULL,
      code        TEXT NOT NULL,              -- department code = first 4 chars of the PO
      format      TEXT NOT NULL DEFAULT 'standard',
      is_new      INTEGER NOT NULL DEFAULT 0, -- arrived on an upload, not yet reviewed
      source      TEXT NOT NULL DEFAULT 'manual',
      name        TEXT,
      updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
      updated_by  TEXT,
      PRIMARY KEY (client_id, code)
    );

    CREATE TABLE IF NOT EXISTS uid_label_file (
      run_id      INTEGER NOT NULL,
      po_number   TEXT NOT NULL,
      bytes       BLOB NOT NULL,
      labels      INTEGER NOT NULL,
      created_at  TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (run_id, po_number)
    );

    CREATE TABLE IF NOT EXISTS uid_label_reprint (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id      INTEGER NOT NULL,
      po_number   TEXT NOT NULL,
      by_user     TEXT,
      at          TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  // Seed the format table once per client from the department owners the team already keeps:
  // a name that says "Footwear" is footwear. Everything else starts as standard. The team can
  // flip any of it on the page.
  function seedFormats(client) {
    try {
      const n = db.prepare('SELECT COUNT(*) n FROM uid_label_format WHERE client_id = ?').get(client).n;
      if (n > 0) return;
      let rows = [];
      try { rows = db.prepare('SELECT code, name, segment FROM department_owner WHERE active = 1').all(); } catch (_) { rows = []; }
      const ins = db.prepare(`INSERT OR IGNORE INTO uid_label_format (client_id, code, format, is_new, source, name, updated_by)
                              VALUES (?, ?, ?, 0, 'seed', ?, 'seed')`);
      const tx = db.transaction(() => {
        for (const r of rows) {
          const code = String(r.code || '').trim().toUpperCase();
          if (!/^[A-Z0-9]{4}$/.test(code)) continue;
          const fw = /footwear/i.test(String(r.name || '')) || /^[WM]F/.test(String(r.segment || ''));
          ins.run(client, code, fw ? 'footwear' : 'standard', r.name || null);
        }
      });
      tx();
      if (rows.length) log.log(`[uid-labels] seeded ${rows.length} department code(s) for ${client} from department owners`);
    } catch (e) { log.warn('[uid-labels] seed failed:', e.message); }
  }

  // ── Helpers ──
  const str = (v) => String(v == null ? '' : v).trim();
  const ymdOk = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));
  const toD = (s) => new Date(String(s).slice(0, 10) + 'T00:00:00Z');
  const addDays = (ymd, n) => { const d = toD(ymd); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const mondayOf = (ymd) => { const d = toD(ymd); const wd = (d.getUTCDay() + 6) % 7; return addDays(ymd, -wd); };
  const weekDiff = (a, b) => Math.round((toD(b) - toD(a)) / (7 * 86400000));
  const isoWeek = (ymd) => {
    const d = toD(ymd); d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
    const y0 = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    return 'W' + String(Math.ceil(((d - y0) / 86400000 + 1) / 7)).padStart(2, '0');
  };
  const deptOf = (po) => str(po).toUpperCase().slice(0, 4);
  // Clerk's user object rides on req.user: a name or email reads far better on the page than
  // the user id, which is all req.auth carries.
  const who = (req) => {
    try {
      const u = req && req.user;
      if (u) {
        const name = [u.firstName, u.lastName].filter(Boolean).join(' ').trim();
        const email = (u.primaryEmailAddress && u.primaryEmailAddress.emailAddress)
          || (u.emailAddresses && u.emailAddresses[0] && u.emailAddresses[0].emailAddress) || null;
        if (name || email) return name || email;
      }
    } catch (_) {}
    const a = req && req.auth;
    return (a && (a.email || a.userEmail || a.userId || a.sub)) || 'user';
  };
  const safeName = (s) => str(s).replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 80) || 'PO';
  // "Size" formatting from the standard generator: numeric sizes lose their decimals.
  const fmtSize = (v) => { const s = str(v); if (!s) return ''; return /^[0-9]+(\.[0-9]+)?$/.test(s) ? s.split('.')[0] : s; };

  function planRowsFor(ws, client) {
    try {
      const r = db.prepare('SELECT data FROM plans WHERE week_start = ? AND client_id = ?').get(ws, client);
      if (!r) return null;                                  // no plan stored for that week
      const arr = JSON.parse(r.data);
      return Array.isArray(arr) ? arr : [];
    } catch (_) { return null; }
  }

  // ── UID minting ──
  // "TIBC" + 9 chars of [0-9a-z]: the prefix is always upper-case, the body never is. This is
  // the rule both generators shared; the scanners, the APO builder and `records.uid` all read
  // this shape, so it does not change.
  const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz';
  const UID_RE = /^TIBC[0-9a-z]{9}$/;
  function randomBody() {
    const buf = crypto.randomBytes(9);
    let s = '';
    for (let i = 0; i < 9; i++) s += ALPHABET[buf[i] % 36];
    return s;
  }
  // Every UID the scanners have already seen. `records.uid` holds either the bare token or the
  // whole DataMatrix payload "STYLE-SKU;TIBC…" — both are read. Loaded once per run, then kept
  // in sync as the run mints.
  function scannedTokens(client) {
    const out = new Set();
    try {
      let hasClient = false;
      try { hasClient = db.prepare('PRAGMA table_info(records)').all().some(c => c.name === 'client_id'); } catch (_) {}
      const rows = hasClient
        ? db.prepare(`SELECT uid FROM records WHERE uid IS NOT NULL AND uid <> '' AND (client_id = ? OR client_id IS NULL)`).all(client)
        : db.prepare(`SELECT uid FROM records WHERE uid IS NOT NULL AND uid <> ''`).all();
      for (const r of rows) {
        const s = str(r.uid);
        const tok = s.includes(';') ? s.split(';').pop().trim() : s;
        if (tok) out.add(tok);
        const m = /TIBC[0-9a-z]{9}/i.exec(s);
        if (m) out.add('TIBC' + m[0].slice(4).toLowerCase());
      }
    } catch (e) { log.warn('[uid-labels] scanned token scan failed:', e.message); }
    return out;
  }
  const existsStmt = () => db.prepare('SELECT 1 FROM uid_label WHERE uid = ?');
  function mintOne(taken, exists) {
    for (let attempt = 0; attempt < 50; attempt++) {
      const uid = 'TIBC' + randomBody();
      if (!UID_RE.test(uid)) continue;
      if (taken.has(uid)) continue;
      if (exists.get(uid)) continue;
      taken.add(uid);
      return uid;
    }
    throw new Error('Could not mint a unique UID after 50 attempts');
  }

  // ── Format table ──
  function formatFor(client, code, touched) {
    const r = db.prepare('SELECT format, is_new FROM uid_label_format WHERE client_id = ? AND code = ?').get(client, code);
    if (r) return r.format === 'footwear' ? 'footwear' : 'standard';
    // Never seen: print as standard, mark New so it is reviewed. Never block a print.
    db.prepare(`INSERT OR IGNORE INTO uid_label_format (client_id, code, format, is_new, source, updated_by)
                VALUES (?, ?, 'standard', 1, 'upload', 'plan upload')`).run(client, code);
    if (touched) touched.add(code);
    return 'standard';
  }

  // ── Reconcile a week's plan against the ledger ──
  // Returns the summary; creates a run only when something was issued or moved.
  function reconcile(client, week, rows, triggerBy) {
    seedFormats(client);
    const ws = mondayOf(week);
    // Planned per (PO, SKU), with the label fields from the plan row. A SKU that appears on
    // several rows of the same PO adds up.
    const planned = new Map();
    for (const r of (rows || [])) {
      const po = str(r.po_number), sku = str(r.sku_code);
      if (!po || !sku) continue;
      const k = po + '\u0000' + sku;
      const cur = planned.get(k) || { po, sku, qty: 0, style: '', ean: '', product: '', color: '', size: '' };
      cur.qty += Math.max(0, Math.floor(Number(r.target_qty) || 0));
      cur.style = cur.style || str(r.style);
      cur.ean = cur.ean || str(r.ean);
      cur.product = cur.product || str(r.item_description);
      cur.color = cur.color || str(r.item_color);
      cur.size = cur.size || fmtSize(r.item_size);
      planned.set(k, cur);
    }

    const issuedStmt = db.prepare(`SELECT COUNT(*) n FROM uid_label WHERE client_id = ? AND week_start = ? AND po_number = ? AND sku_code = ?`);
    const issuedOf = (po, sku) => issuedStmt.get(client, ws, po, sku).n;

    // Moves: a (PO, SKU) with nothing issued this week but labels in another week within the
    // window, where that week's current plan no longer carries it. Those labels follow the PO.
    const moved = [];
    const lo = addDays(ws, -7 * MOVE_WINDOW_WEEKS), hi = addDays(ws, 7 * MOVE_WINDOW_WEEKS);
    const elsewhere = db.prepare(`SELECT week_start, COUNT(*) n FROM uid_label
                                   WHERE client_id = ? AND po_number = ? AND sku_code = ? AND week_start <> ?
                                     AND week_start >= ? AND week_start <= ?
                                   GROUP BY week_start ORDER BY week_start DESC`);
    const planCache = new Map();
    const stillPlanned = (otherWs, po, sku) => {
      if (!planCache.has(otherWs)) planCache.set(otherWs, planRowsFor(otherWs, client));
      const p = planCache.get(otherWs);
      if (p == null) return false;                          // no plan stored → it is not planned there
      return p.some(r => str(r.po_number) === po && str(r.sku_code) === sku && (Number(r.target_qty) || 0) > 0);
    };
    const moveStmt = db.prepare(`UPDATE uid_label SET week_start = ?, moved_from = COALESCE(moved_from, week_start)
                                  WHERE client_id = ? AND po_number = ? AND sku_code = ? AND week_start = ?`);

    const toIssue = [];
    const over = [];
    const newCodes = new Set();
    let total = 0;

    const tx = db.transaction(() => {
      for (const p of planned.values()) {
        if (p.qty <= 0) continue;
        let issued = issuedOf(p.po, p.sku);
        if (issued === 0) {
          for (const e of elsewhere.all(client, p.po, p.sku, ws, lo, hi)) {
            if (stillPlanned(e.week_start, p.po, p.sku)) continue;   // shipping again, not a move
            moveStmt.run(ws, client, p.po, p.sku, e.week_start);
            moved.push({ po: p.po, sku: p.sku, from: e.week_start, labels: e.n });
            issued += e.n;
            break;                                                  // one source week per key
          }
        }
        const delta = p.qty - issued;
        if (delta > 0) { toIssue.push(Object.assign({ delta, issued }, p)); total += delta; }
        else if (delta < 0) over.push({ po: p.po, sku: p.sku, planned: p.qty, issued });
      }
      if (!toIssue.length && !moved.length) return null;

      const runId = db.prepare(`INSERT INTO uid_label_run (client_id, week_start, kind, trigger_by, labels, summary)
                                VALUES (?, ?, 'plan', ?, ?, '{}')`).run(client, ws, triggerBy || 'plan upload', total).lastInsertRowid;
      const ins = db.prepare(`INSERT INTO uid_label (uid, client_id, week_start, po_number, sku_code, style, ean, product, color, size, dept_code, format, run_id, seq)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      const taken = scannedTokens(client), exists = existsStmt();
      let seq = 0;
      const pos = new Map();
      toIssue.sort((a, b) => a.po.localeCompare(b.po) || a.sku.localeCompare(b.sku));
      for (const p of toIssue) {
        const code = deptOf(p.po);
        const format = formatFor(client, code, newCodes);
        for (let i = 0; i < p.delta; i++) {
          ins.run(mintOne(taken, exists), client, ws, p.po, p.sku, p.style, p.ean, p.product, p.color, p.size, code, format, runId, ++seq);
        }
        const cur = pos.get(p.po) || { po: p.po, format, labels: 0, skus: 0 };
        cur.labels += p.delta; cur.skus += 1; pos.set(p.po, cur);
      }
      const summary = {
        pos: [...pos.values()],
        moved, over,
        new_codes: [...newCodes].map(code => ({ code, pos: toIssue.filter(p => deptOf(p.po) === code).map(p => p.po).filter((v, i, a) => a.indexOf(v) === i) })),
      };
      db.prepare('UPDATE uid_label_run SET summary = ? WHERE id = ?').run(JSON.stringify(summary), runId);
      return { runId, summary };
    });

    const out = tx();
    if (!out) return { ok: true, run_id: null, labels: 0, moved: [], over, note: 'nothing to issue' };
    log.log(`[uid-labels] run #${out.runId} ${client} ${ws}: ${total} label(s) across ${out.summary.pos.length} PO(s)${moved.length ? `, ${moved.length} moved` : ''}${newCodes.size ? `, new codes ${[...newCodes].join(',')}` : ''}`);
    return { ok: true, run_id: out.runId, labels: total, ...out.summary };
  }

  // Extras: more labels for a PO/SKU the warehouse needs — always new UIDs.
  function issueExtra(client, week, po, sku, qty, by, reason) {
    const ws = mondayOf(week);
    const sample = db.prepare(`SELECT style, ean, product, color, size, dept_code, format FROM uid_label
                                WHERE client_id = ? AND po_number = ? AND sku_code = ? ORDER BY issued_at DESC LIMIT 1`).get(client, po, sku);
    let fields = sample;
    if (!fields) {
      // Never issued (e.g. PO in the plan with zero planned qty): take the fields from the plan.
      const rows = planRowsFor(ws, client) || [];
      const r = rows.find(x => str(x.po_number) === po && str(x.sku_code) === sku);
      if (!r) throw new Error(`${po} / ${sku} is not in the ${isoWeek(ws)} plan and has never been issued.`);
      const code = deptOf(po);
      fields = { style: str(r.style), ean: str(r.ean), product: str(r.item_description), color: str(r.item_color), size: fmtSize(r.item_size), dept_code: code, format: formatFor(client, code) };
    }
    const tx = db.transaction(() => {
      const runId = db.prepare(`INSERT INTO uid_label_run (client_id, week_start, kind, trigger_by, labels, summary)
                                VALUES (?, ?, 'extra', ?, ?, ?)`)
        .run(client, ws, by || 'user', qty, JSON.stringify({ pos: [{ po, format: fields.format, labels: qty, skus: 1 }], moved: [], over: [], new_codes: [], reason: str(reason) || null, sku })).lastInsertRowid;
      const ins = db.prepare(`INSERT INTO uid_label (uid, client_id, week_start, po_number, sku_code, style, ean, product, color, size, dept_code, format, run_id, seq)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      const taken = scannedTokens(client), exists = existsStmt();
      for (let i = 1; i <= qty; i++) {
        ins.run(mintOne(taken, exists), client, ws, po, sku, fields.style, fields.ean, fields.product, fields.color, fields.size, fields.dept_code, fields.format, runId, i);
      }
      return runId;
    });
    const runId = tx();
    log.log(`[uid-labels] extra run #${runId} ${client} ${ws}: ${qty} label(s) for ${po} / ${sku} by ${by}`);
    return runId;
  }

  // ════════════════════════════════════════════════════════════════════════════════════════
  // Rendering — two layouts, copied constant for constant from the originals.
  // ════════════════════════════════════════════════════════════════════════════════════════
  const MM = 72 / 25.4;                      // points per millimetre
  const mm = (v) => v * MM;

  function dmModules(payload) {
    if (!bwipjs) throw new Error('bwip-js is not installed — add it to api/package.json');
    const r = bwipjs.raw({ bcid: 'datamatrix', text: payload })[0];
    return { w: r.pixx, h: r.pixy, pixs: r.pixs };
  }
  function c128Bars(text) {
    if (!bwipjs) throw new Error('bwip-js is not installed — add it to api/package.json');
    const r = bwipjs.raw({ bcid: 'code128', text: String(text) })[0];
    // sbs: alternating bar/space module widths starting with a bar.
    const bars = []; let x = 0;
    for (let i = 0; i < r.sbs.length; i++) { const w = r.sbs[i]; if (i % 2 === 0) bars.push([x, w]); x += w; }
    return { modules: x, bars };
  }
  // Draw a DataMatrix as filled squares. `box` is the full square the modules fill (edge to
  // edge, after the quiet zone the caller has already allowed for).
  function drawDM(doc, dm, x, yTop, box) {
    const cell = box / Math.max(dm.w, dm.h);
    doc.save().fillColor('#000');
    for (let r = 0; r < dm.h; r++) for (let c = 0; c < dm.w; c++) {
      if (dm.pixs[r * dm.w + c]) doc.rect(x + c * cell, yTop + r * cell, cell + 0.02, cell + 0.02).fill();
    }
    doc.restore();
  }
  function drawBars(doc, bc, x, yTop, moduleW, height, color) {
    doc.save().fillColor(color || '#000');
    for (const [bx, bw] of bc.bars) doc.rect(x + bx * moduleW, yTop, bw * moduleW + 0.01, height).fill();
    doc.restore();
  }

  // ── Standard 76×36 mm (landscape) — the canvas generator at 300 dpi, in points ──
  // Every constant below is the original's, converted px→pt (300 dpi → 72 pt/in).
  const STD = {
    W: mm(76), H: mm(36), pad: mm(3), dmTarget: mm(16), qz: mm(1), lineStep: mm(3.0), colorOff: mm(3.6),
    headPt: 8.4, bodyPt: 7.2, skuMaxPt: 13.2, skuMinPt: 8.4, bcHeight: mm(9), bcBottomPad: mm(1),
    dpi: 300,
  };
  const px2pt = (px) => px * 72 / 300;
  const stdSkuLines = (sku) => {
    const s = String(sku || ''); const di = s.indexOf('-');
    if (di > 0 && di < s.length - 1) return [s.slice(0, di), s.slice(di + 1)];
    const mid = Math.ceil(s.length / 2); return [s.slice(0, mid), s.slice(mid)];
  };
  function drawStandard(doc, L) {
    const { W, H, pad } = STD;
    doc.font('Helvetica');
    // Left column: product (truncated at 20 chars), colour, size, style, UID — top-aligned.
    doc.fontSize(STD.headPt).text(String(L.product || '').slice(0, 20), pad, pad, { lineBreak: false, baseline: 'top' });
    doc.fontSize(STD.bodyPt);
    const colorY = pad + STD.colorOff, sizeY = colorY + STD.lineStep, styleY = sizeY + STD.lineStep, uidY = styleY + STD.lineStep;
    if (L.color && L.color.toLowerCase() !== 'size') doc.text(L.color, pad, colorY, { lineBreak: false, baseline: 'top' });
    if (L.size) doc.text('Size: ' + L.size, pad, sizeY, { lineBreak: false, baseline: 'top' });
    if (L.style) doc.text(L.style, pad, styleY, { lineBreak: false, baseline: 'top' });
    doc.text(L.uid, pad, uidY, { lineBreak: false, baseline: 'top' });

    // DataMatrix top-right. The original asked bwip-js (browser build) for scales 8 down to 2
    // and kept the first whose canvas fitted inside 16 mm (189 px at 300 dpi), falling back to
    // scale 3. That build draws 2 px per module per unit of scale, so a 24-module symbol lands
    // on 6 px/module (144 px), not 7. Reproduced exactly, then drawn with a 1 mm white border.
    const payload = `${L.style}-${L.sku};${L.uid}`;
    const dm = dmModules(payload);
    const targetPx = Math.round(16 / 25.4 * 300);
    let pxPerModule = 3 * 2;                                    // the original's fallback (scale 3)
    for (const sc of [8, 7, 6, 5, 4, 3, 2]) { if (dm.w * sc * 2 <= targetPx) { pxPerModule = sc * 2; break; } }
    const dmPt = px2pt(dm.w * pxPerModule);
    const dx = W - pad - (dmPt + STD.qz * 2), dy = pad;
    drawDM(doc, dm, dx + STD.qz, dy + STD.qz, dmPt);
    const dmBottom = dy + STD.qz + dmPt, dmDrawnW = dmPt + STD.qz * 2;

    // SKU in two lines under the symbol, right-aligned, sized down until it fits the symbol's width + 4 mm.
    const [l1, l2] = stdSkuLines(L.sku);
    const availW = dmDrawnW + mm(4), side = mm(0.5);
    let pt = STD.skuMaxPt;
    const fits = (p) => { doc.fontSize(p); return Math.max(doc.widthOfString(l1), doc.widthOfString(l2)) <= availW - side; };
    while (pt > STD.skuMinPt && !fits(pt)) pt -= 0.4;
    doc.fontSize(pt);
    const ty = dmBottom + mm(3);
    doc.text(l1, W - pad - doc.widthOfString(l1), ty, { lineBreak: false, baseline: 'top' });
    doc.text(l2, W - pad - doc.widthOfString(l2), ty + pt * 1.1, { lineBreak: false, baseline: 'top' });

    // Code128 of the SKU along the bottom-left. The original tried bar widths of 0.50, 0.45,
    // 0.40 and 0.33 mm (as whole pixels: 6, 5, 5, 4) with 1.5 mm then 1 mm margins and kept
    // the first that fitted the label width.
    const bc = c128Bars(L.sku);
    const barPx = [6, 5, 5, 4], marginPx = [18, 12];
    const fullW = W - pad * 2;
    let chosen = null;
    outer: for (const m of marginPx) for (const b of barPx) {
      const wPt = px2pt(m * 2 + bc.modules * b);
      if (wPt <= fullW) { chosen = { m, b }; break outer; }
    }
    if (!chosen) chosen = { m: 12, b: 4 };
    // JsBarcode's margin surrounds the bars on all four sides, so its canvas is the bar height
    // plus two margins tall; the original placed that canvas 1 mm off the bottom and painted
    // it white first (which is what hides part of a long SKU's second line).
    const bcH = STD.bcHeight, bcW = px2pt(chosen.m * 2 + bc.modules * chosen.b), canvasH = bcH + 2 * px2pt(chosen.m);
    const bcX = pad, bcY = H - canvasH - STD.bcBottomPad;
    doc.save().fillColor('#fff').rect(bcX, bcY, bcW, canvasH).fill().restore();
    drawBars(doc, bc, bcX + px2pt(chosen.m), bcY + px2pt(chosen.m), px2pt(chosen.b), bcH, '#000');
  }

  // ── Footwear 36×76 mm (portrait) — the reportlab generator, in points, y measured from the top ──
  const FW = {
    W: mm(36), H: mm(76), headPt: 8.0, bodyPt: 7.0, bigPt: 16.0,
    sideMargin: mm(3.0), bcSideMargin: mm(0.2), dmSize: mm(20.0), dmQuiet: 2 /* modules, as pystrich */,
    uidGap: mm(4.5), bcTopGap: mm(7.0), bcHeight: mm(5.0), hrGap: mm(2.5), dividerGap: mm(2.5), textTopGap: mm(9.5),
    bottomDmPad: mm(1.0),
    // reportlab Code128 defaults: barWidth 0.0075 in, quiet zones max(0.25 in, 10 × barWidth).
    rlBarW: 0.0075 * 72, rlQuiet: Math.max(0.25 * 72, 10 * 0.0075 * 72),
  };
  const fwSplitSku = (sku) => { const s = str(sku); return s.length > 3 ? [s.slice(0, -3), s.slice(-3)] : ['', s]; };
  // reportlab baselines → pdfkit: pdfkit's `baseline:'alphabetic'` puts the baseline at y.
  function fwText(doc, s, x, baselineY, size, align) {
    doc.font('Helvetica-Bold').fontSize(size);
    const w = doc.widthOfString(s);
    const xx = align === 'right' ? x - w : align === 'center' ? x - w / 2 : x;
    doc.text(s, xx, baselineY, { lineBreak: false, baseline: 'alphabetic' });
  }
  function fwWrap(doc, text, maxW, size, maxLines) {
    doc.font('Helvetica-Bold').fontSize(size);
    const words = str(text).split(/\s+/).filter(Boolean);
    const lines = []; let cur = '';
    for (const w of words) {
      const trial = (cur + ' ' + w).trim();
      if (doc.widthOfString(trial) <= maxW) cur = trial;
      else { if (cur) lines.push(cur); cur = w; if (lines.length >= maxLines) break; }
    }
    if (cur && lines.length < maxLines) lines.push(cur);
    return lines;
  }
  function drawFootwear(doc, L) {
    const { W, H } = FW;
    const ean = L.ean || L.sku;
    const payload = (L.style && L.sku && L.uid) ? `${L.style}-${L.sku};${L.uid}` : '';
    const dm = payload ? dmModules(payload) : null;
    // DataMatrix flush to the top-left and bottom-left corners, stretched to the 20 mm box.
    // pystrich's PNG carried a 2-module quiet zone, so the symbol sits inside the box with
    // that border; the box itself is what is 20 mm.
    const dmBox = FW.dmSize;
    const dmDraw = (yTop) => {
      if (!dm) return;
      const cell = dmBox / (dm.w + FW.dmQuiet * 2);
      doc.save().fillColor('#fff').rect(0, yTop, dmBox, dmBox).fill().restore();
      drawDM(doc, dm, FW.dmQuiet * cell, yTop + FW.dmQuiet * cell, cell * dm.w);
    };
    const topDmY = 0;
    dmDraw(topDmY);
    // SKU beside the top symbol: last three characters big, the rest small above, right-aligned.
    const [small, big] = fwSplitSku(L.sku);
    const skuX = W - FW.sideMargin;
    const centerTop = topDmY + dmBox / 2;
    const bigYTop = centerTop + FW.bigPt * 0.4;          // reportlab: centre − 0.4·pt (y up) → centre + 0.4·pt (y down)
    if (big) fwText(doc, big, skuX, bigYTop, FW.bigPt, 'right');
    else if (L.sku) fwText(doc, L.sku, skuX, bigYTop, FW.bigPt, 'right');
    const smallYTop = bigYTop - mm(5.0);
    if (small) fwText(doc, small, skuX, smallYTop, FW.bodyPt, 'right');

    // UID centred under the symbol.
    const uidY = topDmY + dmBox + FW.uidGap;
    if (L.uid) fwText(doc, L.uid, W / 2, uidY, FW.bodyPt, 'center');

    // Code128 of the EAN (or SKU): reportlab's symbol — quiet zones included — scaled so its
    // total width is 1.2 × 1.3 × (label − 0.4 mm), centred. The quiet zones fall off the label;
    // the bars stay inside it. Dark grey, as the original.
    const bc = c128Bars(ean || '000');
    const rlWidth = bc.modules * FW.rlBarW + 2 * FW.rlQuiet;
    const target = 1.3 * (W - 2 * FW.bcSideMargin);
    const desired = 1.2 * target;
    const sx = desired / rlWidth;
    const bcBottom = uidY + FW.bcTopGap;                   // reportlab: bars drawn upward from bc_y
    const bcTop = bcBottom - FW.bcHeight;
    const x0 = W / 2 - desired / 2 + FW.rlQuiet * sx;
    doc.save().rect(0, 0, W, H).clip();
    drawBars(doc, bc, x0, bcTop, FW.rlBarW * sx, FW.bcHeight, '#737373');
    doc.restore();

    // Human-readable digits, divider, then product / colour / size.
    const hrY = bcBottom + FW.hrGap;
    if (ean) fwText(doc, ean, W / 2, hrY, FW.bodyPt, 'center');
    const dividerY = hrY + FW.dividerGap;
    doc.save().lineWidth(0.4).strokeColor('#000').moveTo(0, dividerY).lineTo(W, dividerY).stroke().restore();
    let textY = dividerY + FW.textTopGap;
    const maxW = W - 2 * FW.sideMargin;
    for (const line of fwWrap(doc, L.product, maxW, FW.bodyPt, 2)) { fwText(doc, line, FW.sideMargin, textY, FW.bodyPt, 'left'); textY += FW.bodyPt * 1.4; }
    if (L.color) { fwText(doc, L.color, FW.sideMargin, textY, FW.bodyPt, 'left'); textY += FW.bodyPt * 1.4; }
    if (L.size) { fwText(doc, 'Size: ' + L.size, FW.sideMargin, textY, FW.bodyPt, 'left'); textY += FW.bodyPt * 1.4; }

    // Bottom symbol and SKU, mirrored.
    const bottomDmY = H - FW.bottomDmPad - dmBox;
    dmDraw(bottomDmY);
    const centerBottom = bottomDmY + dmBox / 2;
    const bigYBottom = centerBottom + FW.bigPt * 0.4;
    if (big) fwText(doc, big, skuX, bigYBottom, FW.bigPt, 'right');
    else if (L.sku) fwText(doc, L.sku, skuX, bigYBottom, FW.bigPt, 'right');
    if (small) fwText(doc, small, skuX, bigYBottom - mm(5.0), FW.bodyPt, 'right');
  }

  // One PDF, one page per label. Returns a Buffer.
  function renderPdf(labels, format) {
    const fw = format === 'footwear';
    const size = fw ? [FW.W, FW.H] : [STD.W, STD.H];
    return new Promise((resolve, reject) => {
      const doc = new PDFDocument({ size, margin: 0, autoFirstPage: false, compress: true,
        info: { Title: `UID labels (${fw ? 'footwear 36×76' : 'standard 76×36'})`, Author: 'VelOzity Pinpoint' } });
      const chunks = [];
      doc.on('data', (c) => chunks.push(c));
      doc.on('end', () => resolve(Buffer.concat(chunks)));
      doc.on('error', reject);
      // A PO can run to thousands of labels; yielding every 50 pages keeps the API answering
      // other requests while a big file builds.
      (async () => {
        try {
          let n = 0;
          for (const L of labels) {
            doc.addPage({ size, margin: 0 });
            if (fw) drawFootwear(doc, L); else drawStandard(doc, L);
            if (++n % 50 === 0) await new Promise(r => setImmediate(r));
          }
          doc.end();
        } catch (e) { reject(e); }
      })();
    });
  }

  const labelRow = (r) => ({ uid: r.uid, po: r.po_number, sku: r.sku_code, style: r.style, ean: r.ean, product: r.product, color: r.color, size: r.size, format: r.format });

  // The PDF for one PO within one run — rendered once, cached in the database.
  async function fileFor(client, runId, po) {
    const cached = db.prepare('SELECT bytes, labels FROM uid_label_file WHERE run_id = ? AND po_number = ?').get(runId, po);
    if (cached) return { bytes: cached.bytes, labels: cached.labels };
    const rows = db.prepare(`SELECT * FROM uid_label WHERE run_id = ? AND po_number = ? AND client_id = ? ORDER BY seq`).all(runId, po, client);
    if (!rows.length) return null;
    const formats = [...new Set(rows.map(r => r.format))];
    // A PO has one department code, so one format; defensive all the same.
    const bytes = await renderPdf(rows.map(labelRow), formats[0]);
    db.prepare(`INSERT OR REPLACE INTO uid_label_file (run_id, po_number, bytes, labels) VALUES (?, ?, ?, ?)`).run(runId, po, bytes, rows.length);
    return { bytes, labels: rows.length };
  }

  // ── Email ──
  function recipients() {
    const split = (s) => String(s || '').split(/[,;\s]+/).map(x => x.trim()).filter(Boolean);
    return { to: split(process.env.UID_LABEL_TO), cc: split(process.env.UID_LABEL_CC),
             from: process.env.UID_LABEL_FROM || process.env.ELEVATE_FROM || process.env.WEEKLY_REPORT_FROM
                || process.env.MONTHLY_CLIENT_REPORT_FROM || process.env.EXCEPTION_EMAIL_FROM || null };
  }
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  function emailHtml(run, summary, week) {
    const wk = isoWeek(week);
    const pos = summary.pos || [];
    const fw = pos.filter(p => p.format === 'footwear'), st = pos.filter(p => p.format !== 'footwear');
    const list = (arr) => arr.map(p => `<tr><td style="padding:4px 10px 4px 0;font-family:ui-monospace,Menlo,monospace">${esc(p.po)}</td><td style="padding:4px 10px;text-align:right">${p.labels}</td><td style="padding:4px 0 4px 10px;color:#6E6E73">${p.skus} SKU${p.skus === 1 ? '' : 's'}</td></tr>`).join('');
    const block = (title, arr) => arr.length ? `<h3 style="margin:18px 0 6px;font-size:13px">${title} · ${arr.reduce((a, p) => a + p.labels, 0)} labels</h3><table style="border-collapse:collapse;font-size:13px">${list(arr)}</table>` : '';
    const notes = [];
    if (summary.moved && summary.moved.length) notes.push(`${summary.moved.length} PO line${summary.moved.length === 1 ? '' : 's'} moved from an earlier week — nothing printed for those; choose on the page whether to download again or issue new.`);
    if (summary.new_codes && summary.new_codes.length) notes.push(`New department code${summary.new_codes.length === 1 ? '' : 's'} ${summary.new_codes.map(c => c.code).join(', ')} printed as standard — review on the page.`);
    if (summary.over && summary.over.length) notes.push(`${summary.over.length} PO line${summary.over.length === 1 ? '' : 's'} now plan fewer units than already issued — nothing printed, shown as over-issued.`);
    const kind = run.kind === 'extra' ? `Extra labels requested by ${esc(run.trigger_by || 'the team')}` : 'Plan upload';
    return `<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1C1C1E;max-width:640px">
      <p style="margin:0 0 4px;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#6E6E73">Pinpoint · UID labels</p>
      <h2 style="margin:0 0 10px;font-size:20px">${wk} — ${run.labels} label${run.labels === 1 ? '' : 's'} ready to print</h2>
      <p style="margin:0 0 14px;font-size:14px;color:#6E6E73">${kind} · run #${run.id}</p>
      ${block('Footwear · 36×76', fw)}${block('Standard · 76×36', st)}
      ${notes.length ? `<ul style="margin:16px 0;padding-left:18px;font-size:13px">${notes.map(n => `<li>${esc(n)}</li>`).join('')}</ul>` : ''}
      <p style="margin:22px 0 0"><a href="${PAGE_URL}" style="display:inline-block;padding:11px 18px;border-radius:9px;background:#1C1C1E;color:#fff;text-decoration:none;font-weight:600;font-size:14px">Open the print queue →</a></p>
      <p style="margin:14px 0 0;font-size:12px;color:#6E6E73">One PDF per PO. Print at 100% scale, no fit-to-page.</p>
    </div>`;
  }
  async function emailRun(runId) {
    const run = db.prepare('SELECT * FROM uid_label_run WHERE id = ?').get(runId);
    if (!run) return { sent: false, reason: 'no_run' };
    const r = recipients();
    if (!r.to.length || !r.from) {
      db.prepare('UPDATE uid_label_run SET email_error = ? WHERE id = ?').run('UID_LABEL_TO / from address not set', runId);
      return { sent: false, reason: 'no_recipients' };
    }
    const summary = JSON.parse(run.summary || '{}');
    const subject = `UID labels ${isoWeek(run.week_start)} — ${run.labels} ready${run.kind === 'extra' ? ' (extra)' : ''}`;
    try {
      const out = await sendViaResend({ from: r.from, to: r.to, cc: r.cc, subject, html: emailHtml(run, summary, run.week_start) });
      db.prepare(`UPDATE uid_label_run SET emailed_at = datetime('now'), email_error = NULL WHERE id = ?`).run(runId);
      return { sent: true, id: out && out.id };
    } catch (e) {
      db.prepare('UPDATE uid_label_run SET email_error = ? WHERE id = ?').run(String(e.message || e), runId);
      log.warn('[uid-labels] email failed:', e.message);
      return { sent: false, reason: String(e.message || e) };
    }
  }

  // ── Hook called by the plan upload ──
  // Non-fatal by contract: the plan is already saved; anything that goes wrong here is logged
  // and shown on the page, never surfaced as an upload failure.
  function onPlanSaved(client, monday, rows, by) {
    try {
      const out = reconcile(client, monday, rows, by || 'plan upload');
      if (out.run_id) emailRun(out.run_id).catch(() => {});
      return out;
    } catch (e) {
      log.error('[uid-labels] reconcile failed (non-fatal):', e);
      return { ok: false, error: String(e.message || e) };
    }
  }

  // ════ Routes ════
  const guard = [authenticateRequest, internalOnly];

  // Weeks with anything: runs, issued counts, planned counts.
  router.get('/weeks', ...guard, auditLog('view_uid_labels'), (req, res) => {
    try {
      const client = curClient();
      seedFormats(client);
      const weeks = db.prepare(`SELECT week_start, COUNT(*) runs, SUM(labels) labels, MAX(created_at) last_at
                                  FROM uid_label_run WHERE client_id = ? GROUP BY week_start ORDER BY week_start DESC LIMIT 30`).all(client);
      const newCodes = db.prepare(`SELECT COUNT(*) n FROM uid_label_format WHERE client_id = ? AND is_new = 1`).get(client).n;
      res.json({ ok: true, weeks: weeks.map(w => ({ ...w, label: isoWeek(w.week_start) })), new_codes: newCodes, bwip: !!bwipjs });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // One week in full: every PO with planned vs issued, every run with its files, moves.
  router.get('/week/:ws', ...guard, auditLog('view_uid_labels_week'), (req, res) => {
    try {
      const client = curClient();
      if (!ymdOk(req.params.ws)) return res.status(400).json({ ok: false, error: 'bad week' });
      const ws = mondayOf(req.params.ws);
      const plan = planRowsFor(ws, client) || [];
      const planned = new Map();
      for (const r of plan) {
        const po = str(r.po_number), sku = str(r.sku_code); if (!po || !sku) continue;
        const k = po + '\u0000' + sku;
        planned.set(k, (planned.get(k) || 0) + Math.max(0, Math.floor(Number(r.target_qty) || 0)));
      }
      const issued = db.prepare(`SELECT po_number po, sku_code sku, format, dept_code, COUNT(*) n, MIN(moved_from) moved_from, MAX(issued_at) last_at
                                   FROM uid_label WHERE client_id = ? AND week_start = ? GROUP BY po_number, sku_code`).all(client, ws);
      const lines = new Map();
      for (const [k, qty] of planned) { const [po, sku] = k.split('\u0000'); lines.set(k, { po, sku, planned: qty, issued: 0, format: null, dept: deptOf(po), moved_from: null }); }
      for (const i of issued) {
        const k = i.po + '\u0000' + i.sku;
        const cur = lines.get(k) || { po: i.po, sku: i.sku, planned: 0, issued: 0, format: null, dept: i.dept_code, moved_from: null };
        cur.issued = i.n; cur.format = i.format; cur.moved_from = i.moved_from || null; cur.dept = i.dept_code || cur.dept;
        lines.set(k, cur);
      }
      const fmtOf = new Map(db.prepare('SELECT code, format, is_new FROM uid_label_format WHERE client_id = ?').all(client).map(r => [r.code, r]));
      const pos = new Map();
      for (const l of lines.values()) {
        const f = fmtOf.get(l.dept);
        l.format = l.format || (f ? f.format : 'standard');
        l.dept_new = !!(f && f.is_new);
        const p = pos.get(l.po) || { po: l.po, dept: l.dept, format: l.format, dept_new: l.dept_new, planned: 0, issued: 0, skus: 0, moved_from: null, lines: [] };
        p.planned += l.planned; p.issued += l.issued; p.skus += 1; p.lines.push(l);
        if (l.moved_from) p.moved_from = l.moved_from;
        pos.set(l.po, p);
      }
      const runs = db.prepare(`SELECT * FROM uid_label_run WHERE client_id = ? AND week_start = ? ORDER BY id DESC`).all(client, ws).map(r => {
        const files = db.prepare(`SELECT po_number po, format, COUNT(*) labels, MIN(seq) seq FROM uid_label WHERE run_id = ? GROUP BY po_number ORDER BY po_number`).all(r.id);
        const reprints = db.prepare(`SELECT po_number po, COUNT(*) n, MAX(at) last_at, MAX(by_user) by_user FROM uid_label_reprint WHERE run_id = ? GROUP BY po_number`).all(r.id);
        const rp = new Map(reprints.map(x => [x.po, x]));
        return { id: r.id, kind: r.kind, by: r.trigger_by, at: r.created_at, labels: r.labels, emailed_at: r.emailed_at, email_error: r.email_error,
                 summary: JSON.parse(r.summary || '{}'),
                 files: files.map(f => ({ ...f, reprints: rp.get(f.po) ? rp.get(f.po).n : 0, last_reprint: rp.get(f.po) ? rp.get(f.po).last_at : null })) };
      });
      // Files from other weeks that moved here: the warehouse chooses on these.
      const movedFiles = db.prepare(`SELECT l.run_id, l.po_number po, l.moved_from, COUNT(*) labels, r.week_start run_week
                                       FROM uid_label l JOIN uid_label_run r ON r.id = l.run_id
                                      WHERE l.client_id = ? AND l.week_start = ? AND l.moved_from IS NOT NULL AND r.week_start <> ?
                                      GROUP BY l.run_id, l.po_number`).all(client, ws, ws);
      res.json({ ok: true, week_start: ws, label: isoWeek(ws), has_plan: planRowsFor(ws, client) != null,
                 pos: [...pos.values()].sort((a, b) => a.po.localeCompare(b.po)), runs, moved_files: movedFiles,
                 totals: { planned: [...lines.values()].reduce((a, l) => a + l.planned, 0), issued: [...lines.values()].reduce((a, l) => a + l.issued, 0) } });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // The PDF for one PO in one run. ?reprint=1 records that the sheet was printed again.
  router.get('/file/:runId/:po', ...guard, auditLog('download_uid_labels'), async (req, res) => {
    try {
      const client = curClient();
      const runId = Number(req.params.runId), po = str(req.params.po);
      const run = db.prepare('SELECT * FROM uid_label_run WHERE id = ? AND client_id = ?').get(runId, client);
      if (!run) return res.status(404).json({ ok: false, error: 'No such run.' });
      const f = await fileFor(client, runId, po);
      if (!f) return res.status(404).json({ ok: false, error: 'No labels for that PO in this run.' });
      if (String(req.query.reprint || '') === '1') db.prepare('INSERT INTO uid_label_reprint (run_id, po_number, by_user) VALUES (?, ?, ?)').run(runId, po, who(req));
      const fmt = db.prepare('SELECT format FROM uid_label WHERE run_id = ? AND po_number = ? LIMIT 1').get(runId, po);
      const name = `${safeName(po)}_${isoWeek(run.week_start)}_${fmt && fmt.format === 'footwear' ? '36x76' : '76x36'}_run${runId}.pdf`;
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="${name}"`);
      res.setHeader('X-Labels', String(f.labels));
      res.send(f.bytes);
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // Every label ever issued for a PO in a week, as one PDF (all runs). For a re-print of the
  // whole PO rather than one run's delta.
  router.get('/po/:ws/:po.pdf', ...guard, auditLog('download_uid_labels_po'), async (req, res) => {
    try {
      const client = curClient();
      if (!ymdOk(req.params.ws)) return res.status(400).json({ ok: false, error: 'bad week' });
      const ws = mondayOf(req.params.ws), po = str(req.params.po);
      const rows = db.prepare(`SELECT * FROM uid_label WHERE client_id = ? AND week_start = ? AND po_number = ? ORDER BY run_id, seq`).all(client, ws, po);
      if (!rows.length) return res.status(404).json({ ok: false, error: 'Nothing issued for that PO.' });
      const bytes = await renderPdf(rows.map(labelRow), rows[0].format);
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="${safeName(po)}_${isoWeek(ws)}_all.pdf"`);
      res.send(bytes);
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // Extras — new UIDs, never reprints.
  router.post('/extra', ...guard, requireRole(['admin', 'member', 'api']), limiter, auditLog('issue_uid_labels_extra'), async (req, res) => {
    try {
      const client = curClient();
      const b = req.body || {};
      const ws = ymdOk(b.week) ? mondayOf(b.week) : null;
      const po = str(b.po), sku = str(b.sku), qty = Math.floor(Number(b.qty) || 0);
      if (!ws) return res.status(400).json({ ok: false, error: 'week is required' });
      if (!po || !sku) return res.status(400).json({ ok: false, error: 'PO and SKU are required' });
      if (qty < 1 || qty > 5000) return res.status(400).json({ ok: false, error: 'qty must be between 1 and 5000' });
      const runId = issueExtra(client, ws, po, sku, qty, who(req), b.reason);
      emailRun(runId).catch(() => {});
      res.json({ ok: true, run_id: runId, labels: qty });
    } catch (e) { res.status(400).json({ ok: false, error: String(e.message || e) }); }
  });

  // Re-run the delta for a week by hand (e.g. after fixing a department code, or if an upload's
  // run failed). Same rules as the upload hook: issues only what the ledger lacks.
  router.post('/reconcile/:ws', ...guard, requireRole(['admin', 'member', 'api']), limiter, auditLog('reconcile_uid_labels'), async (req, res) => {
    try {
      const client = curClient();
      if (!ymdOk(req.params.ws)) return res.status(400).json({ ok: false, error: 'bad week' });
      const ws = mondayOf(req.params.ws);
      const rows = planRowsFor(ws, client);
      if (rows == null) return res.status(404).json({ ok: false, error: `No plan stored for ${isoWeek(ws)}.` });
      const out = reconcile(client, ws, rows, `Check plan now · ${who(req)}`);
      if (out.run_id) emailRun(out.run_id).catch(() => {});
      res.json(out);
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // Department codes → format.
  router.get('/formats', ...guard, (req, res) => {
    try {
      const client = curClient();
      seedFormats(client);
      const rows = db.prepare(`SELECT f.code, f.format, f.is_new, f.source, f.name, f.updated_at, f.updated_by,
                                      (SELECT COUNT(*) FROM uid_label l WHERE l.client_id = f.client_id AND l.dept_code = f.code) issued
                                 FROM uid_label_format f WHERE f.client_id = ? ORDER BY f.is_new DESC, f.format DESC, f.code`).all(client);
      let owners = new Map();
      try { owners = new Map(db.prepare('SELECT code, name FROM department_owner').all().map(r => [r.code, r.name])); } catch (_) {}
      res.json({ ok: true, codes: rows.map(r => ({ ...r, name: r.name || owners.get(r.code) || null })) });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  router.post('/formats', ...guard, requireRole(['admin', 'member', 'api']), limiter, auditLog('edit_uid_label_format'), (req, res) => {
    try {
      const client = curClient();
      const b = req.body || {};
      // One code, or a pasted list: "WFDA, WFAT MAEF" → each becomes `format`.
      const codes = [].concat(b.codes || b.code || []).flatMap(s => String(s).split(/[\s,;]+/)).map(s => s.trim().toUpperCase()).filter(Boolean);
      const format = b.format === 'footwear' ? 'footwear' : 'standard';
      const bad = codes.filter(c => !/^[A-Z0-9]{4}$/.test(c));
      if (!codes.length) return res.status(400).json({ ok: false, error: 'a department code is required' });
      if (bad.length) return res.status(400).json({ ok: false, error: `not a 4-character department code: ${bad.join(', ')}` });
      const up = db.prepare(`INSERT INTO uid_label_format (client_id, code, format, is_new, source, updated_at, updated_by)
                             VALUES (?, ?, ?, 0, 'manual', datetime('now'), ?)
                             ON CONFLICT(client_id, code) DO UPDATE SET format = excluded.format, is_new = 0, updated_at = excluded.updated_at, updated_by = excluded.updated_by`);
      const tx = db.transaction(() => { for (const c of codes) up.run(client, c, format, who(req)); });
      tx();
      res.json({ ok: true, codes, format });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  router.delete('/formats/:code', ...guard, requireRole(['admin', 'member', 'api']), limiter, auditLog('remove_uid_label_format'), (req, res) => {
    try {
      const client = curClient();
      const code = str(req.params.code).toUpperCase();
      const n = db.prepare('SELECT COUNT(*) n FROM uid_label WHERE client_id = ? AND dept_code = ?').get(client, code).n;
      if (n) return res.status(409).json({ ok: false, error: `${code} has ${n} labels issued — set it to standard instead of removing it.` });
      db.prepare('DELETE FROM uid_label_format WHERE client_id = ? AND code = ?').run(client, code);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // Send (or resend) the notification for a run.
  router.post('/run/:id/email', ...guard, requireRole(['admin', 'member', 'api']), limiter, auditLog('email_uid_labels'), async (req, res) => {
    try {
      const client = curClient();
      const run = db.prepare('SELECT id FROM uid_label_run WHERE id = ? AND client_id = ?').get(Number(req.params.id), client);
      if (!run) return res.status(404).json({ ok: false, error: 'No such run.' });
      res.json(await emailRun(run.id));
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // A sample sheet of each format with made-up data — for checking the printer and scanner
  // before the first real run.
  router.get('/sample/:format.pdf', ...guard, async (req, res) => {
    try {
      const fw = req.params.format === 'footwear';
      const L = fw
        ? { uid: 'TIBC' + randomBody(), po: 'WFDA000123', sku: '2301234567890', style: 'ATMOS-2024', ean: '9312345678901', product: 'Atmos & Here Leather Ankle Boot', color: 'Black', size: '38', format: 'footwear' }
        : { uid: 'TIBC' + randomBody(), po: 'WAAT003171', sku: 'WAAT-1234567', style: 'DAZIE-SS26', ean: '', product: 'Dazie Linen Blend Wrap Midi Dress', color: 'Sage', size: '10', format: 'standard' };
      const bytes = await renderPdf([L, { ...L, uid: 'TIBC' + randomBody() }], fw ? 'footwear' : 'standard');
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `inline; filename="sample_${fw ? 'footwear_36x76' : 'standard_76x36'}.pdf"`);
      res.send(bytes);
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  return Object.assign(router, { onPlanSaved, reconcile, issueExtra, renderPdf, emailRun, _internal: { mintOne, scannedTokens, UID_RE, mondayOf, isoWeek } });
};
