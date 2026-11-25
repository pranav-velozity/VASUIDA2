// Signals — what the supply chain actually does, measured nightly from Pinpoint's own records.
//
// Everything here is arithmetic over plans, receiving, Transit Movements (and the lane dates
// kept before the Oct 2026 cutover), invoices and approved air quotes. Nothing is estimated by
// a model. The Performance page shows these numbers to the client and to VelOzity; Pulse reads
// them through get_signals; the door-to-door simulator runs on them.
//
// Definitions (agreed 24 Nov 2026):
//   window        last 26 weeks, rolling; trend = last 4 weeks vs the 22 before
//   minimum       4 weeks of plan for a supplier, 5 movements for a lane; below that: "not enough history"
//   fill rate     units applied ÷ units planned for the supplier's POs in that plan week, counted
//                 3+ weeks after the week (so VAS has had time); capped at 100% per PO
//   PO received   a receiving row with a received-at for the PO; on time if received ≤ the PO's
//                 due date (else late); not received 3 weeks after the week's FC date = short
//   readiness     packing-list-ready actual vs planned, from the consignment milestone or the
//                 legacy lane dates; on time = actual ≤ planned; days late = calendar days after
//   transit       actual departed → actual arrived, both actuals; outliers beyond 2.5× the median
//                 excluded and counted; quoted = the carrier figure on the same movement (paired)
//   door-to-door  packing-list-ready → FC receipt, actuals only; each stage's slip vs its plan
//   volume        weekly planned units/POs by supplier and mode; 26-week average, last 8 weighted ×2
//   price         sea: the week's SEA invoice (subtotal + customs) ÷ sea units planned, and per
//                 container; air: approved quotes, sell ÷ chargeable kg. Cost and margin VelOzity-only.
//   confidence    sample size and window on every figure; nothing extrapolated past the data
//
//   GET  /signals/overview?weeks=26     suppliers, lanes, volume, price, run meta (cost/margin stripped for client orgs)
//   GET  /signals/supplier/:name        weekly history for one supplier
//   POST /signals/run                   (VelOzity admin) recompute now
//   SIGNALS_RUN_HOUR                    hour (Australia/Sydney) for the nightly run, default 2
'use strict';

module.exports = function install(deps) {
  const { app, db, authenticateRequest, requireRole, requireInternalOrg, curClient, tenancyResolve, log = console } = deps;
  const WEEKS = Number(process.env.SIGNALS_WINDOW_WEEKS || 26);
  const DEFS_VERSION = '2026-11-25c';   // bump when a definition changes: the boot run recomputes on a new version
  const MIN_SUPPLIER_WEEKS = 4, MIN_LANE_N = 5, LATE_GRACE_DAYS = 21, MATURE_DAYS = 21;

  db.exec(`
    CREATE TABLE IF NOT EXISTS signal_run (id INTEGER PRIMARY KEY AUTOINCREMENT, client_id TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, weeks INTEGER, notes TEXT);
    CREATE TABLE IF NOT EXISTS signal_supplier_week (
      client_id TEXT NOT NULL, supplier TEXT NOT NULL, week_start TEXT NOT NULL,
      planned_units INTEGER, applied_units INTEGER, fill_rate REAL, mature INTEGER,
      pos INTEGER, received_pos INTEGER, on_time_pos INTEGER, late_pos INTEGER, short_pos INTEGER, pending_pos INTEGER,
      ready_n INTEGER, ready_on_time INTEGER, ready_late_days_p50 REAL,
      computed_at TEXT, PRIMARY KEY (client_id, supplier, week_start));
    CREATE TABLE IF NOT EXISTS signal_supplier (
      client_id TEXT NOT NULL, supplier TEXT NOT NULL,
      weeks INTEGER, pos INTEGER, planned_units INTEGER, applied_units INTEGER,
      fill_rate REAL, fill_rate_recent REAL, received_rate REAL, on_time_rate REAL, late_days_p50 REAL, late_days_p90 REAL, short_rate REAL,
      ready_n INTEGER, ready_on_time_rate REAL, ready_late_p50 REAL, ready_late_p90 REAL,
      trend TEXT, confidence TEXT, computed_at TEXT, PRIMARY KEY (client_id, supplier));
    CREATE TABLE IF NOT EXISTS signal_lane (
      client_id TEXT NOT NULL, grain TEXT NOT NULL, key TEXT NOT NULL, mode TEXT, carrier TEXT, supplier TEXT,
      n INTEGER, transit_n INTEGER, transit_p50 REAL, transit_p90 REAL, quoted_p50 REAL, slip_p50 REAL, outliers INTEGER,
      d2d_n INTEGER, d2d_p50 REAL, d2d_p90 REAL,
      stage_json TEXT, slowest_stage TEXT, legacy_n INTEGER, confidence TEXT, computed_at TEXT, PRIMARY KEY (client_id, grain, key));
    CREATE TABLE IF NOT EXISTS signal_volume_week (client_id TEXT NOT NULL, week_start TEXT NOT NULL, supplier TEXT NOT NULL, mode TEXT NOT NULL, units INTEGER, pos INTEGER, cartons INTEGER, PRIMARY KEY (client_id, week_start, supplier, mode));
    CREATE TABLE IF NOT EXISTS signal_price_week (client_id TEXT NOT NULL, week_start TEXT NOT NULL, mode TEXT NOT NULL,
      units INTEGER, price REAL, customs REAL, containers INTEGER, price_per_unit REAL, price_per_container REAL,
      kg REAL, price_per_kg REAL, cost REAL, margin_pct REAL, source TEXT, PRIMARY KEY (client_id, week_start, mode));
  `);

  // ── helpers ──
  const iso = () => new Date().toISOString();
  const ymd = (d) => d.toISOString().slice(0, 10);
  const addDays = (s, n) => { const d = new Date(String(s).slice(0, 10) + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return ymd(d); };
  const mondayOf = (s) => { const d = new Date(String(s).slice(0, 10) + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7)); return ymd(d); };
  const daysBetween = (a, b) => { if (!a || !b) return null; const x = new Date(String(a).slice(0, 10) + 'T00:00:00Z'), y = new Date(String(b).slice(0, 10) + 'T00:00:00Z'); if (isNaN(x) || isNaN(y)) return null; return Math.round((y - x) / 86400000); };
  const pct = (a, b) => (b > 0 ? Math.round(a / b * 1000) / 10 : null);
  const r1 = (v) => v == null || !isFinite(v) ? null : Math.round(v * 10) / 10;
  const r2 = (v) => v == null || !isFinite(v) ? null : Math.round(v * 100) / 100;
  const quantile = (xs, q) => { if (!xs.length) return null; const s = xs.slice().sort((a, b) => a - b); const pos = (s.length - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos); return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (pos - lo); };
  const median = (xs) => quantile(xs, 0.5);
  const hasCol = (table, col) => { try { return db.prepare(`PRAGMA table_info(${table})`).all().some(c => c.name === col); } catch (_) { return false; } };
  // v2 (2026-11-25): delivery accuracy (over / under against plan) and compliance from the
  // Exception log. The signal tables are rebuilt every run, so missing columns are simply added.
  const addCols = (table, cols) => { for (const [c, t] of cols) if (!hasCol(table, c)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${c} ${t}`); };
  addCols('signal_supplier_week', [['over_units', 'INTEGER'], ['under_units', 'INTEGER'], ['accuracy_n', 'INTEGER'], ['within_pos', 'INTEGER'], ['nc_incidents', 'INTEGER'], ['nc_pos', 'INTEGER'], ['nc_units', 'INTEGER'], ['nc_cartons', 'INTEGER'], ['cartons_received', 'INTEGER']]);
  addCols('signal_supplier', [['over_pct', 'REAL'], ['under_pct', 'REAL'], ['accuracy_rate', 'REAL'], ['accuracy_n', 'INTEGER'], ['tolerance_pct', 'REAL'], ['nc_incidents', 'INTEGER'], ['nc_rate', 'REAL'], ['nc_units_per_1000', 'REAL'], ['nc_cartons_per_100', 'REAL'], ['nc_top_category', 'TEXT'], ['nc_top_count', 'INTEGER'], ['nc_chargeable_rate', 'REAL'], ['nc_open', 'INTEGER']]);
  db.exec(`CREATE TABLE IF NOT EXISTS signal_setting (client_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT, updated_at TEXT, updated_by TEXT, PRIMARY KEY (client_id, key))`);
  const setting = (client, key, dflt) => { const r = db.prepare('SELECT value FROM signal_setting WHERE client_id = ? AND key = ?').get(client, key); return r && r.value != null ? r.value : dflt; };
  const toleranceFor = (client) => { const v = Number(setting(client, 'accuracy_tolerance_pct', 2)); return isFinite(v) && v >= 0 ? v : 2; };
  const modeOf = (v) => /air/i.test(String(v || '')) ? 'Air' : 'Sea';
  const supplierOf = (v) => String(v || '').trim().replace(/\s+/g, ' ');
  const todayYmd = () => ymd(new Date());

  function windowWeeks(weeks) {
    const out = []; let m = mondayOf(todayYmd());
    for (let i = 0; i < weeks; i++) { out.push(m); m = addDays(m, -7); }
    return out.reverse();
  }

  // ── the compute ──
  function compute(client, weeks = WEEKS) {
    const runId = db.prepare('INSERT INTO signal_run (client_id, started_at, weeks) VALUES (?,?,?)').run(client, iso(), weeks).lastInsertRowid;
    const wks = windowWeeks(weeks), first = wks[0], last = wks[wks.length - 1], today = todayYmd();
    const notes = [];
    const recvHasClient = hasCol('receiving', 'client_id'), recHasClient = hasCol('records', 'client_id'), ladHasClient = hasCol('lane_actual_dates', 'client_id');

    // Plans → POs per week
    const plans = db.prepare('SELECT week_start, data FROM plans WHERE client_id = ? AND week_start >= ? AND week_start <= ?').all(client, first, last);
    const posByWeek = new Map();          // ws → Map(po → {supplier, mode, zendesk, due, units})
    for (const p of plans) {
      let rows; try { rows = JSON.parse(p.data); } catch (_) { continue; }
      if (!Array.isArray(rows)) continue;
      const m = new Map();
      for (const r of rows) {
        const po = String(r.po_number || '').trim(); if (!po) continue;
        const e = m.get(po) || { po, supplier: supplierOf(r.supplier_name), mode: modeOf(r.freight_type || r.freight || r.mode), freight_raw: String(r.freight_type || r.freight || r.mode || '').trim(), zendesk: String(r.zendesk_ticket || '').trim(), due: r.due_date ? String(r.due_date).slice(0, 10) : null, units: 0, facility: r.facility_name || null };
        e.units += Number(r.target_qty || 0) || 0; m.set(po, e);
      }
      posByWeek.set(p.week_start, m);
    }

    // Receiving and applied units
    const recv = db.prepare(`SELECT week_start, po_number, received_at_utc, received_at_local, cartons_received FROM receiving WHERE ${recvHasClient ? 'client_id = ? AND ' : ''}week_start >= ? AND week_start <= ?`).all(...(recvHasClient ? [client] : []), first, last);
    const recvByWeekPo = new Map(); for (const r of recv) recvByWeekPo.set(r.week_start + '|' + String(r.po_number).trim(), r);
    const applied = db.prepare(`SELECT po_number, COUNT(*) n FROM records WHERE ${recHasClient ? 'client_id = ? AND ' : ''}status = 'complete' AND date_local >= ? GROUP BY po_number`).all(...(recHasClient ? [client] : []), first);
    const appliedByPo = new Map(); for (const a of applied) appliedByPo.set(String(a.po_number || '').trim(), a.n);
    const tolerance = toleranceFor(client);

    // Exception log: incidents are keyed on week + PO (no client column), so they are joined
    // through this client's plan — a PO in the plan is this client's PO.
    let ncRows = []; try { ncRows = db.prepare(`SELECT i.week_start, i.po_number, i.grain, i.qty, i.status, i.chargeable, i.category_id, COALESCE(c.label, i.category_id) label FROM nc_incident i LEFT JOIN nc_category c ON c.id = i.category_id WHERE i.week_start >= ? AND i.week_start <= ?`).all(first, last); } catch (_) {}
    const ncByWeekPo = new Map(); for (const n of ncRows) { const k = n.week_start + '|' + String(n.po_number || '').trim(); (ncByWeekPo.get(k) || ncByWeekPo.set(k, []).get(k)).push(n); }

    // Readiness: consignment milestones via lane keys, and the legacy lane dates
    const laneLinks = db.prepare('SELECT cl.lane_key, cl.week_start, cl.consignment_uid FROM consignment_lane cl WHERE cl.client_id = ? AND cl.week_start >= ? AND cl.week_start <= ?').all(client, first, last);
    const laneToCg = new Map(); for (const l of laneLinks) laneToCg.set(l.week_start + '|' + l.lane_key, l.consignment_uid);
    const msRows = db.prepare(`SELECT m.consignment_uid, m.stage, m.planned_at, m.actual_at, m.state FROM consignment_milestone m JOIN consignment c ON c.consignment_uid = m.consignment_uid WHERE c.client_id = ? AND c.week_start >= ? AND c.week_start <= ?`).all(client, first, last);
    const msByCg = new Map(); for (const m of msRows) { const o = msByCg.get(m.consignment_uid) || {}; o[m.stage] = m; msByCg.set(m.consignment_uid, o); }
    let legacy = []; try { legacy = db.prepare(`SELECT lane_key, week_start, stage, actual_at FROM lane_actual_dates WHERE ${ladHasClient ? '(client_id = ? OR client_id IS NULL) AND ' : ''}week_start >= ? AND week_start <= ?`).all(...(ladHasClient ? [client] : []), first, last); } catch (_) {}
    const legacyByLane = new Map(); for (const l of legacy) { const k = l.week_start + '|' + l.lane_key; const o = legacyByLane.get(k) || {}; o[l.stage] = String(l.actual_at).slice(0, 10); legacyByLane.set(k, o); }
    const rules = {}; try { for (const r of db.prepare('SELECT * FROM consignment_rules WHERE client_id IN (?, ?) AND facility <> ?').all(client, '__default__', '__migration__')) rules[r.mode] = r; } catch (_) {}
    const packingOffset = (mode) => (rules[mode] && rules[mode].packing_list_offset_days != null) ? Number(rules[mode].packing_list_offset_days) : (mode === 'Air' ? 2 : 4);
    const readinessFor = (ws, po) => {
      // Lane keys are built from the plan's own strings (supplier||zendesk||freight as typed, e.g. SEA).
      const key = ws + '|' + [po.supplier, po.zendesk, po.freight_raw || po.mode].join('||');
      const uid = laneToCg.get(key);
      if (uid) { const m = (msByCg.get(uid) || {}).packing_list_ready; if (m && m.actual_at && m.state !== 'assumed') return { planned: (m.planned_at || addDays(ws, packingOffset(po.mode))).slice(0, 10), actual: String(m.actual_at).slice(0, 10), source: 'movement' }; return null; }
      const lg = legacyByLane.get(key); if (lg && lg.packing_list_ready) return { planned: addDays(ws, packingOffset(po.mode)), actual: lg.packing_list_ready, source: 'legacy' };
      return null;
    };

    // Supplier × week
    db.prepare('DELETE FROM signal_supplier_week WHERE client_id = ?').run(client);
    db.prepare('DELETE FROM signal_volume_week WHERE client_id = ?').run(client);
    const insSW = db.prepare(`INSERT INTO signal_supplier_week (client_id, supplier, week_start, planned_units, applied_units, fill_rate, mature, pos, received_pos, on_time_pos, late_pos, short_pos, pending_pos, ready_n, ready_on_time, ready_late_days_p50, computed_at, over_units, under_units, accuracy_n, within_pos, nc_incidents, nc_pos, nc_units, nc_cartons, cartons_received) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const insVW = db.prepare(`INSERT INTO signal_volume_week (client_id, week_start, supplier, mode, units, pos, cartons) VALUES (?,?,?,?,?,?,?)`);
    const perSupplier = new Map();        // supplier → aggregates
    for (const ws of wks) {
      const pos = posByWeek.get(ws); if (!pos) continue;
      const mature = daysBetween(ws, today) >= MATURE_DAYS;
      const fcDate = addDays(ws, 7);
      const bySup = new Map();
      const byVol = new Map();
      for (const po of pos.values()) {
        const s = bySup.get(po.supplier) || { planned: 0, applied: 0, pos: 0, received: 0, onTime: 0, late: 0, short: 0, pending: 0, readyLate: [], readyN: 0, readyOnTime: 0, recvLate: [], over: 0, under: 0, accN: 0, within: 0, nc: 0, ncPos: 0, ncUnits: 0, ncCartons: 0, ncOpen: 0, ncChargeable: 0, ncCats: new Map(), cartons: 0 };
        s.planned += po.units; s.pos++;
        const rawAp = appliedByPo.get(po.po) || 0, ap = Math.min(rawAp, po.units || Infinity); s.applied += ap;
        const r = recvByWeekPo.get(ws + '|' + po.po); const rAt = r && (r.received_at_local || r.received_at_utc) ? String(r.received_at_local || r.received_at_utc).slice(0, 10) : null;
        if (rAt) { s.received++; if (!po.due || rAt <= po.due) s.onTime++; else { s.late++; s.recvLate.push(daysBetween(po.due, rAt)); } }
        else if (daysBetween(fcDate, today) > LATE_GRACE_DAYS) s.short++; else s.pending++;
        s.cartons += r ? (Number(r.cartons_received) || 0) : 0;
        // Accuracy: a received PO in a mature week, applied against planned, both directions.
        if (rAt && mature && po.units > 0) { s.accN++; const diff = rawAp - po.units; if (diff > 0) s.over += diff; else s.under += -diff; if (Math.abs(diff) / po.units * 100 <= tolerance) s.within++; }
        for (const n of (ncByWeekPo.get(ws + '|' + po.po) || [])) {
          if (!s._ncPo) s._ncPo = new Set(); if (!s._ncPo.has(po.po)) { s._ncPo.add(po.po); s.ncPos++; }
          s.nc++; if (n.grain === 'carton') s.ncCartons += Number(n.qty) || 0; else s.ncUnits += Number(n.qty) || 0;
          if (n.status !== 'resolved') s.ncOpen++; if (n.chargeable) s.ncChargeable++;
          s.ncCats.set(n.label, (s.ncCats.get(n.label) || 0) + 1);
        }
        const rd = readinessFor(ws, po);
        if (rd) { s.readyN++; const d = daysBetween(rd.planned, rd.actual); if (d != null) { s.readyLate.push(Math.max(0, d)); if (d <= 0) s.readyOnTime++; } }
        bySup.set(po.supplier, s);
        const vk = po.supplier + '|' + po.mode; const v = byVol.get(vk) || { supplier: po.supplier, mode: po.mode, units: 0, pos: 0, cartons: 0 };
        v.units += po.units; v.pos++; v.cartons += r ? (Number(r.cartons_received) || 0) : 0; byVol.set(vk, v);
      }
      for (const [sup, s] of bySup) {
        insSW.run(client, sup, ws, s.planned, s.applied, mature && s.planned > 0 ? r1(Math.min(100, s.applied / s.planned * 100)) : null, mature ? 1 : 0, s.pos, s.received, s.onTime, s.late, s.short, s.pending, s.readyN, s.readyOnTime, s.readyLate.length ? r1(median(s.readyLate)) : null, iso(),
          s.over, s.under, s.accN, s.within, s.nc, s.ncPos, s.ncUnits, s.ncCartons, s.cartons);
        const a = perSupplier.get(sup) || { weeks: 0, pos: 0, planned: 0, applied: 0, planned_m: 0, applied_m: 0, received: 0, onTime: 0, late: 0, short: 0, decided: 0, readyN: 0, readyOnTime: 0, readyLate: [], recvLate: [], recent: [], fills: [], over: 0, under: 0, accN: 0, within: 0, accPlanned: 0, nc: 0, ncPos: 0, ncUnits: 0, ncCartons: 0, ncOpen: 0, ncChargeable: 0, ncCats: new Map(), cartons: 0 };
        a.weeks++; a.pos += s.pos; a.planned += s.planned; a.applied += s.applied; a.received += s.received; a.onTime += s.onTime; a.late += s.late; a.short += s.short; a.decided += s.received + s.short;
        a.over += s.over; a.under += s.under; a.accN += s.accN; a.within += s.within; if (mature) a.accPlanned += s.planned;
        a.nc += s.nc; a.ncPos += s.ncPos; a.ncUnits += s.ncUnits; a.ncCartons += s.ncCartons; a.ncOpen += s.ncOpen; a.ncChargeable += s.ncChargeable; a.cartons += s.cartons; for (const [k, v] of s.ncCats) a.ncCats.set(k, (a.ncCats.get(k) || 0) + v);
        if (mature && s.planned > 0) { a.planned_m += s.planned; a.applied_m += s.applied; a.fills.push({ ws, f: Math.min(1, s.applied / s.planned) }); }
        a.readyN += s.readyN; a.readyOnTime += s.readyOnTime; a.readyLate.push(...s.readyLate); a.recvLate.push(...s.recvLate);
        perSupplier.set(sup, a);
      }
      for (const v of byVol.values()) insVW.run(client, ws, v.supplier, v.mode, v.units, v.pos, v.cartons);
    }
    db.prepare('DELETE FROM signal_supplier WHERE client_id = ?').run(client);
    const insS = db.prepare(`INSERT INTO signal_supplier (client_id, supplier, weeks, pos, planned_units, applied_units, fill_rate, fill_rate_recent, received_rate, on_time_rate, late_days_p50, late_days_p90, short_rate, ready_n, ready_on_time_rate, ready_late_p50, ready_late_p90, trend, confidence, computed_at,
      over_pct, under_pct, accuracy_rate, accuracy_n, tolerance_pct, nc_incidents, nc_rate, nc_units_per_1000, nc_cartons_per_100, nc_top_category, nc_top_count, nc_chargeable_rate, nc_open) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const [sup, a] of perSupplier) {
      const fill = a.planned_m > 0 ? r1(Math.min(100, a.applied_m / a.planned_m * 100)) : null;
      // Trend: the last 4 mature weeks with data against the earlier ones.
      const sorted = a.fills.slice().sort((x, y) => x.ws < y.ws ? -1 : 1); const rec = sorted.slice(-4), prior = sorted.slice(0, -4);
      const fillRecent = rec.length ? r1(rec.reduce((s, f) => s + f.f, 0) / rec.length * 100) : null;
      const fillPrior = prior.length ? prior.reduce((s, f) => s + f.f, 0) / prior.length * 100 : null;
      let trend = 'stable'; if (fillRecent != null && fillPrior != null) { if (fillRecent - fillPrior >= 5) trend = 'improving'; else if (fillPrior - fillRecent >= 5) trend = 'worsening'; } else if (!rec.length || !prior.length) trend = null;
      const confidence = a.weeks >= MIN_SUPPLIER_WEEKS ? (a.weeks >= 8 ? 'ok' : 'thin') : 'none';
      insS.run(client, sup, a.weeks, a.pos, a.planned, a.applied, fill, fillRecent, pct(a.received, a.pos), a.received ? pct(a.onTime, a.received) : null, a.recvLate.length ? r1(median(a.recvLate)) : null, a.recvLate.length ? r1(quantile(a.recvLate, 0.9)) : null, a.decided ? pct(a.short, a.decided) : null,
        a.readyN, a.readyLate.length ? pct(a.readyOnTime, a.readyLate.length) : null, a.readyLate.length ? r1(median(a.readyLate)) : null, a.readyLate.length ? r1(quantile(a.readyLate, 0.9)) : null, trend, confidence, iso(),
        // Over / under: units above or below plan across received POs in mature weeks, as a share of those POs' plan.
        a.accPlanned > 0 ? pct(a.over, a.accPlanned) : null, a.accPlanned > 0 ? pct(a.under, a.accPlanned) : null, a.accN ? pct(a.within, a.accN) : null, a.accN, tolerance,
        // Compliance: incidents per received PO; units reworked per 1,000 applied; carton failures per 100 cartons received.
        a.nc, a.received ? pct(a.ncPos, a.received) : null, a.applied > 0 ? r1(a.ncUnits / a.applied * 1000) : null, a.cartons > 0 ? r1(a.ncCartons / a.cartons * 100) : null,
        ...(a.ncCats.size ? [...a.ncCats.entries()].sort((x, y) => y[1] - x[1])[0] : [null, null]), a.nc ? pct(a.ncChargeable, a.nc) : null, a.ncOpen);
    }

    // Lanes: consignments with actuals (+ legacy lane dates)
    db.prepare('DELETE FROM signal_lane WHERE client_id = ?').run(client);
    const cgs = db.prepare('SELECT * FROM consignment WHERE client_id = ? AND week_start >= ? AND week_start <= ?').all(client, first, last);
    const moves = [];
    const act = (ms, st) => { const m = ms && ms[st]; return m && m.actual_at && m.state !== 'assumed' ? String(m.actual_at).slice(0, 10) : null; };
    const plan = (ms, st) => { const m = ms && ms[st]; return m && m.planned_at ? String(m.planned_at).slice(0, 10) : null; };
    const cgSuppliers = new Map(); for (const l of laneLinks) { const sup = supplierOf(String(l.lane_key).split('||')[0]); const s = cgSuppliers.get(l.consignment_uid) || new Set(); s.add(sup); cgSuppliers.set(l.consignment_uid, s); }
    // Carrier as a name: some consignments hold an object ({name, code…}) or a vessel/voyage string
    // ("BRIGHTON 0028S") in the carrier field — neither is a carrier. Couriers are air whatever the lane says.
    const COURIER = /^(dhl|fedex|ups|tnt|aramex|startrack|toll)\b/i;
    const carrierName = (v) => { if (v == null) return null; if (typeof v === 'object') v = v.name || v.carrier || v.code || v.label || ''; let t = String(v).trim(); if (!t || t === '[object Object]') return null; try { if (/^\{/.test(t)) { const o = JSON.parse(t); t = String(o.name || o.carrier || o.code || '').trim(); } } catch (_) {} if (/\b\d{3,4}[A-Z]?$/.test(t) && /\s/.test(t)) return null; return t || null; };
    for (const c of cgs) {
      const ms = msByCg.get(c.consignment_uid) || {};
      const carrier = carrierName(c.carrier); const mode = COURIER.test(carrier || '') ? 'Air' : c.mode;
      moves.push({ mode, carrier, suppliers: [...(cgSuppliers.get(c.consignment_uid) || [])], week: c.week_start, quoted: c.transit_confirmed ? Number(c.transit_days) || null : null, legacy: false,
        a: { packing: act(ms, 'packing_list_ready'), cleared: act(ms, 'origin_cleared'), departed: act(ms, 'departed'), arrived: act(ms, 'arrived'), dcleared: act(ms, 'dest_cleared'), fc: act(ms, 'fc_receipt') },
        p: { packing: plan(ms, 'packing_list_ready'), cleared: plan(ms, 'origin_cleared'), departed: plan(ms, 'departed'), arrived: plan(ms, 'arrived'), dcleared: plan(ms, 'dest_cleared'), fc: plan(ms, 'fc_receipt') } });
    }
    for (const [k, lg] of legacyByLane) {
      const [ws, laneKey] = k.split('|'); if (laneToCg.has(k)) continue;           // already a movement
      const parts = laneKey.split('||');
      // Mode from the lane key's freight type; when that is blank, from the plan's POs on the same supplier + ticket. Still unknown → not a lane.
      let mode = String(parts[2] || '').trim() ? modeOf(parts[2]) : null;
      if (!mode) { const wk = posByWeek.get(ws); if (wk) for (const po of wk.values()) if (po.supplier === supplierOf(parts[0]) && (!parts[1] || po.zendesk === String(parts[1] || '').trim())) { mode = po.mode; break; } }
      if (!mode) continue;
      moves.push({ mode, carrier: null, suppliers: [supplierOf(parts[0])], week: ws, quoted: null, legacy: true,
        a: { packing: lg.packing_list_ready || null, cleared: lg.origin_cleared || null, departed: lg.departed || null, arrived: lg.arrived || null, dcleared: lg.dest_cleared || null, fc: lg.fc_receipt || null },
        p: { packing: addDays(ws, packingOffset(mode)), cleared: null, departed: null, arrived: null, dcleared: null, fc: null } });
    }
    const STAGES = [['origin_rhythm', 'packing'], ['origin_cleared', 'cleared'], ['departed', 'departed'], ['arrived', 'arrived'], ['dest_cleared', 'dcleared'], ['fc_receipt', 'fc']];
    const insL = db.prepare(`INSERT INTO signal_lane (client_id, grain, key, mode, carrier, supplier, n, transit_n, transit_p50, transit_p90, quoted_p50, slip_p50, outliers, d2d_n, d2d_p50, d2d_p90, stage_json, slowest_stage, legacy_n, confidence, computed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const laneStats = (group) => {
      const n = group.length; if (!n) return null;
      let transit = group.map(m => daysBetween(m.a.departed, m.a.arrived)).filter(v => v != null && v >= 0);
      const med = median(transit); let outliers = 0; if (med != null) { const keep = transit.filter(v => v <= med * 2.5 + 2); outliers = transit.length - keep.length; transit = keep; }
      const paired = group.filter(m => m.quoted && m.a.departed && m.a.arrived).map(m => daysBetween(m.a.departed, m.a.arrived) - m.quoted);
      const d2d = group.map(m => daysBetween(m.a.packing, m.a.fc)).filter(v => v != null && v >= 0);
      const stage = {};
      let slowest = null, slowestSlip = -Infinity;
      for (const [name, k] of STAGES) {
        const slips = group.map(m => (m.a[k] && m.p[k]) ? daysBetween(m.p[k], m.a[k]) : null).filter(v => v != null);
        stage[name] = { n: slips.length, slip_p50: slips.length ? r1(median(slips)) : null, slip_p90: slips.length ? r1(quantile(slips, 0.9)) : null };
        if (slips.length >= 3 && stage[name].slip_p50 > slowestSlip) { slowestSlip = stage[name].slip_p50; slowest = name; }
      }
      return { n, transit_n: transit.length, transit_p50: r1(median(transit)), transit_p90: r1(quantile(transit, 0.9)), quoted_p50: paired.length ? r1(median(group.filter(m => m.quoted).map(m => m.quoted))) : null, slip_p50: paired.length ? r1(median(paired)) : null, outliers,
        d2d_n: d2d.length, d2d_p50: r1(median(d2d)), d2d_p90: r1(quantile(d2d, 0.9)), stage, slowest: slowestSlip > 0 ? slowest : null, legacy_n: group.filter(m => m.legacy).length,
        confidence: transit.length >= MIN_LANE_N || d2d.length >= MIN_LANE_N ? ((transit.length >= 12 || d2d.length >= 12) ? 'ok' : 'thin') : 'none' };
    };
    const groups = new Map();
    const add = (grain, key, mode, carrier, supplier, m) => { const k = grain + '|' + key; const g = groups.get(k) || { grain, key, mode, carrier, supplier, items: [] }; g.items.push(m); groups.set(k, g); };
    for (const m of moves) {
      add('mode', m.mode, m.mode, null, null, m);
      if (m.carrier) add('carrier', m.mode + '|' + m.carrier, m.mode, m.carrier, null, m);   // thin carrier groups are dropped below
      for (const s of m.suppliers) if (s) add('supplier', m.mode + '|' + s, m.mode, null, s, m);
    }
    for (const g of groups.values()) {
      if (g.grain === 'carrier' && g.items.length < MIN_LANE_N) continue;          // one sailing is not a lane
      const st = laneStats(g.items); if (!st) continue;
      insL.run(client, g.grain, g.key, g.mode, g.carrier, g.supplier, st.n, st.transit_n, st.transit_p50, st.transit_p90, st.quoted_p50, st.slip_p50, st.outliers, st.d2d_n, st.d2d_p50, st.d2d_p90, JSON.stringify(st.stage), st.slowest, st.legacy_n, st.confidence, iso());
    }

    // Price per week per mode
    db.prepare('DELETE FROM signal_price_week WHERE client_id = ?').run(client);
    const insP = db.prepare(`INSERT INTO signal_price_week (client_id, week_start, mode, units, price, customs, containers, price_per_unit, price_per_container, kg, price_per_kg, cost, margin_pct, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const invHasClient = hasCol('fin_invoices', 'client_id');
    for (const ws of wks) {
      const pos = posByWeek.get(ws); const seaUnits = pos ? [...pos.values()].filter(p => p.mode === 'Sea').reduce((s, p) => s + p.units, 0) : 0;
      const inv = db.prepare(`SELECT * FROM fin_invoices WHERE ${invHasClient ? 'client_id = ? AND ' : ''}week_start = ? AND UPPER(type) = 'SEA' ORDER BY created_at DESC LIMIT 1`).get(...(invHasClient ? [client] : []), ws);
      if (inv) {
        const lines = db.prepare('SELECT description, quantity, unit_label FROM fin_invoice_lines WHERE invoice_id = ?').all(inv.id);
        let containers = 0; for (const l of lines) if (/^container\b/i.test(String(l.description || ''))) containers += Number(l.quantity) || 1;
        const price = (Number(inv.subtotal) || 0) + (Number(inv.customs) || 0);
        insP.run(client, ws, 'Sea', seaUnits, r2(price), r2(Number(inv.customs) || 0), containers || null, seaUnits ? r2(price / seaUnits) : null, containers ? r2(price / containers) : null, null, null, null, null, 'invoice');
      }
      // Air: approved quotes for that week
      let aq = []; try { aq = db.prepare(`SELECT q.sell_amount, q.chargeable_kg, q.units, k.cost_amount FROM air_quote q LEFT JOIN air_quote_cost k ON k.quote_id = q.id WHERE q.client_id = ? AND q.week_start = ? AND q.state = 'approved' AND q.sell_amount > 0`).all(client, ws); } catch (_) {}
      if (aq.length) {
        const sell = aq.reduce((s, q) => s + Number(q.sell_amount || 0), 0), kg = aq.reduce((s, q) => s + Number(q.chargeable_kg || 0), 0), units = aq.reduce((s, q) => s + Number(q.units || 0), 0), cost = aq.reduce((s, q) => s + Number(q.cost_amount || 0), 0);
        insP.run(client, ws, 'Air', units || null, r2(sell), null, null, units ? r2(sell / units) : null, null, r2(kg), kg ? r2(sell / kg) : null, cost ? r2(cost) : null, cost && sell ? r1((sell - cost) / sell * 100) : null, 'quotes');
      }
    }
    db.prepare('UPDATE signal_run SET finished_at = ?, notes = ? WHERE id = ?').run(iso(), JSON.stringify({ defs: DEFS_VERSION, suppliers: perSupplier.size, lanes: groups.size, moves: moves.length, plans: plans.length, notes }), runId);
    log.log(`[signals] ${client}: ${perSupplier.size} suppliers, ${groups.size} lane groups, ${moves.length} movements over ${weeks} weeks`);
    return { client, suppliers: perSupplier.size, lanes: groups.size, moves: moves.length };
  }
  function clients() { try { return db.prepare('SELECT DISTINCT client_id FROM plans').all().map(r => r.client_id).filter(Boolean); } catch (_) { return []; } }
  function runAll(weeks) { const out = []; for (const c of clients()) { try { out.push(compute(c, weeks)); } catch (e) { log.warn('[signals] ' + c + ' failed: ' + e.message); out.push({ client: c, error: e.message }); } } return out; }

  // ── read model ──
  const isInternal = (req) => { try { const t = tenancyResolve(req.auth && req.auth.orgId, req.auth && req.auth.orgRole); return !t || !t.org_type || t.org_type === 'internal'; } catch (_) { return true; } };
  function overview(client, internal, weeks = WEEKS) {
    const run = db.prepare('SELECT * FROM signal_run WHERE client_id = ? AND finished_at IS NOT NULL ORDER BY id DESC LIMIT 1').get(client);
    const suppliers = db.prepare('SELECT * FROM signal_supplier WHERE client_id = ? ORDER BY planned_units DESC').all(client).map(s => ({ ...s, client_id: undefined }));
    const weeksRows = db.prepare('SELECT supplier, week_start, planned_units, applied_units, fill_rate, mature, pos, received_pos, on_time_pos, late_pos, short_pos, pending_pos, ready_n, ready_on_time, ready_late_days_p50, over_units, under_units, accuracy_n, within_pos, nc_incidents, nc_pos, nc_units, nc_cartons FROM signal_supplier_week WHERE client_id = ? ORDER BY week_start').all(client);
    if (!internal) for (const s of suppliers) delete s.nc_chargeable_rate;
    const history = {}; for (const w of weeksRows) (history[w.supplier] = history[w.supplier] || []).push(w);
    for (const s of suppliers) s.history = history[s.supplier] || [];
    const lanes = db.prepare('SELECT * FROM signal_lane WHERE client_id = ? ORDER BY grain, n DESC').all(client).map(l => ({ ...l, client_id: undefined, stage: JSON.parse(l.stage_json || '{}'), stage_json: undefined }));
    const volume = db.prepare('SELECT week_start, supplier, mode, units, pos, cartons FROM signal_volume_week WHERE client_id = ? ORDER BY week_start').all(client);
    const wks = windowWeeks(weeks); const recent = wks.slice(-8);
    const byMode = {}; for (const v of volume) { const o = byMode[v.mode] = byMode[v.mode] || { weeks: new Set(), units: 0, units8: 0, weeks8: new Set() }; o.weeks.add(v.week_start); o.units += v.units; if (recent.includes(v.week_start)) { o.units8 += v.units; o.weeks8.add(v.week_start); } }
    const volumeBaseline = Object.entries(byMode).map(([mode, o]) => ({ mode, weeks: o.weeks.size, avg_units_26w: o.weeks.size ? Math.round(o.units / o.weeks.size) : null, avg_units_8w: o.weeks8.size ? Math.round(o.units8 / o.weeks8.size) : null, blended: o.weeks.size ? Math.round((o.units + 2 * o.units8) / (o.weeks.size + 2 * o.weeks8.size)) : null }));
    let price = db.prepare('SELECT * FROM signal_price_week WHERE client_id = ? ORDER BY week_start').all(client).map(p => ({ ...p, client_id: undefined }));
    // Cost and margin stay in the table for internal analysis but never leave through this route —
    // the Performance page is a client page and Pulse replays what it reads.
    price = price.map(p => ({ ...p, cost: undefined, margin_pct: undefined }));
    return { ok: true, window_weeks: weeks, computed_at: run ? run.finished_at : null, definitions_version: DEFS_VERSION, minimums: { supplier_weeks: MIN_SUPPLIER_WEEKS, lane_movements: MIN_LANE_N }, settings: { accuracy_tolerance_pct: toleranceFor(client) }, suppliers, lanes, volume, volume_baseline: volumeBaseline, price };
  }

  // ── routes ──
  app.get('/signals/overview', authenticateRequest, (req, res) => {
    try { const c = curClient(); const o = overview(c, isInternal(req), Math.min(52, Math.max(4, Number(req.query.weeks) || WEEKS))); res.json(o); }
    catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  app.get('/signals/supplier/:name', authenticateRequest, (req, res) => {
    const c = curClient(); const name = String(req.params.name || '');
    const s = db.prepare('SELECT * FROM signal_supplier WHERE client_id = ? AND supplier = ?').get(c, name);
    if (!s) return res.status(404).json({ ok: false, error: 'no history for that supplier' });
    const history = db.prepare('SELECT * FROM signal_supplier_week WHERE client_id = ? AND supplier = ? ORDER BY week_start').all(c, name);
    res.json({ ok: true, supplier: { ...s, client_id: undefined }, history });
  });
  // Settings (admin): the accuracy tolerance, per client. Recomputes straight away so the page agrees.
  app.put('/signals/settings', authenticateRequest, requireRole(['admin']), requireInternalOrg, (req, res) => {
    try {
      const c = curClient(); const b = req.body || {};
      if (b.accuracy_tolerance_pct != null) { const v = Number(b.accuracy_tolerance_pct); if (!isFinite(v) || v < 0 || v > 50) return res.status(400).json({ ok: false, error: 'tolerance must be 0–50 %' });
        db.prepare('INSERT INTO signal_setting (client_id, key, value, updated_at, updated_by) VALUES (?,?,?,?,?) ON CONFLICT(client_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at, updated_by = excluded.updated_by').run(c, 'accuracy_tolerance_pct', String(v), iso(), String((req.auth && req.auth.userId) || 'admin')); }
      compute(c, WEEKS);
      res.json({ ok: true, settings: { accuracy_tolerance_pct: toleranceFor(c) } });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  app.post('/signals/run', authenticateRequest, requireRole(['admin']), requireInternalOrg, (req, res) => {
    try { const c = (req.body && req.body.all) ? null : curClient(); const out = c ? [compute(c, Number(req.body && req.body.weeks) || WEEKS)] : runAll(); res.json({ ok: true, runs: out }); }
    catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // ── nightly ──
  const RUN_HOUR = Number(process.env.SIGNALS_RUN_HOUR ?? 2);
  function sydneyHour() { return Number(new Intl.DateTimeFormat('en-AU', { timeZone: 'Australia/Sydney', hour: 'numeric', hour12: false }).format(new Date())); }
  let lastRunDay = null;
  function tick() {
    try {
      const day = new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
      if (sydneyHour() === RUN_HOUR && lastRunDay !== day) { lastRunDay = day; runAll(); }
    } catch (e) { log.warn('[signals] tick: ' + e.message); }
  }
  if (process.env.SIGNALS_SCHEDULE !== 'off') {
    setInterval(tick, 10 * 60 * 1000).unref();
    // A stale or empty store at boot gets one run straight away (deploys happen at all hours).
    setTimeout(() => { try { const r = db.prepare('SELECT finished_at, notes FROM signal_run WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1').get(); if (!r || Date.now() - new Date(r.finished_at).getTime() > 26 * 3600 * 1000 || (() => { try { return JSON.parse(r.notes || '{}').defs !== DEFS_VERSION; } catch (_) { return true; } })()) runAll(); } catch (e) { log.warn('[signals] boot run: ' + e.message); } }, 15000).unref();
  }

  return { compute, runAll, overview, windowWeeks };
};
