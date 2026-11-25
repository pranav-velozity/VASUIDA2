/* ── VelOzity Pinpoint — Door-to-door requests (Phase 5b) ──
   Tables carry the rq_ prefix and routes live under /rq/: the Shipments hub already owns d2d_request, d2d_event and /d2d/requests.
   A client asks for options for a period (week or month): where from, where to, when it must
   be in the Sydney FC, how much. VelOzity's carrier partners answer through a magic link with
   one row per option (sea or air), VelOzity prices each option from the rate card and releases
   them, the client simulates (5c/5d) and picks, the partner confirms cost, VelOzity releases the
   firm price, the client approves. Approved air legs become approved air quotes; approved sea
   legs become the week's sea rate. Both feed the signals price line and the simulator.

   Request states (seven, plus two terminal):
     requested → options_offered → released → picked → cost_confirmed → firm_released → approved
     declined · expired
   Option states: approximate → picked → confirmed → firm → approved · expired · withdrawn

   CONFIDENTIALITY — same hard rule as air quotes. Cost, margin and the partner's identity live
   in rq_option_cost / quote_partner and never reach a client: every client-facing response is
   built by rqPublic() / optPublic() with an explicit field list. The partner page shows shipment
   facts only — never the client's name, never a price.

   Wording on client surfaces: "Price" (never sell), "Ask VelOzity" (never partner).

   Env: D2D_PARTNER_LINK_DAYS (7), D2D_CLIENT_EMAIL_TO (fallback AIR_QUOTE_CLIENT_EMAIL_TO),
        PUBLIC_API_URL / PUBLIC_APP_URL as for air quotes, D2D_EXPIRY=off to stop the ticker. */
const crypto = require('crypto');

module.exports = function ({ app, db, authenticateRequest, requireRole, requireInternalOrg, requireClientOrInternal, curClient, tenancyResolve, writeOpLimiter, auditLog, sendViaResend, parseEmailList, escHtml, rateFor, aqNewId, aqChargeableKg, log }) {
  const L = log || console;
  const noop = (req, res, next) => next();
  writeOpLimiter = writeOpLimiter || noop; auditLog = auditLog || (() => noop);
  const esc = escHtml || (s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])));
  const iso = () => new Date().toISOString();
  const today = () => new Date().toISOString().slice(0, 10);
  const newId = (p) => (aqNewId ? aqNewId(p) : p + '_' + crypto.randomBytes(10).toString('hex'));
  const r2 = (n) => Math.round(Number(n) * 100) / 100;
  const modeOf = (v) => /air/i.test(String(v || '')) ? 'Air' : 'Sea';
  const supplierKey = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const mondayOf = (ymd) => { const d = new Date(ymd + 'T00:00:00Z'); if (isNaN(d)) return null; d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7)); return d.toISOString().slice(0, 10); };
  const addDays = (ymd, n) => { const d = new Date(ymd + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const isoWeek = (ws) => { const d = new Date(ws + 'T00:00:00Z'); if (isNaN(d)) return ws; const x = new Date(d); x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7) + 3); const f = new Date(Date.UTC(x.getUTCFullYear(), 0, 4)); return 'W' + (1 + Math.round(((x - f) / 86400000 - 3 + ((f.getUTCDay() + 6) % 7)) / 7)); };
  const monthLabel = (ym) => { const d = new Date(ym + '-01T00:00:00Z'); return isNaN(d) ? ym : d.toLocaleDateString('en-AU', { month: 'short', year: 'numeric', timeZone: 'UTC' }); };
  const periodLabel = (type, start) => type === 'month' ? monthLabel(start.slice(0, 7)) : isoWeek(start);
  const fmtMoney = (n, cur) => (cur || 'USD') + ' ' + Number(n || 0).toLocaleString('en-AU', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const dmy = (s) => { if (!s) return '—'; const d = new Date(String(s).slice(0, 10) + 'T00:00:00Z'); return isNaN(d) ? String(s) : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }); };
  const actorOf = (req) => (req.auth && (req.auth.email || req.auth.userId)) || null;
  const MARKUP_DEFAULT = { Air: 40, Sea: 20 };
  const markupFor = (client, mode) => rateFor ? rateFor(client, mode === 'Air' ? 'air_quote_markup' : 'sea_quote_markup', MARKUP_DEFAULT[mode]) : MARKUP_DEFAULT[mode];
  const LINK_DAYS = Math.max(1, parseInt(process.env.D2D_PARTNER_LINK_DAYS, 10) || 7);
  const CONTAINER = ['20', '40', '40HC'];

  // ── Schema ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS quote_partner (
      id TEXT PRIMARY KEY, client_id TEXT NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL,
      air INTEGER NOT NULL DEFAULT 1, sea INTEGER NOT NULL DEFAULT 1, active INTEGER NOT NULL DEFAULT 1, note TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')), updated_at TEXT NOT NULL DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_quote_partner_client ON quote_partner(client_id, active);
    CREATE TABLE IF NOT EXISTS rq_setting (client_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT, updated_at TEXT, updated_by TEXT, PRIMARY KEY (client_id, key));
    CREATE TABLE IF NOT EXISTS rq_origin (id TEXT PRIMARY KEY, client_id TEXT NOT NULL, name TEXT NOT NULL, country TEXT, active INTEGER NOT NULL DEFAULT 1, sort INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS rq_supplier_origin (client_id TEXT NOT NULL, supplier_key TEXT NOT NULL, supplier_name TEXT, origin_id TEXT NOT NULL, updated_at TEXT, PRIMARY KEY (client_id, supplier_key));
    CREATE TABLE IF NOT EXISTS rq_request (
      id TEXT PRIMARY KEY, client_id TEXT NOT NULL, ref TEXT NOT NULL,
      period_type TEXT NOT NULL, period_start TEXT NOT NULL, period_label TEXT, need_by TEXT,
      destination TEXT, origins_json TEXT, volume_json TEXT, source TEXT NOT NULL DEFAULT 'ask',
      state TEXT NOT NULL DEFAULT 'requested', client_note TEXT, internal_note TEXT,
      created_by TEXT, released_at TEXT, picked_at TEXT, firm_released_at TEXT, decided_at TEXT, decided_by TEXT, decision_note TEXT,
      expiry_notified_at TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')), updated_at TEXT NOT NULL DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_rq_request_client ON rq_request(client_id, state, period_start);
    CREATE TABLE IF NOT EXISTS rq_option (
      id TEXT PRIMARY KEY, request_id TEXT NOT NULL, client_id TEXT NOT NULL,
      mode TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'ask', carrier TEXT, service TEXT, origin TEXT,
      container_size TEXT, departs TEXT, cutoff TEXT, transit_days REAL, valid_until TEXT,
      price REAL, currency TEXT NOT NULL DEFAULT 'USD', price_basis TEXT NOT NULL DEFAULT 'total',
      state TEXT NOT NULL DEFAULT 'approximate', sort INTEGER NOT NULL DEFAULT 0, expired_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')), updated_at TEXT NOT NULL DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_rq_option_req ON rq_option(request_id, state);
    CREATE TABLE IF NOT EXISTS rq_option_cost (
      option_id TEXT PRIMARY KEY, partner_id TEXT, partner_name TEXT,
      cost_amount REAL, local_charges REAL NOT NULL DEFAULT 0, cost_currency TEXT NOT NULL DEFAULT 'USD', cost_note TEXT,
      markup_pct REAL, confirmed_cost_amount REAL, confirmed_local_charges REAL, confirmed_at TEXT, confirmed_note TEXT, priced_by TEXT, priced_at TEXT);
    CREATE TABLE IF NOT EXISTS rq_token (token TEXT PRIMARY KEY, request_id TEXT NOT NULL, partner_id TEXT, purpose TEXT NOT NULL, expires_at TEXT NOT NULL, used_at TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS rq_event (id TEXT PRIMARY KEY, request_id TEXT NOT NULL, option_id TEXT, from_state TEXT, to_state TEXT, actor TEXT, actor_role TEXT, detail TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_rq_event_req ON rq_event(request_id, created_at);
    CREATE TABLE IF NOT EXISTS rq_sea_rate (
      id TEXT PRIMARY KEY, client_id TEXT NOT NULL, request_id TEXT, option_id TEXT, week_start TEXT NOT NULL,
      origin TEXT, carrier TEXT, service TEXT, container_size TEXT, price REAL NOT NULL, currency TEXT NOT NULL DEFAULT 'USD',
      transit_days REAL, departs TEXT, valid_until TEXT, approved_at TEXT NOT NULL, approved_by TEXT);
    CREATE INDEX IF NOT EXISTS idx_rq_sea_rate_client ON rq_sea_rate(client_id, week_start);
  `);

  // Deleted requests stay in the table (their events and options with them) but leave every list.
  try { if (!db.prepare('PRAGMA table_info(rq_request)').all().some(c => c.name === 'archived_at')) db.exec('ALTER TABLE rq_request ADD COLUMN archived_at TEXT'); } catch (e) { L.warn('[d2d] migration: ' + e.message); }

  // ── Settings, partners, origins ──
  const setting = (client, key, dflt) => { const r = db.prepare('SELECT value FROM rq_setting WHERE client_id = ? AND key = ?').get(client, key); return r && r.value != null ? r.value : dflt; };
  const setSetting = (client, key, value, by) => db.prepare('INSERT INTO rq_setting (client_id, key, value, updated_at, updated_by) VALUES (?,?,?,?,?) ON CONFLICT(client_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at, updated_by = excluded.updated_by').run(client, key, value == null ? null : String(value), iso(), by || null);
  const partnersFor = (client, mode) => db.prepare(`SELECT * FROM quote_partner WHERE client_id = ? AND active = 1 ${mode ? (mode === 'Air' ? 'AND air = 1' : 'AND sea = 1') : ''} ORDER BY name`).all(client);
  const originsFor = (client) => db.prepare('SELECT * FROM rq_origin WHERE client_id = ? AND active = 1 ORDER BY sort, name').all(client);
  const supplierOrigins = (client) => db.prepare('SELECT so.*, o.name origin_name FROM rq_supplier_origin so LEFT JOIN rq_origin o ON o.id = so.origin_id WHERE so.client_id = ?').all(client);
  const clientEmails = (client) => parseEmailList(setting(client, 'client_notify_to', process.env.D2D_CLIENT_EMAIL_TO || process.env.AIR_QUOTE_CLIENT_EMAIL_TO || ''));
  const internalEmails = () => parseEmailList(process.env.D2D_INTERNAL_EMAIL_TO || process.env.AIR_QUOTE_INTERNAL_EMAIL_TO || process.env.EXCEPTION_EMAIL_TO || '');

  // ── Refs, events, mail ──
  const nextRef = (client) => { const ym = today().slice(2, 4) + today().slice(5, 7); const n = db.prepare("SELECT COUNT(*) n FROM rq_request WHERE client_id = ? AND ref LIKE ?").get(client, `RQ-${ym}-%`).n + 1; return `RQ-${ym}-${String(n).padStart(2, '0')}`; };
  const event = (requestId, optionId, from, to, actor, role, detail) => { try { db.prepare('INSERT INTO rq_event (id, request_id, option_id, from_state, to_state, actor, actor_role, detail) VALUES (?,?,?,?,?,?,?,?)').run(newId('rqe'), requestId, optionId || null, from || null, to || null, actor || null, role || null, detail || null); } catch (e) { L.warn('[d2d:event] ' + e.message); } };
  const setState = (rq, to, actor, role, detail) => { if (rq.state === to) return; db.prepare("UPDATE rq_request SET state = ?, updated_at = datetime('now') WHERE id = ?").run(to, rq.id); event(rq.id, null, rq.state, to, actor, role, detail); rq.state = to; };
  const setOptState = (opt, to, actor, role, detail) => { if (opt.state === to) return; db.prepare("UPDATE rq_option SET state = ?, updated_at = datetime('now')" + (to === 'expired' ? ", expired_at = datetime('now')" : '') + ' WHERE id = ?').run(to, opt.id); event(opt.request_id, opt.id, opt.state, to, actor, role, detail); opt.state = to; };
  async function mail(to, subject, html, text) {
    const list = Array.isArray(to) ? to : parseEmailList(to); const from = process.env.EXCEPTION_EMAIL_FROM;
    if (!list.length || !from) return { skipped: true };
    try { const r = await sendViaResend({ from, replyTo: process.env.EXCEPTION_EMAIL_REPLY_TO || undefined, to: list, subject, html, text }); return { id: r && r.id, to: list }; }
    catch (e) { L.error('[d2d:mail] ' + subject + ': ' + e.message); return { error: String(e.message || e) }; }
  }
  const shell = (title, paras, cta, foot) => `<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#F5F5F7;padding:24px;"><div style="max-width:560px;margin:0 auto;background:#fff;border-radius:14px;padding:24px 26px;">
    <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#6E6E73;margin-bottom:8px;">Pinpoint · Door to door</div>
    <div style="font-size:17px;font-weight:700;color:#1C1C1E;margin-bottom:12px;">${esc(title)}</div>
    ${paras.map(p => `<p style="margin:0 0 10px;font-size:13.5px;line-height:1.55;color:#1C1C1E;">${p}</p>`).join('')}
    ${cta || ''}${foot ? `<div style="margin-top:18px;font-size:11px;color:#AEAEB2;line-height:1.5;">${foot}</div>` : ''}</div></div>`;
  const btn = (href, label, primary) => `<a href="${href}" style="display:inline-block;margin:14px 8px 0 0;padding:11px 20px;border-radius:9px;background:${primary ? '#990033' : '#fff'};color:${primary ? '#fff' : '#1C1C1E'};border:1px solid ${primary ? '#990033' : '#D2D2D7'};text-decoration:none;font-size:13.5px;font-weight:600;">${esc(label)}</a>`;
  const apiBase = (req) => String(process.env.PUBLIC_API_URL || (req ? `${req.protocol}://${req.get('host')}` : '')).replace(/\/+$/, '');
  const appBase = () => String(process.env.PUBLIC_APP_URL || '').replace(/\/+$/, '');
  const makeToken = (requestId, partnerId, purpose, days) => { const t = crypto.randomBytes(24).toString('hex'); db.prepare("INSERT INTO rq_token (token, request_id, partner_id, purpose, expires_at) VALUES (?,?,?,?, datetime('now', ?))").run(t, requestId, partnerId || null, purpose, `+${days} days`); return t; };
  const tokenRow = (token, purpose) => db.prepare("SELECT * FROM rq_token WHERE token = ? AND purpose = ? AND expires_at > datetime('now')").get(String(token || ''), purpose);

  // ── Projections ──
  const rqRow = (id, client) => db.prepare('SELECT * FROM rq_request WHERE id = ?' + (client ? ' AND client_id = ?' : '')).get(...(client ? [id, client] : [id]));
  const optRows = (requestId) => db.prepare('SELECT * FROM rq_option WHERE request_id = ? ORDER BY mode, sort, created_at').all(requestId);
  const costRow = (optionId) => db.prepare('SELECT * FROM rq_option_cost WHERE option_id = ?').get(optionId) || {};
  const J = (s, d) => { try { return s ? JSON.parse(s) : d; } catch (_) { return d; } };
  // Client-facing option: explicit fields, nothing cost-derived, no partner.
  function optPublic(o) {
    const released = ['released', 'picked', 'cost_confirmed', 'firm_released', 'approved', 'declined', 'expired'];
    return { id: o.id, mode: o.mode, kind: o.kind, carrier: o.carrier, service: o.service, origin: o.origin, container_size: o.container_size,
      departs: o.departs, cutoff: o.cutoff, transit_days: o.transit_days, valid_until: o.valid_until,
      price: o.__released ? o.price : null, currency: o.currency, price_basis: o.price_basis,
      state: o.state, firm: o.state === 'firm' || o.state === 'approved', sort: o.sort };
  }
  function rqPublic(rq, opts) {
    const released = ['released', 'picked', 'cost_confirmed', 'firm_released', 'approved', 'declined', 'expired'].includes(rq.state);
    const list = (opts || optRows(rq.id)).map(o => optPublic({ ...o, __released: released && o.price != null }));
    const stateMap = { requested: 'requested', options_offered: 'with_velozity', released: 'options_ready', picked: 'picked', cost_confirmed: 'with_velozity', firm_released: 'firm', approved: 'approved', declined: 'declined', expired: 'expired' };
    return { id: rq.id, ref: rq.ref, period_type: rq.period_type, period_start: rq.period_start, period_label: rq.period_label, need_by: rq.need_by, destination: rq.destination,
      origins: J(rq.origins_json, []), volume: J(rq.volume_json, {}), source: rq.source, state: rq.state, client_state: stateMap[rq.state] || rq.state, client_note: rq.client_note,
      released_at: rq.released_at, picked_at: rq.picked_at, firm_released_at: rq.firm_released_at, decided_at: rq.decided_at, decision_note: rq.decision_note, created_at: rq.created_at,
      options: list, options_count: list.length, picked_count: list.filter(o => ['picked', 'confirmed', 'firm', 'approved'].includes(o.state)).length };
  }
  function rqInternal(rq) {
    const opts = optRows(rq.id).map(o => { const k = costRow(o.id); const c0 = k.cost_amount != null ? optionCost(rq, o, { ...k, confirmed_cost_amount: null }) : null; const cost = c0 ? c0.cost : null; const ccost = k.confirmed_cost_amount != null ? optionCost(rq, o, k).cost : null;
      return { ...optPublic({ ...o, __released: true }), partner_id: k.partner_id, partner_name: k.partner_name, cost_amount: k.cost_amount, local_charges: k.local_charges, cost_currency: k.cost_currency, cost: cost, cost_note: k.cost_note, markup_pct: k.markup_pct, confirmed_cost: ccost, confirmed_at: k.confirmed_at, confirmed_note: k.confirmed_note, priced_at: k.priced_at,
        margin: o.price != null && cost != null ? r2(o.price - (ccost != null ? ccost : cost)) : null, margin_pct: o.price != null && cost != null && cost > 0 ? r2((o.price - (ccost != null ? ccost : cost)) / (ccost != null ? ccost : cost) * 100) : null,
        validity_days: o.valid_until ? Math.round((new Date(o.valid_until + 'T00:00:00Z') - new Date(today() + 'T00:00:00Z')) / 86400000) : null }; });
    const links = db.prepare("SELECT t.purpose, t.partner_id, p.name partner_name, t.expires_at, t.used_at, t.created_at FROM rq_token t LEFT JOIN quote_partner p ON p.id = t.partner_id WHERE t.request_id = ? AND t.purpose LIKE 'partner_%' ORDER BY t.created_at DESC").all(rq.id);
    const events = db.prepare('SELECT * FROM rq_event WHERE request_id = ? ORDER BY created_at').all(rq.id);
    return { ...rqPublic(rq, []), options: opts, options_count: opts.length, picked_count: opts.filter(o => ['picked', 'confirmed', 'firm', 'approved'].includes(o.state)).length, internal_note: rq.internal_note, created_by: rq.created_by, decided_by: rq.decided_by, links, events };
  }

  // ── Prefill from the plan + signals ──
  function planRows(client, periodType, start) {
    const weeks = periodType === 'month' ? (() => { const out = []; const ym = start.slice(0, 7); let w = mondayOf(start); for (let i = 0; i < 6; i++) { const ws = addDays(w, i * 7); if (ws.slice(0, 7) === ym || addDays(ws, 6).slice(0, 7) === ym) out.push(ws); } return out; })() : [mondayOf(start)];
    const rows = [];
    for (const ws of weeks) { const r = db.prepare('SELECT data FROM plans WHERE week_start = ? AND (client_id = ? OR client_id IS NULL)').get(ws, client); for (const x of J(r && r.data, [])) rows.push({ ...x, week_start: ws }); }
    return { weeks, rows };
  }
  function prefill(client, periodType, start) {
    const { weeks, rows } = planRows(client, periodType, start);
    const so = new Map(supplierOrigins(client).map(x => [x.supplier_key, x]));
    const sig = new Map(); try { db.prepare('SELECT supplier, fill_rate_recent, fill_rate, late_days_p50, on_time_rate, confidence FROM signal_supplier WHERE client_id = ?').all(client).forEach(s => sig.set(supplierKey(s.supplier), s)); } catch (_) {}
    const bySup = new Map(); const pos = [];
    for (const r of rows) { const name = String(r.supplier_name || r.supplier || 'Unknown').trim(); const k = supplierKey(name); const units = Number(r.target_qty || r.units || 0) || 0; const mode = modeOf(r.freight_type || r.freight || r.mode);
      const s = bySup.get(k) || { supplier: name, key: k, units: 0, pos: 0, sea_units: 0, air_units: 0, origin: so.get(k) ? so.get(k).origin_name : null, origin_id: so.get(k) ? so.get(k).origin_id : null, signal: sig.get(k) || null };
      s.units += units; s.pos++; if (mode === 'Air') s.air_units += units; else s.sea_units += units; bySup.set(k, s);
      pos.push({ po: String(r.po_number || '').trim(), supplier: name, units, mode, due: r.due_date || null, week_start: r.week_start }); }
    const suppliers = [...bySup.values()].sort((a, b) => b.units - a.units);
    const expected = suppliers.reduce((a, s) => a + Math.round(s.units * ((s.signal && (s.signal.fill_rate_recent != null ? s.signal.fill_rate_recent : s.signal.fill_rate) || 100) / 100)), 0);
    const notes = suppliers.filter(s => s.signal && ((s.signal.late_days_p50 || 0) > 0 || (s.signal.fill_rate || 100) < 95)).map(s => `${s.supplier}: ${s.signal.late_days_p50 ? s.signal.late_days_p50 + ' d late' : 'on time'}, ${Math.round(s.signal.fill_rate)}% of plan`);
    const originNames = [...new Set(suppliers.map(s => s.origin).filter(Boolean))];
    return { period_type: periodType, period_start: periodType === 'month' ? start.slice(0, 7) + '-01' : mondayOf(start), period_label: periodLabel(periodType, periodType === 'month' ? start.slice(0, 7) + '-01' : mondayOf(start)), weeks,
      units: suppliers.reduce((a, s) => a + s.units, 0), expected_units: expected, pos: pos.length, suppliers, origins: originNames, unmapped: suppliers.filter(s => !s.origin).map(s => s.supplier),
      origin_list: originsFor(client), destination: setting(client, 'destination', 'Sydney · VOZ_KY'), supplier_notes: notes, po_list: pos };
  }

  // ── Options from history ("Use what we know") ──
  function historyOptions(client, rq) {
    const since = addDays(today(), -182); const out = [];
    const lanes = new Map(); try { db.prepare("SELECT mode, transit_p50, transit_p90, quoted_p50 FROM signal_lane WHERE client_id = ? AND grain = 'mode'").all(client).forEach(l => lanes.set(l.mode, l)); } catch (_) {}
    const origins = J(rq.origins_json, []);
    // Sea: approved sea rates first; failing that the priced weeks the signals job found in invoices.
    const sea = db.prepare('SELECT origin, container_size, AVG(price) price, currency, AVG(transit_days) transit, COUNT(*) n FROM rq_sea_rate WHERE client_id = ? AND approved_at >= ? GROUP BY origin, container_size, currency').all(client, since);
    if (sea.length) for (const s of sea) { if (origins.length && s.origin && !origins.includes(s.origin)) continue; out.push({ mode: 'Sea', kind: 'history', origin: s.origin, container_size: s.container_size, price: r2(s.price), currency: s.currency, price_basis: 'per_container', transit_days: s.transit || (lanes.get('Sea') || {}).transit_p50 || null, service: `From history · ${s.n} approved`, carrier: null }); }
    else { try { const p = db.prepare("SELECT AVG(price_per_container) p, COUNT(*) n FROM signal_price_week WHERE client_id = ? AND mode = 'Sea' AND price_per_container IS NOT NULL").get(client); if (p && p.p) out.push({ mode: 'Sea', kind: 'history', origin: origins[0] || null, container_size: '40', price: r2(p.p), currency: 'AUD', price_basis: 'per_container', transit_days: (lanes.get('Sea') || {}).transit_p50 || null, service: `From history · ${p.n} invoiced weeks`, carrier: null }); } catch (_) {} }
    // Air: approved air quotes, price per chargeable kg.
    // Air: approved air quotes on the request's origins (quotes with no origin recorded count too), one weighted
    // figure per currency — total sell over total chargeable kg, the number the team quotes.
    const aq = db.prepare("SELECT sell_amount, chargeable_kg, currency, origin FROM air_quote WHERE client_id = ? AND state = 'approved' AND decided_at >= ? AND chargeable_kg > 0 AND sell_amount > 0").all(client, since)
      .filter(a => !origins.length || !a.origin || origins.includes(a.origin));
    const byCur = new Map(); for (const a of aq) { const c = a.currency || 'USD'; const e = byCur.get(c) || { sell: 0, kg: 0, n: 0 }; e.sell += Number(a.sell_amount); e.kg += Number(a.chargeable_kg); e.n++; byCur.set(c, e); }
    for (const [cur, e] of byCur) { if (!e.kg) continue; out.push({ mode: 'Air', kind: 'history', origin: origins[0] || null, price: r2(e.sell / e.kg), currency: cur, price_basis: 'per_kg', transit_days: (lanes.get('Air') || {}).transit_p50 || null, service: `From history · ${e.n} approved`, carrier: null }); }
    return out;
  }

  // ── Partner RFQ ──
  async function sendPartnerLinks(rq, req, opts) {
    const o = opts || {}; const modes = o.modes || ['Sea', 'Air']; const vol = J(rq.volume_json, {}); const origins = J(rq.origins_json, []);
    const partners = new Map(); for (const m of modes) for (const p of partnersFor(rq.client_id, m)) { const e = partners.get(p.id) || { ...p, modes: [] }; e.modes.push(m); partners.set(p.id, e); }
    const out = [];
    for (const p of partners.values()) {
      const token = makeToken(rq.id, p.id, 'partner_options', LINK_DAYS); const link = `${apiBase(req)}/rq/partner?token=${token}`;
      const lines = [`Options request ${rq.ref} — ${p.modes.join(' and ').toLowerCase()} freight, ${rq.period_label}`, '',
        `Origin${origins.length === 1 ? '' : 's'}: ${origins.join(', ') || 'see link'}`, `Destination: ${rq.destination || 'Sydney'}`, `Period: ${rq.period_label} (${rq.period_type})`, rq.need_by ? `Must be in the Sydney FC by: ${dmy(rq.need_by)}` : null,
        vol.units ? `Volume: ${Number(vol.units).toLocaleString()} units${vol.cartons ? ' · ' + vol.cartons + ' cartons' : ''}${vol.cbm ? ' · ' + vol.cbm + ' CBM' : ''}${vol.kg ? ' · ' + vol.kg + ' kg' : ''}` : null,
        o.note ? '' : null, o.note ? `From VelOzity: ${o.note}` : null, '', 'Please enter one row per option (service, sailing or flight, transit, cost, valid-to):', link, '', `This link expires in ${LINK_DAYS} days.`].filter(x => x !== null);
      const html = `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px;color:#1C1C1E;">` + lines.map(l => l === '' ? '<div style="height:10px"></div>' : (l === link ? `<div><a href="${link}">${link}</a></div>` : `<div>${esc(l)}</div>`)).join('') + '</div>';
      const r = await mail([p.email], `${o.subjectPrefix || 'RFQ'} ${rq.ref} — ${p.modes.join('/').toLowerCase()} options, ${rq.period_label}`, html, lines.join('\n'));
      event(rq.id, null, null, null, o.actor || 'system', o.actorRole || 'system', `Options link ${r.error ? 'issued, email failed: ' + r.error : (r.skipped ? 'issued, no email configured' : 'emailed')} → ${p.name} (${p.modes.join(', ')})`);
      out.push({ partner: p.name, modes: p.modes, link, emailed: !r.error && !r.skipped, error: r.error || null });
    }
    return out;
  }
  async function sendConfirmLinks(rq, req, actor) {
    const picked = optRows(rq.id).filter(o => o.state === 'picked'); const byPartner = new Map();
    for (const o of picked) { const k = costRow(o.id); const pid = k.partner_id || '__none'; (byPartner.get(pid) || byPartner.set(pid, []).get(pid)).push(o); }
    const out = [];
    for (const [pid, list] of byPartner) {
      // Options from history have no partner yet: every sea/air partner is asked to firm them up.
      const targets = pid === '__none' ? [...new Map([...list.flatMap(o => partnersFor(rq.client_id, o.mode))].map(p => [p.id, p])).values()] : [db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(pid)].filter(Boolean);
      for (const p of targets) {
        const token = makeToken(rq.id, p.id, 'partner_confirm', LINK_DAYS); const link = `${apiBase(req)}/rq/partner?token=${token}`;
        const lines = [`Please confirm cost for ${rq.ref}`, '', ...list.map(o => `• ${o.mode} · ${[o.carrier, o.service].filter(Boolean).join(' ') || 'option'}${o.container_size ? ' · ' + o.container_size + "'" : ''}${o.departs ? ' · departs ' + dmy(o.departs) : ''}${o.transit_days != null ? ' · ' + o.transit_days + ' d transit' : ''}`), '', 'Confirm or revise here:', link, '', `Please reply within ${Math.min(3, LINK_DAYS)} business days.`];
        const html = `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px;color:#1C1C1E;">` + lines.map(l => l === '' ? '<div style="height:10px"></div>' : (l === link ? `<div><a href="${link}">${link}</a></div>` : `<div>${esc(l)}</div>`)).join('') + '</div>';
        const r = await mail([p.email], `Confirm cost ${rq.ref} — ${rq.period_label}`, html, lines.join('\n'));
        event(rq.id, null, null, null, actor || 'system', 'internal', `Confirm link ${r.error ? 'issued, email failed' : (r.skipped ? 'issued, no email configured' : 'emailed')} → ${p.name} (${list.length} option${list.length === 1 ? '' : 's'})`);
        out.push({ partner: p.name, link, emailed: !r.error && !r.skipped });
      }
    }
    return out;
  }

  // ── Client notifications ──
  async function notifyClient(rq, kind, req, extra) {
    const to = clientEmails(rq.client_id); if (!to.length) return { skipped: true };
    const opts = optRows(rq.id); const live = opts.filter(o => ['approximate', 'picked', 'confirmed', 'firm'].includes(o.state));
    const url = `${appBase()}/#simulator`; const nSea = live.filter(o => o.mode === 'Sea').length, nAir = live.filter(o => o.mode === 'Air').length;
    let subject, html, text;
    if (kind === 'released') {
      const vt = live.map(o => o.valid_until).filter(Boolean).sort()[0];
      subject = `${live.length} option${live.length === 1 ? '' : 's'} ready for ${rq.period_label} — ${rq.ref}`;
      const p = [`<b>${live.length} option${live.length === 1 ? ' is' : 's are'} ready for ${esc(rq.period_label)}${(J(rq.origins_json, []).length ? ', ' + esc(J(rq.origins_json, []).join(' · ')) + ' → ' + esc(rq.destination || 'Sydney') : '')}.</b>`,
        `${nAir ? nAir + ' air' : ''}${nAir && nSea ? ', ' : ''}${nSea ? nSea + ' sea' : ''}. Prices are ${live.every(o => o.kind === 'history') ? 'from history' : 'approximate'}${vt ? ' and valid to ' + esc(dmy(vt)) : ''}. Open the simulator to compare them on price, days${rq.need_by ? ' and the chance of making your ' + esc(dmy(rq.need_by)) + ' date' : ''}.`];
      html = shell(subject, p, btn(url, 'Open the simulator', true), 'Pinpoint · VelOzity'); text = p.map(x => x.replace(/<[^>]+>/g, '')).join('\n') + '\n' + url;
    } else if (kind === 'firm') {
      const firm = opts.filter(o => o.state === 'firm'); const total = firm.reduce((a, o) => a + (Number(o.price) || 0), 0); const cur = firm[0] ? firm[0].currency : 'USD'; const vt = firm.map(o => o.valid_until).filter(Boolean).sort()[0];
      const moved = extra && extra.moved ? extra.moved : [];
      subject = `Firm price for your pick — ${rq.ref}, ${rq.period_label}`;
      const p = [`<b>Firm price for your pick: ${esc(fmtMoney(total, cur))}${vt ? ', valid to ' + esc(dmy(vt)) : ''}.</b>`,
        moved.length ? moved.map(m => esc(m)).join('<br>') : 'Unchanged from the approximate price. Days and cutoff odds unchanged.', `Approve in Pinpoint and we'll instruct the carrier.`];
      html = shell(subject, p, btn(url, 'Approve in Pinpoint', true), 'Pinpoint · VelOzity'); text = p.map(x => x.replace(/<[^>]+>/g, '')).join('\n') + '\n' + url;
    } else if (kind === 'expired') {
      subject = `Your picked option for ${rq.period_label} has expired — ${rq.ref}`;
      const p = [`<b>The option you picked for ${esc(rq.period_label)} passed its validity date before it was approved.</b>`, `It stays on the page marked expired so nothing goes missing. Ask VelOzity to refresh it and we'll come back with current options for the same period.`];
      html = shell(subject, p, btn(url, 'Ask VelOzity to refresh', true), 'Pinpoint · VelOzity'); text = p.map(x => x.replace(/<[^>]+>/g, '')).join('\n') + '\n' + url;
    } else return { skipped: true };
    const r = await mail(to, subject, html, text); event(rq.id, null, null, null, 'system', 'system', `Client ${kind} email ${r.error ? 'failed: ' + r.error : (r.skipped ? 'skipped' : 'sent')}`); return r;
  }
  async function notifyInternal(rq, title, lines) {
    const to = internalEmails(); if (!to.length) return { skipped: true };
    const url = `${appBase()}/#quotes`; return mail(to, `${title} — ${rq.ref}, ${rq.period_label}`, shell(title, lines.map(esc), btn(url, 'Open Quote Review', true), 'Internal · cost and margin may appear here.'), lines.join('\n'));
  }
  async function notifyPartnerProceed(rq, req) {
    const approved = optRows(rq.id).filter(o => o.state === 'approved'); const byP = new Map();
    for (const o of approved) { const k = costRow(o.id); if (!k.partner_id) continue; (byP.get(k.partner_id) || byP.set(k.partner_id, []).get(k.partner_id)).push(o); }
    for (const [pid, list] of byP) { const p = db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(pid); if (!p) continue;
      const lines = [`Please proceed with ${rq.ref} — ${rq.period_label}`, '', ...list.map(o => `• ${o.mode} · ${[o.carrier, o.service].filter(Boolean).join(' ')}${o.container_size ? ' · ' + o.container_size + "'" : ''}${o.departs ? ' · departs ' + dmy(o.departs) : ''} · confirmed cost ${(() => { const k = costRow(o.id); const c = optionCost(rq, o, k); return c ? fmtMoney(c.cost, k.cost_currency) + (c.basis === 'per_kg' ? '/kg' : '') : '—'; })()}`), '', 'Booking details to follow from VelOzity.'];
      const r = await mail([p.email], `Proceed ${rq.ref} — ${rq.period_label}`, `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px;color:#1C1C1E;">` + lines.map(l => l === '' ? '<div style="height:10px"></div>' : `<div>${esc(l)}</div>`).join('') + '</div>', lines.join('\n'));
      event(rq.id, null, null, null, 'system', 'system', `Proceed email ${r.error ? 'failed' : (r.skipped ? 'skipped' : 'sent')} → ${p.name}`); }
  }

  // ── Pricing ──
  // Chargeable kg for the request: the greater of gross and volumetric, as air quotes do.
  const chargeableFor = (rq) => { const vol = J(rq.volume_json, {}); const kg = Number(vol.kg) || 0, cbm = Number(vol.cbm) || 0; if (!kg && !cbm) return 0; return aqChargeableKg ? aqChargeableKg(kg, cbm) : Math.max(kg, cbm * 167); };
  // Total cost of an option on this request. Sea: per container + local. Air: per-kg × chargeable kg + local
  // when the request carries weight or volume; otherwise the per-kg rate alone (basis per_kg, local shown separately).
  function optionCost(rq, o, k) {
    const conf = k.confirmed_cost_amount != null; const unit = Number(conf ? k.confirmed_cost_amount : k.cost_amount); const local = Number(conf ? (k.confirmed_local_charges || 0) : (k.local_charges || 0));
    if (!isFinite(unit)) return null;
    if (o.mode === 'Air' && o.kind === 'ask') { const ch = chargeableFor(rq); return ch > 0 ? { cost: r2(unit * ch + local), basis: 'total' } : { cost: r2(unit), basis: 'per_kg' }; }
    return { cost: r2(unit + local), basis: o.mode === 'Sea' ? 'per_container' : (o.price_basis || 'total') };
  }
  function priceOption(rq, o, k, markupPct) { const c = optionCost(rq, o, k); if (!c) return null; return { price: r2(c.cost * (1 + Number(markupPct) / 100)), basis: c.basis }; }
  function applyDefaultPricing(rq) {
    for (const o of optRows(rq.id)) { if (o.kind !== 'ask') continue; const k = costRow(o.id); if (k.cost_amount == null) continue; const m = k.markup_pct != null ? k.markup_pct : markupFor(rq.client_id, o.mode); const p = priceOption(rq, o, k, m); if (!p) continue;
      db.prepare("UPDATE rq_option_cost SET markup_pct = ? WHERE option_id = ?").run(m, o.id); db.prepare("UPDATE rq_option SET price = ?, price_basis = ?, currency = ?, updated_at = datetime('now') WHERE id = ?").run(p.price, p.basis, k.cost_currency || o.currency, o.id); }
  }

  // ── Approval writes ──
  function writeApproved(rq, opts, actor) {
    const vol = J(rq.volume_json, {}); const origins = J(rq.origins_json, []); const ws = rq.period_type === 'week' ? rq.period_start : mondayOf(rq.period_start);
    for (const o of opts) {
      const k = costRow(o.id);
      if (o.mode === 'Sea') {
        db.prepare('INSERT INTO rq_sea_rate (id, client_id, request_id, option_id, week_start, origin, carrier, service, container_size, price, currency, transit_days, departs, valid_until, approved_at, approved_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
          .run(newId('sr'), rq.client_id, rq.id, o.id, o.departs ? mondayOf(o.departs) || ws : ws, o.origin || origins[0] || null, o.carrier, o.service, o.container_size, o.price, o.currency, o.transit_days, o.departs, o.valid_until, iso(), actor || null);
      } else {
        // An approved air leg is an approved air quote, so Quote Review, reporting and the intel panel see it.
        try {
          const id = newId('aq'); const ym = today().slice(2, 4) + today().slice(5, 7);
          const n = db.prepare("SELECT COUNT(*) n FROM air_quote WHERE client_id = ? AND ref LIKE ?").get(rq.client_id, `AQ-${ym}-%`).n + 1; const ref = `AQ-${ym}-${String(n).padStart(3, '0')}`;
          const kg = Number(vol.kg) || 0, cbm = Number(vol.cbm) || 0; const ch = chargeableFor(rq);
          const total = o.price_basis === 'per_kg' ? r2(o.price * (ch || 0)) : o.price;
          db.prepare(`INSERT INTO air_quote (id, client_id, ref, week_start, week_label, zendesk_ticket, po_numbers, vendor_raw, vendor_key, cartons, units, gross_weight_kg, cbm, chargeable_kg, origin, destination, client_note, state, sell_amount, currency, valid_until, released_at, decided_at, decided_by, decided_via, created_by, created_at, updated_at)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'approved',?,?,?,datetime('now'),datetime('now'),?,'d2d',?,datetime('now'),datetime('now'))`)
            .run(id, rq.client_id, ref, ws, periodLabel('week', ws), null, (vol.pos || []).filter(p => p.mode === 'Air').map(p => p.po).join(', ') || null, (vol.suppliers || []).map(s => s.supplier).join(', ') || 'Door to door', 'd2d', Number(vol.cartons) || 0, Number(vol.units) || null, kg, cbm, ch, o.origin || origins.join(', ') || null, rq.destination, `Door to door ${rq.ref} · ${[o.carrier, o.service].filter(Boolean).join(' ')}`, total, o.currency, o.valid_until, actor || null, actor || null);
          const c = k.cost_amount != null ? optionCost(rq, o, k) : null;
          if (c) db.prepare('INSERT INTO air_quote_cost (quote_id, partner_name, cost_amount, cost_currency, markup_pct, transit_mode, partner_note) VALUES (?,?,?,?,?,?,?)').run(id, k.partner_name || null, c.basis === 'per_kg' ? r2(c.cost * (ch || 0)) : c.cost, k.cost_currency || o.currency, k.markup_pct, 'VOZAIR', `d2d ${rq.ref} ${o.id}`);
          event(rq.id, o.id, null, null, actor, 'client', `Approved air leg written as ${ref}`);
        } catch (e) { L.warn('[d2d] air quote write failed: ' + e.message); event(rq.id, o.id, null, null, 'system', 'system', 'Air quote write failed: ' + e.message); }
      }
    }
  }

  // ── Expiry ticker ──
  async function expire() {
    const t = today();
    const stale = db.prepare("SELECT * FROM rq_option WHERE valid_until IS NOT NULL AND valid_until < ? AND state IN ('approximate','picked','confirmed','firm')").all(t);
    const touched = new Set();
    for (const o of stale) { setOptState(o, 'expired', 'system', 'system', `valid to ${o.valid_until}`); touched.add(o.request_id); }
    for (const id of touched) {
      const rq = rqRow(id); if (!rq || ['approved', 'declined', 'expired'].includes(rq.state)) continue;
      const opts = optRows(id); const live = opts.filter(o => ['approximate', 'picked', 'confirmed', 'firm'].includes(o.state));
      const pickedGone = ['picked', 'cost_confirmed', 'firm_released'].includes(rq.state) && !opts.some(o => ['picked', 'confirmed', 'firm'].includes(o.state));
      if (pickedGone) { setState(rq, 'expired', 'system', 'system', 'picked option passed its validity date'); if (!rq.expiry_notified_at) { db.prepare("UPDATE rq_request SET expiry_notified_at = datetime('now') WHERE id = ?").run(id); await notifyClient(rq, 'expired'); } }
      else if (!live.length && rq.state === 'released') setState(rq, 'expired', 'system', 'system', 'every option passed its validity date');
    }
    return { expired: stale.length };
  }
  if (!/^(off|0|false|no)$/i.test(String(process.env.D2D_EXPIRY || 'on')) && process.env.NODE_ENV !== 'test') { setInterval(() => expire().catch(e => L.warn('[d2d] expiry: ' + e.message)), 10 * 60 * 1000).unref(); setTimeout(() => expire().catch(() => {}), 20000).unref(); }

  // ══════════════ Client + internal routes ══════════════
  const guard = [authenticateRequest, requireClientOrInternal];
  const isInternalReq = (req) => { try { const t = tenancyResolve ? tenancyResolve(req.auth && req.auth.orgId, req.auth && req.auth.orgRole) : null; return !!(t && t.org_type === 'internal'); } catch (_) { return false; } };

  app.get('/rq/prefill', ...guard, (req, res) => {
    try { const type = req.query.period_type === 'month' ? 'month' : 'week'; const start = String(req.query.period_start || mondayOf(today())); if (!mondayOf(start)) return res.status(400).json({ error: 'bad period_start' }); res.json(prefill(curClient(), type, start)); }
    catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.get('/rq/origins', ...guard, (req, res) => { try { res.json({ origins: originsFor(curClient()), suppliers: supplierOrigins(curClient()) }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  // The client may map a supplier to an origin from the request form; adding origins is internal.
  app.put('/rq/supplier-origin', ...guard, writeOpLimiter, auditLog('rq_supplier_origin'), (req, res) => {
    try { const c = curClient(); const b = req.body || {}; const o = db.prepare('SELECT * FROM rq_origin WHERE id = ? AND client_id = ?').get(String(b.origin_id || ''), c); if (!o || !b.supplier) return res.status(400).json({ error: 'origin_id and supplier required' });
      db.prepare("INSERT INTO rq_supplier_origin (client_id, supplier_key, supplier_name, origin_id, updated_at) VALUES (?,?,?,?,datetime('now')) ON CONFLICT(client_id, supplier_key) DO UPDATE SET origin_id = excluded.origin_id, supplier_name = excluded.supplier_name, updated_at = excluded.updated_at").run(c, supplierKey(b.supplier), String(b.supplier), o.id);
      res.json({ ok: true, supplier: b.supplier, origin: o.name }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  app.get('/rq/requests', ...guard, (req, res) => {
    try { const c = curClient(); const rows = db.prepare('SELECT * FROM rq_request WHERE client_id = ? AND archived_at IS NULL ORDER BY period_start DESC, created_at DESC LIMIT 60').all(c); res.json({ requests: rows.map(r => rqPublic(r)) }); }
    catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.get('/rq/requests/:id', ...guard, (req, res) => { try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); res.json(rqPublic(rq)); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });

  app.post('/rq/requests', ...guard, writeOpLimiter, auditLog('create_d2d_request'), async (req, res) => {
    try {
      const c = curClient(); const b = req.body || {}; const type = b.period_type === 'month' ? 'month' : 'week';
      const start = type === 'month' ? String(b.period_start || '').slice(0, 7) + '-01' : mondayOf(String(b.period_start || '')); if (!start || !mondayOf(start)) return res.status(400).json({ error: 'period_start required (YYYY-MM-DD)' });
      const source = b.source === 'history' ? 'history' : 'ask';
      const pf = prefill(c, type, start);
      const origins = Array.isArray(b.origins) && b.origins.length ? b.origins.map(String) : pf.origins;
      const vol = { units: Number(b.units) || pf.units, expected_units: pf.expected_units, cartons: Number(b.cartons) || null, cbm: Number(b.cbm) || null, kg: Number(b.kg) || null, pos: pf.po_list, suppliers: pf.suppliers.map(s => ({ supplier: s.supplier, units: s.units, origin: s.origin, sea_units: s.sea_units, air_units: s.air_units })), supplier_notes: pf.supplier_notes };
      const id = newId('rq'); const ref = nextRef(c);
      db.prepare('INSERT INTO rq_request (id, client_id, ref, period_type, period_start, period_label, need_by, destination, origins_json, volume_json, source, state, client_note, created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
        .run(id, c, ref, type, start, periodLabel(type, start), b.need_by ? String(b.need_by).slice(0, 10) : null, b.destination || pf.destination, JSON.stringify(origins), JSON.stringify(vol), source, 'requested', b.note ? String(b.note).slice(0, 1000) : null, actorOf(req));
      const rq = rqRow(id); event(id, null, null, 'requested', actorOf(req), isInternalReq(req) ? 'internal' : 'client', `${source === 'history' ? 'Use what we know' : 'Ask VelOzity'} · ${origins.join(', ') || 'no origin'} → ${rq.destination} · ${vol.units} units`);
      let links = [];
      if (source === 'history') {
        const hist = historyOptions(c, rq);
        hist.forEach((h, i) => db.prepare('INSERT INTO rq_option (id, request_id, client_id, mode, kind, carrier, service, origin, container_size, transit_days, valid_until, price, currency, price_basis, state, sort) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
          .run(newId('op'), id, c, h.mode, 'history', h.carrier, h.service, h.origin, h.container_size || null, h.transit_days, addDays(today(), 14), h.price, h.currency, h.price_basis, 'approximate', i));
        if (hist.length) { db.prepare("UPDATE rq_request SET released_at = datetime('now') WHERE id = ?").run(id); setState(rq, 'released', 'system', 'system', `${hist.length} option(s) from history`); }
        else event(id, null, null, null, 'system', 'system', 'No history on these lanes yet — Ask VelOzity for options');
      } else {
        links = await sendPartnerLinks(rq, req, { actor: actorOf(req), actorRole: isInternalReq(req) ? 'internal' : 'client', modes: Array.isArray(b.modes) && b.modes.length ? b.modes : ['Sea', 'Air'] });
        if (!links.length) event(id, null, null, null, 'system', 'system', 'No carrier partner configured for this client — add one under Finance › Quotes');
        notifyInternal(rq, 'New door-to-door request', [`${rq.ref} · ${rq.period_label} · ${origins.join(', ')} → ${rq.destination}`, `${vol.units} units · need by ${rq.need_by ? dmy(rq.need_by) : '—'}`, links.length ? `Options links sent to ${links.map(l => l.partner).join(', ')}` : 'No carrier partner configured — add one under Finance › Quotes']).catch(() => {});
      }
      res.status(201).json({ ...rqPublic(rqRow(id)), partner_links_sent: links.length, no_history: source === 'history' && rqRow(id).state === 'requested' });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // Client picks one or more released options. Picking asks VelOzity to confirm — the partner firms up cost.
  app.post('/rq/requests/:id/pick', ...guard, writeOpLimiter, auditLog('pick_d2d_option'), async (req, res) => {
    try {
      const c = curClient(); const rq = rqRow(req.params.id, c); if (!rq) return res.status(404).json({ error: 'not_found' });
      if (!['released', 'picked', 'expired'].includes(rq.state)) return res.status(409).json({ error: 'not_pickable', state: rq.state });
      const ids = Array.isArray((req.body || {}).option_ids) ? (req.body || {}).option_ids.map(String) : []; if (!ids.length) return res.status(400).json({ error: 'option_ids required' });
      const opts = optRows(rq.id); const chosen = opts.filter(o => ids.includes(o.id));
      if (chosen.length !== ids.length || chosen.some(o => !['approximate', 'picked'].includes(o.state))) return res.status(409).json({ error: 'option_not_available' });
      for (const o of opts) { if (ids.includes(o.id)) setOptState(o, 'picked', actorOf(req), 'client', null); else if (o.state === 'picked') setOptState(o, 'approximate', actorOf(req), 'client', 'unpicked'); }
      db.prepare("UPDATE rq_request SET picked_at = datetime('now') WHERE id = ?").run(rq.id); setState(rq, 'picked', actorOf(req), 'client', `${ids.length} option(s) picked`);
      const links = await sendConfirmLinks(rq, req, actorOf(req));
      notifyInternal(rq, 'Client picked options', [`${rq.ref} · ${chosen.map(o => `${o.mode} ${[o.carrier, o.service].filter(Boolean).join(' ')}`).join('; ')}`, links.length ? `Confirm links sent to ${links.map(l => l.partner).join(', ')}` : 'No partner to confirm with — confirm cost manually in Quote Review']).catch(() => {});
      res.json({ ...rqPublic(rqRow(rq.id)), confirm_links_sent: links.length });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Approve the firm options (the record of the week) — or decline the request.
  app.post('/rq/requests/:id/approve', ...guard, writeOpLimiter, auditLog('approve_d2d_request'), async (req, res) => {
    try {
      const c = curClient(); const rq = rqRow(req.params.id, c); if (!rq) return res.status(404).json({ error: 'not_found' });
      if (rq.state !== 'firm_released') return res.status(409).json({ error: 'not_firm', state: rq.state, message: rq.state === 'picked' || rq.state === 'cost_confirmed' ? 'VelOzity is confirming the price — approval opens once it is firm.' : 'Nothing firm to approve.' });
      const firm = optRows(rq.id).filter(o => o.state === 'firm'); if (!firm.length) return res.status(409).json({ error: 'no_firm_options' });
      const actor = actorOf(req);
      for (const o of firm) setOptState(o, 'approved', actor, 'client', null);
      writeApproved(rq, firm, actor);
      db.prepare("UPDATE rq_request SET decided_at = datetime('now'), decided_by = ?, decision_note = ? WHERE id = ?").run(actor, (req.body || {}).note || null, rq.id); setState(rq, 'approved', actor, 'client', `${firm.length} option(s) approved`);
      await notifyPartnerProceed(rq, req); notifyInternal(rq, 'Client approved', [`${rq.ref} · ${firm.map(o => `${o.mode} ${[o.carrier, o.service].filter(Boolean).join(' ')} ${fmtMoney(o.price, o.currency)}`).join('; ')}`]).catch(() => {});
      res.json(rqPublic(rqRow(rq.id)));
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.post('/rq/requests/:id/decline', ...guard, writeOpLimiter, auditLog('decline_d2d_request'), (req, res) => {
    try { const c = curClient(); const rq = rqRow(req.params.id, c); if (!rq) return res.status(404).json({ error: 'not_found' }); if (['approved', 'declined'].includes(rq.state)) return res.status(409).json({ error: 'already_decided' });
      const actor = actorOf(req); db.prepare("UPDATE rq_request SET decided_at = datetime('now'), decided_by = ?, decision_note = ? WHERE id = ?").run(actor, (req.body || {}).note || null, rq.id); setState(rq, 'declined', actor, 'client', (req.body || {}).note || null);
      notifyInternal(rq, 'Client declined', [`${rq.ref} · ${(req.body || {}).note || 'no note'}`]).catch(() => {}); res.json(rqPublic(rqRow(rq.id))); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Delete a request that never materialised. Approved requests are the period's record: they are hidden from the
  // lists but their approved quotes and sea rate stay. Options still live are withdrawn so no partner link can act.
  app.delete('/rq/requests/:id', ...guard, writeOpLimiter, auditLog('delete_d2d_request'), (req, res) => {
    try { const c = curClient(); const rq = rqRow(req.params.id, c); if (!rq) return res.status(404).json({ error: 'not_found' }); if (rq.archived_at) return res.json({ ok: true, already: true });
      const actor = actorOf(req);
      for (const o of optRows(rq.id)) if (['approximate', 'picked', 'confirmed', 'firm'].includes(o.state)) setOptState(o, 'withdrawn', actor, isInternalReq(req) ? 'internal' : 'client', 'request deleted');
      db.prepare("UPDATE rq_request SET archived_at = datetime('now'), updated_at = datetime('now') WHERE id = ?").run(rq.id); event(rq.id, null, rq.state, rq.state === 'approved' ? 'approved' : 'deleted', actor, isInternalReq(req) ? 'internal' : 'client', rq.state === 'approved' ? 'hidden from lists (approved quotes kept)' : 'deleted');
      res.json({ ok: true, hidden: rq.state === 'approved' }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // After an expiry (or any time before approval): Ask VelOzity for fresh options on the same request.
  app.post('/rq/requests/:id/refresh', ...guard, writeOpLimiter, auditLog('refresh_d2d_request'), async (req, res) => {
    try { const c = curClient(); const rq = rqRow(req.params.id, c); if (!rq) return res.status(404).json({ error: 'not_found' }); if (['approved', 'declined'].includes(rq.state)) return res.status(409).json({ error: 'already_decided' });
      for (const o of optRows(rq.id)) if (['approximate', 'picked', 'confirmed', 'firm'].includes(o.state)) setOptState(o, 'withdrawn', actorOf(req), 'client', 'refresh requested');
      db.prepare("UPDATE rq_request SET source = 'ask', expiry_notified_at = NULL WHERE id = ?").run(rq.id); setState(rq, 'requested', actorOf(req), 'client', 'refresh — Ask VelOzity');
      const links = await sendPartnerLinks(rqRow(rq.id), req, { actor: actorOf(req), actorRole: 'client', subjectPrefix: 'RFQ (refresh)' });
      notifyInternal(rqRow(rq.id), 'Client asked for fresh options', [`${rq.ref} · ${rq.period_label}`, links.length ? `Links sent to ${links.map(l => l.partner).join(', ')}` : 'No carrier partner configured']).catch(() => {});
      res.json({ ...rqPublic(rqRow(rq.id)), partner_links_sent: links.length }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ══════════════ Internal: review, pricing, release ══════════════
  const admin = [authenticateRequest, requireRole(['admin']), requireInternalOrg];
  app.get('/rq/internal/requests', ...admin, (req, res) => { try { const c = curClient(); const rows = db.prepare('SELECT * FROM rq_request WHERE client_id = ? AND archived_at IS NULL ORDER BY CASE WHEN state IN (\'approved\',\'declined\',\'expired\') THEN 1 ELSE 0 END, period_start DESC, created_at DESC LIMIT 80').all(c); res.json({ requests: rows.map(rqInternal), partners: partnersFor(c), markup: { Air: markupFor(c, 'Air'), Sea: markupFor(c, 'Sea') } }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  app.get('/rq/internal/requests/:id', ...admin, (req, res) => { try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); res.json({ ...rqInternal(rq), markup: { Air: markupFor(rq.client_id, 'Air'), Sea: markupFor(rq.client_id, 'Sea') } }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  // Re-send or add partner links; `note` goes to the partner.
  app.post('/rq/internal/requests/:id/rfq', ...admin, writeOpLimiter, auditLog('d2d_rfq'), async (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); const b = req.body || {};
      const links = await sendPartnerLinks(rq, req, { actor: actorOf(req), actorRole: 'internal', note: b.note, modes: Array.isArray(b.modes) && b.modes.length ? b.modes : ['Sea', 'Air'] }); res.json({ sent: links }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Internal can add an option by hand (a partner that phoned it in) — cost goes to the cost table.
  app.post('/rq/internal/requests/:id/options', ...admin, writeOpLimiter, auditLog('d2d_add_option'), (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); const b = req.body || {}; const mode = modeOf(b.mode);
      const id = newId('op'); db.prepare('INSERT INTO rq_option (id, request_id, client_id, mode, kind, carrier, service, origin, container_size, departs, cutoff, transit_days, valid_until, currency, price_basis, state, sort) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
        .run(id, rq.id, rq.client_id, mode, 'ask', b.carrier || null, b.service || null, b.origin || null, mode === 'Sea' ? (CONTAINER.includes(String(b.container_size)) ? String(b.container_size) : '40') : null, b.departs || null, b.cutoff || null, b.transit_days != null ? Number(b.transit_days) : null, b.valid_until || addDays(today(), 7), b.cost_currency || 'USD', mode === 'Sea' ? 'per_container' : (b.price_basis === 'per_kg' ? 'per_kg' : 'total'), 'approximate', optRows(rq.id).length);
      const p = b.partner_id ? db.prepare('SELECT * FROM quote_partner WHERE id = ? AND client_id = ?').get(String(b.partner_id), rq.client_id) : null;
      db.prepare('INSERT INTO rq_option_cost (option_id, partner_id, partner_name, cost_amount, local_charges, cost_currency, cost_note) VALUES (?,?,?,?,?,?,?)').run(id, p ? p.id : null, p ? p.name : (b.partner_name || null), b.cost_amount != null ? Number(b.cost_amount) : null, Number(b.local_charges) || 0, b.cost_currency || 'USD', b.cost_note || null);
      event(rq.id, id, null, 'approximate', actorOf(req), 'internal', 'Option added by hand'); if (rq.state === 'requested') setState(rq, 'options_offered', actorOf(req), 'internal', null); applyDefaultPricing(rq);
      res.status(201).json(rqInternal(rqRow(rq.id))); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Price one option: by markup % (from cost) or a price typed directly (margin shown back).
  app.post('/rq/internal/requests/:id/options/:oid/price', ...admin, writeOpLimiter, auditLog('d2d_price_option'), (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); const o = db.prepare('SELECT * FROM rq_option WHERE id = ? AND request_id = ?').get(req.params.oid, rq.id); if (!o) return res.status(404).json({ error: 'option_not_found' });
      if (['approved', 'expired', 'withdrawn'].includes(o.state)) return res.status(409).json({ error: 'not_priceable', state: o.state });
      const b = req.body || {}; const k = costRow(o.id); let price, markup = k.markup_pct;
      if (b.price != null) { price = r2(b.price); const c = k.cost_amount != null ? optionCost(rq, o, k) : null; markup = c && c.cost ? r2((price - c.cost) / c.cost * 100) : markup; }
      else if (b.markup_pct != null) { markup = Number(b.markup_pct); if (!isFinite(markup) || markup < 0) return res.status(400).json({ error: 'bad markup' }); if (k.cost_amount == null) return res.status(409).json({ error: 'no_cost', message: 'This option has no partner cost — type a price instead.' }); const p = priceOption(rq, o, k, markup); price = p.price; db.prepare('UPDATE rq_option SET price_basis = ? WHERE id = ?').run(p.basis, o.id); }
      else return res.status(400).json({ error: 'price or markup_pct required' });
      db.prepare('INSERT INTO rq_option_cost (option_id, markup_pct, priced_by, priced_at) VALUES (?,?,?,?) ON CONFLICT(option_id) DO UPDATE SET markup_pct = excluded.markup_pct, priced_by = excluded.priced_by, priced_at = excluded.priced_at').run(o.id, markup, actorOf(req), iso());
      db.prepare("UPDATE rq_option SET price = ?, currency = COALESCE(?, currency), updated_at = datetime('now') WHERE id = ?").run(price, b.currency || k.cost_currency || null, o.id);
      event(rq.id, o.id, null, null, actorOf(req), 'internal', `Priced ${fmtMoney(price, b.currency || k.cost_currency || o.currency)}${markup != null ? ' · markup ' + markup + '%' : ''}`);
      res.json(rqInternal(rqRow(rq.id))); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.post('/rq/internal/requests/:id/options/:oid/withdraw', ...admin, writeOpLimiter, auditLog('d2d_withdraw_option'), (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); const o = rq && db.prepare('SELECT * FROM rq_option WHERE id = ? AND request_id = ?').get(req.params.oid, rq.id); if (!o) return res.status(404).json({ error: 'not_found' }); if (o.state === 'approved') return res.status(409).json({ error: 'approved' });
      setOptState(o, 'withdrawn', actorOf(req), 'internal', (req.body || {}).note || null); res.json(rqInternal(rqRow(rq.id))); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Release approximate options to the client (state → released) or the firm ones (→ firm_released).
  app.post('/rq/internal/requests/:id/release', ...admin, writeOpLimiter, auditLog('d2d_release'), async (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); const opts = optRows(rq.id); const actor = actorOf(req);
      if (['requested', 'options_offered', 'released'].includes(rq.state)) {
        applyDefaultPricing(rq); const live = optRows(rq.id).filter(o => o.state === 'approximate'); const unpriced = live.filter(o => o.price == null);
        if (!live.length) return res.status(409).json({ error: 'no_options' }); if (unpriced.length) return res.status(409).json({ error: 'unpriced', options: unpriced.map(o => o.id), message: `${unpriced.length} option(s) have no price yet.` });
        db.prepare("UPDATE rq_request SET released_at = datetime('now') WHERE id = ?").run(rq.id); setState(rq, 'released', actor, 'internal', `${live.length} option(s) released`); await notifyClient(rqRow(rq.id), 'released', req);
        return res.json(rqInternal(rqRow(rq.id)));
      }
      if (['picked', 'cost_confirmed'].includes(rq.state)) {
        const picked = opts.filter(o => ['picked', 'confirmed'].includes(o.state)); if (!picked.length) return res.status(409).json({ error: 'nothing_picked' });
        const moved = [];
        for (const o of picked) { const k = costRow(o.id); const m = k.markup_pct != null ? k.markup_pct : markupFor(rq.client_id, o.mode);
          if (o.kind === 'ask' && k.cost_amount != null) { const pp = priceOption(rq, o, k, m); const np = pp ? pp.price : null; if (np != null && o.price != null && Math.abs(np - o.price) >= 0.01) moved.push(`${o.mode} ${[o.carrier, o.service].filter(Boolean).join(' ')}: ${np > o.price ? 'up' : 'down'} ${fmtMoney(Math.abs(np - o.price), o.currency)} — ${k.confirmed_note || 'the carrier\'s confirmed cost moved'}.`); if (np != null) db.prepare("UPDATE rq_option SET price = ?, price_basis = ? WHERE id = ?").run(np, pp.basis, o.id); }
          setOptState({ ...o, state: db.prepare('SELECT state FROM rq_option WHERE id = ?').get(o.id).state }, 'firm', actor, 'internal', null); }
        if ((req.body || {}).valid_until) db.prepare("UPDATE rq_option SET valid_until = ? WHERE request_id = ? AND state = 'firm'").run(String((req.body || {}).valid_until).slice(0, 10), rq.id);
        db.prepare("UPDATE rq_request SET firm_released_at = datetime('now') WHERE id = ?").run(rq.id); setState(rq, 'firm_released', actor, 'internal', `${picked.length} firm option(s) released${moved.length ? ' · price moved' : ''}`);
        await notifyClient(rqRow(rq.id), 'firm', req, { moved }); return res.json(rqInternal(rqRow(rq.id)));
      }
      res.status(409).json({ error: 'not_releasable', state: rq.state }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  // Internal confirms a picked option's cost by hand (partner phoned, or no partner on the option).
  app.post('/rq/internal/requests/:id/options/:oid/confirm', ...admin, writeOpLimiter, auditLog('d2d_confirm_cost'), (req, res) => {
    try { const rq = rqRow(req.params.id, curClient()); const o = rq && db.prepare('SELECT * FROM rq_option WHERE id = ? AND request_id = ?').get(req.params.oid, rq.id); if (!o) return res.status(404).json({ error: 'not_found' }); if (o.state !== 'picked') return res.status(409).json({ error: 'not_picked', state: o.state });
      const b = req.body || {}; const k = costRow(o.id); const cost = b.cost_amount != null ? Number(b.cost_amount) : k.cost_amount; if (cost == null || !isFinite(cost)) return res.status(400).json({ error: 'cost_amount required' });
      db.prepare("INSERT INTO rq_option_cost (option_id, cost_amount, cost_currency, confirmed_cost_amount, confirmed_local_charges, confirmed_at, confirmed_note, partner_name) VALUES (?,?,?,?,?,datetime('now'),?,?) ON CONFLICT(option_id) DO UPDATE SET cost_amount = COALESCE(cost_amount, excluded.cost_amount), confirmed_cost_amount = excluded.confirmed_cost_amount, confirmed_local_charges = excluded.confirmed_local_charges, confirmed_at = excluded.confirmed_at, confirmed_note = excluded.confirmed_note, partner_name = COALESCE(partner_name, excluded.partner_name)")
        .run(o.id, cost, b.cost_currency || k.cost_currency || o.currency, cost, b.local_charges != null ? Number(b.local_charges) : (k.local_charges || 0), b.note || 'confirmed by VelOzity', b.partner_name || null);
      setOptState(o, 'confirmed', actorOf(req), 'internal', 'cost confirmed by hand'); if (!optRows(rq.id).some(x => x.state === 'picked')) setState(rq, 'cost_confirmed', actorOf(req), 'internal', null);
      res.json(rqInternal(rqRow(rq.id))); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.post('/rq/internal/requests/:id/note', ...admin, writeOpLimiter, (req, res) => { try { const rq = rqRow(req.params.id, curClient()); if (!rq) return res.status(404).json({ error: 'not_found' }); db.prepare('UPDATE rq_request SET internal_note = ? WHERE id = ?').run(String((req.body || {}).note || '').slice(0, 2000) || null, rq.id); res.json({ ok: true }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  app.post('/rq/internal/expire', ...admin, async (req, res) => { try { res.json(await expire()); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });

  // ── Partners + settings (Finance › Quotes) ──
  app.get('/rq/partners', ...admin, (req, res) => { try { const c = curClient(); res.json({ client_id: c, partners: db.prepare('SELECT * FROM quote_partner WHERE client_id = ? ORDER BY active DESC, name').all(c), settings: { client_notify_to: setting(c, 'client_notify_to', ''), destination: setting(c, 'destination', 'Sydney · VOZ_KY'), markup: { Air: markupFor(c, 'Air'), Sea: markupFor(c, 'Sea') } }, origins: db.prepare('SELECT * FROM rq_origin WHERE client_id = ? ORDER BY active DESC, sort, name').all(c), supplier_origins: supplierOrigins(c) }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  app.post('/rq/partners', ...admin, writeOpLimiter, auditLog('edit_quote_partner'), (req, res) => {
    try { const c = curClient(); const b = req.body || {}; const email = String(b.email || '').trim(); const name = String(b.name || '').trim(); if (!name || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return res.status(400).json({ error: 'name and a valid email required' });
      const id = b.id ? String(b.id) : newId('qp'); const air = b.air == null ? 1 : (b.air ? 1 : 0), sea = b.sea == null ? 1 : (b.sea ? 1 : 0), active = b.active == null ? 1 : (b.active ? 1 : 0);
      db.prepare("INSERT INTO quote_partner (id, client_id, name, email, air, sea, active, note) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name = excluded.name, email = excluded.email, air = excluded.air, sea = excluded.sea, active = excluded.active, note = excluded.note, updated_at = datetime('now')").run(id, c, name, email, air, sea, active, b.note || null);
      res.json(db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(id)); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.delete('/rq/partners/:id', ...admin, writeOpLimiter, auditLog('delete_quote_partner'), (req, res) => { try { db.prepare("UPDATE quote_partner SET active = 0, updated_at = datetime('now') WHERE id = ? AND client_id = ?").run(req.params.id, curClient()); res.json({ ok: true }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });
  app.put('/rq/settings', ...admin, writeOpLimiter, auditLog('edit_d2d_settings'), (req, res) => {
    try { const c = curClient(); const b = req.body || {}; if (b.client_notify_to != null) setSetting(c, 'client_notify_to', String(b.client_notify_to).trim(), actorOf(req)); if (b.destination != null) setSetting(c, 'destination', String(b.destination).trim(), actorOf(req));
      res.json({ ok: true, settings: { client_notify_to: setting(c, 'client_notify_to', ''), destination: setting(c, 'destination', 'Sydney · VOZ_KY') } }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.post('/rq/origins', ...admin, writeOpLimiter, auditLog('edit_d2d_origin'), (req, res) => {
    try { const c = curClient(); const b = req.body || {}; const name = String(b.name || '').trim(); if (!name) return res.status(400).json({ error: 'name required' }); const id = b.id ? String(b.id) : newId('or');
      db.prepare('INSERT INTO rq_origin (id, client_id, name, country, active, sort) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name = excluded.name, country = excluded.country, active = excluded.active, sort = excluded.sort').run(id, c, name, b.country || null, b.active == null ? 1 : (b.active ? 1 : 0), Number(b.sort) || 0);
      res.json(db.prepare('SELECT * FROM rq_origin WHERE id = ?').get(id)); } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // ══════════════ Partner: magic-link pages (no login) ══════════════
  const jsEmbed = (v) => JSON.stringify(v == null ? null : v).replace(/</g, '\\u003c').replace(/>/g, '\\u003e').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  const page = (title, body) => `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${esc(title)}</title>
    <style>body{margin:0;background:#F5F5F7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#1C1C1E;-webkit-font-smoothing:antialiased}.wrap{max-width:1180px;margin:0 auto;padding:28px 18px 60px}.card{background:#fff;border-radius:14px;padding:22px 24px;margin-bottom:14px;box-shadow:0 1px 2px rgba(0,0,0,.04)}h1{font-size:20px;margin:0 0 4px;letter-spacing:-.01em}.sub{color:#6E6E73;font-size:13px;margin-bottom:14px}.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}.f small{display:block;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#6E6E73;font-weight:600}.f b{display:block;font-size:14px;margin-top:2px}table{width:100%;border-collapse:collapse;font-size:13px}th{text-align:left;font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:#6E6E73;padding:6px 6px;border-bottom:1px solid #E5E5EA;white-space:nowrap}td{padding:6px 6px;border-bottom:1px solid #F0F0F2;vertical-align:middle}input,select{width:100%;min-width:0;box-sizing:border-box;border:1px solid #D2D2D7;border-radius:8px;padding:8px 7px;font:inherit;font-size:12.5px;background:#fff}input[type=number]{text-align:right}input[type=date]{min-width:128px}td{min-width:64px}td:nth-child(2),td:nth-child(3){min-width:110px}.btn{display:inline-block;border:0;border-radius:9px;padding:11px 20px;font:600 14px inherit;cursor:pointer;background:#990033;color:#fff}.btn.sec{background:#fff;color:#1C1C1E;border:1px solid #D2D2D7}.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:14px}.note{font-size:12px;color:#6E6E73;line-height:1.5}.ok{background:#EEF6D6;color:#4A6A00;border-radius:10px;padding:12px 14px;font-weight:600}.err{background:#FBE9EE;color:#990033;border-radius:10px;padding:12px 14px}.pill{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;border-radius:999px;padding:2px 8px;background:#F2F2F0;color:#6E6E73}.pill.sea{background:#E3F2EA;color:#2E7D4F}.pill.air{background:#E6F0FF;color:#2F6FED}@media(max-width:720px){.hide-sm{display:none}}</style></head><body><div class="wrap">
    <div style="font-size:12px;color:#6E6E73;margin-bottom:10px"><b style="color:#1C1C1E">VelOzity</b> · Pinpoint</div>${body}</div></body></html>`;
  const facts = (rq) => { const vol = J(rq.volume_json, {}); const origins = J(rq.origins_json, []); return `<div class="facts">
      <div class="f"><small>Reference</small><b>${esc(rq.ref)}</b></div><div class="f"><small>Period</small><b>${esc(rq.period_label)} <span style="font-weight:400;color:#6E6E73">${esc(rq.period_type)}</span></b></div>
      <div class="f"><small>From</small><b>${esc(origins.join(', ') || '—')}</b></div><div class="f"><small>To</small><b>${esc(rq.destination || 'Sydney')}</b></div>
      ${rq.need_by ? `<div class="f"><small>In Sydney FC by</small><b>${esc(dmy(rq.need_by))}</b></div>` : ''}
      <div class="f"><small>Volume</small><b>${vol.units ? Number(vol.units).toLocaleString() + ' units' : '—'}</b>${[vol.cartons ? vol.cartons + ' ctn' : null, vol.cbm ? vol.cbm + ' CBM' : null, vol.kg ? vol.kg + ' kg' : null].filter(Boolean).join(' · ') ? `<span class="note">${esc([vol.cartons ? vol.cartons + ' ctn' : null, vol.cbm ? vol.cbm + ' CBM' : null, vol.kg ? vol.kg + ' kg' : null].filter(Boolean).join(' · '))}</span>` : ''}</div></div>`; };

  app.get('/rq/partner', (req, res) => {
    const t = db.prepare("SELECT * FROM rq_token WHERE token = ? AND purpose IN ('partner_options','partner_confirm')").get(String(req.query.token || ''));
    if (!t) return res.status(404).send(page('Link not found', `<div class="card"><h1>This link isn't valid.</h1><div class="note">Ask VelOzity for a new one.</div></div>`));
    if (new Date(t.expires_at.replace(' ', 'T') + 'Z') < new Date()) return res.status(410).send(page('Link expired', `<div class="card"><h1>This link has expired.</h1><div class="note">Ask VelOzity for a fresh one.</div></div>`));
    const rq = rqRow(t.request_id); const p = t.partner_id ? db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(t.partner_id) : null; if (!rq) return res.status(404).send(page('Not found', '<div class="card"><h1>Request not found.</h1></div>'));
    const modes = p ? [p.sea ? 'Sea' : null, p.air ? 'Air' : null].filter(Boolean) : ['Sea', 'Air'];
    if (t.purpose === 'partner_confirm') {
      const mine = optRows(rq.id).filter(o => o.state === 'picked' && (costRow(o.id).partner_id === t.partner_id || (!costRow(o.id).partner_id)));
      const done = optRows(rq.id).filter(o => o.state === 'confirmed' && costRow(o.id).partner_id === t.partner_id);
      if (!mine.length) return res.send(page('Nothing to confirm', `<div class="card"><h1>${done.length ? 'Thank you — already confirmed.' : 'Nothing to confirm on this request.'}</h1>${facts(rq)}</div>`));
      const rows = mine.map(o => { const k = costRow(o.id); const c = k.cost_amount != null ? Number(k.cost_amount) : ''; return `<tr data-id="${esc(o.id)}"><td><span class="pill ${o.mode.toLowerCase()}">${esc(o.mode)}</span></td><td>${esc([o.carrier, o.service].filter(Boolean).join(' ') || 'From history')}${o.container_size ? ` · ${esc(o.container_size)}'` : ''}<div class="note">${o.departs ? 'departs ' + esc(dmy(o.departs)) + ' · ' : ''}${o.transit_days != null ? esc(o.transit_days) + ' d transit' : ''}</div></td><td class="hide-sm">${k.cost_amount != null ? esc(fmtMoney(c, k.cost_currency)) + (k.local_charges ? ' + ' + esc(fmtMoney(k.local_charges, k.cost_currency)) + ' local' : '') : '<span class="note">no cost given yet</span>'}</td>
        <td><input type="number" step="0.01" min="0" name="cost" value="${c}" placeholder="freight"></td><td><input type="number" step="0.01" min="0" name="local" value="${k.local_charges || 0}" placeholder="local"></td><td><input name="valid" type="date" value="${esc(o.valid_until || '')}"></td><td><input name="note" placeholder="optional"></td></tr>`; }).join('');
      return res.send(page(`Confirm cost ${rq.ref}`, `<div class="card"><h1>Please confirm your cost for ${esc(rq.ref)}.</h1><div class="sub">The client picked ${mine.length === 1 ? 'this option' : 'these options'}. Confirm the cost as given, or revise it — costs are ex GST in the currency shown.</div>${facts(rq)}</div>
        <form class="card" id="f"><table><tr><th>Mode</th><th>Option</th><th class="hide-sm">Approximate</th><th>Freight cost</th><th>Local charges</th><th>Valid to</th><th>Note</th></tr>${rows}</table>
        <div class="row"><button class="btn" type="submit">Confirm cost</button><span class="note">One click confirms every row above.</span></div><div id="msg" style="margin-top:12px"></div></form>
        <script>const T=${jsEmbed(t.token)};document.getElementById('f').onsubmit=async(e)=>{e.preventDefault();const rows=[...document.querySelectorAll('tr[data-id]')].map(r=>({id:r.dataset.id,cost_amount:r.querySelector('[name=cost]').value,local_charges:r.querySelector('[name=local]').value,valid_until:r.querySelector('[name=valid]').value,note:r.querySelector('[name=note]').value}));const m=document.getElementById('msg');m.innerHTML='';try{const x=await fetch('/rq/partner/confirm',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({token:T,options:rows})});const d=await x.json();if(!x.ok)throw new Error(d.error||x.status);m.innerHTML='<div class="ok">Thank you — cost confirmed. VelOzity will come back to you with the booking.</div>';document.querySelector('.btn').disabled=true;}catch(err){m.innerHTML='<div class="err">'+(err.message||err)+'</div>';}};</script>`));
    }
    // Options form
    const existing = optRows(rq.id).filter(o => costRow(o.id).partner_id === t.partner_id && !['withdrawn', 'expired'].includes(o.state));
    const seaRow = (o, k) => `<tr data-mode="Sea" ${o ? `data-id="${esc(o.id)}"` : ''}><td><span class="pill sea">Sea</span></td><td><input name="carrier" placeholder="Carrier" value="${esc(o ? o.carrier : '')}"></td><td><input name="service" placeholder="Service / loop" value="${esc(o ? o.service : '')}"></td><td><select name="container">${CONTAINER.map(c => `<option ${o && o.container_size === c ? 'selected' : (!o && c === '40' ? 'selected' : '')}>${c}</option>`).join('')}</select></td><td><input name="departs" type="date" value="${esc(o ? o.departs : '')}"></td><td><input name="cutoff" type="date" value="${esc(o ? o.cutoff : '')}"></td><td><input name="transit" type="number" min="1" step="1" placeholder="days" value="${o && o.transit_days != null ? o.transit_days : ''}"></td><td><input name="cost" type="number" step="0.01" min="0" placeholder="per container" value="${k && k.cost_amount != null ? k.cost_amount : ''}"></td><td><input name="local" type="number" step="0.01" min="0" placeholder="0" value="${k && k.local_charges != null ? k.local_charges : 0}"></td><td><select name="cur">${['USD', 'AUD'].map(c => `<option ${(k && k.cost_currency === c) || (!k && c === 'USD') ? 'selected' : ''}>${c}</option>`).join('')}</select></td><td><input name="valid" type="date" value="${esc(o ? o.valid_until : '')}" required></td><td><button type="button" class="btn sec" data-rm style="padding:6px 10px">×</button></td></tr>`;
    const airRow = (o, k) => `<tr data-mode="Air" ${o ? `data-id="${esc(o.id)}"` : ''}><td><span class="pill air">Air</span></td><td><input name="carrier" placeholder="Airline / courier" value="${esc(o ? o.carrier : '')}"></td><td><input name="service" placeholder="Service (deferred, express…)" value="${esc(o ? o.service : '')}"></td><td><span class="note">—</span></td><td><input name="departs" type="date" value="${esc(o ? o.departs : '')}"></td><td><input name="cutoff" type="date" value="${esc(o ? o.cutoff : '')}"></td><td><input name="transit" type="number" min="1" step="1" placeholder="days" value="${o && o.transit_days != null ? o.transit_days : ''}"></td><td><input name="cost" type="number" step="0.01" min="0" placeholder="per kg" value="${k && k.cost_amount != null ? k.cost_amount : ''}"></td><td><input name="local" type="number" step="0.01" min="0" placeholder="0" value="${k && k.local_charges != null ? k.local_charges : 0}"></td><td><select name="cur">${['USD', 'AUD'].map(c => `<option ${(k && k.cost_currency === c) || (!k && c === 'USD') ? 'selected' : ''}>${c}</option>`).join('')}</select></td><td><input name="valid" type="date" value="${esc(o ? o.valid_until : '')}" required></td><td><button type="button" class="btn sec" data-rm style="padding:6px 10px">×</button></td></tr>`;
    const rows = existing.map(o => o.mode === 'Sea' ? seaRow(o, costRow(o.id)) : airRow(o, costRow(o.id))).join('');
    res.send(page(`Options ${rq.ref}`, `<div class="card"><h1>Options for ${esc(rq.period_label)}, ${esc(J(rq.origins_json, []).join(' · ') || 'origin')} → ${esc(rq.destination || 'Sydney')}</h1><div class="sub">One row per option. Costs are your all-inclusive cost to VelOzity, ex GST — freight per container for sea, per chargeable kg for air, plus local/destination charges as one figure. Every row needs a valid-to date.</div>${facts(rq)}${rq.client_note ? `<div class="note" style="margin-top:10px">Note from the client: ${esc(rq.client_note)}</div>` : ''}</div>
      <form class="card" id="f"><div style="overflow:auto"><table id="t"><tr><th>Mode</th><th>Carrier</th><th>Service</th><th>Container</th><th>Departs</th><th>Cut-off</th><th>Transit</th><th>Freight cost</th><th>Local charges</th><th>Cur.</th><th>Valid to</th><th></th></tr>${rows}</table></div>
      <div class="row">${modes.includes('Sea') ? '<button type="button" class="btn sec" id="addSea">+ Sea option</button>' : ''}${modes.includes('Air') ? '<button type="button" class="btn sec" id="addAir">+ Air option</button>' : ''}<span style="flex:1"></span><button class="btn" type="submit">Send options to VelOzity</button></div>
      <div class="note" style="margin-top:10px">You can come back to this link and revise until VelOzity releases the options. Approximate costs are fine — you'll be asked to confirm the one the client picks.</div><div id="msg" style="margin-top:12px"></div></form>
      <script>const T=${jsEmbed(t.token)},SEA=${jsEmbed(seaRow(null, null))},AIR=${jsEmbed(airRow(null, null))};const tb=document.getElementById('t');const add=h=>{tb.insertAdjacentHTML('beforeend',h);};const s=document.getElementById('addSea'),a=document.getElementById('addAir');if(s)s.onclick=()=>add(SEA);if(a)a.onclick=()=>add(AIR);tb.addEventListener('click',e=>{if(e.target.dataset.rm!=null)e.target.closest('tr').remove();});if(!tb.querySelectorAll('tr[data-mode]').length){if(s)add(SEA);else if(a)add(AIR);}
      document.getElementById('f').onsubmit=async(e)=>{e.preventDefault();const rows=[...tb.querySelectorAll('tr[data-mode]')].map(r=>{const g=n=>{const x=r.querySelector('[name='+n+']');return x?x.value:null;};return{id:r.dataset.id||null,mode:r.dataset.mode,carrier:g('carrier'),service:g('service'),container_size:g('container'),departs:g('departs'),cutoff:g('cutoff'),transit_days:g('transit'),cost_amount:g('cost'),local_charges:g('local'),cost_currency:g('cur'),valid_until:g('valid')};});const m=document.getElementById('msg');m.innerHTML='';try{const x=await fetch('/rq/partner/options',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({token:T,options:rows})});const d=await x.json();if(!x.ok)throw new Error(d.error||x.status);m.innerHTML='<div class="ok">Thank you — '+d.saved+' option'+(d.saved===1?'':'s')+' sent to VelOzity.</div>';}catch(err){m.innerHTML='<div class="err">'+(err.message||err)+'</div>';}};</script>`));
  });
  app.post('/rq/partner/options', (req, res) => {
    try {
      const t = tokenRow((req.body || {}).token, 'partner_options'); if (!t) return res.status(403).json({ error: 'invalid or expired link' });
      const rq = rqRow(t.request_id); if (!rq) return res.status(404).json({ error: 'not_found' }); if (!['requested', 'options_offered'].includes(rq.state)) return res.status(409).json({ error: 'This request is past the options stage — VelOzity has released it.' });
      const p = t.partner_id ? db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(t.partner_id) : null; const rows = Array.isArray((req.body || {}).options) ? (req.body || {}).options : []; if (!rows.length) return res.status(400).json({ error: 'Add at least one option.' });
      const mine = new Set(optRows(rq.id).filter(o => costRow(o.id).partner_id === t.partner_id && ['approximate'].includes(o.state)).map(o => o.id)); const keep = new Set(); let saved = 0;
      for (const r of rows) {
        const mode = modeOf(r.mode); if (p && ((mode === 'Sea' && !p.sea) || (mode === 'Air' && !p.air))) continue;
        const cost = Number(r.cost_amount); const valid = String(r.valid_until || '').slice(0, 10); if (!isFinite(cost) || cost <= 0) return res.status(400).json({ error: `Each option needs a freight cost (${mode}).` }); if (!/^\d{4}-\d{2}-\d{2}$/.test(valid)) return res.status(400).json({ error: 'Each option needs a valid-to date.' }); if (valid < today()) return res.status(400).json({ error: 'Valid-to must be today or later.' });
        const transit = r.transit_days === '' || r.transit_days == null ? null : Number(r.transit_days); const id = r.id && mine.has(String(r.id)) ? String(r.id) : newId('op');
        if (mine.has(id)) db.prepare("UPDATE rq_option SET carrier = ?, service = ?, container_size = ?, departs = ?, cutoff = ?, transit_days = ?, valid_until = ?, currency = ?, updated_at = datetime('now') WHERE id = ?").run(r.carrier || null, r.service || null, mode === 'Sea' ? (CONTAINER.includes(String(r.container_size)) ? String(r.container_size) : '40') : null, r.departs || null, r.cutoff || null, transit, valid, r.cost_currency || 'USD', id);
        else db.prepare('INSERT INTO rq_option (id, request_id, client_id, mode, kind, carrier, service, origin, container_size, departs, cutoff, transit_days, valid_until, currency, price_basis, state, sort) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(id, rq.id, rq.client_id, mode, 'ask', r.carrier || null, r.service || null, J(rq.origins_json, [])[0] || null, mode === 'Sea' ? (CONTAINER.includes(String(r.container_size)) ? String(r.container_size) : '40') : null, r.departs || null, r.cutoff || null, transit, valid, r.cost_currency || 'USD', mode === 'Sea' ? 'per_container' : 'per_kg', 'approximate', optRows(rq.id).length);
        db.prepare('INSERT INTO rq_option_cost (option_id, partner_id, partner_name, cost_amount, local_charges, cost_currency) VALUES (?,?,?,?,?,?) ON CONFLICT(option_id) DO UPDATE SET cost_amount = excluded.cost_amount, local_charges = excluded.local_charges, cost_currency = excluded.cost_currency').run(id, p ? p.id : null, p ? p.name : null, cost, Number(r.local_charges) || 0, r.cost_currency || 'USD');
        keep.add(id); saved++;
      }
      for (const id of mine) if (!keep.has(id)) setOptState(db.prepare('SELECT * FROM rq_option WHERE id = ?').get(id), 'withdrawn', p ? p.name : 'partner', 'partner', 'removed by partner');
      if (!saved) return res.status(400).json({ error: 'No option could be saved.' });
      db.prepare("UPDATE rq_token SET used_at = datetime('now') WHERE token = ?").run(t.token); event(rq.id, null, null, null, p ? p.name : 'partner', 'partner', `${saved} option(s) offered`);
      if (rq.state === 'requested') setState(rq, 'options_offered', p ? p.name : 'partner', 'partner', null); applyDefaultPricing(rq);
      notifyInternal(rq, 'Options offered — review and release', [`${rq.ref} · ${p ? p.name : 'a partner'} offered ${saved} option(s)`, 'Prices are pre-filled from the rate card; adjust and release in Quote Review.']).catch(() => {});
      res.json({ ok: true, saved });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });
  app.post('/rq/partner/confirm', async (req, res) => {
    try {
      const t = tokenRow((req.body || {}).token, 'partner_confirm'); if (!t) return res.status(403).json({ error: 'invalid or expired link' });
      const rq = rqRow(t.request_id); if (!rq) return res.status(404).json({ error: 'not_found' }); const p = t.partner_id ? db.prepare('SELECT * FROM quote_partner WHERE id = ?').get(t.partner_id) : null;
      const rows = Array.isArray((req.body || {}).options) ? (req.body || {}).options : []; let n = 0;
      for (const r of rows) { const o = db.prepare('SELECT * FROM rq_option WHERE id = ? AND request_id = ?').get(String(r.id || ''), rq.id); if (!o || o.state !== 'picked') continue; const k = costRow(o.id); if (k.partner_id && k.partner_id !== t.partner_id) continue;
        const cost = Number(r.cost_amount); if (!isFinite(cost) || cost <= 0) return res.status(400).json({ error: 'Each option needs a confirmed freight cost.' });
        db.prepare("INSERT INTO rq_option_cost (option_id, partner_id, partner_name, cost_amount, cost_currency, confirmed_cost_amount, confirmed_local_charges, confirmed_at, confirmed_note) VALUES (?,?,?,?,?,?,?,datetime('now'),?) ON CONFLICT(option_id) DO UPDATE SET partner_id = COALESCE(partner_id, excluded.partner_id), partner_name = COALESCE(partner_name, excluded.partner_name), cost_amount = COALESCE(cost_amount, excluded.cost_amount), confirmed_cost_amount = excluded.confirmed_cost_amount, confirmed_local_charges = excluded.confirmed_local_charges, confirmed_at = excluded.confirmed_at, confirmed_note = excluded.confirmed_note").run(o.id, p ? p.id : null, p ? p.name : null, cost, k.cost_currency || o.currency, cost, Number(r.local_charges) || 0, r.note || null);
        if (r.valid_until && /^\d{4}-\d{2}-\d{2}$/.test(String(r.valid_until)) && String(r.valid_until) >= today()) db.prepare('UPDATE rq_option SET valid_until = ? WHERE id = ?').run(String(r.valid_until).slice(0, 10), o.id);
        setOptState(o, 'confirmed', p ? p.name : 'partner', 'partner', `confirmed ${fmtMoney(cost, k.cost_currency || o.currency)}${Number(r.local_charges) ? ' + ' + fmtMoney(r.local_charges, k.cost_currency || o.currency) + ' local' : ''}`); n++; }
      if (!n) return res.status(409).json({ error: 'Nothing to confirm.' });
      db.prepare("UPDATE rq_token SET used_at = datetime('now') WHERE token = ?").run(t.token);
      if (!optRows(rq.id).some(o => o.state === 'picked')) setState(rq, 'cost_confirmed', p ? p.name : 'partner', 'partner', 'every picked option confirmed');
      notifyInternal(rqRow(rq.id), 'Cost confirmed — release the firm price', [`${rq.ref} · ${p ? p.name : 'partner'} confirmed ${n} option(s)`]).catch(() => {});
      res.json({ ok: true, confirmed: n });
    } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  });

  // Approved sea rates for the signals price line and the simulator.
  app.get('/rq/sea-rates', ...guard, (req, res) => { try { res.json({ rates: db.prepare('SELECT week_start, origin, carrier, service, container_size, price, currency, transit_days, departs, approved_at FROM rq_sea_rate WHERE client_id = ? ORDER BY week_start DESC LIMIT 60').all(curClient()) }); } catch (e) { res.status(500).json({ error: String(e.message || e) }); } });

  return { rqPublic, rqInternal, prefill, expire, historyOptions, _internal: { sendPartnerLinks, writeApproved } };
};
