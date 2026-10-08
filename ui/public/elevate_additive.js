/* ── VelOzity Pinpoint — Daily client tracker (elevate_additive.js) ──
   The working page behind THE ICONIC's daily VOZ FF Tracker. Route #elevate, section
   #page-elevate, reached from Transit Movements. Its own full page, not a panel.

   What the page is for. Every day at 23:00 the client gets the tracker and a sheet of what
   changed. The context on each change is written by the server from provenance — a carrier
   revision, a confirmed arrival, a dock booking — so nobody has to write it. What a person
   adds here is the part the system cannot know: why. "Vessel held at Yantian, congestion."
   That note binds to the change itself (the field, the old value, the new one), so if the
   date moves again tomorrow it is a new fact and the note does not travel with it.

   One button sends: the workbook by email, the CSV to their SFTP, and records what went so
   tomorrow's diff is against what the client actually saw. If nobody presses it, 23:00 does.

   Nothing here lives outside the page root. No overlays on <body>, nothing to leak onto
   another page when this one is hidden. */
(function () {
  'use strict';
  if (window.__ELEVATE_LOADED__) return;
  window.__ELEVATE_LOADED__ = true;
  const VERSION = '4';

  const INK = '#121212', MUTED = '#5F5F5F', LINE = '#E3E3E0', SOFT = '#EFEFEC';
  const GREEN = '#C7EA46', AMBER = '#F5BD25', RED = '#990033', BLUE = '#1E9BD7', PURPLE = '#7C5CBF', GREY = '#8A8A8A';
  // Status, never yellow: yellow reads as a warning, and a booked dock is good news.
  const STATUS_DOT = { 'Shipped': GREY, 'Landed On Route': INK, 'Delivery Booked': PURPLE, 'Delivered': GREEN };
  const REFRESH_MS = 3 * 60 * 1000;

  // ── API (same shape as Transit Movements) ──
  const apiBase = () => ((document.querySelector('meta[name="api-base"]') || {}).content || '').replace(/\/+$/, '');
  async function token() {
    try { return (window.Clerk && window.Clerk.session) ? await window.Clerk.session.getToken() : null; } catch (_) { return null; }
  }
  async function req(method, path, body, opts) {
    const t = await token();
    const h = {};
    if (body) h['Content-Type'] = 'application/json';
    if (t) h.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { method, headers: h, body: body ? JSON.stringify(body) : undefined });
    if (opts && opts.raw) {
      if (!r.ok) { let d = null; try { d = await r.json(); } catch (_) {} throw new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); }
      return r;
    }
    let d = null; try { d = await r.json(); } catch (_) { d = null; }
    if (!r.ok) throw new Error((d && (d.message || d.error)) || ('HTTP ' + r.status));
    return d;
  }
  async function download(path, fallbackName) {
    const r = await req('GET', path, null, { raw: true });
    const blob = await r.blob();
    const cd = r.headers.get('content-disposition') || '';
    const m = cd.match(/filename="?([^";]+)"?/);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = (m && m[1]) || fallbackName;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }

  // ── Helpers ──
  const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const isDate = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));
  const day = (v) => isDate(v) ? new Date(v + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }) : (v == null ? '' : String(v));
  const longDay = (v) => isDate(v) ? new Date(v + 'T00:00:00Z').toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' }) : '';
  const when = (iso) => { try { return new Date(iso).toLocaleString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'Australia/Sydney' }); } catch (_) { return String(iso || ''); } };
  const val = (c, k) => isDate(c[k]) ? day(c[k]) : String(c[k] == null ? '' : c[k]).trim();
  const STATUS_INK = { 'Shipped': MUTED, 'Landed On Route': INK, 'Delivery Booked ': INK, 'Delivered': '#5F6B0D' };

  // ── State ──
  const S = { root: null, data: null, loading: false, error: null, loadedAt: 0, timer: null,
              confirm: false, sending: false, result: null, saving: {},
              q: '', filter: 'all', sort: { key: 'ex_factory', dir: 1 }, pulsed: new Set(), posOpen: new Set() };

  // ── Styles ──
  function styles() {
    if (document.getElementById('el-css')) return;
    const st = document.createElement('style');
    st.id = 'el-css';
    st.textContent = `
.el{font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK};box-sizing:border-box;padding:24px 28px 48px;max-width:1600px;margin:0 auto;display:flex;flex-direction:column;gap:20px;-webkit-font-smoothing:antialiased}
.el *{box-sizing:border-box}
.el button{font-family:inherit;cursor:pointer;color:inherit}
.el button:disabled{opacity:.45;cursor:default}
.el input,.el textarea{font-family:inherit;font-size:13px;color:${INK}}
.el-head{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap}
.el-head h1{margin:0;font-size:34px;font-weight:600;letter-spacing:-.02em;line-height:1}
.el-kicker{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:${MUTED};font-weight:500}
.el-note{font-size:13px;color:${MUTED}}
.el-back{background:none;border:0;padding:0;font-size:12px;color:${MUTED};display:inline-flex;align-items:center;gap:4px}
.el-back:hover{color:${INK}}
.el-btn{height:38px;padding:0 14px;border-radius:8px;border:1px solid #D6D6D2;background:#fff;font-size:12.5px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.el .el-btn-p{height:42px;padding:0 18px;border-radius:8px;border:0;background:${INK};color:#fff;font-size:13px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.el-btn-s{height:42px;padding:0 16px;border-radius:8px;background:#fff;border:1px solid #D6D6D2;font-size:13px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.el-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}
.el-stat{background:#fff;border:1px solid ${LINE};border-radius:12px;padding:14px 16px}
.el-stat .k{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};font-weight:500}
.el-stat .v{font-size:26px;font-weight:600;letter-spacing:-.02em;margin-top:4px;line-height:1}
.el-stat .s{font-size:12px;color:${MUTED};margin-top:4px}
.el-card{background:#fff;border:1px solid ${LINE};border-radius:12px;overflow:hidden}
.el-cardh{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:14px 18px;border-bottom:1px solid ${LINE}}
.el-h2{margin:0;font-size:15px;font-weight:600}
.el-tools{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:12px 16px;border-bottom:1px solid ${LINE}}
.el-search{flex:1 1 260px;max-width:420px;height:36px;border:1px solid #D6D6D2;border-radius:8px;padding:0 12px 0 34px;font-size:13px;outline:none;background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%235F5F5F' stroke-width='2'%3E%3Ccircle cx='11' cy='11' r='7'/%3E%3Cpath d='m20 20-3.5-3.5'/%3E%3C/svg%3E") 11px center no-repeat}
.el-search:focus{border-color:#8A8A86}
.el-chips{display:flex;gap:6px}
.el-chipb{height:30px;padding:0 12px;border-radius:15px;border:1px solid #D6D6D2;background:#fff;font-size:12px;font-weight:500;display:inline-flex;align-items:center;gap:6px}
.el-chipb[aria-pressed="true"]{background:${INK};color:#fff;border-color:${INK}}
.el-chipb .n{font-size:11px;opacity:.7}
.el-scroll{overflow:auto;max-height:70vh;position:relative}
.el-table{border-collapse:separate;border-spacing:0;font-size:12.5px;min-width:100%}
.el-table th{position:sticky;top:0;z-index:3;background:#fff;text-align:left;font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:${MUTED};font-weight:500;padding:9px 10px;border-bottom:1px solid ${LINE};white-space:nowrap;cursor:pointer;user-select:none}
.el-table th:hover{color:${INK}}
.el-table th .arr{margin-left:4px;font-size:9px}
.el-table th.num,.el-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.el-table td{padding:8px 10px;border-bottom:1px solid ${SOFT};vertical-align:top;white-space:nowrap;background:#fff}
.el-table tr:hover td{background:#FAFAF8}
.el-table td.pos{font-size:11.5px;color:${MUTED};line-height:1.35}
.el-more{margin-left:6px;height:18px;padding:0 7px;border-radius:9px;border:1px solid #D6D6D2;background:#fff;font-size:10.5px;font-weight:500;color:${MUTED};vertical-align:1px}
.el-more:hover{color:${INK};border-color:#B8B8B4}
.el-table td.wrap{white-space:normal;min-width:180px;max-width:260px}
.el-table td.dim{color:#C9C9C5}
.el-table .fz{position:sticky;z-index:2;background:#fff}
.el-table th.fz{z-index:4}
.el-table .fz-last{box-shadow:4px 0 6px -4px rgba(0,0,0,.12)}
.el-table tr:hover td.fz{background:#FAFAF8}
.el-d{position:relative;padding-left:16px !important}
.el-d::before{content:'';position:absolute;left:5px;top:7px;bottom:7px;width:3px;border-radius:2px;background:transparent}
.el-d.fc{color:#2E6C8E}
.el-d.fc::before{background:${BLUE}}
.el-d.late{color:${RED}}
.el-d.late::before{background:${RED}}
.el-d.bad{color:${RED};text-decoration:underline dotted;text-underline-offset:3px}
.el-d.bad::before{background:${RED}}
.el-chg{position:relative}
.el-chg::after{content:'';position:absolute;right:5px;top:6px;width:7px;height:7px;border-radius:50%;background:${RED}}
.el-chg.pulse{animation:elPulse 2.2s ease-out 1}
@keyframes elPulse{0%{box-shadow:inset 0 0 0 999px rgba(153,0,51,.16)}60%{box-shadow:inset 0 0 0 999px rgba(153,0,51,.05)}100%{box-shadow:none}}
.el-leg{display:inline-flex;align-items:center;gap:6px}
.el-leg .bar{display:inline-block;width:3px;height:12px;border-radius:2px}
.el-leg .dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:${RED}}
.el-notes{white-space:normal !important;min-width:320px;max-width:360px;position:sticky;right:0;z-index:2;background:#fff;box-shadow:-4px 0 6px -4px rgba(0,0,0,.12)}
.el-table th.el-notes{z-index:4}
.el-table tr:hover td.el-notes{background:#FAFAF8}
.el-ctx{font-size:12px;color:#48484A;line-height:1.4;margin-bottom:3px}
.el-ctx b{font-weight:600;color:${INK}}
.el-noteI{width:100%;border:1px solid transparent;background:${SOFT};border-radius:7px;padding:5px 9px;font-size:12.5px;line-height:1.35;resize:vertical;min-height:28px;outline:none;display:block}
.el-noteI:focus{border-color:#B8B8B4;background:#fff}
.el-noteI::placeholder{color:#9C9C98}
.el-saved{font-size:11px;color:${MUTED};margin:2px 0 4px;min-height:0}
.el-saved:empty{display:none}
.el-notes>div+div{margin-top:8px}
.el-flag{display:inline-flex;align-items:center;gap:6px;font-size:11px;font-weight:500;color:#7A5200;background:#FFF4D6;padding:2px 8px;border-radius:10px;white-space:nowrap}
.el-empty{padding:28px 18px;font-size:13.5px;color:${MUTED};text-align:center}
.el-confirm{background:#FAFAF8;border:1px solid ${LINE};border-radius:12px;padding:18px 20px;display:flex;flex-direction:column;gap:12px}
.el-confirm .row{display:flex;gap:18px;flex-wrap:wrap;font-size:13px}
.el-confirm .row b{font-weight:600}
.el-result{font-size:13px;padding:12px 16px;border-radius:10px;background:#F0F7D9}
.el-result.err{background:#FDECEF;color:${RED}}
.el-legend{display:flex;gap:16px;align-items:center;font-size:11.5px;color:${MUTED};margin-left:auto}
.el-legend i{display:inline-block;width:14px;height:10px;border-radius:3px;vertical-align:-1px;margin-right:5px}
.el-pill{display:inline-flex;align-items:center;gap:6px;font-size:12px}
.el-dot{width:8px;height:8px;border-radius:50%;display:inline-block;flex:none}
@media (max-width:900px){.el{padding:16px 12px 40px}.el-head h1{font-size:28px}}
`;
    document.head.appendChild(st);
  }

  // ── Render ──
  function head(d) {
    const st = d.stats, ls = d.last_sent, sch = d.schedule;
    const lastLine = ls
      ? `Last sent ${esc(when(ls.at))} for ${esc(longDay(ls.for_date))} · ${ls.rows} lines · ${ls.changes} change${ls.changes === 1 ? '' : 's'}${ls.sftp ? ' · published to SFTP' : ''}`
      : 'Not sent yet. The first send sets the baseline; nothing is "changed" until there is something to compare against.';
    const due = sch.due_sent
      ? `Today's send is done. Next at ${sch.hour}:00 tomorrow.`
      : `Auto-send at ${sch.hour}:00 ${esc(sch.zone.replace('Australia/', ''))} unless someone sends first.`;
    return `
<div class="el-head">
  <div style="display:flex;flex-direction:column;gap:6px">
    <button class="el-back" data-act="back">← Transit Movements</button>
    <div class="el-kicker">Pinpoint · ICONIC · VOZ FF Tracker</div>
    <h1>Daily client tracker</h1>
    <div class="el-note">${lastLine}</div>
    <div class="el-note">${due}</div>
  </div>
  <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <button class="el-btn" data-act="refresh" ${S.loading ? 'disabled' : ''}>${S.loading ? 'Refreshing…' : 'Refresh'}</button>
    <button class="el-btn" data-act="download">Download workbook</button>
    <button class="el-btn-p" data-act="send-open" ${S.sending ? 'disabled' : ''}>
      Email &amp; publish${st.changes ? ` · ${st.changes} change${st.changes === 1 ? '' : 's'}` : ''}</button>
  </div>
</div>`;
  }

  function statsRow(d) {
    const s = d.stats;
    const tile = (k, v, sub) => `<div class="el-stat"><div class="k">${k}</div><div class="v">${v}</div>${sub ? `<div class="s">${sub}</div>` : ''}</div>`;
    return `<div class="el-stats">
      ${tile('Lines', s.rows, `${s.delivered} delivered`)}
      ${tile('Open', s.open, 'not yet delivered')}
      ${tile('Shipped', s.shipped, 'on the water or in the air')}
      ${tile('Landed', s.landed, 'arrived, awaiting delivery')}
      ${tile('Dock booked', s.booked, 'slot confirmed at the FC')}
      ${tile('Changes', s.changes, s.anomalies ? `<span style="color:#7A5200">${s.anomalies} after delivery</span>` : 'since the last send')}
    </div>`;
  }

  function confirmPanel(d) {
    if (!S.confirm) return '';
    const r = d.recipients || {}, s = d.stats;
    const noted = d.changes.filter(c => c.note).length;
    const ok = (r.to || []).length && r.from;
    return `
<div class="el-confirm">
  <div style="font-size:14px;font-weight:600">Send now?</div>
  <div class="row">
    <span>Email to <b>${(r.to || []).length ? esc(r.to.join(', ')) : '<span style="color:' + RED + '">nobody — ELEVATE_TO is not set</span>'}</b></span>
    ${(r.cc || []).length ? `<span>cc <b>${esc(r.cc.join(', '))}</b></span>` : ''}
    <span>SFTP <b>${d.sftp_enabled ? 'CSV will be published' : 'off'}</b></span>
  </div>
  <div class="row">
    <span><b>${s.rows}</b> lines</span>
    <span><b>${s.changes}</b> change${s.changes === 1 ? '' : 's'}${noted ? ` · ${noted} with a note` : ''}</span>
    ${s.anomalies ? `<span class="el-flag">${s.anomalies} change${s.anomalies === 1 ? '' : 's'} on delivered lines — flagged for their review</span>` : ''}
  </div>
  <div style="display:flex;gap:10px;align-items:center">
    <button class="el-btn-p" data-act="send-go" ${(!ok || S.sending) ? 'disabled' : ''}>${S.sending ? 'Sending…' : 'Send'}</button>
    <button class="el-btn-s" data-act="send-cancel" ${S.sending ? 'disabled' : ''}>Cancel</button>
    <span class="el-note">This records what was sent. Tomorrow's changes are measured against it.</span>
  </div>
</div>`;
  }

  function resultLine() {
    if (!S.result) return '';
    const r = S.result;
    if (r.error && !r.sent) return `<div class="el-result err">Not sent — ${esc(r.error)}</div>`;
    if (!r.sent) return `<div class="el-result err">Not sent — ${esc(r.message || r.reason || 'unknown reason')}</div>`;
    return `<div class="el-result">Sent ${esc(when(new Date().toISOString()))} to ${esc((r.to || []).join(', '))}${r.sftp_path ? ` · published ${esc(r.sftp_path)}` : ''}${r.error ? ` · <span style="color:${RED}">${esc(r.error)}</span>` : ''}. ${r.stats ? `${r.stats.changes} change${r.stats.changes === 1 ? '' : 's'} went with it.` : ''}</div>`;
  }

  // ── The one table ──
  // Frozen identity on the left, THE ICONIC's 28 columns in their order, notes on the right.
  const FROZEN = [
    { key: 'ex_week',   head: 'Week',    w: 54 },
    { key: 'zendesk',   head: 'Zendesk', w: 74 },
    { key: 'transport', head: 'Mode',    w: 52 },
    { key: 'status',    head: 'Status',  w: 132 },
    { key: 'vendor',    head: 'Vendor',  w: 210 },
  ];
  const SHEET = [
    ['vessel', 'Vessel'], ['ccl', 'CCL'], ['sca', 'SCA'], ['dor', 'DOR'], ['shipment', 'Shipment #'],
    ['hbl', 'HBL'], ['mbl', 'MBL / CNTR'], ['pos', 'PO #'], ['ex_factory', 'Ex Factory'],
    ['handover', 'Handover'], ['etd', 'ETD'], ['eta', 'ETA'], ['delivery', 'Delivery to FC'],
    ['dw_week', 'Expected DW'], ['lt_d2d', 'Door to door'], ['lt_transit', 'Transit days'],
    ['lt_port_fc', 'Port to FC'], ['lt_xf_fc', 'XF > FC'], ['weight_kg', 'Weight kg'],
    ['cartons', 'CTNS'], ['units', 'Units'], ['origin_country', 'Origin country'],
    ['origin_city', 'Origin city'], ['rolling_dw', 'Rolling DW in BC'],
  ];
  const DATE_K = new Set(['ex_factory', 'handover', 'etd', 'eta', 'delivery']);
  const NUM_K = new Set(['lt_d2d', 'lt_transit', 'lt_port_fc', 'lt_xf_fc', 'weight_kg', 'cartons', 'units']);
  const EMPTY_K = new Set(['ccl', 'sca', 'dor', 'rolling_dw']);

  function changesByRow(d) {
    const m = new Map();
    for (const c of (d.changes || [])) {
      if (!m.has(c.row_key)) m.set(c.row_key, []);
      m.get(c.row_key).push(c);
    }
    return m;
  }

  function visibleRows(d, byRow) {
    const q = S.q.trim().toLowerCase();
    let rows = (d.rows || []).slice();
    if (S.filter === 'changed') rows = rows.filter(r => byRow.has(r.zendesk + '|' + r.transport));
    else if (S.filter === 'open') rows = rows.filter(r => r.status !== 'Delivered');
    else if (S.filter === 'delivered') rows = rows.filter(r => r.status === 'Delivered');
    else if (S.filter === 'late') rows = rows.filter(r => r.late_days != null && r.late_days >= 2 && r.status !== 'Delivered');
    else if (S.filter === 'attention') rows = rows.filter(r => r.eta_impossible);
    if (q) rows = rows.filter(r => [r.zendesk, r.pos, r.vendor, r.mbl, r.vessel, r.shipment, r.hbl, r.status]
      .join(' ').toLowerCase().includes(q));
    const { key, dir } = S.sort;
    const num = NUM_K.has(key);
    rows.sort((a, b) => {
      let x = a[key], y = b[key];
      if (num) { x = x === '' ? -Infinity : Number(x); y = y === '' ? -Infinity : Number(y); }
      else { x = String(x == null ? '' : x); y = String(y == null ? '' : y); }
      const c = x < y ? -1 : x > y ? 1 : 0;
      return (c || String(a.ex_factory).localeCompare(String(b.ex_factory)) || String(a.zendesk).localeCompare(String(b.zendesk))) * (c ? dir : 1);
    });
    return rows;
  }

  function cell(r, key, chg) {
    const v = r[key];
    const classes = [];
    let inner;
    let title = '';
    if (key === 'status') {
      const t = String(v).trim();
      inner = `<span class="el-pill"><span class="el-dot" style="background:${STATUS_DOT[t] || '#C9C9C5'}"></span>${esc(t)}</span>`;
    } else if (key === 'ex_week') {
      inner = `<b>${esc(v)}</b>`;
    } else if (key === 'zendesk') {
      inner = `<b>${esc(v)}</b>`;
    } else if (key === 'vendor') {
      classes.push('wrap'); inner = esc(v);
    } else if (key === 'pos') {
      // One PO per line keeps the rows even; the rest sit behind a count until asked for.
      classes.push('pos');
      const list = String(v || '').split('\n').map(x => x.trim()).filter(Boolean);
      const rk = r.zendesk + '|' + r.transport;
      if (list.length <= 1) inner = esc(list[0] || '');
      else if (S.posOpen.has(rk)) inner = `${list.map(esc).join('<br>')}<button class="el-more" data-act="pos" data-v="${esc(rk)}">less</button>`;
      else inner = `${esc(list[0])}<button class="el-more" data-act="pos" data-v="${esc(rk)}" title="${esc(list.slice(1).join(', '))}">+${list.length - 1}</button>`;
    } else if (DATE_K.has(key)) {
      classes.push('el-d');
      inner = v ? esc(day(v)) : '<span style="color:#C9C9C5">—</span>';
      const p = r.prov && r.prov[key];
      const forecast = v && p && p !== 'actual';
      const late = r.late_days != null && r.late_days >= 2 && (key === 'delivery' || key === 'eta') && String(r.status).trim() !== 'Delivered';
      if (key === 'eta' && r.eta_impossible) {
        classes.push('bad');
        title = `Arrival not confirmed. ${esc(r.eta_basis || '')} gives ${esc(day(v))}, after the ${r.prov.delivery === 'actual' ? 'delivery' : 'booked dock'} on ${esc(day(r.delivery))}. Confirm the arrival on the movement; the file sends this ETA blank until then.`;
      } else if (late) {
        classes.push('late');
        title = `${r.late_days} days later than first promised.${forecast ? ' Forecast — ' + esc(key === 'eta' ? r.eta_basis : p) + '.' : ''}`;
      } else if (forecast) {
        classes.push('fc');
        title = key === 'eta' ? `Forecast — ${esc(r.eta_basis || p)}.` : p === 'booked' ? 'Dock booked for this day; not yet delivered.' : 'Forecast from the plan.';
      }
    } else if (NUM_K.has(key)) {
      classes.push('num'); inner = v === '' || v == null ? '' : esc(v);
    } else if (EMPTY_K.has(key)) {
      classes.push('dim'); inner = v ? esc(v) : '—';
    } else {
      inner = esc(v);
    }
    if (chg) {
      classes.push('el-chg');
      const sig = r.zendesk + '|' + r.transport + '::' + chg.signature;
      if (!S.pulsed.has(sig)) { classes.push('pulse'); S.pulsed.add(sig); }
    }
    if (chg) title = (chg.old ? val(chg, 'old') + ' → ' : '') + val(chg, 'new') + '. ' + esc(chg.context) + (title ? ' ' + title : '');
    if ((key === 'cartons' || key === 'units' || key === 'weight_kg') && r.qty_source === 'plan') { classes.push('dim'); title = 'Planned quantity — nothing binned for these POs yet.'; }
    return { cls: classes.join(' '), inner, title: title ? ` title="${title}"` : '' };
  }

  function notesCell(r, list) {
    if (!list || !list.length) return `<td class="el-notes dim">—</td>`;
    return `<td class="el-notes">${list.map(c => {
      const key = c.row_key + '::' + c.signature;
      const sv = S.saving[key];
      return `<div>
        <div class="el-ctx"><b>${esc(c.label)}</b> ${esc(c.context)}${c.anomaly ? ' <span class="el-flag">after delivery</span>' : ''}</div>
        <textarea class="el-noteI" rows="1" data-note="${esc(key)}" data-row="${esc(c.row_key)}" data-sig="${esc(c.signature)}"
          placeholder="Why — what the client should know">${esc(c.note || '')}</textarea>
        <div class="el-saved">${sv === 'saving' ? 'Saving…' : sv === 'saved' ? 'Saved' : sv === 'error' ? '<span style="color:' + RED + '">Could not save</span>' : ''}</div>
      </div>`;
    }).join('')}</td>`;
  }

  function table(d) {
    const byRow = changesByRow(d);
    const rows = visibleRows(d, byRow);
    const all = d.rows || [];
    const counts = { all: all.length, changed: byRow.size, open: all.filter(r => r.status !== 'Delivered').length, delivered: all.filter(r => r.status === 'Delivered').length,
                     late: all.filter(r => r.late_days != null && r.late_days >= 2 && r.status !== 'Delivered').length,
                     attention: all.filter(r => r.eta_impossible).length };
    const chip = (k, l) => `<button class="el-chipb" data-act="filter" data-v="${k}" aria-pressed="${S.filter === k}">${l}<span class="n">${counts[k]}</span></button>`;
    const arrow = (k) => S.sort.key === k ? `<span class="arr">${S.sort.dir > 0 ? '▲' : '▼'}</span>` : '';

    let left = 0;
    const fzHead = FROZEN.map((c, i) => { const h = `<th class="fz${i === FROZEN.length - 1 ? ' fz-last' : ''}" style="left:${left}px;min-width:${c.w}px;max-width:${c.w}px" data-act="sort" data-v="${c.key}">${c.head}${arrow(c.key)}</th>`; left += c.w; return h; }).join('');
    const shHead = SHEET.map(([k, h]) => `<th class="${NUM_K.has(k) ? 'num' : ''}" data-act="sort" data-v="${k}">${h}${arrow(k)}</th>`).join('');

    const body = rows.map(r => {
      const list = byRow.get(r.zendesk + '|' + r.transport) || [];
      const chgOf = {}; for (const c of list) chgOf[c.field] = c;
      let l = 0;
      const fz = FROZEN.map((c, i) => { const x = cell(r, c.key, chgOf[c.key]); const td = `<td class="fz${i === FROZEN.length - 1 ? ' fz-last' : ''} ${x.cls}" style="left:${l}px;min-width:${c.w}px;max-width:${c.w}px"${x.title}>${x.inner}</td>`; l += c.w; return td; }).join('');
      const sh = SHEET.map(([k]) => { const x = cell(r, k, chgOf[k]); return `<td class="${x.cls}"${x.title}>${x.inner}</td>`; }).join('');
      return `<tr>${fz}${sh}${notesCell(r, list)}</tr>`;
    }).join('');

    return `
<div class="el-card">
  <div class="el-tools">
    <input class="el-search" type="search" placeholder="Zendesk, PO, vendor, container, vessel…" value="${esc(S.q)}" data-search="1">
    <div class="el-chips">${chip('all', 'All')}${chip('changed', 'Changed')}${chip('open', 'Open')}${chip('delivered', 'Delivered')}${counts.late ? chip('late', 'Late') : ''}${counts.attention ? chip('attention', 'Needs attention') : ''}</div>
    <div class="el-legend">
      <span class="el-leg"><span class="bar" style="background:${BLUE}"></span>forecast</span>
      <span class="el-leg"><span class="bar" style="background:transparent;border-left:1px solid #C9C9C5"></span>actual</span>
      <span class="el-leg"><span class="bar" style="background:${RED}"></span>later than promised</span>
      <span class="el-leg"><span class="dot"></span>changed since last send</span>
    </div>
  </div>
  ${rows.length ? `<div class="el-scroll"><table class="el-table">
    <thead><tr>${fzHead}${shHead}<th class="el-notes">Context &amp; notes</th></tr></thead>
    <tbody>${body}</tbody></table></div>`
  : `<div class="el-empty">${S.q || S.filter !== 'all' ? 'Nothing matches.' : (d.last_sent ? 'No lines on the tracker yet.' : 'Nothing has departed since the start of the tracker window.')}</div>`}
</div>`;
  }

  function render() {
    if (!S.root) return;
    if (S.error && !S.data) {
      S.root.innerHTML = `<div class="el"><div class="el-head"><div><button class="el-back" data-act="back">← Transit Movements</button><h1 style="margin-top:8px">Daily client tracker</h1></div></div>
        <div class="el-result err">${esc(S.error)}</div><div><button class="el-btn" data-act="refresh">Try again</button></div></div>`;
      return;
    }
    if (!S.data) {
      S.root.innerHTML = `<div class="el"><div class="el-head"><div><button class="el-back" data-act="back">← Transit Movements</button><h1 style="margin-top:8px">Daily client tracker</h1><div class="el-note">Building the tracker…</div></div></div></div>`;
      return;
    }
    const d = S.data;
    // Keep what someone is typing. A refresh must not eat a half-written note.
    const active = document.activeElement;
    const typing = active && active.matches && active.matches('textarea.el-noteI') ? { key: active.getAttribute('data-note'), value: active.value, pos: active.selectionStart } : null;
    const searching = active && active.matches && active.matches('input[data-search]') ? { pos: active.selectionStart } : null;

    S.root.innerHTML = `<div class="el">
      ${head(d)}
      ${resultLine()}
      ${confirmPanel(d)}
      ${statsRow(d)}
      ${table(d)}
      <div class="el-note">Sheet 1 is the tracker in THE ICONIC's layout, with context and notes in a final column after theirs. Sheet 2 lists the changes. The week column is on screen only — it would shift their column letters. Status is read off the dates every time; nothing here is stored except what was sent and the notes.</div>
    </div>`;

    if (typing) {
      const t = S.root.querySelector(`textarea[data-note="${CSS.escape(typing.key)}"]`);
      if (t) { t.value = typing.value; t.focus(); try { t.setSelectionRange(typing.pos, typing.pos); } catch (_) {} }
    }
    if (searching) {
      const i = S.root.querySelector('input[data-search]');
      if (i) { i.focus(); try { i.setSelectionRange(searching.pos, searching.pos); } catch (_) {} }
    }
    S.root.querySelectorAll('textarea.el-noteI').forEach(autosize);
  }

  function autosize(t) {
    t.style.height = 'auto';
    t.style.height = Math.max(28, t.scrollHeight) + 'px';
  }

  // ── Data ──
  async function load(opts) {
    const quiet = opts && opts.quiet;
    if (!quiet) { S.loading = true; render(); }
    try {
      const d = await req('GET', '/elevate/preview');
      S.data = d; S.error = null; S.loadedAt = Date.now();
    } catch (e) {
      S.error = e.message || String(e);
    }
    S.loading = false;
    render();
  }

  const noteTimers = {};
  function queueNote(el) {
    const key = el.getAttribute('data-note');
    clearTimeout(noteTimers[key]);
    noteTimers[key] = setTimeout(() => saveNote(el), 600);
  }
  async function saveNote(el) {
    const key = el.getAttribute('data-note');
    const row_key = el.getAttribute('data-row'), signature = el.getAttribute('data-sig');
    const note = el.value.trim();
    S.saving[key] = 'saving';
    const sv = el.parentElement && el.parentElement.querySelector('.el-saved');
    if (sv) sv.textContent = 'Saving…';
    try {
      await req('POST', '/elevate/note', { row_key, signature, note });
      S.saving[key] = 'saved';
      if (sv) sv.textContent = note ? 'Saved' : '';
      const c = (S.data && S.data.changes || []).find(x => x.row_key === row_key && x.signature === signature);
      if (c) c.note = note;
      setTimeout(() => { if (S.saving[key] === 'saved') { delete S.saving[key]; if (sv) sv.textContent = ''; } }, 2500);
    } catch (e) {
      S.saving[key] = 'error';
      if (sv) sv.innerHTML = `<span style="color:${RED}">Could not save</span>`;
    }
  }

  async function sendNow() {
    S.sending = true; S.result = null; render();
    try {
      const r = await req('POST', '/elevate/send');
      S.result = r;
    } catch (e) {
      S.result = { sent: false, error: e.message || String(e) };
    }
    S.sending = false; S.confirm = false;
    await load({ quiet: true });
  }

  // ── Events ──
  function bind(root) {
    root.addEventListener('click', (ev) => {
      const b = ev.target.closest('[data-act]');
      if (!b || !root.contains(b)) return;
      const act = b.getAttribute('data-act');
      if (act === 'back') { if (typeof window.show === 'function') window.show('#map'); }
      else if (act === 'refresh') load();
      else if (act === 'download') download('/elevate/tracker.xlsx', 'VOZ_FF_Tracker.xlsx').catch(e => { S.result = { sent: false, error: 'Download failed — ' + e.message }; render(); });
      else if (act === 'send-open') { S.confirm = true; S.result = null; render(); }
      else if (act === 'send-cancel') { S.confirm = false; render(); }
      else if (act === 'send-go') sendNow();
      else if (act === 'filter') { S.filter = b.getAttribute('data-v'); render(); }
      else if (act === 'pos') { const k = b.getAttribute('data-v'); if (S.posOpen.has(k)) S.posOpen.delete(k); else S.posOpen.add(k); render(); }
      else if (act === 'sort') {
        const k = b.getAttribute('data-v');
        S.sort = S.sort.key === k ? { key: k, dir: -S.sort.dir } : { key: k, dir: 1 };
        render();
      }
    });
    let qT = null;
    root.addEventListener('input', (ev) => {
      const t = ev.target;
      if (t && t.matches && t.matches('input[data-search]')) {
        S.q = t.value;
        clearTimeout(qT); qT = setTimeout(render, 120);
      }
    });
    root.addEventListener('input', (ev) => {
      const t = ev.target;
      if (t && t.matches && t.matches('textarea.el-noteI')) { autosize(t); queueNote(t); }
    });
    root.addEventListener('focusout', (ev) => {
      const t = ev.target;
      if (t && t.matches && t.matches('textarea.el-noteI')) { clearTimeout(noteTimers[t.getAttribute('data-note')]); saveNote(t); }
    });
  }

  // ════ Mount ════
  function mount(host) {
    styles();
    host.innerHTML = '';
    const root = document.createElement('div');
    host.appendChild(root);
    S.root = root;
    bind(root);
    render();
    load();
    clearInterval(S.timer);
    S.timer = setInterval(() => {
      const pg = document.getElementById('page-elevate');
      // Never refresh under someone's hands: a note being typed, or the send panel open.
      const ae = document.activeElement;
      const busy = S.confirm || S.sending || (ae && ae.matches && (ae.matches('textarea.el-noteI') || ae.matches('input[data-search]')));
      if (pg && pg.style.display !== 'none' && !document.hidden && !busy) load({ quiet: true });
    }, REFRESH_MS);
  }

  window.showElevatePage = function () {
    let pg = document.getElementById('page-elevate');
    if (!pg) {
      pg = document.createElement('section');
      pg.id = 'page-elevate';
      pg.style.cssText = 'padding:0;display:block;';
      const main = document.querySelector('main.vo-wrap') || document.querySelector('main') || document.body;
      main.appendChild(pg);
      mount(pg);
    } else if (!S.root || !pg.contains(S.root)) {
      mount(pg);
    } else if (Date.now() - S.loadedAt > 2 * 60 * 1000) {
      load({ quiet: true });
    }
    pg.classList.remove('hidden');
    pg.style.display = 'block';
    try { window.scrollTo({ top: 0 }); } catch (_) {}
  };

  window.hideElevatePage = function () {
    const pg = document.getElementById('page-elevate');
    if (pg) { pg.classList.add('hidden'); pg.style.display = 'none'; }
    S.confirm = false;
  };

  // Transit Movements opens this through its report pills: openReport('__openElevate').
  window.__openElevate = function () { if (typeof window.show === 'function') window.show('#elevate'); };

  window.__elevate = { reload: () => load(), state: () => S, version: VERSION };
  console.log('[elevate] v' + VERSION + ' loaded');
})();
