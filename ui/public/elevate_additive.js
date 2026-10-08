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
  const VERSION = '1';

  const INK = '#121212', MUTED = '#5F5F5F', LINE = '#E3E3E0', SOFT = '#EFEFEC';
  const GREEN = '#C7EA46', AMBER = '#F5BD25', RED = '#990033';
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
              showTracker: false, showDelivered: false, confirm: false, sending: false, result: null, saving: {} };

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
.el-table{width:100%;border-collapse:collapse;font-size:13px}
.el-table th{text-align:left;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:${MUTED};font-weight:500;padding:10px 12px;border-bottom:1px solid ${LINE};white-space:nowrap}
.el-table td{padding:10px 12px;border-bottom:1px solid ${SOFT};vertical-align:top}
.el-table tr:last-child td{border-bottom:0}
.el-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.el-chip{display:inline-block;font-size:11px;font-weight:500;padding:2px 8px;border-radius:10px;background:${SOFT};color:${MUTED};white-space:nowrap}
.el-was{color:${MUTED};text-decoration:line-through;text-decoration-color:#C9C9C5}
.el-arrow{color:${MUTED};margin:0 6px}
.el-now{font-weight:600}
.el-ctx{font-size:12.5px;color:#48484A;line-height:1.4}
.el-noteI{width:100%;border:1px solid transparent;background:${SOFT};border-radius:7px;padding:7px 9px;font-size:12.5px;line-height:1.35;resize:vertical;min-height:34px;outline:none}
.el-noteI:focus{border-color:#B8B8B4;background:#fff}
.el-noteI::placeholder{color:#9C9C98}
.el-saved{font-size:11px;color:${MUTED};margin-top:3px;height:14px}
.el-flag{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:500;color:#7A5200;background:#FFF4D6;padding:3px 9px;border-radius:10px}
.el-empty{padding:28px 18px;font-size:13.5px;color:${MUTED};text-align:center}
.el-confirm{background:#FAFAF8;border:1px solid ${LINE};border-radius:12px;padding:18px 20px;display:flex;flex-direction:column;gap:12px}
.el-confirm .row{display:flex;gap:18px;flex-wrap:wrap;font-size:13px}
.el-confirm .row b{font-weight:600}
.el-result{font-size:13px;padding:12px 16px;border-radius:10px;background:#F0F7D9}
.el-result.err{background:#FDECEF;color:${RED}}
.el-toggle{background:none;border:0;padding:0;font-size:12.5px;color:${MUTED};text-decoration:underline;text-underline-offset:3px}
.el-toggle:hover{color:${INK}}
.el-tracker{overflow:auto;max-height:560px}
.el-tracker .el-table th{position:sticky;top:0;background:#fff;z-index:1}
.el-tracker td{white-space:nowrap}
.el-tracker td.pos{white-space:pre-line;font-size:12px;color:${MUTED}}
.el-pill{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:${MUTED}}
.el-dot{width:8px;height:8px;border-radius:50%;display:inline-block}
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

  function changes(d) {
    const list = d.changes || [];
    const normal = list.filter(c => !c.anomaly), odd = list.filter(c => c.anomaly);
    const row = (c) => {
      const key = c.row_key + '::' + c.signature;
      const sv = S.saving[key];
      return `<tr>
        <td><b>${esc(c.zendesk)}</b><div style="margin-top:3px"><span class="el-chip">${esc(c.transport)}</span></div></td>
        <td><div style="font-weight:500">${esc(c.vendor)}</div><div style="font-size:12px;color:${MUTED};margin-top:2px">${esc(c.pos)}</div></td>
        <td><span class="el-chip">${esc(c.label)}</span></td>
        <td style="white-space:normal">${c.old ? `<span class="el-was">${esc(val(c, 'old'))}</span><span class="el-arrow">→</span>` : ''}<span class="el-now" style="${c.field === 'status' ? 'color:' + (STATUS_INK[c.new] || INK) : ''}">${esc(val(c, 'new')) || '—'}</span></td>
        <td><div class="el-ctx">${esc(c.context)}</div></td>
        <td>
          <textarea class="el-noteI" rows="1" data-note="${esc(key)}" data-row="${esc(c.row_key)}" data-sig="${esc(c.signature)}"
            placeholder="Why — what the client should know">${esc(c.note || '')}</textarea>
          <div class="el-saved">${sv === 'saving' ? 'Saving…' : sv === 'saved' ? 'Saved' : sv === 'error' ? '<span style="color:' + RED + '">Could not save</span>' : ''}</div>
        </td>
      </tr>`;
    };
    const table = (rows) => `<table class="el-table" style="table-layout:fixed">
      <colgroup><col style="width:92px"><col style="width:22%"><col style="width:150px"><col style="width:230px"><col><col style="width:26%"></colgroup>
      <thead><tr><th>Zendesk</th><th>Vendor · PO</th><th>Field</th><th>Was → now</th><th>Context</th><th>Note to the client</th></tr></thead>
      <tbody>${rows.map(row).join('')}</tbody></table>`;

    return `
<div class="el-card">
  <div class="el-cardh">
    <div>
      <h2 class="el-h2">What changed since the last send</h2>
      <div class="el-note" style="margin-top:2px">Context is written from the record. Add the why; it goes on the Changes sheet and in the email beside the change.</div>
    </div>
    <span class="el-note">${list.length ? `${list.length} change${list.length === 1 ? '' : 's'}` : ''}</span>
  </div>
  ${list.length ? '' : `<div class="el-empty">${d.last_sent ? 'No changes since the last send. Tonight\'s email will say exactly that.' : 'Nothing to compare against yet. The first send sets the baseline.'}</div>`}
  ${normal.length ? table(normal) : ''}
  ${odd.length ? `
    <div class="el-cardh" style="border-top:1px solid ${LINE};background:#FFFBF0">
      <div><h2 class="el-h2">Changed after delivery</h2>
        <div class="el-note" style="margin-top:2px">A delivered line has no next state, so a change on one is a question, not an update. Flagged for their review on the sheet.</div></div>
      <span class="el-flag">${odd.length}</span>
    </div>
    ${table(odd)}` : ''}
</div>`;
  }

  function tracker(d) {
    const all = d.rows || [];
    const rows = S.showDelivered ? all : all.filter(r => r.status !== 'Delivered');
    const delivered = all.length - all.filter(r => r.status !== 'Delivered').length;
    return `
<div class="el-card">
  <div class="el-cardh">
    <div><h2 class="el-h2">The tracker as it stands</h2>
      <div class="el-note" style="margin-top:2px">${all.length} lines, one per Zendesk per transport. This is sheet 1 of what they receive; all 28 columns are in the download.</div></div>
    <div style="display:flex;gap:14px;align-items:center">
      ${S.showTracker ? `<button class="el-toggle" data-act="toggle-delivered">${S.showDelivered ? `Hide ${delivered} delivered` : `Show ${delivered} delivered`}</button>` : ''}
      <button class="el-toggle" data-act="toggle-tracker">${S.showTracker ? 'Collapse' : 'Expand'}</button>
    </div>
  </div>
  ${S.showTracker ? `<div class="el-tracker"><table class="el-table">
    <thead><tr><th>Zendesk</th><th>Mode</th><th>Vendor</th><th>PO #</th><th>Status</th><th>Vessel / flight</th><th>Container / MAWB</th><th>Ex factory</th><th>Handover</th><th>ETD</th><th>ETA</th><th>Delivery</th><th>Week</th><th class="num">Ctns</th><th class="num">Units</th></tr></thead>
    <tbody>${rows.map(r => `<tr>
      <td><b>${esc(r.zendesk)}</b></td><td>${esc(r.transport)}</td>
      <td style="white-space:normal;min-width:220px;max-width:300px">${esc(r.vendor)}</td>
      <td class="pos">${esc(r.pos)}</td>
      <td><span class="el-pill"><span class="el-dot" style="background:${r.status === 'Delivered' ? GREEN : r.status.trim() === 'Delivery Booked' ? AMBER : r.status === 'Landed On Route' ? INK : '#C9C9C5'}"></span>${esc(String(r.status).trim())}</span></td>
      <td>${esc(r.vessel)}</td><td>${esc(r.mbl)}</td>
      <td>${esc(day(r.ex_factory))}</td><td>${esc(day(r.handover)) || '<span style="color:#C9C9C5">—</span>'}</td>
      <td>${esc(day(r.etd))}</td><td>${esc(day(r.eta))}</td><td>${esc(day(r.delivery))}</td>
      <td>${esc(r.dw_week)}</td>
      <td class="num">${r.cartons === '' ? '' : esc(r.cartons)}</td><td class="num">${r.units === '' ? '' : esc(r.units)}</td>
    </tr>`).join('')}</tbody></table></div>` : ''}
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

    S.root.innerHTML = `<div class="el">
      ${head(d)}
      ${resultLine()}
      ${confirmPanel(d)}
      ${statsRow(d)}
      ${changes(d)}
      ${tracker(d)}
      <div class="el-note">Sheet 1 is the full tracker, 28 columns in THE ICONIC's layout. Sheet 2 is this list of changes with context and notes. Status is read off the dates every time; nothing on this page is stored except what was sent and the notes.</div>
    </div>`;

    if (typing) {
      const t = S.root.querySelector(`textarea[data-note="${CSS.escape(typing.key)}"]`);
      if (t) { t.value = typing.value; t.focus(); try { t.setSelectionRange(typing.pos, typing.pos); } catch (_) {} }
    }
    S.root.querySelectorAll('textarea.el-noteI').forEach(autosize);
  }

  function autosize(t) {
    t.style.height = 'auto';
    t.style.height = Math.max(34, t.scrollHeight) + 'px';
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
      else if (act === 'toggle-tracker') { S.showTracker = !S.showTracker; render(); }
      else if (act === 'toggle-delivered') { S.showDelivered = !S.showDelivered; render(); }
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
      const busy = S.confirm || S.sending || (document.activeElement && document.activeElement.matches && document.activeElement.matches('textarea.el-noteI'));
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
