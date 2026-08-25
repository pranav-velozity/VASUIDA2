/* ── VelOzity Pinpoint — Air Freight Quotes (client surface) v1 ──
   Panel on the Week Hub plus the request modal.

   Two deliberate choices:

   1. The modal carries its OWN week selector rather than inheriting the page's week.
      Quotes run several weeks ahead of the plan upload — a request raised in week 35 is
      usually for week 40 — so making the client navigate to the target week first would
      be backwards. Nothing here pre-fills from the plan, because there is no plan yet.

   2. The panel lists ALL open quotes regardless of week, not just the displayed one. A
      quote raised for week 40 while sitting on week 35 would otherwise be invisible the
      moment it was submitted.

   Nothing in this file references cost or margin. The server never sends them to a client;
   this module must never start asking. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const AMBER = '#B7791F', GREEN = '#1B7F3B', RED = '#B33F40';
  const LIFT = '0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06)';

  let _on = false, _capClient = null, _data = null, _vendors = [];

  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const nf = n => Number(n || 0).toLocaleString();
  const money = (n, c) => n == null ? '—' : (c || 'AUD') + ' ' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');

  async function tok() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts) {
    const t = await tok(), o = opts || {};
    const headers = { 'Content-Type': 'application/json', ...(o.headers || {}) };
    if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers });
    const txt = await r.text(); let d = null;
    try { d = txt ? JSON.parse(txt) : null; } catch (e) { d = txt; }
    if (!r.ok) { const e = new Error((d && d.error) || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return d;
  }

  // ── week helpers ──
  // ISO week number, so the selector can be labelled the way the client talks about it
  // ("Week 40") while Pinpoint keys on the Monday date underneath.
  function isoWeek(d) {
    const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    t.setUTCDate(t.getUTCDate() + 4 - (t.getUTCDay() || 7));
    const y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
    return Math.ceil((((t - y0) / 86400000) + 1) / 7);
  }
  function mondayOf(d) {
    const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7));
    return x;
  }
  const fmtDay = d => d.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' });
  // Four weeks back through twenty-six ahead. Past weeks stay selectable but flagged —
  // retrospective quotes happen, and blocking them just moves the conversation to email.
  function weekOptions() {
    const base = mondayOf(new Date()), out = [];
    for (let i = -4; i <= 26; i++) {
      const d = new Date(base); d.setUTCDate(d.getUTCDate() + i * 7);
      out.push({ value: d.toISOString().slice(0, 10), week: isoWeek(d), date: fmtDay(d), past: i < 0, next: i === 1 });
    }
    return out;
  }

  function styles() {
    if (el('aq-styles')) return;
    const s = document.createElement('style'); s.id = 'aq-styles';
    s.textContent = `
      #aq-panel{margin:14px 0;}
      .aq-card{background:#fff;border:.5px solid rgba(0,0,0,.09);border-radius:12px;
               padding:15px 17px;box-shadow:${LIFT};}
      .aq-h{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:2px;}
      .aq-t{font-size:13px;font-weight:700;color:${DARK};}
      .aq-s{font-size:10px;color:${LIGHT};}
      .aq-btn{border:0;border-radius:9px;background:${BRAND};color:#fff;
              font:600 12px inherit;padding:8px 14px;cursor:pointer;}
      .aq-btn.g{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);}
      .aq-btn:disabled{opacity:.55;cursor:default;}
      .aq-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:12px 0 4px;}
      .aq-tile{border:.5px solid rgba(0,0,0,.09);border-radius:11px;padding:11px 13px;box-shadow:${LIFT};}
      .aq-tl{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .aq-tv{font-size:19px;font-weight:700;color:${DARK};margin-top:2px;font-variant-numeric:tabular-nums;letter-spacing:-.02em;}
      .aq-ts{font-size:10px;color:${LIGHT};margin-top:2px;}
      table.aq{width:100%;border-collapse:collapse;font-size:11px;margin-top:8px;}
      table.aq th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;
                  color:${LIGHT};font-weight:600;padding:6px;border-bottom:.5px solid rgba(0,0,0,.08);}
      table.aq td{padding:7px 6px;border-bottom:.5px solid rgba(0,0,0,.05);color:${DARK};vertical-align:middle;}
      table.aq td.n{text-align:right;font-variant-numeric:tabular-nums;}
      .aq-pill{display:inline-block;font-size:9px;font-weight:600;padding:2px 7px;border-radius:20px;}
      .aq-none{font-size:11px;color:${LIGHT};padding:14px 2px;}
      .aq-ov{position:fixed;inset:0;background:rgba(0,0,0,.34);z-index:9600;display:flex;
             align-items:flex-start;justify-content:center;padding:28px 18px;overflow:auto;
             -webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px);}
      .aq-modal{background:#fff;border-radius:16px;width:min(520px,100%);padding:22px 24px;
                box-shadow:0 18px 60px rgba(0,0,0,.22);}
      .aq-lbl{display:block;font-size:11px;font-weight:600;color:${MID};margin:12px 0 4px;}
      .aq-in{width:100%;padding:9px 11px;border:.5px solid rgba(0,0,0,.14);border-radius:9px;
             font:inherit;font-size:13px;color:${DARK};background:#fff;}
      .aq-in:focus{outline:2px solid rgba(153,0,51,.22);outline-offset:1px;}
      .aq-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
      .aq-chg{margin-top:14px;padding:11px 13px;background:#F5F5F7;border-radius:10px;}
      .aq-chg b{font-size:17px;}
      .aq-msg{margin-top:10px;padding:9px 11px;border-radius:8px;font-size:11px;}
    `;
    document.head.appendChild(s);
  }

  const STATE_STYLE = {
    submitted:   { t: 'Submitted',        c: MID,   b: 'rgba(0,0,0,.05)' },
    in_progress: { t: 'Pricing',          c: AMBER, b: 'rgba(183,121,31,.12)' },
    quoted:      { t: 'Awaiting you',     c: BRAND, b: 'rgba(153,0,51,.10)' },
    approved:    { t: 'Approved',         c: GREEN, b: 'rgba(27,127,59,.12)' },
    declined:    { t: 'Declined',         c: RED,   b: 'rgba(179,63,64,.12)' },
    expired:     { t: 'Expired',          c: LIGHT, b: 'rgba(0,0,0,.05)' },
  };
  const pill = st => { const s = STATE_STYLE[st] || STATE_STYLE.submitted;
    return `<span class="aq-pill" style="color:${s.c};background:${s.b}">${s.t}</span>`; };

  // ── mount ──
  function onWeekHub() {
    const h = (location.hash || '').toLowerCase();
    return h === '' || h === '#' || h.includes('week-hub') || h.includes('flow') || h.includes('dashboard');
  }
  function ensureHost() {
    let host = el('aq-panel');
    if (!host) {
      const dash = el('page-dashboard'); if (!dash) return null;
      host = document.createElement('div'); host.id = 'aq-panel';
      dash.appendChild(host);                     // below the existing week content
    }
    return host;
  }

  // ── panel ──
  function render() {
    const host = ensureHost(); if (!host || !_data) return;
    const t = _data.tiles || {}, cur = t.currency || 'AUD';
    const open = _data.open || [], hist = (_data.history || []).slice(0, 12);

    const tile = (label, value, sub) => `<div class="aq-tile">
      <div class="aq-tl">${esc(label)}</div><div class="aq-tv">${value}</div>
      <div class="aq-ts">${esc(sub || '')}</div></div>`;

    const rows = (list, showAction) => list.map(q => `<tr>
      <td><b>${esc(q.ref || '')}</b><div style="color:${LIGHT};font-size:10px;">${esc(q.week_label || q.week_start)}</div></td>
      <td>${esc(q.vendor)}</td>
      <td class="n">${nf(q.cartons)}</td>
      <td class="n">${nf(q.chargeable_kg)}</td>
      <td class="n">${q.quoted_amount == null ? '—' : money(q.quoted_amount, q.currency)}</td>
      <td>${pill(q.state)}${q.state === 'quoted' && q.valid_until ? `<div style="color:${LIGHT};font-size:9px;margin-top:2px;">until ${esc(q.valid_until)}</div>` : ''}</td>
      <td style="text-align:right;white-space:nowrap;">
        ${showAction && q.state === 'quoted'
          ? `<button class="aq-btn" data-ok="${esc(q.id)}" style="padding:5px 10px;">Approve</button>
             <button class="aq-btn g" data-no="${esc(q.id)}" style="padding:5px 10px;margin-left:4px;">Decline</button>`
          : (q.decided_at ? `<span style="color:${LIGHT};font-size:10px;">${esc(String(q.decided_at).slice(0,10))}</span>` : '')}
      </td></tr>`).join('');

    host.innerHTML = `
      <div class="aq-card">
        <div class="aq-h">
          <div><div class="aq-t">Air freight quotes</div>
            <div class="aq-s">${open.length ? nf(open.length) + ' open' : 'No open requests'}${
              _data.awaiting_approval ? ` · <b style="color:${BRAND}">${_data.awaiting_approval} awaiting your approval</b>` : ''}</div></div>
          <button class="aq-btn" id="aq-new">Request a quote</button>
        </div>

        ${t.approved_count ? `<div class="aq-tiles">
          ${tile('Approved', nf(t.approved_count), 'last ' + t.window_weeks + ' weeks')}
          ${tile('Total spend', money(t.total_spend, cur), '')}
          ${tile('Per chargeable kg', t.per_chargeable_kg == null ? '—' : money(t.per_chargeable_kg, cur), 'not per gross kg')}
          ${tile('Per carton', t.per_carton == null ? '—' : money(t.per_carton, cur), '')}
          ${tile('Per unit', t.per_unit == null ? '—' : money(t.per_unit, cur),
                 t.per_unit == null ? 'no unit counts entered' : t.per_unit_basis + ' of ' + t.approved_count + ' quotes')}
          ${tile('Approval rate', t.approval_rate_pct == null ? '—' : t.approval_rate_pct + '%', '')}
          ${tile('Avg turnaround', t.avg_turnaround_hours == null ? '—' : Math.round(t.avg_turnaround_hours) + 'h', 'request to quote')}
        </div>
        <div class="aq-s" style="margin-bottom:4px;">Averages cover approved quotes only, over a rolling ${t.window_weeks} weeks. Air rates move seasonally &mdash; read them against the trend, not as a fixed benchmark.</div>` : ''}

        ${open.length ? `<table class="aq"><thead><tr>
            <th>Reference</th><th>Vendor</th><th class="n">Cartons</th><th class="n">Chargeable kg</th>
            <th class="n">Quoted</th><th>Status</th><th></th></tr></thead>
          <tbody>${rows(open, true)}</tbody></table>`
          : `<div class="aq-none">No open quote requests. Raise one for any upcoming week &mdash; you don't need the plan uploaded first.</div>`}

        ${hist.length ? `<div style="font-size:10px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;margin:16px 0 2px;">History</div>
          <table class="aq"><thead><tr>
            <th>Reference</th><th>Vendor</th><th class="n">Cartons</th><th class="n">Chargeable kg</th>
            <th class="n">Quoted</th><th>Status</th><th></th></tr></thead>
          <tbody>${rows(hist, false)}</tbody></table>` : ''}
        <div id="aq-msg"></div>
      </div>`;

    el('aq-new').addEventListener('click', openModal);
    host.querySelectorAll('[data-ok]').forEach(b => b.addEventListener('click', () => decide(b.getAttribute('data-ok'), 'approve')));
    host.querySelectorAll('[data-no]').forEach(b => b.addEventListener('click', () => decide(b.getAttribute('data-no'), 'decline')));
  }

  async function decide(id, decision) {
    const q = (_data.open || []).find(x => x.id === id) || {};
    const label = decision === 'approve' ? 'Approve' : 'Decline';
    if (!confirm(`${label} ${q.ref || 'this quote'}?\n\n${q.vendor || ''} · ${money(q.quoted_amount, q.currency)}\n\nThis is recorded against your Pinpoint login.`)) return;
    let note = null;
    if (decision === 'decline') { note = prompt('Reason for declining (optional):', '') || null; }
    try {
      await req('/air-quotes/' + encodeURIComponent(id) + '/decision',
        { method: 'POST', body: JSON.stringify({ decision, note }) });
      await load();
    } catch (e) {
      const m = el('aq-msg');
      if (m) m.innerHTML = `<div class="aq-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(e.message || e)}</div>`;
    }
  }

  // ── modal ──
  function closeModal() { const o = document.querySelector('.aq-ov'); if (o) o.remove(); }

  function openModal() {
    styles(); closeModal();
    const weeks = weekOptions();
    const o = document.createElement('div'); o.className = 'aq-ov';
    o.addEventListener('click', e => { if (e.target === o) closeModal(); });
    o.innerHTML = `<div class="aq-modal">
      <div style="font-size:15px;font-weight:700;color:${DARK};">Request an air freight quote</div>
      <div style="font-size:11px;color:${LIGHT};margin-top:2px;">Choose any upcoming week — the plan doesn't need to be uploaded yet.</div>

      <label class="aq-lbl" for="aq-week">Shipping week</label>
      <select class="aq-in" id="aq-week">
        ${weeks.map(w => `<option value="${w.value}" data-wk="Week ${w.week}" ${w.next ? 'selected' : ''}>
          Week ${w.week} · ${esc(w.date)}${w.past ? '  (past)' : ''}</option>`).join('')}
      </select>

      <label class="aq-lbl" for="aq-vendor">Vendor</label>
      <input class="aq-in" id="aq-vendor" list="aq-vendorlist" placeholder="Start typing…" autocomplete="off">
      <datalist id="aq-vendorlist">${_vendors.map(v => `<option value="${esc(v.name)}">`).join('')}</datalist>

      <div class="aq-row">
        <div><label class="aq-lbl" for="aq-cartons">Carton count</label>
          <input class="aq-in" id="aq-cartons" type="number" min="0" step="1" inputmode="numeric" placeholder="0"></div>
        <div><label class="aq-lbl" for="aq-units">Units <span style="font-weight:400;color:${LIGHT}">(optional)</span></label>
          <input class="aq-in" id="aq-units" type="number" min="0" step="1" inputmode="numeric" placeholder="0"></div>
      </div>

      <div class="aq-row">
        <div><label class="aq-lbl" for="aq-gross">Gross weight (kg)</label>
          <input class="aq-in" id="aq-gross" type="number" min="0" step="0.01" inputmode="decimal" placeholder="0.00"></div>
        <div><label class="aq-lbl" for="aq-cbm">Volume (CBM)</label>
          <input class="aq-in" id="aq-cbm" type="number" min="0" step="0.01" inputmode="decimal" placeholder="0.00"></div>
      </div>

      <div class="aq-chg" id="aq-chg">
        <div style="font-size:9px;color:${MID};text-transform:uppercase;letter-spacing:.06em;font-weight:600;">Chargeable weight</div>
        <b id="aq-chgv">—</b>
        <div style="font-size:10px;color:${MID};margin-top:3px;" id="aq-chgn">Air freight bills on the greater of gross and volumetric weight.</div>
      </div>

      <label class="aq-lbl" for="aq-note">Note <span style="font-weight:400;color:${LIGHT}">(optional)</span></label>
      <input class="aq-in" id="aq-note" placeholder="Anything the carrier should know">

      <div style="display:flex;gap:10px;margin-top:18px;">
        <button class="aq-btn g" id="aq-cancel" style="flex:1;padding:11px;">Cancel</button>
        <button class="aq-btn" id="aq-submit" style="flex:2;padding:11px;">Send request</button>
      </div>
      <div id="aq-mmsg"></div>
    </div>`;
    document.body.appendChild(o);

    // Live chargeable weight. Showing it before submission removes an entire category of
    // "why is this so expensive" once the quote comes back.
    const recalc = () => {
      const g = Number(el('aq-gross').value) || 0, c = Number(el('aq-cbm').value) || 0;
      const vol = Math.round(c * 166.67 * 100) / 100;
      const chg = Math.max(g, vol);
      el('aq-chgv').textContent = chg ? nf(Math.round(chg * 100) / 100) + ' kg' : '—';
      el('aq-chgn').innerHTML = chg
        ? `Greater of gross (${nf(g)} kg) and volumetric (${nf(vol)} kg at 166.67 kg/CBM) &mdash; <b>${vol > g ? 'volumetric' : 'gross'}</b> applies.`
        : 'Air freight bills on the greater of gross and volumetric weight.';
    };
    ['aq-gross', 'aq-cbm'].forEach(id => el(id).addEventListener('input', recalc));

    el('aq-cancel').addEventListener('click', closeModal);
    el('aq-submit').addEventListener('click', async () => {
      const sel = el('aq-week');
      const payload = {
        week_start: sel.value,
        week_label: sel.selectedOptions[0].getAttribute('data-wk'),
        vendor: el('aq-vendor').value.trim(),
        cartons: parseInt(el('aq-cartons').value, 10) || 0,
        units: el('aq-units').value === '' ? null : (parseInt(el('aq-units').value, 10) || 0),
        gross_weight_kg: Number(el('aq-gross').value) || 0,
        cbm: Number(el('aq-cbm').value) || 0,
        client_note: el('aq-note').value.trim() || null,
      };
      const fail = m => { el('aq-mmsg').innerHTML = `<div class="aq-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(m)}</div>`; };
      if (!payload.vendor) return fail('Vendor is required.');
      if (!payload.gross_weight_kg && !payload.cbm) return fail('Enter a gross weight or a CBM figure.');
      const btn = el('aq-submit'); btn.disabled = true; btn.textContent = 'Sending…';
      try {
        await req('/air-quotes', { method: 'POST', body: JSON.stringify(payload) });
        closeModal();
        await load();
      } catch (e) { fail(e.message || String(e)); btn.disabled = false; btn.textContent = 'Send request'; }
    });
  }

  // ── load / gate ──
  async function load() {
    try {
      _data = await req('/air-quotes');
      try { _vendors = (await req('/air-quotes/vendors')).vendors || []; } catch (e) {}
      render();
    } catch (e) { /* leave the panel as it was */ }
  }

  async function check() {
    styles();
    const active = window.pinpointClient || 'unknown';
    if (!onWeekHub()) { const h = el('aq-panel'); if (h) h.style.display = 'none'; return; }
    const h = el('aq-panel'); if (h) h.style.display = '';
    if (_capClient === active) { if (_on) load(); return; }
    // Fail closed on a definite refusal; ignore transient errors so a cold load (401 before
    // the Clerk token is ready) never permanently hides the panel.
    try { _data = await req('/air-quotes'); _on = true; }
    catch (e) { if (e.status === 403 || e.status === 409) { _on = false; _capClient = active;
                  const p = el('aq-panel'); if (p) p.remove(); } return; }
    _capClient = active;
    try { _vendors = (await req('/air-quotes/vendors')).vendors || []; } catch (e) {}
    render();
  }

  function init() {
    styles();
    window.addEventListener('hashchange', check);
    window.addEventListener('state:ready', check);
    window.addEventListener('air-quotes:changed', load);
    setInterval(check, 20000);
    setTimeout(check, 800);
    console.log('[air-quotes] module v1 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
