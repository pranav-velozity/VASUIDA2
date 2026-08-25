/* ── VelOzity Pinpoint — Air Quote Review (internal) v1 ──
   The one surface in Pinpoint where partner cost and margin are visible.

   Admin only, and it fails closed: /air-quotes/internal returns 403 for anyone else, at
   which point this module removes its own nav entry. Nothing here is reachable by a client
   or partner login — but the server is the guard, not this file.

   Design intent: a reviewer should be able to tell a sensible quote from a suspect one in
   about three seconds. That means rate per chargeable kg next to the vendor's own recent
   average, not just an absolute figure. 6,400 against 9,100 says nothing; 3.35/kg against
   4.80/kg says everything. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const AMBER = '#B7791F', GREEN = '#1B7F3B', RED = '#B33F40';

  let _on = false, _data = null, _sel = null, _busy = false;

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

  function styles() {
    if (el('aqi-styles')) return;
    const s = document.createElement('style'); s.id = 'aqi-styles';
    s.textContent = `
      .aqi-ov{position:fixed;inset:0;background:#fff;z-index:9500;display:flex;
              align-items:stretch;justify-content:center;}
      body.aqi-open{overflow:hidden;}
      .aqi-panel{width:100%;height:100%;display:flex;flex-direction:column;overflow:hidden;}
      .aqi-head{display:flex;justify-content:space-between;align-items:center;gap:14px;
                padding:16px 28px;border-bottom:.5px solid rgba(0,0,0,.08);flex:0 0 auto;}
      .aqi-t{font-size:15px;font-weight:700;color:${DARK};letter-spacing:-.01em;}
      .aqi-s{font-size:11px;color:${LIGHT};margin-top:2px;}
      .aqi-x{background:none;border:0;font-size:22px;color:${LIGHT};cursor:pointer;line-height:1;padding:0 4px;}
      .aqi-x:hover{color:${DARK};}
      .aqi-body{padding:20px 28px 40px;overflow:auto;flex:1 1 auto;max-width:1560px;width:100%;margin:0 auto;}
      .aqi-grid{display:grid;grid-template-columns:minmax(0,1fr) 400px;gap:20px;align-items:start;}
      @media (max-width:1180px){ .aqi-grid{grid-template-columns:1fr;} }
      .aqi-card{background:#fff;border:.5px solid rgba(0,0,0,.1);border-radius:12px;padding:15px 17px;
                box-shadow:0 1px 2px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.06);}
      .aqi-sec{font-size:10px;font-weight:700;color:${LIGHT};text-transform:uppercase;
               letter-spacing:.07em;margin:0 0 9px;}
      table.aqi{width:100%;border-collapse:collapse;font-size:11px;}
      table.aqi th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;
                   color:${LIGHT};font-weight:600;padding:6px;border-bottom:.5px solid rgba(0,0,0,.08);}
      table.aqi th.n{text-align:right;}
      table.aqi td{padding:8px 6px;border-bottom:.5px solid rgba(0,0,0,.05);color:${DARK};}
      table.aqi td.n{text-align:right;font-variant-numeric:tabular-nums;}
      table.aqi tr.sel td{background:rgba(153,0,51,.05);}
      table.aqi tbody tr{cursor:pointer;}
      table.aqi tbody tr:hover td{background:rgba(0,0,0,.02);}
      .aqi-pill{display:inline-block;font-size:9px;font-weight:600;padding:2px 7px;border-radius:20px;}
      .aqi-kv{display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin-bottom:2px;}
      .aqi-kv dt{color:${MID};}
      .aqi-kv dd{margin:0;text-align:right;font-variant-numeric:tabular-nums;}
      .aqi-calc{margin-top:12px;padding:12px 14px;background:#F5F5F7;border-radius:10px;}
      .aqi-big{font-size:22px;font-weight:700;letter-spacing:-.02em;color:${DARK};
               font-variant-numeric:tabular-nums;}
      .aqi-lbl{display:block;font-size:11px;font-weight:600;color:${MID};margin:12px 0 4px;}
      .aqi-in{width:100%;padding:9px 11px;border:.5px solid rgba(0,0,0,.14);border-radius:9px;
              font:inherit;font-size:13px;color:${DARK};background:#fff;}
      .aqi-in:focus{outline:2px solid rgba(153,0,51,.22);outline-offset:1px;}
      .aqi-btn{border:0;border-radius:9px;background:${BRAND};color:#fff;font:600 12px inherit;
               padding:10px 16px;cursor:pointer;}
      .aqi-btn.g{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);}
      .aqi-btn:disabled{opacity:.55;cursor:default;}
      .aqi-none{font-size:11px;color:${LIGHT};padding:16px 2px;}
      .aqi-msg{margin-top:10px;padding:9px 11px;border-radius:8px;font-size:11px;}
      .aqi-warn{font-size:10px;color:${AMBER};margin-top:4px;}
    `;
    document.head.appendChild(s);
  }

  const ST = {
    submitted:      { t: 'Sending',            c: AMBER, b: 'rgba(183,121,31,.12)' },
    rfq_sent:       { t: 'Submitted to partner', c: MID, b: 'rgba(0,0,0,.05)' },
    costed:         { t: 'Costed',       c: BRAND, b: 'rgba(153,0,51,.10)' },
    pending_review: { t: 'Review',       c: BRAND, b: 'rgba(153,0,51,.10)' },
    quoted:         { t: 'With client',  c: MID,   b: 'rgba(0,0,0,.05)' },
    approved:       { t: 'Approved',     c: GREEN, b: 'rgba(27,127,59,.12)' },
    declined:       { t: 'Declined',     c: RED,   b: 'rgba(179,63,64,.12)' },
    expired:        { t: 'Expired',      c: LIGHT, b: 'rgba(0,0,0,.05)' },
  };
  const pill = st => { const s = ST[st] || ST.submitted;
    return `<span class="aqi-pill" style="color:${s.c};background:${s.b}">${s.t}</span>`; };

  // ── nav ──
  function injectNav() {
    if (el('nav-aqi')) return;
    const after = el('nav-finance') || el('nav-reports') || el('nav-exec');
    if (!after || !after.parentNode) return;
    const a = document.createElement('a');
    a.className = after.className; a.id = 'nav-aqi'; a.href = '#air-quote-review';
    a.textContent = 'Quote Review';
    a.style.display = 'none';
    a.addEventListener('click', e => { e.preventDefault(); open(); });
    after.parentNode.insertBefore(a, after.nextSibling);
  }

  function paintNav() {
    const a = el('nav-aqi'); if (!a) return;
    const n = _data ? (_data.quotes || []).filter(q => q.state === 'pending_review' || q.state === 'costed').length : 0;
    a.textContent = 'Quote Review';
    if (n) {
      const b = document.createElement('span');
      b.style.cssText = `background:${BRAND};color:#fff;font-size:9px;font-weight:700;border-radius:20px;padding:1px 6px;margin-left:6px;`;
      b.textContent = String(n);
      a.appendChild(b);
    }
    a.style.display = _on ? '' : 'none';
  }

  // ── render ──
  function render() {
    const body = el('aqi-body'); if (!body || !_data) return;
    const list = _data.quotes || [];
    const q = list.find(x => x.id === _sel) || list.find(x => x.state === 'pending_review' || x.state === 'costed') || list[0] || null;
    _sel = q ? q.id : null;

    body.innerHTML = `<div class="aqi-grid">
      <div class="aqi-card">
        <div class="aqi-sec">Queue &middot; ${list.length} quote(s)</div>
        ${list.length ? `<table class="aqi"><thead><tr>
            <th>Ref</th><th>Week</th><th>Sent to partner</th><th>Vendor</th><th class="n">Chargeable kg</th>
            <th class="n">Cost</th><th class="n">Sell</th><th class="n">$/kg</th><th>Status</th>
          </tr></thead><tbody>${list.map(x => `<tr data-q="${esc(x.id)}" class="${x.id === _sel ? 'sel' : ''}">
            <td><b>${esc(x.ref || '')}</b></td>
            <td style="color:${MID}">${esc(x.week_label || x.week_start)}</td>
            <td style="color:${LIGHT};font-size:10px;">${x.rfq_sent_at ? esc(String(x.rfq_sent_at).slice(0, 10)) : '—'}</td>
            <td>${esc(x.vendor_raw)}</td>
            <td class="n">${nf(x.chargeable_kg)}</td>
            <td class="n">${x.cost == null ? '—' : money(x.cost, x.cost_currency)}</td>
            <td class="n">${x.sell_preview == null ? '—' : money(x.sell_preview, x.currency)}</td>
            <td class="n">${x.sell_per_kg == null ? '—' : x.sell_per_kg.toFixed(2)}</td>
            <td>${pill(x.state)}</td></tr>`).join('')}</tbody></table>`
          : `<div class="aqi-none">No quote requests yet.</div>`}
      </div>
      <div id="aqi-detail">${detailHtml(q)}</div>
    </div>`;

    body.querySelectorAll('[data-q]').forEach(r => r.addEventListener('click', () => {
      _sel = r.getAttribute('data-q'); render();
    }));
    wireDetail();
  }

  function detailHtml(q) {
    if (!q) return `<div class="aqi-card"><div class="aqi-none">Select a quote.</div></div>`;
    const dflt = _data.markup_default_pct;
    const canReview = ['pending_review', 'costed', 'quoted'].includes(q.state) && q.cost != null;
    const canRfq = ['submitted', 'rfq_sent', 'costed'].includes(q.state);
    const bench = q.vendor_benchmark_per_kg;
    // Flag an outlier rather than expecting the reviewer to notice it. 25% is loose enough
    // not to cry wolf on ordinary rate movement.
    const off = (bench && q.sell_per_kg) ? (q.sell_per_kg - bench) / bench : null;

    return `<div class="aqi-card">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px;">
        <div><div style="font-size:14px;font-weight:700;color:${DARK};">${esc(q.ref || '')}</div>
          <div class="aqi-s">${esc(q.week_label || q.week_start)} &middot; ${esc(q.vendor_raw)}</div></div>
        ${pill(q.state)}
      </div>

      <dl class="aqi-kv" style="margin-top:14px;">
        <dt>Cartons</dt><dd>${nf(q.cartons)}</dd>
        ${q.units ? `<dt>Units</dt><dd>${nf(q.units)}</dd>` : ''}
        <dt>Gross weight</dt><dd>${nf(q.gross_weight_kg)} kg</dd>
        <dt>Volume</dt><dd>${nf(q.cbm)} CBM</dd>
        <dt><b>Chargeable</b></dt><dd><b>${nf(q.chargeable_kg)} kg</b></dd>
        ${q.transit_label ? `<dt>Transit</dt><dd>${esc(q.transit_label)}</dd>` : ''}
        ${q.partner_name ? `<dt>Priced by</dt><dd>${esc(q.partner_name)}</dd>` : ''}
        ${q.cost_valid_until ? `<dt>Cost valid to</dt><dd>${esc(q.cost_valid_until)}</dd>` : ''}
        ${q.rfq_sent_at ? `<dt>Sent to partner</dt><dd>${esc(String(q.rfq_sent_at).slice(0, 10))}</dd>` : ''}
      </dl>
      ${q.client_note ? `<div style="font-size:11px;color:${MID};margin-top:8px;"><b>Client note:</b> ${esc(q.client_note)}</div>` : ''}
      ${q.partner_note ? `<div style="font-size:11px;color:${MID};margin-top:4px;"><b>Partner note:</b> ${esc(q.partner_note)}</div>` : ''}

      ${q.cost == null ? `
        <div class="aqi-calc">
          <div class="aqi-s">${q.rfq_sent_at
            ? `Submitted to the partner on <b>${esc(String(q.rfq_sent_at).slice(0, 10))}</b>. Awaiting their cost.`
            : 'Not yet submitted to the partner.'}</div>
        </div>
        ${canRfq ? `<button class="aqi-btn g" id="aqi-rfq" style="margin-top:14px;width:100%;">
          Reissue partner link</button>
          <div class="aqi-s" style="margin-top:5px;">Use if the link expired, bounced, or a second contact needs it.</div>` : ''}
        <div id="aqi-msg"></div>`
      : `
        <div class="aqi-calc">
          <dl class="aqi-kv">
            <dt>Partner cost</dt><dd>${money(q.cost, q.cost_currency)}</dd>
            <dt>Markup</dt><dd>${q.markup_pct}%${q.markup_source === 'override' ? ` <span style="color:${AMBER}">(override)</span>` : ''}</dd>
          </dl>
          <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-top:8px;">
            <div><div class="aqi-s">Sell price</div><div class="aqi-big">${money(q.sell_preview, q.currency)}</div></div>
            <div style="text-align:right;">
              <div class="aqi-s">Gross margin</div>
              <div style="font-size:15px;font-weight:700;color:${DARK};">${q.gross_margin_pct == null ? '—' : q.gross_margin_pct + '%'}</div>
            </div>
          </div>
          <div class="aqi-s" style="margin-top:8px;">
            ${q.sell_per_kg == null ? '' : `<b>${q.sell_per_kg.toFixed(2)}</b> per chargeable kg`}
            ${bench ? ` &middot; this vendor averages <b>${bench.toFixed(2)}</b> on approved quotes` : ''}
          </div>
          ${off != null && Math.abs(off) > 0.25
            ? `<div class="aqi-warn">${off > 0 ? 'Above' : 'Below'} this vendor's average by ${Math.abs(Math.round(off * 100))}% — worth a look before releasing.</div>` : ''}
        </div>

        ${canReview ? `
          <label class="aqi-lbl" for="aqi-markup">Markup % <span style="font-weight:400;color:${LIGHT}">(rate card: ${dflt}%)</span></label>
          <input class="aqi-in" id="aqi-markup" type="number" min="0" step="0.1" value="${q.markup_pct}">
          <label class="aqi-lbl" for="aqi-reason">Reason <span style="font-weight:400;color:${LIGHT}">(required if you change it)</span></label>
          <input class="aqi-in" id="aqi-reason" placeholder="Why this differs from the rate card" value="${esc(q.markup_reason || '')}">
          <label class="aqi-lbl" for="aqi-valid">Quote valid for (days)</label>
          <input class="aqi-in" id="aqi-valid" type="number" min="1" step="1" value="7">
          <div style="display:flex;gap:10px;margin-top:16px;">
            ${canRfq ? `<button class="aqi-btn g" id="aqi-rfq" style="flex:1;">Reissue link</button>` : ''}
            <button class="aqi-btn" id="aqi-release" style="flex:2;">
              ${q.state === 'quoted' ? 'Re-release to client' : 'Release to client'}</button>
          </div>
          ${q.state === 'quoted' ? `<div class="aqi-s" style="margin-top:6px;">Already with the client${q.valid_until ? ` until ${esc(q.valid_until)}` : ''}. Re-releasing replaces the price they see.</div>` : ''}
          <div id="aqi-msg"></div>` : ''}
      `}
      ${q.decided_at ? `<div class="aqi-s" style="margin-top:12px;">Client ${q.state} on ${esc(String(q.decided_at).slice(0, 10))}${q.decision_note ? ` — ${esc(q.decision_note)}` : ''}</div>` : ''}
    </div>`;
  }

  function wireDetail() {
    const msg = t => { const m = el('aqi-msg'); if (m) m.innerHTML = t; };
    const err = t => msg(`<div class="aqi-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(t)}</div>`);
    const ok = t => msg(`<div class="aqi-msg" style="background:rgba(27,127,59,.10);color:${GREEN};">${t}</div>`);

    const rfq = el('aqi-rfq');
    if (rfq) rfq.addEventListener('click', async () => {
      if (_busy) return; _busy = true; rfq.disabled = true;
      try {
        const r = await req('/air-quotes/' + encodeURIComponent(_sel) + '/rfq', { method: 'POST', body: '{}' });
        // The link is always returned, so the workflow still functions if email is not
        // configured — the reviewer can paste it into their own message.
        ok((r.resend_id ? `RFQ emailed to ${esc((r.emailed_to || []).join(', '))}.` : 'Link issued — email not sent.')
           + `<div style="margin-top:6px;word-break:break-all;font-family:ui-monospace,Menlo,monospace;font-size:10px;">${esc(r.link)}</div>`
           + (r.email_error ? `<div style="margin-top:4px;color:${RED};">${esc(r.email_error)}</div>` : ''));
        await load();
      } catch (e) { err(e.message || String(e)); rfq.disabled = false; }
      _busy = false;
    });

    const rel = el('aqi-release');
    if (rel) rel.addEventListener('click', async () => {
      if (_busy) return;
      const markup = Number(el('aqi-markup').value);
      const reason = el('aqi-reason').value.trim();
      const days = parseInt(el('aqi-valid').value, 10) || 7;
      if (!isFinite(markup) || markup < 0) return err('Enter a valid markup percentage.');
      if (Math.abs(markup - _data.markup_default_pct) > 0.0001 && !reason)
        return err('A reason is required when the markup differs from the rate card.');
      const q = (_data.quotes || []).find(x => x.id === _sel) || {};
      const sell = Math.round(q.cost * (1 + markup / 100) * 100) / 100;
      if (!confirm(`Release ${q.ref} to the client at ${money(sell, q.currency)}?\n\n`
        + `Cost ${money(q.cost, q.cost_currency)} · markup ${markup}%\n`
        + `The client sees the sell price only.`)) return;
      _busy = true; rel.disabled = true;
      try {
        const r = await req('/air-quotes/' + encodeURIComponent(_sel) + '/release',
          { method: 'POST', body: JSON.stringify({ markup_pct: markup, markup_reason: reason || null, valid_days: days }) });
        ok(`Released at ${esc(money(r.sell_amount, q.currency))} — valid until ${esc(r.valid_until)}, gross margin ${r.gross_margin_pct}%.`);
        await load();
      } catch (e) { err(e.message || String(e)); rel.disabled = false; }
      _busy = false;
    });
  }

  // ── load / gate ──
  async function load() {
    try { _data = await req('/air-quotes/internal'); _on = true; paintNav(); render(); }
    catch (e) {
      if (e.status === 403 || e.status === 401) { _on = false; const a = el('nav-aqi'); if (a) a.style.display = 'none'; }
    }
  }

  function close() {
    const o = document.querySelector('.aqi-ov'); if (o) o.remove();
    document.body.classList.remove('aqi-open');
  }

  function open() {
    styles();
    if (document.querySelector('.aqi-ov')) return;
    const o = document.createElement('div'); o.className = 'aqi-ov';
    o.innerHTML = `<div class="aqi-panel">
      <div class="aqi-head">
        <div><div class="aqi-t">Air quote review</div>
          <div class="aqi-s">Partner cost, margin and release &middot; internal only</div></div>
        <button class="aqi-x" id="aqi-close">&times;</button>
      </div>
      <div class="aqi-body" id="aqi-body">Loading…</div>
    </div>`;
    document.body.appendChild(o);
    document.body.classList.add('aqi-open');
    el('aqi-close').addEventListener('click', close);
    document.addEventListener('keydown', function k(ev) {
      if (ev.key === 'Escape') { close(); document.removeEventListener('keydown', k); }
    });
    load();
  }

  function init() {
    styles(); injectNav();
    window.openAirQuoteReview = open;
    load();
    window.addEventListener('state:ready', load);
    setInterval(load, 30000);
    if (location.hash === '#air-quote-review') setTimeout(open, 700);
    console.log('[air-quote-review] module v3 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
