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
  const _expanded = new Set();

  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const nf = n => Number(n || 0).toLocaleString();
  const money = (n, c) => n == null ? '—' : (c || 'USD') + ' ' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
      .aqi-tiles{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;}
      @media (max-width:1100px){ .aqi-tiles{grid-template-columns:repeat(2,minmax(0,1fr));} }
      @media (max-width:620px){ .aqi-tiles{grid-template-columns:1fr;} }
      .aqi-tile{border:.5px solid rgba(0,0,0,.09);border-radius:10px;padding:10px 12px;
                box-shadow:0 1px 2px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.06);}
      .aqi-tl{font-size:8px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;
              color:${LIGHT};line-height:1.25;}
      .aqi-tv{font-size:17px;font-weight:700;color:${DARK};letter-spacing:-.02em;margin-top:3px;
              font-variant-numeric:tabular-nums;line-height:1.15;}
      .aqi-ts{font-size:9px;color:${LIGHT};margin-top:2px;line-height:1.3;}
      .aqi-msg{margin-top:10px;padding:9px 11px;border-radius:8px;font-size:11px;}
      .aqi-warn{font-size:10px;color:${AMBER};margin-top:4px;}
    `;
    document.head.appendChild(s);
  }

  const ST = {
    submitted:      { t: 'Sending',            c: AMBER, b: 'rgba(183,121,31,.12)' },
    rfq_sent:       { t: 'Submitted to partner', c: MID, b: 'rgba(0,0,0,.05)' },
    repricing:      { t: 'Repricing',          c: AMBER, b: 'rgba(183,121,31,.12)' },
    costed:         { t: 'Costed',       c: BRAND, b: 'rgba(153,0,51,.10)' },
    pending_review: { t: 'Review',       c: BRAND, b: 'rgba(153,0,51,.10)' },
    quoted:         { t: 'With client',  c: MID,   b: 'rgba(0,0,0,.05)' },
    approved:       { t: 'Approved',     c: GREEN, b: 'rgba(27,127,59,.12)' },
    declined:       { t: 'Declined',     c: RED,   b: 'rgba(179,63,64,.12)' },
    expired:        { t: 'Expired',      c: LIGHT, b: 'rgba(0,0,0,.05)' },
  };
  const pill = st => { const s = ST[st] || ST.submitted;
    return `<span class="aqi-pill" style="color:${s.c};background:${s.b}">${s.t}</span>`; };

  // Distinguishes priced-but-not-sent from untouched. Without it a reviewer cannot tell
  // whether a quote is waiting on them or already handled and simply not released.
  const reviewPill = x => x.review_state === 'draft'
    ? `<span class="aqi-pill" style="color:${AMBER};background:rgba(183,121,31,.12);margin-left:4px;">Draft</span>`
    : (x.review_state === 'sent'
      ? `<span class="aqi-pill" style="color:${GREEN};background:rgba(27,127,59,.12);margin-left:4px;">Sent</span>` : '');

  const n2 = v => v == null ? '—' : Number(v).toFixed(2);

  // Sparkline in the tile corner. Deliberately no axes or labels — at this size it conveys
  // direction, and anything more detailed would be unreadable rather than informative.
  function spark(vals, colour) {
    const pts = (vals || []).filter(v => v != null && isFinite(v));
    if (pts.length < 2) return '';
    const W = 56, H = 18, min = Math.min(...pts), max = Math.max(...pts);
    const span = (max - min) || 1;
    const x = i => (i / (pts.length - 1)) * W;
    const y = v => H - ((v - min) / span) * (H - 2) - 1;
    const d = pts.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('');
    const last = pts[pts.length - 1], first = pts[0];
    const col = colour || (last >= first ? GREEN : RED);
    return `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" style="overflow:visible;">
      <path d="${d}" fill="none" stroke="${col}" stroke-width="1.4" stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="${x(pts.length - 1).toFixed(1)}" cy="${y(last).toFixed(1)}" r="1.9" fill="${col}"/>
    </svg>`;
  }

  function tilesHtml() {
    const t = _data.tiles; if (!t) return '';
    if (!t.decided_count) return `<div class="aqi-card" style="margin-bottom:16px;">
      <div class="aqi-sec">Performance</div>
      <div class="aqi-none">No decided quotes in the last ${t.window_weeks} weeks.</div></div>`;

    // Rising cost per kg is bad news, rising margin is good — so the colour follows meaning
    // rather than direction. A green line on a rising cost would read as a win.
    const card = (label, value, sub, series, invert) => {
      const pts = (series || []).filter(v => v != null);
      const up = pts.length > 1 && pts[pts.length - 1] >= pts[0];
      const col = pts.length > 1 ? ((up !== !!invert) ? GREEN : RED) : MID;
      return `<div class="aqi-tile">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;">
          <div class="aqi-tl">${esc(label)}</div>
          <div style="flex:0 0 auto;margin-top:-2px;">${spark(series, col)}</div>
        </div>
        <div class="aqi-tv">${value}</div>
        <div class="aqi-ts">${sub || ''}</div>
      </div>`;
    };
    const m = v => v == null ? '—' : 'USD ' + Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    return `<div class="aqi-card" style="margin-bottom:16px;">
      <div class="aqi-sec">Performance &middot; last ${t.window_weeks} weeks &middot; ${t.approved_count} approved</div>
      <div class="aqi-tiles">
        ${card('Per chargeable kg', n2(t.per_kg.value), 'sell / chargeable kg', t.per_kg.trend, true)}
        ${card('Per carton', n2(t.per_carton.value), 'sell / carton', t.per_carton.trend, true)}
        ${card('Per unit', t.per_unit.value == null ? 'NA' : n2(t.per_unit.value), 'sell / unit', t.per_unit.trend, true)}
        ${card('Approval rate', t.approval_rate.value == null ? '—' : t.approval_rate.value + '%',
               `${t.approved_count} of ${t.decided_count} decided`, t.approval_rate.trend, false)}
        ${card('Total cost', m(t.total_cost.value), 'partner cost, approved', t.total_cost.trend, true)}
        ${card('Avg margin %', t.margin.value == null ? '—' : t.margin.value + '%', 'approved only', t.margin.trend, false)}
        ${card('Total margin', m((t.margin_total || {}).value), `${t.approved_count} approved quotes`,
               (t.margin_total || {}).trend, false)}
        ${card('Margin per quote', m((t.margin_per_quote || {}).value), 'average, approved only',
               (t.margin_per_quote || {}).trend, false)}
      </div>
    </div>`;
  }

  // Cost and sell side by side, per line, both per kg and per unit — so an odd price shows
  // up as an odd rate rather than needing mental arithmetic.
  function linesHtml(x) {
    const ls = x.lines || [];
    if (!ls.length) return `<div class="aqi-s" style="padding:8px 2px;">No cost lines yet.</div>`;
    const tot = k => ls.reduce((a, l) => a + (l[k] || 0), 0);
    return `<table class="aqi" style="margin:4px 0 8px;background:#FAFAFB;">
      <thead><tr><th>Line</th>
        <th class="n">Cost</th><th class="n">Cost /kg</th><th class="n">Cost /unit</th>
        <th class="n">Markup</th>
        <th class="n">Sell</th><th class="n">Sell /kg</th><th class="n">Sell /unit</th></tr></thead>
      <tbody>${ls.map(l => `<tr>
        <td>${esc(l.label)}</td>
        <td class="n">${n2(l.cost_amount)}</td>
        <td class="n" style="color:${MID}">${n2(l.cost_per_kg)}</td>
        <td class="n" style="color:${MID}">${n2(l.cost_per_unit)}</td>
        <td class="n">${l.markup_pct == null ? '—' : l.markup_pct + '%'}</td>
        <td class="n"><b>${n2(l.sell_amount)}</b></td>
        <td class="n" style="color:${MID}">${n2(l.sell_per_kg)}</td>
        <td class="n" style="color:${MID}">${n2(l.sell_per_unit)}</td></tr>`).join('')}
        <tr style="font-weight:700;"><td>Total</td>
          <td class="n">${n2(tot('cost_amount'))}</td>
          <td class="n" style="color:${MID}">${n2(tot('cost_per_kg'))}</td>
          <td class="n" style="color:${MID}">${n2(tot('cost_per_unit'))}</td>
          <td class="n"></td>
          <td class="n">${n2(tot('sell_amount'))}</td>
          <td class="n" style="color:${MID}">${n2(tot('sell_per_kg'))}</td>
          <td class="n" style="color:${MID}">${n2(tot('sell_per_unit'))}</td></tr>
      </tbody></table>`;
  }

  // ── nav ──
  // This panel shows partner cost and margin. Three independent guards, because one of
  // them failing should not be enough to expose it:
  //   * server — /air-quotes/internal is requireRole(['admin'])
  //   * tenancy — nav gated on the quote_review capability
  //   * here    — never mount the nav for a client that lacks the capability
  // Entitlement follows the PERSON, not the client they happen to be viewing. A VelOzity
  // admin works as ICONIC or EHP through the picker, so a client-scoped capability would
  // lock them out of their own review screen.
  // Fails CLOSED. Returning null while unresolved meant an org whose identity never
  // resolved kept the nav — which is how partner logins reached this screen. Unknown is
  // treated as not entitled; the nav appears only once the server has confirmed internal.
  function entitled() { return window.pinpointIsInternal === true; }

  function injectNav() {
    if (!entitled()) { const a = el('nav-aqi'); if (a) a.remove(); return; }
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

    const ins = _data.insights || [], bands = _data.win_bands || [];
    body.innerHTML = `
    ${tilesHtml()}
    ${(ins.length || bands.length) ? `<div class="aqi-card" style="margin-bottom:16px;">
      <div class="aqi-sec">Pricing intelligence</div>
      ${ins.map(t => `<div style="font-size:12px;color:${DARK};line-height:1.55;margin-bottom:6px;">
        <span style="color:${BRAND};font-weight:700;">&bull;</span> ${esc(t)}</div>`).join('')}
      ${bands.length ? `<div style="display:grid;grid-template-columns:repeat(${bands.length},minmax(0,1fr));gap:10px;margin-top:10px;">
        ${bands.map(b => `<div style="border:.5px solid rgba(0,0,0,.09);border-radius:9px;padding:8px 10px;">
          <div style="font-size:8px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};">${esc(b.band)} /kg</div>
          <div style="font-size:16px;font-weight:700;color:${b.win_pct >= 70 ? GREEN : b.win_pct >= 40 ? AMBER : RED};margin-top:2px;">${b.win_pct}%</div>
          <div style="font-size:9px;color:${LIGHT};">won &middot; ${b.n} decided</div>
        </div>`).join('')}
      </div>
      <div class="aqi-s" style="margin-top:7px;">Bands are terciles of decided quotes. Internal only &mdash; never shown to the client or the partner.</div>` : ''}
    </div>` : ''}
    <div class="aqi-grid">
      <div class="aqi-card">
        <div class="aqi-sec">Queue &middot; ${list.length} quote(s)</div>
        ${list.length ? `<table class="aqi"><thead><tr>
            <th style="width:22px;"></th>
            <th>Ref</th><th>Week</th><th>Sent to partner</th><th>Vendor</th><th class="n">Chargeable kg</th>
            <th class="n">Cost</th><th class="n">Sell</th><th class="n">/kg</th><th class="n">/unit</th><th>Status</th>
          </tr></thead><tbody>${list.map(x => `<tr data-q="${esc(x.id)}" class="${x.id === _sel ? 'sel' : ''}">
            <td style="text-align:center;color:${LIGHT};" data-exp="${esc(x.id)}"
                title="Show the cost lines">${_expanded.has(x.id) ? '&#9662;' : '&#9656;'}</td>
            <td><b>${esc(x.ref || '')}</b></td>
            <td style="color:${MID}">${esc(x.week_label || x.week_start)}</td>
            <td style="color:${LIGHT};font-size:10px;">${x.rfq_sent_at ? esc(String(x.rfq_sent_at).slice(0, 10)) : '—'}</td>
            <td>${esc(x.vendor_raw)}</td>
            <td class="n">${nf(x.chargeable_kg)}</td>
            <td class="n">${x.cost == null ? '—' : money(x.cost, 'USD')}</td>
            <td class="n">${x.sell_preview == null ? '—' : money(x.sell_preview, 'USD')}</td>
            <td class="n">${x.sell_per_kg == null ? '—' : x.sell_per_kg.toFixed(2)}</td>
            <td class="n">${x.sell_per_unit == null
              ? `<span style="color:${LIGHT}">NA</span>` : x.sell_per_unit.toFixed(2)}</td>
            <td>${pill(x.state)}${reviewPill(x)}</td></tr>
            ${_expanded.has(x.id) ? `<tr class="aqi-exp"><td></td><td colspan="10">${linesHtml(x)}</td></tr>` : ''}`).join('')}</tbody></table>`
          : `<div class="aqi-none">No quote requests yet.</div>`}
      </div>
      <div id="aqi-detail">${detailHtml(q)}</div>
    </div>`;

    body.querySelectorAll('[data-exp]').forEach(t => t.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const id = t.getAttribute('data-exp');
      if (_expanded.has(id)) _expanded.delete(id); else _expanded.add(id);
      render();
    }));
    body.querySelectorAll('[data-q]').forEach(r => r.addEventListener('click', () => {
      _sel = r.getAttribute('data-q'); render();
    }));
    wireDetail();
  }

  function detailHtml(q) {
    if (!q) return `<div class="aqi-card"><div class="aqi-none">Select a quote.</div></div>`;
    const dflt = _data.markup_default_pct;
    const canReview = ['pending_review', 'costed', 'quoted', 'repricing'].includes(q.state) && q.cost != null;
    const canRfq = ['submitted', 'rfq_sent', 'costed', 'repricing'].includes(q.state);
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
        ${q.zendesk_ticket ? `<dt>Zendesk</dt><dd>${esc(q.zendesk_ticket)}</dd>` : ''}
        ${q.po_numbers ? `<dt>PO number(s)</dt><dd>${esc(q.po_numbers)}</dd>` : ''}
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
            <dt>Partner cost</dt><dd>${money(q.cost, 'USD')}</dd>
            <dt>Markup</dt><dd>${q.markup_pct}%${q.markup_source === 'override' ? ` <span style="color:${AMBER}">(override)</span>` : ''}</dd>
          </dl>
          <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-top:8px;">
            <div><div class="aqi-s">Sell price</div><div class="aqi-big">${money(q.sell_preview, 'USD')}</div></div>
            <div style="text-align:right;">
              <div class="aqi-s">Gross margin</div>
              <div style="font-size:15px;font-weight:700;color:${DARK};">${q.gross_margin_pct == null ? '—' : q.gross_margin_pct + '%'}</div>
            </div>
          </div>
          <div class="aqi-s" style="margin-top:8px;">
            ${q.sell_per_kg == null ? '' : `<b>${q.sell_per_kg.toFixed(2)}</b> per chargeable kg`}
            &middot; ${q.sell_per_unit == null ? '<b>NA</b> per unit (no unit count given)'
              : `<b>${q.sell_per_unit.toFixed(2)}</b> per unit`}
            ${bench ? `<br>This vendor averages <b>${bench.toFixed(2)}</b> per kg on approved quotes.` : ''}
          </div>
          ${off != null && Math.abs(off) > 0.25
            ? `<div class="aqi-warn">${off > 0 ? 'Above' : 'Below'} this vendor's average by ${Math.abs(Math.round(off * 100))}% — worth a look before releasing.</div>` : ''}
        </div>

        ${canReview ? `
          <div class="aqi-sec" style="margin:16px 0 6px;">Pricing <span style="font-weight:400;text-transform:none;letter-spacing:0;">&mdash; rate card ${dflt}%</span></div>
          <table class="aqi"><thead><tr><th>Line</th><th class="n">Cost</th>
            <th class="n" style="width:78px;">Markup %</th><th class="n" style="width:96px;">Sell</th></tr></thead>
          <tbody>${(q.lines || []).map(l => `<tr>
            <td style="font-size:11px;">${esc(l.label)}</td>
            <td class="n" style="color:${MID}">${n2(l.cost_amount)}</td>
            <td class="n"><input class="aqi-in aqi-pct" data-code="${esc(l.code)}" type="number" step="0.1" min="0"
                 value="${l.markup_pct == null ? dflt : l.markup_pct}" style="padding:5px 7px;font-size:12px;text-align:right;"></td>
            <td class="n"><input class="aqi-in aqi-sell" data-code="${esc(l.code)}" type="number" step="0.01" min="0"
                 value="${l.sell_amount == null ? '' : l.sell_amount}" style="padding:5px 7px;font-size:12px;text-align:right;"></td>
          </tr>`).join('')}
            <tr style="font-weight:700;"><td>Total</td>
              <td class="n" style="color:${MID}" id="aqi-tcost">${n2(q.cost)}</td>
              <td class="n" id="aqi-tpct" style="color:${MID};font-weight:400;font-size:10px;"></td>
              <td class="n"><input class="aqi-in" id="aqi-tsell" type="number" step="0.01" min="0"
                   style="padding:5px 7px;font-size:12px;text-align:right;font-weight:700;"></td></tr>
          </tbody></table>
          <div class="aqi-s" style="margin-top:4px;">Edit a markup or a sell figure on any line. Change the total and it distributes across the lines in proportion to their cost.</div>

          <label class="aqi-lbl" for="aqi-reason">Reason <span style="font-weight:400;color:${LIGHT}">(required if you change the rate card markup)</span></label>
          <input class="aqi-in" id="aqi-reason" placeholder="Why this differs from the rate card" value="${esc(q.markup_reason || '')}">
          <label class="aqi-lbl" for="aqi-valid">Quote valid for (days)</label>
          <input class="aqi-in" id="aqi-valid" type="number" min="1" step="1" value="7">
          ${(q.rounds || []).length ? `<div class="aqi-s" style="margin-top:8px;">
            Partner rounds: ${q.rounds.map(r => `<b>${r.round_no}</b> USD ${Number(r.total_cost).toFixed(2)}`).join(' &rarr; ')}
            ${q.rounds.length > 1 && q.cost != null ? (() => {
              const first = q.rounds[0].total_cost;
              const move = first > 0 ? Math.round((q.cost - first) / first * 1000) / 10 : null;
              return move == null ? '' : ` &rarr; now <b>${Number(q.cost).toFixed(2)}</b>
                <span style="color:${move < 0 ? GREEN : RED}">(${move > 0 ? '+' : ''}${move}%)</span>`;
            })() : ''}
          </div>` : ''}

          ${(q.messages || []).length ? `<div style="margin-top:12px;">
            <div class="aqi-sec" style="margin:0 0 6px;">Correspondence</div>
            ${q.messages.map(m => `<div style="padding:8px 10px;border-radius:8px;margin-bottom:6px;
                background:${m.author_role === 'internal' ? 'rgba(153,0,51,.05)' : '#F5F5F7'};">
              <div style="font-size:9px;color:${LIGHT};margin-bottom:2px;">
                ${m.author_role === 'internal' ? 'VelOzity' : esc(m.author || 'Partner')} &middot; ${esc(String(m.created_at).slice(0, 16))}</div>
              <div style="font-size:11px;color:${DARK};line-height:1.45;">${esc(m.body)}</div>
            </div>`).join('')}
          </div>` : ''}

          <label class="aqi-lbl" for="aqi-repcomment">Send back to partner <span style="font-weight:400;color:${LIGHT}">(what needs revisiting)</span></label>
          <textarea class="aqi-in" id="aqi-repcomment" rows="2"
            placeholder="e.g. Air freight looks high against the last two quotes on this lane"
            style="resize:vertical;font-family:inherit;"></textarea>
          <button class="aqi-btn g" id="aqi-reprice" style="width:100%;margin-top:8px;color:${AMBER};border-color:rgba(183,121,31,.35);">
            Request revised pricing</button>

          <div style="display:flex;gap:10px;margin-top:16px;">
            <button class="aqi-btn g" id="aqi-save" style="flex:1;">Save draft</button>
            <button class="aqi-btn" id="aqi-release" style="flex:2;">
              ${q.state === 'quoted' ? 'Re-release to client' : 'Release to client'}</button>
          </div>
          ${canRfq ? `<button class="aqi-btn g" id="aqi-rfq" style="width:100%;margin-top:8px;">Reissue partner link</button>` : ''}
          ${q.state === 'quoted' ? `<div class="aqi-s" style="margin-top:6px;">Already with the client${q.valid_until ? ` until ${esc(q.valid_until)}` : ''}. Re-releasing replaces the price they see.</div>` : ''}
          <div id="aqi-msg"></div>` : ''}
      `}
      ${['approved', 'declined'].includes(q.state) ? `
        <div style="margin-top:16px;padding-top:14px;border-top:.5px solid rgba(0,0,0,.08);">
          <button class="aqi-btn g" id="aqi-purge" style="width:100%;color:${RED};border-color:rgba(179,63,64,.3);">
            Delete this quote</button>
          <div class="aqi-s" style="margin-top:5px;">Removes an agreed price and its history. Password required.</div>
          <div id="aqi-pmsg"></div>
        </div>` : ''}
      <div class="aqi-s" style="margin-top:12px;">
        Requested by ${esc(q.created_by_email || q.created_by_name || 'unknown')}
        on ${esc(String(q.created_at || '').slice(0, 10))}
        ${q.decided_at ? `<br>${q.state === 'approved' ? 'Approved' : q.state === 'declined' ? 'Declined' : q.state}
          by ${esc(q.decided_by_email || q.decided_by_name || 'unknown')}
          on ${esc(String(q.decided_at).slice(0, 10))}
          <span style="color:${LIGHT}">(${esc(q.decided_via === 'email' ? 'email link' : 'Pinpoint')})</span>
          ${q.decision_note ? `<br>&ldquo;${esc(q.decision_note)}&rdquo;` : ''}` : ''}
      </div>
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

    const purge = el('aqi-purge');
    if (purge) purge.addEventListener('click', async () => {
      const q = (_data.quotes || []).find(x => x.id === _sel) || {};
      if (!confirm(`Delete ${q.ref}?\n\n${q.vendor_raw || ''} · ${q.week_label || q.week_start || ''}\n`
        + `This quote was ${q.state}. Its price, partner cost and full history will be removed permanently.`)) return;
      const pw = prompt('Enter the deletion password:');
      if (!pw) return;
      purge.disabled = true;
      try {
        await req('/air-quotes/' + encodeURIComponent(_sel) + '/purge',
          { method: 'POST', body: JSON.stringify({ password: pw }) });
        _sel = null; await load();
      } catch (e) {
        const m = el('aqi-pmsg');
        const t = e.message === 'bad_password' ? 'Incorrect password.' : (e.message || String(e));
        if (m) m.innerHTML = `<div class="aqi-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(t)}</div>`;
        purge.disabled = false;
      }
    });

    // Two-way binding, resolved here so the reviewer always sees the same arithmetic the
    // server will apply on save. _pricingMode records which field they last drove, because
    // the server needs to know whether to treat this as line edits or a total to distribute.
    let _pricingMode = 'lines';
    const costOf = code => ((_data.quotes || []).find(x => x.id === _sel)?.lines || [])
      .find(l => l.code === code)?.cost_amount || 0;

    function syncTotals() {
      const sells = [...document.querySelectorAll('.aqi-sell')];
      const total = sells.reduce((a, i) => a + (parseFloat(i.value) || 0), 0);
      const cost = parseFloat((el('aqi-tcost') || {}).textContent) || 0;
      const t = el('aqi-tsell'); if (t && document.activeElement !== t) t.value = total ? total.toFixed(2) : '';
      const p = el('aqi-tpct');
      if (p) p.textContent = cost > 0 && total ? (Math.round((total - cost) / cost * 1000) / 10) + '%' : '';
    }

    document.querySelectorAll('.aqi-pct').forEach(inp => inp.addEventListener('input', () => {
      _pricingMode = 'lines';
      const code = inp.getAttribute('data-code');
      const pct = parseFloat(inp.value);
      const sell = document.querySelector(`.aqi-sell[data-code="${code}"]`);
      if (sell && isFinite(pct)) sell.value = (costOf(code) * (1 + pct / 100)).toFixed(2);
      syncTotals();
    }));

    document.querySelectorAll('.aqi-sell').forEach(inp => inp.addEventListener('input', () => {
      _pricingMode = 'lines';
      const code = inp.getAttribute('data-code');
      const sell = parseFloat(inp.value);
      const pct = document.querySelector(`.aqi-pct[data-code="${code}"]`);
      const c = costOf(code);
      if (pct && isFinite(sell) && c > 0) pct.value = (Math.round((sell - c) / c * 1000) / 10);
      syncTotals();
    }));

    const tsell = el('aqi-tsell');
    if (tsell) tsell.addEventListener('input', () => {
      _pricingMode = 'total';
      // Preview the distribution so the lines on screen match what will be saved.
      const target = parseFloat(tsell.value);
      const cost = parseFloat((el('aqi-tcost') || {}).textContent) || 0;
      if (!isFinite(target) || !(cost > 0)) return;
      let allocated = 0;
      const rows = [...document.querySelectorAll('.aqi-sell')].map(i => {
        const c = costOf(i.getAttribute('data-code'));
        const v = Math.round(target * (c / cost) * 100) / 100;
        allocated += v; return { i, c, v };
      });
      const drift = Math.round((target - allocated) * 100) / 100;
      if (drift !== 0 && rows.length) {
        const big = rows.reduce((m, x) => (x.c > m.c ? x : m), rows[0]);
        big.v = Math.round((big.v + drift) * 100) / 100;
      }
      rows.forEach(r => {
        r.i.value = r.v.toFixed(2);
        const p = document.querySelector(`.aqi-pct[data-code="${r.i.getAttribute('data-code')}"]`);
        if (p && r.c > 0) p.value = Math.round((r.v - r.c) / r.c * 1000) / 10;
      });
      const pEl = el('aqi-tpct');
      if (pEl) pEl.textContent = (Math.round((target - cost) / cost * 1000) / 10) + '%';
    });

    syncTotals();

    function pricingPayload() {
      if (_pricingMode === 'total') {
        const t = parseFloat((el('aqi-tsell') || {}).value);
        if (isFinite(t)) return { total_sell: t };
      }
      return { lines: [...document.querySelectorAll('.aqi-sell')].map(i => ({
        code: i.getAttribute('data-code'),
        sell_amount: i.value === '' ? null : parseFloat(i.value),
      })) };
    }

    const save = el('aqi-save');
    if (save) save.addEventListener('click', async () => {
      if (_busy) return; _busy = true; save.disabled = true;
      try {
        const r = await req('/air-quotes/' + encodeURIComponent(_sel) + '/pricing',
          { method: 'POST', body: JSON.stringify(pricingPayload()) });
        ok(`Saved as draft — total ${esc(money(r.sell, 'USD'))}. Nothing has been sent to the client.`);
        await load();
      } catch (e) { err(e.message || String(e)); save.disabled = false; }
      _busy = false;
    });

    const rep = el('aqi-reprice');
    if (rep) rep.addEventListener('click', async () => {
      if (_busy) return;
      const comment = (el('aqi-repcomment') || {}).value?.trim();
      if (!comment) return err('Say what needs revisiting — a bare request to reprice wastes a round.');
      const q = (_data.quotes || []).find(x => x.id === _sel) || {};
      if (!confirm(`Send ${q.ref} back to the partner?\n\nTheir current figures are kept for comparison, `
        + `and they will receive your comment with a fresh link.\nThe client sees no change.`)) return;
      _busy = true; rep.disabled = true;
      try {
        const r = await req('/air-quotes/' + encodeURIComponent(_sel) + '/reprice',
          { method: 'POST', body: JSON.stringify({ comment }) });
        ok(`Sent back to the partner (round ${r.round}).`
          + (r.resend_id ? ` Emailed to ${esc((r.emailed_to || []).join(', '))}.` : ' Email not configured — link below.')
          + `<div style="margin-top:6px;word-break:break-all;font-family:ui-monospace,Menlo,monospace;font-size:10px;">${esc(r.link)}</div>`);
        await load();
      } catch (e) { err(e.message || String(e)); rep.disabled = false; }
      _busy = false;
    });

    const rel = el('aqi-release');
    if (rel) rel.addEventListener('click', async () => {
      if (_busy) return;
      const reason = el('aqi-reason').value.trim();
      const days = parseInt(el('aqi-valid').value, 10) || 7;
      const q = (_data.quotes || []).find(x => x.id === _sel) || {};
      const total = parseFloat((el('aqi-tsell') || {}).value);
      if (!isFinite(total) || total <= 0) return err('Price the lines before releasing.');
      const dflt = _data.markup_default_pct;
      const eff = q.cost > 0 ? Math.round((total - q.cost) / q.cost * 1000) / 10 : null;
      if (eff != null && Math.abs(eff - dflt) > 0.05 && !reason)
        return err('A reason is required when the effective markup differs from the rate card.');
      if (!confirm(`Release ${q.ref} to the client at ${money(total, 'USD')}?\n\n`
        + `Cost ${money(q.cost, 'USD')} · effective markup ${eff}%\n`
        + `The client sees the total only — never the three lines.`)) return;
      _busy = true; rel.disabled = true;
      try {
        // Persist what is on screen first, so the released price is exactly what was seen.
        await req('/air-quotes/' + encodeURIComponent(_sel) + '/pricing',
          { method: 'POST', body: JSON.stringify(pricingPayload()) });
        const r = await req('/air-quotes/' + encodeURIComponent(_sel) + '/release',
          { method: 'POST', body: JSON.stringify({ markup_pct: eff, markup_reason: reason || null, valid_days: days }) });
        ok(`Released at ${esc(money(r.sell_amount, 'USD'))} — valid until ${esc(r.valid_until)}, gross margin ${r.gross_margin_pct}%.`);
        await load();
      } catch (e) { err(e.message || String(e)); rel.disabled = false; }
      _busy = false;
    });
  }

  // ── load / gate ──
  async function load() {
    if (!entitled()) {
      _on = false; const a = el('nav-aqi'); if (a) a.remove();
      const o = document.querySelector('.aqi-ov'); if (o) o.remove();
      return;
    }
    try { _data = await req('/air-quotes/internal'); _on = true; injectNav(); paintNav(); render(); }
    catch (e) {
      if (e.status === 403 || e.status === 401) {
        _on = false; const a = el('nav-aqi'); if (a) a.style.display = 'none';
        return;
      }
      // Anything else is a real failure. Say so rather than leaving a spinner running.
      const body = el('aqi-body');
      if (body) body.innerHTML = `<div class="aqi-card"><div class="aqi-sec">Could not load</div>
        <div style="font-size:12px;color:${RED};">${esc(e.message || String(e))}</div>
        <div class="aqi-s" style="margin-top:6px;">Status ${esc(String(e.status || 'unknown'))}. Try again, or send this message on if it persists.</div>
        <button class="aqi-btn g" id="aqi-retry" style="margin-top:12px;">Retry</button></div>`;
      const r = el('aqi-retry');
      if (r) r.addEventListener('click', () => { const b = el('aqi-body'); if (b) b.textContent = 'Loading…'; load(); });
    }
  }

  function close() {
    const o = document.querySelector('.aqi-ov'); if (o) o.remove();
    document.body.classList.remove('aqi-open');
  }

  function open() {
    if (!entitled()) return;
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
    // entitled() is false until tenancy resolves, so the nav cannot be created on the first
    // pass. This must therefore ADD as well as remove — a gate that only ever removes would
    // leave internal staff with no Quote Review at all once identity arrives late.
    setInterval(() => {
      if (entitled()) { injectNav(); paintNav(); }
      else { const a = el('nav-aqi'); if (a) a.remove(); }
    }, 1500);
    window.openAirQuoteReview = open;
    load();
    window.addEventListener('state:ready', load);
    window.addEventListener('tenancy:ready', () => { if (entitled()) { injectNav(); load(); } });
    // Only poll while the panel is actually open and the tab is visible. A background
    // reload every 30 seconds redrew the whole queue whether or not anything had changed,
    // which is what made the app look like it was refreshing itself.
    setInterval(() => {
      if (document.visibilityState !== 'visible') return;
      if (!document.querySelector('.aqi-ov')) return;
      if (_busy) return;                       // never redraw under someone's cursor mid-edit
      if (document.activeElement && /INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName)) return;
      load();
    }, 30000);
    if (location.hash === '#air-quote-review') setTimeout(open, 700);
    console.log('[air-quote-review] module v17 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
