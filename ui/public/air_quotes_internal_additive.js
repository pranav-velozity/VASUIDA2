/* ── VelOzity Pinpoint — Air Quote Review (internal) v2 ──
   v2.1: third tab, Requests — door-to-door request review lives here (rq_review_additive.js), never on the client page.
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

  let _on = false, _data = null, _sel = null, _busy = false, _q = '';
  let _weeks = null, _wkAnchor = null, _wkBusy = false;   // the four-week revenue strip
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
      /* Dimmed and struck through, so a replaced quote reads as history at a glance rather
         than needing its pill read first. The status cell keeps its pills legible. */
      table.aqi tr.superseded td{opacity:.5;text-decoration:line-through;}
      table.aqi tr.superseded td:last-child{text-decoration:none;opacity:.8;}
      table.aqi tr.superseded td:first-child{text-decoration:none;}
      .aqi-btn{border:0;border-radius:9px;background:${BRAND};color:#fff;font:600 12px inherit;
               padding:10px 16px;cursor:pointer;}
      .aqi-btn.g{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);}
      .aqi-btn:disabled{opacity:.55;cursor:default;}
      .aqi-none{font-size:11px;color:${LIGHT};padding:16px 2px;}
      .aqi-tabs{display:inline-flex;border:.5px solid rgba(0,0,0,.14);border-radius:8px;overflow:hidden;}
      .aqi-tabs button{border:0;background:#fff;color:${MID};font:600 11px inherit;padding:6px 14px;cursor:pointer;}
      .aqi-tabs button.on{background:${DARK};color:#fff;}
      .aqi-wk{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;}
      @media (max-width:1100px){ .aqi-wk{grid-template-columns:repeat(2,minmax(0,1fr));} }
      .aqi-wkt{border:.5px solid rgba(0,0,0,.09);border-radius:10px;padding:10px 12px;background:#fff;}
      .aqi-wkt.now{border-color:${BRAND};box-shadow:inset 0 0 0 .5px ${BRAND};}
      .aqi-wkh{display:flex;justify-content:space-between;align-items:baseline;gap:8px;}
      .aqi-wkw{font-size:13px;font-weight:700;color:${DARK};letter-spacing:-.01em;}
      .aqi-wkr{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .aqi-wkt.now .aqi-wkr{color:${BRAND};}
      .aqi-wkd{font-size:10px;color:${LIGHT};margin-top:1px;}
      .aqi-wkv{font-size:17px;font-weight:700;color:${DARK};letter-spacing:-.02em;margin-top:8px;}
      .aqi-wkl{font-size:8px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .aqi-wkrow{display:flex;justify-content:space-between;align-items:baseline;gap:8px;margin-top:6px;font-size:11.5px;color:${DARK};}
      .aqi-wkrow .k{font-size:10px;color:${MID};}
      .aqi-wkp{font-size:10.5px;color:${MID};margin-top:7px;padding-top:6px;border-top:.5px dashed rgba(0,0,0,.1);}
      .aqi-wknav{display:flex;align-items:center;gap:6px;}
      .aqi-wkb{height:26px;min-width:26px;padding:0 8px;border-radius:7px;border:.5px solid rgba(0,0,0,.15);background:#fff;
               font-size:11.5px;font-weight:500;color:${DARK};cursor:pointer;font-family:inherit;}
      .aqi-wkb:disabled{opacity:.4;cursor:default;}
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
  // Only meaningful BEFORE a decision. review_state is 'draft' whenever pricing was ever
  // saved, and that stays true after approval — so an approved quote was showing "Approved"
  // and "Draft" side by side, which reads as a contradiction.
  const reviewPill = (x) => {
    if (['approved', 'declined', 'expired'].includes(x.state)) return '';
    if (x.review_state === 'draft')
      return `<span class="aqi-pill" style="color:${AMBER};background:rgba(183,121,31,.12);margin-left:4px;">Draft</span>`;
    if (x.review_state === 'sent')
      return `<span class="aqi-pill" style="color:${GREEN};background:rgba(27,127,59,.12);margin-left:4px;">Sent</span>`;
    return '';
  };

  // A quote that has been revised is history, not work in progress.
  const supersededPill = (x) => x.superseded_by
    ? `<span class="aqi-pill" style="color:${LIGHT};background:rgba(0,0,0,.05);margin-left:4px;"
             title="Replaced by ${esc(x.superseded_by)}">Superseded</span>` : '';

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
  // Same field set as the client screen, plus the internal figures — someone chasing a
  // margin query has a cost or a sell price to hand as often as a reference.
  function aqiMatches(x, needle) {
    if (!needle) return true;
    const hay = [x.ref, x.zendesk_ticket, x.po_numbers, x.vendor, x.week_label, x.week_start,
                 x.state, x.transit_mode, x.partner_name, x.priced_by, x.client_note,
                 x.chargeable_kg, x.cost, x.sell, x.per_kg, x.per_unit]
      .filter(v => v != null).join(' ').toLowerCase();
    return needle.toLowerCase().split(/\s+/).filter(Boolean).every(t => hay.indexOf(t) >= 0);
  }

  // ── Four weeks of air: last, this, next two. Revenue, cost, margin; approved only, with
  // what is still awaiting a decision shown beneath rather than added in. ◀ ▶ walk the window
  // a week at a time; "This week" brings it back.

  const money0 = (v) => v == null ? '—' : 'USD ' + Math.round(Number(v)).toLocaleString();
  const wkDay = (d) => new Date(d + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  function weeksHtml() {
    const d = _weeks;
    const nav = `<div class="aqi-wknav">
      <button class="aqi-wkb" data-wk="-1" title="Earlier" ${_wkBusy ? 'disabled' : ''}>&#8249;</button>
      <button class="aqi-wkb" data-wk="0" ${_wkBusy || !d || d.anchor === addDays(d.this_week, -7) ? 'disabled' : ''}>This week</button>
      <button class="aqi-wkb" data-wk="1" title="Later" ${_wkBusy ? 'disabled' : ''}>&#8250;</button>
    </div>`;
    const head = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <div class="aqi-sec" style="margin:0;">Air by execution week &middot; approved quotes</div>${nav}</div>`;
    if (!d) return `<div class="aqi-card" style="margin-bottom:16px;">${head}<div class="aqi-none">Loading…</div></div>`;
    const tile = (w) => {
      const a = w.approved, p = w.pending;
      const rel = w.rel === 0 ? 'This week' : w.rel === -1 ? 'Last week' : w.rel === 1 ? 'Next week' : w.rel < 0 ? `${-w.rel} weeks ago` : `In ${w.rel} weeks`;
      const mcol = a.margin_pct == null ? DARK : a.margin_pct >= 20 ? '#1B7F3B' : a.margin_pct >= 10 ? DARK : BRAND;
      return `<div class="aqi-wkt${w.rel === 0 ? ' now' : ''}">
        <div class="aqi-wkh"><span class="aqi-wkw">W${w.iso_week}</span><span class="aqi-wkr">${rel}</span></div>
        <div class="aqi-wkd">w/c ${wkDay(w.week_start)} &middot; ${a.n} approved${w.declined ? ` &middot; ${w.declined} declined` : ''}</div>
        <div class="aqi-wkl" style="margin-top:9px;">Revenue</div>
        <div class="aqi-wkv" style="margin-top:1px;">${a.n ? money0(a.revenue) : '<span style="color:' + LIGHT + ';font-weight:500;">nothing approved</span>'}</div>
        <div class="aqi-wkrow"><span class="k">Cost</span><span>${a.n ? money0(a.cost) : '—'}</span></div>
        <div class="aqi-wkrow"><span class="k">Margin</span><span style="font-weight:700;color:${mcol};">${a.n ? money0(a.margin) : '—'}${a.margin_pct != null ? ` <span style="font-weight:500;color:${MID};">&middot; ${a.margin_pct}%</span>` : ''}</span></div>
        ${p.n ? `<div class="aqi-wkp">+ ${p.n} awaiting decision &middot; ${money0(p.revenue)} if approved</div>` : ''}
      </div>`;
    };
    return `<div class="aqi-card" style="margin-bottom:16px;">${head}<div class="aqi-wk">${d.weeks.map(tile).join('')}</div></div>`;
  }
  function addDays(d, n) { const x = new Date(d + 'T00:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); }
  async function loadWeeks(anchor) {
    _wkBusy = true; paintWeeks();
    try { _weeks = await req('/air-quotes/internal/weeks' + (anchor ? '?anchor=' + encodeURIComponent(anchor) : '')); _wkAnchor = _weeks.anchor; }
    catch (e) { _weeks = null; }
    _wkBusy = false; paintWeeks();
  }
  // Repaint only the strip, so walking the weeks never disturbs the queue or the selection.
  function paintWeeks() {
    const host = el('aqi-weeks'); if (!host) return;
    host.innerHTML = weeksHtml();
    host.querySelectorAll('[data-wk]').forEach(b => b.addEventListener('click', () => {
      const k = Number(b.getAttribute('data-wk'));
      if (k === 0) return loadWeeks(null);
      loadWeeks(addDays(_wkAnchor || (_weeks && _weeks.anchor) || new Date().toISOString().slice(0, 10), k * 7));
    }));
  }

  function render() {
    const body = el('aqi-body'); if (!body || !_data) return;
    const all = _data.quotes || [];
    const list = _q ? all.filter(x => aqiMatches(x, _q)) : all;
    const q = list.find(x => x.id === _sel) || list.find(x => x.state === 'pending_review' || x.state === 'costed') || list[0] || null;
    // Falls back to the first match when the selected quote is filtered out, rather than
    // leaving the panel describing a row nobody can see.
    _sel = q ? q.id : null;

    const ins = _data.insights || [], bands = _data.win_bands || [];
    body.innerHTML = `
    ${tilesHtml()}
    <div id="aqi-weeks">${weeksHtml()}</div>
    <div id="aqi-intel">${intelFallbackHtml(ins, bands)}</div>
    <div class="aqi-grid">
      <div class="aqi-card">
        <div class="aqi-sec">Queue &middot; ${_q ? `${list.length} of ${all.length}` : list.length} quote(s)</div>
        <div style="display:flex;gap:9px;align-items:center;margin:0 0 9px;">
          <input id="aqi-search" placeholder="Search ref, Zendesk, PO, vendor, cost, sell…"
                 value="${esc(_q)}" autocomplete="off"
                 style="flex:1;min-width:200px;padding:7px 10px;border:.5px solid rgba(0,0,0,.14);
                        border-radius:8px;font:inherit;font-size:12px;">
          ${_q ? `<button class="aqi-btn g" id="aqi-clear" style="padding:6px 11px;">Clear</button>` : ''}
        </div>
        ${list.length ? `<table class="aqi"><thead><tr>
            <th style="width:22px;"></th>
            <th>Ref</th><th>Week</th><th>Sent to partner</th><th>Vendor</th><th class="n">Chargeable kg</th>
            <th class="n">Cost</th><th class="n">Sell</th><th class="n">/kg</th><th class="n">/unit</th><th>Status</th>
          </tr></thead><tbody>${list.map(x => `<tr data-q="${esc(x.id)}" class="${x.id === _sel ? 'sel' : ''}${x.superseded_by ? ' superseded' : ''}">
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
            <td>${pill(x.state)}${reviewPill(x)}${supersededPill(x)}</td></tr>
            ${_expanded.has(x.id) ? `<tr class="aqi-exp"><td></td><td colspan="10">${linesHtml(x)}</td></tr>` : ''}`).join('')}</tbody></table>`
          : `<div class="aqi-none">No quote requests yet.</div>`}
      </div>
      <div id="aqi-detail">${detailHtml(q)}</div>
    </div>`;

    paintIntel(q); loadIntel(q);
    body.querySelectorAll('[data-exp]').forEach(t => t.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const id = t.getAttribute('data-exp');
      if (_expanded.has(id)) _expanded.delete(id); else _expanded.add(id);
      render();
    }));
    const si = el('aqi-search');
    if (si) {
      let t = null;
      si.addEventListener('input', () => {
        clearTimeout(t);
        t = setTimeout(() => { _q = si.value.trim(); render(); }, 200);
      });
      // render() rebuilds the panel, so focus and caret must be restored or the box is
      // unusable after one character.
      si.focus();
      si.setSelectionRange(si.value.length, si.value.length);
    }
    el('aqi-clear')?.addEventListener('click', () => { _q = ''; render(); });
    paintWeeks();
    if (!_weeks && !_wkBusy) loadWeeks(null);

    body.querySelectorAll('[data-q]').forEach(r => r.addEventListener('click', () => {
      _sel = r.getAttribute('data-q'); render();
    }));
    wireDetail();
  }

  // ── Pricing intelligence ──
  // The numbers come from /air-quotes/:id/intel (arithmetic over decided quotes, no model);
  // Pulse explains them on request. The old bullet list stays as the fallback when the
  // selected quote has no partner cost yet.
  const _intel = {};          // quote id → { key, data }
  function intelFallbackHtml(ins, bands) {
    if (!ins.length && !bands.length) return '';
    return `<div class="aqi-card" style="margin-bottom:16px;">
      <div class="aqi-sec">Pricing intelligence</div>
      ${ins.map(t => `<div style="font-size:12px;color:${DARK};line-height:1.55;margin-bottom:6px;"><span style="color:${BRAND};font-weight:700;">&bull;</span> ${esc(t)}</div>`).join('')}
      ${bands.length ? `<div style="display:grid;grid-template-columns:repeat(${bands.length},minmax(0,1fr));gap:10px;margin-top:10px;">
        ${bands.map(b => `<div style="border:.5px solid rgba(0,0,0,.09);border-radius:9px;padding:8px 10px;">
          <div style="font-size:8px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};">${esc(b.band)} /kg</div>
          <div style="font-size:16px;font-weight:700;color:${b.win_pct >= 70 ? GREEN : b.win_pct >= 40 ? AMBER : RED};margin-top:2px;">${b.win_pct}%</div>
          <div style="font-size:9px;color:${LIGHT};">won &middot; ${b.n} decided</div></div>`).join('')}</div>
      <div class="aqi-s" style="margin-top:7px;">Bands are terciles of decided quotes. Internal only &mdash; never shown to the client or the partner.</div>` : ''}
    </div>`;
  }
  async function loadIntel(q) {
    if (!q || q.cost == null) return;
    const key = q.id + '|' + q.cost + '|' + (q.updated_at || '') + '|' + q.state;
    if (_intel[q.id] && _intel[q.id].key === key) return;
    try { const r = await req('/air-quotes/' + encodeURIComponent(q.id) + '/intel'); _intel[q.id] = { key, data: r.intel }; }
    catch (e) { _intel[q.id] = { key, data: null, error: e.message || String(e) }; }
    if (_sel === q.id) paintIntel(q);
  }
  const pct = (n, sign) => n == null ? '—' : ((sign && n > 0 ? '+' : '') + n + '%');
  function intelHtml(q) {
    const rec = _intel[q.id]; if (!rec) return `<div class="aqi-card" style="margin-bottom:16px;"><div class="aqi-sec">Pricing intelligence &middot; ${esc(q.ref || '')}</div><div class="aqi-s">Working it out…</div></div>`;
    if (!rec.data) return `<div class="aqi-card" style="margin-bottom:16px;"><div class="aqi-sec">Pricing intelligence</div><div style="font-size:12px;color:${RED};">${esc(rec.error || 'unavailable')}</div></div>`;
    const i = rec.data, p = i.partner, b = i.benchmarks, wc = i.win_curve;
    const kindCol = i.kind === 'opportunity' ? GREEN : i.kind === 'squeeze' ? AMBER : LIGHT;
    const conf = wc.confidence === 'ok' ? `<span style="color:${GREEN};">win curve on ${wc.decided} decisions</span>` : wc.confidence === 'thin' ? `<span style="color:${AMBER};">thin — ${wc.decided} decisions, treat win % as a hint</span>` : `<span style="color:${LIGHT};">${wc.decided} decisions — benchmarks only, no win estimate yet</span>`;
    const canApply = ['pending_review', 'costed', 'repricing', 'submitted', 'rfq_sent'].includes(q.state) && q.cost != null;
    // partner range bar: min … median … max with today's cost marked
    let bar = '';
    if (p.n >= 2 && p.min_kg != null && p.max_kg > p.min_kg) {
      const pos = (v) => Math.max(0, Math.min(100, (v - p.min_kg) / (p.max_kg - p.min_kg) * 100));
      bar = `<div style="position:relative;height:26px;margin:8px 0 2px;">
        <div style="position:absolute;left:0;right:0;top:11px;height:4px;border-radius:2px;background:rgba(0,0,0,.08);"></div>
        <div style="position:absolute;left:${pos(p.median_kg)}%;top:6px;width:1px;height:14px;background:${MID};" title="median ${p.median_kg}/kg"></div>
        ${p.cost_kg != null ? `<div style="position:absolute;left:calc(${pos(p.cost_kg)}% - 6px);top:7px;width:12px;height:12px;border-radius:50%;background:${i.kind === 'opportunity' ? GREEN : i.kind === 'squeeze' ? AMBER : BRAND};box-shadow:0 0 0 3px rgba(255,255,255,.9);" title="this quote ${p.cost_kg}/kg"></div>` : ''}
        <div style="position:absolute;left:0;top:18px;font-size:9px;color:${LIGHT};">${p.min_kg}</div>
        <div style="position:absolute;left:${pos(p.median_kg)}%;top:18px;transform:translateX(-50%);font-size:9px;color:${MID};">med ${p.median_kg}</div>
        <div style="position:absolute;right:0;top:18px;font-size:9px;color:${LIGHT};">${p.max_kg}</div>
      </div>`;
    }
    const chip = (label, v, sub) => `<div style="border:.5px solid rgba(0,0,0,.09);border-radius:9px;padding:7px 10px;min-width:0;">
      <div style="font-size:8px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${label}</div>
      <div style="font-size:14px;font-weight:700;color:${DARK};margin-top:1px;">${v == null ? '—' : v}</div>${sub ? `<div style="font-size:9px;color:${LIGHT};">${sub}</div>` : ''}</div>`;
    const opt = (o) => {
      const isRec = o.key === i.recommended;
      return `<div style="border:${isRec ? '1.5px solid ' + BRAND : '.5px solid rgba(0,0,0,.1)'};border-radius:11px;padding:10px 12px;position:relative;background:${isRec ? '#FFF7F9' : '#fff'};">
        ${isRec ? `<div style="position:absolute;top:-7px;left:10px;background:${BRAND};color:#fff;font-size:8px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:2px 7px;border-radius:7px;">Recommended</div>` : ''}
        <div style="font-size:11px;font-weight:700;color:${DARK};">${esc(o.label)}</div>
        <div style="font-size:10px;color:${MID};margin-bottom:6px;min-height:13px;">${esc(o.why || '')}</div>
        <div style="font-size:16px;font-weight:700;color:${DARK};">${money(o.sell, 'USD')}</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;font-size:10.5px;color:${MID};margin-top:4px;">
          <span><b style="color:${DARK}">${o.markup_pct}%</b> markup</span>
          <span><b style="color:${o.gross_margin_pct >= 25 ? GREEN : o.gross_margin_pct >= 12 ? DARK : RED}">${o.gross_margin_pct}%</b> GM &middot; ${money(o.margin, 'USD')}</span>
          <span><b style="color:${DARK}">${o.sell_kg ?? '—'}</b>/kg${o.vs_anchor_pct != null ? ` <span style="color:${o.vs_anchor_pct > 10 ? AMBER : LIGHT}">(${pct(o.vs_anchor_pct, true)} vs usual)</span>` : ''}</span>
          ${o.sell_unit != null ? `<span><b style="color:${DARK}">${o.sell_unit}</b>/unit</span>` : ''}
          ${o.win_pct != null ? `<span><b style="color:${o.win_pct >= 70 ? GREEN : o.win_pct >= 40 ? AMBER : RED}">${o.win_pct}%</b> likely &middot; EV ${money(o.expected_margin, 'USD')}</span>` : ''}
        </div>
        ${o.over_ceiling ? `<div style="font-size:10px;color:${RED};margin-top:5px;">&#9888; above ${i.ceiling.vendor_kg ?? i.ceiling.lane_kg ?? i.ceiling.client_kg}/kg — a price the client has declined before</div>` : ''}
        ${canApply ? `<button class="aqi-btn ${isRec ? '' : 'g'}" data-apply="${esc(o.key)}" style="margin-top:8px;padding:5px 11px;font-size:11px;">Apply ${money(o.sell, 'USD')}</button>` : ''}
      </div>`;
    };
    const curve = wc.buckets && wc.buckets.length && wc.confidence !== 'none' ? `<div style="display:flex;gap:6px;align-items:flex-end;height:46px;margin-top:6px;">
      ${wc.buckets.map(c => `<div style="flex:1;text-align:center;" title="${c.n} decided">
        <div style="height:${c.win_pct == null ? 2 : Math.max(2, c.win_pct * 0.3)}px;background:${c.win_pct == null ? 'rgba(0,0,0,.08)' : c.win_pct >= 70 ? GREEN : c.win_pct >= 40 ? AMBER : RED};border-radius:3px 3px 0 0;margin:0 auto;"></div>
        <div style="font-size:8px;color:${LIGHT};margin-top:2px;white-space:nowrap;">${esc(c.band)}</div><div style="font-size:9px;color:${MID};">${c.win_pct == null ? '—' : c.win_pct + '%'} <span style="color:${LIGHT}">(${c.n})</span></div></div>`).join('')}
    </div>` : '';
    return `<div class="aqi-card" style="margin-bottom:16px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap;">
        <div class="aqi-sec" style="margin:0;">Pricing intelligence &middot; ${esc(q.ref || '')} &middot; ${esc(q.vendor_raw)}${i.quote.lane ? ' &middot; ' + esc(i.quote.lane) : ''}</div>
        <div style="font-size:10px;">${conf}</div>
      </div>
      ${i.headline ? `<div style="margin:10px 0 12px;padding:9px 12px;border-left:3px solid ${kindCol};background:#FAFAFA;border-radius:0 8px 8px 0;font-size:12.5px;color:${DARK};line-height:1.5;">${esc(i.headline)}</div>` : ''}
      <div style="display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,1fr);gap:16px;align-items:start;">
        <div>
          <div class="aqi-sec" style="margin-bottom:4px;">Partner cost &middot; ${esc(q.vendor_raw)} &middot; ${p.n} costed in ${i.window_days} days</div>
          <div style="font-size:12px;color:${DARK};">${p.cost_kg != null ? `<b>${p.cost_kg}/kg</b> today` : 'no cost yet'}${p.vs_median_pct != null ? ` &middot; <span style="color:${p.vs_median_pct <= -5 ? GREEN : p.vs_median_pct >= 5 ? AMBER : MID}">${pct(p.vs_median_pct, true)} vs median</span>` : ''}${p.rank_low ? ` &middot; ${p.rank_low === 1 ? 'lowest' : p.rank_low + (p.rank_low === 2 ? 'nd' : p.rank_low === 3 ? 'rd' : 'th') + ' lowest'} of ${p.n + 1}` : ''}${p.volatility_pct != null ? ` &middot; volatility ${p.volatility_pct}%` : ''}</div>
          ${bar}
          ${p.by_size.length > 1 || p.by_mode.length > 1 ? `<div style="font-size:10px;color:${MID};margin-top:6px;line-height:1.6;">
            ${p.by_size.length > 1 ? `By size: ${p.by_size.map(x => `${esc(x.band)} <b>${x.median_kg}</b> (${x.n})`).join(' &middot; ')}${p.vs_size_median_pct != null ? ` — this one ${pct(p.vs_size_median_pct, true)} vs its size band` : ''}<br>` : ''}
            ${p.by_mode.length > 1 ? `By mode: ${p.by_mode.map(x => `${esc(x.mode)} <b>${x.median_kg}</b> (${x.n})`).join(' &middot; ')}` : ''}
          </div>` : ''}
        </div>
        <div>
          <div class="aqi-sec" style="margin-bottom:4px;">What the client pays &middot; approved, ${i.window_days} days</div>
          <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;">
            ${chip('Client /kg', b.client.sell_kg, b.client.n + ' approved')}
            ${chip('Vendor /kg', b.vendor.sell_kg, b.vendor.n + ' approved')}
            ${chip(b.lane.label ? 'Lane /kg' : 'Lane', b.lane.label ? b.lane.sell_kg : null, b.lane.label ? b.lane.n + ' approved' : 'no lane on quote')}
            ${i.quote.units ? chip('Client /unit', b.client.sell_unit, b.client.n_units + ' with units') : ''}
            ${chip('Ceiling /kg', i.ceiling.vendor_kg ?? i.ceiling.lane_kg ?? i.ceiling.client_kg, i.ceiling.n_declined ? 'lowest declined' : 'nothing declined')}
            ${chip('Win rate', wc.overall_win_pct != null ? wc.overall_win_pct + '%' : null, wc.decided + ' decided')}
          </div>
          ${curve}
        </div>
      </div>
      ${i.options.length ? `<div style="display:grid;grid-template-columns:repeat(${Math.min(4, i.options.length)},minmax(0,1fr));gap:10px;margin-top:14px;">${i.options.map(opt).join('')}</div>` : ''}
      <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;margin-top:10px;flex-wrap:wrap;">
        <div class="aqi-s">Win likelihood is the share of decided quotes approved at a similar price position against the usual /kg. Internal only &mdash; never shown to the client or the partner.</div>
        <button class="aqi-btn g" id="aqi-explain" style="padding:5px 11px;font-size:11px;white-space:nowrap;">Explain with Pulse</button>
      </div>
    </div>`;
  }
  function paintIntel(q) {
    const host = el('aqi-intel'); if (!host) return;
    if (!q || q.cost == null) { host.innerHTML = intelFallbackHtml((_data && _data.insights) || [], (_data && _data.win_bands) || []); window._pulsePin = q ? { page: 'Quote review', label: 'quote ' + (q.ref || ''), ids: [q.ref || q.id] } : null; return; }
    host.innerHTML = intelHtml(q);
    window._pulsePin = { page: 'Quote review', label: 'air quote ' + (q.ref || '') + ' (' + (q.vendor_raw || '') + ')', ids: [q.ref || q.id] };
    host.querySelectorAll('[data-apply]').forEach(bt => bt.addEventListener('click', async () => {
      if (_busy) return; const rec = _intel[q.id]; const o = rec && rec.data && rec.data.options.find(x => x.key === bt.getAttribute('data-apply')); if (!o) return;
      _busy = true; bt.disabled = true;
      try {
        const reason = `intelligence: ${o.label.toLowerCase()} — ${o.sell_kg}/kg${o.vs_anchor_pct != null ? ' (' + pct(o.vs_anchor_pct, true) + ' vs usual)' : ''}${o.win_pct != null ? ', ' + o.win_pct + '% likely' : ''}`;
        const r = await req('/air-quotes/' + encodeURIComponent(q.id) + '/pricing', { method: 'POST', body: JSON.stringify({ total_sell: o.sell, reason }) });
        await load();
        ok(`Priced at ${esc(money(r.sell, 'USD'))} (${esc(o.label)}) — saved as draft, nothing sent to the client.`);
      } catch (e) { err(e.message || String(e)); bt.disabled = false; }
      _busy = false;
    }));
    const ex = el('aqi-explain');
    if (ex) ex.addEventListener('click', () => {
      const qn = `Explain the pricing recommendation for air quote ${q.ref}: which option and why, where the partner's cost sits, and the risk. Three lines.`;
      if (window._pulseRail && window._pulseRail.fire) window._pulseRail.fire(qn);
      else if (typeof window._fireIntoPulse === 'function') window._fireIntoPulse(qn);
      else { const inp = el('pulse-input-display'); if (inp) { inp.value = qn; if (typeof window.pulseSend === 'function') window.pulseSend(); } }
    });
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

  const msg = t => { const m = el('aqi-msg'); if (m) m.innerHTML = t; };
  const err = t => msg(`<div class="aqi-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(t)}</div>`);
  const ok = t => msg(`<div class="aqi-msg" style="background:rgba(27,127,59,.10);color:${GREEN};">${t}</div>`);
  function wireDetail() {

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
    window._pulsePin = null;
    const o = document.querySelector('.aqi-ov'); if (o) o.remove();
    document.body.classList.remove('aqi-open');
  }

  let _qtab = 'air', _sea = null;

  function open() {
    if (!entitled()) return;
    styles();
    if (document.querySelector('.aqi-ov')) return;
    const o = document.createElement('div'); o.className = 'aqi-ov';
    o.innerHTML = `<div class="aqi-panel">
      <div class="aqi-head">
        <div><div class="aqi-t">Quote review</div>
          <div class="aqi-s">Partner cost and margin &middot; internal only</div></div>
        <div style="display:flex;gap:10px;align-items:center;">
          <div class="aqi-tabs">
            <button data-qtab="air" class="on">Air</button>
            <button data-qtab="sea">Sea</button>
            <button data-qtab="requests">Requests</button>
          </div>
          <button class="aqi-x" id="aqi-close">&times;</button>
        </div>
      </div>
      <div class="aqi-body" id="aqi-body">Loading…</div>
    </div>`;
    document.body.appendChild(o);
    document.body.classList.add('aqi-open');
    el('aqi-close').addEventListener('click', close);
    o.querySelectorAll('[data-qtab]').forEach(b => b.addEventListener('click', () => {
      const v = b.getAttribute('data-qtab');
      if (v === _qtab) return;
      _qtab = v;
      o.querySelectorAll('[data-qtab]').forEach(x => x.classList.toggle('on', x.getAttribute('data-qtab') === v));
      // Air is a priced quote with a client decision behind it; sea is weekly cost capture
      // with no client in it at all. Same screen, different question.
      // Requests: door-to-door requests (sea + air options) — review module, internal only.
      if (v === 'sea') loadSea(); else if (v === 'requests') { const b = el('aqi-body'); if (window._rqReview) window._rqReview.load(b); else if (b) b.innerHTML = '<div class="aqi-none">Request review module not loaded — hard-refresh.</div>'; } else load();
    }));
    document.addEventListener('keydown', function k(ev) {
      if (ev.key === 'Escape') { close(); document.removeEventListener('keydown', k); }
    });
    load();
  }

  // ── Sea: weekly container costs ──
  // Deliberately plainer than the air view. There is no margin, no release and no client
  // decision — this is cost capture, and dressing it up as a quote screen would imply a
  // workflow that does not exist.
  async function loadSea() {
    const body = el('aqi-body'); if (!body) return;
    if (!_sea) body.innerHTML = '<div class="aqi-none">Loading…</div>';
    try { _sea = await req('/sea-quotes'); } catch (e) {
      body.innerHTML = `<div class="aqi-none" style="color:${RED}">Could not load: ${esc(e.message || e)}</div>`;
      return;
    }
    paintSea();
  }

  function paintSea() {
    const body = el('aqi-body'); if (!body || !_sea) return;
    const s = _sea.summary || {}, rows = _sea.quotes || [];
    const wk = (ws) => { // ISO week, so a row ties to the week everyone quotes
      const d = new Date(ws + 'T00:00:00Z'); const day = (d.getUTCDay() + 6) % 7;
      d.setUTCDate(d.getUTCDate() - day + 3);
      const f = new Date(Date.UTC(d.getUTCFullYear(), 0, 4)); const fd = (f.getUTCDay() + 6) % 7;
      f.setUTCDate(f.getUTCDate() - fd + 3);
      return 'W' + (1 + Math.round((d - f) / (7 * 86400000)));
    };
    const usd = (n) => n == null ? '—' : 'USD ' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    body.innerHTML = `
      <div class="aqi-tiles" style="grid-template-columns:repeat(4,minmax(0,1fr));">
        <div class="aqi-tile"><div class="aqi-tl">Weeks received</div>
          <div class="aqi-tv">${s.weeks_received || 0}</div>
          <div class="aqi-ts">${s.outstanding ? `<b style="color:${AMBER}">${s.outstanding} awaiting</b>` : 'none outstanding'}</div></div>
        <div class="aqi-tile"><div class="aqi-tl">Containers</div>
          <div class="aqi-tv">${nf(s.containers)}</div>
          <div class="aqi-ts">across all weeks</div></div>
        <div class="aqi-tile"><div class="aqi-tl">Average per container</div>
          <div class="aqi-tv">${s.avg_per_container == null ? '—' : usd(s.avg_per_container)}</div>
          <div class="aqi-ts">container cost only</div></div>
        <div class="aqi-tile"><div class="aqi-tl">Average transit</div>
          <div class="aqi-tv">${s.avg_transit_days == null ? '—' : s.avg_transit_days + 'd'}</div>
          <div class="aqi-ts">port to port</div></div>
      </div>

      ${rows.length ? `<table class="aqi"><thead><tr>
          <th>Week</th><th>Supplier</th><th class="n">Containers</th><th>Sizes</th>
          <th>Carrier</th><th class="n">Transit</th><th class="n">Cost</th><th>Status</th>
        </tr></thead><tbody>${rows.map(q => {
          const sizes = Object.entries(q.by_size || {}).map(([k, v]) => `${v.containers}×${k}`).join(' ');
          return `<tr data-sea="${esc(q.id)}" style="cursor:pointer;">
            <td><b>${wk(q.week_start)}</b> <span style="color:${LIGHT};font-size:10px;">${esc(q.week_start)}</span></td>
            <td style="color:${MID}">${esc(q.supplier)}</td>
            <td class="n">${q.container_count || 0}</td>
            <td style="color:${MID};font-size:10px;">${esc(sizes || '—')}</td>
            <td style="color:${MID}">${esc((q.carriers || []).join(', ') || '—')}</td>
            <td class="n">${q.transit_days_avg == null ? '—' : q.transit_days_avg + 'd'}</td>
            <td class="n">${usd(q.total_cost)}</td>
            <td>${q.state === 'received'
              ? `<span class="aqi-pill" style="color:${GREEN};background:rgba(27,127,59,.12)">Received</span>`
              : `<span class="aqi-pill" style="color:${MID};background:rgba(0,0,0,.05)">Awaiting</span>`}</td>
          </tr>`; }).join('')}</tbody></table>`
        : `<div class="aqi-none">No container costs requested yet. The weekly request goes out on Tuesday evening.</div>`}

      <div style="font-size:10px;color:${LIGHT};margin-top:10px;line-height:1.6;">
        Container cost only &mdash; other charges arrive on the monthly invoice, where this figure is
        shown alongside for reference. Transit is port to port. Internal only: never shown to the client.
      </div>
      <div id="aqi-seadetail"></div>`;

    body.querySelectorAll('[data-sea]').forEach(r => r.addEventListener('click', () => {
      const q = rows.find(x => x.id === r.getAttribute('data-sea'));
      if (!q) return;
      const box = el('aqi-seadetail');
      box.innerHTML = `<div class="aqi-card" style="margin-top:14px;">
        <div class="aqi-sec">${wk(q.week_start)} &middot; ${esc(q.week_start)} &rarr; ${esc(q.supplier)}</div>
        ${(q.lines || []).length ? `<table class="aqi"><thead><tr>
            <th>Container</th><th>Size</th><th>Carrier</th><th class="n">Transit</th><th class="n">Cost</th>
          </tr></thead><tbody>${q.lines.map(l => `<tr>
            <td>${esc(l.container_ref || '—')}</td>
            <td style="color:${MID}">${esc(l.size || '—')}</td>
            <td style="color:${MID}">${esc(l.carrier || '—')}</td>
            <td class="n">${l.transit_days == null ? '—' : l.transit_days + 'd'}</td>
            <td class="n">${usd(l.cost_amount)}</td>
          </tr>`).join('')}</tbody></table>`
          : `<div class="aqi-none">Nothing submitted for this week yet.</div>`}
        ${q.partner_note ? `<div style="font-size:11px;color:${MID};margin-top:9px;">
          <b>Partner note:</b> ${esc(q.partner_note)}</div>` : ''}
        ${q.submitted_by ? `<div style="font-size:10px;color:${LIGHT};margin-top:6px;">
          Submitted by ${esc(q.submitted_by)} on ${esc(String(q.submitted_at || '').slice(0, 16))}</div>` : ''}
      </div>`;
      box.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }));
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
    console.log('[air-quote-review] module v19 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
