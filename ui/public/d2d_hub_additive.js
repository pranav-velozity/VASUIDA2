/* ── VelOzity Pinpoint — door-to-door hub (GRBA) v1 ──
   Reads the /d2d endpoints and shows a week's shipments with plan against actual.

   Rendered as an OVERLAY, like the EHP module, so it never touches the page router — a fault
   here cannot break navigation for ICONIC or EHP.

   Capability-gated twice over: the nav item only appears when the active client has
   freight_d2d, and every endpoint behind it 404s for anyone else. The nav gate is convenience;
   the server gate is the security. */
;(function () {
  'use strict';
  if (window.__D2D_HUB__) return;
  window.__D2D_HUB__ = true;

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const BLUE = '#2C6FBB', LIME = '#9BAB15', YELL = '#FED000', YINK = '#8A6D00', LINK = '#5F6B0D';
  const STAGES = [
    ['pickup', 'Pickup'], ['origin_cleared', 'Origin cleared'], ['departed', 'Departed'],
    ['arrived', 'Arrived'], ['dest_cleared', 'Dest cleared'],
    ['out_for_delivery', 'Out for delivery'], ['delivered', 'Delivered'],
  ];
  const el = (id) => document.getElementById(id);
  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const day = (ymd) => { try { return new Date(ymd + 'T00:00:00Z').toLocaleDateString('en-AU', { day:'numeric', month:'short', timeZone:'UTC' }); } catch (e) { return ymd || ''; } };
  const daysBetween = (a, b) => {
    if (!a || !b) return null;
    const x = new Date(a + 'T00:00:00Z'), y = new Date(b + 'T00:00:00Z');
    if (isNaN(x) || isNaN(y)) return null;
    return Math.round((y - x) / 86400000);
  };

  let _enabled = null, _capClient = null, _week = null, _autoOpened = false;

  async function api(path, opts) {
    const base = (document.querySelector('meta[name="api-base"]') || {}).content || '';
    const t = (window.Clerk && window.Clerk.session) ? await window.Clerk.session.getToken() : null;
    const headers = { 'Content-Type': 'application/json' };
    if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(String(base).replace(/\/+$/, '') + path, { ...(opts || {}), headers });
    if (!r.ok) { const e = new Error('http ' + r.status); e.status = r.status; throw e; }
    return r.json();
  }

  // ── Is this client a door-to-door client? ──
  async function refreshEnabled() {
    const active = window.pinpointClient || 'unknown';
    if (_capClient === active) { paintNav(); return; }
    const caps = window.pinpointCaps;
    if (Array.isArray(caps)) {              // published by the tenancy module
      _enabled = caps.includes('freight_d2d');
      _capClient = active; paintNav(); return;
    }
    try { await api('/d2d/health'); _enabled = true; }
    catch (e) {
      // Fail closed only on a definite answer; a transient error leaves the nav alone.
      if (e.status === 404 || e.status === 403) _enabled = false; else return;
    }
    _capClient = active; paintNav();
  }
  function paintNav() {
    const n = el('nav-d2d'); if (n) n.style.display = _enabled ? '' : 'none';
    const wh = el('nav-weekhub');
    const noWeekHub = !wh || wh.style.display === 'none';

    // The legacy screens cache their data in localStorage keyed on the WEEK only, never the
    // client. The server refuses their endpoints for a door-to-door client, but the page still
    // paints from whatever another client left in this browser — which is how ICONIC's week
    // appeared under a GRBA session. Hiding the pages is what actually stops that being seen.
    if (_enabled && noWeekHub) hideLegacyPages(true);
    else if (_enabled === false) hideLegacyPages(false);

    // Nothing to land on, so open the hub once.
    if (_enabled && noWeekHub && !_autoOpened) {
      _autoOpened = true; setTimeout(() => { open().catch(() => {}); }, 80);
    }
  }

  // Only ever un-hides what this function hid, marked with a data attribute — the same rule
  // the tenancy module follows, so it cannot reverse another module's decision to hide.
  function hideLegacyPages(on) {
    document.querySelectorAll('section[id^="page-"]').forEach(secn => {
      if (on) { secn.dataset.d2dHidden = '1'; secn.style.display = 'none'; }
      else if (secn.dataset.d2dHidden === '1') { delete secn.dataset.d2dHidden; secn.style.display = ''; }
    });
    let back = el('d2d-backdrop');
    if (on && !back) {
      back = document.createElement('div');
      back.id = 'd2d-backdrop';
      back.style.cssText = 'padding:80px 26px;text-align:center;font-family:inherit;';
      back.innerHTML = `<div style="font-size:15px;font-weight:600;color:${DARK};">Shipments</div>
        <div style="font-size:12px;color:${MID};margin:6px 0 16px;">Door to door &middot; plan against actual</div>
        <button id="d2d-reopen" style="border:.5px solid rgba(0,0,0,.16);background:#fff;color:${DARK};
          border-radius:9px;padding:10px 16px;font:600 12px inherit;cursor:pointer;min-height:44px;">Open shipments &rarr;</button>`;
      const host = document.querySelector('main') || document.body;
      host.appendChild(back);
      const btn = el('d2d-reopen'); if (btn) btn.onclick = () => open().catch(() => {});
    } else if (!on && back) back.remove();
  }

  function injectNav() {
    if (el('nav-d2d')) { paintNav(); return; }
    const nav = el('pn-nav-items'); if (!nav) return;
    const a = document.createElement('a');
    a.className = 'pn-nav-item'; a.id = 'nav-d2d'; a.href = '#d2d';
    a.textContent = 'Shipments';
    a.style.display = 'none';
    a.addEventListener('click', (e) => { e.preventDefault(); open(); });
    const after = el('nav-weekhub');
    if (after && after.parentNode === nav) nav.insertBefore(a, after.nextSibling); else nav.appendChild(a);
    paintNav();
  }

  // ── Styles ──
  function styles() {
    if (el('d2d-css')) return;
    const st = document.createElement('style'); st.id = 'd2d-css';
    st.textContent = `
      .d2d-ov{position:fixed;inset:0;background:#F7F8FA;z-index:9500;display:flex;flex-direction:column;font-family:inherit;}
      .d2d-top{display:flex;justify-content:space-between;align-items:center;padding:0 26px;height:56px;background:#fff;border-bottom:.5px solid rgba(0,0,0,.08);}
      .d2d-tabs{display:flex;gap:16px;}
      .d2d-tab{border:0;background:none;font:600 13px inherit;color:${MID};cursor:pointer;padding:18px 0 15px;border-bottom:2px solid transparent;}
      .d2d-tab.on{color:${DARK};border-bottom-color:${BRAND};}
      .d2d-tick{height:34px;background:${DARK};display:flex;align-items:center;gap:10px;padding:0 26px;overflow:hidden;}
      .d2d-tickt{font-size:12px;color:#EDEDF0;white-space:nowrap;}
      .d2d-body{flex:1;overflow-y:auto;padding:18px 26px 40px;}
      .d2d-x{border:0;background:#F2F2F5;width:34px;height:34px;border-radius:9px;cursor:pointer;font-size:18px;color:${MID};}
      .d2d-card{border:.5px solid rgba(16,18,27,.08);border-radius:14px;background:#fff;
        box-shadow:0 1px 2px rgba(16,18,27,.04),0 4px 12px rgba(16,18,27,.05);}
      .d2d-grid4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}
      .d2d-tile{padding:14px 16px;}
      .d2d-tl{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;}
      .d2d-tv{font-size:25px;font-weight:700;color:${DARK};margin-top:3px;letter-spacing:-.02em;}
      .d2d-ts{font-size:11px;color:${MID};}
      .d2d-wk{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;}
      .d2d-wkb,.d2d-btn{border:.5px solid rgba(0,0,0,.16);background:#fff;color:${DARK};border-radius:9px;padding:9px 13px;
        font:600 12px inherit;cursor:pointer;min-height:44px;transition:border-color .2s ease,background .2s ease;}
      .d2d-wkb:hover,.d2d-btn:hover{border-color:rgba(0,0,0,.34);background:#F7F8FA;}
      .d2d-wkb.on{background:${DARK};color:#fff;border-color:${DARK};}
      .d2d-btn.dark{background:${DARK};color:#fff;border-color:${DARK};}
      .d2d-sh{padding:15px 18px;margin-bottom:12px;}
      .d2d-strip{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));margin-top:14px;}
      .d2d-st{display:flex;flex-direction:column;align-items:center;gap:5px;position:relative;}
      .d2d-line{position:absolute;top:30px;height:3px;}
      .d2d-dot{width:34px;height:34px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;
        border:2px solid;box-sizing:border-box;position:relative;z-index:1;}
      .d2d-nba{border-left:3px solid;padding:12px 14px;display:flex;flex-direction:column;gap:5px;}
      .d2d-kind{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;}
      .d2d-opt{padding:15px 17px;display:flex;flex-direction:column;gap:10px;border-top:3px solid;}
      .d2d-num{font-family:ui-monospace,SFMono-Regular,monospace;}
      .d2d-in{width:64px;font:inherit;font-size:12.5px;text-align:right;border:.5px solid rgba(0,0,0,.18);border-radius:7px;padding:7px 8px;}
      .d2d-empty{padding:44px;text-align:center;color:${MID};font-size:13px;}
      @keyframes d2d-live{0%,100%{transform:scale(1);}50%{transform:scale(1.13);}}
      @keyframes d2d-halo{0%{transform:scale(1);opacity:.45;}75%{transform:scale(2.1);opacity:0;}100%{opacity:0;}}
      @keyframes d2d-rise{from{opacity:0;transform:translateY(7px);}to{opacity:1;transform:none;}}
      @keyframes d2d-tick{0%,30%{opacity:1;transform:none;}36%,100%{opacity:0;transform:translateY(-9px);}}
      .d2d-pulse{animation:d2d-live 2.4s ease-in-out infinite;}
      .d2d-halo{position:absolute;width:34px;height:34px;border-radius:50%;animation:d2d-halo 2.4s ease-out infinite;}
      .d2d-rise{animation:d2d-rise .42s cubic-bezier(.22,1,.36,1) both;}
      .d2d-lift{transition:transform .26s cubic-bezier(.22,1,.36,1),box-shadow .26s cubic-bezier(.22,1,.36,1);}
      .d2d-lift:hover{transform:translateY(-4px);box-shadow:0 14px 30px rgba(16,18,27,.13);}
      @media (prefers-reduced-motion:reduce){.d2d-pulse,.d2d-halo,.d2d-rise{animation:none;}.d2d-lift:hover{transform:none;}}
    `;
    document.head.appendChild(st);
  }

  // Stage glyphs, in journey order: crate, stamp, vessel, anchor, shield, van, door.
  const ICONS = [
    'M4 8h16v11H4z M4 8l2-4h12l2 4 M10 12h4',
    'M6 20h12 M12 4v9 M8 8l4 5 4-5',
    'M3 17h18 M5 17l-1-5h16l-1 5 M9 12V7h6v5',
    'M12 5v14 M8 9h8 M5 14a7 7 0 0 0 14 0',
    'M12 3l7 3v6c0 4-3 7-7 9-4-2-7-5-7-9V6z',
    'M3 16V8h11v8z M14 11h4l3 3v2h-7z M7 19a1.6 1.6 0 1 0 0-3 1.6 1.6 0 0 0 0 3 M17 19a1.6 1.6 0 1 0 0-3 1.6 1.6 0 0 0 0 3',
    'M5 20V5h10v15 M15 11h4v9 M11 12h.01',
  ];

  function close() { const o = document.querySelector('.d2d-ov'); if (o) o.remove(); document.body.style.overflow = ''; }

  async function open() {
    styles();
    if (document.querySelector('.d2d-ov')) return;
    const ov = document.createElement('div');
    ov.className = 'd2d-ov';
    ov.innerHTML = `
      <div class="d2d-top">
        <div style="display:flex;align-items:center;gap:26px;">
          <span style="font-size:15px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Door to door</span>
          <div class="d2d-tabs" id="d2d-tabs"></div>
        </div>
        <div style="display:flex;align-items:center;gap:12px;">
          <span class="d2d-num" style="font-size:11px;color:${MID};" id="d2d-scope"></span>
          <button class="d2d-x" id="d2d-close" aria-label="Close">&times;</button>
        </div>
      </div>
      <div class="d2d-tick"><span style="width:6px;height:6px;border-radius:50%;background:${LIME};flex-shrink:0;"></span>
        <div style="position:relative;height:34px;flex:1;" id="d2d-ticker"></div></div>
      <div class="d2d-body" id="d2d-body"><div class="d2d-empty">Loading&hellip;</div></div>`;
    document.body.appendChild(ov);
    document.body.style.overflow = 'hidden';
    el('d2d-close').onclick = close;
    const onKey = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);
    await load();
  }

  // ── Data ──
  let _tab = 'shipments', _data = null, _internal = false;

  async function load() {
    try {
      const weeks = (await api('/d2d/weeks')).weeks || [];
      if (!_week && weeks.length) _week = weeks[0].week_start;
      let shipments = [], bookings = [], pricingVisible = false;
      if (_week) {
        shipments = (await api('/d2d/shipments?week=' + encodeURIComponent(_week))).shipments || [];
        const bk = await api('/d2d/bookings?week=' + encodeURIComponent(_week));
        bookings = bk.bookings || []; pricingVisible = !!bk.pricing_visible;
      }
      _internal = pricingVisible;
      _data = { weeks, shipments, bookings };
      paint();
    } catch (e) {
      const b = el('d2d-body');
      if (b) b.innerHTML = `<div class="d2d-empty" style="color:${BRAND}">Could not load (${esc(e.message)}).</div>`;
    }
  }

  // ── Derived signals: every figure below comes from the loaded rows, never a stored total ──
  const evMap = (s) => { const m = {}; for (const e of (s.events || [])) m[e.stage] = e; return m; };
  const today = () => new Date().toISOString().slice(0, 10);

  function slipOf(s) {
    const ev = evMap(s);
    let worst = null;
    for (const [k] of STAGES) {
      const d = daysBetween(s['plan_' + k], ev[k] && ev[k].actual_at);
      if (d != null && (worst == null || d > worst)) worst = d;
    }
    return worst;
  }
  // A stage whose planned date has passed with nothing recorded is overdue — the most useful
  // signal on the screen, and the one a stored status would never notice.
  function overdueOf(s) {
    const ev = evMap(s); const now = today();
    for (const [k, label] of STAGES) {
      if (ev[k] && ev[k].actual_at) continue;
      const plan = s['plan_' + k];
      if (plan && plan < now) return { stage: k, label, days: daysBetween(plan, now) };
      return null;                      // stages are sequential: stop at the first unrecorded
    }
    return null;
  }

  function actions(d) {
    const out = [];
    for (const s of d.shipments) {
      const od = overdueOf(s);
      if (od) out.push({ kind: 'Chase', accent: BRAND, ink: BRAND,
        what: `${od.label} not recorded for ${s.reference || 'an unadvised container'}`,
        effect: `Planned ${day(s['plan_' + od.stage])}, ${od.days} day${od.days === 1 ? '' : 's'} ago`, sort: 100 + od.days });
      if (!s.reference) out.push({ kind: 'Missing', accent: YELL, ink: YINK,
        what: 'Container number not advised', effect: 'Kerry cannot report milestones without it', sort: 60 });
    }
    const released = d.bookings.filter(b => b.status === 'released');
    if (released.length) out.push({ kind: 'Decide', accent: BLUE, ink: BLUE,
      what: `${released.length} option${released.length === 1 ? '' : 's'} awaiting a decision`,
      effect: 'Space is held until the cut-off', sort: 90 });
    const drafts = d.bookings.filter(b => b.status === 'draft');
    if (drafts.length && _internal) out.push({ kind: 'Price', accent: YELL, ink: YINK,
      what: `${drafts.length} option${drafts.length === 1 ? '' : 's'} not yet released`,
      effect: 'Set the margin, then release to the client', sort: 80 });
    if (!out.length) out.push({ kind: 'Clear', accent: LIME, ink: LINK,
      what: 'Nothing needs a decision', effect: 'Every stage is on plan or recorded', sort: 0 });
    return out.sort((a, b) => b.sort - a.sort).slice(0, 4);
  }

  function tickerLines(d) {
    const lines = [];
    for (const s of d.shipments) {
      const sl = slipOf(s);
      if (sl != null && sl > 0) lines.push(`${s.reference || 'A container'} is running ${sl} day${sl === 1 ? '' : 's'} behind plan`);
    }
    const rel = d.bookings.filter(b => b.status === 'released').length;
    if (rel) lines.push(`${rel} sailing option${rel === 1 ? '' : 's'} awaiting your decision`);
    const done = d.shipments.filter(s => s.status === 'delivered').length;
    if (done) lines.push(`${done} of ${d.shipments.length} delivered this week`);
    if (!lines.length) lines.push('Everything on plan this week');
    return lines.slice(0, 3);
  }

  // ── Paint ──
  function paint() {
    const d = _data; if (!d) return;
    const scope = el('d2d-scope'); if (scope) scope.textContent = (window.pinpointClient || '') + (_internal ? ' · VelOzity view' : '');

    const tabs = [['shipments', 'Shipments'], ['bookings', 'Bookings']].concat(_internal ? [['pricing', 'Pricing']] : []);
    const tw = el('d2d-tabs');
    if (tw) {
      tw.innerHTML = tabs.map(([k, l]) => `<button class="d2d-tab ${_tab === k ? 'on' : ''}" data-tab="${k}">${l}</button>`).join('');
      tw.querySelectorAll('[data-tab]').forEach(b => b.onclick = () => { _tab = b.getAttribute('data-tab'); paint(); });
    }
    const tick = el('d2d-ticker');
    if (tick) tick.innerHTML = tickerLines(d).map((t, i) =>
      `<span class="d2d-tickt" style="position:absolute;left:0;top:0;height:34px;display:flex;align-items:center;animation:d2d-tick 12s ${i * 4}s infinite;">${esc(t)}</span>`).join('');

    const body = el('d2d-body'); if (!body) return;
    if (!d.weeks.length) {
      body.innerHTML = `<div class="d2d-empty">No weeks booked yet.<br><span style="font-size:11.5px;">A week appears once an option has been approved.</span></div>`;
      return;
    }
    const weekBar = `<div class="d2d-wk">${d.weeks.map(w =>
      `<button class="d2d-wkb ${w.week_start === _week ? 'on' : ''}" data-w="${esc(w.week_start)}">${esc(day(w.week_start))}
        <span style="opacity:.6;font-weight:500;">&middot; ${w.shipments}</span></button>`).join('')}</div>`;

    body.innerHTML = weekBar + (_tab === 'shipments' ? paintShipments(d) : _tab === 'bookings' ? paintBookings(d) : paintPricing(d));
    body.querySelectorAll('[data-w]').forEach(b => b.onclick = () => { _week = b.getAttribute('data-w'); load(); });
    wireActions(body);
  }

  function paintShipments(d) {
    const inTransit = d.shipments.filter(s => s.status === 'in_transit').length;
    const delivered = d.shipments.filter(s => s.status === 'delivered').length;
    const slips = d.shipments.map(slipOf).filter(x => x != null && x > 0);
    const approved = d.bookings.find(b => b.status === 'approved');
    const acts = actions(d);
    return `
      <div class="d2d-grid4" style="margin-bottom:14px;">
        <div class="d2d-card d2d-tile d2d-rise"><div class="d2d-tl">Shipments</div><div class="d2d-tv">${d.shipments.length}</div><div class="d2d-ts">this week</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.05s"><div class="d2d-tl">In transit</div><div class="d2d-tv">${inTransit}</div><div class="d2d-ts">${delivered} delivered</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.1s"><div class="d2d-tl">Behind plan</div>
          <div class="d2d-tv" style="color:${slips.length ? BRAND : DARK}">${slips.length}</div>
          <div class="d2d-ts">${slips.length ? 'worst ' + Math.max(...slips) + ' days' : 'all on plan'}</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.15s"><div class="d2d-tl">Booked as</div>
          <div class="d2d-tv" style="font-size:17px;line-height:1.3;">${approved ? esc(approved.title || approved.option_ref) : '&ndash;'}</div>
          <div class="d2d-ts">${approved ? esc([approved.carrier, approved.transit_days ? approved.transit_days + ' days' : ''].filter(Boolean).join(' · ')) : 'no approved option'}</div></div>
      </div>

      <div style="display:flex;align-items:baseline;gap:9px;margin-bottom:8px;">
        <span style="font-size:12.5px;font-weight:600;color:${DARK};">Next best action</span>
        <span style="font-size:11px;color:${MID};">from this week's dates</span>
      </div>
      <div class="d2d-grid4" style="margin-bottom:18px;">
        ${acts.map((a, i) => `<div class="d2d-card d2d-nba d2d-rise d2d-lift" style="border-left-color:${a.accent};animation-delay:${i * .05}s">
          <span class="d2d-kind" style="color:${a.ink};">${esc(a.kind)}</span>
          <span style="font-size:12.5px;font-weight:600;color:${DARK};line-height:1.35;">${esc(a.what)}</span>
          <span style="font-size:11px;color:${MID};line-height:1.4;">${esc(a.effect)}</span>
        </div>`).join('')}
      </div>

      ${d.shipments.map(shipmentCard).join('')}`;
  }

  function shipmentCard(s) {
    const ev = evMap(s);
    let liveIdx = -1;
    STAGES.forEach(([k], i) => { if (ev[k] && ev[k].actual_at) liveIdx = i; });
    const od = overdueOf(s);
    const odIdx = od ? STAGES.findIndex(([k]) => k === od.stage) : -1;

    const cells = STAGES.map(([k, label], i) => {
      const e = ev[k], actual = e && e.actual_at ? e.actual_at : null;
      const plan = s['plan_' + k];
      const slip = daysBetween(plan, actual);
      const late = slip != null && slip > 0;
      const overdue = i === odIdx;
      const done = !!actual;
      const colour = overdue ? BRAND : (!done ? '#D6D6DB' : (late ? BRAND : LIME));
      const ink = overdue ? BRAND : (!done ? LIGHT : (late ? BRAND : LINK));
      const fill = overdue ? 'rgba(153,0,51,.08)' : (done ? (late ? 'rgba(153,0,51,.10)' : 'rgba(155,171,21,.16)') : '#fff');
      const live = overdue || i === liveIdx;
      const leftFill = i === 0 ? 'transparent' : (i <= liveIdx ? LIME : '#E4E4E9');
      const rightFill = i === STAGES.length - 1 ? 'transparent' : (i < liveIdx ? LIME : '#E4E4E9');
      return `<div class="d2d-st">
        <span class="d2d-line" style="left:0;right:50%;background:${leftFill};"></span>
        <span class="d2d-line" style="left:50%;right:0;background:${rightFill};"></span>
        <span style="font-size:9px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;text-align:center;min-height:22px;">${label}</span>
        <span style="position:relative;display:inline-flex;align-items:center;justify-content:center;">
          ${live ? `<span class="d2d-halo" style="background:${overdue ? 'rgba(153,0,51,.22)' : 'rgba(155,171,21,.22)'};"></span>` : ''}
          <span class="d2d-dot ${live ? 'd2d-pulse' : ''}" style="border-color:${colour};background:${fill};">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="${colour}" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${ICONS[i]}"></path></svg>
          </span>
        </span>
        <span class="d2d-num" style="font-size:10px;color:${LIGHT};">${plan ? 'plan ' + day(plan) : ''}</span>
        <span class="d2d-num" style="font-size:11.5px;font-weight:600;color:${ink};">${actual ? day(actual) : (overdue ? 'overdue' : '&middot;')}</span>
        ${actual ? `<span style="font-size:9px;text-transform:uppercase;letter-spacing:.03em;color:${e.source === 'manual' ? YINK : MID};">${esc(e.source)}</span>` : ''}
        ${late ? `<span style="font-size:10px;font-weight:700;color:${BRAND};">+${slip}d</span>` : ''}
      </div>`;
    }).join('');

    const slip = slipOf(s);
    const badge = s.status === 'delivered' ? ['Delivered', LINK, 'rgba(155,171,21,.20)']
                : od ? [od.label + ' overdue', '#fff', BRAND]
                : (slip != null && slip > 0) ? ['+' + slip + ' days', '#fff', BRAND]
                : s.status === 'in_transit' ? ['On plan', LINK, 'rgba(155,171,21,.20)']
                : ['Booked', MID, '#F2F2F5'];
    return `<div class="d2d-card d2d-sh d2d-rise">
      <div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;">
        <div style="display:flex;align-items:baseline;gap:11px;">
          <span class="d2d-num" style="font-size:14px;color:${DARK};">${esc(s.reference || 'container not yet advised')}</span>
          <span style="font-size:11px;color:${MID};">${esc([s.container_type, s.carrier, s.vessel].filter(Boolean).join(' · '))}</span>
        </div>
        <span style="font-size:10.5px;font-weight:700;border-radius:6px;padding:3px 9px;color:${badge[1]};background:${badge[2]};">${esc(badge[0])}</span>
      </div>
      <div class="d2d-strip">${cells}</div>
    </div>`;
  }

  // ── Bookings: what the client decides ──
  function paintBookings(d) {
    if (!d.bookings.length) return `<div class="d2d-empty">No options for this week.</div>`;
    const order = { released: 0, approved: 1, draft: 2, declined: 3, expired: 4 };
    const rows = [...d.bookings].sort((a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9));
    return `<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;">
      ${rows.map((b, i) => {
        const st = b.status;
        const accent = st === 'approved' ? LIME : st === 'released' ? BLUE : st === 'draft' ? YELL : '#D6D6DB';
        const badge = st === 'approved' ? ['Approved', LINK, 'rgba(155,171,21,.20)']
                    : st === 'released' ? ['Awaiting decision', BLUE, 'rgba(44,111,187,.12)']
                    : st === 'draft' ? ['Not released', YINK, 'rgba(254,208,0,.20)']
                    : [st.charAt(0).toUpperCase() + st.slice(1), MID, '#F2F2F5'];
        return `<div class="d2d-card d2d-opt d2d-rise d2d-lift" style="border-top-color:${accent};animation-delay:${i * .05}s">
          <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;">
            <div>
              <div style="font-size:15px;font-weight:700;color:${DARK};letter-spacing:-.01em;">${esc(b.title || 'Option ' + b.option_ref)}</div>
              <div style="font-size:11px;color:${MID};margin-top:2px;">${esc([b.carrier, b.container_qty ? b.container_qty + ' x ' + (b.container_type || '') : '', b.transhipment ? 'via transhipment' : 'direct'].filter(Boolean).join(' · '))}</div>
            </div>
            <span style="font-size:9.5px;font-weight:700;border-radius:6px;padding:3px 8px;white-space:nowrap;color:${badge[1]};background:${badge[2]};">${esc(badge[0])}</span>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:9px 12px;padding:11px 0;border-top:.5px solid rgba(0,0,0,.06);border-bottom:.5px solid rgba(0,0,0,.06);">
            <div><div style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Transit</div>
              <div class="d2d-num" style="font-size:15px;color:${DARK};">${b.transit_days ? b.transit_days + ' days' : '&ndash;'}</div></div>
            <div><div style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Door to door</div>
              <div class="d2d-num" style="font-size:15px;color:${DARK};">${money(b.sell_amount, b.currency)}</div></div>
          </div>
          ${st === 'released' ? `<div style="display:flex;gap:8px;">
              <button class="d2d-btn dark" style="flex:1;" data-approve="${esc(b.id)}">Approve &rarr;</button>
              <button class="d2d-btn" data-decline="${esc(b.id)}">Decline</button>
            </div>`
            : st === 'approved' ? `<div style="font-size:11px;color:${MID};">Approved${b.decided_at ? ' ' + day(String(b.decided_at).slice(0, 10)) : ''} &middot; plan frozen</div>`
            : `<div style="font-size:11px;color:${LIGHT};">No action</div>`}
        </div>`;
      }).join('')}
    </div>`;
  }

  // ── Pricing: VelOzity only ──
  function paintPricing(d) {
    const rows = d.bookings.filter(b => ['draft', 'released', 'approved'].includes(b.status));
    if (!rows.length) return `<div class="d2d-empty">No options to price for this week.</div>`;
    const drafts = rows.filter(b => b.status === 'draft');
    const below = drafts.filter(b => Number(b.margin_pct) < 15);
    return `
      <div class="d2d-card" style="padding:0;overflow:hidden;margin-bottom:12px;">
        <div style="display:grid;grid-template-columns:1fr 110px 110px 104px 116px 128px;gap:0 12px;padding:11px 18px 8px;background:#FBFBFC;
             font-size:9.5px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">
          <span>Option</span><span style="text-align:right;">Kerry cost</span><span style="text-align:right;">Our cost</span>
          <span style="text-align:right;">Margin %</span><span style="text-align:right;">Sell</span><span>Status</span>
        </div>
        ${rows.map(b => {
          const ourCost = (Number(b.cost_amount) || 0) + (Number(b.accessorial_amount) || 0);
          const low = Number(b.margin_pct) < 15;
          return `<div style="display:grid;grid-template-columns:1fr 110px 110px 104px 116px 128px;gap:0 12px;padding:12px 18px;
                       border-top:.5px solid rgba(0,0,0,.06);align-items:center;">
            <span><span style="display:block;font-size:13px;font-weight:600;color:${DARK};">${esc(b.title || 'Option ' + b.option_ref)}</span>
              <span style="display:block;font-size:10.5px;color:${MID};">${esc(b.carrier || '')}</span></span>
            <span class="d2d-num" style="font-size:12.5px;color:${MID};text-align:right;">${money(b.cost_amount, b.currency)}</span>
            <span class="d2d-num" style="font-size:12.5px;color:${DARK};text-align:right;">${money(ourCost, b.currency)}</span>
            <span style="text-align:right;">${b.status === 'draft'
              ? `<input class="d2d-in" type="text" value="${esc(b.margin_pct)}" data-margin="${esc(b.id)}" aria-label="Margin percent">`
              : `<span class="d2d-num" style="font-size:12.5px;color:${MID};">${esc(b.margin_pct)}%</span>`}</span>
            <span class="d2d-num" style="font-size:14px;font-weight:600;color:${DARK};text-align:right;">${money(b.sell_amount, b.currency)}</span>
            <span style="display:flex;align-items:center;gap:8px;">
              <span style="font-size:10.5px;font-weight:700;color:${b.status === 'draft' ? YINK : b.status === 'approved' ? LINK : BLUE};">${esc(b.status)}</span>
              ${low && b.status === 'draft' ? `<span style="font-size:10px;color:${BRAND};">below floor</span>` : ''}
            </span>
          </div>`;
        }).join('')}
      </div>
      ${drafts.length ? `<div style="display:flex;align-items:center;gap:12px;">
        <button class="d2d-btn dark" data-release="1">Release ${drafts.length} option${drafts.length === 1 ? '' : 's'} to the client &rarr;</button>
        <span style="font-size:11px;color:${below.length ? BRAND : MID};">
          ${below.length ? below.length + ' option(s) price below the 15% floor and will be refused.' : 'All options meet the 15% floor.'}</span>
      </div>` : `<div style="font-size:11px;color:${MID};">Everything for this week has been released.</div>`}`;
  }

  function money(v, cur) {
    if (v == null || v === '') return '&ndash;';
    const n = Number(v); if (!isFinite(n)) return '&ndash;';
    return (cur && cur !== 'USD' ? cur + ' ' : '$') + n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  // ── Actions ──
  function wireActions(root) {
    const busy = (b, on) => { if (b) { b.disabled = on; b.style.opacity = on ? '.6' : ''; } };
    root.querySelectorAll('[data-approve]').forEach(b => b.onclick = async () => {
      busy(b, true);
      try { await api('/d2d/bookings/' + b.getAttribute('data-approve') + '/decision', { method: 'POST', body: JSON.stringify({ decision: 'approve' }) }); await load(); }
      catch (e) { busy(b, false); alert('Could not approve: ' + e.message); }
    });
    root.querySelectorAll('[data-decline]').forEach(b => b.onclick = async () => {
      busy(b, true);
      try { await api('/d2d/bookings/' + b.getAttribute('data-decline') + '/decision', { method: 'POST', body: JSON.stringify({ decision: 'decline' }) }); await load(); }
      catch (e) { busy(b, false); alert('Could not decline: ' + e.message); }
    });
    root.querySelectorAll('[data-margin]').forEach(inp => inp.onchange = async () => {
      const pct = Number(inp.value);
      if (!isFinite(pct)) return;
      try { await api('/d2d/bookings/' + inp.getAttribute('data-margin') + '/pricing', { method: 'PATCH', body: JSON.stringify({ margin_pct: pct }) }); await load(); }
      catch (e) { alert('Could not save the margin: ' + e.message); }
    });
    const rel = root.querySelector('[data-release]');
    if (rel) rel.onclick = async () => {
      busy(rel, true);
      try { await api('/d2d/bookings/week/' + encodeURIComponent(_week) + '/release', { method: 'POST', body: JSON.stringify({}) }); await load(); }
      catch (e) { busy(rel, false); alert('Could not release: ' + e.message); }
    };
  }

  // ── Wiring ──
  const boot = () => { injectNav(); refreshEnabled().catch(() => {}); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);
  const mo = new MutationObserver(() => injectNav());
  const start = () => mo.observe(document.body, { childList: true, subtree: true });
  if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);
  window.addEventListener('focus', () => { refreshEnabled().catch(() => {}); }, { passive: true });

  window.__openD2D = open;
  console.log('[d2d-hub] v2 loaded');
})();
