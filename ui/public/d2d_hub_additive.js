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
    if (!r.ok) {
      // The server explains its refusals — a margin floor, a stage out of order. Showing
      // "http 400" instead would throw that away.
      let detail = '';
      try { const j = await r.json(); detail = j.message || j.error || ''; } catch (e) {}
      const err = new Error(detail || ('http ' + r.status));
      err.status = r.status; throw err;
    }
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
    // getComputedStyle, not the inline style: the link may be hidden by a class rather than
    // an inline rule, and reading only .style would miss it.
    const hidden = !wh || wh.style.display === 'none'
      || (wh.ownerDocument.defaultView.getComputedStyle(wh).display === 'none');
    const noWeekHub = hidden;

    // The legacy screens cache their data in localStorage keyed on the WEEK only, never the
    // client. The server refuses their endpoints for a door-to-door client, but the page still
    // paints from whatever another client left in this browser — which is how ICONIC's week
    // appeared under a GRBA session. Hiding the pages is what actually stops that being seen.
    if (_enabled && noWeekHub) hideLegacyPages(true);
    else if (_enabled === false) {
      hideLegacyPages(false);
      // This client has no door-to-door. Leaving the page visible meant switching to ICONIC
      // showed an empty Door to door shell with "not_found", because the hash was still #d2d.
      const page = el('page-d2d');
      if (page) { page.style.display = 'none'; page.classList.add('hidden'); page.innerHTML = ''; }
      _autoOpened = false;
      if (String(location.hash || '') === '#d2d') {
        try { if (typeof window.show === 'function') window.show('#week-hub'); } catch (e) {}
      }
    }

    // Nothing to land on, so open the hub once.
    if (_enabled && noWeekHub && !_autoOpened) {
      // Navigate through the router, the same way every other page is opened.
      _autoOpened = true;
      setTimeout(() => { try { if (typeof window.show === 'function') window.show('#d2d'); } catch (e) {} }, 80);
    }
  }

  // Only ever un-hides what this function hid, marked with a data attribute — the same rule
  // the tenancy module follows, so it cannot reverse another module's decision to hide.
  function hideLegacyPages(on) {
    document.querySelectorAll('section[id^="page-"]').forEach(secn => {
      // Not this page. It is a real page now, and the router has just shown it — hiding every
      // section without exception would hide the one we are trying to display.
      if (secn.id === 'page-d2d') return;
      if (on) { secn.dataset.d2dHidden = '1'; secn.style.display = 'none'; }
      else if (secn.dataset.d2dHidden === '1') { delete secn.dataset.d2dHidden; secn.style.display = ''; }
    });
  }

  // The nav entry is declared in index.html alongside the others; this only decides whether
  // this client should see it. Building nav from a module is what made it look bolted on.
  function injectNav() { paintNav(); }

  // ── Styles ──
  function styles() {
    if (el('d2d-css')) return;
    const st = document.createElement('style'); st.id = 'd2d-css';
    st.textContent = `
      /* The page sits inside the app's own container, so it needs no width of its own. */
      .d2d-head{margin-bottom:14px;};border-radius:10px;padding:0 14px;align-items:center;gap:10px;overflow:hidden;display:flex;margin-bottom:12px;}
      .d2d-tabs{display:flex;gap:18px;}
      .d2d-filt{border:.5px solid rgba(0,0,0,.14);background:#fff;color:${MID};border-radius:7px;padding:4px 9px;
        font:600 10.5px inherit;cursor:pointer;transition:border-color .18s ease,background .18s ease,color .18s ease;}
      .d2d-filt:hover{border-color:rgba(0,0,0,.3);}
      .d2d-filt.on{background:${DARK};border-color:${DARK};color:#fff;}
      .d2d-tab{border:0;background:none;font:600 13px inherit;color:${MID};cursor:pointer;padding:6px 0;border-bottom:2px solid transparent;}
      .d2d-tab.on{color:${DARK};border-bottom-color:${BRAND};}
      /* Week chips, matching the Week Hub's date circles rather than generic pills. */
      .d2d-chip{width:64px;height:64px;border-radius:50%;border:1.5px solid rgba(0,0,0,.10);background:#fff;
        display:flex;flex-direction:column;align-items:center;justify-content:center;gap:1px;cursor:pointer;
        font:inherit;color:${DARK};transition:border-color .18s ease,transform .18s cubic-bezier(.22,1,.36,1);flex-shrink:0;}
      .d2d-chip:hover{border-color:rgba(0,0,0,.28);transform:translateY(-2px);}
      .d2d-chip.on{border-color:${BRAND};border-width:2px;}
      .d2d-chip .m{font-size:9px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:${LIGHT};}
      .d2d-chip.on .m{color:${BRAND};}
      .d2d-chip .d{font-size:19px;font-weight:600;line-height:1;}
      .d2d-chip .n{font-size:8.5px;color:${MID};}
      /* The strip is capped so stages sit a sensible distance apart on a wide monitor. */
      .d2d-striphold{max-width:1080px;}
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
      .d2d-st{display:flex;flex-direction:column;align-items:center;gap:5px;position:relative;
        background:none;border:0;padding:0;font:inherit;color:inherit;text-align:center;}
      button.d2d-st{cursor:pointer;border-radius:10px;transition:background .18s ease;}
      button.d2d-st:hover{background:rgba(16,18,27,.035);}
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
      @keyframes d2d-rise{from{opacity:0;transform:translateY(7px);}to{opacity:1;transform:none;}}36%,100%{opacity:0;transform:translateY(-9px);}}
      .d2d-pulse{animation:d2d-live 2.4s ease-in-out infinite;}
      @keyframes d2d-ping{0%{transform:scale(1);opacity:.35;}70%{transform:scale(2.2);opacity:0;}100%{opacity:0;}}
      .d2d-ping{animation:d2d-ping 2.8s ease-out infinite;transform-origin:center;transform-box:fill-box;}
      .d2d-halo{position:absolute;width:34px;height:34px;border-radius:50%;animation:d2d-halo 2.4s ease-out infinite;}
      .d2d-rise{animation:d2d-rise .42s cubic-bezier(.22,1,.36,1) both;}
      .d2d-lift{transition:transform .26s cubic-bezier(.22,1,.36,1),box-shadow .26s cubic-bezier(.22,1,.36,1);}
      .d2d-lift:hover{transform:translateY(-4px);box-shadow:0 14px 30px rgba(16,18,27,.13);}
      @media (prefers-reduced-motion:reduce){.d2d-pulse,.d2d-halo,.d2d-rise,.d2d-ping{animation:none;}.d2d-lift:hover{transform:none;}}
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

  // ── Page shell ──
  // Same structure as Live Map and Reports: a header card with the title and controls, then
  // the content. No overlay, no close button — it is a page.
  function shell() {
    const host = el('page-d2d'); if (!host) return null;
    if (el('d2d-body')) return el('d2d-body');
    // Same header tile as the Week Hub: white card, title and week on the left, status inline.
    // The black strip was a mockup device and does not appear anywhere else in Pinpoint.
    host.innerHTML = `
      <div class="d2d-head">
        <div id="d2d-header" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;
             padding:8px 16px;display:flex;align-items:center;justify-content:space-between;gap:12px;
             margin-bottom:8px;flex-wrap:wrap;">
          <div style="display:flex;align-items:center;gap:18px;flex:1;min-width:0;flex-wrap:wrap;">
            <div style="flex-shrink:0;">
              <div style="font-size:15px;font-weight:600;color:${DARK};letter-spacing:-.02em;line-height:1;">Door to door</div>
              <div style="font-size:9px;color:${LIGHT};margin-top:2px;" id="d2d-sub">Plan against actual</div>
            </div>
            <div class="d2d-tabs" id="d2d-tabs"></div>
          </div>
          <div style="display:flex;align-items:center;gap:12px;flex-shrink:0;">
            <span id="d2d-status" style="display:flex;align-items:center;gap:8px;font-size:12px;color:${MID};"></span>
            <span class="d2d-num" style="font-size:10.5px;color:${LIGHT};" id="d2d-scope"></span>
          </div>
        </div>
      </div>
      <div id="d2d-body"><div class="d2d-empty">Loading&hellip;</div></div>`;
    return el('d2d-body');
  }

  async function open() {
    // A bookmarked or leftover #d2d must not render for a client that does not have it.
    if (_enabled === false) {
      try { if (typeof window.show === 'function') window.show('#week-hub'); } catch (e) {}
      return;
    }
    styles();
    if (!shell()) return;
    await load();
  }

  // ── Data ──
  let _tab = 'shipments', _data = null, _internal = false, _mapFilter = 'all';

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
      if (e.status === 404 || e.status === 403) {
        // Not available for the active client — leave rather than show an error page.
        _enabled = false; paintNav();
        return;
      }
      const b = el('d2d-body');
      if (b) b.innerHTML = `<div class="d2d-empty" style="color:${BRAND}">Could not load. ${esc(e.message)}</div>`;
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
        what: 'Container number not advised', effect: 'The partner cannot report milestones without it', sort: 60 });
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
    // Nothing booked and nothing quoted means there is genuinely nothing to say — an empty
    // black bar reads as broken chrome, so the bar hides instead.
    if (!lines.length && (d.shipments.length || d.bookings.length)) lines.push('Everything on plan this week');
    return lines.slice(0, 3);
  }

  // ── Paint ──
  function paint() {
    const d = _data; if (!d) return;
    const scope = el('d2d-scope'); if (scope) scope.textContent = (window.pinpointClient || '') + (_internal ? ' · VelOzity view' : '');

    const tabs = [['shipments', 'Shipments'], ['bookings', 'Bookings']]
      .concat(_internal ? [['pricing', 'Pricing'], ['baselines', 'Transit rules']] : []);
    const tw = el('d2d-tabs');
    if (tw) {
      tw.innerHTML = tabs.map(([k, l]) => `<button class="d2d-tab ${_tab === k ? 'on' : ''}" data-tab="${k}">${l}</button>`).join('');
      tw.querySelectorAll('[data-tab]').forEach(b => b.onclick = () => { _tab = b.getAttribute('data-tab'); paint(); });
    }
    const lines = tickerLines(d);
    const st = el('d2d-status');
    if (st) {
      const worst = d.shipments.map(x => ({ od: overdueOf(x), slip: slipOf(x) }));
      const bad = worst.some(x => x.od || (x.slip || 0) > 0);
      st.innerHTML = lines.length
        ? `<span style="width:7px;height:7px;border-radius:50%;background:${bad ? BRAND : LIME};flex-shrink:0;"></span>
           <span style="color:${bad ? DARK : MID};">${esc(lines[0])}</span>
           ${lines.length > 1 ? `<span style="color:${LIGHT};">&middot; ${lines.length - 1} more</span>` : ''}`
        : '';
    }

    const body = el('d2d-body'); if (!body) return;
    if (!d.weeks.length) {
      body.innerHTML = `<div class="d2d-empty">No weeks booked yet.<br><span style="font-size:11.5px;">A week appears once an option has been approved.</span></div>`;
      return;
    }
    // The pill says what the week needs, not just how many shipments it has — a quoted week
    // with nothing approved has no shipments at all and would otherwise read "· 0".
    // Oldest to newest, left to right, like the Week Hub's date circles.
    const chips = [...d.weeks].sort((a, b) => a.week_start < b.week_start ? -1 : 1);
    const weekBar = `<div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap;">
      ${chips.map(w => {
        const dt = new Date(w.week_start + 'T00:00:00Z');
        const mon = isNaN(dt) ? '' : dt.toLocaleDateString('en-AU', { month: 'short', timeZone: 'UTC' }).toUpperCase();
        const dnum = isNaN(dt) ? w.week_start : dt.getUTCDate();
        const note = w.shipments ? w.shipments + (w.shipments === 1 ? ' shpt' : ' shpts')
          : w.awaiting ? w.awaiting + ' to decide' : w.drafts ? w.drafts + ' to price' : '';
        return `<button class="d2d-chip ${w.week_start === _week ? 'on' : ''}" data-w="${esc(w.week_start)}"
          aria-label="Week of ${esc(day(w.week_start))}">
          <span class="m">${esc(mon)}</span><span class="d">${esc(dnum)}</span><span class="n">${esc(note)}</span></button>`;
      }).join('')}
    </div>`;

    // Transit rules are not a property of a week, so the week bar is left off that tab.
    body.innerHTML = _tab === 'baselines' ? paintBaselines()
      : weekBar + (_tab === 'shipments' ? paintShipments(d) : _tab === 'bookings' ? paintBookings(d) : paintPricing(d));
    if (_tab === 'baselines') loadBaselines();
    body.querySelectorAll('[data-w]').forEach(b => b.onclick = () => { _week = b.getAttribute('data-w'); load(); });
    wireActions(body);
  }

  function paintShipments(d) {
    const inTransit = d.shipments.filter(s => s.status === 'in_transit').length;
    const delivered = d.shipments.filter(s => s.status === 'delivered').length;
    const slips = d.shipments.map(slipOf).filter(x => x != null && x > 0);
    const approved = d.bookings.find(b => b.status === 'approved');
    const acts = actions(d);
    const stat = (label, value, sub, colour) => `
      <div class="flex items-baseline justify-between py-2.5" style="border-top:.5px solid rgba(0,0,0,.05);">
        <span style="font-size:12px;color:${MID};">${label}</span>
        <span style="text-align:right;">
          <span style="display:block;font-size:15px;font-weight:600;color:${colour || DARK};">${value}</span>
          ${sub ? `<span style="display:block;font-size:10.5px;color:${LIGHT};">${sub}</span>` : ''}
        </span>
      </div>`;

    return `
      ${paintMap(d)}
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-3 items-start">
        <div class="lg:col-span-2" style="min-width:0;">
          ${d.shipments.length ? d.shipments.map(shipmentCard).join('')
            : `<div class="rounded-2xl border bg-white shadow-sm d2d-empty">Nothing booked for this week yet.</div>`}
        </div>

        <div style="display:flex;flex-direction:column;gap:12px;">
          <div class="rounded-2xl border bg-white shadow-sm d2d-rise" style="padding:14px 16px;">
            <div style="font-size:12.5px;font-weight:600;color:${DARK};margin-bottom:2px;">This week</div>
            ${stat('Shipments', d.shipments.length, null)}
            ${stat('In transit', inTransit, delivered + ' delivered')}
            ${stat('Behind plan', slips.length, slips.length ? 'worst ' + Math.max(...slips) + ' days' : 'all on plan', slips.length ? BRAND : DARK)}
            ${stat('Booked as', approved ? esc(approved.title || approved.option_ref) : '&ndash;',
                   approved ? esc([approved.carrier, approved.transit_days ? approved.transit_days + ' days' : ''].filter(Boolean).join(' · ')) : 'no approved option')}
          </div>

          ${paintArriving(d)}

          <div class="rounded-2xl border bg-white shadow-sm d2d-rise" style="padding:14px 16px;animation-delay:.06s;">
            <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:10px;">
              <span style="font-size:12.5px;font-weight:600;color:${DARK};">Next best action</span>
              <span style="font-size:10.5px;color:${LIGHT};">from this week's dates</span>
            </div>
            ${acts.map((a, i) => `<div class="d2d-nba" style="border-left-color:${a.accent};padding:9px 0 9px 11px;margin-bottom:${i === acts.length - 1 ? 0 : 8}px;">
              <span class="d2d-kind" style="color:${a.ink};">${esc(a.kind)}</span>
              <span style="font-size:12px;font-weight:600;color:${DARK};line-height:1.35;">${esc(a.what)}</span>
              <span style="font-size:10.5px;color:${MID};line-height:1.4;">${esc(a.effect)}</span>
            </div>`).join('')}
          </div>
        </div>
      </div>`;
  }

  // ── Live tracking ──
  // Follows the Live Map page: dotted basemap, a header strip with search and legend, named
  // nodes with counts. What is REAL here is the stage each shipment has reached, which places
  // it along its route. What is NOT real is a precise position at sea — that needs a carrier
  // feed, and the panel says so rather than implying GPS accuracy.
  const MAP = { w: 900, h: 430 };
  // Coarse landmasses for the Asia–Australia corridor, in map units.
  const LAND = [
    [[250,20],[470,10],[560,70],[600,130],[520,175],[430,160],[360,115],[280,100]],   // China
    [[140,95],[265,92],[300,150],[250,205],[165,185]],                                 // India
    [[555,215],[640,200],[700,240],[690,295],[610,305],[555,265]],                     // Philippines
    [[470,215],[560,235],[575,275],[500,290],[440,265]],                               // Indochina
    [[560,330],[790,315],[845,380],[800,425],[640,428],[575,390]],                     // Australia
  ];
  const PORTS = {
    origin: { x: 505, y: 120, label: 'Origin port' },
    transhipment: { x: 640, y: 262, label: 'Transhipment' },
    destination: { x: 700, y: 352, label: 'Port Botany' },
    customs: { x: 676, y: 386, label: 'Customs' },
    lastmile: { x: 735, y: 402, label: 'Last mile' },
  };
  // How far along the route each stage sits. Between recorded stages a shipment simply holds
  // its last known position: inventing motion between milestones would be a guess drawn as fact.
  const STAGE_T = { pickup: 0, origin_cleared: 0.04, departed: 0.10, arrived: 0.82,
                    dest_cleared: 0.90, out_for_delivery: 0.95, delivered: 1 };

  let _dots = null;
  function dotField() {
    if (_dots) return _dots;
    const inside = (pt, poly) => {
      let hit = false;
      for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
        const [xi, yi] = poly[i], [xj, yj] = poly[j];
        if ((yi > pt[1]) !== (yj > pt[1]) && pt[0] < (xj - xi) * (pt[1] - yi) / (yj - yi) + xi) hit = !hit;
      }
      return hit;
    };
    const out = [];
    for (let y = 8; y < MAP.h; y += 9) {
      for (let x = 8; x < MAP.w; x += 9) {
        if (LAND.some(poly => inside([x, y], poly))) out.push(`<circle cx="${x}" cy="${y}" r="1.5"/>`);
      }
    }
    _dots = `<g fill="#D6DAE1">${out.join('')}</g>`;
    return _dots;
  }

  // Quadratic curve from origin to destination, bowed the way a great-circle route looks here.
  const routeC = { x: 640, y: 210 };
  const atT = (t) => {
    const u = 1 - t, A = PORTS.origin, B = PORTS.destination;
    return { x: u * u * A.x + 2 * u * t * routeC.x + t * t * B.x,
             y: u * u * A.y + 2 * u * t * routeC.y + t * t * B.y };
  };
  const ROUTE_D = `M${PORTS.origin.x} ${PORTS.origin.y} Q${routeC.x} ${routeC.y} ${PORTS.destination.x} ${PORTS.destination.y}`;

  function shipmentPositions(list) {
    return list.map(sh => {
      const ev = evMap(sh);
      let last = null, lastStage = null;
      for (const [k, label] of STAGES) if (ev[k] && ev[k].actual_at) { last = k; lastStage = label; }
      const t = last ? STAGE_T[last] : 0;
      const slip = slipOf(sh);
      const od = overdueOf(sh);
      const colour = od || (slip != null && slip > 0) ? BRAND
                   : sh.status === 'delivered' ? LINK
                   : last ? LIME : LIGHT;
      return { sh, t, pos: atT(t), stage: lastStage || 'not yet collected', colour,
               note: od ? od.label + ' overdue' : (slip > 0 ? '+' + slip + ' days' : null) };
    });
  }

  function mapFiltered(list) {
    if (_mapFilter === 'sea') return list.filter(x => (x.mode || 'sea') === 'sea');
    if (_mapFilter === 'air') return list.filter(x => x.mode === 'air');
    if (_mapFilter === 'late') return list.filter(x => overdueOf(x) || (slipOf(x) || 0) > 0);
    return list;
  }

  function paintMap(d, opts) {
    const big = !!(opts && opts.big);
    const marks = shipmentPositions(mapFiltered(d.shipments));
    const moving = marks.filter(m => m.t > 0 && m.t < 1).length;
    const node = (pt, count, colour) => `
      <g>
        ${count ? `<circle cx="${pt.x}" cy="${pt.y}" r="9" fill="${colour}" opacity=".22" class="d2d-ping"/>` : ''}
        <circle cx="${pt.x}" cy="${pt.y}" r="4.5" fill="${count ? colour : '#fff'}" stroke="${count ? colour : '#AEB4BD'}" stroke-width="1.6"/>
        <text x="${pt.x}" y="${pt.y - 11}" text-anchor="middle" font-size="9.5" fill="${MID}"
              font-family="inherit">${esc(pt.label)}</text>
        ${count ? `<text x="${pt.x}" y="${pt.y + 17}" text-anchor="middle" font-size="9" fill="${colour}"
              font-family="ui-monospace,monospace">${count}</text>` : ''}
      </g>`;

    const atOrigin = marks.filter(m => m.t === 0).length;
    const atDest = marks.filter(m => m.t >= 0.82 && m.t < 0.9).length;
    const atCustoms = marks.filter(m => m.t >= 0.9 && m.t < 0.95).length;
    const delivered = marks.filter(m => m.t === 1).length;

    return `
      <div class="rounded-2xl border bg-white shadow-sm d2d-rise" style="overflow:hidden;margin-bottom:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;
             padding:12px 16px;border-bottom:.5px solid rgba(0,0,0,.07);">
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
            <span style="font-size:13px;font-weight:600;color:${DARK};">Live tracking</span>
            <span style="font-size:11px;color:${MID};">${moving} in transit &middot; ${d.shipments.length} this week</span>
            <span style="display:flex;gap:6px;">
              ${[['all', 'All'], ['sea', 'Sea'], ['air', 'Air'], ['late', 'Late only']].map(([k, l]) =>
                `<button class="d2d-filt ${_mapFilter === k ? 'on' : ''}" data-filt="${k}">${l}</button>`).join('')}
            </span>
          </div>
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
            ${[['On plan', LIME], ['Drifting', YELL], ['Late or held', BRAND], ['Delivered', LINK]].map(([l, c]) =>
              `<span style="font-size:10.5px;color:${MID};"><span style="display:inline-block;width:8px;height:8px;
                 border-radius:50%;background:${c};margin-right:5px;"></span>${l}</span>`).join('')}
            <button class="d2d-btn" data-mapfull="1" style="padding:7px 11px;min-height:36px;display:inline-flex;align-items:center;gap:6px;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M9 3H3v6"/><path d="M15 21h6v-6"/><path d="M3 3l7 7"/><path d="M21 21l-7-7"/></svg>
              Full screen</button>
          </div>
        </div>

        <div style="position:relative;background:#FBFCFD;">
          <svg viewBox="0 0 ${MAP.w} ${MAP.h}" width="100%" style="display:block;max-height:430px;" role="img"
               aria-label="Where this week's shipments are">
            ${dotField()}
            <path d="${ROUTE_D}" fill="none" stroke="#C9CED6" stroke-width="1.6" stroke-dasharray="5 6"/>
            ${node(PORTS.origin, atOrigin, LIME)}
            ${node(PORTS.destination, atDest, BRAND)}
            ${node(PORTS.customs, atCustoms, BRAND)}
            ${node(PORTS.lastmile, delivered, LINK)}
            ${marks.filter(m => m.t > 0 && m.t < 1).map(m => `
              <g>
                <circle cx="${m.pos.x.toFixed(1)}" cy="${m.pos.y.toFixed(1)}" r="11" fill="${m.colour}" opacity=".18" class="d2d-ping"/>
                <g transform="translate(${m.pos.x.toFixed(1)},${m.pos.y.toFixed(1)})">
                  <path d="M-7 3 L7 3 L5 7 L-5 7 Z M0 -7 L0 3 M0 -7 L5 1 L0 1" fill="none"
                        stroke="${m.colour}" stroke-width="1.7" stroke-linejoin="round"/>
                </g>
                <text x="${(m.pos.x + 14).toFixed(1)}" y="${(m.pos.y + 3).toFixed(1)}" font-size="10"
                      fill="${DARK}" font-family="ui-monospace,monospace">${esc(m.sh.reference || 'unadvised')}</text>
              </g>`).join('')}
          </svg>

          <div style="position:absolute;left:14px;bottom:12px;background:rgba(255,255,255,.92);border:.5px solid rgba(0,0,0,.08);
               border-radius:9px;padding:7px 11px;max-width:330px;">
            <span style="font-size:10.5px;color:${MID};line-height:1.45;display:block;">
              Positions follow the last recorded milestone. Exact vessel positions need carrier tracking
              &mdash; <b style="color:${DARK};">not yet connected</b>.</span>
          </div>
        </div>
      </div>`;
  }

  // Arriving next — soonest first, with the exception spelled out rather than a bare "late".
  function paintArriving(d) {
    const rows = d.shipments
      .filter(x => x.status !== 'delivered')
      .map(x => {
        const ev = evMap(x);
        const arrived = ev.arrived && ev.arrived.actual_at;
        const eta = arrived || x.plan_arrived;
        const od = overdueOf(x), slip = slipOf(x);
        return { x, eta, actual: !!arrived, od, slip,
                 colour: od || (slip || 0) > 0 ? BRAND : arrived ? LINK : LIME };
      })
      .filter(r => r.eta)
      .sort((a, b) => a.eta < b.eta ? -1 : 1)
      .slice(0, 5);
    if (!rows.length) return '';
    return `
      <div class="rounded-2xl border bg-white shadow-sm d2d-rise" style="padding:14px 16px;animation-delay:.12s;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:12.5px;font-weight:600;color:${DARK};">Arriving next</span>
          <span style="font-size:10.5px;color:${LIGHT};">by planned arrival</span>
        </div>
        ${rows.map(r => `
          <div style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-top:.5px solid rgba(0,0,0,.05);">
            <span style="width:7px;height:7px;border-radius:50%;background:${r.colour};margin-top:5px;flex-shrink:0;"></span>
            <span style="flex:1;min-width:0;">
              <span class="d2d-num" style="display:block;font-size:12px;color:${DARK};">${esc(r.x.reference || 'container not advised')}</span>
              <span style="display:block;font-size:10.5px;color:${MID};">${esc([r.x.container_type, r.x.carrier].filter(Boolean).join(' · '))}</span>
              ${r.od ? `<span style="display:inline-block;margin-top:3px;font-size:10.5px;font-weight:600;color:${BRAND};
                   background:rgba(153,0,51,.10);border-radius:6px;padding:2px 7px;">${esc(r.od.label)} overdue by ${r.od.days}d</span>` : ''}
            </span>
            <span style="text-align:right;flex-shrink:0;">
              <span class="d2d-num" style="display:block;font-size:11.5px;color:${r.colour};">${esc(day(r.eta))}</span>
              <span style="display:block;font-size:10px;color:${LIGHT};">${r.actual ? 'arrived' : 'planned'}</span>
            </span>
          </div>`).join('')}
      </div>`;
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
      const clickable = _internal;
      const tag = clickable ? 'button' : 'div';
      const attrs = clickable
        ? ` type="button" class="d2d-st d2d-edit" data-ship="${esc(s.id)}" data-stage="${k}" data-label="${esc(label)}"
            aria-label="Record ${esc(label)} for ${esc(s.reference || 'this shipment')}"`
        : ' class="d2d-st"';
      return `<${tag}${attrs}>
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
      </${tag}>`;
    }).join('');

    const slip = slipOf(s);
    const badge = s.status === 'delivered' ? ['Delivered', LINK, 'rgba(155,171,21,.20)']
                : od ? [od.label + ' overdue', '#fff', BRAND]
                : (slip != null && slip > 0) ? ['+' + slip + ' days', '#fff', BRAND]
                : s.status === 'in_transit' ? ['On plan', LINK, 'rgba(155,171,21,.20)']
                : ['Booked', MID, '#F2F2F5'];
    return `<div class="rounded-2xl border bg-white shadow-sm d2d-sh d2d-rise">
      <div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;">
        <div style="display:flex;align-items:baseline;gap:11px;">
          ${_internal
            ? `<button type="button" class="d2d-ref" data-ref="${esc(s.id)}" data-cur="${esc(s.reference || '')}"
                 style="font-family:ui-monospace,SFMono-Regular,monospace;font-size:14px;color:${s.reference ? DARK : YINK};
                        background:none;border:0;border-bottom:1px dashed rgba(0,0,0,.25);padding:0 0 1px;cursor:pointer;">
                 ${esc(s.reference || 'advise container number')}</button>`
            : `<span class="d2d-num" style="font-size:14px;color:${DARK};">${esc(s.reference || 'container not yet advised')}</span>`}
          <span style="font-size:11px;color:${MID};">${esc([s.container_type, s.carrier, s.vessel].filter(Boolean).join(' · '))}</span>
        </div>
        <span style="font-size:10.5px;font-weight:700;border-radius:6px;padding:3px 9px;color:${badge[1]};background:${badge[2]};">${esc(badge[0])}</span>
      </div>
      <div class="d2d-striphold"><div class="d2d-strip">${cells}</div></div>
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
        return `<div class="rounded-2xl border bg-white shadow-sm d2d-opt d2d-rise d2d-lift" style="border-top-color:${accent};animation-delay:${i * .05}s">
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
      <div class="rounded-2xl border bg-white shadow-sm" style="padding:0;overflow:hidden;margin-bottom:12px;">
        <div style="display:grid;grid-template-columns:1fr 110px 110px 104px 116px 128px;gap:0 12px;padding:11px 18px 8px;background:#FBFBFC;
             font-size:9.5px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">
          <span>Option</span><span style="text-align:right;">Partner cost</span><span style="text-align:right;">Our cost</span>
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

  // ── Transit rules ──
  // What turns an approved option into a plan. Editable because the numbers started as
  // plausible guesses, and a wrong baseline shows up as slip that never happened.
  const BL_FIELDS = [
    ['origin_cleared', 'Origin cleared', 'days after cargo ready'],
    ['departed', 'Departed', 'days after cargo ready'],
    ['dest_cleared', 'Destination cleared', 'days after arrival'],
    ['out_for_delivery', 'Out for delivery', 'days after arrival'],
    ['delivered', 'Delivered', 'days after arrival'],
  ];

  function paintBaselines() {
    return `
      <div style="max-width:880px;">
        <div style="font-size:13px;font-weight:600;color:${DARK};">How a plan is built</div>
        <div style="font-size:11.5px;color:${MID};margin:3px 0 14px;line-height:1.5;">
          Pickup is the cargo-ready date. Arrival is departure plus the transit time quoted on the option that was approved.
          Everything else comes from the rules below.
          <b style="color:${DARK};">Changing them affects bookings approved from now on — plans already frozen never move.</b>
        </div>
        <div id="d2d-bl">${`<div class="d2d-empty">Loading&hellip;</div>`}</div>
      </div>`;
  }

  async function loadBaselines() {
    const host = el('d2d-bl'); if (!host) return;
    let rows = [];
    try { rows = (await api('/d2d/baselines')).baselines || []; }
    catch (e) { host.innerHTML = `<div class="d2d-empty" style="color:${BRAND}">Could not load (${esc(e.message)}).</div>`; return; }

    host.innerHTML = rows.map(b => `
      <div class="rounded-2xl border bg-white shadow-sm" style="padding:15px 18px;margin-bottom:12px;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:12px;">
          <span style="font-size:14px;font-weight:700;color:${DARK};text-transform:capitalize;">${esc(b.mode)} freight</span>
          <span style="font-size:11px;color:${LIGHT};">${b.updated_by === 'default' ? 'never edited — starting values' : 'updated by ' + esc(b.updated_by || '')}</span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;">
          ${BL_FIELDS.map(([f, label, hint]) => `
            <div>
              <label for="bl-${esc(b.mode)}-${f}" style="display:block;font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">${label}</label>
              <input id="bl-${esc(b.mode)}-${f}" type="number" min="0" max="60" step="1" value="${esc(b[f])}"
                     data-bl="${esc(b.mode)}" data-f="${f}"
                     style="width:100%;box-sizing:border-box;font:inherit;font-size:14px;text-align:right;border:.5px solid rgba(0,0,0,.18);
                            border-radius:8px;padding:9px;min-height:44px;margin-top:4px;">
              <span style="display:block;font-size:10px;color:${MID};margin-top:3px;">${hint}</span>
            </div>`).join('')}
        </div>
        <div style="display:flex;align-items:center;gap:12px;margin-top:13px;">
          <button class="d2d-btn dark" data-blsave="${esc(b.mode)}">Save ${esc(b.mode)} rules</button>
          <span style="font-size:11px;color:${MID};" id="bl-msg-${esc(b.mode)}"></span>
        </div>
      </div>`).join('');

    host.querySelectorAll('[data-blsave]').forEach(btn => btn.onclick = async () => {
      const mode = btn.getAttribute('data-blsave');
      const body = {};
      host.querySelectorAll(`[data-bl="${mode}"]`).forEach(i => { body[i.getAttribute('data-f')] = Number(i.value); });
      const msg = el('bl-msg-' + mode);
      btn.disabled = true; btn.style.opacity = '.6';
      try {
        const out = await api('/d2d/baselines/' + mode, { method: 'PUT', body: JSON.stringify(body) });
        msg.style.color = LINK;
        msg.textContent = `Saved. Applies from the next approval; ${out.unchanged_plans} existing plan${out.unchanged_plans === 1 ? '' : 's'} unchanged.`;
      } catch (e) {
        msg.style.color = BRAND;
        msg.textContent = 'Not saved: ' + e.message;
      }
      btn.disabled = false; btn.style.opacity = '';
    });
  }

  function money(v, cur) {
    if (v == null || v === '') return '&ndash;';
    const n = Number(v); if (!isFinite(n)) return '&ndash;';
    return (cur && cur !== 'USD' ? cur + ' ' : '$') + n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  // The same map, larger. Reusing the renderer means the two can never drift apart.
  function openFullMap() {
    if (el('d2d-fullmap')) return;
    const ov = document.createElement('div');
    ov.id = 'd2d-fullmap';
    ov.style.cssText = 'position:fixed;inset:0;z-index:9600;background:#fff;display:flex;flex-direction:column;';
    ov.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 22px;border-bottom:.5px solid rgba(0,0,0,.08);">
        <div>
          <div style="font-size:16px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Live tracking</div>
          <div style="font-size:11px;color:${MID};margin-top:1px;">Week of ${esc(day(_week))}</div>
        </div>
        <button class="d2d-btn" id="d2d-fullclose" aria-label="Close full screen map">Close</button>
      </div>
      <div style="flex:1;overflow:auto;padding:16px 22px;">${paintMap(_data, { big: true })}</div>`;
    document.body.appendChild(ov);
    el('d2d-fullclose').onclick = () => ov.remove();
    wireActions(ov);
    const onKey = (e) => { if (e.key === 'Escape') { ov.remove(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);
  }

  // ── Recording a milestone ──
  // A small popover anchored to the stage. Deliberately not a modal: the strip behind it is
  // the context, and hiding it to ask for one date would be a worse trade.
  function closeEditor() { const e = el('d2d-pop'); if (e) e.remove(); }

  function openEditor(btn) {
    closeEditor();
    const ship = btn.getAttribute('data-ship'), stage = btn.getAttribute('data-stage'), label = btn.getAttribute('data-label');
    const card = _data.shipments.find(x => x.id === ship) || {};
    const existing = (card.events || []).find(e => e.stage === stage);
    const plan = card['plan_' + stage] || '';
    const r = btn.getBoundingClientRect();

    const pop = document.createElement('div');
    pop.id = 'd2d-pop';
    pop.style.cssText = `position:fixed;z-index:9600;background:#fff;border:.5px solid rgba(0,0,0,.14);border-radius:12px;
      box-shadow:0 18px 40px rgba(16,18,27,.18);padding:14px;width:250px;font-family:inherit;`;
    pop.innerHTML = `
      <div style="font-size:12.5px;font-weight:600;color:${DARK};">${esc(label)}</div>
      <div style="font-size:11px;color:${MID};margin:2px 0 10px;">${plan ? 'Planned ' + day(plan) : 'No planned date'}</div>
      <label for="d2d-date" style="display:block;font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">Actual date</label>
      <input id="d2d-date" type="date" value="${esc(existing && existing.actual_at ? existing.actual_at : '')}"
             style="width:100%;box-sizing:border-box;font:inherit;font-size:12.5px;border:.5px solid rgba(0,0,0,.18);border-radius:8px;padding:9px;min-height:44px;">
      <div id="d2d-var" style="font-size:11px;margin-top:6px;min-height:15px;"></div>
      <div style="display:flex;gap:8px;margin-top:8px;">
        <button class="d2d-btn dark" id="d2d-save" style="flex:1;">Save</button>
        ${existing ? `<button class="d2d-btn" id="d2d-clear">Clear</button>` : ''}
      </div>
      ${existing ? `<div style="font-size:10.5px;color:${MID};margin-top:8px;">Recorded by ${esc(existing.source)}</div>` : ''}`;
    document.body.appendChild(pop);
    const top = Math.min(r.bottom + 8, window.innerHeight - pop.offsetHeight - 12);
    pop.style.top = Math.max(12, top) + 'px';
    pop.style.left = Math.max(12, Math.min(r.left + r.width / 2 - 125, window.innerWidth - 262)) + 'px';

    const dateEl = el('d2d-date'), varEl = el('d2d-var');
    // Show the variance while typing, so a slip is visible before it is saved.
    const showVar = () => {
      const v = dateEl.value;
      if (!v || !plan) { varEl.textContent = ''; return; }
      const d = daysBetween(plan, v);
      varEl.style.color = d > 0 ? BRAND : LINK;
      varEl.textContent = d > 0 ? `${d} day${d === 1 ? '' : 's'} behind plan` : (d === 0 ? 'On plan' : `${-d} day${d === -1 ? '' : 's'} early`);
    };
    dateEl.oninput = showVar; showVar();

    const send = async (body, b) => {
      if (b) { b.disabled = true; b.style.opacity = '.6'; }
      try { await api('/d2d/shipments/' + ship + '/events', { method: 'POST', body: JSON.stringify(body) }); closeEditor(); await load(); }
      catch (e) { if (b) { b.disabled = false; b.style.opacity = ''; } alert('Could not save: ' + e.message); }
    };
    el('d2d-save').onclick = () => {
      if (!dateEl.value) { alert('Pick a date, or use Clear to remove it.'); return; }
      send({ stage, actual_at: dateEl.value, source: 'manual' }, el('d2d-save'));
    };
    const clr = el('d2d-clear');
    if (clr) clr.onclick = () => send({ stage, clear: true }, clr);
    setTimeout(() => dateEl.focus(), 30);
  }

  function openRefEditor(btn) {
    closeEditor();
    const ship = btn.getAttribute('data-ref'), cur = btn.getAttribute('data-cur') || '';
    const r = btn.getBoundingClientRect();
    const pop = document.createElement('div');
    pop.id = 'd2d-pop';
    pop.style.cssText = `position:fixed;z-index:9600;background:#fff;border:.5px solid rgba(0,0,0,.14);border-radius:12px;
      box-shadow:0 18px 40px rgba(16,18,27,.18);padding:14px;width:262px;font-family:inherit;`;
    pop.innerHTML = `
      <div style="font-size:12.5px;font-weight:600;color:${DARK};">Container and vessel</div>
      <div style="font-size:11px;color:${MID};margin:2px 0 10px;">The partner can advise this themselves</div>
      <label for="d2d-refin" style="display:block;font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">Container number</label>
      <input id="d2d-refin" type="text" value="${esc(cur)}" placeholder="ABCD1234567"
             style="width:100%;box-sizing:border-box;font:inherit;font-size:12.5px;border:.5px solid rgba(0,0,0,.18);border-radius:8px;padding:9px;min-height:44px;text-transform:uppercase;">
      <label for="d2d-vesin" style="display:block;font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;margin:9px 0 4px;">Vessel</label>
      <input id="d2d-vesin" type="text" placeholder="ONE Olympus 041E"
             style="width:100%;box-sizing:border-box;font:inherit;font-size:12.5px;border:.5px solid rgba(0,0,0,.18);border-radius:8px;padding:9px;min-height:44px;">
      <button class="d2d-btn dark" id="d2d-refsave" style="width:100%;margin-top:10px;">Save</button>`;
    document.body.appendChild(pop);
    pop.style.top = Math.max(12, Math.min(r.bottom + 8, window.innerHeight - pop.offsetHeight - 12)) + 'px';
    pop.style.left = Math.max(12, Math.min(r.left, window.innerWidth - 274)) + 'px';
    el('d2d-refsave').onclick = async () => {
      const b = el('d2d-refsave'); b.disabled = true; b.style.opacity = '.6';
      try {
        await api('/d2d/shipments/' + ship, { method: 'PATCH', body: JSON.stringify({
          reference: el('d2d-refin').value.trim().toUpperCase(),
          vessel: el('d2d-vesin').value.trim() || undefined }) });
        closeEditor(); await load();
      } catch (e) { b.disabled = false; b.style.opacity = ''; alert('Could not save: ' + e.message); }
    };
    setTimeout(() => el('d2d-refin').focus(), 30);
  }

  document.addEventListener('click', (e) => {
    const pop = el('d2d-pop');
    if (pop && !pop.contains(e.target) && !e.target.closest('.d2d-edit') && !e.target.closest('.d2d-ref')) closeEditor();
  }, true);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeEditor(); });

  // ── Actions ──
  function wireActions(root) {
    root.querySelectorAll('[data-filt]').forEach(b => b.onclick = () => { _mapFilter = b.getAttribute('data-filt'); paint(); });
    const full = root.querySelector('[data-mapfull]');
    if (full) full.onclick = () => openFullMap();
    root.querySelectorAll('.d2d-edit').forEach(b => b.onclick = () => openEditor(b));
    root.querySelectorAll('.d2d-ref').forEach(b => b.onclick = () => openRefEditor(b));
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
  // Whether this client has a Week Hub is read from the nav, and the tenancy module hides that
  // link with style.display AFTER its whoami resolves — later than this module boots. A
  // childList observer never sees that: hiding an element is an ATTRIBUTE change. The result
  // was a client with no Week Hub still showing the Week Hub, because the one check ran too
  // early and nothing looked again.
  const boot = () => { injectNav(); refreshEnabled().catch(e => console.warn('[d2d-hub] enable check failed', e)); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);

  const mo = new MutationObserver(() => { try { injectNav(); } catch (e) { console.error('[d2d-hub] nav update failed', e); } });
  const start = () => {
    mo.observe(document.body, { childList: true, subtree: true });
    // Attribute changes on the nav itself — this is what catches the link being hidden.
    const nav = el('pn-nav-items');
    if (nav) mo.observe(nav, { attributes: true, subtree: true, attributeFilter: ['style', 'class'] });
  };
  if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);
  window.addEventListener('focus', () => { refreshEnabled().catch(() => {}); }, { passive: true });

  // The router calls this when #d2d is opened.
  window.renderD2D = () => { open().catch(e => console.error('[d2d-hub] render failed', e)); };
  window.__openD2D = open;
  console.log('[d2d-hub] v9 loaded');
})();
