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
  // ISO week number: freight is planned and talked about in weeks, not dates.
  function isoWeek(ymd) {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    t.setUTCDate(t.getUTCDate() + 4 - (t.getUTCDay() || 7));       // Thursday decides the year
    const start = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
    return Math.ceil(((t - start) / 86400000 + 1) / 7);
  }

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
      .d2d-tabs{display:flex;align-items:center;}
      .d2d-tabs .d2d-tab + .d2d-tab{margin-left:20px;}
      .d2d-cells > span:first-child{border-left:0 !important;}
      /* One in-flight row: arrival, identity, its own lane, units, outlook. */
      .d2d-frow{display:grid;grid-template-columns:88px 148px minmax(0,1fr) 86px 92px;gap:0 14px;align-items:center;
        width:100%;text-align:left;background:none;border:0;border-top:.5px solid rgba(0,0,0,.05);
        padding:9px 16px;cursor:pointer;font-family:inherit;transition:background .18s ease;}
      .d2d-frow:hover{background:#FAFBFC;}
      .d2d-fhead{cursor:default;border-top:0;padding-top:6px;padding-bottom:2px;}
      .d2d-fhead:hover{background:none;}
      .d2d-lane{position:relative;height:24px;}
      .d2d-rail{position:absolute;left:0;right:0;top:10px;height:3px;background:#EDEFF3;border-radius:2px;}
      .d2d-done{position:absolute;left:0;top:10px;height:3px;border-radius:2px;}
      .d2d-node{position:absolute;top:6px;width:10px;height:10px;border-radius:50%;background:#fff;
        border:2px solid #DDE1E7;transform:translateX(-50%);box-sizing:border-box;}
      .d2d-here{position:absolute;top:1px;transform:translateX(-50%);width:20px;height:20px;border-radius:50%;
        background:#fff;display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 2px #fff;}
      .d2d-ends{position:absolute;top:-3px;font-size:8.5px;color:#C2C6CD;}

      /* Slow on purpose: a sheen along the leg being travelled and one breath on the marker.
         Anything faster becomes wallpaper on a page left open all day. */
      @keyframes d2d-drift{from{background-position:120% 0;}to{background-position:-20% 0;}}
      .d2d-travel{background-image:linear-gradient(90deg,transparent 0%,rgba(255,255,255,.85) 45%,transparent 90%);
        background-size:220% 100%;animation:d2d-drift 4.2s linear infinite;}
      @keyframes d2d-breathe{0%,100%{transform:translateX(-50%) scale(1);}50%{transform:translateX(-50%) scale(1.09);}}
      .d2d-breathe{animation:d2d-breathe 3.4s ease-in-out infinite;}
      @media (prefers-reduced-motion:reduce){.d2d-travel,.d2d-breathe{animation:none;}}

      .d2d-seg{border:.5px solid rgba(0,0,0,.14);background:#fff;color:${MID};border-radius:8px;padding:5px 11px;
        font-family:inherit;font-weight:600;font-size:11px;cursor:pointer;min-height:32px;
        transition:background .18s ease,color .18s ease,border-color .18s ease;}
      .d2d-seg:hover{border-color:rgba(0,0,0,.3);}
      .d2d-seg.on{background:${BRAND};border-color:${BRAND};color:#fff;}
      .d2d-crumb{border:0;background:none;font:inherit;font-size:11.5px;color:${BLUE};cursor:pointer;padding:0;}
      .d2d-crumb:hover{text-decoration:underline;}
      /* Controls, not content: smaller and quieter than the legend, which is what a reader
         actually needs to decode the map. 8px is as small as this face stays legible. */
      .d2d-filt{border:.5px solid rgba(0,0,0,.13);background:#fff;color:${MID};border-radius:5px;padding:2px 6px;
        font-family:inherit;font-weight:600;font-size:7.5px;letter-spacing:.03em;text-transform:uppercase;
        cursor:pointer;line-height:1.6;
        transition:border-color .18s ease,background .18s ease,color .18s ease;}
      .d2d-filt:hover{border-color:rgba(0,0,0,.3);}
      .d2d-filt.on{background:${DARK};border-color:${DARK};color:#fff;}
      .d2d-tab{border:0;background:none;font-family:inherit;font-weight:600;font-size:13px;color:${MID};
        cursor:pointer;padding:6px 0;border-bottom:2px solid transparent;}
      .d2d-tab.on{color:${DARK};border-bottom-color:${BRAND};}
      /* Week chips, matching the Week Hub's date circles rather than generic pills. */
      .d2d-chip{width:46px;height:46px;border-radius:50%;border:1.5px solid rgba(0,0,0,.10);background:#fff;
        display:flex;flex-direction:column;align-items:center;justify-content:center;gap:1px;cursor:pointer;
        font:inherit;color:${DARK};transition:border-color .18s ease,transform .18s cubic-bezier(.22,1,.36,1);flex-shrink:0;}
      .d2d-chip:hover{border-color:rgba(0,0,0,.28);transform:translateY(-2px);}
      .d2d-chip.on{border-color:${BRAND};border-width:2px;}
      .d2d-chip .m{font-size:9px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:${LIGHT};}
      .d2d-chip.on .m{color:${BRAND};}
      .d2d-chip .d{font-size:14px;font-weight:600;line-height:1;}
      .d2d-chip .n{display:none;}
      .d2d-chip .m{font-size:7.5px;}
      /* The strip is capped so stages sit a sensible distance apart on a wide monitor. */
      .d2d-striphold{max-width:1080px;}
      .d2d-card{border:.5px solid rgba(16,18,27,.08);border-radius:14px;background:#fff;
        box-shadow:0 1px 2px rgba(16,18,27,.04),0 4px 12px rgba(16,18,27,.05);}
      /* The same glass as the Week Hub tiles: a travelling specular sheen, an ambient bloom
         from the light source at top-left, and a body gradient that cools toward the bottom.
         Copied rather than approximated so the two cannot drift apart. */
      #page-d2d .rounded-2xl.bg-white, #d2d-drawer .d2d-glass{
        background-image:
          linear-gradient(115deg, rgba(255,255,255,0) 28%, rgba(255,255,255,.75) 42%,
            rgba(255,255,255,.95) 48%, rgba(255,255,255,.75) 54%, rgba(255,255,255,0) 68%),
          radial-gradient(120% 90% at 8% 0%, rgba(255,255,255,.95) 0%, rgba(255,255,255,0) 60%),
          linear-gradient(168deg, #ffffff 0%, #ffffff 46%, #f7f9fc 100%);
        background-size:220% 220%,100% 100%,100% 100%;
        background-position:100% 0%,0% 0%,0% 0%;
        background-repeat:no-repeat;
        box-shadow:
          inset 0 1px 0 rgba(255,255,255,1),
          inset 1px 0 0 rgba(255,255,255,.65),
          inset -1px 0 0 rgba(255,255,255,.5),
          inset 0 -1px 0 rgba(16,18,27,.055),
          inset 0 18px 30px -20px rgba(255,255,255,1),
          0 1px 2px rgba(16,18,27,.045), 0 4px 12px rgba(16,18,27,.055);
        border-color:rgba(16,18,27,.08);
        transition:box-shadow .28s cubic-bezier(.22,1,.36,1), transform .28s cubic-bezier(.22,1,.36,1),
          border-color .28s cubic-bezier(.22,1,.36,1), background-position .55s cubic-bezier(.22,1,.36,1);
      }
      /* The lift and the shadow grow together, at the same -6px as every other Pinpoint tile.
         At -4px with a smaller shadow the cards looked stuck to the page. */
      #page-d2d .rounded-2xl.bg-white:hover{
        transform:translateY(-6px);
        background-position:0% 0%,0% 0%,0% 0%;
        box-shadow:
          inset 0 1px 0 rgba(255,255,255,1),
          inset 1px 0 0 rgba(255,255,255,.8),
          inset -1px 0 0 rgba(255,255,255,.65),
          inset 0 -1px 0 rgba(16,18,27,.06),
          inset 0 22px 36px -20px rgba(255,255,255,1),
          0 2px 4px rgba(16,18,27,.06), 0 18px 38px rgba(16,18,27,.13);
        border-color:rgba(16,18,27,.14);
      }
      @media (prefers-reduced-motion:reduce){#page-d2d .rounded-2xl.bg-white:hover{transform:none;}}
      @media (prefers-reduced-motion:reduce){#page-d2d .rounded-2xl.bg-white{transition:none;}}
      .d2d-grid4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}
      .d2d-tile{padding:14px 16px;}
      .d2d-tl{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;}
      .d2d-tv{font-size:25px;font-weight:700;color:${DARK};margin-top:3px;letter-spacing:-.02em;}
      .d2d-ts{font-size:11px;color:${MID};}
      .d2d-wk{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;}
      .d2d-wkb,.d2d-btn{border:.5px solid rgba(0,0,0,.16);background:#fff;color:${DARK};border-radius:9px;padding:9px 13px;
        font-family:inherit;font-weight:600;font-size:12px;cursor:pointer;min-height:44px;
        transition:border-color .2s ease,background .2s ease;}
      .d2d-wkb:hover,.d2d-btn:hover{border-color:rgba(0,0,0,.34);background:#F7F8FA;}
      .d2d-wkb.on{background:${DARK};color:#fff;border-color:${DARK};}
      .d2d-btn.dark{background:${DARK};color:#fff;border-color:${DARK};}
      .d2d-sh{padding:15px 18px;margin-bottom:12px;}
      .d2d-strip{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));margin-top:14px;}
      .d2d-st{display:flex;flex-direction:column;align-items:center;gap:5px;position:relative;
        background:none;border:0;padding:0;font:inherit;color:inherit;text-align:center;}
      button.d2d-st{cursor:pointer;border-radius:10px;transition:background .18s ease;}
      button.d2d-st:hover{background:rgba(16,18,27,.035);}
      .d2d-line{position:absolute;top:43px;height:3px;}   /* 22px label + 5px gap + half of the 34px dot */
      .d2d-dot{width:34px;height:34px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;
        border:2px solid;box-sizing:border-box;position:relative;z-index:1;}
      .d2d-nba{border-left:3px solid;padding:12px 14px;display:flex;flex-direction:column;gap:5px;}
      .d2d-kind{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;}
      .d2d-opt{padding:15px 17px;display:flex;flex-direction:column;gap:10px;border-top:3px solid;}
      .d2d-num{font-family:ui-monospace,SFMono-Regular,monospace;}
      .d2d-in2{display:block;width:100%;box-sizing:border-box;font:inherit;font-size:12.5px;color:${DARK};
        border:.5px solid rgba(0,0,0,.18);border-radius:8px;padding:8px 9px;min-height:38px;margin-top:4px;background:#fff;
        text-transform:none;letter-spacing:normal;}
      .d2d-in2:focus{outline:2px solid rgba(44,111,187,.35);outline-offset:1px;}
      .d2d-in{width:64px;font:inherit;font-size:12.5px;text-align:right;border:.5px solid rgba(0,0,0,.18);border-radius:7px;padding:7px 8px;}
      .d2d-empty{padding:44px;text-align:center;color:${MID};font-size:13px;}
      @keyframes d2d-live{0%,100%{transform:scale(1);}50%{transform:scale(1.13);}}
      @keyframes d2d-halo{0%{transform:scale(1);opacity:.45;}75%{transform:scale(2.1);opacity:0;}100%{opacity:0;}}
      @keyframes d2d-rise{from{opacity:0;transform:translateY(7px);}to{opacity:1;transform:none;}}36%,100%{opacity:0;transform:translateY(-9px);}}
      .d2d-pulse{animation:d2d-live 2.4s ease-in-out infinite;}
      /* The leg being travelled drifts forward — a slow, quiet signal that this row is live. */
      @keyframes d2d-travel{from{background-position:100% 0;}to{background-position:0% 0;}}
      .d2d-travel{animation:d2d-travel 3.4s linear infinite;}
      @keyframes d2d-sonar{0%{transform:scale(.7);opacity:.7;}80%{transform:scale(2.4);opacity:0;}100%{opacity:0;}}
      .d2d-sonar{transform-box:fill-box;transform-origin:center;animation:d2d-sonar 3.5s ease-out infinite;}
      @keyframes d2d-ping{0%{transform:scale(1);opacity:.35;}70%{transform:scale(2.2);opacity:0;}100%{opacity:0;}}
      .d2d-ping{animation:d2d-ping 2.8s ease-out infinite;transform-origin:center;transform-box:fill-box;}
      .d2d-halo{position:absolute;width:34px;height:34px;border-radius:50%;animation:d2d-halo 2.4s ease-out infinite;}
      .d2d-rise{animation:d2d-rise .42s cubic-bezier(.22,1,.36,1) both;}
      .d2d-lift{}
      @media (prefers-reduced-motion:reduce){.d2d-pulse,.d2d-halo,.d2d-rise,.d2d-ping,.d2d-travel,.d2d-sonar{animation:none;}.d2d-lift:hover{transform:none;}}
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
          <div style="display:flex;align-items:center;gap:22px;flex-shrink:0;">
            <div style="flex-shrink:0;">
              <div style="font-size:15px;font-weight:600;color:${DARK};letter-spacing:-.02em;line-height:1;">Door to door</div>
              <div style="font-size:9px;color:${LIGHT};margin-top:2px;" id="d2d-sub">Plan against actual</div>
            </div>
            <div class="d2d-tabs" id="d2d-tabs"></div>
            <span id="d2d-weeks" style="display:flex;align-items:center;gap:8px;"></span>
          </div>
          <!-- The live line sits between the tabs and the account, where the eye lands. -->
          <span id="d2d-status" style="display:flex;align-items:center;gap:8px;font-size:12px;color:${MID};
                flex:1;min-width:0;justify-content:center;"></span>
          <span class="d2d-num" style="font-size:10.5px;color:${LIGHT};flex-shrink:0;" id="d2d-scope"></span>
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
  let _tab = 'shipments', _data = null, _internal = false, _mapFilter = 'all', _mapScope = 'live';
  // Which screen inside the Shipments tab: the dashboard, one week, or one container.
  let _view = 'dashboard', _openId = null;
  // The main panel shows one of two things: the list of what is in flight, or the same fleet
  // on a map. They answer the same question two ways, so they share the space and the toggle.
  // The rail keeps its tiles and updates whichever is showing.
  let _main = 'flight';
  const go = (view, id) => { _view = view; _openId = id || null; paint(); window.scrollTo({ top: 0, behavior: 'smooth' }); };

  async function load() {
    // The FIRST call decides availability: a 404 there means this client has no door-to-door.
    // A 404 on a later call means that week or list is empty, and must not tear down the page
    // — treating every 404 the same emptied the whole page when a week had no shipments.
    let weeks;
    try {
      weeks = (await api('/d2d/weeks')).weeks || [];
    } catch (e) {
      if (e.status === 404 || e.status === 403) { _enabled = false; paintNav(); return; }
      const b0 = el('d2d-body');
      if (b0) b0.innerHTML = `<div class="d2d-empty" style="color:${BRAND}">Could not load. ${esc(e.message)}</div>`;
      return;
    }

    // Fetch the geography alongside the data; the map renders with whichever is ready and
    // re-renders once the world arrives, so a slow file never blocks the page.
    loadWorld().then(w => { if (w) paint(); }).catch(() => {});

    if (!_week && weeks.length) _week = weeks[0].week_start;
    let shipments = [], bookings = [], pricingVisible = _internal, allShipments = [];
    const soft = async (path, fallback) => {
      try { return await api(path); } catch (e) { console.warn('[d2d-hub]', path, e.message); return fallback; }
    };
    if (_week) {
      shipments = (await soft('/d2d/shipments?week=' + encodeURIComponent(_week), { shipments: [] })).shipments || [];
      const bk = await soft('/d2d/bookings?week=' + encodeURIComponent(_week), { bookings: [], pricing_visible: _internal });
      bookings = bk.bookings || []; pricingVisible = !!bk.pricing_visible;
    }
    // Everything, for the map: a vessel in flight belongs to no particular week.
    allShipments = (await soft('/d2d/shipments', { shipments })).shipments || shipments;
    const po = _week ? await soft('/d2d/po?week=' + encodeURIComponent(_week), { orders: [] }) : { orders: [] };
    const rq = _week ? await soft('/d2d/requests?week=' + encodeURIComponent(_week), { requests: [] }) : { requests: [] };

    _internal = pricingVisible;
    _data = { weeks, shipments, bookings, allShipments, orders: po.orders || [], requests: rq.requests || [] };
    paint();
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
    // An action is only useful if you can tell what it is about. "Chase origin clearance"
    // against forty containers is a to-do; against ONEU7654321 in week 41 it is a task.
    const where = (sh) => [sh.reference || 'container not advised',
                           'week ' + (isoWeek(sh.week_start) || day(sh.week_start))].join(' · ');

    for (const sh of mapSource(d)) {
      const od = overdueOf(sh);
      const slip = slipOf(sh);
      if (od) out.push({ kind: 'Overdue', accent: BRAND, ink: BRAND, who: 'the partner',
        what: DELAY_LABEL[od.stage] || `${od.label} delayed`,
        where: where(sh),
        effect: `Planned ${day(sh['plan_' + od.stage])} — ${od.days} day${od.days === 1 ? '' : 's'} ago, nothing reported since`,
        next: 'Chase the partner for the date, or record it if you have it',
        id: sh.id, sort: 100 + od.days });
      else if (slip != null && slip > 0 && sh.status !== 'delivered') out.push({ kind: 'Behind plan', accent: BRAND, ink: BRAND, who: 'the carrier',
        what: `Running ${slip} day${slip === 1 ? '' : 's'} behind`,
        where: where(sh),
        effect: `Arrival moves to about ${day(addDaysISO(sh.plan_arrived, slip))}`,
        next: slip > 4 ? 'Warn the client and look at the last-mile booking' : 'Recoverable before delivery — watch the next stage',
        id: sh.id, sort: 70 + slip });
      else if (!sh.reference && sh.status !== 'delivered') out.push({ kind: 'Missing', accent: YELL, ink: YINK, who: 'the partner',
        what: 'Container number not advised',
        where: 'week ' + (isoWeek(sh.week_start) || day(sh.week_start)) + ' · ' + (sh.container_type || 'container'),
        effect: 'The partner cannot report milestones without it',
        next: 'Ask the partner to advise it, or enter it on the shipment',
        id: sh.id, sort: 60 });
    }

    const released = d.bookings.filter(b => b.status === 'released');
    if (released.length) out.push({ kind: 'Undecided', accent: BLUE, ink: BLUE, who: 'the client',
      what: `${released.length} option${released.length === 1 ? '' : 's'} awaiting a decision`,
      where: 'week ' + (isoWeek(_week) || day(_week)),
      effect: 'Space is held until the cut-off',
      next: 'Nudge the client — the rate expires with the sailing', tab: 'bookings', sort: 90 });

    const drafts = d.bookings.filter(b => b.status === 'draft');
    if (drafts.length && _internal) out.push({ kind: 'Unpriced', accent: YELL, ink: YINK, who: 'you',
      what: `${drafts.length} option${drafts.length === 1 ? '' : 's'} not yet released`,
      where: 'week ' + (isoWeek(_week) || day(_week)),
      effect: 'The client cannot see them until they are released',
      next: 'Set the margin on Pricing, then release', tab: 'pricing', sort: 80 });

    // Waiting on somebody is an exception too — it was in its own panel answering the same
    // question, so neither list was complete.
    const awaitingPartner = (d.requests || []).filter(r => ['sent', 'repricing'].includes(r.state)).length;
    if (awaitingPartner) out.push({ kind: 'Awaiting rates', accent: BLUE, ink: BLUE, who: 'the partner',
      what: `${awaitingPartner} rate request${awaitingPartner === 1 ? '' : 's'} out`,
      where: 'week ' + (isoWeek(_week) || day(_week)),
      effect: 'Nothing can be priced until they come back',
      next: 'Chase the partner, or resend the link', tab: 'bookings', sort: 85 });

    const unassigned = (d.orders || []).filter(x => !(x.containers || []).length).length;
    if (unassigned && _internal) out.push({ kind: 'Unassigned', accent: YELL, ink: YINK, who: 'you',
      what: `${unassigned} order${unassigned === 1 ? '' : 's'} not on a container`,
      where: 'week ' + (isoWeek(_week) || day(_week)),
      effect: 'They will not appear against any shipment',
      next: 'Name the container in the order file, or assign it here', tab: 'po', sort: 50 });

    if (!out.length) out.push({ kind: 'Clear', accent: LIME, ink: LINK, who: '',
      what: 'No exceptions', where: '', effect: 'Every stage is on plan or recorded', next: '', sort: 0 });

    // Three containers stuck at the same stage is one problem, not three cards. They collapse
    // into a single card with a count; two or fewer stay named, because naming them is useful.
    const grouped = [], byKey = {};
    for (const a of out.sort((x, y) => y.sort - x.sort)) {
      const key = a.id ? a.kind + '|' + a.what : null;      // only per-shipment ones group
      if (!key) { grouped.push(a); continue; }
      if (!byKey[key]) { byKey[key] = { ...a, items: [a] }; grouped.push(byKey[key]); }
      else byKey[key].items.push(a);
    }
    for (const g of grouped) {
      if (!g.items || g.items.length < 3) continue;
      const n = g.items.length;
      g.what = `${g.what} — ${n} shipments`;
      const weeks = [...new Set(g.items.map(x => String(x.where || '').split('·').pop().trim()))].filter(Boolean);
      g.where = weeks.length === 1 ? weeks[0] : `across ${weeks.length} weeks`;
      g.id = null;                                          // a group opens the list, not one shipment
      g.tab = 'list';
    }
    return grouped.slice(0, 6);
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

    const tabs = [['shipments', 'Live map'], ['list', 'Shipments'], ['po', 'Orders'],
                  ['performance', 'Performance'], ['bookings', 'Bookings']]
      .concat(_internal ? [['pricing', 'Pricing'], ['baselines', 'Transit rules']] : []);
    const tw = el('d2d-tabs');
    if (tw) {
      tw.innerHTML = tabs.map(([k, l]) => `<button class="d2d-tab ${_tab === k ? 'on' : ''}" data-tab="${k}">${l}</button>`).join('');
      tw.querySelectorAll('[data-tab]').forEach(b => b.onclick = () => {
        _tab = b.getAttribute('data-tab');
        _view = 'dashboard';                 // a tab always returns to its top level
        paint();
      });
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
    const weekBar = `<div style="display:flex;align-items:center;gap:7px;flex-wrap:nowrap;">
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
    // The week selector belongs with the menu, not on top of the content.
    // The week selector belongs to week-scoped screens only. On the dashboard it competed with
    // the map, and on Transit rules it meant nothing at all.
    const weekScoped = (_tab === 'shipments' && _view !== 'dashboard')
      || ['list', 'bookings', 'pricing', 'po'].includes(_tab);
    const wk = el('d2d-weeks');
    if (wk) wk.innerHTML = weekScoped ? weekBar : '';
    body.innerHTML =
        // A drill-down takes precedence over the tab: the tab says which list you came from,
        // the view says what you opened from it.
        _view === 'container' ? paintContainer(d)
      : _view === 'week'      ? paintWeek(d)
      : _tab === 'baselines'  ? paintBaselines()
      : _tab === 'bookings'   ? paintBookings(d)
      : _tab === 'pricing'    ? paintPricing(d)
      : _tab === 'list'       ? paintList(d)
      : _tab === 'po'         ? paintPO(d)
      : _tab === 'performance' ? paintPerformance(d)
      : paintShipments(d);
    if (_tab === 'baselines') loadBaselines();
    document.querySelectorAll('#d2d-weeks [data-w]').forEach(b => b.onclick = () => {
      _week = b.getAttribute('data-w');
      _rfqNote = '';
      if (_tab === 'shipments' && _view === 'container') _view = 'week';
      load();
    });
    wireActions(body);
    // The map needs the panel to have a size before it can be projected.
    setTimeout(() => { mountMaps(d).catch(e => console.warn('[d2d-hub] map mount', e)); }, 30);
  }

  // Each in-flight shipment on its own rail: origin to the DC, stage nodes, and the vessel at
  // its real position. Two containers on the same sailing get separate rails, so they cannot
  // collide the way map markers do.
  const LANE_STOPS = ['0%', '12%', '22%', '78%', '88%', '100%'];
  const ICON_SHIP_SM = 'M2 14 L22 14 L19.5 19 L4.5 19 Z M6 9 H10 V14 H6 Z M11 6 H15 V14 H11 Z M17 8 H19 V14 H17 Z';
  const ICON_PLANE_SM = 'M12 2 L13.8 8 L22 12.5 L22 14.6 L13.8 12.6 L13.8 18 L16.4 20 L16.4 21.4 L12 20.2 L7.6 21.4 L7.6 20 L10.2 18 L10.2 12.6 L2 14.6 L2 12.5 L10.2 8 Z';

  function flightRow(m, i) {
    const sh = m.sh;
    const look = outlook(sh);
    const pct = Math.round(m.t * 100);
    const moving = m.t > 0 && m.t < 1;
    const eta = sh.plan_arrived;
    const slip = slipOf(sh) || 0;
    return `
      <button type="button" data-open="${esc(sh.id)}" class="d2d-frow d2d-rise"
              style="animation-delay:${(i * 0.035).toFixed(2)}s;">
        <span>
          <span class="d2d-num" style="display:block;font-size:13px;font-weight:600;color:${look.ink};">${eta ? esc(day(eta)) : '—'}</span>
          <span style="display:block;font-size:9px;color:${LIGHT};">${slip > 0 ? '+' + slip + 'd on plan' : 'on plan'}</span>
        </span>
        <span style="min-width:0;">
          <span class="d2d-num" style="display:block;font-size:12px;color:${DARK};overflow:hidden;
                text-overflow:ellipsis;white-space:nowrap;">${esc(sh.reference || 'not advised')}</span>
          <span style="display:block;font-size:10px;color:${MID};">${esc([sh.carrier, sh.po_count ? sh.po_count + ' orders' : ''].filter(Boolean).join(' · '))}</span>
        </span>

        <span class="d2d-lane">
          ${/* Named, not implied: the destination is whatever was booked. */ ''}
          <span class="d2d-ends" style="left:0;">
            <span style="color:${LIGHT};">FROM</span> ${esc(sh.service || sh.origin || 'origin')}</span>
          <span class="d2d-ends" style="right:0;">
            <span style="color:${LIGHT};">TO</span> ${esc(destOf(sh).label)}</span>
          <span class="d2d-rail"></span>
          <span class="d2d-done ${moving ? 'd2d-travel' : ''}" style="width:${pct}%;background:${look.ink};"></span>
          ${LANE_STOPS.map(at => `<span class="d2d-node" style="left:${at};border-color:${parseFloat(at) <= pct ? look.ink : '#DDE1E7'};"></span>`).join('')}
          <span class="d2d-here ${moving ? 'd2d-breathe' : ''}" style="left:${pct}%;">
            <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true"><path d="${m.air ? ICON_PLANE_SM : ICON_SHIP_SM}" fill="${look.ink}"/></svg>
          </span>
        </span>

        <span class="d2d-num" style="font-size:12px;color:${DARK};text-align:right;">${sh.units ? Number(sh.units).toLocaleString() : (sh.cargo && sh.cargo.units ? Math.round(sh.cargo.units).toLocaleString() : '—')}</span>
        <span style="font-size:10.5px;font-weight:600;color:${look.ink};">${esc(look.label)}</span>
      </button>`;
  }

  function paintShipments(d) {
    const source = mapSource(d);
    const inTransit = source.filter(x => x.status === 'in_transit').length;
    const slips = source.map(slipOf).filter(x => x != null && x > 0);
    const acts = actions(d);
    const updates = recentActivity(d);
    const units = source.reduce((n, x) => n + (Number(x.units) || 0), 0);

    // Soonest arrival first: the order someone actually reads a fleet in.
    const marks = shipmentPositions(source)
      .sort((a, b) => String(a.sh.plan_arrived || '').localeCompare(String(b.sh.plan_arrived || '')));
    const moving = marks.filter(m => m.t > 0 && m.t < 1).length;

    return `
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-3 items-start" style="margin-bottom:14px;">
        <div class="lg:col-span-2" style="min-width:0;display:flex;flex-direction:column;">
          ${/* A minimum height so the page keeps its shape on a quiet week. Four rows and a
                short rail used to leave a hole down the middle of the screen. */ ''}
          <div class="rounded-2xl border bg-white shadow-sm" style="min-height:560px;display:flex;
               flex-direction:column;overflow:hidden;">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;
                 padding:12px 16px;flex-wrap:wrap;">
              <div style="display:flex;align-items:center;gap:8px;">
                ${[['flight', 'In flight'], ['map', 'Live map']].map(([k2, l]) =>
                  `<button class="d2d-seg ${_main === k2 ? 'on' : ''}" data-main="${k2}">${l}</button>`).join('')}
              </div>
              <span style="font-size:10.5px;color:${LIGHT};">
                ${_main === 'flight' ? 'soonest arrival first' : moving + ' moving · ' + source.length + ' shown'}</span>
            </div>

            ${_main === 'flight' ? `
              <div class="d2d-frow d2d-fhead">
                <span class="d2d-tl">Arriving</span><span class="d2d-tl">Shipment</span>
                <span class="d2d-tl">Origin &rarr; Sydney</span>
                <span class="d2d-tl" style="text-align:right;">Units</span><span class="d2d-tl">Outlook</span>
              </div>
              ${marks.length ? marks.map(flightRow).join('')
                : `<div class="d2d-empty">Nothing in flight.</div>`}`
            : `<div style="flex:1;display:flex;flex-direction:column;min-height:0;">${paintMap(d, { inPanel: true })}</div>`}
          </div>
        </div>

        <div style="display:flex;flex-direction:column;gap:12px;min-width:0;">
          <div class="grid grid-cols-2 gap-3">
            <div class="rounded-2xl border bg-white shadow-sm d2d-tile">
              <div class="d2d-tl">In transit</div>
              <div class="d2d-tv">${inTransit}</div>
              <div class="d2d-ts">${source.length} shown${units ? ' · ' + units.toLocaleString() + ' units' : ''}</div>
            </div>
            <div class="rounded-2xl border bg-white shadow-sm d2d-tile">
              <div class="d2d-tl">Behind plan</div>
              <div class="d2d-tv" style="color:${slips.length ? BRAND : DARK};">${slips.length}</div>
              <div class="d2d-ts">${slips.length ? 'worst ' + Math.max(...slips) + ' days' : 'all on plan'}</div>
            </div>
          </div>

          ${updates.length ? `
            <div class="rounded-2xl border bg-white shadow-sm" style="padding:13px 15px;flex:1;min-height:0;">
              <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:2px;">
                <span style="font-size:12px;font-weight:600;color:${DARK};">Latest updates</span>
                <span style="font-size:10px;color:${LIGHT};">newest first</span>
              </div>
              ${updates.slice(0, 9).map((r, i2) => `
                <button type="button" data-open="${esc(r.sh.id)}" class="d2d-rise"
                        style="display:flex;gap:9px;width:100%;text-align:left;background:none;border:0;
                               padding:7px 0;cursor:pointer;border-top:.5px solid rgba(0,0,0,.05);
                               animation-delay:${(i2 * .04).toFixed(2)}s;">
                  <span style="width:6px;height:6px;border-radius:50%;margin-top:5px;flex-shrink:0;
                        background:${r.drift > 0 ? BRAND : (r.drift < 0 ? LIME : '#C9CED6')};"></span>
                  <span style="flex:1;min-width:0;">
                    <span style="display:block;font-size:11.5px;color:${DARK};line-height:1.3;">${esc(r.label)} &middot;
                      <span class="d2d-num">${esc(r.sh.reference || 'not advised')}</span></span>
                    <span style="display:block;font-size:10px;color:${r.meaning.ink};">${esc(r.meaning.text)}</span>
                  </span>
                  <span class="d2d-num" style="font-size:10px;color:${LIGHT};flex-shrink:0;">${esc(day(r.at))}</span>
                </button>`).join('')}
            </div>` : ''}

          <div style="display:flex;gap:8px;flex-wrap:wrap;">
            <button class="d2d-btn" data-go="week" style="flex:1;min-width:0;">Week summary &rarr;</button>
            <button class="d2d-btn" data-tabgo="list" style="flex:1;min-width:0;">All shipments &rarr;</button>
          </div>
        </div>
      </div>

      <div style="display:flex;align-items:baseline;gap:9px;margin:2px 0 8px;">
        <span style="font-size:12.5px;font-weight:600;color:${DARK};">Exceptions</span>
        <span style="font-size:11px;color:${MID};">what is off plan or waiting on somebody</span>
        ${acts.length && acts[0].kind !== 'Clear'
          ? `<span class="d2d-num" style="font-size:11px;color:${BRAND};font-weight:700;">${acts.length}</span>` : ''}
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3" style="margin-bottom:18px;">
        ${acts.map((a, i2) => {
          const tag = (a.id || a.tab) ? 'button' : 'div';
          const attrs = a.id ? ` type="button" data-open="${esc(a.id)}" style="text-align:left;width:100%;cursor:pointer;`
            : a.tab ? ` type="button" data-tabgo="${esc(a.tab)}" style="text-align:left;width:100%;cursor:pointer;`
            : ' style="';
          return `
          <${tag} class="rounded-2xl border bg-white shadow-sm d2d-nba d2d-rise d2d-lift"${attrs}
               border-left:3px solid ${a.accent};padding:13px 15px;animation-delay:${i2 * .05}s;">
            <span style="display:flex;align-items:baseline;justify-content:space-between;gap:8px;">
              <span class="d2d-kind" style="color:${a.ink};">${esc(a.kind)}</span>
              ${a.where ? `<span class="d2d-num" style="font-size:9.5px;color:${LIGHT};">${esc(a.where)}</span>` : ''}
            </span>
            <span style="font-size:12.5px;font-weight:600;color:${DARK};line-height:1.35;">${esc(a.what)}</span>
            <span style="font-size:11px;color:${MID};line-height:1.4;">${esc(a.effect)}</span>
            ${a.next ? `<span style="display:flex;align-items:baseline;gap:6px;margin-top:7px;padding-top:7px;
                  border-top:.5px solid rgba(0,0,0,.06);">
                 <span style="font-size:9px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">Next</span>
                 <span style="font-size:11px;color:${DARK};line-height:1.4;">${esc(a.next)}</span>
                 ${a.who ? `<span style="font-size:10px;color:${LIGHT};white-space:nowrap;">· ${esc(a.who)}</span>` : ''}
               </span>` : ''}
          </${tag}>`;
        }).join('')}
      </div>`;
  }

  // ── The world, drawn the way the Live Map page draws it ──
  // Lifted from map_live_additive: a canvas dot field rendered from real country shapes
  // through a Mercator projection centred on the Asia–Australia corridor. The hand-rolled
  // equirectangular grid this replaces could not be made to look right, because the problem
  // was the projection, not the styling.
  const MAP_PAL = {
    origin_port: '#F4BC1C',   // the mark: bright against the dot field
    port_ink:    '#8A6D00',   // the label: the bright value on white is 1.7:1, unreadable
    transit:     '#990033',
    clearing:    '#1E9BD7',   // the mark: bright, like the amber on the origin side
    dest_ink:    '#15618F',   // the label: 6.7:1 on white, where the bright value is 3.1:1
    customs:     '#DC2626',
    last_mile:   '#1C1C1E',
    air:         '#4A9B8E',
    land:        '#C2C2C2',
    sea_bg:      '#FAFAFA',
  };

  let _libsReady = null;
  function loadMapLibs() {
    if (_libsReady) return _libsReady;
    const one = (src) => new Promise((res, rej) => {
      const el2 = document.createElement('script');
      el2.src = src; el2.onload = res; el2.onerror = rej;
      document.head.appendChild(el2);
    });
    _libsReady = (async () => {
      if (!window.d3) await one('https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js');
      if (!window.topojson) await one('https://cdn.jsdelivr.net/npm/topojson@3/dist/topojson.min.js');
    })();
    return _libsReady;
  }

  let _topo = null;
  async function worldShapes() {
    if (_topo) return _topo;
    const raw = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r => r.json());
    _topo = window.topojson.feature(raw, raw.objects.countries).features;
    return _topo;
  }

  // Returns the projection so markers land where the coastline says they should.
  async function drawWorld(canvas) {
    await loadMapLibs();
    const w = canvas.offsetWidth || 900, h = canvas.offsetHeight || 480;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = w * dpr; canvas.height = h * dpr;
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.fillStyle = MAP_PAL.sea_bg; ctx.fillRect(0, 0, w, h);

    // The corridor runs roughly 36°N (Qingdao) to 34°S (Sydney) — about 70° of latitude. The
    // scale has to honour the shorter side or the top of China falls off the panel.
    const projection = window.d3.geoMercator()
      .center([133, -4])                 // the action is between 110°E and 155°E
      .scale(Math.min(w * 0.30, h * 0.52))
      .translate([w * 0.46, h * 0.48]);

    const features = await worldShapes();
    const STEP = 6, R = 1.4;
    ctx.fillStyle = MAP_PAL.land;
    const off = document.createElement('canvas');
    off.width = w; off.height = h;
    const octx = off.getContext('2d');
    for (const feature of features) {
      octx.clearRect(0, 0, w, h);
      octx.beginPath();
      window.d3.geoPath(projection, octx)(feature);
      octx.fillStyle = '#000'; octx.fill();
      const img = octx.getImageData(0, 0, w, h).data;
      for (let px = STEP / 2; px < w; px += STEP) {
        for (let py = STEP / 2; py < h; py += STEP) {
          const idx = (Math.floor(py) * w + Math.floor(px)) * 4;
          if (img[idx + 3] > 128) { ctx.beginPath(); ctx.arc(px, py, R, 0, Math.PI * 2); ctx.fill(); }
        }
      }
    }
    return { w, h, project: (lon, lat) => projection([lon, lat]) };
  }

  // Painted after the panel has a size: the projection needs real pixels, and the country
  // shapes have to arrive first.
  async function mountMaps(d) {
    const boxes = document.querySelectorAll('#page-d2d .d2d-mapbox, #d2d-fullmap .d2d-mapbox');
    for (const box of boxes) {
      const canvas = box.querySelector('.d2d-mapcanvas');
      const svg = box.querySelector('.d2d-mapsvg');
      const loading = box.querySelector('.d2d-maploading');
      if (!canvas || !svg || box.dataset.drawn === '1') continue;
      try {
        const dims = await drawWorld(canvas);
        box.dataset.drawn = '1';
        if (loading) loading.style.display = 'none';
        svg.setAttribute('viewBox', `0 0 ${dims.w} ${dims.h}`);
        svg.innerHTML = mapLayers(d, dims);
        wireActions(svg);
      } catch (e) {
        // Offline, or the CDN is blocked: say so rather than leaving a blank panel.
        console.warn('[d2d-hub] world map unavailable', e);
        if (loading) loading.textContent = 'Map unavailable offline';
      }
    }
  }

  // Everything above the coastline: lanes, ports, vessels.
  function mapLayers(d, dims) {
    const P = (lon, lat) => { const q = dims.project(lon, lat); return { x: q[0], y: q[1] }; };
    const source = mapFiltered(mapSource(d));

    // One lane per origin and mode, each bowed differently so two sailings out of neighbouring
    // ports do not trace the same line. The elongation is deliberate: a fatter arc separates
    // the lanes and reads as a longer voyage, which it is.
    const originList = [...new Set(source.map(x => originKey(x.service || x.origin) || 'ningbo'))];
    const destList = [...new Set(source.map(x => destKey(x.destination) || 'sydney'))];
    const bowOfOrigin = (k2) => 0.20 + originList.indexOf(k2) * 0.09;

    const arc = (A, B, bow, away) => {
      const mx = (A.x + B.x) / 2, my = (A.y + B.y) / 2;
      const dx = B.x - A.x, dy = B.y - A.y;
      const cx = mx + (away ? -dy : dy) * bow, cy = my - (away ? -dx : dx) * bow;
      return { d: `M${A.x.toFixed(1)} ${A.y.toFixed(1)} Q${cx.toFixed(1)} ${cy.toFixed(1)} ${B.x.toFixed(1)} ${B.y.toFixed(1)}`,
               at: (t) => { const u = 1 - t; return { x: u * u * A.x + 2 * u * t * cx + t * t * B.x,
                                                     y: u * u * A.y + 2 * u * t * cy + t * t * B.y }; } };
    };

    const laneFor = (sh) => {
      const k2 = originKey(sh.service || sh.origin) || 'ningbo';
      const A = P(ORIGIN_PORTS[k2].lon, ORIGIN_PORTS[k2].lat);
      const B = P(destOf(sh).lon, destOf(sh).lat);
      const air = sh.mode === 'air';
      return arc(A, B, air ? 0.12 : bowOfOrigin(k2), air);
    };

    // A lane is an origin, a destination and a mode. It was keyed on origin alone, so a
    // Melbourne booking borrowed the Sydney line.
    const lanes = new Map();
    for (const sh of source) {
      const k2 = originKey(sh.service || sh.origin) || 'ningbo';
      const dk = destKey(sh.destination) || 'sydney';
      const key = (sh.mode === 'air' ? 'a' : 's') + k2 + '>' + dk;
      if (!lanes.has(key)) lanes.set(key, { air: sh.mode === 'air', k: k2, dk });
    }
    const lanePaths = [...lanes.values()].map(({ air, k: k2, dk }) => {
      const A = P(ORIGIN_PORTS[k2].lon, ORIGIN_PORTS[k2].lat);
      const B = P(DEST_PORTS[dk].lon, DEST_PORTS[dk].lat);
      const a = arc(A, B, air ? 0.12 : bowOfOrigin(k2), air);
      return air
        ? `<path d="${a.d}" fill="none" stroke="${MAP_PAL.air}" stroke-width="1.5" stroke-linecap="round"
                 stroke-dasharray="2 7" opacity=".85"/>`
        : `<path d="${a.d}" fill="none" stroke="${MAP_PAL.transit}" stroke-width="1.6" stroke-linecap="round"
                 opacity=".45"/>`;
    }).join('');

    // Every label on the map, laid out in one pass so a port cannot land on a vessel.
    const moving = source.filter(sh => { const t = markT(sh); return t > 0 && t < 1; });
    const allLabels = spreadLabels([
      ...originList.map(k2 => {
        const place = ORIGIN_PORTS[k2], q = P(place.lon, place.lat);
        const n = source.filter(x => (originKey(x.service || x.origin) || 'ningbo') === k2).length;
        return { kind: 'port', side: 'l', x: q.x, y: q.y, ly: q.y, label: place.label, n };
      }),
      ...moving.map((sh, i2) => {
        // Anything sharing a position with an earlier vessel is nudged a little further along
        // its own lane, so every ship and plane is actually visible.
        const t = markT(sh);
        const same = moving.filter((o2, j) => j < i2 && Math.abs(markT(o2) - t) < 0.02
                     && (originKey(o2.service || o2.origin) || 'ningbo') === (originKey(sh.service || sh.origin) || 'ningbo')
                     && (o2.mode === 'air') === (sh.mode === 'air')).length;
        const q = laneFor(sh).at(Math.min(0.97, t + same * 0.045));
        return { kind: 'vessel', side: 'r', sh, x: q.x, y: q.y, ly: q.y + 10 };
      }),
      ...destList.map(dk => {
        const q = P(DEST_PORTS[dk].lon, DEST_PORTS[dk].lat);
        return { kind: 'dest', dk, side: 'r', x: q.x, y: q.y, ly: q.y };
      }),
    ], dims);
    const portPts = allLabels.filter(x => x.kind === 'port');
    const portPins = portPts.map(pt => `<g>
        <circle cx="${pt.x.toFixed(1)}" cy="${pt.y.toFixed(1)}" r="7" fill="none" stroke="${MAP_PAL.origin_port}"
                stroke-width="1.5" class="d2d-sonar"/>
        <circle cx="${pt.x.toFixed(1)}" cy="${pt.y.toFixed(1)}" r="5" fill="${MAP_PAL.origin_port}"
                stroke="#fff" stroke-width="1.5"/>
        ${pinLabel(pt.x, pt.y, pt.label, MAP_PAL.port_ink, true,
                   pt.n + (pt.n === 1 ? ' shipment' : ' shipments'), pt.ly, pt.lx)}
      </g>`).join('');

    // One pin per destination actually booked. Port and DC are 40km apart — one dot at this
    // scale — so each city is a single arrival point with its breakdown beneath.
    const destPins = destList.map(dk => {
      const place = DEST_PORTS[dk], q = P(place.lon, place.lat);
      const mine = source.filter(x => (destKey(x.destination) || 'sydney') === dk);
      const atPort = mine.filter(x => { const t = markT(x); return t >= 0.82 && t < 0.9; }).length;
      const inCust = mine.filter(x => { const t = markT(x); return t >= 0.9 && t < 0.95; }).length;
      const done = mine.filter(x => markT(x) >= 1).length;
      const here = atPort + inCust + done;
      const parts = [atPort && atPort + ' at port', inCust && inCust + ' in customs',
                     done && done + ' delivered'].filter(Boolean).join(' · ');
      const colour = inCust ? MAP_PAL.customs : MAP_PAL.clearing;
      const lbl = allLabels.find(x => x.kind === 'dest' && x.dk === dk) || {};
      return `<g>
        ${here ? `<circle cx="${q.x.toFixed(1)}" cy="${q.y.toFixed(1)}" r="7" fill="none"
              stroke="${colour}" stroke-width="1.5" class="d2d-sonar"/>` : ''}
        <circle cx="${q.x.toFixed(1)}" cy="${q.y.toFixed(1)}" r="${here ? 5 : 3.5}" fill="${colour}"
                stroke="#fff" stroke-width="1.5" opacity="${here ? 1 : .5}"/>
        ${pinLabel(q.x, q.y, place.label, inCust ? MAP_PAL.customs : MAP_PAL.dest_ink, false,
                   parts || mine.length + (mine.length === 1 ? ' booked' : ' booked'), lbl.ly, lbl.lx)}
      </g>`;
    }).join('');

    const vessels = allLabels.filter(x => x.kind === 'vessel').map(({ sh, x, y, ly, lx }) => {
      const look = outlook(sh);
      const air = sh.mode === 'air';
      const colour = air ? MAP_PAL.air : MAP_PAL.transit;
      return `<g class="d2d-vessel" data-open="${esc(sh.id)}" style="cursor:pointer;" role="button"
                 aria-label="${esc(sh.reference || 'shipment')}">
        <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="13" fill="transparent"/>
        ${look.state === 'late' ? `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="10" fill="${BRAND}"
              opacity=".18" class="d2d-sonar"/>` : ''}
        <g transform="translate(${x.toFixed(1)},${y.toFixed(1)}) scale(.58)">
          ${air
            ? `<path d="${ICON_PLANE}" fill="${colour}"/>`
            : `<path d="M-16,5 L-16,2 L-12,2 L12,2 L17,-1 L18,5 Z" fill="${colour}"/>
               <path d="M-12,2 L-12,-4 L-2,-4 L-2,2 Z" fill="${colour}" opacity=".7"/>
               <path d="M2,2 L2,-7 L8,-7 L8,2 Z" fill="${colour}" opacity=".85"/>`}
        </g>
        ${pinLabel(x, y, sh.reference || 'not advised', DARK, false, look.label, ly, lx)}
      </g>`;
    }).join('');

    return lanePaths + portPins + destPins + vessels;
  }

  // Where a shipment sits along its lane, 0 to 1.
  function markT(sh) {
    const ev = evMap(sh);
    let last = null;
    for (const [k2] of STAGES) if (ev[k2] && ev[k2].actual_at) last = k2;
    let base = last ? STAGE_T[last] : 0;
    if (last === 'departed') {
      const left = ev.departed && ev.departed.actual_at;
      const total = daysBetween(left, sh.plan_arrived);
      const gone = daysBetween(left, today());
      if (total && total > 0 && gone != null) {
        base = STAGE_T.departed + Math.max(0, Math.min(1, gone / total)) * (STAGE_T.arrived - STAGE_T.departed);
      }
    }
    return base;
  }

  // Pins within a few pixels print their names over each other. Labels are pushed apart
  // vertically and given a leader line back to their dot, which is what a map does when two
  // places are genuinely close together.
  function spreadLabels(pins, frame) {
    // Real box collision rather than column buckets: a left-anchored label and a right-anchored
    // one can still cross, and bucketing by x never catches that. Each label is placed in turn
    // and nudged down until its box is clear of everything already placed.
    const boxOf = (p2) => {
      const text = String(p2.label || (p2.sh && p2.sh.reference) || 'not advised');
      const sub = String(p2.n != null ? p2.n + ' shipments' : 'status');
      const w = Math.max(text.length, sub.length * 0.85) * 5.9 + 20;
      const ax = (p2.lx == null ? p2.x : p2.lx);
      const x0 = p2.side === 'l' ? ax - 11 - w : ax + 7;
      return { x0, x1: x0 + w, y0: p2.ly - 15, y1: p2.ly + 14 };
    };
    const hits = (a, b) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;

    // Out of the shipping lanes entirely: left-hand labels to a gutter left of the westernmost
    // mark, right-hand ones to the right of the easternmost, roughly 12% of the frame clear.
    if (frame && pins.length) {
      const pad = Math.max(40, frame.w * 0.078);        // 35% closer than before
      const minX = Math.min(...pins.map(p2 => p2.x));
      const maxX = Math.max(...pins.map(p2 => p2.x));
      for (const p2 of pins) {
        p2.lx = p2.side === 'l'
          ? Math.max(90, minX - pad)
          : Math.min(frame.w - 40, maxX + pad);
      }
    }

    const placed = [];
    for (const p2 of [...pins].sort((a, b) => a.y - b.y)) {
      let box = boxOf(p2), guard = 0;
      while (placed.some(q => hits(box, q)) && guard++ < 60) {
        p2.ly += 7;
        box = boxOf(p2);
      }
      placed.push(box);
    }
    return pins;
  }

  // A label that can be read over a dot field: white pill, then the text.
  function pinLabel(x, y, text, colour, left, sub, ly, lx) {
    const w = Math.max(text.length, (sub || '').length * 0.85) * 5.9 + 12;
    const ax = (lx == null ? x : lx);              // where the label sits, out in the gutter
    const tx = left ? ax - 11 : ax + 11;
    const y2 = (ly == null ? y : ly);
    const moved = Math.abs(y2 - y) > 2 || Math.abs(ax - x) > 2;
    return `
      ${moved ? `<path d="M${x.toFixed(1)} ${y.toFixed(1)} L${(left ? ax - 6 : ax + 6).toFixed(1)} ${y2.toFixed(1)}"
            stroke="${colour}" stroke-width=".7" opacity=".22" fill="none" stroke-linecap="round"/>
        <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="1.4" fill="${colour}" opacity=".35"/>` : ''}
      <rect x="${(left ? tx - w : tx - 4).toFixed(1)}" y="${(y2 - (sub ? 14 : 9)).toFixed(1)}"
            width="${(w + 8).toFixed(1)}" height="${sub ? 27 : 17}" rx="4" fill="#ffffff" opacity=".92"/>
      <text x="${tx.toFixed(1)}" y="${(y2 + (sub ? -2 : 4)).toFixed(1)}" text-anchor="${left ? 'end' : 'start'}"
            font-size="11" font-weight="600" fill="${colour}" font-family="inherit">${esc(text)}</text>
      ${sub ? `<text x="${tx.toFixed(1)}" y="${(y2 + 10).toFixed(1)}" text-anchor="${left ? 'end' : 'start'}"
            font-size="9.5" fill="${MID}" font-family="ui-monospace,monospace">${esc(sub)}</text>` : ''}`;
  }

  // ── Live tracking ──
  // Follows the Live Map page: dotted basemap, a header strip with search and legend, named
  // nodes with counts. What is REAL here is the stage each shipment has reached, which places
  // it along its route. What is NOT real is a precise position at sea — that needs a carrier
  // feed, and the panel says so rather than implying GPS accuracy.
  // Geography comes from the same pre-generated dot map the sign-in screen uses
  // (/public/login_map.json): real coastlines, no d3 and no CDN at runtime. Hand-drawn
  // polygons were never going to look like the Live Map page, and this file already exists.
  // 900x700 rather than 900x430: the lane runs 64 degrees of latitude against 34 of longitude,
  // so a wide card forced the frame to span half the globe to keep the dots round. This shape
  // frames Asia to Australia, which is the map this client needs.
  const MAP = { w: 900, h: 700 };
  // Drawn for a 430-high canvas, so y is scaled to the taller one.
  const LAND = [
    [[250,20],[470,10],[560,70],[600,130],[520,175],[430,160],[360,115],[280,100]],
    [[140,95],[265,92],[300,150],[250,205],[165,185]],
    [[555,215],[640,200],[700,240],[690,295],[610,305],[555,265]],
    [[470,215],[560,235],[575,275],[500,290],[440,265]],
    [[560,330],[790,315],[845,380],[800,425],[640,428],[575,390]],
  ].map(poly => poly.map(([x, y]) => [x, y * (700 / 430)]));
  const S = 700 / 430;
  const PORTS_FALLBACK = {
    origin: { x: 505, y: 120 * S, label: 'Origin port' },
    transhipment: { x: 640, y: 262 * S, label: 'Transhipment' },
    destination: { x: 700, y: 352 * S, label: 'Port Botany' },
    customs: { x: 676, y: 386 * S, label: 'Customs' },
    lastmile: { x: 735, y: 402 * S, label: 'Last mile' },
  };
  let PORTS = PORTS_FALLBACK;
  let VIEW = { x0: 0, y0: 0, w: MAP.w, h: MAP.h };

  // Real places, so the map can be geographic rather than schematic.
  // Origin ports actually used on this lane, plus the inland area the factories sit in.
  const ORIGIN_PORTS = {
    ningbo:    { lon: 121.55, lat: 29.87, label: 'Ningbo' },
    qingdao:   { lon: 120.32, lat: 36.09, label: 'Qingdao' },
    xiamen:    { lon: 118.09, lat: 24.48, label: 'Xiamen' },
    hongkong:  { lon: 114.17, lat: 22.32, label: 'Hong Kong' },
    shanghai:  { lon: 121.47, lat: 31.23, label: 'Shanghai' },
    shenzhen:  { lon: 114.06, lat: 22.54, label: 'Shenzhen' },
  };
  // Free text on a booking is matched loosely: files say "NINGBO", "Ningbo, CN", "ningbo port".
  function originKey(text) {
    const t = String(text || '').toLowerCase();
    for (const k of Object.keys(ORIGIN_PORTS)) if (t.includes(k)) return k;
    if (/hong\s*kong|hkg/.test(t)) return 'hongkong';
    return null;
  }

  // Where each label sits relative to its dot: [dx, dy, anchor]. Fixed per place rather than
  // computed, because these few points are always in the same relation to each other.
  // Each label is pushed to its own side, far enough that neighbouring ports do not share
  // space: Qingdao sits left, Ningbo above, Xiamen right and below.
  // These ports are within a few hundred kilometres of each other, so two labels above two
  // dots will always touch. Each is given a different SIDE instead: Qingdao left, Ningbo
  // right, Xiamen right and lower.
  const LABEL_AT = {
    Qingdao:         [-16, -6, 'end'],
    Ningbo:          [16, -2, 'start'],
    Shanghai:        [-16, -14, 'end'],
    Xiamen:          [16, 16, 'start'],
    'Hong Kong':     [-16, 16, 'end'],
    Shenzhen:        [-16, 26, 'end'],
    'Port Botany':   [16, -8, 'start'],
    'Sydney customs':[16, 8, 'start'],
    'Eastern Creek': [16, 24, 'start'],
  };

  // Where a booking can actually land. Matched loosely, the same way origins are.
  const DEST_PORTS = {
    sydney:    { lon: 151.21, lat: -33.87, label: 'Sydney' },
    melbourne: { lon: 144.94, lat: -37.84, label: 'Melbourne' },
    brisbane:  { lon: 153.10, lat: -27.38, label: 'Brisbane' },
    perth:     { lon: 115.75, lat: -32.05, label: 'Perth' },
    adelaide:  { lon: 138.51, lat: -34.85, label: 'Adelaide' },
    fremantle: { lon: 115.74, lat: -32.06, label: 'Fremantle' },
    auckland:  { lon: 174.77, lat: -36.84, label: 'Auckland' },
  };
  const DEST_ALIAS = {
    'port botany': 'sydney', 'botany': 'sydney', 'eastern creek': 'sydney', 'syd': 'sydney',
    'melb': 'melbourne', 'mel': 'melbourne', 'bne': 'brisbane', 'per': 'perth', 'akl': 'auckland',
  };
  function destKey(text) {
    const t = String(text || '').toLowerCase();
    for (const [alias, k] of Object.entries(DEST_ALIAS)) if (t.includes(alias)) return k;
    for (const k of Object.keys(DEST_PORTS)) if (t.includes(k)) return k;
    return null;
  }
  // Falls back to Sydney only when nothing was booked — and says so rather than pretending.
  const destOf = (sh) => DEST_PORTS[destKey(sh.destination || sh.service_to) || 'sydney'];

  const PLACES = {
    origin:       { lon: 121.55, lat: 29.87,  label: 'Ningbo' },
    transhipment: { lon: 120.98, lat: 14.60,  label: 'Manila' },
    destination:  { lon: 151.23, lat: -33.96, label: 'Port Botany' },
    customs:      { lon: 151.19, lat: -33.86, label: 'Sydney customs' },
    lastmile:     { lon: 150.86, lat: -33.80, label: 'Eastern Creek' },
  };

  // How far along the route each stage sits. Between recorded stages a shipment holds its last
  // known position: inventing motion between milestones would be a guess drawn as fact.
  const STAGE_T = { pickup: 0, origin_cleared: 0.04, departed: 0.10, arrived: 0.82,
                    dest_cleared: 0.90, out_for_delivery: 0.95, delivered: 1 };

  // Equirectangular, which is what these generated dot maps use — verified below against known
  // coastal cities rather than assumed, because a wrong projection puts ports in the sea.
  const project = (v, lon, lat) => ({ x: v.x0 + (lon + 180) / 360 * v.w, y: v.y0 + (90 - lat) / 180 * v.h });

  // Seen from above, as a map should be: a container ship with stacked boxes, and a swept
  // airframe. The earlier marks were a line drawing that read as the same shape at map scale.
  const ICON_SHIP = 'M-10 2 L10 2 L7.5 7 L-7.5 7 Z M-6.5 -2 H-2.5 V2 H-6.5 Z M-1.5 -4.5 H2.5 V2 H-1.5 Z M4 -3 H6 V2 H4 Z';
  const ICON_PLANE = 'M0 -10 L1.8 -4 L10 0.5 L10 2.6 L1.8 0.6 L1.8 6 L4.4 8 L4.4 9.4 L0 8.2 L-4.4 9.4 L-4.4 8 L-1.8 6 L-1.8 0.6 L-10 2.6 L-10 0.5 L-1.8 -4 Z';

  let _world = null, _worldTried = false, _dots = null, _sea = null;

  async function loadWorld() {
    if (_world || _worldTried) return _world;
    _worldTried = true;
    try {
      const r = await fetch('/public/login_map.json');
      if (!r.ok) return null;
      const m = await r.json();
      const vb = String(m.viewBox || '').trim().split(/\s+/).map(Number);
      if (vb.length !== 4 || vb.some(n => !isFinite(n))) return null;

      const pts = [];
      const rx = /cx="([-\d.]+)"\s+cy="([-\d.]+)"/g;
      let mm; while ((mm = rx.exec(String(m.circles || '')))) pts.push([+mm[1], +mm[2]]);
      if (pts.length < 200) return null;

      const xs = [...new Set(pts.slice(0, 1200).map(q => q[0]))].sort((a, b) => a - b);
      let step = Infinity;
      for (let k = 1; k < xs.length; k++) { const dd = xs[k] - xs[k - 1]; if (dd > 0.01) step = Math.min(step, dd); }
      if (!isFinite(step) || step <= 0) step = 6;

      const view = { x0: vb[0], y0: vb[1], w: vb[2], h: vb[3] };
      const occupied = new Set(pts.map(q => Math.round(q[0] / step) + ':' + Math.round(q[1] / step)));
      const near = (lon, lat) => {
        const q = project(view, lon, lat);
        for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++)
          if (occupied.has((Math.round(q.x / step) + dx) + ':' + (Math.round(q.y / step) + dy))) return true;
        return false;
      };
      const hits = [[121.55, 29.87], [151.21, -33.87], [114.06, 22.54], [-0.13, 51.51]]
        .filter(([a, b]) => near(a, b)).length;
      if (hits < 3) { console.warn('[d2d-hub] map projection did not line up; using the simple map'); return null; }

      _world = { view, step, r: Number(m.r) || 1.4, circles: String(m.circles || ''), pts, occupied };
      PORTS = {};
      for (const [k, v] of Object.entries(PLACES)) {
        const q = project(view, v.lon, v.lat);
        PORTS[k] = { x: q.x, y: q.y, label: v.label };
      }

      // Frame the trade lane rather than the whole planet. Drawing the entire world would put
      // this route in one corner with an empty ocean filling the card.
      const px = Object.values(PORTS).map(q => q.x), py = Object.values(PORTS).map(q => q.y);
      const c = curveC();
      const minX = Math.min(...px, c.x), maxX = Math.max(...px, c.x);
      const minY = Math.min(...py, c.y), maxY = Math.max(...py, c.y);
      // At 0.55/0.30 the lane filled the card and read as a close-up; at 1.25/0.85 it pulled
      // back to almost the whole globe. This sits about 60% wider than the tight frame, which
      // shows east Asia through to Australia with recognisable coastline around the route.
      const padX = Math.max((maxX - minX) * 0.78, view.w * 0.08);
      const padY = Math.max((maxY - minY) * 0.48, view.h * 0.08);
      let x0 = minX - padX, y0 = minY - padY;
      let vw = (maxX - minX) + padX * 2, vh = (maxY - minY) + padY * 2;
      // Keep the card's own proportions so the dots stay round and nothing is squashed.
      const targetRatio = MAP.w / MAP.h;
      if (vw / vh < targetRatio) { const need = vh * targetRatio; x0 -= (need - vw) / 2; vw = need; }
      else { const need = vw / targetRatio; y0 -= (need - vh) / 2; vh = need; }
      // One more step back, as a plain multiplier on the finished frame.
      const ZOOM_OUT = 1.5;
      const cx0 = x0 + vw / 2, cy0 = y0 + vh / 2;
      vw *= ZOOM_OUT; vh *= ZOOM_OUT;
      x0 = cx0 - vw / 2; y0 = cy0 - vh / 2;

      // Never wider than the world itself.
      vw = Math.min(vw, view.w); vh = Math.min(vh, view.h);
      x0 = Math.max(view.x0, Math.min(x0, view.x0 + view.w - vw));
      y0 = Math.max(view.y0, Math.min(y0, view.y0 + view.h - vh));
      VIEW = { x0, y0, w: vw, h: vh };

      _dots = null; _sea = null;
      return _world;
    } catch (e) { return null; }
  }

  // The route bows through the sea rather than across land, anchored on the two ports.
  // Where a route starts: the shipment's own port when we know it, Ningbo otherwise.
  const portXY = (place) => {
    if (!place) return PORTS.origin;
    const q = project(VIEWSRC(), place.lon, place.lat);
    return { x: q.x, y: q.y, label: place.label };
  };
  const VIEWSRC = () => (_world && _world.view) || { x0: 0, y0: 0, w: MAP.w, h: MAP.h };

  // Each origin gets its own bow, so three lanes read as three rather than one thick line.
  const BOW = { ningbo: 0.55, qingdao: 0.78, xiamen: 0.34, hongkong: 0.22, shanghai: 0.66, shenzhen: 0.44 };
  const bowOf = (from) => {
    if (!from) return 0.55;
    const k = Object.keys(ORIGIN_PORTS).find(key => ORIGIN_PORTS[key].label === from.label);
    return BOW[k] != null ? BOW[k] : 0.55;
  };
  const curveC = (from) => {
    const A = from || PORTS.origin, B = PORTS.destination;
    // Push the control point east so the arc runs down the Pacific side, as the sailing does.
    return { x: Math.max(A.x, B.x) + Math.abs(B.x - A.x) * bowOf(from) + 10, y: (A.y + B.y) / 2 };
  };
  
  // Air bows the other way and less far: it is a different journey, and overlaying it on the
  // sea lane made two modes look like one.
  const airC = (from) => {
    const A = from || PORTS.origin, B = PORTS.destination;
    return { x: (A.x + B.x) / 2 - Math.abs(B.x - A.x) * 0.35, y: (A.y + B.y) / 2 };
  };
  
  const pathFrom = (A, C, B) =>
    `M${A.x.toFixed(1)} ${A.y.toFixed(1)} Q${C.x.toFixed(1)} ${C.y.toFixed(1)} ${B.x.toFixed(1)} ${B.y.toFixed(1)}`;
  

  
  // Which origin ports are actually in play, so a lane is drawn for each rather than one
  // pretending every factory ships from the same place.
  

  // What each shipment is doing, for the rows and the counters. Position on the map is worked
  // out separately, from the live projection.
  function shipmentPositions(list) {
    return list.map(sh => {
      const ev = evMap(sh);
      let lastStage = null;
      for (const [k2, label] of STAGES) if (ev[k2] && ev[k2].actual_at) lastStage = label;
      const slip = slipOf(sh);
      const od = overdueOf(sh);
      const colour = od || (slip != null && slip > 0) ? BRAND
                   : sh.status === 'delivered' ? LINK
                   : lastStage ? LIME : LIGHT;
      return { sh, t: markT(sh), air: sh.mode === 'air',
               stage: lastStage || 'not yet collected', colour,
               note: od ? od.label + ' overdue' : (slip > 0 ? '+' + slip + ' days' : null) };
    });
  }

  function mapFiltered(list) {
    if (_mapFilter === 'sea') return list.filter(x => (x.mode || 'sea') === 'sea');
    if (_mapFilter === 'air') return list.filter(x => x.mode === 'air');
    if (_mapFilter === 'late') return list.filter(x => overdueOf(x) || (slipOf(x) || 0) > 0);
    return list;
  }

  // A vessel does not belong to a week — it is either moving or it is not. The map therefore
  // shows everything in flight by default, and can be narrowed to the selected week.
  function mapSource(d) {
    if (_mapScope === 'week') return d.shipments;
    const all = d.allShipments && d.allShipments.length ? d.allShipments : d.shipments;
    return all.filter(x => x.status !== 'delivered' || (d.shipments || []).some(y => y.id === x.id));
  }

  // Sea is a heavier dashed line in water blue; air a fine dotted line in brand blue. Each
  // shipment's line is offset by the same amount as its marker, so the two agree.
  const SEA_LINE = '#6F93BC';
  const SEA_INK = '#3D6488';          // the vessel itself, darker than its lane

  function paintMap(d, opts) {
    const big = !!(opts && opts.big);
    const inPanel = !!(opts && opts.inPanel);
    const source = mapSource(d);
    const marks = shipmentPositions(mapFiltered(source));
    const moving = marks.filter(m => m.t > 0 && m.t < 1).length;
    // Scale marks with the frame so they stay the same visual size however far the camera is.
    const k = VIEW.w / MAP.w;

    // The count goes INSIDE the dot. Drawn beside it, the number was a second piece of text
    // per point, and with three ports a few hundred kilometres apart those collided with the
    // neighbouring port's name.
    const node = (pt, count, colour, opts2) => {
      const o2 = opts2 || {};
      const showLabel = o2.label !== false;
      const at = LABEL_AT[pt.label] || [0, -15, 'middle'];
      const r = (count ? 9 : 5.5) * k;
      const lx = pt.x + at[0] * k, ly = pt.y + at[1] * k;
      return `
      <g>
        ${count ? `<circle cx="${pt.x}" cy="${pt.y}" r="${(r * 1.7).toFixed(1)}" fill="${colour}" opacity=".16" class="d2d-ping"/>` : ''}
        <circle cx="${pt.x}" cy="${pt.y}" r="${r.toFixed(1)}" fill="${count ? colour : '#fff'}"
                stroke="${count ? colour : '#AEB4BD'}" stroke-width="${(1.6 * k).toFixed(2)}"/>
        ${count ? `<text x="${pt.x}" y="${(pt.y + 3.2 * k).toFixed(1)}" text-anchor="middle"
              font-size="${(9.5 * k).toFixed(1)}" font-weight="700" fill="#ffffff"
              font-family="ui-monospace,monospace">${count}</text>` : ''}
        ${showLabel ? `<text x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${at[2]}"
              font-size="${(11.5 * k).toFixed(1)}" font-weight="600"
              fill="${/Creek|customs/i.test(pt.label) ? DARK : BLUE}" font-family="inherit"
              stroke="#ffffff" stroke-width="${(3.8 * k).toFixed(2)}" paint-order="stroke"
              stroke-linejoin="round">${esc(pt.label)}</text>` : ''}
      </g>`;
    };

    const atOrigin = marks.filter(m => m.t === 0).length;
    const atDest = marks.filter(m => m.t >= 0.82 && m.t < 0.9).length;
    const atCustoms = marks.filter(m => m.t >= 0.9 && m.t < 0.95).length;
    const delivered = marks.filter(m => m.t === 1).length;

    return `
      <div class="${inPanel ? '' : 'rounded-2xl border bg-white shadow-sm'} d2d-rise"
           style="overflow:hidden;${inPanel ? 'flex:1;display:flex;flex-direction:column;min-height:0;' : 'margin-bottom:12px;'}">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;
             padding:${inPanel ? '0 16px 10px' : '11px 14px'};${inPanel ? '' : 'border-bottom:.5px solid rgba(0,0,0,.07);'}">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
            ${inPanel ? '' : `<span style="font-size:12px;font-weight:600;color:${DARK};">Live tracking</span>
            <span style="font-size:10.5px;color:${MID};">${moving} in transit</span>`}
            <span style="display:flex;gap:6px;">
              ${[['live', 'All in flight'], ['week', 'This week']].map(([k, l]) =>
                `<button class="d2d-filt ${_mapScope === k ? 'on' : ''}" data-scope="${k}">${l}</button>`).join('')}
            </span>
            <span style="display:flex;gap:6px;">
              ${[['all', 'All'], ['sea', 'Sea'], ['air', 'Air'], ['late', 'Late only']].map(([k, l]) =>
                `<button class="d2d-filt ${_mapFilter === k ? 'on' : ''}" data-filt="${k}">${l}</button>`).join('')}
            </span>
          </div>
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
            ${(inPanel ? [['Sea', SEA_LINE], ['Air', BLUE], ['On plan', LIME], ['Drifting', YELL], ['Late or held', BRAND], ['Delivered', LINK]]
                        : [['Sea', SEA_LINE], ['Air', BLUE]]).map(([l, c]) =>
              `<span style="font-size:10.5px;color:${MID};"><span style="display:inline-block;width:8px;height:8px;
                 border-radius:50%;background:${c};margin-right:5px;"></span>${l}</span>`).join('')}
            <button class="d2d-btn" data-mapfull="1" style="padding:5px 9px;min-height:32px;font-size:10.5px;display:inline-flex;align-items:center;gap:5px;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M9 3H3v6"/><path d="M15 21h6v-6"/><path d="M3 3l7 7"/><path d="M21 21l-7-7"/></svg>
              Full screen</button>
          </div>
        </div>

        <div class="d2d-mapbox" style="position:relative;background:${MAP_PAL.sea_bg};
             ${inPanel ? 'flex:1;min-height:420px;' : 'height:400px;'}">
          ${/* The world is painted onto a canvas from real country shapes; routes and markers
                sit on an SVG above it, positioned by the same projection. */ ''}
          <canvas class="d2d-mapcanvas" style="position:absolute;inset:0;width:100%;height:100%;display:block;"></canvas>
          <svg class="d2d-mapsvg" style="position:absolute;inset:0;width:100%;height:100%;display:block;"
               role="img" aria-label="Where this week's shipments are"></svg>
          <div class="d2d-maploading" style="position:absolute;inset:0;display:flex;align-items:center;
               justify-content:center;font-size:11.5px;color:${MID};">Drawing the map&hellip;</div>

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

  // A tile, not a full-width strip. Seven stages across 2,000px was a lot of furniture for
  // "where is it"; the tile answers that, and the detail lives one click away.
  function shipmentCard(sh) {
    const ev = evMap(sh);
    const done = STAGES.filter(([k]) => ev[k] && ev[k].actual_at).length;
    const pct = Math.round(done / STAGES.length * 100);
    const od = overdueOf(sh);
    const slip = slipOf(sh);
    let liveIdx = -1;
    STAGES.forEach(([k], i2) => { if (ev[k] && ev[k].actual_at) liveIdx = i2; });
    const stageLabel = od ? od.label + ' overdue'
      : liveIdx >= 0 ? STAGES[liveIdx][1]
      : 'Not yet collected';
    const pill = sh.status === 'delivered' ? ['Delivered', LINK, 'rgba(155,171,21,.20)']
      : od ? ['+' + od.days + 'd overdue', '#fff', BRAND]
      : (slip != null && slip > 0) ? ['+' + slip + ' days', '#fff', BRAND]
      : sh.status === 'in_transit' ? ['On plan', LINK, 'rgba(155,171,21,.20)']
      : ['Booked', MID, '#F2F2F5'];
    const bar = od || (slip || 0) > 0 ? BRAND : LIME;
    const nextStage = STAGES[Math.min(liveIdx + 1, STAGES.length - 1)];
    const nextPlan = sh['plan_' + nextStage[0]];

    return `
      <button type="button" class="rounded-2xl border bg-white shadow-sm d2d-rise d2d-lift d2d-tilecard"
              data-open="${esc(sh.id)}" style="padding:14px 15px;text-align:left;width:100%;cursor:pointer;">
        <span style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px;">
          <span style="min-width:0;">
            <span class="d2d-num" style="display:block;font-size:13px;color:${DARK};overflow:hidden;
                  text-overflow:ellipsis;white-space:nowrap;">${esc(sh.reference || 'container not advised')}</span>
            <span style="display:block;font-size:10.5px;color:${MID};margin-top:2px;">${esc([sh.container_type, sh.carrier].filter(Boolean).join(' · '))}</span>
          </span>
          <span style="font-size:10px;font-weight:700;border-radius:6px;padding:3px 8px;white-space:nowrap;
                color:${pill[1]};background:${pill[2]};">${esc(pill[0])}</span>
        </span>

        <span style="display:block;margin-top:12px;height:6px;background:#F0F0F3;border-radius:3px;overflow:hidden;">
          <span style="display:block;height:6px;width:${pct}%;background:${bar};border-radius:3px;"></span>
        </span>
        <span style="display:flex;align-items:baseline;justify-content:space-between;margin-top:6px;">
          <span style="font-size:11px;color:${od ? BRAND : DARK};font-weight:${od ? 600 : 500};">${esc(stageLabel)}</span>
          <span style="font-size:10px;color:${LIGHT};">${done} of ${STAGES.length}</span>
        </span>

        <span style="display:flex;align-items:baseline;justify-content:space-between;margin-top:10px;
              padding-top:9px;border-top:.5px solid rgba(0,0,0,.05);">
          <span style="font-size:10.5px;color:${MID};">${sh.status === 'delivered' ? 'Delivered' : 'Next: ' + esc(nextStage[1])}</span>
          <span class="d2d-num" style="font-size:11px;color:${DARK};">${nextPlan && sh.status !== 'delivered' ? 'plan ' + esc(day(nextPlan)) : ''}</span>
        </span>
      </button>`;
  }

  // How a missed milestone reads to someone who has to act on it.
  const DELAY_LABEL = {
    pickup:           'Pickup delayed',
    origin_cleared:   'Origin clearance delayed',
    departed:         'Departure delayed',
    arrived:          'Arrival delayed',
    dest_cleared:     'Customs clearance delayed',
    out_for_delivery: 'Dispatch delayed',
    delivered:        'Delivery delayed',
  };

  // Will this arrive on time? Answered from slip so far against the buffer still left before
  // the planned delivery. Deliberately not dressed up as a probability: it is arithmetic on
  // recorded dates, and the card says so.
  function outlook(sh) {
    const ev = evMap(sh);
    if (sh.status === 'delivered') {
      const d = daysBetween(sh.plan_delivered, ev.delivered && ev.delivered.actual_at);
      return { state: 'done', label: d > 0 ? `Delivered ${d}d late` : 'Delivered on time',
               ink: d > 0 ? BRAND : LINK, pct: 100, why: 'closed' };
    }
    const slip = slipOf(sh) || 0;
    const od = overdueOf(sh);
    const drift = Math.max(slip, od ? od.days : 0);
    const total = daysBetween(sh.plan_pickup, sh.plan_delivered);
    let done = -1;
    STAGES.forEach(([k], i) => { if (ev[k] && ev[k].actual_at) done = i; });
    const pct = Math.round((done + 1) / STAGES.length * 100);

    // Everything after arrival is short and hard to recover in; before departure there is
    // still a sailing's worth of room.
    const recoverable = done < 3 ? 4 : 2;
    if (!drift) return { state: 'on', label: 'On track', ink: LINK, pct,
      why: total ? `${total} days planned, nothing lost yet` : 'nothing lost yet' };
    if (drift <= recoverable) return { state: 'watch', label: `${drift}d behind, recoverable`, ink: YINK, pct,
      why: `Can still be made up before ${done < 3 ? 'departure' : 'delivery'}` };
    return { state: 'late', label: `${drift}d behind`, ink: BRAND, pct,
      why: `Arrival slips to about ${day(addDaysISO(sh.plan_delivered, drift))}` };
  }
  const addDaysISO = (ymd, n) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return ymd;
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };

  // ── What is waiting on somebody ──
  // The dashboard's job is to say what needs a person. Counts of things already moving are
  // reassurance; this is the part that changes what you do next.
  function decisions(d) {
    const out = [];
    const released = (d.bookings || []).filter(b => b.status === 'released').length;
    const drafts = (d.bookings || []).filter(b => b.status === 'draft').length;
    const awaitingPartner = (d.requests || []).filter(r => ['sent', 'repricing'].includes(r.state)).length;
    const unassigned = (d.orders || []).filter(x => !(x.containers || []).length).length;
    const noRef = (d.allShipments || []).filter(x => !x.reference && x.status !== 'delivered').length;

    if (awaitingPartner) out.push({ n: awaitingPartner, who: 'the partner',
      what: 'rate request' + (awaitingPartner === 1 ? '' : 's') + ' out', tab: 'bookings', ink: BLUE });
    if (drafts && _internal) out.push({ n: drafts, who: 'you',
      what: 'option' + (drafts === 1 ? '' : 's') + ' to price and release', tab: 'pricing', ink: YINK });
    if (released) out.push({ n: released, who: 'the client',
      what: 'option' + (released === 1 ? '' : 's') + ' to approve', tab: 'bookings', ink: BLUE });
    if (noRef) out.push({ n: noRef, who: 'the partner',
      what: 'container number' + (noRef === 1 ? '' : 's') + ' not advised', tab: 'list', ink: YINK });
    if (unassigned) out.push({ n: unassigned, who: 'you',
      what: 'order' + (unassigned === 1 ? '' : 's') + ' not on a container', tab: 'po', ink: YINK });
    return out;
  }

  // What has actually been recorded lately, newest first. On a screen that is mostly plans,
  // this is the part that shows the week moving.
  function recentActivity(d) {
    const rows = [];
    for (const sh of (d.allShipments || [])) {
      for (const e of (sh.events || [])) {
        if (!e.actual_at) continue;
        rows.push({ sh, stage: e.stage, at: e.actual_at, source: e.source,
                    recorded: e.recorded_at || e.actual_at });
      }
    }
    rows.sort((a, b) => String(b.recorded).localeCompare(String(a.recorded)));
    const label = (k) => (STAGES.find(([kk]) => kk === k) || [k, k])[1];
    // A stage name and a container number is a log line. What matters is whether that date
    // put the shipment ahead, behind, or left it where it was.
    return rows.slice(0, 6).map(r => {
      const plan = r.sh['plan_' + r.stage];
      const d = daysBetween(plan, r.at);
      const meaning = d == null ? { text: 'recorded', ink: MID }
        : d > 0 ? { text: `${d} day${d === 1 ? '' : 's'} behind plan — needs bringing back`, ink: BRAND }
        : d < 0 ? { text: `${-d} day${d === -1 ? '' : 's'} early — buffer gained`, ink: LINK }
        : { text: 'on plan', ink: LINK };
      return { ...r, label: label(r.stage), meaning, drift: d };
    });
  }

  // ── Shipments, in full ──
  // The long row the dashboard could not carry: every milestone, what is aboard, and the one
  // thing this shipment needs next. One row per shipment, newest week first.
  // One cell shape for every figure on a row, so the columns line up down the page instead of
  // each row sizing itself.
  const cell = (label, value, ink) => `
    <span style="padding:5px 11px;text-align:right;min-width:62px;border-left:.5px solid rgba(0,0,0,.07);">
      <span style="display:block;font-size:8.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">${label}</span>
      <span class="d2d-num" style="display:block;font-size:13px;font-weight:600;color:${ink || DARK};line-height:1.25;">${value}</span>
    </span>`;

  function shipmentRow(sh) {
    const ev = evMap(sh);
    const od = overdueOf(sh), slip = slipOf(sh);
    let liveIdx = -1; STAGES.forEach(([kk], i) => { if (ev[kk] && ev[kk].actual_at) liveIdx = i; });
    const odIdx = od ? STAGES.findIndex(([kk]) => kk === od.stage) : -1;
    const pill = sh.status === 'delivered' ? ['Delivered', LINK, 'rgba(155,171,21,.20)']
      : od ? [od.label + ' overdue', '#fff', BRAND]
      : (slip != null && slip > 0) ? ['+' + slip + ' days', '#fff', BRAND]
      : sh.status === 'in_transit' ? ['On plan', LINK, 'rgba(155,171,21,.20)']
      : ['Booked', MID, '#F2F2F5'];

    const look = outlook(sh);
    // Pallets, cartons, units and volume — the figures the booking was quoted on. Volume is a
    // division of the booking total across its containers, so it arrives as 29.599999999999998
    // unless it is rounded here.
    const cg = sh.cargo || {};
    const tidy = (v, dp) => {
      const n = Number(v);
      if (!isFinite(n)) return null;
      const r = Math.round(n * Math.pow(10, dp)) / Math.pow(10, dp);
      return r.toLocaleString(undefined, { maximumFractionDigits: dp });
    };
    const cargo = [
      ['Pallets', cg.pallets, 0], ['Cartons', cg.cartons, 0],
      ['Units', cg.units != null ? cg.units : (sh.units || null), 0],
      ['CBM', cg.cbm != null ? cg.cbm : (sh.cbm || null), 1],
    ].filter(([, v]) => v != null && v !== '' && Number(v) > 0)
     .map(([l, v, dp]) => [l, tidy(v, dp)]);
    // Capacity is roughly 67 CBM for a 40ft box and 33 for a 20ft. Only shown when the CBM
    // aboard is known, which means the order file has been loaded.
    const cap = /40/.test(sh.container_type || '') ? 67 : /20/.test(sh.container_type || '') ? 33 : null;
    const cbmAboard = sh.cbm != null ? sh.cbm : (sh.cargo && sh.cargo.cbm);
    const util = (cap && cbmAboard) ? { pct: Math.round(Number(cbmAboard) / cap * 100) } : null;

    // Door to door, as planned and — once delivered — as it actually ran.
    const planned = daysBetween(sh.plan_pickup, sh.plan_delivered);
    const actualSpan = daysBetween(ev.pickup && ev.pickup.actual_at, ev.delivered && ev.delivered.actual_at);

    // The next thing to do about THIS shipment, from its own dates.
    const next = !sh.reference ? { kind: 'Missing', ink: YINK, accent: YELL,
                    what: 'Container number not advised', why: 'The partner cannot report milestones without it' }
      : od ? { kind: 'Chase', ink: BRAND, accent: BRAND,
               what: od.label + ' not recorded', why: 'Planned ' + day(sh['plan_' + od.stage]) + ', ' + od.days + ' days ago' }
      : sh.status === 'delivered' ? { kind: 'Done', ink: LINK, accent: LIME,
               what: 'Delivered', why: 'Nothing outstanding' }
      : { kind: 'Next', ink: BLUE, accent: BLUE,
          what: STAGES[Math.min(liveIdx + 1, STAGES.length - 1)][1],
          why: 'planned ' + day(sh['plan_' + STAGES[Math.min(liveIdx + 1, STAGES.length - 1)][0]]) };

    const cells = STAGES.map(([kk, label], i) => {
      const e = ev[kk], actual = e && e.actual_at;
      const plan = sh['plan_' + kk];
      const dd = daysBetween(plan, actual);
      const late = dd != null && dd > 0;
      const overdue = i === odIdx;
      const colour = overdue ? BRAND : (!actual ? '#D6D6DB' : (late ? BRAND : LIME));
      const ink = overdue ? BRAND : (!actual ? LIGHT : (late ? BRAND : LINK));
      const live = overdue || i === liveIdx;
      const leftFill = i === 0 ? 'transparent' : (i <= liveIdx ? LIME : '#E4E4E9');
      const rightFill = i === STAGES.length - 1 ? 'transparent' : (i < liveIdx ? LIME : '#E4E4E9');
      const tag = _internal ? 'button' : 'div';
      const attrs = _internal
        ? ` type="button" class="d2d-st d2d-edit" data-ship="${esc(sh.id)}" data-stage="${kk}" data-label="${esc(label)}"
            aria-label="Record ${esc(label)} for ${esc(sh.reference || 'this shipment')}"` : ' class="d2d-st"';
      return `<${tag}${attrs}>
        <span class="d2d-line" style="left:0;right:50%;background:${leftFill};"></span>
        <span class="d2d-line ${i === liveIdx ? 'd2d-travel' : ''}" style="left:50%;right:0;
              background:${i === liveIdx ? `linear-gradient(90deg, ${LIME} 0%, ${LIME} 38%, #E4E4E9 62%, #E4E4E9 100%)` : rightFill};
              ${i === liveIdx ? 'background-size:220% 100%;' : ''}"></span>
        <span style="font-size:9px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;text-align:center;min-height:22px;">${label}</span>
        <span style="position:relative;display:inline-flex;align-items:center;justify-content:center;">
          ${live ? `<span class="d2d-halo" style="background:${overdue ? 'rgba(153,0,51,.22)' : 'rgba(155,171,21,.22)'};"></span>` : ''}
          <span class="d2d-dot ${live ? 'd2d-pulse' : ''}" style="border-color:${colour};
                background:${actual ? (late ? 'rgba(153,0,51,.10)' : 'rgba(155,171,21,.16)') : '#fff'};">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="${colour}" stroke-width="1.9"
                 stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${ICONS[i]}"></path></svg>
          </span>
        </span>
        <span class="d2d-num" style="font-size:10px;color:${LIGHT};">${plan ? day(plan) : ''}</span>
        <span class="d2d-num" style="font-size:11px;font-weight:600;color:${ink};">${actual ? day(actual) : (overdue ? 'overdue' : '&middot;')}</span>
        ${late ? `<span style="font-size:10px;font-weight:700;color:${BRAND};">+${dd}d</span>` : ''}
      </${tag}>`;
    }).join('');

    return `
      <div class="rounded-2xl border bg-white shadow-sm d2d-rise" style="padding:15px 18px;margin-bottom:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;">
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
            ${/* The week, stated plainly. Everyone plans in week numbers, so it leads. */ ''}
            <span style="display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0;
                  background:#F2F2F5;border-radius:10px;padding:5px 11px;min-width:56px;">
              <span style="font-size:8.5px;font-weight:700;color:${LIGHT};letter-spacing:.08em;">WEEK</span>
              <span class="d2d-num" style="font-size:17px;font-weight:700;color:${DARK};line-height:1.05;">${isoWeek(sh.week_start) || '—'}</span>
              <span style="font-size:8.5px;color:${MID};">${esc(day(sh.week_start))}</span>
            </span>
            <span>
              <button type="button" class="d2d-num" data-open="${esc(sh.id)}"
                      style="font-size:14px;color:${DARK};background:none;border:0;padding:0;cursor:pointer;
                             border-bottom:1px dashed rgba(0,0,0,.25);">${esc(sh.reference || 'container not advised')}</button>
              <span style="display:block;font-size:11px;color:${MID};margin-top:2px;">${esc([sh.container_type, sh.carrier, sh.vessel].filter(Boolean).join(' · '))}</span>
              ${/* The booked lane, named rather than implied. */ ''}
              <span style="display:block;font-size:11px;color:${MID};margin-top:3px;">
                <span style="font-size:8.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">From</span>
                <b style="color:${DARK};font-weight:600;">${esc(sh.service || sh.origin || '—')}</b>
                <span style="color:${LIGHT};">&rarr;</span>
                <span style="font-size:8.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">To</span>
                <b style="color:${DARK};font-weight:600;">${esc(destOf(sh).label)}</b></span>
            </span>
          </div>
          <div style="display:flex;flex-direction:column;align-items:flex-end;gap:7px;flex-shrink:0;">
            <div style="display:flex;align-items:center;gap:9px;">
              ${/* The forecast: arithmetic on recorded dates, not a dressed-up probability. */ ''}
              <span style="display:flex;align-items:center;gap:9px;padding:5px 11px;border-radius:9px;
                    background:${look.state === 'late' ? 'rgba(153,0,51,.08)' : look.state === 'watch' ? 'rgba(254,208,0,.14)' : 'rgba(155,171,21,.14)'};">
                <svg width="24" height="24" viewBox="0 0 36 36" aria-hidden="true" style="flex-shrink:0;">
                  <circle cx="18" cy="18" r="15" fill="none" stroke="rgba(0,0,0,.08)" stroke-width="4"/>
                  <circle cx="18" cy="18" r="15" fill="none" stroke="${look.ink}" stroke-width="4" stroke-linecap="round"
                          stroke-dasharray="${(look.pct / 100 * 94.2).toFixed(1)} 94.2" transform="rotate(-90 18 18)"/>
                </svg>
                <span>
                  <span style="display:block;font-size:11.5px;font-weight:600;color:${look.ink};line-height:1.3;">${esc(look.label)}</span>
                  <span style="display:block;font-size:10px;color:${MID};">${esc(look.why)}</span>
                </span>
              </span>
              <span style="font-size:10.5px;font-weight:700;border-radius:6px;padding:3px 9px;
                    color:${pill[1]};background:${pill[2]};">${esc(pill[0])}</span>
            </div>

            ${/* Everything measurable about the load, on one line beneath the outlook. */ ''}
            ${(cargo.length || util || planned != null || sh.po_count) ? `
              <div class="d2d-cells" style="display:flex;align-items:stretch;border:.5px solid rgba(0,0,0,.08);
                   border-radius:9px;overflow:hidden;background:#fff;">
                ${planned != null ? cell('Planned', planned + 'd', null) : ''}
                ${sh.po_count ? cell('Orders', String(sh.po_count), null) : ''}
                ${cargo.map(([l, v]) => cell(l, v, null)).join('')}
                ${util ? cell('Util', util.pct + '%', util.pct < 70 ? YINK : LINK) : ''}
              </div>` : ''}
          </div>
        </div>

        <div class="d2d-striphold" style="max-width:820px;margin-top:4px;"><div class="d2d-strip">${cells}</div></div>

        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:11px;
             padding-top:11px;border-top:.5px solid rgba(0,0,0,.05);flex-wrap:wrap;">
          <div style="display:flex;align-items:baseline;gap:9px;padding-left:10px;border-left:3px solid ${next.accent};">
            <span style="font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:${next.ink};">${esc(next.kind)}</span>
            <span style="font-size:12px;font-weight:600;color:${DARK};">${esc(next.what)}</span>
            <span style="font-size:11px;color:${MID};">${esc(next.why)}</span>
          </div>
          <button class="d2d-btn" data-open="${esc(sh.id)}" style="min-height:34px;padding:5px 11px;font-size:11px;">Open &rarr;</button>
        </div>
      </div>`;
  }

  function paintList(d) {
    const rows = (d.shipments || []).slice();
    const other = (d.allShipments || []).filter(x => !rows.some(y => y.id === x.id) && x.status !== 'delivered');
    return `
      <div style="display:flex;align-items:baseline;justify-content:space-between;gap:9px;margin-bottom:10px;flex-wrap:wrap;">
        <span style="display:flex;align-items:baseline;gap:9px;">
          <span style="font-size:13px;font-weight:600;color:${DARK};">Week of ${esc(day(_week))}</span>
          <span style="font-size:11px;color:${MID};">every milestone, and what each shipment needs next</span>
        </span>
        <button class="d2d-btn" data-go="week" style="min-height:34px;padding:5px 11px;font-size:11px;">Week summary &rarr;</button>
      </div>
      ${rows.length ? rows.map(shipmentRow).join('')
        : `<div class="rounded-2xl border bg-white shadow-sm d2d-empty">Nothing booked for this week yet.</div>`}

      ${other.length ? `
        <div style="display:flex;align-items:baseline;gap:9px;margin:18px 0 10px;">
          <span style="font-size:13px;font-weight:600;color:${DARK};">Still in flight from other weeks</span>
          <span style="font-size:11px;color:${MID};">${other.length} shipment${other.length === 1 ? '' : 's'}</span>
        </div>
        ${other.map(shipmentRow).join('')}` : ''}`;
  }

  // ── Shared pieces ──
  const crumb = (parts) => `
    <div style="display:flex;align-items:center;gap:7px;font-size:11.5px;margin-bottom:12px;flex-wrap:wrap;">
      ${parts.map((p2, i) => p2.go
        ? `<button class="d2d-crumb" data-go="${p2.go}" ${p2.id ? `data-goid="${esc(p2.id)}"` : ''}>${esc(p2.label)}</button>
           ${i < parts.length - 1 ? `<span style="color:#D6D6DB;">&rsaquo;</span>` : ''}`
        : `<span style="color:${DARK};font-weight:600;">${esc(p2.label)}</span>
           ${i < parts.length - 1 ? `<span style="color:#D6D6DB;">&rsaquo;</span>` : ''}`).join('')}
    </div>`;

  // Data we do not have yet is shown as a marked panel rather than invented. A convincing
  // placeholder is worse than an empty one: it gets mistaken for fact.
  const pending = (title, why, height) => `
    <div class="rounded-2xl border bg-white shadow-sm" style="padding:16px 18px;${height ? `min-height:${height}px;` : ''}
         display:flex;flex-direction:column;justify-content:center;">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="width:7px;height:7px;border-radius:50%;background:${YELL};"></span>
        <span style="font-size:12.5px;font-weight:600;color:${DARK};">${esc(title)}</span>
      </div>
      <div style="font-size:11.5px;color:${MID};line-height:1.5;margin-top:5px;">${why}</div>
    </div>`;

  const statRow = (label, value, sub, colour) => `
    <div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;padding:8px 0;
         border-bottom:.5px solid rgba(0,0,0,.05);">
      <span style="font-size:11.5px;color:${MID};">${label}</span>
      <span style="text-align:right;">
        <span class="d2d-num" style="display:block;font-size:13px;font-weight:600;color:${colour || DARK};">${value}</span>
        ${sub ? `<span style="display:block;font-size:10px;color:${LIGHT};">${sub}</span>` : ''}
      </span>
    </div>`;

  // ── Week ──
  // Mockup 2: the containers moving this week, what the week holds, and what is outstanding.
  function paintWeek(d) {
    const ship = d.shipments;
    const approved = d.bookings.find(b => b.status === 'approved');
    const slips = ship.map(slipOf).filter(x => x != null && x > 0);
    const docsPending = ship.filter(x => !x.reference).length;

    return `
      ${crumb([{ label: _tab === 'list' ? 'Shipments' : 'Live map', go: 'dashboard' }, { label: 'Week of ' + day(_week) }])}
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-3 items-start">
        <div class="lg:col-span-2" style="min-width:0;">
          <div style="display:flex;align-items:baseline;gap:9px;margin-bottom:8px;">
            <span style="font-size:12.5px;font-weight:600;color:${DARK};">Containers this week</span>
            <span style="font-size:11px;color:${MID};">click one for its milestones</span>
          </div>
          ${ship.length
            ? `<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">${ship.map(shipmentCard).join('')}</div>`
            : `<div class="rounded-2xl border bg-white shadow-sm d2d-empty">Nothing booked for this week yet.</div>`}
        </div>

        <div style="display:flex;flex-direction:column;gap:12px;min-width:0;">
          <div class="rounded-2xl border bg-white shadow-sm" style="padding:14px 16px;">
            <div style="font-size:12.5px;font-weight:600;color:${DARK};margin-bottom:4px;">The week</div>
            ${statRow('Cargo ready', esc(day(_week)))}
            ${statRow('Containers', ship.length, approved ? esc(approved.title || '') : '')}
            ${statRow('Behind plan', slips.length, slips.length ? 'worst ' + Math.max(...slips) + ' days' : 'all on plan', slips.length ? BRAND : DARK)}
            ${statRow('Transit booked', approved && approved.transit_days ? approved.transit_days + ' days' : '&ndash;', approved ? esc(approved.carrier || '') : '')}
          </div>

          ${pending('Volume and utilisation',
            'CBM, weight and how full each box is arrive with GRBA&rsquo;s order file. Until then a utilisation figure would be a guess.')}

          <div class="rounded-2xl border bg-white shadow-sm" style="padding:14px 16px;">
            <div style="font-size:12.5px;font-weight:600;color:${DARK};margin-bottom:8px;">Exceptions</div>
            ${docsPending ? `<div style="font-size:12px;color:${DARK};line-height:1.45;">
                 <b>${docsPending}</b> container${docsPending === 1 ? '' : 's'} without a number advised</div>
               <div style="font-size:10.5px;color:${MID};margin-top:2px;">The partner cannot report milestones without it</div>` : ''}
            ${actions(d).filter(a => a.kind !== 'Clear').slice(0, 3).map(a => `
              <div style="margin-top:10px;padding-left:10px;border-left:2px solid ${a.accent};">
                <div style="font-size:12px;color:${DARK};line-height:1.4;">${esc(a.what)}</div>
                ${a.where ? `<div class="d2d-num" style="font-size:10px;color:${LIGHT};">${esc(a.where)}</div>` : ''}
                <div style="font-size:10.5px;color:${MID};">${esc(a.effect)}</div>
              </div>`).join('')}
            ${!docsPending && actions(d).every(a => a.kind === 'Clear')
              ? `<div style="font-size:12px;color:${LINK};">Nothing outstanding.</div>` : ''}
          </div>
        </div>
      </div>`;
  }

  // ── Container ──
  // Mockup 3: plan against actual, where the days went, what is aboard, and where each date
  // came from.
  function paintContainer(d) {
    const sh = findShip(_openId);
    if (!sh) return `<div class="d2d-empty">That shipment is no longer in view.</div>`;
    const ev = evMap(sh);
    const od = overdueOf(sh), slip = slipOf(sh);
    let liveIdx = -1; STAGES.forEach(([kk], i) => { if (ev[kk] && ev[kk].actual_at) liveIdx = i; });

    // Where the days went: variance at each stage that has both a plan and an actual.
    const legs = STAGES.map(([kk, label]) => {
      const dd = daysBetween(sh['plan_' + kk], ev[kk] && ev[kk].actual_at);
      return dd == null ? null : { label, d: dd };
    }).filter(Boolean);
    const worstLeg = legs.reduce((m, x) => (m == null || x.d > m.d ? x : m), null);
    const maxAbs = Math.max(1, ...legs.map(x => Math.abs(x.d)));

    return `
      ${crumb([{ label: _tab === 'list' ? 'Shipments' : 'Live map', go: 'dashboard' },
               { label: 'Week of ' + day(sh.week_start), go: 'week' },
               { label: sh.reference || 'container not advised' }])}

      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:12px;">
        <div>
          <div class="d2d-num" style="font-size:20px;color:${DARK};letter-spacing:-.01em;">${esc(sh.reference || 'container not advised')}</div>
          <div style="font-size:11.5px;color:${MID};margin-top:2px;">${esc([sh.container_type, sh.carrier, sh.vessel].filter(Boolean).join(' · ')) || 'no vessel advised'}</div>
        </div>
        <div style="display:flex;align-items:center;gap:10px;">
          <span style="font-size:11px;font-weight:700;border-radius:7px;padding:5px 11px;
                color:${od || slip > 0 ? '#fff' : LINK};background:${od || slip > 0 ? BRAND : 'rgba(155,171,21,.20)'};">
            ${od ? esc(od.label) + ' overdue by ' + od.days + 'd' : (slip > 0 ? '+' + slip + ' days' : 'On plan')}</span>
          ${_internal ? `<button class="d2d-btn d2d-ref" data-ref="${esc(sh.id)}" data-cur="${esc(sh.reference || '')}">Container &amp; vessel</button>` : ''}
        </div>
      </div>

      <div class="rounded-2xl border bg-white shadow-sm" style="padding:16px 18px;margin-bottom:12px;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:14px;">
          <span style="font-size:13px;font-weight:600;color:${DARK};">Plan against actual</span>
          <span style="font-size:11px;color:${MID};">${sh.plan_frozen_at ? 'plan frozen at approval' : 'no frozen plan'}</span>
        </div>
        <div class="d2d-striphold" style="max-width:none;"><div class="d2d-strip">
          ${STAGES.map(([kk, label], i) => {
            const e = ev[kk], actual = e && e.actual_at;
            const plan = sh['plan_' + kk];
            const dd = daysBetween(plan, actual);
            const late = dd != null && dd > 0;
            const isOd = od && od.stage === kk;
            const colour = isOd ? BRAND : (!actual ? '#D6D6DB' : (late ? BRAND : LIME));
            const ink = isOd ? BRAND : (!actual ? LIGHT : (late ? BRAND : LINK));
            const live = isOd || i === liveIdx;
            const leftFill = i === 0 ? 'transparent' : (i <= liveIdx ? LIME : '#E4E4E9');
            const rightFill = i === STAGES.length - 1 ? 'transparent' : (i < liveIdx ? LIME : '#E4E4E9');
            const tag = _internal ? 'button' : 'div';
            const attrs = _internal
              ? ` type="button" class="d2d-st d2d-edit" data-ship="${esc(sh.id)}" data-stage="${kk}" data-label="${esc(label)}"
                  aria-label="Record ${esc(label)}"` : ' class="d2d-st"';
            return `<${tag}${attrs}>
              <span class="d2d-line" style="left:0;right:50%;background:${leftFill};"></span>
              <span class="d2d-line" style="left:50%;right:0;background:${rightFill};"></span>
              <span style="font-size:9px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;text-align:center;min-height:22px;">${label}</span>
              <span style="position:relative;display:inline-flex;align-items:center;justify-content:center;">
                ${live ? `<span class="d2d-halo" style="background:${isOd ? 'rgba(153,0,51,.22)' : 'rgba(155,171,21,.22)'};"></span>` : ''}
                <span class="d2d-dot ${live ? 'd2d-pulse' : ''}" style="border-color:${colour};
                      background:${actual ? (late ? 'rgba(153,0,51,.10)' : 'rgba(155,171,21,.16)') : '#fff'};">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="${colour}" stroke-width="1.9"
                       stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${ICONS[i]}"></path></svg>
                </span>
              </span>
              <span class="d2d-num" style="font-size:10px;color:${LIGHT};">${plan ? 'plan ' + day(plan) : ''}</span>
              <span class="d2d-num" style="font-size:11.5px;font-weight:600;color:${ink};">${actual ? day(actual) : (isOd ? 'overdue' : '&middot;')}</span>
              ${actual ? `<span style="font-size:9px;text-transform:uppercase;letter-spacing:.03em;color:${e.source === 'manual' ? YINK : MID};">${esc(e.source)}</span>` : ''}
              ${late ? `<span style="font-size:10px;font-weight:700;color:${BRAND};">+${dd}d</span>` : ''}
            </${tag}>`;
          }).join('')}
        </div></div>
        ${od ? `<div style="margin-top:14px;padding:9px 13px;background:rgba(153,0,51,.08);border-radius:9px;font-size:12px;color:${DARK};">
            <b>${esc(od.label)} has not been recorded.</b> Planned ${esc(day(sh['plan_' + od.stage]))}, ${od.days} day${od.days === 1 ? '' : 's'} ago.</div>` : ''}
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-3 gap-3 items-start">
        <div class="rounded-2xl border bg-white shadow-sm" style="padding:15px 17px;">
          <div style="font-size:12.5px;font-weight:600;color:${DARK};margin-bottom:3px;">Where the days went</div>
          <div style="font-size:11px;color:${MID};margin-bottom:11px;">variance at each recorded stage</div>
          ${legs.length ? legs.map(x => `
            <div style="display:flex;align-items:center;gap:10px;padding:5px 0;">
              <span style="font-size:11.5px;color:${MID};width:104px;flex-shrink:0;">${esc(x.label)}</span>
              <span style="flex:1;height:8px;background:#F0F0F3;border-radius:4px;overflow:hidden;">
                <span style="display:block;height:8px;width:${Math.round(Math.abs(x.d) / maxAbs * 100)}%;
                      background:${x.d > 0 ? BRAND : LIME};border-radius:4px;"></span></span>
              <span class="d2d-num" style="font-size:11px;color:${x.d > 0 ? BRAND : LINK};width:52px;text-align:right;">
                ${x.d > 0 ? '+' + x.d + 'd' : (x.d === 0 ? 'on plan' : x.d + 'd')}</span>
            </div>`).join('')
            : `<div style="font-size:11.5px;color:${MID};">Nothing recorded yet.</div>`}
          ${worstLeg && worstLeg.d > 0 ? `<div style="font-size:11px;color:${MID};margin-top:9px;padding-top:9px;
               border-top:.5px solid rgba(0,0,0,.06);line-height:1.45;">
               Most of the delay is at <b style="color:${DARK};">${esc(worstLeg.label)}</b>.</div>` : ''}
        </div>

        <div class="rounded-2xl border bg-white shadow-sm" style="padding:15px 17px;">
          <div style="font-size:12.5px;font-weight:600;color:${DARK};margin-bottom:3px;">Every date, and its source</div>
          <div style="font-size:11px;color:${MID};margin-bottom:9px;">a carrier feed and someone&rsquo;s typing deserve different trust</div>
          ${STAGES.filter(([kk]) => ev[kk] && ev[kk].actual_at).map(([kk, label]) => {
            const e = ev[kk];
            const badge = e.source === 'carrier' ? [LINK, 'rgba(155,171,21,.20)']
                        : e.source === 'partner' ? [BLUE, 'rgba(44,111,187,.12)']
                        : e.source === 'import' ? [MID, '#F2F2F5'] : [YINK, 'rgba(254,208,0,.20)'];
            return `<div style="display:flex;align-items:baseline;justify-content:space-between;gap:10px;padding:6px 0;
                    border-bottom:.5px solid rgba(0,0,0,.05);">
              <span style="font-size:12px;color:${DARK};">${esc(label)}</span>
              <span style="display:flex;align-items:center;gap:8px;">
                <span class="d2d-num" style="font-size:11px;color:${MID};">${esc(day(e.actual_at))}</span>
                <span style="font-size:9.5px;font-weight:700;border-radius:5px;padding:2px 7px;
                      color:${badge[0]};background:${badge[1]};">${esc(e.source)}</span>
              </span></div>`;
          }).join('') || `<div style="font-size:11.5px;color:${MID};">No dates recorded yet.</div>`}
        </div>

        ${pending('Orders and lines aboard',
          'POs, SKUs and unit counts appear here once GRBA&rsquo;s purchase order file is connected. Nothing is shown until then rather than a placeholder that could be mistaken for cargo.')}
      </div>`;
  }

  // ── Orders ──
  // Upload, preview, then apply. Two steps on purpose: a bad file must never half-load into a
  // live week, and the preview is where supplier and container mismatches surface.
  let _poPreview = null, _poCsv = '';
  // Kept in state, not in the DOM: sending reloads the week, which would wipe a message
  // written straight onto the element.
  let _rfqNote = '';

  function paintPO(d) {
    const orders = d.orders || [];
    const units = orders.reduce((n, x) => n + (Number(x.units) || 0), 0);
    const assigned = orders.filter(x => (x.containers || []).length).length;

    return `
      <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:14px;margin-bottom:12px;flex-wrap:wrap;">
        <div>
          <div style="font-size:16px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Orders</div>
          <div style="font-size:11.5px;color:${MID};margin-top:2px;">Week of ${esc(day(_week))} &middot; purchase orders, their SKUs, and the containers carrying them</div>
        </div>
        ${_internal ? `<span style="display:flex;gap:8px;">
          <button class="d2d-btn" data-potemplate="1">Download the format</button>
          <button class="d2d-btn dark" data-poupload="1">Upload order file</button></span>` : ''}
      </div>

      ${orders.length ? `
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-3" style="margin-bottom:12px;">
          <div class="rounded-2xl border bg-white shadow-sm d2d-tile"><div class="d2d-tl">Orders</div>
            <div class="d2d-tv">${orders.length}</div><div class="d2d-ts">this week</div></div>
          <div class="rounded-2xl border bg-white shadow-sm d2d-tile"><div class="d2d-tl">Units</div>
            <div class="d2d-tv">${units.toLocaleString()}</div><div class="d2d-ts">across all SKUs</div></div>
          <div class="rounded-2xl border bg-white shadow-sm d2d-tile"><div class="d2d-tl">On a container</div>
            <div class="d2d-tv" style="color:${assigned === orders.length ? DARK : YINK};">${assigned}</div>
            <div class="d2d-ts">${orders.length - assigned} not yet assigned</div></div>
          <div class="rounded-2xl border bg-white shadow-sm d2d-tile"><div class="d2d-tl">Volume</div>
            <div class="d2d-tv">${orders.reduce((n, x) => n + (Number(x.cbm) || 0), 0).toFixed(1)}</div>
            <div class="d2d-ts">CBM declared</div></div>
        </div>

        <div class="rounded-2xl border bg-white shadow-sm" style="padding:0;overflow:hidden;">
          <div style="display:grid;grid-template-columns:110px 1fr 120px 92px 92px 1fr;gap:0 12px;padding:11px 18px 8px;
               background:#FBFBFC;font-size:9.5px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">
            <span>PO</span><span>Supplier</span><span>Cargo ready</span>
            <span style="text-align:right;">Units</span><span style="text-align:right;">SKUs</span><span>On board</span>
          </div>
          ${orders.map(x => `
            <div style="display:grid;grid-template-columns:110px 1fr 120px 92px 92px 1fr;gap:0 12px;padding:11px 18px;
                 border-top:.5px solid rgba(0,0,0,.06);align-items:center;">
              <span class="d2d-num" style="font-size:12.5px;color:${DARK};">${esc(x.po_number)}</span>
              <span style="font-size:12px;color:${DARK};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(x.supplier || '—')}</span>
              <span class="d2d-num" style="font-size:11.5px;color:${MID};">${x.cargo_ready_date ? esc(day(x.cargo_ready_date)) : '—'}</span>
              <span class="d2d-num" style="font-size:12.5px;color:${DARK};text-align:right;">${Number(x.units || 0).toLocaleString()}</span>
              <span class="d2d-num" style="font-size:12px;color:${MID};text-align:right;">${(x.lines || []).length}</span>
              <span style="display:flex;gap:6px;flex-wrap:wrap;">
                ${(x.containers || []).length
                  ? x.containers.map(c => `<button class="d2d-btn d2d-num" data-open="${esc(c.shipment_id)}"
                       style="min-height:28px;padding:2px 8px;font-size:10.5px;">${esc(c.reference || 'container')}</button>`).join('')
                  : `<span style="font-size:11px;color:${YINK};">not yet assigned</span>`}
              </span>
            </div>
            ${(x.lines || []).length ? `
              <div style="padding:0 18px 11px 128px;display:flex;flex-wrap:wrap;gap:6px;">
                ${x.lines.map(l => `<span style="font-size:10.5px;color:${MID};background:#F7F8FA;border-radius:6px;padding:3px 8px;">
                    <b class="d2d-num" style="color:${DARK};">${esc(l.sku_code)}</b>
                    ${l.units != null ? ` · ${Number(l.units).toLocaleString()}` : ''}</span>`).join('')}
              </div>` : ''}
          `).join('')}
        </div>`
      : `<div class="rounded-2xl border bg-white shadow-sm" style="padding:22px 24px;">
          <div style="font-size:14px;font-weight:600;color:${DARK};">No orders loaded for this week</div>
          <div style="font-size:12px;color:${MID};line-height:1.6;margin-top:6px;max-width:620px;">
            Upload GRBA&rsquo;s order file and the chain fills in: each PO, the SKUs and units inside it, and which
            container carries it. An order can span several containers and a container carries many orders, so both
            sides stay linked.
          </div>
          <div style="display:flex;align-items:center;gap:8px;margin:16px 0 6px;flex-wrap:wrap;">
            ${['Week', 'Container', 'PO', 'SKU', 'Units'].map((x, i2) => `
              <span style="font-size:11px;font-weight:600;color:${DARK};background:#F2F2F5;border-radius:7px;padding:5px 10px;">${x}</span>
              ${i2 < 4 ? `<span style="color:#D6D6DB;">&rsaquo;</span>` : ''}`).join('')}
          </div>
          <div style="font-size:11px;color:${MID};">Wanted columns: PO number, supplier, cargo-ready date, container, SKU, description, units, CBM, value.
            Headers are matched loosely, so the file does not need renaming.</div>
        </div>`}`;
  }

  function openPoUpload() {
    if (el('d2d-poov')) return;
    const ov = document.createElement('div');
    ov.id = 'd2d-poov';
    ov.style.cssText = 'position:fixed;inset:0;z-index:9700;background:rgba(16,18,27,.28);display:flex;align-items:center;justify-content:center;padding:24px;';
    ov.innerHTML = `
      <div class="rounded-2xl" style="background:#fff;width:min(820px,96vw);max-height:90vh;overflow-y:auto;
           box-shadow:0 40px 80px rgba(16,18,27,.22);">
        <div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:.5px solid rgba(0,0,0,.08);">
          <div>
            <div style="font-size:15px;font-weight:700;color:${DARK};">Upload order file</div>
            <div style="font-size:11px;color:${MID};margin-top:2px;">Week of ${esc(day(_week))} &middot; CSV, one row per SKU line</div>
          </div>
          <button class="d2d-btn" id="d2d-poclose">Close</button>
        </div>
        <div style="padding:18px 20px;">
          <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px;">
            <input type="file" id="d2d-pofile" accept=".csv,text/csv" style="font:inherit;font-size:12px;">
            <span style="font-size:11px;color:${MID};">or paste below</span>
          </div>
          <textarea id="d2d-potext" rows="7" placeholder="PO #,Vendor,Cargo Ready,Container Number,Style,Qty,CBM&#10;40117,D&amp;J Industries,05/10/2026,ONEU7654321,SKU-8891,420,18.4"
            style="width:100%;box-sizing:border-box;font-family:ui-monospace,SFMono-Regular,monospace;font-size:11.5px;
                   border:.5px solid rgba(0,0,0,.18);border-radius:10px;padding:11px;resize:vertical;"></textarea>
          <div style="display:flex;align-items:center;gap:10px;margin-top:12px;">
            <button class="d2d-btn" id="d2d-pocheck">Check the file</button>
            <button class="d2d-btn dark" id="d2d-poapply" disabled style="opacity:.5;">Apply to this week</button>
            <span style="font-size:11px;color:${MID};" id="d2d-pomsg"></span>
          </div>
          <div id="d2d-poresult" style="margin-top:14px;"></div>
        </div>
      </div>`;
    document.body.appendChild(ov);
    el('d2d-poclose').onclick = () => ov.remove();
    ov.addEventListener('click', (e) => { if (e.target === ov) ov.remove(); });
    el('d2d-pofile').onchange = (e) => {
      const f = e.target.files && e.target.files[0]; if (!f) return;
      const rd = new FileReader();
      rd.onload = () => { el('d2d-potext').value = String(rd.result || ''); };
      rd.readAsText(f);
    };
    el('d2d-pocheck').onclick = async () => {
      const csv = el('d2d-potext').value.trim();
      const msg = el('d2d-pomsg'), out = el('d2d-poresult');
      if (!csv) { msg.textContent = 'Nothing to check yet.'; return; }
      msg.style.color = MID; msg.textContent = 'Checking…';
      try {
        const pv = await api('/d2d/po/preview', { method: 'POST', body: JSON.stringify({ csv, week_start: _week }) });
        _poPreview = pv; _poCsv = csv;
        msg.textContent = '';
        const stops = (pv.problems || []).filter(x => x.level === 'stop');
        const warns = (pv.problems || []).filter(x => x.level !== 'stop');
        out.innerHTML = `
          <div class="grid grid-cols-2 lg:grid-cols-4 gap-3" style="margin-bottom:12px;">
            ${[['Rows', pv.rows], ['Orders', pv.orders], ['SKU lines', pv.lines], ['Units', Number(pv.units || 0).toLocaleString()]]
              .map(([l, v]) => `<div class="rounded-2xl border bg-white shadow-sm d2d-tile">
                 <div class="d2d-tl">${l}</div><div class="d2d-tv">${v}</div></div>`).join('')}
          </div>
          ${pv.sample && pv.sample.length ? `
            <div style="border:.5px solid rgba(0,0,0,.08);border-radius:11px;overflow:hidden;margin-bottom:12px;">
              <div style="display:grid;grid-template-columns:90px 1fr 84px 70px 90px;gap:0 10px;padding:8px 14px;background:#FBFBFC;
                   font-size:9.5px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">
                <span>PO</span><span>Supplier</span><span style="text-align:right;">Units</span>
                <span style="text-align:right;">SKUs</span><span style="text-align:right;">Containers</span></div>
              ${pv.sample.map(x => `<div style="display:grid;grid-template-columns:90px 1fr 84px 70px 90px;gap:0 10px;
                   padding:8px 14px;border-top:.5px solid rgba(0,0,0,.05);font-size:11.5px;color:${DARK};">
                <span class="d2d-num">${esc(x.po_number)}</span><span>${esc(x.supplier || '—')}</span>
                <span class="d2d-num" style="text-align:right;">${Number(x.units || 0).toLocaleString()}</span>
                <span class="d2d-num" style="text-align:right;">${x.lines}</span>
                <span class="d2d-num" style="text-align:right;color:${x.containers ? DARK : YINK};">${x.containers || 'none'}</span></div>`).join('')}
            </div>` : ''}
          ${stops.length ? `<div style="border-left:3px solid ${BRAND};padding:10px 13px;background:rgba(153,0,51,.06);
               border-radius:9px;margin-bottom:9px;">
               ${stops.map(x => `<div style="font-size:12px;color:${DARK};">${esc(x.message)}</div>`).join('')}</div>` : ''}
          ${warns.length ? `<div style="border-left:3px solid ${YELL};padding:10px 13px;background:rgba(254,208,0,.10);border-radius:9px;">
               <div style="font-size:11px;font-weight:700;color:${YINK};text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">Worth knowing</div>
               ${warns.map(x => `<div style="font-size:11.5px;color:${DARK};line-height:1.5;">${esc(x.message)}</div>`).join('')}</div>` : ''}
          <div style="font-size:11px;color:${MID};margin-top:10px;">
            Applying replaces any order with the same number in this week, so sending the file twice is safe.</div>`;
        const ap = el('d2d-poapply');
        ap.disabled = !pv.ok; ap.style.opacity = pv.ok ? '' : '.5';
      } catch (e2) { msg.style.color = BRAND; msg.textContent = 'Could not check: ' + e2.message; }
    };
    el('d2d-poapply').onclick = async () => {
      const b = el('d2d-poapply'), msg = el('d2d-pomsg');
      b.disabled = true; b.style.opacity = '.5'; msg.style.color = MID; msg.textContent = 'Applying…';
      try {
        const out = await api('/d2d/po/apply', { method: 'POST', body: JSON.stringify({ csv: _poCsv, week_start: _week, confirm: '1' }) });
        ov.remove();
        await load();
        console.warn('[d2d-hub] orders applied', out);
      } catch (e2) { b.disabled = false; b.style.opacity = ''; msg.style.color = BRAND; msg.textContent = 'Could not apply: ' + e2.message; }
    };
  }

  // ── Performance ──
  // Mockup 5. The shapes are here and honest about their inputs: distribution needs a few
  // months of completed shipments, and anything costed needs the rates and the order file.
  function paintPerformance(d) {
    const all = (d.allShipments || []).slice();
    const done = all.filter(x => x.status === 'delivered');
    const durations = done.map(x => {
      const ev = evMap(x);
      return daysBetween(ev.pickup && ev.pickup.actual_at, ev.delivered && ev.delivered.actual_at);
    }).filter(v => v != null && v > 0).sort((a, b) => a - b);
    const median = durations.length ? durations[Math.floor(durations.length / 2)] : null;
    const p90 = durations.length ? durations[Math.min(durations.length - 1, Math.floor(durations.length * 0.9))] : null;

    const slipAll = all.map(slipOf).filter(v => v != null && v > 0);
    const onPlan = all.length ? Math.round((all.length - slipAll.length) / all.length * 100) : null;

    // Where lost days actually come from, from the dates we hold.
    const byStage = {};
    for (const sh of all) {
      const ev = evMap(sh);
      for (const [kk, label] of STAGES) {
        const dd = daysBetween(sh['plan_' + kk], ev[kk] && ev[kk].actual_at);
        if (dd != null && dd > 0) byStage[label] = (byStage[label] || 0) + dd;
      }
    }
    const stages = Object.entries(byStage).sort((a, b) => b[1] - a[1]);
    const maxStage = Math.max(1, ...stages.map(x => x[1]));

    const tile = (label, value, sub, colour) => `
      <div class="rounded-2xl border bg-white shadow-sm d2d-tile">
        <div class="d2d-tl">${label}</div>
        <div class="d2d-tv" style="color:${colour || DARK};">${value}</div>
        <div class="d2d-ts">${sub}</div>
      </div>`;

    return `
      <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:14px;margin-bottom:12px;flex-wrap:wrap;">
        <div>
          <div style="font-size:16px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Performance</div>
          <div style="font-size:11.5px;color:${MID};margin-top:2px;">Every measure from recorded dates. Costed figures wait on the rates and the order file.</div>
        </div>
        <span style="font-size:11px;color:${MID};">${all.length} shipment${all.length === 1 ? '' : 's'} &middot; ${done.length} completed</span>
      </div>

      <div class="grid grid-cols-2 lg:grid-cols-4 gap-3" style="margin-bottom:12px;">
        ${tile('Door to door', median != null ? median + 'd' : '&ndash;',
               p90 != null ? '90% within ' + p90 + 'd' : 'needs completed shipments')}
        ${tile('On plan', onPlan != null ? onPlan + '%' : '&ndash;',
               slipAll.length ? slipAll.length + ' behind plan' : 'all on plan', onPlan != null && onPlan < 80 ? BRAND : DARK)}
        ${tile('Days lost', stages.reduce((n, x) => n + x[1], 0) || 0, stages.length ? 'across ' + stages.length + ' stages' : 'none recorded', stages.length ? BRAND : DARK)}
        ${tile('Container utilisation', '&ndash;', 'needs CBM per PO', LIGHT)}
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-3 items-start">
        <div class="rounded-2xl border bg-white shadow-sm" style="padding:16px 18px;">
          <div style="font-size:13px;font-weight:600;color:${DARK};">Where the days are lost</div>
          <div style="font-size:11px;color:${MID};margin-bottom:12px;">every stage that has run past its plan</div>
          ${stages.length ? stages.map(([label, days]) => `
            <div style="display:flex;align-items:center;gap:10px;padding:6px 0;">
              <span style="font-size:11.5px;color:${DARK};width:118px;flex-shrink:0;">${esc(label)}</span>
              <span style="flex:1;height:10px;background:#F0F0F3;border-radius:5px;overflow:hidden;">
                <span style="display:block;height:10px;width:${Math.round(days / maxStage * 100)}%;background:${BRAND};border-radius:5px;"></span></span>
              <span class="d2d-num" style="font-size:11.5px;color:${BRAND};width:44px;text-align:right;">${days}d</span>
            </div>`).join('')
            : `<div style="font-size:11.5px;color:${LINK};">Nothing has run late yet.</div>`}
          <div style="font-size:11px;color:${MID};margin-top:10px;padding-top:9px;border-top:.5px solid rgba(0,0,0,.06);line-height:1.45;">
            This is the one performance figure that is real today, because it comes from the dates being recorded.
          </div>
        </div>

        <div style="display:flex;flex-direction:column;gap:12px;">
          ${pending('Transit distribution by lane',
            'The spread matters more than the average, and a distribution needs a few months of completed shipments. With ' + done.length + ' so far, any curve would be noise.', 150)}
          ${pending('Container utilisation and cost of empty space',
            'Needs CBM and weight per PO from the order file, plus your contracted rate per container.', 150)}
          ${pending('Cash in transit and landed cost per unit',
            'Needs the commercial invoice value per PO. Until then these are the CFO figures we cannot honestly show.', 150)}
        </div>
      </div>`;
  }

  // ── Bookings: what the client decides ──
  // From and to, on anything that represents a booked movement. It reads off the option first
  // and falls back to the request it belongs to.
  function laneLine(b) {
    const from = b.origin || b.service || (b.request && b.request.origin);
    const to = b.destination || (b.request && b.request.destination);
    if (!from && !to) return '';
    return `<div style="font-size:10.5px;color:${MID};margin-top:3px;">
      <span style="font-size:8.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">From</span>
      <b style="color:${DARK};font-weight:600;">${esc(from || '—')}</b>
      <span style="color:${LIGHT};">&rarr;</span>
      <span style="font-size:8.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">To</span>
      <b style="color:${DARK};font-weight:600;">${esc(to || '—')}</b>
    </div>`;
  }

  function paintBookings(d) {
    const header = `
      <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:14px;margin-bottom:12px;flex-wrap:wrap;">
        <div>
          <div style="font-size:14px;font-weight:600;color:${DARK};">Week of ${esc(day(_week))}</div>
          <div style="font-size:11px;color:${MID};margin-top:2px;">${_internal
            ? 'Options quoted for this week. The client approves one, and the plan freezes.'
            : 'Choose the sailing that suits you. Approving freezes the plan.'}</div>
        </div>
        ${_internal ? `<button class="d2d-btn dark" data-newbooking="1">New booking</button>` : ''}
      </div>`;
    const rq = (d.requests || [])[0];
    const cargo = rq ? `
      <div class="rounded-2xl border bg-white shadow-sm" style="padding:13px 16px;margin-bottom:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
          <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;">
            <span class="d2d-num" style="font-size:12px;font-weight:600;color:${DARK};">${esc(rq.ref || 'request')}</span>
            <span style="font-size:11px;color:${MID};">${esc([rq.origin, rq.destination].filter(Boolean).join(' → ')) || 'origin not stated'}</span>
            <span style="font-size:10.5px;font-weight:700;border-radius:6px;padding:2px 8px;color:${MID};background:#F2F2F5;">${esc(rq.state)}</span>
          </div>
          <div style="display:flex;gap:16px;flex-wrap:wrap;">
            ${[['Packed as', rq.pack_type || '—'], ['Pallets', rq.pallets], ['Cartons', rq.cartons],
               ['Units', rq.units], ['CBM', rq.cbm], ['Gross kg', rq.gross_weight_kg]]
              .filter(([, v]) => v != null && v !== '')
              .map(([l, v]) => `<span style="text-align:right;">
                 <span style="display:block;font-size:9.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">${l}</span>
                 <span class="d2d-num" style="display:block;font-size:13px;font-weight:600;color:${DARK};">${
                   typeof v === 'number' ? v.toLocaleString() : esc(v)}</span></span>`).join('')}
          </div>
        </div>
        ${rq.notes ? `<div style="font-size:11px;color:${MID};margin-top:8px;">${esc(rq.notes)}</div>` : ''}
        ${_internal ? `
          <div style="display:flex;align-items:center;gap:8px;margin-top:11px;padding-top:11px;
               border-top:.5px solid rgba(0,0,0,.05);flex-wrap:wrap;">
            <button class="d2d-btn" data-rfq="${esc(rq.id)}" style="min-height:34px;padding:5px 11px;font-size:11.5px;">
              ${rq.state === 'sent' || rq.state === 'costed' || rq.state === 'repricing' ? 'Resend to partner' : 'Send to partner'}</button>
            ${(d.bookings || []).some(b => b.status === 'draft')
              ? `<button class="d2d-btn" data-review="${esc(rq.id)}" style="min-height:34px;padding:5px 11px;font-size:11.5px;">Ask for a review</button>` : ''}
            <span style="font-size:11px;color:${_rfqNote ? LINK : MID};" id="d2d-rfqmsg">${
              _rfqNote ? esc(_rfqNote) : (rq.sent_at ? 'Sent ' + esc(day(String(rq.sent_at).slice(0, 10))) : '')}</span>
          </div>` : ''}
      </div>` : '';

    if (!d.bookings.length) return header + cargo + `
      <div class="rounded-2xl border bg-white shadow-sm" style="padding:22px 24px;">
        <div style="font-size:14px;font-weight:600;color:${DARK};">Nothing quoted for this week</div>
        <div style="font-size:12px;color:${MID};line-height:1.6;margin-top:6px;max-width:600px;">
          ${_internal
            ? 'Start a booking with the rates the partner quoted. Enter cost only — margin is set on Pricing, and nothing reaches the client until you release it.'
            : 'Options will appear here once they have been quoted.'}
        </div>
      </div>`;
    const order = { released: 0, approved: 1, draft: 2, declined: 3, expired: 4 };
    const rows = [...d.bookings].sort((a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9));
    return header + cargo + `<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;">
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
              ${laneLine(b)}
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

  // ── Starting a booking ──
  // Options come in from the partner as rates; this is where they are entered. Cost only —
  // the margin and the sell price are set on Pricing, so the two jobs stay separate.
  const nextMonday = () => {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() + ((8 - d.getUTCDay()) % 7 || 7));
    return d.toISOString().slice(0, 10);
  };

  function optionRow(n) {
    return `
      <div class="d2d-optrow" data-optrow="${n}" style="border:.5px solid rgba(0,0,0,.10);border-radius:12px;padding:13px 15px;margin-bottom:10px;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px;">
          <span style="font-size:12px;font-weight:600;color:${DARK};">Option ${String.fromCharCode(65 + n)}</span>
          <label style="display:flex;align-items:center;gap:6px;font-size:11px;color:${MID};cursor:pointer;">
            <input type="radio" name="d2d-rec" value="${n}" ${n === 0 ? 'checked' : ''}> recommend this one</label>
        </div>
        <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;">
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Description
            <input class="d2d-in2" data-f="title" placeholder="2 x 40HQ, direct"></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Carrier
            <input class="d2d-in2" data-f="carrier" placeholder="ONE"></label>
        </div>
        <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-top:10px;">
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Box type
            <select class="d2d-in2" data-f="container_type">
              <option value="40HQ">40HQ</option><option value="40GP">40GP</option>
              <option value="20GP">20GP</option><option value="">n/a (air)</option></select></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">How many
            <input class="d2d-in2 d2d-num" data-f="container_qty" type="number" min="1" max="20" value="1"></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Transit days
            <input class="d2d-in2 d2d-num" data-f="transit_days" type="number" min="1" max="120" placeholder="26"></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Via
            <select class="d2d-in2" data-f="transhipment">
              <option value="0">direct</option><option value="1">transhipment</option></select></label>
        </div>
        <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px;">
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Partner cost
            <input class="d2d-in2 d2d-num" data-f="cost_amount" type="number" min="0" step="0.01" placeholder="8000"></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Origin + destination
            <input class="d2d-in2 d2d-num" data-f="accessorial_amount" type="number" min="0" step="0.01" placeholder="900"></label>
          <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Currency
            <select class="d2d-in2" data-f="currency"><option>USD</option><option>AUD</option><option>CNY</option></select></label>
        </div>
      </div>`;
  }

  function openNewBooking() {
    if (el('d2d-nbov')) return;
    const ov = document.createElement('div');
    ov.id = 'd2d-nbov';
    ov.style.cssText = 'position:fixed;inset:0;z-index:9700;background:rgba(16,18,27,.28);display:flex;align-items:center;justify-content:center;padding:24px;';
    ov.innerHTML = `
      <div class="rounded-2xl" style="background:#fff;width:min(860px,96vw);max-height:90vh;overflow-y:auto;
           box-shadow:0 40px 80px rgba(16,18,27,.22);">
        <div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;
             border-bottom:.5px solid rgba(0,0,0,.08);position:sticky;top:0;background:#fff;">
          <div>
            <div style="font-size:15px;font-weight:700;color:${DARK};">New booking</div>
            <div style="font-size:11px;color:${MID};margin-top:2px;">Enter the rates the partner quoted. Margin is set afterwards, on Pricing.</div>
          </div>
          <button class="d2d-btn" id="d2d-nbclose">Close</button>
        </div>

        <div style="padding:18px 20px;">
          <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:14px;">
            <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Cargo ready week
              <input class="d2d-in2" id="d2d-nbweek" type="date" value="${esc(nextMonday())}"></label>
            <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Mode
              <select class="d2d-in2" id="d2d-nbmode">
                <option value="sea">Sea</option><option value="air">Air</option><option value="both">Sea and air</option></select></label>
            <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Origin
              <input class="d2d-in2" id="d2d-nborigin" placeholder="Ningbo"></label>
            <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Destination
              <input class="d2d-in2" id="d2d-nbdest" placeholder="Port Botany"></label>
          </div>

          ${/* The cargo, stated once. A partner cannot quote a sailing without it, and stating
                it on the header stops the same figures being retyped per option. */ ''}
          <div style="border:.5px solid rgba(0,0,0,.10);border-radius:12px;padding:13px 15px;margin-bottom:16px;background:#FBFBFC;">
            <div style="display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
              <span style="font-size:12px;font-weight:600;color:${DARK};">What is shipping</span>
              <span style="display:flex;align-items:center;gap:10px;">
                <span style="font-size:11px;color:${MID};" id="d2d-nbprefillnote"></span>
                <button class="d2d-btn" id="d2d-nbprefill" style="min-height:32px;padding:4px 10px;font-size:11px;display:none;">Use the order file</button>
              </span>
            </div>
            <div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px;">
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Packed as
                <select class="d2d-in2" id="d2d-nbpack">
                  <option value="">—</option><option value="loose">Loose cartons</option>
                  <option value="pallets">Pallets</option><option value="mixed">Mixed</option></select></label>
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Pallets
                <input class="d2d-in2 d2d-num" id="d2d-nbpallets" type="number" min="0" placeholder="18"></label>
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Cartons
                <input class="d2d-in2 d2d-num" id="d2d-nbcartons" type="number" min="0" placeholder="940"></label>
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Units
                <input class="d2d-in2 d2d-num" id="d2d-nbunits" type="number" min="0" placeholder="11400"></label>
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">CBM
                <input class="d2d-in2 d2d-num" id="d2d-nbcbm" type="number" min="0" step="0.01" placeholder="62.5"></label>
              <label style="font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;">Gross kg
                <input class="d2d-in2 d2d-num" id="d2d-nbkg" type="number" min="0" step="0.1" placeholder="8400"></label>
            </div>
            <label style="display:block;font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;margin-top:10px;">Notes for the partner
              <input class="d2d-in2" id="d2d-nbnotes" placeholder="Two suppliers, one collection"></label>
          </div>

          <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:8px;">
            <span style="font-size:12.5px;font-weight:600;color:${DARK};">Options quoted</span>
            <span style="font-size:11px;color:${MID};">the client will choose one</span>
          </div>
          <div id="d2d-nbopts">${optionRow(0)}${optionRow(1)}</div>
          <button class="d2d-btn" id="d2d-nbadd" style="min-height:36px;padding:6px 12px;font-size:11.5px;">Add another option</button>

          <div style="display:flex;align-items:center;gap:12px;margin-top:16px;padding-top:14px;border-top:.5px solid rgba(0,0,0,.07);">
            <button class="d2d-btn dark" id="d2d-nbsave">Create and price &rarr;</button>
            <span style="font-size:11px;color:${MID};" id="d2d-nbmsg">Options are created as drafts. Nothing reaches the client until you release them.</span>
          </div>
        </div>
      </div>`;
    document.body.appendChild(ov);
    el('d2d-nbclose').onclick = () => ov.remove();
    ov.addEventListener('click', (e) => { if (e.target === ov) ov.remove(); });

    // If the orders for that week are already loaded, offer their figures rather than asking
    // for numbers the system can already add up.
    let _prefill = null;
    const checkPrefill = async () => {
      const wk = el('d2d-nbweek').value;
      const note = el('d2d-nbprefillnote'), btn = el('d2d-nbprefill');
      if (!note) return;
      note.textContent = ''; btn.style.display = 'none'; _prefill = null;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(wk)) return;
      try {
        const r = await api('/d2d/requests/prefill?week=' + encodeURIComponent(wk));
        if (r && r.from_orders && r.from_orders.orders) {
          _prefill = r.from_orders;
          note.textContent = `${_prefill.orders} orders loaded for this week`;
          btn.style.display = '';
        }
      } catch (e2) { /* the offer is a convenience, not a requirement */ }
    };
    el('d2d-nbweek').onchange = checkPrefill;
    checkPrefill();
    el('d2d-nbprefill').onclick = () => {
      if (!_prefill) return;
      if (_prefill.units != null) el('d2d-nbunits').value = Math.round(_prefill.units);
      if (_prefill.cbm != null) el('d2d-nbcbm').value = Number(_prefill.cbm).toFixed(2);
      if (_prefill.gross_weight_kg != null) el('d2d-nbkg').value = Number(_prefill.gross_weight_kg).toFixed(1);
      el('d2d-nbprefillnote').textContent = 'taken from the order file';
    };

    let count = 2;
    el('d2d-nbadd').onclick = () => {
      if (count >= 4) return;
      el('d2d-nbopts').insertAdjacentHTML('beforeend', optionRow(count));
      count++;
      if (count >= 4) { el('d2d-nbadd').disabled = true; el('d2d-nbadd').style.opacity = '.5'; }
    };

    el('d2d-nbsave').onclick = async () => {
      const msg = el('d2d-nbmsg'), btn = el('d2d-nbsave');
      const week = el('d2d-nbweek').value;
      const mode = el('d2d-nbmode').value;
      const origin = el('d2d-nborigin').value.trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(week)) { msg.style.color = BRAND; msg.textContent = 'Pick a cargo-ready week.'; return; }

      const options = [];
      ov.querySelectorAll('[data-optrow]').forEach((row, i) => {
        const get = (f) => { const n2 = row.querySelector(`[data-f="${f}"]`); return n2 ? n2.value.trim() : ''; };
        const cost = Number(get('cost_amount'));
        // A row with no cost is an empty row, not a zero-cost option.
        if (!get('title') && !isFinite(cost)) return;
        if (!isFinite(cost) || cost <= 0) return;
        const rec = ov.querySelector('input[name="d2d-rec"]:checked');
        options.push({
          option_ref: String.fromCharCode(65 + i),
          title: get('title') || (get('container_qty') + ' x ' + get('container_type')),
          mode,
          container_type: mode === 'air' ? null : (get('container_type') || null),
          container_qty: Number(get('container_qty')) || 1,
          carrier: get('carrier') || null,
          service: origin || null,
          transhipment: get('transhipment') === '1',
          transit_days: Number(get('transit_days')) || null,
          cost_amount: cost,
          accessorial_amount: Number(get('accessorial_amount')) || 0,
          currency: get('currency') || 'USD',
          margin_pct: 18,
          recommended: rec ? Number(rec.value) === i : i === 0,
        });
      });
      if (!options.length) { msg.style.color = BRAND; msg.textContent = 'Add at least one option with a cost.'; return; }

      btn.disabled = true; btn.style.opacity = '.5';
      msg.style.color = MID; msg.textContent = 'Creating…';
      const numOr = (id2) => { const v = el(id2).value.trim(); return v === '' ? null : Number(v); };
      try {
        // The header first: the options answer it, and it is what the partner will be asked to quote.
        const rq = await api('/d2d/requests', { method: 'POST', body: JSON.stringify({
          week_start: week, mode, origin: origin || null,
          destination: el('d2d-nbdest').value.trim() || null,
          pack_type: el('d2d-nbpack').value || null,
          pallets: numOr('d2d-nbpallets'), cartons: numOr('d2d-nbcartons'), units: numOr('d2d-nbunits'),
          cbm: numOr('d2d-nbcbm'), gross_weight_kg: numOr('d2d-nbkg'),
          notes: el('d2d-nbnotes').value.trim() || null,
        }) });
        await api('/d2d/bookings', { method: 'POST', body: JSON.stringify({ week_start: week, request_id: rq.id, options }) });
        ov.remove();
        _week = week; _tab = 'pricing'; _view = 'dashboard';
        await load();                       // lands on Pricing for the week just created
      } catch (e2) {
        btn.disabled = false; btn.style.opacity = '';
        msg.style.color = BRAND; msg.textContent = 'Could not create: ' + e2.message;
      }
    };
    setTimeout(() => el('d2d-nbweek').focus(), 40);
  }

  // ── Pricing: VelOzity only ──
  function paintPricing(d) {
    const rows = d.bookings.filter(b => ['draft', 'released', 'approved'].includes(b.status));
    if (!rows.length) return `
      <div class="rounded-2xl border bg-white shadow-sm" style="padding:22px 24px;">
        <div style="font-size:14px;font-weight:600;color:${DARK};">Nothing to price for this week</div>
        <div style="font-size:12px;color:${MID};margin:6px 0 14px;">Enter the rates the partner quoted and they will appear here.</div>
        <button class="d2d-btn dark" data-newbooking="1">New booking</button>
      </div>`;
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
              <span style="display:block;font-size:10.5px;color:${MID};">${esc(b.carrier || '')}</span>
              ${laneLine(b)}</span>
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

  // Truly full screen: the map fills the viewport with the controls floating over it. The
  // first version simply re-rendered the page card inside an overlay, which gave a small map
  // in a large empty page — the opposite of what full screen is for.
  // Full screen uses the SAME renderer as the panel — one map, drawn bigger. Keeping a second
  // implementation for full screen is how the two ended up looking different.
  function openFullMap() {
    if (el('d2d-fullmap')) return;
    const d = _data; if (!d) return;
    const source = mapFiltered(mapSource(d));
    const moving = source.filter(x => { const t = markT(x); return t > 0 && t < 1; }).length;

    const ov = document.createElement('div');
    ov.id = 'd2d-fullmap';
    ov.style.cssText = `position:fixed;inset:0;z-index:9600;background:${MAP_PAL.sea_bg};display:flex;flex-direction:column;`;
    ov.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;
           padding:12px 20px;background:#fff;border-bottom:.5px solid rgba(0,0,0,.08);">
        <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
          <span style="font-size:14px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Live tracking</span>
          <span style="font-size:11.5px;color:${MID};">${moving} moving &middot; ${source.length} shown</span>
          <span style="display:flex;gap:6px;">
            ${[['live', 'All in flight'], ['week', 'This week']].map(([k2, l]) =>
              `<button class="d2d-filt ${_mapScope === k2 ? 'on' : ''}" data-scope="${k2}">${l}</button>`).join('')}
          </span>
          <span style="display:flex;gap:6px;">
            ${[['all', 'All'], ['sea', 'Sea'], ['air', 'Air'], ['late', 'Late only']].map(([k2, l]) =>
              `<button class="d2d-filt ${_mapFilter === k2 ? 'on' : ''}" data-filt="${k2}">${l}</button>`).join('')}
          </span>
        </div>
        <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
          ${[['Origin port', MAP_PAL.origin_port], ['Destination', MAP_PAL.clearing],
             ['In transit', MAP_PAL.transit], ['Air', MAP_PAL.air],
             ['Customs', MAP_PAL.customs]].map(([l, c]) =>
            `<span style="font-size:11px;color:${MID};"><span style="display:inline-block;width:9px;height:9px;
               border-radius:50%;background:${c};margin-right:5px;"></span>${l}</span>`).join('')}
          <button class="d2d-btn" id="d2d-fullclose" aria-label="Close full screen">Close</button>
        </div>
      </div>

      <div class="d2d-mapbox" style="position:relative;flex:1;min-height:0;background:${MAP_PAL.sea_bg};">
        <canvas class="d2d-mapcanvas" style="position:absolute;inset:0;width:100%;height:100%;display:block;"></canvas>
        <svg class="d2d-mapsvg" style="position:absolute;inset:0;width:100%;height:100%;display:block;"
             role="img" aria-label="Live tracking, full screen"></svg>
        <div class="d2d-maploading" style="position:absolute;inset:0;display:flex;align-items:center;
             justify-content:center;font-size:12px;color:${MID};">Drawing the map&hellip;</div>

        <div style="position:absolute;right:20px;top:20px;width:250px;background:rgba(255,255,255,.96);
             border:.5px solid rgba(0,0,0,.08);border-radius:12px;padding:10px 12px;
             box-shadow:0 12px 30px rgba(16,18,27,.10);max-height:calc(100vh - 140px);overflow-y:auto;">
          <div style="font-size:11px;font-weight:600;color:${DARK};margin-bottom:6px;">In flight</div>
          ${source.filter(x => { const t = markT(x); return t > 0 && t < 1; }).map(sh => {
            const look = outlook(sh);
            return `<button type="button" data-open="${esc(sh.id)}" style="display:flex;align-items:center;gap:8px;
                    width:100%;text-align:left;background:none;border:0;padding:7px 2px;cursor:pointer;
                    border-top:.5px solid rgba(0,0,0,.05);">
              <span style="width:7px;height:7px;border-radius:50%;flex-shrink:0;
                    background:${sh.mode === 'air' ? MAP_PAL.air : MAP_PAL.transit};"></span>
              <span style="flex:1;min-width:0;">
                <span class="d2d-num" style="display:block;font-size:11.5px;color:${DARK};overflow:hidden;
                      text-overflow:ellipsis;white-space:nowrap;">${esc(sh.reference || 'not advised')}</span>
                <span style="display:block;font-size:10px;color:${MID};">${esc(sh.mode === 'air' ? 'air' : 'sea')} &middot; ${esc(sh.service || sh.origin || '')}</span>
              </span>
              <span style="font-size:10px;color:${look.ink};flex-shrink:0;">${esc(look.label)}</span>
            </button>`;
          }).join('') || `<div style="font-size:11px;color:${MID};">Nothing in transit.</div>`}
        </div>

        <div style="position:absolute;left:20px;bottom:18px;background:rgba(255,255,255,.92);
             border:.5px solid rgba(0,0,0,.08);border-radius:10px;padding:8px 12px;max-width:380px;">
          <span style="font-size:11px;color:${MID};line-height:1.45;display:block;">
            Positions follow the last recorded milestone. Click a vessel for what is aboard.
            Exact positions need carrier tracking &mdash; <b style="color:${DARK};">not yet connected</b>.</span>
        </div>
      </div>`;
    document.body.appendChild(ov);
    document.body.style.overflow = 'hidden';

    const shut = () => { ov.remove(); document.body.style.overflow = ''; };
    el('d2d-fullclose').onclick = shut;
    ov.querySelectorAll('[data-filt]').forEach(b => b.onclick = () => { _mapFilter = b.getAttribute('data-filt'); shut(); openFullMap(); });
    ov.querySelectorAll('[data-scope]').forEach(b => b.onclick = () => { _mapScope = b.getAttribute('data-scope'); shut(); openFullMap(); });
    ov.querySelectorAll('[data-open]').forEach(b => b.addEventListener('click', () => {
      shut();
      if (_tab !== 'shipments') _tab = 'shipments';
      go('container', b.getAttribute('data-open'));
    }));
    const onKey = (e) => { if (e.key === 'Escape') { shut(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);

    setTimeout(() => { mountMaps(d).catch(e => console.warn('[d2d-hub] full map', e)); }, 30);
  }

  // What is in a cluster, as a short list rather than three labels fighting for the same spot.
  function openCluster(ids, anchor) {
    closeEditor();
    const rect = anchor.getBoundingClientRect();
    const pop = document.createElement('div');
    pop.id = 'd2d-pop';
    pop.style.cssText = `position:fixed;z-index:9800;background:#fff;border:.5px solid rgba(0,0,0,.14);border-radius:12px;
      box-shadow:0 18px 40px rgba(16,18,27,.18);padding:10px;width:250px;font-family:inherit;`;
    pop.innerHTML = `
      <div style="font-size:11px;color:${MID};padding:2px 6px 8px;">${ids.length} shipments here</div>
      ${ids.map(id => {
        const sh = findShip(id); if (!sh) return '';
        const look = outlook(sh);
        return `<button type="button" data-open="${esc(id)}" style="display:flex;align-items:center;gap:9px;width:100%;
                text-align:left;background:none;border:0;padding:8px 6px;cursor:pointer;border-radius:8px;
                border-top:.5px solid rgba(0,0,0,.05);">
          <span style="width:7px;height:7px;border-radius:50%;background:${look.ink};flex-shrink:0;"></span>
          <span style="flex:1;min-width:0;">
            <span class="d2d-num" style="display:block;font-size:12px;color:${DARK};">${esc(sh.reference || 'not advised')}</span>
            <span style="display:block;font-size:10.5px;color:${MID};">${esc([sh.container_type || (sh.mode === 'air' ? 'air' : ''), sh.carrier].filter(Boolean).join(' · '))}</span>
          </span>
          <span style="font-size:10.5px;color:${look.ink};">${esc(look.label)}</span>
        </button>`;
      }).join('')}`;
    document.body.appendChild(pop);
    pop.style.top = Math.max(12, Math.min(rect.bottom + 8, window.innerHeight - pop.offsetHeight - 12)) + 'px';
    pop.style.left = Math.max(12, Math.min(rect.left, window.innerWidth - 262)) + 'px';
    pop.querySelectorAll('[data-open]').forEach(b => b.onclick = () => {
      closeEditor();
      const fm = el('d2d-fullmap'); if (fm) { fm.remove(); document.body.style.overflow = ''; }
      if (_tab !== 'shipments') _tab = 'shipments';
      go('container', b.getAttribute('data-open'));
    });
  }

  // Used by the container screen and by the map's cluster list.
  function findShip(id) {
    const all = (_data && _data.allShipments) || [];
    return ((_data && _data.shipments) || []).find(x => x.id === id) || all.find(x => x.id === id);
  }

  // ── Recording a milestone ──
  // A small popover anchored to the stage. Deliberately not a modal: the strip behind it is
  // the context, and hiding it to ask for one date would be a worse trade.
  function closeEditor() { const e = el('d2d-pop'); if (e) e.remove(); }

  function openEditor(btn) {
    // 9800 sits above the detail drawer (9700) and the full-screen map (9600). At 9600 the
    // date picker opened BEHIND the drawer that launched it and looked like a dead button.
    closeEditor();
    const ship = btn.getAttribute('data-ship'), stage = btn.getAttribute('data-stage'), label = btn.getAttribute('data-label');
    const card = _data.shipments.find(x => x.id === ship) || {};
    const existing = (card.events || []).find(e => e.stage === stage);
    const plan = card['plan_' + stage] || '';
    const r = btn.getBoundingClientRect();

    const pop = document.createElement('div');
    pop.id = 'd2d-pop';
    pop.style.cssText = `position:fixed;z-index:9800;background:#fff;border:.5px solid rgba(0,0,0,.14);border-radius:12px;
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
    pop.style.cssText = `position:fixed;z-index:9800;background:#fff;border:.5px solid rgba(0,0,0,.14);border-radius:12px;
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
    root.querySelectorAll('[data-cluster]').forEach(b => {
      const ids = (b.getAttribute('data-cluster') || '').split(',').filter(Boolean);
      if (ids.length < 2) return;                    // a single shipment already opens directly
      b.onclick = (e) => { e.stopPropagation(); openCluster(ids, b); };
    });
    root.querySelectorAll('[data-open]').forEach(b => b.onclick = () => {
      const id = b.getAttribute('data-open');
      const fm = el('d2d-fullmap'); if (fm) { fm.remove(); document.body.style.overflow = ''; }
      go('container', id);
    });
    root.querySelectorAll('[data-go]').forEach(b => b.onclick = () => go(b.getAttribute('data-go'), b.getAttribute('data-goid')));
    root.querySelectorAll('[data-tabgo]').forEach(b => b.onclick = () => { _tab = b.getAttribute('data-tabgo'); paint(); });
    root.querySelectorAll('[data-filt]').forEach(b => b.onclick = () => { _mapFilter = b.getAttribute('data-filt'); paint(); });
    root.querySelectorAll('[data-scope]').forEach(b => b.onclick = () => { _mapScope = b.getAttribute('data-scope'); paint(); });
    root.querySelectorAll('[data-main]').forEach(b => b.onclick = () => { _main = b.getAttribute('data-main'); paint(); });
    const rfq = root.querySelector('[data-rfq]');
    if (rfq) rfq.onclick = async () => {
      const msg = el('d2d-rfqmsg');
      rfq.disabled = true; rfq.style.opacity = '.5';
      if (msg) { msg.style.color = MID; msg.textContent = 'Sending…'; }
      try {
        const out = await api('/d2d/requests/' + rfq.getAttribute('data-rfq') + '/send', { method: 'POST', body: JSON.stringify({}) });
        // The link is shown either way, so it can be pasted into an email if mail is not configured.
        _rfqNote = (out.mail && out.mail.to && out.mail.to.length)
          ? 'Sent to ' + out.mail.to.join(', ')
          : 'Link ready — mail is not configured, copy it from the console';
        if (!(out.mail && out.mail.to && out.mail.to.length)) console.warn('[d2d-hub] partner link:', out.link);
        await load();
      } catch (e) {
        rfq.disabled = false; rfq.style.opacity = '';
        if (msg) { msg.style.color = BRAND; msg.textContent = e.message; }
      }
    };
    const rev = root.querySelector('[data-review]');
    if (rev) rev.onclick = async () => {
      const note = window.prompt('What should the partner look at again?');
      if (!note || !note.trim()) return;
      const msg = el('d2d-rfqmsg');
      try {
        await api('/d2d/requests/' + rev.getAttribute('data-review') + '/review', { method: 'POST', body: JSON.stringify({ note: note.trim() }) });
        _rfqNote = 'Sent back for review';
        await load();
      } catch (e) { if (msg) { msg.style.color = BRAND; msg.textContent = e.message; } }
    };
    const nb = root.querySelector('[data-newbooking]');
    if (nb) nb.onclick = () => openNewBooking();
    const tpl = root.querySelector('[data-potemplate]');
    if (tpl) tpl.onclick = () => {
      // A filled example, not an empty header row: the sample answers the questions a header
      // alone raises — one row per SKU, PO fields repeated, d/m/Y dates.
      const rows = [
        'PO Number,Supplier,Cargo Ready,Container,SKU,Description,Units,CBM,Weight,Value,Currency',
        '40117,D&J Industries,05/10/2026,ONEU7654321,SKU-8891,Ribbed tank black,420,18.4,2480,21500,USD',
        '40117,D&J Industries,05/10/2026,ONEU7654321,SKU-8892,Ribbed tank white,380,18.4,2480,21500,USD',
        '40118,NIR Accessories,05/10/2026,,SKU-5510,Canvas tote,610,11.2,1310,9800,USD',
      ].join('\n');
      const blob = new Blob([rows], { type: 'text/csv;charset=utf-8;' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'order-file-format.csv';
      document.body.appendChild(a); a.click();
      setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 0);
    };
    const up = root.querySelector('[data-poupload]');
    if (up) up.onclick = () => openPoUpload();
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
  console.log('[d2d-hub] v31 loaded');
})();
