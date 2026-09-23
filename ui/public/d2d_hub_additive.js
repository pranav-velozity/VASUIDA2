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

  let _enabled = null, _capClient = null, _week = null;

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

  function styles() {
    if (el('d2d-css')) return;
    const s = document.createElement('style'); s.id = 'd2d-css';
    s.textContent = `
      .d2d-ov{position:fixed;inset:0;background:#F7F8FA;z-index:9500;display:flex;flex-direction:column;font-family:inherit;}
      .d2d-head{display:flex;justify-content:space-between;align-items:center;padding:16px 26px;background:#fff;border-bottom:.5px solid rgba(0,0,0,.08);}
      .d2d-body{flex:1;overflow-y:auto;padding:18px 26px 40px;}
      .d2d-x{border:0;background:#F2F2F5;width:34px;height:34px;border-radius:9px;cursor:pointer;font-size:18px;color:${MID};}
      .d2d-tiles{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px;}
      .d2d-card{border:.5px solid rgba(16,18,27,.08);border-radius:14px;background:#fff;
        box-shadow:0 1px 2px rgba(16,18,27,.04),0 4px 12px rgba(16,18,27,.05);}
      .d2d-tile{padding:14px 16px;}
      .d2d-tl{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;}
      .d2d-tv{font-size:25px;font-weight:700;color:${DARK};margin-top:3px;letter-spacing:-.02em;}
      .d2d-ts{font-size:11px;color:${MID};}
      .d2d-wk{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;}
      .d2d-wkb{border:.5px solid rgba(0,0,0,.14);background:#fff;color:${DARK};border-radius:9px;padding:8px 13px;
        font:600 12px inherit;cursor:pointer;min-height:44px;transition:border-color .2s ease,background .2s ease;}
      .d2d-wkb:hover{border-color:rgba(0,0,0,.34);}
      .d2d-wkb.on{background:${DARK};color:#fff;border-color:${DARK};}
      .d2d-sh{padding:15px 18px;margin-bottom:12px;}
      .d2d-strip{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));margin-top:12px;}
      .d2d-st{display:flex;flex-direction:column;align-items:center;gap:5px;position:relative;}
      .d2d-line{position:absolute;top:26px;height:3px;}
      .d2d-dot{width:30px;height:30px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;
        border:2px solid;box-sizing:border-box;background:#fff;position:relative;z-index:1;}
      @keyframes d2d-live{0%,100%{transform:scale(1);}50%{transform:scale(1.12);}}
      @keyframes d2d-rise{from{opacity:0;transform:translateY(6px);}to{opacity:1;transform:none;}}
      .d2d-pulse{animation:d2d-live 2.4s ease-in-out infinite;}
      .d2d-rise{animation:d2d-rise .4s cubic-bezier(.22,1,.36,1) both;}
      @media (prefers-reduced-motion:reduce){.d2d-pulse,.d2d-rise{animation:none;}}
      .d2d-src{font-size:9px;letter-spacing:.03em;text-transform:uppercase;}
      .d2d-empty{padding:40px;text-align:center;color:${MID};font-size:13px;}
    `;
    document.head.appendChild(s);
  }

  function close() { const o = document.querySelector('.d2d-ov'); if (o) o.remove(); document.body.style.overflow = ''; }

  async function open() {
    styles();
    if (document.querySelector('.d2d-ov')) return;
    const ov = document.createElement('div');
    ov.className = 'd2d-ov';
    ov.innerHTML = `
      <div class="d2d-head">
        <div>
          <div style="font-size:18px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Shipments</div>
          <div style="font-size:11px;color:${MID};margin-top:2px;" id="d2d-sub">Door to door &middot; plan against actual</div>
        </div>
        <button class="d2d-x" id="d2d-close" aria-label="Close">&times;</button>
      </div>
      <div class="d2d-body" id="d2d-body"><div class="d2d-empty">Loading&hellip;</div></div>`;
    document.body.appendChild(ov);
    document.body.style.overflow = 'hidden';
    el('d2d-close').onclick = close;
    const onKey = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);
    await render();
  }

  async function render() {
    const body = el('d2d-body'); if (!body) return;
    let weeks = [], shipments = [], bookings = [];
    try {
      weeks = (await api('/d2d/weeks')).weeks || [];
      if (!_week && weeks.length) _week = weeks[0].week_start;
      if (_week) {
        shipments = (await api('/d2d/shipments?week=' + encodeURIComponent(_week))).shipments || [];
        bookings = (await api('/d2d/bookings?week=' + encodeURIComponent(_week))).bookings || [];
      }
    } catch (e) {
      body.innerHTML = `<div class="d2d-empty" style="color:${BRAND}">Could not load shipments (${esc(e.message)}).</div>`;
      return;
    }

    if (!weeks.length) {
      body.innerHTML = `<div class="d2d-empty">No weeks booked yet.<br><span style="font-size:11.5px;">A week appears here once an option has been approved.</span></div>`;
      return;
    }

    // Tiles, computed from what is actually loaded rather than a stored figure.
    const inTransit = shipments.filter(s => s.status === 'in_transit').length;
    const delivered = shipments.filter(s => s.status === 'delivered').length;
    let late = 0, slipTotal = 0, slipN = 0;
    for (const s of shipments) {
      const ev = {}; for (const e of (s.events || [])) ev[e.stage] = e.actual_at;
      for (const [k] of STAGES) {
        const d = daysBetween(s['plan_' + k], ev[k]);
        if (d != null && d > 0) { slipTotal += d; slipN++; }
      }
      const worst = STAGES.map(([k]) => daysBetween(s['plan_' + k], ev[k])).filter(x => x != null);
      if (worst.some(x => x > 0)) late++;
    }
    const approved = bookings.find(b => b.status === 'approved');

    body.innerHTML = `
      <div class="d2d-wk">
        ${weeks.map(w => `<button class="d2d-wkb ${w.week_start === _week ? 'on' : ''}" data-w="${esc(w.week_start)}">
            ${esc(day(w.week_start))} <span style="opacity:.6;font-weight:500;">&middot; ${w.shipments}</span>
          </button>`).join('')}
      </div>

      <div class="d2d-tiles">
        <div class="d2d-card d2d-tile d2d-rise"><div class="d2d-tl">Shipments</div>
          <div class="d2d-tv">${shipments.length}</div><div class="d2d-ts">this week</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.05s"><div class="d2d-tl">In transit</div>
          <div class="d2d-tv">${inTransit}</div><div class="d2d-ts">${delivered} delivered</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.1s"><div class="d2d-tl">Behind plan</div>
          <div class="d2d-tv" style="color:${late ? BRAND : DARK}">${late}</div>
          <div class="d2d-ts">${slipN ? 'avg slip ' + (slipTotal / slipN).toFixed(1) + ' days' : 'on plan'}</div></div>
        <div class="d2d-card d2d-tile d2d-rise" style="animation-delay:.15s"><div class="d2d-tl">Booked as</div>
          <div class="d2d-tv" style="font-size:18px;">${approved ? esc(approved.title || approved.option_ref) : '&ndash;'}</div>
          <div class="d2d-ts">${approved ? esc(approved.carrier || '') + (approved.transit_days ? ' &middot; ' + approved.transit_days + ' days' : '') : 'no approved option'}</div></div>
      </div>

      ${shipments.map(s => shipmentCard(s)).join('')}
    `;

    body.querySelectorAll('[data-w]').forEach(b => b.onclick = () => { _week = b.getAttribute('data-w'); render(); });
    const sub = el('d2d-sub');
    if (sub) sub.textContent = 'Week of ' + day(_week) + ' · plan against actual';
  }

  function shipmentCard(s) {
    const ev = {}; for (const e of (s.events || [])) ev[e.stage] = e;
    let liveIdx = -1;
    STAGES.forEach(([k], i) => { if (ev[k] && ev[k].actual_at) liveIdx = i; });

    const cells = STAGES.map(([k, label], i) => {
      const actual = ev[k] && ev[k].actual_at ? ev[k].actual_at : null;
      const plan = s['plan_' + k];
      const slip = daysBetween(plan, actual);
      const done = !!actual;
      const late = slip != null && slip > 0;
      const colour = !done ? '#D6D6DB' : (late ? BRAND : LIME);
      const ink = !done ? LIGHT : (late ? BRAND : LINK);
      const isLive = i === liveIdx;
      const leftFill = i === 0 ? 'transparent' : (i <= liveIdx ? LIME : '#E4E4E9');
      const rightFill = i === STAGES.length - 1 ? 'transparent' : (i < liveIdx ? LIME : '#E4E4E9');
      return `<div class="d2d-st">
        <span class="d2d-line" style="left:0;right:50%;background:${leftFill};"></span>
        <span class="d2d-line" style="left:50%;right:0;background:${rightFill};"></span>
        <span style="font-size:9px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;text-align:center;min-height:22px;">${label}</span>
        <span class="d2d-dot ${isLive ? 'd2d-pulse' : ''}" style="border-color:${colour};background:${done ? (late ? 'rgba(153,0,51,.10)' : 'rgba(155,171,21,.16)') : '#fff'};">
          <span style="width:8px;height:8px;border-radius:50%;background:${done ? colour : '#fff'};border:1px solid ${colour};"></span>
        </span>
        <span style="font-size:10px;color:${LIGHT};font-family:ui-monospace,monospace;">${plan ? 'plan ' + day(plan) : ''}</span>
        <span style="font-size:11.5px;font-weight:600;color:${ink};font-family:ui-monospace,monospace;">${actual ? day(actual) : '&middot;'}</span>
        ${actual ? `<span class="d2d-src" style="color:${ev[k].source === 'manual' ? YINK : MID};">${esc(ev[k].source)}</span>` : ''}
        ${late ? `<span style="font-size:10px;font-weight:700;color:${BRAND};">+${slip}d</span>` : ''}
      </div>`;
    }).join('');

    const worst = STAGES.map(([k]) => daysBetween(s['plan_' + k], ev[k] && ev[k].actual_at)).filter(x => x != null);
    const maxSlip = worst.length ? Math.max(...worst) : null;
    const badge = s.status === 'delivered' ? ['Delivered', LINK, 'rgba(155,171,21,.20)']
                : (maxSlip != null && maxSlip > 0) ? ['+' + maxSlip + ' days', '#fff', BRAND]
                : s.status === 'in_transit' ? ['On plan', LINK, 'rgba(155,171,21,.20)']
                : ['Booked', MID, '#F2F2F5'];

    return `<div class="d2d-card d2d-sh d2d-rise">
      <div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;">
        <div style="display:flex;align-items:baseline;gap:11px;">
          <span style="font-family:ui-monospace,monospace;font-size:14px;color:${DARK};">${esc(s.reference || 'container not yet advised')}</span>
          <span style="font-size:11px;color:${MID};">${esc([s.container_type, s.carrier, s.vessel].filter(Boolean).join(' · '))}</span>
        </div>
        <span style="font-size:10.5px;font-weight:700;border-radius:6px;padding:3px 9px;color:${badge[1]};background:${badge[2]};">${badge[0]}</span>
      </div>
      <div class="d2d-strip">${cells}</div>
    </div>`;
  }

  // ── Wiring ──
  const boot = () => { injectNav(); refreshEnabled().catch(() => {}); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);
  const mo = new MutationObserver(() => injectNav());
  const start = () => mo.observe(document.body, { childList: true, subtree: true });
  if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);
  // No polling: switching client reloads the page (the tenancy module calls location.reload),
  // so the capability is re-read on boot. A timer here would run forever for no gain.
  window.addEventListener('focus', () => { refreshEnabled().catch(() => {}); }, { passive: true });

  window.__openD2D = open;
  console.log('[d2d-hub] v1 loaded');
})();
