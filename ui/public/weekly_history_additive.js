/* ── VelOzity Pinpoint — Control Tower history (last 10 weeks) v1 ──
   Lives on Reports & Downloads, in its own file, so a fault here cannot reach the dashboard.

   It draws each week with the dashboard's OWN renderer (window.__FLOW_API__.renderJourney),
   not a copy, so the graphic is identical by construction and cannot drift.

   Isolation, deliberately:
   - Reads only. The per-week preparation it calls writes nothing the dashboard reads.
   - Clicks inside each graphic are switched off: the dashboard's node handler would otherwise
     set the dashboard's selection from a historical week. Clicking a row opens that week on
     the Week Hub through the dashboard's own navigation instead.
   - Adds its button beside "Download All" by observing the page, without editing the Reports
     page code. If this module fails to load, Reports & Downloads is unaffected. */
;(function () {
  'use strict';
  if (window.__WH_HISTORY__) return;
  window.__WH_HISTORY__ = true;

  const WEEKS = 10, PARALLEL = 3;
  const DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#1B7F3B', AMBER = '#B7791F', RED = '#B33F40', BRAND = '#990033';
  const el = (id) => document.getElementById(id);
  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const api = () => window.__FLOW_API__ || null;

  // ISO week number, so rows read W37 / W38 like everywhere else in Pinpoint.
  function isoWeek(ymd) {
    const d = new Date(ymd + 'T00:00:00Z'); const day = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - day + 3);
    const f = new Date(Date.UTC(d.getUTCFullYear(), 0, 4)); const fd = (f.getUTCDay() + 6) % 7;
    f.setUTCDate(f.getUTCDate() - fd + 3);
    return 1 + Math.round((d - f) / (7 * 86400000));
  }
  const fmtDay = (ymd) => { try { return new Date(ymd + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); } catch (e) { return ymd; } };

  // ── Plain-language status, derived by rule rather than AI so it is instant and exact ──
  function describe(r) {
    const c = r.completion || {};
    const open = [];
    const plannedPOs = Number(r.receiving && r.receiving.plannedPOs) || 0;
    const receivedPOs = Number(r.receiving && r.receiving.receivedPOs) || 0;
    const plannedUnits = Number(r.vas && r.vas.plannedUnits) || 0;
    const appliedUnits = Number(r.vas && r.vas.appliedUnits) || 0;
    const lanes = Array.isArray(r.intl && r.intl.lanes) ? r.intl.lanes : [];
    const has = (m, k) => !!(m && m[k] && !isNaN(new Date(m[k]).getTime()));
    const isDelivered = (x) => !!(x && (x.status === 'Delivered' || x.status === 'Complete' || x.delivered_local || x.delivered_at));

    if (!c.receiving || (!c.receiving.complete && !c.receiving.na)) {
      if (plannedPOs) open.push(`${Math.max(0, plannedPOs - receivedPOs)} of ${plannedPOs} POs still to receive`);
      else open.push('Receiving not signed off');
    }
    if (!c.vas || (!c.vas.complete && !c.vas.na)) {
      if (plannedUnits) open.push(`${Math.min(100, Math.round(appliedUnits / plannedUnits * 100))}% of units applied`);
      else open.push('VAS not signed off');
    }
    if (!c.intl || (!c.intl.complete && !c.intl.na)) {
      if (lanes.length) {
        const notDone = lanes.filter(l => !(has(l.manual, 'departed_at') && has(l.manual, 'arrived_at') && has(l.manual, 'dest_customs_cleared_at'))).length;
        open.push(`${notDone} of ${lanes.length} lane${lanes.length === 1 ? '' : 's'} not yet cleared`);
      }
    }
    if (!c.lastmile || (!c.lastmile.complete && !c.lastmile.na)) {
      if (c.lastmile && c.lastmile.open) open.push(c.lastmile.open.charAt(0).toUpperCase() + c.lastmile.open.slice(1));
      else if ((r.containers || []).length) {
        const d = (r.receipts || []).filter(isDelivered).length;
        open.push(`${Math.max(0, r.containers.length - d)} of ${r.containers.length} containers undelivered`);
      }
    }

    const keys = ['receiving', 'vas', 'intl', 'lastmile'];
    const closed = keys.every(k => c[k] && (c[k].complete || c[k].na));
    const empty = keys.every(k => c[k] && c[k].na);
    const late = keys.filter(k => c[k] && c[k].complete && c[k].late).length;
    // Not closed and nothing specific outstanding: the remaining stages simply have not started.
    const upcoming = !closed && open.length === 0;
    return { open, closed, empty, late, upcoming };
  }

  // ── Shell ──
  function styles() {
    if (el('wh-hist-css')) return;
    const st = document.createElement('style'); st.id = 'wh-hist-css';
    st.textContent = `
      .wh-ov{position:fixed;inset:0;background:#fff;z-index:9600;display:flex;flex-direction:column;font-family:inherit;}
      .wh-head{display:flex;justify-content:space-between;align-items:center;padding:18px 28px;border-bottom:.5px solid rgba(0,0,0,.08);}
      .wh-body{flex:1;overflow-y:auto;padding:20px 28px 40px;background:#fafafb;}
      .wh-x{border:0;background:#F5F5F7;width:32px;height:32px;border-radius:9px;cursor:pointer;font-size:18px;color:${MID};}
      .wh-tiles{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:18px;}
      .wh-tile,.wh-row{border:.5px solid rgba(16,18,27,.08);border-radius:14px;
        background-image:linear-gradient(115deg,rgba(255,255,255,0) 28%,rgba(255,255,255,.75) 42%,rgba(255,255,255,.95) 48%,rgba(255,255,255,.75) 54%,rgba(255,255,255,0) 68%),
                         linear-gradient(168deg,#ffffff 0%,#ffffff 46%,#f7f9fc 100%);
        background-size:220% 220%,100% 100%;background-position:100% 0%,0% 0%;background-repeat:no-repeat;
        box-shadow:inset 0 1px 0 #fff,inset 0 -1px 0 rgba(16,18,27,.05),0 1px 2px rgba(16,18,27,.045),0 4px 12px rgba(16,18,27,.055);
        transition:transform .28s cubic-bezier(.22,1,.36,1),box-shadow .28s cubic-bezier(.22,1,.36,1),background-position .55s cubic-bezier(.22,1,.36,1);}
      .wh-tile{padding:14px 16px;}
      .wh-tl{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;}
      .wh-tv{font-size:24px;font-weight:700;color:${DARK};margin-top:4px;letter-spacing:-.02em;}
      .wh-ts{font-size:11px;color:${MID};margin-top:2px;}
      .wh-row{display:grid;grid-template-columns:minmax(220px,28%) 1fr;gap:18px;align-items:center;padding:12px 18px;margin-bottom:12px;cursor:pointer;}
      .wh-row:hover{transform:translateY(-4px);background-position:0% 0%,0% 0%;box-shadow:inset 0 1px 0 #fff,0 2px 4px rgba(16,18,27,.06),0 18px 38px rgba(16,18,27,.13);}
      @media (prefers-reduced-motion:reduce){.wh-row:hover{transform:none;}}
      .wh-wk{font-size:17px;font-weight:700;color:${DARK};letter-spacing:-.01em;}
      .wh-dt{font-size:11px;color:${LIGHT};margin-left:6px;font-weight:500;}
      .wh-st{font-size:13px;color:${DARK};margin-top:6px;line-height:1.45;}
      .wh-op{font-size:11px;color:${MID};margin-top:4px;line-height:1.5;}
      .wh-badge{display:inline-block;font-size:10px;font-weight:700;border-radius:6px;padding:2px 7px;margin-top:6px;}
      /* The dashboard's own graphic, drawn smaller and inert. pointer-events:none stops its node
         handler from changing the dashboard's selection from a historical week. */
      .wh-journey{pointer-events:none;}
      .wh-journey svg{height:150px !important;}
      .wh-skel{height:150px;border-radius:10px;background:linear-gradient(90deg,#f2f2f5,#fafafb,#f2f2f5);background-size:200% 100%;animation:whsk 1.2s linear infinite;}
      @keyframes whsk{to{background-position:-200% 0;}}
      @media (max-width:900px){.wh-tiles{grid-template-columns:repeat(2,1fr);}.wh-row{grid-template-columns:1fr;}}
    `;
    document.head.appendChild(st);
  }

  function baseWeek() {
    const a = api();
    const pick = (window._reportsWeek || (window.state && window.state.weekStart) || new Date().toISOString().slice(0, 10));
    return a && a.toMonday ? a.toMonday(pick) : pick;
  }

  async function open() {
    const a = api();
    if (!a) { alert('The Week Hub data has not finished loading yet. Open Week Hub once, then try again.'); return; }
    styles();
    if (el('wh-hist')) return;
    const weeks = [];
    const start = baseWeek();
    for (let i = 0; i < WEEKS; i++) weeks.push(a.shiftWeek(start, -i));

    const ov = document.createElement('div');
    ov.className = 'wh-ov'; ov.id = 'wh-hist';
    ov.innerHTML = `
      <div class="wh-head">
        <div>
          <div style="font-size:18px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Control Tower &middot; last ${WEEKS} weeks</div>
          <div style="font-size:11px;color:${MID};margin-top:2px;">W${isoWeek(weeks[weeks.length - 1])}&ndash;W${isoWeek(weeks[0])} &middot; current week first &middot; click a week to open it on the Week Hub</div>
        </div>
        <button class="wh-x" id="wh-close" aria-label="Close">&times;</button>
      </div>
      <div class="wh-body">
        <div class="wh-tiles" id="wh-tiles">${tilesHtml(null)}</div>
        <div id="wh-rows">${weeks.map(ws => `
          <div class="wh-row" data-ws="${esc(ws)}">
            <div><div class="wh-wk">W${isoWeek(ws)} <span class="wh-dt">${esc(fmtDay(ws))}</span></div>
              <div class="wh-st" style="color:${LIGHT}">Loading&hellip;</div></div>
            <div class="wh-skel"></div>
          </div>`).join('')}</div>
      </div>`;
    document.body.appendChild(ov);
    document.body.style.overflow = 'hidden';

    const close = () => { ov.remove(); document.body.style.overflow = ''; document.removeEventListener('keydown', onKey); };
    const onKey = (e) => { if (e.key === 'Escape') close(); };
    document.addEventListener('keydown', onKey);
    el('wh-close').onclick = close;
    ov.querySelectorAll('.wh-row').forEach(row => row.addEventListener('click', () => {
      const ws = row.getAttribute('data-ws');
      close();
      try { if (typeof window.setWeek === 'function') window.setWeek(ws); } catch (e) {}
      try { if (typeof window.show === 'function') window.show('#week-hub'); } catch (e) {}
    }));

    // Weeks load a few at a time and appear as they arrive, so the page is never blank while
    // ten weeks of plans and records come in.
    const results = {};
    let next = 0;
    const worker = async () => {
      while (next < weeks.length) {
        const ws = weeks[next++];
        try { results[ws] = await a.computeWeek(ws); paintRow(ws, results[ws]); }
        catch (e) { paintError(ws, e); }
        el('wh-tiles') && (el('wh-tiles').innerHTML = tilesHtml(results));
      }
    };
    await Promise.all(Array.from({ length: PARALLEL }, worker));
  }

  function tilesHtml(results) {
    const rs = results ? Object.values(results) : [];
    const loaded = rs.length;
    const d = rs.map(describe);
    const closed = d.filter(x => x.closed && !x.empty).length;
    const late = d.reduce((s, x) => s + x.late, 0);
    const openItems = d.reduce((s, x) => s + x.open.length, 0);
    const v = (n) => loaded ? n : '&ndash;';
    return `
      <div class="wh-tile"><div class="wh-tl">Weeks closed</div><div class="wh-tv">${v(closed)}<span style="font-size:13px;color:${LIGHT};font-weight:500;"> / ${WEEKS}</span></div><div class="wh-ts">every stage complete</div></div>
      <div class="wh-tile"><div class="wh-tl">Open items</div><div class="wh-tv" style="color:${openItems ? AMBER : DARK}">${v(openItems)}</div><div class="wh-ts">across the last ${WEEKS} weeks</div></div>
      <div class="wh-tile"><div class="wh-tl">Completed late</div><div class="wh-tv" style="color:${late ? RED : DARK}">${v(late)}</div><div class="wh-ts">stages finished after plan</div></div>
      <div class="wh-tile"><div class="wh-tl">Loaded</div><div class="wh-tv">${loaded}<span style="font-size:13px;color:${LIGHT};font-weight:500;"> / ${WEEKS}</span></div><div class="wh-ts">${loaded < WEEKS ? 'loading remaining weeks' : 'all weeks loaded'}</div></div>`;
  }

  function paintRow(ws, r) {
    const row = document.querySelector(`.wh-row[data-ws="${CSS.escape(ws)}"]`); if (!row) return;
    const d = describe(r);
    const badge = d.empty
      ? `<span class="wh-badge" style="background:#F2F2F5;color:${MID};">No activity</span>`
      : d.upcoming
        ? `<span class="wh-badge" style="background:#F2F2F5;color:${MID};">In progress</span>`
      : d.closed
        ? `<span class="wh-badge" style="background:rgba(27,127,59,.12);color:${GREEN};">Closed${d.late ? ` &middot; ${d.late} late` : ''}</span>`
        : `<span class="wh-badge" style="background:rgba(183,121,31,.12);color:${AMBER};">${d.open.length} open</span>`;
    const lead = d.empty ? 'Nothing planned or shipped this week.'
               : d.upcoming ? 'Remaining stages have not started yet.'
               : d.closed ? 'Every stage complete.' : esc(d.open[0]);
    const rest = (!d.closed && d.open.length > 1) ? d.open.slice(1).map(esc).join(' &middot; ') : '';
    row.innerHTML = `
      <div><div class="wh-wk">W${isoWeek(ws)} <span class="wh-dt">${esc(fmtDay(ws))}</span></div>
        ${badge}
        <div class="wh-st">${lead}</div>
        ${rest ? `<div class="wh-op">Also open: ${rest}</div>` : ''}</div>
      <div class="wh-journey"></div>`;
    try {
      api().renderJourney(r.ws, r.tz, r.receiving, r.vas, r.intl, r.manual, row.querySelector('.wh-journey'));
    } catch (e) {
      row.querySelector('.wh-journey').innerHTML = `<div style="font-size:11px;color:${RED};padding:20px;">Could not draw this week.</div>`;
    }
  }

  function paintError(ws, e) {
    const row = document.querySelector(`.wh-row[data-ws="${CSS.escape(ws)}"]`); if (!row) return;
    row.querySelector('.wh-st') && (row.querySelector('.wh-st').innerHTML = `<span style="color:${RED}">Could not load: ${esc(e && e.message || e)}</span>`);
    const sk = row.querySelector('.wh-skel'); if (sk) sk.remove();
  }

  // ── Button on Reports & Downloads, beside "Download All" ──
  function inject() {
    const anchor = el('btn-consolidated-download');
    if (!anchor || el('wh-hist-btn')) return;
    const b = document.createElement('button');
    b.id = 'wh-hist-btn'; b.type = 'button';
    b.style.cssText = `display:flex;align-items:center;gap:8px;background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.14);border-radius:9px;padding:10px 16px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;margin-right:8px;`;
    b.innerHTML = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="${BRAND}" stroke-width="2" stroke-linecap="round"><path d="M3 12h4l3-8 4 16 3-8h4"/></svg>Last ${WEEKS} weeks`;
    b.onclick = open;
    anchor.parentNode.insertBefore(b, anchor);
  }
  const mo = new MutationObserver(() => inject());
  const startObserving = () => { inject(); mo.observe(document.body, { childList: true, subtree: true }); };
  if (document.body) startObserving(); else document.addEventListener('DOMContentLoaded', startObserving);

  window.__openWeeklyHistory = open;
  console.log('[weekly-history] v1 loaded');
})();
