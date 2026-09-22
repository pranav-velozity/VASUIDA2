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

  // ── Path to green, derived by rule rather than AI so it is instant and exact ──
  // For each stage that is not finished: where it stands, what is missing, and the specific
  // action that turns it green — naming the POs, suppliers, lanes and containers involved.
  const LEVEL = { red: ['Delayed', RED, 'rgba(179,63,64,.10)'], yellow: ['At risk', AMBER, 'rgba(183,121,31,.11)'],
                  green: ['On track', GREEN, 'rgba(27,127,59,.10)'], gray: ['Upcoming', MID, '#F2F2F5'] };
  const nf = (n) => Number(n || 0).toLocaleString();
  const topN = (arr, n) => arr.slice(0, n).join(', ') + (arr.length > n ? ` +${arr.length - n} more` : '');

  function describe(r) {
    const c = r.completion || {};
    const now = Date.now();
    const due = (d) => { const t = d ? new Date(d).getTime() : NaN; return isNaN(t) ? null : t; };
    const isDelivered = (x) => !!(x && (x.status === 'Delivered' || x.status === 'Complete' || x.delivered_local || x.delivered_at));
    const has = (m, k) => !!(m && m[k] && !isNaN(new Date(m[k]).getTime()));
    const stages = [];
    const late = [];
    const add = (key, name, level, facts, path, upcoming) => stages.push({ key, name, level, facts, path, upcoming });
    const live = (k) => c[k] && (c[k].complete || c[k].na);
    const lvl = (obj) => (obj && obj.level) || 'gray';

    // Receiving
    if (c.receiving && c.receiving.complete && c.receiving.late) late.push('Receiving');
    if (!live('receiving')) {
      const R = r.receiving || {};
      const missing = Array.isArray(R.missingPOList) ? R.missingPOList : [];
      const lateN = Array.isArray(R.latePOList) ? R.latePOList.length : (R.latePOs || 0);
      const bySup = (R.suppliers || []).map(x => ({ s: x.supplier, n: (x.poCount || 0) - (x.receivedPOs || 0) }))
                                       .filter(x => x.n > 0).sort((a, b) => b.n - a.n).map(x => `${x.s} (${x.n} PO${x.n === 1 ? '' : 's'})`);
      const upcoming = due(R.due) != null && now < due(R.due) && !(R.receivedPOs > 0);
      if (!missing.length && (R.plannedPOs || 0) > 0) {
        add('receiving', 'Receiving', 'green', `All ${nf(R.plannedPOs)} POs received${lateN ? ` · ${nf(lateN)} late` : ''}`,
            'Tick Receiving complete to close it');
      } else if ((R.plannedPOs || 0) > 0) {
        add('receiving', 'Receiving', lvl(R), `${nf(missing.length)} of ${nf(R.plannedPOs)} POs not received${lateN ? ` · ${nf(lateN)} late` : ''}`,
            bySup.length ? `Chase ${topN(bySup, 2)}` : `Receive ${topN(missing, 3)}`, upcoming);
      } else add('receiving', 'Receiving', 'gray', 'No plan uploaded', 'Upload the week plan', upcoming);
    }

    // VAS
    if (c.vas && c.vas.complete && c.vas.late) late.push('VAS');
    if (!live('vas')) {
      const V = r.vas || {};
      const planned = V.plannedUnits || 0, applied = V.appliedUnits || 0;
      const pct = planned ? Math.min(100, Math.round(applied / planned * 100)) : 0;
      const bySup = (V.supplierRows || []).filter(x => x.remaining > 0).sort((a, b) => b.remaining - a.remaining)
                                         .map(x => `${x.supplier} (${nf(x.remaining)} units)`);
      const upcoming = !(applied > 0) && due(V.due) != null && now < due(V.due);
      if (planned && pct >= 98) {
        add('vas', 'VAS', 'green', `${pct}% of ${nf(planned)} units applied`, 'Tick VAS complete to close it');
      } else if (planned) {
        add('vas', 'VAS', lvl(V), `${pct}% applied · ${nf(Math.max(0, planned - applied))} units to go`,
            bySup.length ? `Apply units for ${topN(bySup, 2)}` : 'Apply remaining units', upcoming);
      }
    }

    // Transit & Clearing — each lane counted at the first of the three steps it still needs.
    if (c.intl && c.intl.complete && c.intl.late) late.push('Transit');
    if (!live('intl')) {
      const I = r.intl || {};
      const lanes = Array.isArray(I.lanes) ? I.lanes : [];
      if (lanes.length) {
        let dep = 0, arr = 0, clr = 0; const stuck = new Map();
        for (const l of lanes) {
          const m = l.manual || {};
          const step = !has(m, 'departed_at') ? 'dep' : !has(m, 'arrived_at') ? 'arr' : !has(m, 'dest_customs_cleared_at') ? 'clr' : null;
          if (!step) continue;
          if (step === 'dep') dep++; else if (step === 'arr') arr++; else clr++;
          const sup = l.supplier || 'Unknown'; stuck.set(sup, (stuck.get(sup) || 0) + 1);
        }
        const open = dep + arr + clr;
        const parts = [dep && `${dep} to depart`, arr && `${arr} to arrive`, clr && `${clr} to clear`].filter(Boolean);
        const holds = Number(I.holds || 0);
        const who = [...stuck.entries()].sort((a, b) => b[1] - a[1]).map(([s2, n]) => `${s2} (${n} lane${n === 1 ? '' : 's'})`);
        const upcoming = due(I.originMin) != null && now < due(I.originMin);
        add('intl', 'Transit & Clearing', lvl(I),
            `${nf(open)} of ${nf(lanes.length)} lanes open${holds ? ` · ${holds} customs hold${holds === 1 ? '' : 's'}` : ''}`,
            open ? `${parts.join(' · ')}${who.length ? ` — ${topN(who, 2)}` : ''}` : 'Record the remaining dates', upcoming);
      }
    }

    // Last Mile — names the containers still out, not just a count.
    if (c.lastmile && c.lastmile.complete && c.lastmile.late) late.push('Last mile');
    if (!live('lastmile')) {
      const M = r.manual || {};
      const containers = Array.isArray(r.containers) ? r.containers : [];
      const map = r.receiptMap || {};
      const idOf = (x) => x && (x.container_uid || x.uid || x.container_id);
      const labelOf = (x) => (x && (x.container_id || x.container_uid || x.uid)) || (x && x.vessel) || 'container';
      const out = containers.filter(x => {
        const k = idOf(x); const rc = (k && map[k]) || (x && x.container_id && map[x.container_id]) || null;
        return !isDelivered(rc);
      });
      const lm = (M.levels && M.levels.lastMile) || 'gray';
      const upcoming = due(M.baselines && M.baselines.lastMileMin) != null && now < due(M.baselines.lastMileMin);
      if (c.lastmile && c.lastmile.open === 'no containers recorded') {
        add('lastmile', 'Last Mile', lm, 'No containers recorded yet', 'Add this week\'s containers in Transit & Clearing', upcoming);
      } else if (c.lastmile && c.lastmile.open) {
        add('lastmile', 'Last Mile', lm, 'Air lane without a delivery record', 'Record the air pallet and its delivery', upcoming);
      } else if (containers.length) {
        add('lastmile', 'Last Mile', lm, `${nf(out.length)} of ${nf(containers.length)} undelivered`,
            out.length ? `Deliver ${topN(out.map(labelOf), 3)}` : 'Record the remaining deliveries', upcoming);
      }
    }

    const keys = ['receiving', 'vas', 'intl', 'lastmile'];
    const closed = keys.every(k => c[k] && (c[k].complete || c[k].na));
    const empty = keys.every(k => c[k] && c[k].na);
    const active = stages.filter(x => !x.upcoming);
    const upcoming = !closed && active.length === 0;
    // "open" drives the Open items tile: stages that need something done, not ones not yet due.
    return { stages, active, open: active, closed, empty, late, upcoming, lateCount: late.length };
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
      .wh-row{display:grid;grid-template-columns:96px minmax(0,1fr) minmax(300px,34%);gap:20px;align-items:center;padding:12px 18px;margin-bottom:12px;cursor:pointer;}
      .wh-left{border-right:.5px solid rgba(0,0,0,.06);padding-right:12px;align-self:stretch;display:flex;flex-direction:column;justify-content:center;align-items:flex-start;}
      .wh-stg{display:grid;grid-template-columns:auto 1fr;gap:2px 9px;align-items:baseline;padding:6px 0;border-top:.5px solid rgba(0,0,0,.05);}
      .wh-stg:first-of-type{border-top:0;}
      .wh-chip{font-size:9.5px;font-weight:700;border-radius:5px;padding:2px 6px;white-space:nowrap;}
      .wh-sn{font-size:12px;font-weight:600;color:${DARK};}
      .wh-sf{grid-column:2;font-size:11.5px;color:${DARK};line-height:1.4;}
      .wh-sp{grid-column:2;font-size:11px;color:${MID};line-height:1.4;}
      .wh-sp b{color:${DARK};font-weight:600;}
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
      .wh-journey svg{height:190px !important;}
      .wh-skel{height:190px;border-radius:10px;background:linear-gradient(90deg,#f2f2f5,#fafafb,#f2f2f5);background-size:200% 100%;animation:whsk 1.2s linear infinite;}
      @keyframes whsk{to{background-position:-200% 0;}}
      @media (max-width:1100px){.wh-row{grid-template-columns:80px minmax(0,1fr);}.wh-row>.wh-right{grid-column:1 / -1;}}
      @media (max-width:700px){.wh-tiles{grid-template-columns:repeat(2,1fr);}.wh-row{grid-template-columns:1fr;}.wh-left{border-right:0;}}
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
            <div class="wh-left"><div class="wh-wk">W${isoWeek(ws)}</div><div class="wh-dt" style="margin:2px 0 0;">${esc(fmtDay(ws))}</div></div>
            <div class="wh-skel"></div>
            <div class="wh-right"><div class="wh-st" style="color:${LIGHT}">Loading&hellip;</div></div>
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
    const late = d.reduce((s, x) => s + x.lateCount, 0);
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
      : d.upcoming ? `<span class="wh-badge" style="background:#F2F2F5;color:${MID};">In progress</span>`
      : d.closed ? `<span class="wh-badge" style="background:rgba(27,127,59,.12);color:${GREEN};">Closed</span>`
      : `<span class="wh-badge" style="background:rgba(183,121,31,.12);color:${AMBER};">${d.active.length} open</span>`;

    let right;
    if (d.empty) right = `<div class="wh-st">Nothing planned or shipped this week.</div>`;
    else if (d.closed) right = `<div class="wh-st">Every stage complete.</div>`;
    else {
      // Active stages first (the ones needing action), then anything simply not due yet.
      const order = [...d.stages].sort((a, b) => (a.upcoming ? 1 : 0) - (b.upcoming ? 1 : 0));
      right = order.map(x => {
        const [label, fg, bg] = x.upcoming ? LEVEL.gray : (LEVEL[x.level] || LEVEL.gray);
        return `<div class="wh-stg">
          <span class="wh-chip" style="color:${fg};background:${bg};">${label}</span>
          <span class="wh-sn">${esc(x.name)}</span>
          <div class="wh-sf">${esc(x.facts)}</div>
          ${x.upcoming ? '' : `<div class="wh-sp"><b>Path to green:</b> ${esc(x.path)}</div>`}
        </div>`;
      }).join('');
    }
    if (d.late.length) right += `<div class="wh-op" style="margin-top:6px;">Completed late: ${esc(d.late.join(', '))}</div>`;

    row.innerHTML = `
      <div class="wh-left"><div class="wh-wk">W${isoWeek(ws)}</div>
        <div class="wh-dt" style="margin:2px 0 0;">${esc(fmtDay(ws))}</div>${badge}</div>
      <div class="wh-journey"></div>
      <div class="wh-right">${right}</div>`;
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
  console.log('[weekly-history] v3 loaded');
})();
