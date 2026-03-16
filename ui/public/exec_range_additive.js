/*
 * Pinpoint Exec Range Extension (exec_range_additive.js)
 * -------------------------------------------------------
 * Adds rolling-window filter bar + WoW throughput chart + Completion % trend
 * to the Executive page. Fully additive — zero changes to exec_live_additive.js.
 *
 * Strategy:
 *  1. Injects a sticky filter bar above #exec-live once that div exists
 *  2. Fetches multi-week data independently (does not touch window.state)
 *  3. Renders two new charts below the existing KPI tiles
 *  4. Re-renders existing tiles/radar/donut with averaged range data via
 *     window.__execRangeOverride (exec_live_additive reads this if present)
 *  5. Compare-period toggle injects delta badges onto existing tiles
 */
(function ExecRangeAdditive() {
  'use strict';

  // ── Constants ────────────────────────────────────────────────────────────────
  const BRAND       = '#990033';
  const GREEN_VIS   = '#C8F902';
  const GREEN_TEXT  = '#97DC21';
  const DARK        = '#1C1C1E';
  const MID         = '#6E6E73';
  const LIGHT       = '#AEAEB2';
  const BG          = '#F5F5F7';
  const BORDER      = 'rgba(0,0,0,0.08)';
  const BUSINESS_TZ = document.querySelector('meta[name="business-tz"]')?.content || 'Asia/Shanghai';

  const _rawBase = document.querySelector('meta[name="api-base"]')?.content || location.origin;
  const API_BASE = (() => {
    const b = String(_rawBase || '').replace(/\/+$/, '');
    return /\/api$/i.test(b) ? b : (b ? b + '/api' : '/api');
  })();

  // ── State ────────────────────────────────────────────────────────────────────
  let _window   = 30;   // days — default Last 30d
  let _compare  = false;
  let _loading  = false;
  let _rangeData   = null; // { weeks: [{ws, we, plan, records, bins}] }
  let _compareData = null;

  // ── Helpers ──────────────────────────────────────────────────────────────────
  const $ = s => document.querySelector(s);
  const fmt = n => Number(n || 0).toLocaleString();
  const pct = (n, d) => d > 0 ? Math.round(n * 100 / d) : (n > 0 ? 100 : 0);

  function toISODate(d) {
    const x = new Date(d);
    return `${x.getFullYear()}-${String(x.getMonth()+1).padStart(2,'0')}-${String(x.getDate()).padStart(2,'0')}`;
  }

  function addDays(ymd, n) {
    const d = new Date(ymd + 'T12:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return toISODate(d);
  }

  function mondayOf(ymd) {
    if (typeof window.mondayOfInTZ === 'function') return window.mondayOfInTZ(ymd, BUSINESS_TZ);
    const d = new Date(ymd + 'T12:00:00Z');
    const dow = d.getUTCDay(); // 0=Sun
    const offset = dow === 0 ? -6 : 1 - dow;
    d.setUTCDate(d.getUTCDate() + offset);
    return toISODate(d);
  }

  function todayYMD() {
    if (typeof window.todayInTZ === 'function') return window.todayInTZ(BUSINESS_TZ);
    return toISODate(new Date());
  }

  // Generate list of Monday week-starts within [fromYMD, toYMD]
  function weeksInRange(fromYMD, toYMD) {
    const weeks = [];
    let cur = mondayOf(fromYMD);
    while (cur <= toYMD) {
      weeks.push(cur);
      cur = addDays(cur, 7);
    }
    return weeks;
  }

  // Compute [from, to] for a rolling window
  function windowDates(days) {
    const today = todayYMD();
    const from  = addDays(today, -days + 1);
    return { from, to: today };
  }

  // Previous equivalent period
  function prevWindowDates(days) {
    const { from } = windowDates(days);
    const prevTo   = addDays(from, -1);
    const prevFrom = addDays(prevTo, -days + 1);
    return { from: prevFrom, to: prevTo };
  }

  // ── Auth ─────────────────────────────────────────────────────────────────────
  async function getToken() {
    if (window.Clerk?.session) {
      try { return await window.Clerk.session.getToken(); } catch (_) {}
    }
    return null;
  }

  async function apiFetch(path) {
    const token = await getToken();
    const headers = { 'Content-Type': 'application/json' };
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const urls = [
      `${API_BASE}/${path}`,
      `${API_BASE.replace(/\/api$/, '')}/${path}`
    ];
    for (const url of urls) {
      try {
        const r = await fetch(url, { headers });
        if (r.ok) return r.json();
      } catch (_) {}
    }
    throw new Error('All endpoints failed: ' + path);
  }

  // ── Data fetching ─────────────────────────────────────────────────────────────
  async function fetchWeekData(ws) {
    const we = addDays(ws, 6);
    const [planRaw, recsRaw, binsRaw] = await Promise.all([
      apiFetch(`plan/weeks/${ws}`).catch(() => []),
      apiFetch(`records?from=${ws}&to=${we}&status=complete&limit=50000`).catch(() => ({ records: [] })),
      apiFetch(`bins/weeks/${ws}`).catch(() => [])
    ]);
    const plan    = Array.isArray(planRaw) ? planRaw : [];
    const records = Array.isArray(recsRaw) ? recsRaw : (Array.isArray(recsRaw?.records) ? recsRaw.records : []);
    const bins    = Array.isArray(binsRaw) ? binsRaw : [];
    return { ws, we, plan, records, bins };
  }

  async function loadRange(days) {
    const { from, to } = windowDates(days);
    const weekStarts   = weeksInRange(from, to);
    // fetch all weeks in parallel (cap at 14 to avoid hammering)
    const capped = weekStarts.slice(-14);
    const weeks  = await Promise.all(capped.map(ws => fetchWeekData(ws)));
    return { weeks, from, to, days };
  }

  async function loadCompare(days) {
    const { from, to } = prevWindowDates(days);
    const weekStarts   = weeksInRange(from, to);
    const capped       = weekStarts.slice(-14);
    const weeks        = await Promise.all(capped.map(ws => fetchWeekData(ws)));
    return { weeks, from, to, days };
  }

  // ── Metrics per week ──────────────────────────────────────────────────────────
  function computeWeekMetrics(wk) {
    const { ws, we, plan, records, bins } = wk;
    const plannedTotal = plan.reduce((s, p) => s + Number(p.target_qty || 0), 0);
    const appliedTotal = records.reduce((s, r) => s + Number(r.qty ?? r.quantity ?? 1), 0);
    const completionPct = pct(appliedTotal, plannedTotal);

    // Dup UIDs
    const uidMap = new Map();
    for (const r of records) {
      const key = `${r.sku_code||''}:${r.uid||''}`;
      if (r.uid) uidMap.set(key, (uidMap.get(key) || 0) + 1);
    }
    const dupScanCount = [...uidMap.values()].filter(v => v > 1).length;

    // SKU discrepancy
    const planBySKU = new Map();
    for (const p of plan) {
      const sku = String(p.sku_code || '').trim();
      if (sku) planBySKU.set(sku, (planBySKU.get(sku) || 0) + Number(p.target_qty || 0));
    }
    const recBySKU = new Map();
    for (const r of records) {
      const sku = String(r.sku_code || '').trim();
      if (sku) recBySKU.set(sku, (recBySKU.get(sku) || 0) + 1);
    }
    let skuPctSum = 0, skuCnt = 0;
    for (const [sku, planned] of planBySKU) {
      if (planned > 0) { skuPctSum += Math.abs((recBySKU.get(sku)||0) - planned) / planned; skuCnt++; }
    }
    const avgSkuDiscPct = Math.round((skuCnt ? skuPctSum / skuCnt : 0) * 100);

    // PO discrepancy
    const planByPO = new Map();
    for (const p of plan) {
      const po = String(p.po_number || '').trim();
      if (po) planByPO.set(po, (planByPO.get(po) || 0) + Number(p.target_qty || 0));
    }
    const recByPO = new Map();
    for (const r of records) {
      const po = String(r.po_number || '').trim();
      if (po) recByPO.set(po, (recByPO.get(po) || 0) + 1);
    }
    let poPctSum = 0, poCnt = 0;
    for (const [po, planned] of planByPO) {
      if (planned > 0) { poPctSum += Math.abs((recByPO.get(po)||0) - planned) / planned; poCnt++; }
    }
    const avgPoDiscPct = Math.round((poCnt ? poPctSum / poCnt : 0) * 100);

    // Heavy bins
    const heavyCount = bins.filter(b => Number(b.weight_kg || 0) > 12).length;

    // Late appliers (applied after earliest PO due date)
    const poDue = new Map();
    for (const p of plan) {
      const po = String(p.po_number||'').trim(), d = String(p.due_date||'').trim();
      if (po && d) { if (!poDue.has(po) || d < poDue.get(po)) poDue.set(po, d); }
    }
    let lateCount = 0;
    for (const r of records) {
      const po  = String(r.po_number||'').trim();
      const ymd = r.date_local ? String(r.date_local).trim() : '';
      const due = poDue.get(po);
      if (due && ymd && ymd > due) lateCount++;
    }
    const lateRatePct = Math.round(pct(lateCount, records.length));

    return { ws, plannedTotal, appliedTotal, completionPct, dupScanCount, avgSkuDiscPct, avgPoDiscPct, heavyCount, lateCount, lateRatePct };
  }

  // Average a set of weekly metrics
  function averageMetrics(weeks) {
    if (!weeks.length) return null;
    const mArr = weeks.map(computeWeekMetrics);
    const n    = mArr.length;
    const avg  = k => Math.round(mArr.reduce((s, m) => s + (m[k] || 0), 0) / n);
    return {
      completionPct:  avg('completionPct'),
      dupScanCount:   avg('dupScanCount'),
      avgSkuDiscPct:  avg('avgSkuDiscPct'),
      avgPoDiscPct:   avg('avgPoDiscPct'),
      heavyCount:     avg('heavyCount'),
      lateRatePct:    avg('lateRatePct'),
      plannedTotal:   mArr.reduce((s,m) => s + m.plannedTotal,  0),
      appliedTotal:   mArr.reduce((s,m) => s + m.appliedTotal,  0),
      _mArr: mArr
    };
  }

  // ── Inject filter bar ─────────────────────────────────────────────────────────
  function injectFilterBar() {
    if (document.getElementById('exec-range-bar')) return;

    const bar = document.createElement('div');
    bar.id = 'exec-range-bar';
    bar.style.cssText = `
      position:sticky;top:52px;z-index:30;
      background:#fff;border-bottom:0.5px solid ${BORDER};
      padding:10px 24px;display:flex;align-items:center;gap:10px;
      font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;
    `;
    bar.innerHTML = `
      <span style="font-size:11px;font-weight:500;color:${MID};white-space:nowrap;">View range</span>
      <div style="display:flex;gap:4px;background:${BG};border:0.5px solid ${BORDER};border-radius:9px;padding:3px;">
        ${[7,30,90].map(d => `
          <button data-days="${d}" class="exec-range-pill" style="
            font-size:11px;font-weight:500;padding:5px 13px;border-radius:7px;
            border:none;cursor:pointer;font-family:inherit;transition:all .15s;
            background:${d===30?DARK:BG};color:${d===30?'#fff':MID};
            ${d===30?'box-shadow:0 1px 3px rgba(0,0,0,0.12)':''}
          ">Last ${d}d</button>
        `).join('')}
      </div>
      <div style="flex:1;"></div>
      <label style="display:flex;align-items:center;gap:7px;cursor:pointer;font-size:11px;color:${MID};user-select:none;">
        <span>Compare to previous period</span>
        <div id="exec-compare-toggle" style="
          width:32px;height:18px;border-radius:9px;background:${BORDER};
          position:relative;cursor:pointer;transition:background .2s;flex-shrink:0;
          border:0.5px solid ${BORDER};
        ">
          <div id="exec-compare-knob" style="
            position:absolute;top:2px;left:2px;width:14px;height:14px;
            border-radius:50%;background:#fff;transition:transform .2s;
            box-shadow:0 1px 3px rgba(0,0,0,0.2);
          "></div>
        </div>
      </label>
      <div id="exec-range-loading" style="display:none;align-items:center;gap:6px;font-size:11px;color:${LIGHT};">
        <svg width="14" height="14" viewBox="0 0 14 14" style="animation:execSpin 1s linear infinite">
          <circle cx="7" cy="7" r="5.5" fill="none" stroke="${LIGHT}" stroke-width="1.5" stroke-dasharray="20 15"/>
        </svg>
        Loading…
      </div>
      <style>
        @keyframes execSpin{to{transform:rotate(360deg)}}
        .exec-range-pill:hover:not([data-active]){background:rgba(0,0,0,0.05)!important;color:${DARK}!important;}
      </style>
    `;

    // Insert before #exec-live or at the top of #page-exec
    const host  = document.getElementById('page-exec');
    const live  = document.getElementById('exec-live');
    if (host) {
      if (live) host.insertBefore(bar, live);
      else host.prepend(bar);
    }

    // Wire pills
    bar.querySelectorAll('.exec-range-pill').forEach(btn => {
      btn.addEventListener('click', () => {
        const days = Number(btn.dataset.days);
        setWindow(days);
      });
    });

    // Wire compare toggle
    const toggle = document.getElementById('exec-compare-toggle');
    const knob   = document.getElementById('exec-compare-knob');
    if (toggle) {
      toggle.addEventListener('click', () => {
        _compare = !_compare;
        toggle.style.background = _compare ? BRAND : BORDER;
        knob.style.transform    = _compare ? 'translateX(14px)' : '';
        if (_compare && _rangeData) loadAndRenderCompare();
        else { _compareData = null; removeDeltaBadges(); }
      });
    }
  }

  function setActivePill(days) {
    document.querySelectorAll('.exec-range-pill').forEach(btn => {
      const active = Number(btn.dataset.days) === days;
      btn.style.background   = active ? DARK  : BG;
      btn.style.color        = active ? '#fff' : MID;
      btn.style.boxShadow    = active ? '0 1px 3px rgba(0,0,0,0.12)' : '';
      if (active) btn.setAttribute('data-active','1');
      else btn.removeAttribute('data-active');
    });
  }

  function setLoading(on) {
    _loading = on;
    const el = document.getElementById('exec-range-loading');
    if (el) el.style.display = on ? 'flex' : 'none';
  }

  // ── Inject chart containers ───────────────────────────────────────────────────
  function injectRangeCharts() {
    if (document.getElementById('exec-range-charts')) return;
    const wrap = document.createElement('div');
    wrap.id = 'exec-range-charts';
    wrap.style.cssText = `display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;`;
    wrap.innerHTML = `
      <!-- WoW Throughput -->
      <div style="background:#fff;border:0.5px solid ${BORDER};border-radius:12px;padding:16px 18px;">
        <div style="font-size:13px;font-weight:500;color:${DARK};margin-bottom:2px;">Week-over-Week Throughput</div>
        <div style="font-size:10px;color:${LIGHT};margin-bottom:14px;">Planned vs Applied per week</div>
        <div id="exec-wow-chart" style="height:160px;position:relative;"></div>
        <div id="exec-wow-legend" style="display:flex;gap:14px;margin-top:10px;"></div>
      </div>
      <!-- Completion Trend -->
      <div style="background:#fff;border:0.5px solid ${BORDER};border-radius:12px;padding:16px 18px;">
        <div style="font-size:13px;font-weight:500;color:${DARK};margin-bottom:2px;">Completion % Trend</div>
        <div style="font-size:10px;color:${LIGHT};margin-bottom:14px;">Weekly completion rate across range</div>
        <div id="exec-trend-chart" style="height:160px;position:relative;"></div>
      </div>
    `;

    // Insert after filter bar, before exec-live
    const bar  = document.getElementById('exec-range-bar');
    const live = document.getElementById('exec-live');
    const host = document.getElementById('page-exec');
    if (bar && bar.nextSibling) host.insertBefore(wrap, bar.nextSibling);
    else if (live) host.insertBefore(wrap, live);
    else if (host) host.appendChild(wrap);
  }

  // ── SVG bar chart (WoW throughput) ────────────────────────────────────────────
  function renderWoWChart(mArr) {
    const el = document.getElementById('exec-wow-chart');
    if (!el) return;

    const W = el.offsetWidth || 400, H = 160;
    const pad = { t: 10, r: 10, b: 32, l: 44 };
    const innerW = W - pad.l - pad.r;
    const innerH = H - pad.t - pad.b;

    const maxVal = Math.max(1, ...mArr.map(m => Math.max(m.plannedTotal, m.appliedTotal)));
    const scaleY = v => innerH - (v / maxVal) * innerH;

    const n = mArr.length;
    const groupW = innerW / n;
    const barW   = Math.min(groupW * 0.35, 22);
    const gap    = barW * 0.4;

    let svg = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="overflow:visible;font-family:-apple-system,sans-serif;">`;

    // Y grid lines
    for (let i = 0; i <= 4; i++) {
      const y = pad.t + (i / 4) * innerH;
      const val = Math.round(maxVal * (1 - i/4));
      svg += `<line x1="${pad.l}" y1="${y}" x2="${pad.l+innerW}" y2="${y}" stroke="${BORDER}" stroke-width="0.5"/>`;
      svg += `<text x="${pad.l - 5}" y="${y + 3}" text-anchor="end" font-size="9" fill="${LIGHT}">${val >= 1000 ? Math.round(val/1000)+'k' : val}</text>`;
    }

    mArr.forEach((m, i) => {
      const cx      = pad.l + (i + 0.5) * groupW;
      const px      = cx - gap/2 - barW;
      const ax      = cx + gap/2;
      const planH   = (m.plannedTotal / maxVal) * innerH;
      const appH    = (m.appliedTotal / maxVal) * innerH;
      const planY   = pad.t + innerH - planH;
      const appY    = pad.t + innerH - appH;

      // Planned bar (dark)
      svg += `<rect x="${px}" y="${planY}" width="${barW}" height="${planH}" rx="2" fill="${DARK}" opacity="0.15"/>`;
      // Applied bar (green)
      svg += `<rect x="${ax}" y="${appY}" width="${barW}" height="${appH}" rx="2" fill="${GREEN_VIS}"/>`;

      // Week label
      const label = m.ws ? m.ws.slice(5) : `W${i+1}`;
      svg += `<text x="${cx}" y="${H - pad.b + 14}" text-anchor="middle" font-size="9" fill="${LIGHT}">${label}</text>`;
    });

    // Axes
    svg += `<line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${pad.t+innerH}" stroke="${BORDER}" stroke-width="0.5"/>`;
    svg += `<line x1="${pad.l}" y1="${pad.t+innerH}" x2="${pad.l+innerW}" y2="${pad.t+innerH}" stroke="${BORDER}" stroke-width="0.5"/>`;

    svg += `</svg>`;
    el.innerHTML = svg;

    // Legend
    const leg = document.getElementById('exec-wow-legend');
    if (leg) leg.innerHTML = `
      <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:${MID};">
        <div style="width:10px;height:10px;border-radius:2px;background:${DARK};opacity:.15;"></div>Planned
      </div>
      <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:${MID};">
        <div style="width:10px;height:10px;border-radius:2px;background:${GREEN_VIS};"></div>Applied
      </div>
    `;
  }

  // ── SVG line chart (Completion % trend) ──────────────────────────────────────
  function renderTrendChart(mArr, compareArr) {
    const el = document.getElementById('exec-trend-chart');
    if (!el) return;

    const W = el.offsetWidth || 400, H = 160;
    const pad = { t: 10, r: 14, b: 32, l: 36 };
    const innerW = W - pad.l - pad.r;
    const innerH = H - pad.t - pad.b;

    const n    = mArr.length;
    const maxV = 110; // allow headroom above 100%
    const scaleX = i  => pad.l + (n > 1 ? (i / (n-1)) * innerW : innerW/2);
    const scaleY = v  => pad.t + innerH - Math.min(1, v / maxV) * innerH;

    let svg = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="overflow:visible;font-family:-apple-system,sans-serif;">`;

    // Y grid + labels
    [0, 25, 50, 75, 100].forEach(v => {
      const y = scaleY(v);
      svg += `<line x1="${pad.l}" y1="${y}" x2="${pad.l+innerW}" y2="${y}" stroke="${v===100?BRAND:BORDER}" stroke-width="${v===100?0.8:0.5}" stroke-dasharray="${v===100?'3,3':''}"/>`;
      svg += `<text x="${pad.l-4}" y="${y+3}" text-anchor="end" font-size="9" fill="${v===100?BRAND:LIGHT}">${v}%</text>`;
    });

    // Area fill (primary)
    if (mArr.length > 1) {
      const areaPoints = mArr.map((m, i) => `${scaleX(i)},${scaleY(m.completionPct)}`).join(' ');
      const areaPath   = `M${scaleX(0)},${scaleY(mArr[0].completionPct)} ` +
                         mArr.slice(1).map((m,i) => `L${scaleX(i+1)},${scaleY(m.completionPct)}`).join(' ') +
                         ` L${scaleX(n-1)},${pad.t+innerH} L${pad.l},${pad.t+innerH} Z`;
      svg += `<path d="${areaPath}" fill="${GREEN_VIS}" opacity="0.08"/>`;
    }

    // Compare line (dashed, lighter)
    if (compareArr && compareArr.length > 1) {
      const pts = compareArr.map((m, i) => `${scaleX(i)},${scaleY(m.completionPct)}`);
      svg += `<polyline points="${pts.join(' ')}" fill="none" stroke="${LIGHT}" stroke-width="1.2" stroke-dasharray="4,3"/>`;
    }

    // Primary line
    if (mArr.length > 1) {
      const pts = mArr.map((m, i) => `${scaleX(i)},${scaleY(m.completionPct)}`);
      svg += `<polyline points="${pts.join(' ')}" fill="none" stroke="${GREEN_TEXT}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>`;
    }

    // Dots + tooltips
    mArr.forEach((m, i) => {
      const x = scaleX(i), y = scaleY(m.completionPct);
      svg += `<circle cx="${x}" cy="${y}" r="3.5" fill="${GREEN_TEXT}" stroke="#fff" stroke-width="1.5"/>`;
      svg += `<title>${m.ws}: ${m.completionPct}%</title>`;
    });

    // X labels
    const step = n > 8 ? Math.ceil(n / 6) : 1;
    mArr.forEach((m, i) => {
      if (i % step === 0 || i === n-1) {
        const label = m.ws ? m.ws.slice(5) : `W${i+1}`;
        svg += `<text x="${scaleX(i)}" y="${H - pad.b + 14}" text-anchor="middle" font-size="9" fill="${LIGHT}">${label}</text>`;
      }
    });

    svg += `</svg>`;
    el.innerHTML = svg;
  }

  // ── Update existing tiles with averaged data ──────────────────────────────────
  function updateExecTiles(avg, compareAvg) {
    const tilesWrap = document.getElementById('exec-tiles');
    if (!tilesWrap) return;

    const tileData = [
      { label: 'Completion %',  value: `${avg.completionPct}%`,  cKey: 'completionPct',  higher: true },
      { label: 'Duplicate UIDs',value: fmt(avg.dupScanCount),     cKey: 'dupScanCount',   higher: false },
      { label: 'Avg SKU %Δ',    value: `${avg.avgSkuDiscPct}%`,   cKey: 'avgSkuDiscPct',  higher: false },
      { label: 'Avg PO %Δ',     value: `${avg.avgPoDiscPct}%`,    cKey: 'avgPoDiscPct',   higher: false },
      { label: 'Heavy bins >12kg', value: fmt(avg.heavyCount),    cKey: 'heavyCount',     higher: false },
      { label: 'Late appliers', value: `${avg.lateRatePct}%`,     cKey: 'lateRatePct',    higher: false },
    ];

    tilesWrap.innerHTML = tileData.map(t => {
      let delta = '';
      if (compareAvg && t.cKey) {
        const curr  = avg[t.cKey] || 0;
        const prev  = compareAvg[t.cKey] || 0;
        if (prev > 0) {
          const diff  = curr - prev;
          const diffP = Math.round((diff / prev) * 100);
          const up    = diff > 0;
          const good  = t.higher ? up : !up;
          const arrow = up ? '↑' : '↓';
          const color = good ? GREEN_TEXT : '#D61A3C';
          delta = `<div style="font-size:10px;color:${color};font-weight:500;margin-top:2px;">${arrow} ${Math.abs(diffP)}% vs prev</div>`;
        }
      }
      return `
        <div style="background:#fff;border:0.5px solid ${BORDER};border-radius:10px;padding:14px 16px;">
          <div style="font-size:10px;color:${MID};margin-bottom:4px;">${t.label}</div>
          <div style="font-size:20px;font-weight:600;color:${DARK};line-height:1;font-variant-numeric:tabular-nums;">${t.value}</div>
          ${delta}
        </div>`;
    }).join('');

    // Label update
    const rangeLabel = document.getElementById('exec-range-label');
    if (rangeLabel) rangeLabel.textContent = `Avg across Last ${_window}d (${avg._mArr?.length || '?'} weeks)`;
  }

  function removeDeltaBadges() {
    // Re-render tiles without deltas
    if (_rangeData) {
      const avg = averageMetrics(_rangeData.weeks);
      if (avg) updateExecTiles(avg, null);
    }
  }

  // ── Range label below tiles ───────────────────────────────────────────────────
  function injectRangeLabel() {
    if (document.getElementById('exec-range-label')) return;
    const tilesWrap = document.getElementById('exec-tiles');
    if (!tilesWrap) return;
    const label = document.createElement('div');
    label.id = 'exec-range-label';
    label.style.cssText = `font-size:10px;color:${LIGHT};margin-bottom:8px;margin-top:-4px;`;
    tilesWrap.after(label);
  }

  // ── Main render ───────────────────────────────────────────────────────────────
  async function renderRange(days) {
    setLoading(true);
    setActivePill(days);

    try {
      _rangeData = await loadRange(days);

      if (_compare) {
        _compareData = await loadCompare(days);
      }

      const avg        = averageMetrics(_rangeData.weeks);
      const compareAvg = (_compare && _compareData) ? averageMetrics(_compareData.weeks) : null;

      if (!avg) return;

      injectRangeLabel();
      updateExecTiles(avg, compareAvg);
      renderWoWChart(avg._mArr);
      renderTrendChart(avg._mArr, compareAvg?._mArr);

    } catch (e) {
      console.error('[ExecRange] render failed:', e);
    } finally {
      setLoading(false);
    }
  }

  async function loadAndRenderCompare() {
    setLoading(true);
    try {
      _compareData = await loadCompare(_window);
      const avg        = averageMetrics(_rangeData.weeks);
      const compareAvg = averageMetrics(_compareData.weeks);
      if (avg) {
        updateExecTiles(avg, compareAvg);
        renderTrendChart(avg._mArr, compareAvg._mArr);
      }
    } catch (e) {
      console.error('[ExecRange] compare load failed:', e);
    } finally {
      setLoading(false);
    }
  }

  function setWindow(days) {
    _window = days;
    renderRange(days);
  }

  // ── Mount point — wait for #exec-live to exist ────────────────────────────────
  function tryMount() {
    if (location.hash !== '#exec') return;
    const live = document.getElementById('exec-live');
    if (!live) {
      setTimeout(tryMount, 120);
      return;
    }
    injectFilterBar();
    injectRangeCharts();
    renderRange(_window);
  }

  // Re-mount on resize (chart widths depend on container)
  let _resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(_resizeTimer);
    _resizeTimer = setTimeout(() => {
      if (_rangeData) {
        const avg = averageMetrics(_rangeData.weeks);
        if (avg) {
          renderWoWChart(avg._mArr);
          renderTrendChart(avg._mArr, _compareData ? averageMetrics(_compareData.weeks)._mArr : null);
        }
      }
    }, 200);
  });

  // ── Event hooks ───────────────────────────────────────────────────────────────
  window.addEventListener('hashchange', () => {
    if (location.hash === '#exec') setTimeout(tryMount, 80);
  });

  // Fire immediately if already on exec
  if (location.hash === '#exec') setTimeout(tryMount, 80);

  // Also hook into state:ready in case exec loads after state is available
  window.addEventListener('state:ready', () => {
    if (location.hash === '#exec') setTimeout(tryMount, 120);
  });

})();
