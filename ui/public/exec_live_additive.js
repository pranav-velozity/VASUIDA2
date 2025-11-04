/*
 * Pinpoint Exec — Additive Live Widgets (No baseline changes)
 * - Mounts read-only KPIs, Radar (exceptions), Double-Donut (planned vs applied), and 3 exception cards
 * - Uses existing global state & helpers: state, weekEndISO, ymdFromCompletedAtInTZ, todayInTZ, mondayOfInTZ, toNum, aggregate, joinPOProgress
 * - No edits to existing routes/exports/logic; safe to include after current script.
 */
(function ExecLiveAdditive(){
  const BRAND = (typeof window.BRAND !== 'undefined') ? window.BRAND : '#990033';
  const BUSINESS_TZ = document.querySelector('meta[name="business-tz"]')?.content || 'Asia/Shanghai';

// --- Exec should not fetch; rely on Ops-populated window.state
const EXEC_USE_NETWORK = false;

// --- API base (normalized; always ends with /api)
const _rawBase =
  document.querySelector('meta[name="api-base"]')?.content
  || (typeof window.API_BASE !== 'undefined' ? window.API_BASE : location.origin);

const API_BASE = (() => {
  const b = String(_rawBase || '').replace(/\/+$/, ''); // strip trailing /
  if (/\/api$/i.test(b)) return b;                      // already ends with /api
  return b ? (b + '/api') : '/api';                     // append /api or default
})();

const g = (path) =>
  fetch(`${API_BASE}/${path}`, { headers: { 'Content-Type': 'application/json' } })
    .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)));

// --- Robust fetchers with endpoint fallback -------------------
async function tryFetchJson(urls) {
  for (const u of urls) {
    try {
      const r = await fetch(u, { headers: { 'Content-Type': 'application/json' } });
      if (r.ok) return r.json();
    } catch (_) {}
  }
  throw new Error('All endpoints failed: ' + urls.join(' | '));
}

function trimBase(base) { return String(base || '').replace(/\/+$/,''); }

async function fetchPlanForWeek(ws) {
  const base = trimBase(API_BASE);
return tryFetchJson([
  // API_BASE already includes /api
  `${base}/plan?weekStart=${ws}`,
  // fallback to non-/api base if the alias isn’t present
  `${base.replace(/\/api$/,'')}/plan/weeks/${ws}`
]);

}

async function fetchBinsForWeek(ws) {
  const base = trimBase(API_BASE);
return tryFetchJson([
  `${base}/bins?weekStart=${ws}`,
  `${base.replace(/\/api$/,'')}/bins/weeks/${ws}`
]);

}

async function fetchRecordsForWeek(ws, we) {
  const base = trimBase(API_BASE);
return tryFetchJson([
  `${base}/records?from=${ws}&to=${we}&status=complete`,
  `${base.replace(/\/api$/,'')}/records?from=${ws}&to=${we}&status=complete`
]);

}



 // --- Alias real app state if it lives somewhere else ---
  const _maybeState =
    window.state ||
    window.app?.state ||
    window.pinpoint?.state ||
    window.vo?.state ||
    window.store?.state ||
    window.__APP__?.state;

  if (_maybeState && !window.state) window.state = _maybeState;

  // ---------- Small utilities ----------
  const $ = (s)=>document.querySelector(s);
  const fmt = (n)=> Number(n||0).toLocaleString();
  const clamp = (v,min,max)=> Math.max(min, Math.min(max, v));
  const pct = (num,den)=> den>0 ? Math.round((num*100)/den) : 0;
  const weekEndISO = window.weekEndISO || function(ws){ const d=new Date(ws); d.setDate(d.getDate()+6); return toISODate(d); };
  function toISODate(v){
    if (typeof window.toISODate === 'function') return window.toISODate(v);
    const d = new Date(v); const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  }


// --- replace bizYMDFromRecord with this ---
function bizYMDFromRecord(r){
  if (r?.date_local || r?.date) {
    const raw = r.date_local || r.date;
    const s = String(raw).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    if (/^\d{2}-\d{2}-\d{4}$/.test(s)) { const [d,m,y]=s.split('-'); return `${y}-${m}-${d}`; }
  }
  if (r?.completed_at && typeof window.ymdFromCompletedAtInTZ === 'function') {
    return window.ymdFromCompletedAtInTZ(r.completed_at, BUSINESS_TZ);
  }
  return '';
}

// ---------- Metric computations (scoped to selected week) ----------
function computeExecMetrics() {
  const ws = window.state?.weekStart; 
  if (!ws) return null;

  const we = (window.weekEndISO || function (ws) {
    const d = new Date(ws);
    d.setDate(d.getDate() + 6);
    const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), day = String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  })(ws);

  const plan       = Array.isArray(window.state?.plan)    ? window.state.plan    : [];
  const recordsAll = Array.isArray(window.state?.records) ? window.state.records : [];
  const bins       = Array.isArray(window.state?.bins)    ? window.state.bins    : [];

  // Week records = in-range & "looks done"
  const wkRecords = recordsAll.filter(r => {
    const ymd = bizYMDFromRecord(r);
    if (!(ymd && ymd >= ws && ymd <= we)) return false;
    const st = String(r?.status || '').toLowerCase();
    return (st === 'complete' || st === 'applied' || !!r.uid || Number(r.qty || r.quantity || 0) > 0);
  });

  // Planned total (sum plan.target_qty)
  const plannedTotal = plan.reduce((s, p) => s + (window.toNum ? toNum(p.target_qty) : Number(p.target_qty || 0)), 0);

  // Applied total (sum qty/quantity if present, else 1 per record)
  const appliedTotal = wkRecords.reduce((s, r) => {
    const q = Number(r.qty ?? r.quantity ?? 1);
    return s + (Number.isFinite(q) && q > 0 ? q : 0);
  }, 0);

  const pct = (num, den) => den > 0 ? Math.round((num * 100) / den) : (num > 0 ? 100 : 0);
  const completionPct = pct(appliedTotal, plannedTotal);

  // Aggregates (if helper exists)
  const agg = (typeof window.aggregate === 'function') ? window.aggregate(wkRecords) : { byPO:new Map(), bySKU:new Map() };

  // Discrepancy % (SKU)
  const planBySKU = new Map();
  for (const p of plan) {
    const sku = String(p.sku_code || '').trim();
    if (!sku) continue;
    planBySKU.set(sku, (planBySKU.get(sku) || 0) + (window.toNum ? toNum(p.target_qty) : Number(p.target_qty || 0)));
  }
  let skuPctSum = 0, skuCnt = 0;
  for (const [sku, planned] of planBySKU.entries()) {
    const applied = agg.bySKU.get(sku) || 0;
    if (planned > 0) { skuPctSum += Math.abs(applied - planned) / planned; skuCnt++; }
  }
  const avgSkuDiscPct = Math.round((skuCnt ? (skuPctSum / skuCnt) : 0) * 100);

  // Discrepancy % (PO) + earliest due per PO
  const planByPO = new Map();
  const poDue = new Map();
  for (const p of plan) {
    const po = String(p.po_number || '').trim(); 
    if (!po) continue;
    planByPO.set(po, (planByPO.get(po) || 0) + (window.toNum ? toNum(p.target_qty) : Number(p.target_qty || 0)));
    const d = String(p.due_date || '').trim();
    if (!poDue.has(po)) poDue.set(po, d); else if (d && (!poDue.get(po) || d < poDue.get(po))) poDue.set(po, d);
  }
  let poPctSum = 0, poCnt = 0;
  for (const [po, planned] of planByPO.entries()) {
    const applied = agg.byPO.get(po) || 0;
    if (planned > 0) { poPctSum += Math.abs(applied - planned) / planned; poCnt++; }
  }
  const avgPoDiscPct = Math.round((poCnt ? (poPctSum / poCnt) : 0) * 100);

  // Duplicate UIDs (same SKU+UID >1)
  const pairCounts = new Map();
  for (const r of wkRecords) {
    const sku = String(r.sku_code || '').trim();
    const uid = String(r.uid || '').trim();
    if (!sku || !uid) continue;
    const k = `${sku}||${uid}`;
    pairCounts.set(k, (pairCounts.get(k) || 0) + 1);
  }
  let dupScanCount = 0;
  for (const c of pairCounts.values()) if (c > 1) dupScanCount += c;

  // Heavy bins + diversity
  const heavyBins  = (bins || []).filter(b => Number(b.weight_kg || 0) > 12);
  const heavyBinSet = new Set(heavyBins.map(b => String(b.mobile_bin || '').trim()).filter(Boolean));
  const heavyCount  = heavyBinSet.size;
  const skuByBin = new Map();
  for (const r of wkRecords) {
    const bin = String(r.mobile_bin || '').trim();
    const sku = String(r.sku_code || '').trim();
    if (!bin || !sku) continue;
    if (!skuByBin.has(bin)) skuByBin.set(bin, new Set());
    skuByBin.get(bin).add(sku);
  }
  let diversitySum = 0, diversityN = 0;
  for (const bin of heavyBinSet) { diversitySum += (skuByBin.get(bin)?.size || 0); diversityN++; }
  const avgDiversityHeavy = diversityN ? (diversitySum / diversityN) : 0;

  // Late appliers
  let lateCount = 0;
  for (const r of wkRecords) {
    const po = String(r.po_number || '').trim(); if (!po) continue;
    const due = poDue.get(po); if (!due) continue;
    const ymd = bizYMDFromRecord(r); if (!ymd) continue;
    if (ymd > due) lateCount++;
  }
  const lateRatePct = pct(lateCount, appliedTotal);

  // Pareto: gaps by PO×SKU
  const appliedPOSKU = new Map();
  for (const r of wkRecords) {
    const po  = String(r.po_number || '').trim();
    const sku = String(r.sku_code  || '').trim();
    if (!po || !sku) continue;
    const k = `${po}|||${sku}`;
    appliedPOSKU.set(k, (appliedPOSKU.get(k) || 0) + 1);
  }
  const plannedPOSKU = new Map();
  for (const p of plan) {
    const po  = String(p.po_number || '').trim();
    const sku = String(p.sku_code  || '').trim();
    if (!po || !sku) continue;
    const k = `${po}|||${sku}`;
    plannedPOSKU.set(k, (plannedPOSKU.get(k) || 0) + (window.toNum ? toNum(p.target_qty) : Number(p.target_qty || 0)));
  }
  const gaps = [];
  for (const [k, planned] of plannedPOSKU.entries()) {
    const applied = appliedPOSKU.get(k) || 0;
    const gap = planned - applied;
    if (gap !== 0) gaps.push({ k, gap, planned, applied });
  }
  gaps.sort((a,b) => Math.abs(b.gap) - Math.abs(a.gap));
  const topGap = gaps.slice(0, 5).map(g => {
    const [po, sku] = g.k.split('|||');
    return { po, sku, gap: g.gap, planned: g.planned, applied: g.applied };
  });

  return {
    ws, we,
    plannedTotal, appliedTotal, completionPct,
    avgSkuDiscPct, avgPoDiscPct,
    dupScanCount,
    heavyCount, avgDiversityHeavy,
    lateCount, lateRatePct,
    topGap
  };
}


  // ---------- SVG renderers ----------
  function radarSVG({axes, values, size=260}){
    const cx=size/2, cy=size/2, r=size*0.38; const n=axes.length; const toRad=(deg)=> (deg*Math.PI/180);
    const pts=[]; const gridSteps=4; // 25%, 50%, 75%, 100%
    for (let i=0;i<n;i++){ const angle = toRad(-90 + (360*i/n)); const vr = r * clamp(values[i], 0, 100)/100; const x = cx + vr*Math.cos(angle); const y = cy + vr*Math.sin(angle); pts.push([x,y]); }
    const poly = pts.map(p=> p.join(',')).join(' ');
    let grid='';
    for (let s=1; s<=gridSteps; s++){
      const rr = (r*s)/gridSteps; const ring=[];
      for (let i=0;i<n;i++){ const angle = toRad(-90 + (360*i/n)); ring.push((cx + rr*Math.cos(angle))+','+(cy + rr*Math.sin(angle))); }
      grid += `<polygon points="${ring.join(' ')}" fill="none" stroke="#e5e7eb" stroke-width="1" />`;
    }
    let spokes='';
    for (let i=0;i<n;i++){ const a=toRad(-90 + (360*i/n)); const x=cx + r*Math.cos(a), y=cy + r*Math.sin(a); spokes += `<line x1="${cx}" y1="${cy}" x2="${x}" y2="${y}" stroke="#e5e7eb" stroke-width="1" />`; }
    const labels = axes.map((t,i)=>{ const a=toRad(-90 + (360*i/n)); const lx=cx + (r+16)*Math.cos(a); const ly=cy + (r+16)*Math.sin(a); return `<text x="${lx}" y="${ly}" font-size="11" text-anchor="${Math.cos(a)>0? 'start':'end'}" dominant-baseline="middle" fill="#374151">${t}</text>`; }).join('');
    return `<svg viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" role="img" aria-label="Exceptions radar">
      <g>${grid}${spokes}</g>
      <polygon points="${poly}" fill="${BRAND}20" stroke="${BRAND}" stroke-width="2" />
      ${labels}
    </svg>`;
  }

  function donutDoubleSVG(planned, applied, size=260){
    const cx=size/2, cy=size/2, r1=size*0.40, r2=size*0.30; // outer planned, inner applied
    const tau = Math.PI*2;
    function arc(pct,r){ const a = clamp(pct,0,100)*tau/100; const x = cx + r*Math.sin(a); const y = cy - r*Math.cos(a); const large = a>Math.PI?1:0; return `M ${cx} ${cy-r} A ${r} ${r} 0 ${large} 1 ${x} ${y}`; }
    const comp = pct(applied, planned);
    return `<svg viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" role="img" aria-label="Planned vs applied">
      <circle cx="${cx}" cy="${cy}" r="${r1}" fill="none" stroke="#e5e7eb" stroke-width="16"/>
      <path d="${arc(100, r1)}" stroke="#e5e7eb" stroke-width="16" fill="none" />
      <path d="${arc( clamp( planned>0?100:0,0,100 ), r1)}" stroke="#cbd5e1" stroke-width="16" fill="none" />
      <circle cx="${cx}" cy="${cy}" r="${r2}" fill="none" stroke="#f1f5f9" stroke-width="16"/>
      <path d="${arc( clamp( planned>0? (applied*100/planned):0, 0, 100 ), r2)}" stroke="${BRAND}" stroke-width="16" fill="none" />
      <text x="${cx}" y="${cy-6}" text-anchor="middle" font-size="22" font-weight="700" fill="#111827">${fmt(applied)}</text>
      <text x="${cx}" y="${cy+16}" text-anchor="middle" font-size="12" fill="#6b7280">of ${fmt(planned)} • ${comp}%</text>
    </svg>`;
  }

  function sparklineSVG(values, width=280, height=60){
    if (!values.length) return `<svg width="${width}" height="${height}"></svg>`;
    const max = Math.max(...values, 1), min = Math.min(...values, 0);
    const step = width / Math.max(1, values.length-1);
    const pts = values.map((v,i)=>{
      const x = i*step; const y = height - ((v - min) / Math.max(1, max-min)) * (height-12) - 6;
      return `${x},${y}`;
    }).join(' ');
    return `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">
      <polyline points="${pts}" fill="none" stroke="${BRAND}" stroke-width="2" />
    </svg>`;
  }

// ===== SVG Helpers (no libs) =====
const GREY  = '#e5e7eb';
const GREY_STROKE = '#d1d5db';

function _el(tag, attrs={}, children=[]) {
  const e = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  children.forEach(c => e.appendChild(c));
  return e;
}

function renderDonutWithBaseline(slot, planned, applied, opts = {}) {
  slot.innerHTML = '';

  // size derived from slot/card, with safe bounds
  const auto = Math.min(
    slot.clientWidth || 9999,
    (slot.parentElement?.clientHeight || 9999)
  ) * 0.75;

  const size = Math.max(140, Math.min(opts.size ?? auto || 180, 240));
  const r    = Math.round(size * 0.41);
  const cx = size / 2, cy = size / 2;
  const CIRC = 2 * Math.PI * r;

  const svg = _el('svg', { viewBox: `0 0 ${size} ${size}`, width: size, height: size, style: 'display:block' });
  
  // baseline ring
  svg.appendChild(_el('circle', {
    cx, cy, r,
    fill: 'none',
    stroke: GREY,
    'stroke-width': 16
  }));

  // applied ring
  const appliedArc = _el('circle', {
    cx, cy, r,
    fill: 'none',
    stroke: BRAND,
    'stroke-width': 16,
    'stroke-linecap': 'round',
    transform: `rotate(-90 ${cx} ${cy})`,
    'stroke-dasharray': CIRC,
    'stroke-dashoffset': CIRC
  });
  svg.appendChild(appliedArc);
  slot.appendChild(svg);

  // fixed % label in the center
  const pctText = document.createElement('div');
  pctText.className = 'absolute inset-0 flex items-center justify-center text-sm text-gray-600';
  pctText.style.pointerEvents = 'none';
  slot.style.position = 'relative';
  slot.appendChild(pctText);

  // % computation (robust when planned <= 0)
  let p;
  if (!Number.isFinite(planned) || planned <= 0) {
    p = (Number(applied) > 0) ? 1 : 0;
  } else {
    p = Math.max(0, Math.min(1, Number(applied || 0) / Number(planned)));
  }

// set once, no animation
const dash = CIRC * (1 - p);
appliedArc.style.transition = 'none';
appliedArc.setAttribute('stroke-dashoffset', dash);
pctText.textContent = Math.round(p * 100) + '%';

}

function renderRadarWithBaseline(slot, labels, baselineValues, actualValues, opts = {}) {
  slot.innerHTML = '';

  const N = labels.length;
  const displaySize = opts.size ?? 260;     // smaller chart
  const vbPad  = 56;                        // extra bleed for labels
  const vbSize = displaySize + vbPad * 2;
  const pad = 18;
  const cx  = displaySize / 2, cy = displaySize / 2;
  const R   = (displaySize / 2) - pad;

  const svg = _el('svg', {
    viewBox: `${-vbPad} ${-vbPad} ${vbSize} ${vbSize}`,
    width: displaySize, height: displaySize, style: 'display:block'
  });

  const pt = (i, v) => {
    const ang = (-Math.PI / 2) + (i * 2 * Math.PI / N);
    const r = (Math.max(0, Math.min(100, v)) / 100) * R;
    return [cx + r * Math.cos(ang), cy + r * Math.sin(ang)];
  };
  const poly = vals => vals.map((v, i) => pt(i, v)).map(([x, y]) => `${x},${y}`).join(' ');

  // grid
  [20, 40, 60, 80, 100].forEach(t => {
    svg.appendChild(_el('circle', { cx, cy, r: (t / 100) * R, fill: 'none', stroke: GREY_STROKE, 'stroke-width': 1 }));
  });
  for (let i = 0; i < N; i++) {
    const [x, y] = pt(i, 100);
    svg.appendChild(_el('line', { x1: cx, y1: cy, x2: x, y2: y, stroke: GREY_STROKE, 'stroke-width': 1 }));
  }

  // label/value placement (more space)
  const labelR = R + 34;
  const valueR = R + 18;

  for (let i = 0; i < N; i++) {
    const ang = (-Math.PI / 2) + (i * 2 * Math.PI / N);
    const lx = cx + labelR * Math.cos(ang);
    const ly = cy + labelR * Math.sin(ang);
    const vx = cx + valueR * Math.cos(ang);
    const vy = cy + valueR * Math.sin(ang);

    const t = _el('text', {
      x: lx, y: ly, 'text-anchor': 'middle', 'dominant-baseline': 'middle',
      'font-size': '14', 'font-weight': '600', fill: '#374151'
    });
    t.textContent = labels[i];
    svg.appendChild(t);

    if (actualValues && actualValues.length === N) {
      const v = _el('text', {
        x: vx, y: vy, 'text-anchor': 'middle', 'dominant-baseline': 'middle',
        'font-size': '12', 'font-weight': '600', fill: BRAND
      });
      v.textContent = Math.round(actualValues[i]) + '';
      svg.appendChild(v);
    }
  }

  // polygons
  svg.appendChild(_el('polygon', {
    points: poly(baselineValues),
    fill: GREY, 'fill-opacity': 0.35, stroke: GREY_STROKE, 'stroke-width': 1
  }));
  if (actualValues && actualValues.length === N) {
    svg.appendChild(_el('polygon', {
      points: poly(actualValues),
      fill: BRAND, 'fill-opacity': 0.15, stroke: BRAND, 'stroke-width': 2
    }));
  }

  slot.appendChild(svg);
}


  // ---------- Cards render ----------
  function renderExec(){
    const host = $('#page-exec'); if (!host) return;
    if (!host.querySelector('#exec-live')){
  const wrap = document.createElement('div'); 
  wrap.id = 'exec-live';
  wrap.innerHTML = `
    <div class="grid grid-cols-1 gap-3">
      <!-- Tiles -->
      <div id="exec-tiles" class="grid grid-cols-2 sm:grid-cols-6 gap-3"></div>

      <!-- Charts row -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">

        <!-- Radar -->
        <div class="bg-white rounded-2xl border shadow p-3 flex flex-col max-h-[360px]" id="card-radar">
          <div class="text-base font-semibold">Exceptions Radar</div>
          <div class="text-xs text-gray-500">Risk-normalized (0–100)</div>
          <div class="flex-1 min-h-[220px] flex items-center justify-center">
            <div id="radar-slot"></div>
          </div>
          <div id="radar-note" class="mt-2 text-xs text-gray-500"></div>
        </div>

        <!-- Donut -->
        <div class="bg-white rounded-2xl border shadow p-3 flex flex-col max-h-[360px]" id="card-donut">
          <div class="flex items-baseline justify-between">
            <div>
              <div class="text-base font-semibold leading-tight">Planned vs Applied</div>
              <div class="text-xs text-gray-500">Week scope (business TZ)</div>
            </div>
            <div id="donut-stats" class="text-sm font-semibold text-gray-700"></div>
          </div>
          <div class="flex-1 min-h-[220px] flex items-center justify-center">
            <div id="donut-slot"></div>
          </div>
        </div>

      </div>

      <!-- Exception widgets -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
        <div class="bg-white rounded-2xl border shadow p-4" id="card-pareto">
          <div class="text-base font-semibold mb-1">Top Gap Drivers (PO × SKU)</div>
          <div class="text-xs text-gray-500 mb-2">Planned − Applied</div>
          <div id="pareto-list" class="space-y-2"></div>
        </div>

        <div class="bg-white rounded-2xl border shadow p-4" id="card-heavy">
          <div class="text-base font-semibold mb-1">Heavy Bins Snapshot</div>
          <div class="text-xs text-gray-500 mb-2">weight_kg > 12</div>
          <table class="w-full text-sm"><thead class="text-gray-500">
            <tr><th class="text-left py-1 pr-2">Bin</th><th class="text-right py-1 pr-2">Units</th><th class="text-right py-1 pr-2">kg</th><th class="text-right py-1">SKU div.</th></tr>
          </thead><tbody id="heavy-body"><tr><td colspan="4" class="text-xs text-gray-400 text-center py-3">Loading…</td></tr></tbody></table>
          <div id="heavy-foot" class="text-xs text-gray-500 mt-2"></div>
        </div>

        <div class="bg-white rounded-2xl border shadow p-4" id="card-anom">
          <div class="text-base font-semibold mb-1">Intake Anomalies</div>
          <div class="text-xs text-gray-500 mb-2">Daily/Hourly sparkline</div>
          <div id="anom-spark"></div>
          <div id="anom-badges" class="mt-2 text-xs text-gray-600"></div>
        </div>
      </div>
    </div>`;
  host.appendChild(wrap);

  // paint ghost charts immediately
  renderDonutWithBaseline(document.getElementById('donut-slot'), 0, 0);
  renderRadarWithBaseline(
    document.getElementById('radar-slot'),
    ['Dup UIDs','Avg SKU %Δ','Avg PO %Δ','Heavy bins','Diversity (heavy)','Late appliers %'],
    [55,50,45,60,50,40],
    null,
    { size: 260 }
  );}

// ---- DEBUG: Exec data snapshot (runs once) ----
if (!window.__execDebugOnce) {
  window.__execDebugOnce = true;
  (function () {
    const out = {};
    out.hash = location.hash;
    out.execContainer = !!document.querySelector('#page-exec');
    out.execLive = !!document.querySelector('#exec-live');

    const s = window.state || {};
    out.stateKeys = Object.keys(s);
    out.weekStart = s.weekStart;
    out.planCount = Array.isArray(s.plan) ? s.plan.length : -1;
    out.recordsCount = Array.isArray(s.records) ? s.records.length : -1;
    out.binsCount = Array.isArray(s.bins) ? s.bins.length : -1;

    const plan = Array.isArray(s.plan) ? s.plan : [];
    out.planSample = plan.slice(0, 3).map(p => ({
      po: p.po_number, sku: p.sku_code, target_qty: p.target_qty, due_date: p.due_date
    }));

    const BUSINESS_TZ = document.querySelector('meta[name="business-tz"]')?.content || 'Asia/Shanghai';
    const toISODate = v => { const d=new Date(v); const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), day=String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${day}`; };
    const weekEndISO = ws => { const d=new Date(ws); d.setDate(d.getDate()+6); return toISODate(d); };
    const bizYMDFromRecord = r => r?.date_local ? String(r.date_local).trim()
      : (r?.completed_at && typeof window.ymdFromCompletedAtInTZ==='function'
          ? window.ymdFromCompletedAtInTZ(r.completed_at, BUSINESS_TZ) : '');

    if (s.weekStart) {
      const ws = s.weekStart, we = weekEndISO(ws);
      out.weekEnd = we;
      const wk = (Array.isArray(s.records)? s.records:[]).filter(r=>{
        if (r?.status !== 'complete') return false;
        const ymd = bizYMDFromRecord(r);
        return ymd && ymd >= ws && ymd <= we;
      });
      out.wkRecordsCount = wk.length;
      out.wkRecordDatesSample = (s.records||[]).slice(0,5).map(r=>({
        uid:r.uid, status:r.status, date_local:r.date_local, completed_at:r.completed_at, ymd: bizYMDFromRecord(r)
      }));
    }

    const toNum = window.toNum || (x => Number(String(x||0).replace(/[, ]/g,'')));
    out.plannedTotal = plan.reduce((sum,p)=> sum + toNum(p.target_qty), 0);

    console.log('[Exec DEBUG] Snapshot below:');
    console.table(out.planSample);
    console.table(out.wkRecordDatesSample || []);
    console.log(out);
  })();
}



    const m = computeExecMetrics(); if (!m) return;

const donutStatsEl = document.getElementById('donut-stats');
if (donutStatsEl) {
  donutStatsEl.textContent = `Planned ${fmt(m.plannedTotal)} · Applied ${fmt(m.appliedTotal)}`;
}


// compute sizes from actual slots
const donutSlot = document.getElementById('donut-slot');
const radarSlot = document.getElementById('radar-slot');

const donutSize = Math.floor(
  Math.min(donutSlot.clientWidth || 240, (donutSlot.parentElement?.clientHeight || 280)) * 0.75
);
const radarSize = Math.floor(
  Math.min(radarSlot.clientWidth || 300, (radarSlot.parentElement?.clientHeight || 320)) * 0.90
);


    // Tiles
    const tiles = [
      {label:'Completion %', value: `${m.completionPct}%`},
      {label:'Duplicate UIDs', value: fmt(m.dupScanCount)},
      {label:'Avg SKU %Δ', value: `${m.avgSkuDiscPct}%`},
      {label:'Avg PO %Δ', value: `${m.avgPoDiscPct}%`},
      {label:'Heavy bins >12kg', value: fmt(m.heavyCount)},
      {label:'Late appliers', value: `${fmt(m.lateCount)} (${m.lateRatePct}%)`},
    ];
    const tilesWrap = $('#exec-tiles');
    tilesWrap.innerHTML = tiles.map(t=>
      `<div class="bg-white rounded-2xl border shadow p-4">
        <div class="text-xs text-gray-500">${t.label}</div>
        <div class="text-xl font-bold tabular-nums">${t.value}</div>
      </div>`
    ).join('');

    // Radar (normalize to risk index)
    const targets = {
      dup: 0, // any >0 is risk
      skuDisc: 5, // %
      poDisc: 5,  // %
      heavy: 3,   // count
      diversity: 4, // desired >=4 (invert)
      lateRate: 5 // %
    };
    const axes = ['Dup UIDs','Avg SKU %Δ','Avg PO %Δ','Heavy bins','Diversity (heavy)','Late appliers %'];
    const values = [
      clamp(m.dupScanCount>0 ? 100 : 0, 0, 100),
      clamp((m.avgSkuDiscPct/targets.skuDisc)*100, 0, 100),
      clamp((m.avgPoDiscPct/targets.poDisc)*100, 0, 100),
      clamp((m.heavyCount/targets.heavy)*100, 0, 100),
      clamp(((targets.diversity>0? (Math.max(0, targets.diversity - m.avgDiversityHeavy)/targets.diversity):0)*100), 0, 100),
      clamp((m.lateRatePct/targets.lateRate)*100, 0, 100)
    ];
        const topIdx = values.reduce((bi, v,i)=> v>values[bi]? i:bi, 0);
    $('#radar-note').textContent = `Top driver: ${axes[topIdx]} (${Math.round(values[topIdx])})`;

        // Pareto list
    const list = m.topGap.map(row=>{
      const sign = row.gap>0? '+' : ''; const color = row.gap>0? 'text-rose-600' : 'text-green-600';
      return `<div class="flex items-center justify-between text-sm">
        <div class="truncate"><span class="text-gray-500">${row.po}</span> · <span>${row.sku}</span></div>
        <div class="tabular-nums ${color}">${sign}${fmt(row.gap)}</div>
      </div>`;
    }).join('') || '<div class="text-xs text-gray-400">No gaps.</div>';
    $('#pareto-list').innerHTML = list;

    // Heavy bins tablelet
    const bins = Array.isArray(window.state?.bins) ? window.state.bins : [];
    const heavy = bins.filter(b=> Number(b.weight_kg||0) > 12);
    const ws = m.ws, we = m.we;
    // Build units & diversity from week records
    const wkRecords = (window.state?.records||[]).filter(r=>{
  const ymd = bizYMDFromRecord(r);
  if (!(ymd && ymd >= ws && ymd <= we)) return false;
  const st = String(r?.status || '').toLowerCase();
  return st === 'complete' || st === 'applied' || !!r.uid;
});
    const unitsByBin = new Map(); const skuSetByBin = new Map();
    for (const r of wkRecords){ const bin = String(r.mobile_bin||'').trim(); const sku = String(r.sku_code||'').trim(); if(!bin) continue; unitsByBin.set(bin,(unitsByBin.get(bin)||0)+1); if(sku){ if(!skuSetByBin.has(bin)) skuSetByBin.set(bin,new Set()); skuSetByBin.get(bin).add(sku); } }
    const rows = heavy.slice(0,3).map(b=>{
      const id = String(b.mobile_bin||'').trim();
      return `<tr class="odd:bg-gray-50">
        <td class="py-1 pr-2">${id||'—'}</td>
        <td class="py-1 pr-2 text-right tabular-nums">${fmt(unitsByBin.get(id)||0)}</td>
        <td class="py-1 pr-2 text-right tabular-nums">${Number(b.weight_kg||0).toFixed(1)}</td>
        <td class="py-1 text-right tabular-nums">${fmt(skuSetByBin.get(id)?.size||0)}</td>
      </tr>`;
    }).join('');
    $('#heavy-body').innerHTML = rows || `<tr><td colspan="4" class="text-xs text-gray-400 text-center py-3">No heavy bins this week.</td></tr>`;
    const avgDiv = m.avgDiversityHeavy ? m.avgDiversityHeavy.toFixed(2) : '0.00';
    $('#heavy-foot').textContent = `Heavy bins: ${fmt(m.heavyCount)} · Avg diversity: ${avgDiv}`;

    // Anomaly sparkline — simple daily buckets
    const dayLabels=[]; const counts=[];
    const start = new Date(ws+'T00:00:00');
    for (let i=0;i<7;i++){ const d=new Date(start); d.setDate(d.getDate()+i); const ymd=toISODate(d); dayLabels.push(ymd); counts.push(0); }
    for (const r of wkRecords){ const ymd=bizYMDFromRecord(r); const idx=dayLabels.indexOf(ymd); if (idx>=0) counts[idx]++; }
    $('#anom-spark').innerHTML = sparklineSVG(counts);
    // basic z-score flags
    const mean = counts.reduce((s,v)=>s+v,0)/Math.max(1,counts.length);
    const std = Math.sqrt(counts.reduce((s,v)=> s + Math.pow(v-mean,2),0)/Math.max(1,counts.length));
    let dips=0, spikes=0; const lo=mean - 1.5*std, hi=mean + 1.5*std;
    for (const v of counts){ if (v < lo) dips++; else if (v > hi) spikes++; }
    $('#anom-badges').textContent = `Dips: ${dips} · Spikes: ${spikes}`;

renderDonutWithBaseline(donutSlot, m.plannedTotal, m.appliedTotal, { size: donutSize });
renderRadarWithBaseline(radarSlot, axes, [55,50,45,60,50,40], values, {
  size: Math.max(220, Math.min(radarSize, 300))
});

}

  // Render when Exec page is shown
  function onHash(){
    const hash = location.hash || '#dashboard';
    if (hash === '#exec') renderExec();
  }
  window.addEventListener('hashchange', onHash);
  // Also render once if Exec is already selected
  if ((location.hash||'#dashboard') === '#exec') setTimeout(renderExec, 0);
  // Re-render on week change from existing app
  const _oldSetWeek = window.setWeek;
  if (typeof _oldSetWeek === 'function'){
    window.setWeek = async function(ws){
      const r = await _oldSetWeek.apply(this, arguments);
      if ((location.hash||'#dashboard') === '#exec') setTimeout(renderExec, 0);
      return r;
    };
  }

async function _execLoadWeek(ws) {
  const s = window.state || (window.state = {});
  if (ws) s.weekStart = ws;
  // no fetch here on Exec
}

// Ensure state is populated for the selected week if Exec needs to fetch
async function execEnsureStateLoaded(ws) {
  const s = window.state || (window.state = {});
  const we = (window.weekEndISO || (w => { const d=new Date(w); d.setDate(d.getDate()+6); return d.toISOString().slice(0,10); }))(ws);

  const needPlan    = !Array.isArray(s.plan)    || s.plan.length === 0;
  const needRecords = !Array.isArray(s.records) || s.records.length === 0;
  const needBins    = !Array.isArray(s.bins);

  const [plan, records, bins] = await Promise.all([
    needPlan    ? fetchPlanForWeek(ws).catch(()=>[])         : Promise.resolve(s.plan),
    needRecords ? fetchRecordsForWeek(ws, we).then(x => x.records || []).catch(()=>[]) : Promise.resolve(s.records),
    needBins    ? fetchBinsForWeek(ws).catch(()=>[])         : Promise.resolve(s.bins),
  ]);

  s.plan    = Array.isArray(plan)    ? plan    : (s.plan || []);
  s.records = Array.isArray(records) ? records : (s.records || []);
  s.bins    = Array.isArray(bins)    ? bins    : (s.bins || []);
}



async function _execEnsureStateLoaded(ws) {
  const s = window.state || (window.state = {});
  if (ws) s.weekStart = ws;

  // compute weekEnd for records
  const we = (window.weekEndISO || (w => { const d=new Date(w); d.setDate(d.getDate()+6); return d.toISOString().slice(0,10); }))(s.weekStart);

  const needPlan    = !Array.isArray(s.plan)    || s.plan.length === 0;
  const needRecords = !Array.isArray(s.records) || s.records.length === 0;
  const needBins    = !Array.isArray(s.bins)    || s.bins.length === 0;

  if (!(needPlan || needRecords || needBins)) return;

  const [plan, records, bins] = await Promise.all([
    needPlan    ? fetchPlanForWeek(s.weekStart).catch(() => [])                                : Promise.resolve(s.plan),
    needRecords ? fetchRecordsForWeek(s.weekStart, we).then(x => x.records || []).catch(() => []) : Promise.resolve(s.records),
    needBins    ? fetchBinsForWeek(s.weekStart).catch(() => [])                                : Promise.resolve(s.bins),
  ]);

  s.plan    = Array.isArray(plan)    ? plan    : (s.plan || []);
  s.records = Array.isArray(records) ? records : (s.records || []);
  s.bins    = Array.isArray(bins)    ? bins    : (s.bins || []);
}


let _execBootTimer = null;


// --- Fallback: if Ops hasn't hydrated window.state on Exec, load minimal data once
async function __execEnsureStateLoaded() {
  const s = window.state || (window.state = {});

  // Ensure we at least have a weekStart
  if (!s.weekStart) {
    try {
      if (typeof window.todayInTZ === 'function' && typeof window.mondayOfInTZ === 'function') {
        const today = window.todayInTZ(BUSINESS_TZ);
        s.weekStart = window.mondayOfInTZ(today);
      } else {
        // plain Monday-of-week fallback
        const d = new Date();
        const day = (d.getDay() + 6) % 7; // 0 = Monday
        d.setHours(0,0,0,0);
        d.setDate(d.getDate() - day);
        s.weekStart = toISODate(d);
      }
    } catch {}
  }
  if (!s.weekStart) return; // nothing to do

  const ws = s.weekStart;
  const we = (window.weekEndISO || function (w) { const d=new Date(w); d.setDate(d.getDate()+6); return toISODate(d); })(ws);

  const hasPlan = Array.isArray(s.plan)    && s.plan.length > 0;
  const hasRecs = Array.isArray(s.records) && s.records.length > 0;
  const hasBins = Array.isArray(s.bins)    && s.bins.length > 0;
  if (hasPlan || hasRecs || hasBins) return; // already hydrated by Ops

  try {
    // Minimal endpoints: adjust the paths if your server uses different names
    const [plan, records, bins] = await Promise.all([
      g(`plan?weekStart=${ws}&weekEnd=${we}`),
      g(`records?weekStart=${ws}&weekEnd=${we}`),
      g(`bins?weekStart=${ws}&weekEnd=${we}`),
    ]);

    s.plan    = Array.isArray(plan)    ? plan    : [];
    s.records = Array.isArray(records) ? records : [];
    s.bins    = Array.isArray(bins)    ? bins    : [];

    // Let Exec render now, and also allow Ops to overwrite later if it wants
    window.dispatchEvent(new Event('state:ready'));
  } catch (e) {
    console.warn('[Exec fallback] fetch failed:', e);
  }
}


async function _execTryRender() {
  if (location.hash !== '#exec') return;

  const s = window.state || (window.state = {});

  // Keep the week Ops chose; only derive if still missing
  if (!s.weekStart && typeof window.todayInTZ === 'function' && typeof window.mondayOfInTZ === 'function') {
    try {
      const today = window.todayInTZ(BUSINESS_TZ);
      s.weekStart = window.mondayOfInTZ(today);
    } catch {}
  }
  if (!s.weekStart) return;


  // Ensure data exists at least once (fallback fetch), then continue
await _execEnsureStateLoaded(s.weekStart);

  const hasPlan = Array.isArray(s.plan) && s.plan.length > 0;
  const hasRecs = Array.isArray(s.records) && s.records.length > 0;

  if (!(hasPlan || hasRecs)) {
    // Fetch what we’re missing for this week, then render
    execEnsureStateLoaded(s.weekStart)
      .then(() => { try { renderExec(); } catch (e) { console.error('[Exec render error]', e); } })
      .catch(e  => console.error('[Exec fetch error]', e));
    return;
  }

  try { renderExec(); } catch (e) { console.error('[Exec render error]', e); }
}


// 🔹 NEW: re-render exactly when the app signals data is ready
window.addEventListener('state:ready', _execTryRender);


  _execBootTimer = setInterval(_execTryRender, 600);
  document.addEventListener('visibilitychange', _execTryRender);
  setTimeout(_execTryRender, 0);


})();


  

