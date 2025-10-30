/*
 * Pinpoint Exec — Additive Live Widgets (No baseline changes)
 * - Mounts read-only KPIs, Radar (exceptions), Double-Donut (planned vs applied), and 3 exception cards
 * - Uses existing global state & helpers: state, weekEndISO, ymdFromCompletedAtInTZ, todayInTZ, mondayOfInTZ, toNum, aggregate, joinPOProgress
 * - No edits to existing routes/exports/logic; safe to include after current script.
 */
(function ExecLiveAdditive(){
  const BRAND = (typeof window.BRAND !== 'undefined') ? window.BRAND : '#990033';
  const BUSINESS_TZ = document.querySelector('meta[name="business-tz"]')?.content || 'Asia/Shanghai';

  // ---------- Small utilities ----------
  const $ = (s)=>document.querySelector(s);
  const fmt = (n)=> Number(n||0).toLocaleString();
  const clamp = (v,min,max)=> Math.max(min, Math.min(max, v));
  const pct = (num,den)=> den>0 ? Math.round((num*100)/den) : 0;
  const weekEndISO = window.weekEndISO || function(ws){ const d=new Date(ws); d.setDate(d.getDate()+6); return iso(d); };
  function toISODate(v){
    if (typeof window.toISODate === 'function') return window.toISODate(v);
    const d = new Date(v); const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  }
  function bizYMDFromRecord(r){
    // Prefer date_local (already YYYY-MM-DD in biz TZ), else bucket completed_at -> business day
    if (r?.date_local) return String(r.date_local).trim();
    if (r?.completed_at && typeof window.ymdFromCompletedAtInTZ === 'function') return window.ymdFromCompletedAtInTZ(r.completed_at, BUSINESS_TZ);
    return '';
  }

  // ---------- Metric computations (scoped to selected week) ----------
  function computeExecMetrics(){
    const ws = window.state?.weekStart; if (!ws) return null;
    const we = weekEndISO(ws);
    const plan = Array.isArray(window.state?.plan) ? window.state.plan : [];
    const recordsAll = Array.isArray(window.state?.records) ? window.state.records : [];
    const bins = Array.isArray(window.state?.bins) ? window.state.bins : [];

    // window.state.records already week-filtered in app, but keep a defensive guard
    const wkRecords = recordsAll.filter(r=>{
      if (r?.status !== 'complete') return false;
      const ymd = bizYMDFromRecord(r);
      return ymd && ymd >= ws && ymd <= we;
    });

    // Planned totals
    const plannedTotal = plan.reduce((s,p)=> s + (window.toNum? toNum(p.target_qty) : Number(p.target_qty||0)), 0);
    const appliedTotal = wkRecords.length;
    const completionPct = pct(appliedTotal, plannedTotal);

    // Aggregates for discrepancy
    const agg = (typeof window.aggregate === 'function') ? window.aggregate(wkRecords) : {byPO:new Map(), bySKU:new Map()};

    // --- Discrepancy % (SKU) ---
    const planBySKU = new Map();
    for (const p of plan){ const sku=String(p.sku_code||'').trim(); if(!sku) continue; planBySKU.set(sku, (planBySKU.get(sku)||0) + (toNum? toNum(p.target_qty):Number(p.target_qty||0))); }
    let skuPctSum=0, skuCnt=0;
    for (const [sku, planned] of planBySKU.entries()){
      const applied = agg.bySKU.get(sku)||0;
      if (planned>0){ skuPctSum += Math.abs(applied - planned)/planned; skuCnt++; }
    }
    const avgSkuDiscPct = Math.round((skuCnt? (skuPctSum/skuCnt) : 0)*100);

    // --- Discrepancy % (PO) ---
    const planByPO = new Map();
    const poDue = new Map();
    for (const p of plan){
      const po = String(p.po_number||'').trim(); if(!po) continue;
      planByPO.set(po,(planByPO.get(po)||0)+(toNum? toNum(p.target_qty):Number(p.target_qty||0)));
      const d = String(p.due_date||'').trim();
      if (!poDue.has(po)) poDue.set(po,d); else if (d && (!poDue.get(po) || d < poDue.get(po))) poDue.set(po,d);
    }
    let poPctSum=0, poCnt=0;
    for (const [po, planned] of planByPO.entries()){
      const applied = agg.byPO.get(po)||0;
      if (planned>0){ poPctSum += Math.abs(applied - planned)/planned; poCnt++; }
    }
    const avgPoDiscPct = Math.round((poCnt? (poPctSum/poCnt) : 0)*100);

    // --- Duplicate UIDs (same SKU+UID >1 within the week) ---
    const pairCounts = new Map();
    for (const r of wkRecords){
      const sku = String(r.sku_code||'').trim();
      const uid = String(r.uid||'').trim();
      if (!sku || !uid) continue;
      const k = `${sku}||${uid}`;
      pairCounts.set(k, (pairCounts.get(k)||0) + 1);
    }
    let dupScanCount = 0; const dupPairs=[];
    for (const [k,c] of pairCounts.entries()) if (c>1){ const [sku,uid]=k.split('||'); dupPairs.push({sku,uid,count:c}); dupScanCount+=c; }

    // --- Heavy bins (>12kg) + diversity
    const heavyBins = (bins||[]).filter(b => Number(b.weight_kg||0) > 12);
    const heavyBinSet = new Set(heavyBins.map(b=> String(b.mobile_bin||'').trim()).filter(Boolean));
    const heavyCount = heavyBinSet.size;
    const skuByBin = new Map();
    for (const r of wkRecords){ const bin = String(r.mobile_bin||'').trim(); const sku=String(r.sku_code||'').trim(); if(!bin||!sku) continue; if(!skuByBin.has(bin)) skuByBin.set(bin,new Set()); skuByBin.get(bin).add(sku); }
    let diversitySum=0, diversityN=0;
    for (const bin of heavyBinSet){ diversitySum += (skuByBin.get(bin)?.size || 0); diversityN++; }
    const avgDiversityHeavy = diversityN? (diversitySum/diversityN) : 0;

    // --- Late appliers: record business date > PO due_date (earliest per PO)
    let lateCount = 0;
    for (const r of wkRecords){
      const po = String(r.po_number||'').trim(); if(!po) continue;
      const due = poDue.get(po); if (!due) continue; // skip when due_date missing
      const ymd = bizYMDFromRecord(r); if (!ymd) continue;
      if (ymd > due) lateCount++;
    }
    const lateRatePct = pct(lateCount, appliedTotal);

    // Pareto Top gap drivers (PO×SKU by discrepancy amount)
    const appliedPOSKU = new Map();
    for (const r of wkRecords){ const po=String(r.po_number||'').trim(); const sku=String(r.sku_code||'').trim(); if(!po||!sku) continue; const k=`${po}|||${sku}`; appliedPOSKU.set(k,(appliedPOSKU.get(k)||0)+1); }
    const plannedPOSKU = new Map();
    for (const p of plan){ const po=String(p.po_number||'').trim(); const sku=String(p.sku_code||'').trim(); if(!po||!sku) continue; const k=`${po}|||${sku}`; plannedPOSKU.set(k,(plannedPOSKU.get(k)||0)+ (toNum? toNum(p.target_qty):Number(p.target_qty||0))); }
    const gaps=[]; let totalGapAbs=0;
    for (const [k, planned] of plannedPOSKU.entries()){
      const applied = appliedPOSKU.get(k)||0; const gap = planned - applied; // planned minus applied
      if (gap !== 0){ gaps.push({k, gap, planned, applied}); totalGapAbs += Math.abs(gap); }
    }
    gaps.sort((a,b)=> Math.abs(b.gap)-Math.abs(a.gap));
    const topGap = gaps.slice(0,5).map(g=>{ const [po,sku]=g.k.split('|||'); return {po,sku,gap:g.gap,planned:g.planned,applied:g.applied}; });

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

// ---- Donut ----
function renderDonutWithBaseline(slot, planned, applied) {
  slot.innerHTML = '';
  const size = 220, r = 90, cx = size/2, cy = size/2;
  const CIRC = 2*Math.PI*r;

  const svg = _el('svg', { viewBox: `0 0 ${size} ${size}`, width: size, height: size, style: 'display:block' });

  // Baseline ring (light grey)
  svg.appendChild(_el('circle', {
    cx, cy, r,
    fill: 'none',
    stroke: GREY,
    'stroke-width': 18
  }));

  // Applied ring (brand)
  const appliedArc = _el('circle', {
    cx, cy, r,
    fill: 'none',
    stroke: BRAND,
    'stroke-width': 18,
    'stroke-linecap': 'round',
    'transform': `rotate(-90 ${cx} ${cy})`,
    'stroke-dasharray': CIRC,
    'stroke-dashoffset': CIRC
  });
  svg.appendChild(appliedArc);

  slot.appendChild(svg);

  // Label center
  const pctText = document.createElement('div');
  pctText.className = 'absolute inset-0 flex items-center justify-center text-sm text-gray-600';
  pctText.style.pointerEvents = 'none';
  pctText.textContent = '—';
  slot.style.position = 'relative';
  slot.appendChild(pctText);

  // Animate in
  const animateTo = (pct) => {
    const dash = CIRC * (1 - Math.max(0, Math.min(1, pct)));
    appliedArc.style.transition = 'stroke-dashoffset 600ms ease';
    appliedArc.setAttribute('stroke-dashoffset', dash);
    pctText.textContent = Math.round(pct * 100) + '%';
  };

  if (Number.isFinite(planned) && planned > 0 && Number.isFinite(applied)) {
    const pct = Math.max(0, Math.min(1, applied / planned));
    // small delay so the sweep is visible
    requestAnimationFrame(() => animateTo(pct));
  } else {
    // Idle sweep (no data yet)
    pctText.textContent = '';
    appliedArc.style.animation = 'donutIdle 2.2s linear infinite';
    const style = document.createElement('style');
    style.textContent = `
      @keyframes donutIdle {
        0%   { stroke-dashoffset: ${CIRC}; }
        50%  { stroke-dashoffset: ${CIRC*0.25}; }
        100% { stroke-dashoffset: ${CIRC}; }
      }
    `;
    slot.appendChild(style);
  }
}

// ---- Radar (N axes) ----
function renderRadarWithBaseline(slot, labels, baselineValues, actualValues) {
  slot.innerHTML = '';

  const N = labels.length;
  const size = 320, pad = 24, cx = size/2, cy = size/2, R = (size/2) - pad;
  const svg = _el('svg', { viewBox: `0 0 ${size} ${size}`, width: size, height: size, style: 'display:block' });

  // Polar helpers
  const pt = (i, v) => {
    const ang = (-Math.PI/2) + (i * 2*Math.PI / N);
    const r = (v/100) * R;
    return [cx + r * Math.cos(ang), cy + r * Math.sin(ang)];
  };
  const poly = (vals) => vals.map((v,i)=>pt(i,v)).map(([x,y])=>`${x},${y}`).join(' ');

  // Radar grid circles
  [20,40,60,80,100].forEach(t => {
    svg.appendChild(_el('circle', {
      cx, cy, r: (t/100)*R, fill: 'none', stroke: GREY_STROKE, 'stroke-width': 1
    }));
  });

  // Axes
  for (let i=0;i<N;i++){
    const [x,y] = pt(i, 100);
    svg.appendChild(_el('line', { x1: cx, y1: cy, x2: x, y2: y, stroke: GREY_STROKE, 'stroke-width': 1 }));
  }

  // Labels
  labels.forEach((lab,i)=>{
    const [x,y] = pt(i, 112);
    const t = _el('text', { x, y, 'text-anchor':'middle', 'dominant-baseline':'middle', 'font-size':'11', fill:'#6b7280' });
    t.textContent = lab;
    svg.appendChild(t);
  });

  // Baseline ghost polygon (light grey)
  const base = _el('polygon', {
    points: poly(baselineValues),
    fill: GREY,
    'fill-opacity': 0.35,
    stroke: GREY_STROKE,
    'stroke-width': 1
  });
  svg.appendChild(base);

  // Actual polygon (brand)
  const actual = _el('polygon', {
    points: poly((actualValues && actualValues.length===N) ? actualValues.map(()=>0) : baselineValues.map(()=>0)),
    fill: BRAND,
    'fill-opacity': 0.15,
    stroke: BRAND,
    'stroke-width': 2
  });
  svg.appendChild(actual);

  slot.appendChild(svg);

  if (actualValues && actualValues.length === N) {
    // animate morph (simple frame tween)
    const steps = 22;
    let k = 0;
    const from = baselineValues.map(()=>0);
    const to   = actualValues.slice();
    const tick = () => {
      k++;
      const cur = from.map((_,i)=> from[i] + (to[i]-from[i])*(k/steps));
      actual.setAttribute('points', poly(cur));
      if (k < steps) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  } else {
    // Idle live feel: subtle rotating glare
    const glare = _el('circle', {
      cx, cy, r: R*0.55, fill: 'url(#radar-glare)'
    });
    const defs = _el('defs');
    const grad = _el('radialGradient', { id: 'radar-glare' }, [
      _el('stop', { offset: '0%', 'stop-color': '#ffffff', 'stop-opacity': 0.15 }),
      _el('stop', { offset: '100%', 'stop-color': '#ffffff', 'stop-opacity': 0 })
    ]);
    defs.appendChild(grad);
    svg.insertBefore(defs, svg.firstChild);
    svg.appendChild(glare);

    const style = document.createElement('style');
    style.textContent = `
      @keyframes radarSpin { to { transform: rotate(360deg); } }
      svg { transform-origin: ${cx}px ${cy}px; }
      svg { animation: radarSpin 12s linear infinite; }
    `;
    slot.appendChild(style);
  }
}


  // ---------- Cards render ----------
  function renderExec(){
    const host = $('#page-exec'); if (!host) return;
    if (!host.querySelector('#exec-live')){
      const wrap = document.createElement('div'); wrap.id='exec-live';
      wrap.innerHTML = `
        <div class="grid grid-cols-1 gap-3">
          <!-- Tiles -->
          <div id="exec-tiles" class="grid grid-cols-2 sm:grid-cols-6 gap-3"></div>
          <!-- Charts row -->
          <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div class="bg-white rounded-2xl border shadow p-4" id="card-radar">
              <div class="text-base font-semibold mb-1">Exceptions Radar</div>
              <div class="text-xs text-gray-500 mb-2">Risk-normalized (0–100)</div>
              <div id="radar-slot" class="flex items-center justify-center"></div>
              <div id="radar-note" class="mt-2 text-xs text-gray-500"></div>
            </div>
            <div class="bg-white rounded-2xl border shadow p-4" id="card-donut">
              <div class="text-base font-semibold mb-1">Planned vs Applied</div>
              <div class="text-xs text-gray-500 mb-2">Week scope (business TZ)</div>
              <div id="donut-slot" class="flex items-center justify-center"></div>
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
// --- ADD: paint ghost charts immediately ---
renderDonutWithBaseline(document.getElementById('donut-slot'), null, null);
renderRadarWithBaseline(
  document.getElementById('radar-slot'),
  ['Late appliers','Under pace','Heavy bins','Duplicates','Manifest gaps'],
  [55,50,45,60,50],   // ghost baseline shape
  null                // no actual yet -> idle animation
);


  // --- INLINE SKELETONS (shows immediately) ---
  // 1) Ensure skeleton CSS exists once
  if (!document.getElementById('vo-skel-css')) {
    const style = document.createElement('style');
    style.id = 'vo-skel-css';
    style.textContent = `
      @keyframes voShimmer {
        0% { background-position:-200% 0; }
        100% { background-position:200% 0; }
      }
      .vo-skel {
        background: linear-gradient(90deg, #f3f4f6 0%, #e5e7eb 50%, #f3f4f6 100%);
        background-size: 200% 100%;
        animation: voShimmer 1.2s ease-in-out infinite;
        border-radius: 12px;
      }
      .vo-skel-radar  { width: 260px; height: 180px; }
      .vo-skel-donut  { width: 180px; height: 180px; border-radius: 9999px; }
    `;
    document.head.appendChild(style);
  }

  // 2) Drop skeleton blocks into the two chart slots (only once)
  const radarSlot = document.getElementById('radar-slot');
  if (radarSlot && !radarSlot.dataset.skel) {
    radarSlot.dataset.skel = '1';
    radarSlot.innerHTML = `<div class="vo-skel vo-skel-radar"></div>`;
  }

  const donutSlot = document.getElementById('donut-slot');
  if (donutSlot && !donutSlot.dataset.skel) {
    donutSlot.dataset.skel = '1';
    donutSlot.innerHTML = `<div class="vo-skel vo-skel-donut"></div>`;
  }

  // (Optional) if your external helper exists, run it too
  if (window.execBootSkeletons) window.execBootSkeletons();
    }

    const m = computeExecMetrics(); if (!m) return;

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
    $('#radar-slot').innerHTML = radarSVG({axes, values});
    const topIdx = values.reduce((bi, v,i)=> v>values[bi]? i:bi, 0);
    $('#radar-note').textContent = `Top driver: ${axes[topIdx]} (${Math.round(values[topIdx])})`;

    // Double donut
    $('#donut-slot').innerHTML = donutDoubleSVG(m.plannedTotal, m.appliedTotal);

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
    const wkRecords = (window.state?.records||[]).filter(r=> r.status==='complete' && (bizYMDFromRecord(r) >= ws && bizYMDFromRecord(r) <= we));
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

// Repaint charts using grey baseline + brand overlay (uses m, axes, values already computed)
renderDonutWithBaseline(document.getElementById('donut-slot'), m.plannedTotal, m.appliedTotal);
renderRadarWithBaseline(document.getElementById('radar-slot'), axes, [55,50,45,60,50], values);
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
})();


  // 1) Mount skeletons once
  function mountSkeletons() {
    const r = document.querySelector(RADAR_ID);
    if (r && !r.querySelector('svg')) {
      r.innerHTML = `
        <svg viewBox="0 0 200 200" width="100%" height="140">
          <g transform="translate(100,100)" fill="none" stroke="#e5e7eb">
            ${[20,40,60,80].map(rad => `<circle r="${rad}" />`).join('')}
          </g>
          <g transform="translate(100,100)" stroke="#cbd5e1" stroke-width="2">
            ${Array.from({length:6}).map((_,i)=>{
              const a = (Math.PI*2/6)*i - Math.PI/2;
              const x = 90*Math.cos(a), y=90*Math.sin(a);
              return `<line x1="0" y1="0" x2="${x.toFixed(1)}" y2="${y.toFixed(1)}"/>`;
            }).join('')}
          </g>
          <g transform="translate(100,100)">
            <polygon id="radar-poly" points="" fill="${BRAND}11" stroke="${BRAND}" stroke-width="2"/>
          </g>
        </svg>`;
    }
    const d = document.querySelector(DONUT_ID);
    if (d && !d.querySelector('svg')) {
      d.innerHTML = `
        <svg viewBox="0 0 160 160" width="100%" height="140">
          <g transform="translate(80,80)" fill="none" stroke-linecap="round">
            <circle r="54" stroke="#e5e7eb" stroke-width="12"/>
            <circle id="ring-planned" r="54" stroke="#d1d5db" stroke-width="12"
              stroke-dasharray="0 339" transform="rotate(-90)"/>
            <circle r="36" stroke="#f3f4f6" stroke-width="12"/>
            <circle id="ring-applied" r="36" stroke="${BRAND}" stroke-width="12"
              stroke-dasharray="0 226" transform="rotate(-90)"/>
            <text id="donut-label" x="0" y="6" text-anchor="middle" font-size="14" fill="#374151">—</text>
          </g>
        </svg>`;
    }
  }

  // 2) Helpers
  const lerp = (a,b,t)=>a+(b-a)*t;
  function animate(from, to, ms, step, done) {
    const t0 = performance.now();
    function tick(now){
      const p = Math.min(1,(now-t0)/ms);
      step(lerp(from,to,p));
      if (p<1) requestAnimationFrame(tick); else done && done();
    }
    requestAnimationFrame(tick);
  }

  // 3) Draw/animate
  function drawRadar(values /* 6 numbers 0..100 */){
    const poly = document.getElementById('radar-poly');
    if (!poly) return;
    const target = values.map((v,i)=>{
      const a = (Math.PI*2/6)*i - Math.PI/2;
      const r = 90*(Math.max(0,Math.min(100,v))/100);
      return [r*Math.cos(a), r*Math.sin(a)];
    });
    // animate from current to target
    const cur = (poly.getAttribute('points')||'')
      .trim()
      .split(/\s+/)
      .map(p=>p.split(',').map(Number))
      .filter(p=>p.length===2);
    const from = cur.length===6 ? cur : target.map(()=>[0,0]);
    const dur = 650;
    animate(0,1,dur,(t)=>{
      const pts = target.map((p,i)=>[
        lerp(from[i][0], p[0], t).toFixed(1),
        lerp(from[i][1], p[1], t).toFixed(1)
      ].join(',')).join(' ');
      poly.setAttribute('points', pts);
    });
  }

  function drawDonut(planned, applied){
    const ringP = document.getElementById('ring-planned');
    const ringA = document.getElementById('ring-applied');
    const label = document.getElementById('donut-label');
    if (!ringP || !ringA) return;

    const C_P = 2*Math.PI*54; // ≈ 339
    const C_A = 2*Math.PI*36; // ≈ 226

    const pct = planned>0 ? Math.round((applied/planned)*100) : 0;
    label && (label.textContent = `${applied.toLocaleString()} / ${planned.toLocaleString()} (${pct}%)`);

    const curP = +(ringP.getAttribute('data-val')||0);
    const curA = +(ringA.getAttribute('data-val')||0);
    const tgtP = 100;                  // planned ring fills to 100%
    const tgtA = Math.max(0,Math.min(100,pct));

    animate(curP,tgtP,700,(v)=>{
      ringP.setAttribute('stroke-dasharray', `${(v/100*C_P).toFixed(1)} ${C_P.toFixed(1)}`);
      ringP.setAttribute('data-val', v.toFixed(1));
    });
    animate(curA,tgtA,900,(v)=>{
      ringA.setAttribute('stroke-dasharray', `${(v/100*C_A).toFixed(1)} ${C_A.toFixed(1)}`);
      ringA.setAttribute('data-val', v.toFixed(1));
    });
  }

  // 4) Simple watcher: mount skeletons immediately; when data arrives, animate once
  let booted = false, painted = false;
  function tick() {
    if (location.hash !== '#exec') { setTimeout(tick, 600); return; }
    if (!booted) { mountSkeletons(); booted = true; }
    try {
      // Need plan + records; bins only for radar’s heavy-bins component (we’ll derive inputs)
      const hasPlan = Array.isArray(window.state?.plan) && window.state.plan.length>=0;
      const hasRecs = Array.isArray(window.state?.records);
      if (hasPlan && hasRecs && !painted) {
        // Compute inputs
        const planned = window.state.plan.reduce((s,p)=> s + (Number(p.target_qty)||0), 0);
        const applied = window.state.records.filter(r=>r.status==='complete').length;

        // Radar inputs (0..100) — reuse what you already compute; place quick guards here
        // You can replace these with your actual normalized metrics:
        const dup = Math.min(100, (document.getElementById('ops-dupes') ? Number(document.getElementById('ops-dupes').textContent)||0 : 0)*5);
        const skuGap = 100 - Math.min(100, Math.round((applied/(planned||1))*100)); // invert completion as "gap"
        const poGap  = skuGap; // placeholder; wire your PO-level metric if desired
        const heavy  = Math.min(100, (window.state?.binQA?.anomalies||[]).filter(a=>a.type==='units_mismatch').length*10);
        const late   = 0; // optional metric if you implement it
        const badSync= Math.min(100, (window.state?.records||[]).filter(r=>r.sync_state && r.sync_state!=='synced').length);

        drawRadar([dup, skuGap, poGap, heavy, late, badSync]);
        drawDonut(planned, applied);
        painted = true;
      }
    } catch {}
    setTimeout(tick, painted ? 1500 : 600);
  }

  // start
  mountSkeletons();
  tick();
})();

