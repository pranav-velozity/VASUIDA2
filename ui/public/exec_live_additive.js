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
