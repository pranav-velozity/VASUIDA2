/*
 * Pinpoint Exec — Cross-Week Executive Dashboard v4
 * Sharp visuals · animated charts · full-width radar
 */
(function ExecLiveAdditive() {
  'use strict';

  const BRAND    = '#990033';
  const BRAND_LT = 'rgba(153,0,51,0.08)';
  const AIR_COL  = '#4A90D9';
  const SEA_COL  = '#2E7D9E';
  const GREEN    = '#22C55E';
  const AMBER    = '#F59E0B';

  const _apiBase = (() => {
    const b = String(document.querySelector('meta[name="api-base"]')?.content || location.origin).replace(/\/+$/, '');
    return /\/api$/i.test(b) ? b : b + '/api';
  })();

  async function _getToken() { try { return await window.Clerk?.session?.getToken(); } catch { return null; } }
  async function _api(path) {
    const token = await _getToken();
    const res = await fetch(`${_apiBase}${path}`, { headers: Object.assign({'content-type':'application/json'}, token?{'authorization':'Bearer '+token}:{}) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  let _weeks = [], _range = 8, _loading = false;
  const _charts = {};

  const fmtN   = (n,d) => { d=d||0; if(n==null||isNaN(n)) return '—'; return Number(n).toLocaleString('en-AU',{minimumFractionDigits:d,maximumFractionDigits:d}); };
  const fmtPct = n => n==null?'—':Math.round(n)+'%';
  const fmtDays= n => n==null?'—':Number(n).toFixed(1)+'d';
  const el     = id => document.getElementById(id);
  const shortWk= ws => { try{const d=new Date(ws+'T00:00:00Z');return d.toLocaleDateString('en-AU',{month:'short',day:'numeric',timeZone:'UTC'});}catch{return ws;} };
  const avg    = arr => arr.length?arr.reduce((a,b)=>a+b,0)/arr.length:null;

  function computeBaseline(weeks) {
    if (!weeks.length) return {};
    const s_days = weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null).sort((a,b)=>a-b);
    const s_otr  = weeks.map(w=>w.on_time_receiving_pct).filter(v=>v!=null).sort((a,b)=>b-a);
    const s_thru = weeks.map(w=>w.planned_units>0?w.applied_units/w.planned_units:0).sort((a,b)=>b-a);
    const q = arr => arr.length?arr[Math.floor(arr.length*0.25)]:null;
    return { avg_days_to_apply:q(s_days), on_time_receiving_pct:q(s_otr), throughput_pct:q(s_thru) };
  }

  // ── CSS injection ─────────────────────────────────────────────
  function _injectCSS() {
    if (el('exec-styles')) return;
    const style = document.createElement('style');
    style.id = 'exec-styles';
    style.textContent = `
      @keyframes exec-fade-up { from { opacity:0; transform:translateY(12px); } to { opacity:1; transform:translateY(0); } }
      @keyframes exec-count-in { from { opacity:0; } to { opacity:1; } }
      .exec-kpi-tile { animation: exec-fade-up 0.4s ease both; }
      .exec-kpi-tile:nth-child(1) { animation-delay:0.05s; }
      .exec-kpi-tile:nth-child(2) { animation-delay:0.1s; }
      .exec-kpi-tile:nth-child(3) { animation-delay:0.15s; }
      .exec-kpi-tile:nth-child(4) { animation-delay:0.2s; }
      .exec-chart-card { animation: exec-fade-up 0.5s ease both; }
      .exec-chart-card:nth-child(1) { animation-delay:0.25s; }
      .exec-chart-card:nth-child(2) { animation-delay:0.3s; }
      .exec-insight-card { animation: exec-fade-up 0.35s ease both; }
      .exec-range-btn:hover { opacity:0.8; }
    `;
    document.head.appendChild(style);
  }

  function _tryRender() {
    const host = el('page-exec');
    if (!host || host.style.display==='none' || host.classList.contains('hidden')) return;
    _injectCSS();
    if (!el('exec-dashboard-root')) { host.innerHTML = _shellHTML(); _wireRangeBar(); }
    _loadAndRender();
  }

  function _shellHTML() {
    const btn = w => `<button class="exec-range-btn" data-weeks="${w}" style="font-size:11px;font-weight:500;padding:5px 12px;border-radius:6px;border:0.5px solid rgba(0,0,0,0.12);background:${w===8?'#1C1C1E':'#fff'};color:${w===8?'#fff':'#6E6E73'};cursor:pointer;transition:opacity .15s;">${w}W</button>`;
    return `<div id="exec-dashboard-root" style="min-height:100vh;background:#F9F9FB;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:0 0 48px;">
<div id="exec-range-bar" style="position:sticky;top:0;z-index:100;background:#fff;border-bottom:0.5px solid rgba(0,0,0,0.08);padding:10px 28px;display:flex;align-items:center;gap:8px;">
  <span style="font-size:11px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-right:4px;">Range</span>
  ${[4,8,12,16].map(btn).join('')}
  <span id="exec-range-label" style="font-size:11px;color:#AEAEB2;margin-left:8px;"></span>
  <div id="exec-loading" style="margin-left:auto;font-size:11px;color:#AEAEB2;display:none;">Loading…</div>
</div>
<div style="padding:20px 28px;display:grid;grid-template-columns:1fr 320px;gap:24px;align-items:start;">
  <div>
    <!-- KPIs -->
    <div id="exec-kpis" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;"></div>

    <!-- Row 1: Avg Time to Live — full width segmented timeline -->
    <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;margin-bottom:16px;">
      <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:2px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;">Avg Time to Live</div>
        <div id="exec-ttl-summary" style="font-size:11px;color:#AEAEB2;"></div>
      </div>
      <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Avg days from receiving to FC delivery · 3 segments per week</div>
      <div id="chart-ttl" style="width:100%;"></div>
    </div>

    <!-- Row 2: Performance Radar (moved here per request) -->
    <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:24px;margin-bottom:16px;">
      <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Performance Radar</div>
      <div style="font-size:10px;color:#AEAEB2;margin-bottom:20px;">Actual vs best-week baseline · 100 = best achieved</div>
      <div style="display:grid;grid-template-columns:420px 1fr;gap:32px;align-items:center;">
        <div style="margin-left:15%;width:360px;height:360px;flex-shrink:0;">
          <canvas id="chart-radar" width="360" height="360" style="display:block;width:360px;height:360px;"></canvas>
        </div>
        <div id="exec-radar-legend" style="display:flex;flex-direction:column;gap:14px;"></div>
      </div>
    </div>

    <!-- Row 3: Throughput (wide) + Receiving Health -->
    <div style="display:grid;grid-template-columns:3fr 2fr;gap:16px;margin-bottom:16px;">
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:2px;">
          <div style="font-size:13px;font-weight:600;color:#1C1C1E;">Weekly Throughput</div>
          <div id="exec-thru-summary" style="font-size:11px;color:#AEAEB2;"></div>
        </div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Planned vs applied units · completion %</div>
        <div id="chart-throughput-svg" style="width:100%;"></div>
      </div>
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Receiving Health</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">On-time % per week</div>
        <div id="chart-receiving-dots" style="width:100%;"></div>
      </div>
    </div>

    <!-- Row 3: Air vs Sea (full width without pipeline since TTL replaces it) -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Air vs Sea Mix</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Unit volume by freight mode</div>
        <canvas id="chart-airvsea" height="170"></canvas>
      </div>
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:2px;">
          <div style="font-size:13px;font-weight:600;color:#1C1C1E;">Container Utilisation</div>
          <div id="exec-cont-summary" style="font-size:11px;color:#AEAEB2;"></div>
        </div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Avg units per container · 20ft vs 40ft week over week</div>
        <canvas id="chart-container-util" height="170"></canvas>
      </div>
    </div>


  </div>

  <!-- Intelligence Panel -->
  <div id="exec-intelligence" style="position:sticky;top:60px;max-height:calc(100vh - 80px);overflow-y:auto;">
    <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:14px;display:flex;align-items:center;gap:8px;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="${BRAND}" stroke-width="2.5" stroke-linecap="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      Improvement Intelligence
    </div>
    <div id="exec-insights-list" style="display:flex;flex-direction:column;gap:10px;">
      <div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">Loading…</div>
    </div>
  </div>
</div></div>`;
  }

  function _wireRangeBar() {
    document.querySelectorAll('.exec-range-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        _range = Number(btn.dataset.weeks);
        document.querySelectorAll('.exec-range-btn').forEach(b => { b.style.background=b===btn?'#1C1C1E':'#fff'; b.style.color=b===btn?'#fff':'#6E6E73'; });
        _loadAndRender();
      });
    });
  }

  async function _loadAndRender() {
    if (_loading) return;
    _loading = true;
    const loadEl = el('exec-loading');
    if (loadEl) loadEl.style.display = 'block';
    try {
      const toDate = new Date(), fromDate = new Date(toDate);
      fromDate.setUTCDate(fromDate.getUTCDate() - (_range * 7));
      const from = fromDate.toISOString().slice(0,10), to = toDate.toISOString().slice(0,10);
      const facility = ((window.state?.plan)||[]).map(p=>String(p.facility_name||p.facility||'').trim()).find(Boolean)
        || String(window.state?.facility||'').trim() || '';
      if (!facility) { _renderError('No facility found. Please navigate to Week Hub and load a plan first.'); return; }
      const data = await _api(`/exec/summary?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&facility=${encodeURIComponent(facility)}`);
      _weeks = Array.isArray(data.weeks) ? data.weeks : [];
      const lbl = el('exec-range-label');
      if (lbl) lbl.textContent = `${from} – ${to} · ${_weeks.length} weeks`;
      _renderAll();
    } catch(e) {
      console.error('[exec] load failed', e);
      _renderError('Failed to load: '+(e.message||e));
    } finally {
      _loading = false;
      if (loadEl) loadEl.style.display = 'none';
    }
  }

  function _renderError(msg) {
    const kpis = el('exec-kpis');
    if (kpis) kpis.innerHTML = `<div style="grid-column:span 4;font-size:12px;color:#D61A3C;padding:16px;">${msg}</div>`;
  }

  function _renderAll() {
    if (!_weeks.length) { _renderError('No data for this period.'); return; }
    const baseline = computeBaseline(_weeks);
    _renderKPIs(baseline);
    _renderCharts(baseline);
    _renderInsights(baseline);
  }

  // ── KPIs ──────────────────────────────────────────────────────
  function _renderKPIs(baseline) {
    const kpis = el('exec-kpis'); if (!kpis) return;
    const totPl = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const pct   = totPl>0?Math.round(totAp/totPl*100):0;
    const daysA = avg(_weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null));
    const wtA   = avg(_weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null&&v>0));
    const totAir= _weeks.reduce((s,w)=>s+(w.air_units||0),0);
    const totSea= _weeks.reduce((s,w)=>s+(w.sea_units||0),0);
    const airP  = (totAir+totSea)>0?Math.round(totAir/(totAir+totSea)*100):0;
    const last2 = _weeks.slice(-2);
    const dArrow= last2.length===2&&last2[0].avg_days_to_apply!=null&&last2[1].avg_days_to_apply!=null
      ?(last2[1].avg_days_to_apply<last2[0].avg_days_to_apply?'↓':'↑'):'';
    const aArrow= last2.length===2?(()=>{
      const a0=last2[0].planned_units>0?last2[0].air_units/last2[0].planned_units:0;
      const a1=last2[1].planned_units>0?last2[1].air_units/last2[1].planned_units:0;
      return a1>a0?'↑':a1<a0?'↓':'';
    })():'';
    const tile = (label, value, badge, bCol, sub, delay) => `
      <div class="exec-kpi-tile" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:18px;animation-delay:${delay}s;">
        <div style="font-size:9px;font-weight:600;color:#AEAEB2;text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px;">${label}</div>
        <div style="display:flex;align-items:baseline;gap:6px;margin-bottom:4px;">
          <div style="font-size:24px;font-weight:700;color:#1C1C1E;letter-spacing:-0.03em;line-height:1;">${value}</div>
          ${badge?`<div style="font-size:14px;font-weight:700;color:${bCol};">${badge}</div>`:''}
        </div>
        <div style="font-size:10px;color:#AEAEB2;">${sub}</div>
      </div>`;
    kpis.innerHTML = [
      tile('Units Applied / Planned', `${fmtN(totAp)} / ${fmtN(totPl)}`, fmtPct(pct), pct>=90?GREEN:pct>=70?AMBER:BRAND, 'Season-to-date', 0.05),
      tile('Avg Days to Apply', fmtDays(daysA), dArrow, dArrow==='↓'?GREEN:dArrow==='↑'?BRAND:'#AEAEB2', 'Recv → VAS'+(baseline.avg_days_to_apply!=null?' · best '+fmtDays(baseline.avg_days_to_apply):''), 0.10),
      tile('Avg Carton Weight', wtA!=null?fmtN(wtA,1)+' kg':'—', '', '#6E6E73', 'Per bin from manifest', 0.15),
      tile('Air vs Sea Split', airP+'% air', aArrow, aArrow==='↑'?AMBER:GREEN, `${fmtN(totAir)} air · ${fmtN(totSea)} sea`, 0.20),
    ].join('');
  }

  // ── Charts ────────────────────────────────────────────────────
  function _destroyCharts() {
    Object.keys(_charts).forEach(k=>{ try{_charts[k].destroy();}catch{} delete _charts[k]; });
  }
  function _mkChart(id, config) {
    const canvas = el(id); if (!canvas||!window.Chart) return;
    try { _charts[id] = new window.Chart(canvas, config); } catch(e){ console.warn('[exec] chart',id,e); }
  }

  function _renderCharts(baseline) {
    _destroyCharts();
    const labels = _weeks.map(w=>shortWk(w.week_start));

    // Chart 0: Avg Time to Live — segmented horizontal timeline per week
    _renderTTLChart(labels);

    // Chart 1: Throughput — smooth slope SVG
    const totPl = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const sumEl = el('exec-thru-summary');
    if (sumEl) sumEl.textContent = `${fmtN(totAp)} / ${fmtN(totPl)} total`;

    _renderThroughputSVG(labels);

    // Chart 2: Receiving Health — custom SVG dot chart (not Chart.js bar)
    _renderReceivingDots(baseline);

    // Container Utilisation — avg units per container by size
    const COL_20 = '#8B5CF6'; // violet — matches TTL seg2
    const COL_40 = '#06B6D4'; // cyan — matches TTL seg3
    const has20 = _weeks.some(w=>w.avg_units_per_20ft!=null);
    const has40 = _weeks.some(w=>w.avg_units_per_40ft!=null);
    _mkChart('chart-container-util', { type:'bar', data:{ labels, datasets:[
      // Count bars (subtle, background)
      { label:'20ft count', data:_weeks.map(w=>w.count_20ft||0),
        backgroundColor:'rgba(139,92,246,0.12)', borderRadius:3, yAxisID:'yCount', order:3 },
      { label:'40ft count', data:_weeks.map(w=>w.count_40ft||0),
        backgroundColor:'rgba(6,182,212,0.12)', borderRadius:3, yAxisID:'yCount', order:3 },
      // Units per container lines (primary metric)
      { label:'Avg units / 20ft', data:_weeks.map(w=>w.avg_units_per_20ft),
        type:'line', borderColor:COL_20, backgroundColor:'transparent',
        borderWidth:2.5, pointBackgroundColor:'#fff', pointBorderColor:COL_20,
        pointBorderWidth:2, pointRadius:4, yAxisID:'y', order:1, spanGaps:false, tension:0.3 },
      { label:'Avg units / 40ft', data:_weeks.map(w=>w.avg_units_per_40ft),
        type:'line', borderColor:COL_40, backgroundColor:'transparent',
        borderWidth:2.5, pointBackgroundColor:'#fff', pointBorderColor:COL_40,
        pointBorderWidth:2, pointRadius:4, yAxisID:'y', order:2, spanGaps:false, tension:0.3 },
    ]}, options:{
      plugins:{ legend:{display:true,position:'top',align:'end',labels:{font:{size:10},boxWidth:8,padding:10,usePointStyle:true}} },
      scales:{
        x:{ ticks:{font:{size:10}}, grid:{display:false} },
        y:{ position:'left', ticks:{font:{size:10}}, grid:{color:'rgba(0,0,0,0.04)'}, beginAtZero:true, title:{display:true,text:'Units/container',font:{size:9},color:'#AEAEB2'} },
        yCount:{ position:'right', ticks:{font:{size:10},callback:v=>v+' ctrs'}, grid:{display:false}, beginAtZero:true }
      },
      responsive:true, maintainAspectRatio:true,
      animation:{ duration:900, easing:'easeOutQuart' }
    }});
    // Update summary
    const contSumEl = el('exec-cont-summary');
    if (contSumEl) {
      const avg40 = _weeks.map(w=>w.avg_units_per_40ft).filter(v=>v!=null);
      const avg20 = _weeks.map(w=>w.avg_units_per_20ft).filter(v=>v!=null);
      const parts = [];
      if (avg40.length) parts.push('40ft avg '+Math.round(avg40.reduce((a,b)=>a+b,0)/avg40.length)+' units');
      if (avg20.length) parts.push('20ft avg '+Math.round(avg20.reduce((a,b)=>a+b,0)/avg20.length)+' units');
      contSumEl.textContent = parts.join(' · ') || 'Populates as containers are assigned';
    }

    // Chart 4: Air vs Sea — smooth stacked area
    // Air = warm amber/orange | Sea = deep emerald — maximally distinct
    const AIR_WARM = '#F97316'; // orange
    const SEA_DEEP = '#059669'; // emerald
    _mkChart('chart-airvsea', { type:'line', data:{ labels, datasets:[
      { label:'✈ Air', data:_weeks.map(w=>w.air_units||0), borderColor:AIR_WARM,
        backgroundColor:'rgba(249,115,22,0.12)', borderWidth:2.5, pointRadius:4,
        pointBackgroundColor:'#fff', pointBorderColor:AIR_WARM, pointBorderWidth:2,
        fill:true, tension:0.35 },
      { label:'⛴ Sea', data:_weeks.map(w=>w.sea_units||0), borderColor:SEA_DEEP,
        backgroundColor:'rgba(5,150,105,0.12)', borderWidth:2.5, pointRadius:4,
        pointBackgroundColor:'#fff', pointBorderColor:SEA_DEEP, pointBorderWidth:2,
        fill:true, tension:0.35 },
    ]}, options:{
      plugins:{ legend:{display:true,position:'top',align:'end',labels:{font:{size:10},boxWidth:8,padding:12,usePointStyle:true}} },
      scales:{
        x:{ ticks:{font:{size:10}}, grid:{display:false} },
        y:{ ticks:{font:{size:10}}, grid:{color:'rgba(0,0,0,0.04)'}, beginAtZero:true }
      },
      responsive:true, maintainAspectRatio:true,
      animation:{ duration:1000, easing:'easeOutQuart' }
    }});

    // Radar: large, full-tile
    _renderRadar(baseline);
  }

  // Time to Live: custom SVG segmented horizontal bar per week
  function _renderTTLChart(labels) {
    const container = el('chart-ttl'); if (!container) return;
    const W = container.offsetWidth || 600, H = 200;
    const pad = {t:20, b:36, l:8, r:120};
    const cW = W - pad.l - pad.r, cH = H - pad.t - pad.b;
    const n = _weeks.length; if (!n) { container.innerHTML=''; return; }

    const SEG1_COL = '#C8F902';    // Receiving → VAS (brand green)
    const SEG2_COL = '#8B5CF6';    // VAS → ETA FC (violet/purple)
    const SEG3_COL = '#06B6D4';    // ETA FC → Delivery (cyan)
    const EMPTY_COL= '#F0F0F2';    // segment not yet available

    // Max total for scale
    const totals = _weeks.map(w => {
      const s1 = w.avg_days_to_apply || 0;
      const s2 = w.avg_days_vas_to_eta || 0;
      const s3 = w.avg_days_eta_to_delivery || 0;
      return s1 + s2 + s3;
    });
    const maxDays = Math.max(...totals, 1);

    const rowH = Math.floor((cH) / n);
    const rowPad = Math.max(3, Math.floor(rowH * 0.18));

    let svg = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">`;

    // Subtle vertical grid at 25% intervals
    for (let i=1; i<=4; i++) {
      const gx = pad.l + (i/4)*cW;
      svg += `<line x1="${gx}" y1="${pad.t}" x2="${gx}" y2="${pad.t+cH}" stroke="rgba(0,0,0,0.05)" stroke-width="1"/>`;
      const v = Math.round(maxDays*(i/4));
      svg += `<text x="${gx}" y="${pad.t-4}" font-size="9" fill="#AEAEB2" text-anchor="middle">${v}d</text>`;
    }

    _weeks.forEach((w, i) => {
      const y = pad.t + i * rowH + rowPad;
      const barH = rowH - rowPad*2;
      const s1 = w.avg_days_to_apply || 0;
      const s2 = w.avg_days_vas_to_eta || 0;
      const s3 = w.avg_days_eta_to_delivery || 0;
      const total = s1 + s2 + s3;
      const label = labels[i];

      // If no data at all, show empty placeholder bar
      if (total === 0 && !s1 && !s2 && !s3) {
        svg += `<rect x="${pad.l}" y="${y}" width="${cW}" height="${barH}" rx="4" fill="${EMPTY_COL}"/>`;
        svg += `<text x="${pad.l + cW/2}" y="${y + barH/2 + 3}" font-size="9" fill="#AEAEB2" text-anchor="middle">Data pending</text>`;
        svg += `<text x="${W - pad.r + 6}" y="${y + barH/2 + 3}" font-size="9" fill="#AEAEB2">${label}</text>`;
        return;
      }

      let curX = pad.l;
      const seg = (days, col, label2, isLast) => {
        if (!days) return;
        const segW = (days / maxDays) * cW;
        svg += `<rect x="${curX}" y="${y}" width="${segW}" height="${barH}" rx="${isLast?'0 4 4 0':'0'}" fill="${col}" opacity="0.9"/>`;
        if (segW > 22) svg += `<text x="${curX + segW/2}" y="${y + barH/2 + 3}" font-size="9" font-weight="600" fill="#fff" text-anchor="middle">${days.toFixed(1)}d</text>`;
        curX += segW;
      };

      // Round left corners on first segment
      if (s1) {
        const segW = (s1 / maxDays) * cW;
        svg += `<rect x="${pad.l}" y="${y}" width="${segW}" height="${barH}" rx="4" fill="${SEG1_COL}" opacity="0.9"/>`;
        // Override right corners to square
        svg += `<rect x="${pad.l + segW - 4}" y="${y}" width="4" height="${barH}" fill="${SEG1_COL}" opacity="0.9"/>`;
        if (segW > 22) svg += `<text x="${pad.l + segW/2}" y="${y + barH/2 + 3}" font-size="9" font-weight="600" fill="#1C1C1E" text-anchor="middle">${s1.toFixed(1)}d</text>`;
        curX = pad.l + segW;
      }
      if (s2) {
        const segW = (s2 / maxDays) * cW;
        svg += `<rect x="${curX}" y="${y}" width="${segW}" height="${barH}" fill="${SEG2_COL}" opacity="0.9"/>`;
        if (segW > 22) svg += `<text x="${curX + segW/2}" y="${y + barH/2 + 3}" font-size="9" font-weight="600" fill="#fff" text-anchor="middle">${s2.toFixed(1)}d</text>`;
        curX += segW;
      }
      if (s3) {
        const segW = (s3 / maxDays) * cW;
        svg += `<rect x="${curX}" y="${y}" width="${segW}" height="${barH}" rx="0 4 4 0" fill="${SEG3_COL}" opacity="0.9"/>`;
        // Fix right corners
        svg += `<rect x="${curX}" y="${y}" width="4" height="${barH}" fill="${SEG3_COL}" opacity="0.9"/>`;
        if (segW > 22) svg += `<text x="${curX + segW/2}" y="${y + barH/2 + 3}" font-size="9" font-weight="600" fill="#fff" text-anchor="middle">${s3.toFixed(1)}d</text>`;
        curX += segW;
      }

      // Show empty segments as ghost blocks
      if (!s2 && s1) {
        const ghostX = curX;
        const ghostW = Math.min(40, cW - (curX - pad.l));
        if (ghostW > 0) {
          svg += `<rect x="${ghostX}" y="${y}" width="${ghostW}" height="${barH}" fill="${EMPTY_COL}" rx="0 2 2 0"/>`;
          svg += `<text x="${ghostX + ghostW/2}" y="${y + barH/2 + 3}" font-size="8" fill="#AEAEB2" text-anchor="middle">?</text>`;
        }
      }

      // Total label + week label on right
      const totalStr = total > 0 ? total.toFixed(1)+'d total' : '—';
      svg += `<text x="${W - pad.r + 8}" y="${y + barH/2 - 2}" font-size="9" font-weight="600" fill="#1C1C1E">${totalStr}</text>`;
      svg += `<text x="${W - pad.r + 8}" y="${y + barH/2 + 9}" font-size="8" fill="#AEAEB2">${label}</text>`;
    });

    // Legend at bottom
    const legendY = H - 8;
    const items = [
      {col: SEG1_COL, label: 'Receiving → VAS'},
      {col: SEG2_COL, label: 'VAS → ETA FC'},
      {col: SEG3_COL, label: 'ETA FC → Delivery'},
    ];
    let lx = pad.l;
    items.forEach(item => {
      svg += `<rect x="${lx}" y="${legendY-7}" width="10" height="6" rx="2" fill="${item.col}"/>`;
      svg += `<text x="${lx+14}" y="${legendY}" font-size="9" fill="#6E6E73">${item.label}</text>`;
      lx += 115;
    });

    svg += '</svg>';
    container.innerHTML = svg;

    // Update summary tile
    const ttlSummary = el('exec-ttl-summary');
    if (ttlSummary) {
      const avgTotal = _weeks.reduce((s,w)=>{
        const t=(w.avg_days_to_apply||0)+(w.avg_days_vas_to_eta||0)+(w.avg_days_eta_to_delivery||0);
        return s+t;
      }, 0) / Math.max(_weeks.filter(w=>w.avg_days_to_apply).length, 1);
      ttlSummary.textContent = avgTotal > 0 ? `avg ${avgTotal.toFixed(1)}d end-to-end` : 'segments populate as data is entered';
    }
  }

  // Throughput: custom SVG slope chart — area fill shows cumulative progress
  function _renderThroughputSVG(labels) {
    const container = el('chart-throughput-svg'); if (!container) return;
    const W = container.offsetWidth || 400, H = 190;
    const pad = {t:24, b:32, l:8, r:44};
    const cW = W - pad.l - pad.r, cH = H - pad.t - pad.b;
    const n = _weeks.length;
    if (!n) { container.innerHTML='<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:40px 0;">No data</div>'; return; }

    const maxPl = Math.max(..._weeks.map(w=>w.planned_units||0), 1);
    const xs = _weeks.map((_,i) => pad.l + (i/(Math.max(n-1,1)))*cW);
    const yPl = _weeks.map(w => pad.t + (1 - (w.planned_units||0)/maxPl)*cH);
    const yAp = _weeks.map(w => pad.t + (1 - (w.applied_units||0)/maxPl)*cH);
    const yPct= _weeks.map(w => w.planned_units>0 ? Math.round(w.applied_units/w.planned_units*100) : null);

    // Build smooth path using cubic bezier
    const smooth = (pts) => {
      if (pts.length < 2) return `M${pts[0][0]},${pts[0][1]}`;
      let d = `M${pts[0][0]},${pts[0][1]}`;
      for (let i=1; i<pts.length; i++) {
        const cp1x = pts[i-1][0] + (pts[i][0]-pts[i-1][0])*0.4;
        const cp1y = pts[i-1][1];
        const cp2x = pts[i][0] - (pts[i][0]-pts[i-1][0])*0.4;
        const cp2y = pts[i][1];
        d += ` C${cp1x},${cp1y} ${cp2x},${cp2y} ${pts[i][0]},${pts[i][1]}`;
      }
      return d;
    };

    const plPts  = xs.map((x,i)=>[x, yPl[i]]);
    const apPts  = xs.map((x,i)=>[x, yAp[i]]);
    const baseY  = pad.t + cH;

    // Applied fill path (close bottom)
    const apFill = smooth(apPts) + ` L${xs[n-1]},${baseY} L${xs[0]},${baseY} Z`;
    const plPath = smooth(plPts);
    const apPath = smooth(apPts);

    let svg = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">`;

    // Subtle grid lines
    for (let i=0; i<=4; i++) {
      const gy = pad.t + (i/4)*cH;
      svg += `<line x1="${pad.l}" y1="${gy}" x2="${W-pad.r}" y2="${gy}" stroke="rgba(0,0,0,0.04)" stroke-width="1"/>`;
      const v = Math.round(maxPl*(1-i/4));
      svg += `<text x="${W-pad.r+4}" y="${gy+3}" font-size="9" fill="#AEAEB2">${v>999?Math.round(v/1000)+'k':v}</text>`;
    }

    // Applied area fill
    svg += `<path d="${apFill}" fill="#C8F902" opacity="0.18"/>`;

    // Gap between planned and applied (fill with brand)
    const gapFill = smooth(plPts) + ` L${xs[n-1]},${yAp[n-1]}` + smooth(apPts.slice().reverse().map(p=>p)) + ` Z`;
    // Actually: area between planned line and applied line
    const gapPath = smooth(plPts) + ` ${apPts.slice().reverse().map((p,i)=>i===0?`L${p[0]},${p[1]}`:`L${p[0]},${p[1]}`).join(' ')} Z`;
    svg += `<path d="${gapPath}" fill="${BRAND}" opacity="0.06"/>`;

    // Planned line — dashed subtle
    svg += `<path d="${plPath}" fill="none" stroke="rgba(0,0,0,0.15)" stroke-width="1.5" stroke-dasharray="5,3"/>`;

    // Applied line — solid bold brand green
    svg += `<path d="${apPath}" fill="none" stroke="#8DB800" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>`;

    // Dots + completion % labels
    _weeks.forEach((w,i) => {
      const pct = yPct[i];
      const x = xs[i], y = yAp[i];
      // Dot
      svg += `<circle cx="${x}" cy="${y}" r="4" fill="#fff" stroke="#8DB800" stroke-width="2"/>`;
      // Completion % above dot
      if (pct!=null) {
        const col = pct>=90?'#22C55E':pct>=70?'#F59E0B':BRAND;
        svg += `<text x="${x}" y="${y-10}" font-size="9" font-weight="700" fill="${col}" text-anchor="middle">${pct}%</text>`;
      }
      // Week label
      svg += `<text x="${x}" y="${H-4}" font-size="9" fill="#AEAEB2" text-anchor="middle">${labels[i]}</text>`;
    });

    // Legend
    svg += `<circle cx="${pad.l+8}" cy="8" r="4" fill="#8DB800"/>`;
    svg += `<text x="${pad.l+16}" y="12" font-size="9" fill="#6E6E73">Applied</text>`;
    svg += `<line x1="${pad.l+68}" y1="8" x2="${pad.l+80}" y2="8" stroke="rgba(0,0,0,0.2)" stroke-width="1.5" stroke-dasharray="4,2"/>`;
    svg += `<text x="${pad.l+84}" y="12" font-size="9" fill="#6E6E73">Planned</text>`;

    svg += '</svg>';
    container.innerHTML = svg;
  }

  // Receiving health: custom dot/bubble SVG — no bar chart
  function _renderReceivingDots(baseline) {
    const container = el('chart-receiving-dots'); if (!container) return;
    const weeks = _weeks;
    if (!weeks.length) { container.innerHTML='<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:32px 0;">No data yet</div>'; return; }

    const hasOTR = weeks.some(w=>w.on_time_receiving_pct!=null);
    if (!hasOTR) { container.innerHTML='<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:32px 0;">Receiving date data not yet available</div>'; return; }

    const W=container.offsetWidth||280, dotH=240;
    const pad={t:56,b:32,l:12,r:12}; // increased top pad to center dots in tile
    const n=weeks.length;
    const colW=(W-pad.l-pad.r)/Math.max(n,1);
    const baselineY = baseline.on_time_receiving_pct!=null ? (1-baseline.on_time_receiving_pct/100)*(dotH-pad.t-pad.b)+pad.t : null;

    let svg = `<svg width="${W}" height="${dotH}" viewBox="0 0 ${W} ${dotH}" xmlns="http://www.w3.org/2000/svg" style="overflow:visible;">`;

    // Baseline line
    if (baselineY!=null) {
      svg += `<line x1="${pad.l}" y1="${baselineY}" x2="${W-pad.r}" y2="${baselineY}" stroke="rgba(0,0,0,0.2)" stroke-width="1" stroke-dasharray="4,3"/>`;
      svg += `<text x="${W-pad.r+2}" y="${baselineY+3}" font-size="9" fill="#AEAEB2">best</text>`;
    }

    weeks.forEach((w,i) => {
      const v = w.on_time_receiving_pct;
      const cx = pad.l + colW*i + colW/2;
      const label = shortWk(w.week_start);
      // Label
      svg += `<text x="${cx}" y="${dotH-4}" font-size="9" fill="#AEAEB2" text-anchor="middle">${label}</text>`;
      if (v==null) { svg += `<circle cx="${cx}" cy="${(dotH-pad.t-pad.b)/2+pad.t}" r="5" fill="#E5E7EB"/>`; return; }

      const cy = (1-v/100)*(dotH-pad.t-pad.b)+pad.t;
      const col = v>=85?GREEN:v>=60?AMBER:'#D61A3C';
      const r   = 10 + (v/100)*8; // bigger dot = more on-time

      // Glow
      svg += `<circle cx="${cx}" cy="${cy}" r="${r+6}" fill="${col}" opacity="0.12"/>`;
      // Dot
      svg += `<circle cx="${cx}" cy="${cy}" r="${r}" fill="${col}" opacity="0.9"/>`;
      // Value label inside dot
      svg += `<text x="${cx}" y="${cy+4}" font-size="${r>14?11:9}" font-weight="600" fill="#fff" text-anchor="middle">${Math.round(v)}%</text>`;
    });

    svg += '</svg>';
    container.innerHTML = svg;
  }

  function _renderRadar(baseline) {
    const daysA  = avg(_weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null));
    const otrA   = avg(_weeks.map(w=>w.on_time_receiving_pct).filter(v=>v!=null));
    const totPl  = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp  = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const thruP  = totPl>0?totAp/totPl*100:null;
    const totAir = _weeks.reduce((s,w)=>s+(w.air_units||0),0);
    const totAll = _weeks.reduce((s,w)=>s+(w.air_units||0)+(w.sea_units||0),0);
    const airP   = totAll>0?totAir/totAll*100:null;
    const wtArr  = _weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null&&v>0);
    const wtMean = avg(wtArr);
    const wtStd  = wtArr.length>1?Math.sqrt(wtArr.reduce((s,v)=>s+Math.pow(v-wtMean,2),0)/wtArr.length):0;
    const wtCons = wtMean?Math.max(0,100-(wtStd/wtMean)*100):null;

    const sc = (actual, base, high) => {
      if (actual==null) return 0;
      if (base==null) return Math.min(100,Math.round(actual));
      const r=high?actual/base:base/actual;
      return Math.min(100,Math.max(0,Math.round(r*100)));
    };

    const actuals=[
      sc(otrA, baseline.on_time_receiving_pct, true),
      thruP!=null?Math.min(100,Math.round(thruP)):0,
      sc(daysA, baseline.avg_days_to_apply, false),
      airP!=null?Math.max(0,Math.round(100-airP)):50,
      wtCons!=null?Math.round(wtCons):50,
    ];
    const radarLabels = ['On-time Receiving','VAS Throughput','Processing Speed','Sea Efficiency','Carton Consistency'];

    _mkChart('chart-radar', { type:'radar', data:{ labels:radarLabels, datasets:[
      { label:'Best-week', data:[100,100,100,100,100], borderColor:'rgba(0,0,0,0.12)', backgroundColor:'rgba(0,0,0,0.03)', borderDash:[4,4], borderWidth:1.5, pointRadius:2 },
      { label:'Current', data:actuals, borderColor:BRAND, backgroundColor:BRAND_LT, borderWidth:2.5, pointBackgroundColor:BRAND, pointRadius:5, pointHoverRadius:7 },
    ]}, options:{
      plugins:{ legend:{display:false} },
      scales:{ r:{ min:0, max:100, ticks:{display:false,stepSize:25}, grid:{color:'rgba(0,0,0,0.06)'}, angleLines:{color:'rgba(0,0,0,0.06)'}, pointLabels:{font:{size:11,weight:'500'},color:'#6E6E73'} } },
      responsive:false, maintainAspectRatio:false,
      animation:{ duration:1000, easing:'easeOutQuart' }
    }});

    const legend = el('exec-radar-legend'); if (!legend) return;
    const axes = [
      {label:'On-time Receiving', actual:otrA!=null?fmtPct(otrA):'—', base:baseline.on_time_receiving_pct!=null?fmtPct(baseline.on_time_receiving_pct):'—', score:actuals[0]},
      {label:'VAS Throughput', actual:thruP!=null?fmtPct(thruP):'—', base:baseline.throughput_pct!=null?fmtPct(baseline.throughput_pct*100):'—', score:actuals[1]},
      {label:'Processing Speed', actual:daysA!=null?fmtDays(daysA):'—', base:baseline.avg_days_to_apply!=null?fmtDays(baseline.avg_days_to_apply):'—', score:actuals[2]},
      {label:'Sea Efficiency', actual:airP!=null?fmtPct(airP)+' air':'—', base:'lower is better', score:actuals[3]},
      {label:'Carton Consistency', actual:wtCons!=null?fmtPct(wtCons):'—', base:'higher is better', score:actuals[4]},
    ];
    legend.innerHTML = `
      <div style="font-size:10px;font-weight:600;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">Score vs best-week baseline</div>
      ${axes.map(a=>{
        const sc2=a.score>=80?GREEN:a.score>=60?AMBER:BRAND;
        const bg2=a.score>=80?'rgba(34,197,94,0.08)':a.score>=60?'rgba(245,158,11,0.08)':BRAND_LT;
        const bar=`<div style="height:3px;background:#F5F5F7;border-radius:2px;margin-top:6px;overflow:hidden;"><div style="width:${a.score}%;height:100%;background:${sc2};border-radius:2px;transition:width 1s ease;"></div></div>`;
        return `<div style="padding:10px 12px;background:#F9F9FB;border-radius:10px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:2px;">
            <div style="font-size:11px;font-weight:500;color:#1C1C1E;">${a.label}</div>
            <div style="font-size:14px;font-weight:700;color:${sc2};">${a.score}</div>
          </div>
          <div style="font-size:10px;color:#AEAEB2;">Actual: ${a.actual} · Best: ${a.base}</div>
          ${bar}
        </div>`;
      }).join('')}`;
  }

  // ── Improvement Intelligence ───────────────────────────────────
  function _insightCard(ins) {
    const dotCol=ins.priority==='high'?'#D61A3C':ins.priority==='medium'?AMBER:GREEN;
    const bgCol =ins.priority==='high'?'rgba(214,26,60,0.04)':ins.priority==='medium'?'rgba(245,158,11,0.04)':'rgba(34,197,94,0.04)';
    const catCol=ins.category==='time-to-live'?BRAND:ins.category==='throughput'?'#2E7D9E':AMBER;
    return `<div class="exec-insight-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:14px;border-left:3px solid ${dotCol};">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
        <span style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${catCol};">${ins.category.replace(/-/g,' ')}</span>
        <span style="font-size:9px;font-weight:700;color:${dotCol};background:${bgCol};padding:2px 7px;border-radius:4px;">${ins.priority.toUpperCase()}</span>
      </div>
      <div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:5px;line-height:1.3;">${ins.title}</div>
      <div style="font-size:11px;color:#6E6E73;line-height:1.45;margin-bottom:7px;">${ins.observation}</div>
      <div style="font-size:11px;color:${BRAND};font-weight:500;margin-bottom:6px;">↗ ${ins.impact}</div>
      <div style="font-size:10px;color:#6E6E73;background:#F9F9FB;border-radius:7px;padding:7px 10px;line-height:1.45;">${ins.action}</div>
    </div>`;
  }

  function _renderInsights(baseline) {
    const list=el('exec-insights-list'); if(!list) return;
    const insights=_computeInsights(baseline);
    const pri={high:3,medium:2,low:1};
    insights.sort((a,b)=>(pri[b.priority]||0)-(pri[a.priority]||0));
    list.innerHTML=insights.length?insights.map(_insightCard).join('')
      :'<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">No insights yet — data builds as more weeks accumulate.</div>';
  }

  function _computeInsights(baseline) {
    const ins=[], weeks=_weeks; if(!weeks.length) return ins;
    const last4=weeks.slice(-4);

    const daysArr=weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null);
    const avgDays=avg(daysArr);

    // 1. Processing lag
    if (avgDays!=null&&baseline.avg_days_to_apply!=null) {
      const lag=Math.round((avgDays-baseline.avg_days_to_apply)*10)/10;
      if (lag>0.5) ins.push({category:'time-to-live',priority:lag>3?'high':'medium',
        title:'Processing lag reducing FC delivery speed',
        observation:`VelOzity is averaging ${fmtDays(avgDays)} from receiving to VAS complete. Best-week baseline is ${fmtDays(baseline.avg_days_to_apply)} — a ${fmtDays(lag)} gap.`,
        impact:`Closing this gap moves FC delivery ${fmtDays(lag)} earlier across all active lanes.`,
        action:`Review intake scanning throughput and VAS queue management. Identify what conditions drove the ${fmtDays(baseline.avg_days_to_apply)} best-week performance and replicate them.`});
    }

    // 2. VAS pace
    const thruArr=weeks.map(w=>w.planned_units>0?w.applied_units/w.planned_units*100:null).filter(v=>v!=null);
    const avgThru=avg(thruArr);
    const bestThru=baseline.throughput_pct!=null?baseline.throughput_pct*100:null;
    if (avgThru!=null&&bestThru!=null&&(bestThru-avgThru)>5) {
      const gap=Math.round(bestThru-avgThru);
      ins.push({category:'throughput',priority:gap>20?'high':'medium',
        title:'VAS processing pace below best-week benchmark',
        observation:`VelOzity's average weekly completion is ${fmtPct(avgThru)} vs a best-week pace of ${fmtPct(bestThru)}.`,
        impact:`${gap}% pace gap leaves ~${fmtN(Math.round(weeks.reduce((s,w)=>s+(w.planned_units||0),0)*gap/100/weeks.length))} units unprocessed per week.`,
        action:`Review staffing, equipment availability, and intake scanning rates on below-average weeks. The best-week pace is achievable.`});
    }

    // 3. Air freight creep
    const airPcts=last4.map(w=>(w.air_units+w.sea_units)>0?w.air_units/(w.air_units+w.sea_units)*100:null).filter(v=>v!=null);
    if (airPcts.length>=3&&airPcts[airPcts.length-1]-airPcts[0]>5) {
      const trend=airPcts[airPcts.length-1]-airPcts[0];
      ins.push({category:'risk',priority:trend>15?'high':'medium',
        title:'Air freight reliance trending upward',
        observation:`Air share grew from ${fmtPct(airPcts[0])} to ${fmtPct(airPcts[airPcts.length-1])} over ${airPcts.length} weeks.`,
        impact:`Rising air dependency signals upstream lead-time pressure. Air costs 4–6× sea freight.`,
        action:`Identify manufacturers defaulting to air and engage on earlier dispatch. Review plan lead times to enable sea freight selection.`});
    }

    // 4. Manufacturer constraint
    const supMap=new Map();
    weeks.forEach(w=>(w.suppliers||[]).forEach(s=>{
      if(!supMap.has(s.supplier)) supMap.set(s.supplier,{planned:0,applied:0});
      const m=supMap.get(s.supplier); m.planned+=(s.planned||0); m.applied+=(s.applied||0);
    }));
    const supRows=Array.from(supMap.entries()).map(([n,d])=>({name:n,pct:d.planned>0?Math.min(100,d.applied/d.planned*100):0,planned:d.planned})).filter(s=>s.planned>500).sort((a,b)=>a.pct-b.pct);
    if (supRows.length>=2) {
      const worst=supRows[0], avgP=avg(supRows.map(s=>s.pct));
      if (worst.pct<avgP-15) ins.push({category:'throughput',priority:worst.pct<50?'high':'medium',
        title:`${worst.name} volume lagging`,
        observation:`Units from ${worst.name} are at ${fmtPct(worst.pct)} vs manufacturer avg of ${fmtPct(avgP)}.`,
        impact:`${fmtN(Math.round(worst.planned*(1-worst.pct/100)))} units unprocessed, constraining VelOzity's total throughput.`,
        action:`Investigate receiving delays, missing documentation, or queue prioritisation. Coordinate with manufacturer on dispatch timing.`});
    }

    // 5. At-risk recent week
    const recent=weeks.slice(-2);
    if (recent.length===2) {
      const rPct=recent[1].planned_units>0?recent[1].applied_units/recent[1].planned_units*100:null;
      if (rPct!=null&&rPct<70) ins.push({category:'risk',priority:'high',
        title:'Most recent week at risk',
        observation:`Last week completed only ${fmtPct(rPct)} with ${fmtN(recent[1].planned_units-recent[1].applied_units)} units remaining.`,
        impact:`These units may miss their planned FC arrival window at current pace.`,
        action:`Review VAS queue immediately. Escalate high-impact POs. Consider resource reallocation.`});
    }

    // 6. Late PO pattern
    const lateArr=weeks.map(w=>w.late_pos||0);
    const avgLate=avg(lateArr), maxLate=Math.max(...lateArr);
    if (avgLate>2) ins.push({category:'risk',priority:avgLate>5?'high':'medium',
      title:'Persistent late receiving pattern',
      observation:`Avg ${fmtN(avgLate,1)} POs arrive late each week. Peak was ${maxLate} in one week.`,
      impact:`Late arrivals create VAS queue backlog and delay FC delivery.`,
      action:`Identify manufacturers with recurring late dispatch. Implement earlier booking cut-offs.`});

    // 7. Carton weight variance
    const wtArr=weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null&&v>0);
    if (wtArr.length>=3) {
      const wtAv=avg(wtArr), wtMx=Math.max(...wtArr), wtMn=Math.min(...wtArr);
      if ((wtMx-wtMn)/wtAv*100>30) ins.push({category:'risk',priority:'low',
        title:'Carton weight variance across weeks',
        observation:`Bin weight ranges ${fmtN(wtMn,1)}–${fmtN(wtMx,1)} kg — ${fmtPct((wtMx-wtMn)/wtAv*100)} variance.`,
        impact:`Inconsistent packing makes freight cost and CBM forecasting unreliable.`,
        action:`Review packing standards across manufacturers. Consistent carton weight improves both forecasting and container utilisation.`});
    }

    // 8. Recovery opportunity
    if (avgDays!=null&&baseline.avg_days_to_apply!=null&&avgDays>baseline.avg_days_to_apply) {
      const h=(avgDays-baseline.avg_days_to_apply)/2;
      if (h>0.5) ins.push({category:'time-to-live',priority:'low',
        title:`${fmtDays(h)} faster FC delivery within reach`,
        observation:`Halving the processing gap (${fmtDays(avgDays)} → ${fmtDays(avgDays-h)}) puts VelOzity halfway to best-week performance.`,
        impact:`~${fmtDays(h)} earlier FC delivery achievable through incremental improvements.`,
        action:`Set a 4-week target. Track progress on the Processing Pace chart against the best-week baseline.`});
    }

    return ins;
  }

  // ── Listeners ─────────────────────────────────────────────────
  window.addEventListener('state:ready', () => {
    if ((location.hash||'').toLowerCase()==='#exec') _tryRender();
  });
  new MutationObserver(()=>{
    const ep=el('page-exec');
    if (ep&&ep.style.display!=='none'&&!ep.classList.contains('hidden')&&!el('exec-dashboard-root')) _tryRender();
  }).observe(document.querySelector('#page-exec')||document.body,{attributes:true,attributeFilter:['style','class']});
  if ((location.hash||'').toLowerCase()==='#exec') setTimeout(_tryRender,150);

})();
