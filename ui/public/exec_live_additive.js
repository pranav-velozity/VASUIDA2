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

    <!-- Row 1: Throughput (wide) + Receiving Health (dot chart) -->
    <div style="display:grid;grid-template-columns:3fr 2fr;gap:16px;margin-bottom:16px;">
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:2px;">
          <div style="font-size:13px;font-weight:600;color:#1C1C1E;">Weekly Throughput</div>
          <div id="exec-thru-summary" style="font-size:11px;color:#AEAEB2;"></div>
        </div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Planned vs applied units · completion %</div>
        <canvas id="chart-throughput" height="170"></canvas>
      </div>
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Receiving Health</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">On-time % per week</div>
        <div id="chart-receiving-dots" style="width:100%;"></div>
      </div>
    </div>

    <!-- Row 2: Pipeline line + Air vs Sea area -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Processing Pace</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Avg days receiving → VAS complete</div>
        <canvas id="chart-pipeline" height="170"></canvas>
      </div>
      <div class="exec-chart-card" style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;">
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Air vs Sea Mix</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:16px;">Unit volume by freight mode</div>
        <canvas id="chart-airvsea" height="170"></canvas>
      </div>
    </div>

    <!-- Radar: full width, 50/50 -->
    <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:24px;display:grid;grid-template-columns:1fr 1fr;gap:32px;align-items:center;">
      <div>
        <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Performance Radar</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:20px;">Actual vs best-week baseline · 100 = best achieved</div>
        <div style="position:relative;">
          <canvas id="chart-radar" style="display:block;width:100%;max-width:360px;height:300px;"></canvas>
        </div>
      </div>
      <div id="exec-radar-legend" style="display:flex;flex-direction:column;gap:16px;"></div>
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

    // Chart 1: Throughput — clean bars with smooth completion line
    const totPl = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const sumEl = el('exec-thru-summary');
    if (sumEl) sumEl.textContent = `${fmtN(totAp)} / ${fmtN(totPl)} total`;

    _mkChart('chart-throughput', { type:'bar', data:{ labels, datasets:[
      { label:'Planned', data:_weeks.map(w=>w.planned_units), backgroundColor:'rgba(0,0,0,0.06)', borderRadius:4, borderSkipped:false, order:2 },
      { label:'Applied', data:_weeks.map(w=>w.applied_units), backgroundColor:'#C8F902', borderRadius:4, borderSkipped:false, order:2 },
      { label:'Completion %', data:_weeks.map(w=>w.planned_units>0?Math.round(w.applied_units/w.planned_units*100):null),
        type:'line', borderColor:BRAND, backgroundColor:'transparent', borderWidth:2.5,
        pointBackgroundColor:'#fff', pointBorderColor:BRAND, pointBorderWidth:2, pointRadius:4,
        yAxisID:'y2', order:1, spanGaps:false, tension:0.3 },
    ]}, options:{
      plugins:{ legend:{ display:true, position:'top', align:'end', labels:{font:{size:10},boxWidth:8,padding:12,usePointStyle:true} } },
      scales:{
        x:{ ticks:{font:{size:10}}, grid:{display:false} },
        y:{ ticks:{font:{size:10}}, grid:{color:'rgba(0,0,0,0.04)'}, beginAtZero:true },
        y2:{ position:'right', min:0, max:100, ticks:{font:{size:10},callback:v=>v+'%'}, grid:{display:false} }
      },
      responsive:true, maintainAspectRatio:true,
      animation:{ duration:800, easing:'easeOutQuart' }
    }});

    // Chart 2: Receiving Health — custom SVG dot chart (not Chart.js bar)
    _renderReceivingDots(baseline);

    // Chart 3: Processing Pace — smooth area line
    const pipeData = _weeks.map(w=>w.avg_days_to_apply);
    const hasPipe  = pipeData.some(v=>v!=null);
    _mkChart('chart-pipeline', { type:'line', data:{ labels, datasets:[
      { label:'Days', data:pipeData, borderColor:BRAND, backgroundColor:'rgba(153,0,51,0.08)',
        borderWidth:2.5, pointBackgroundColor:'#fff', pointBorderColor:BRAND, pointBorderWidth:2,
        pointRadius:4, fill:true, tension:0.35, spanGaps:false },
      ...(baseline.avg_days_to_apply!=null ? [{
        label:'Best-week', data:_weeks.map(()=>baseline.avg_days_to_apply),
        borderColor:'rgba(0,0,0,0.2)', borderDash:[5,4], borderWidth:1.5,
        pointRadius:0, fill:false, backgroundColor:'transparent'
      }]:[]),
    ]}, options:{
      plugins:{ legend:{display:true,position:'top',align:'end',labels:{font:{size:10},boxWidth:8,padding:12,usePointStyle:true}} },
      scales:{
        x:{ ticks:{font:{size:10}}, grid:{display:false} },
        y:{ ticks:{font:{size:10},callback:v=>v+'d'}, grid:{color:'rgba(0,0,0,0.04)'}, beginAtZero:true }
      },
      responsive:true, maintainAspectRatio:true,
      animation:{ duration:900, easing:'easeOutQuart' }
    }});
    if (!hasPipe) { const c=el('chart-pipeline'); if(c){const ctx=c.getContext('2d');ctx.save();ctx.fillStyle='#AEAEB2';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.fillText('Populates as receiving & VAS data accumulates',c.width/2,90);ctx.restore();} }

    // Chart 4: Air vs Sea — smooth stacked area
    _mkChart('chart-airvsea', { type:'line', data:{ labels, datasets:[
      { label:'Air', data:_weeks.map(w=>w.air_units||0), borderColor:AIR_COL,
        backgroundColor:'rgba(74,144,217,0.15)', borderWidth:2, pointRadius:3,
        pointBackgroundColor:'#fff', pointBorderColor:AIR_COL, pointBorderWidth:2,
        fill:true, tension:0.35 },
      { label:'Sea', data:_weeks.map(w=>w.sea_units||0), borderColor:SEA_COL,
        backgroundColor:'rgba(46,125,158,0.25)', borderWidth:2, pointRadius:3,
        pointBackgroundColor:'#fff', pointBorderColor:SEA_COL, pointBorderWidth:2,
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

  // Receiving health: custom dot/bubble SVG — no bar chart
  function _renderReceivingDots(baseline) {
    const container = el('chart-receiving-dots'); if (!container) return;
    const weeks = _weeks;
    if (!weeks.length) { container.innerHTML='<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:32px 0;">No data yet</div>'; return; }

    const hasOTR = weeks.some(w=>w.on_time_receiving_pct!=null);
    if (!hasOTR) { container.innerHTML='<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:32px 0;">Receiving date data not yet available</div>'; return; }

    const W=container.offsetWidth||280, dotH=200;
    const pad={t:16,b:32,l:12,r:12};
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
      responsive:true, maintainAspectRatio:false,
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
