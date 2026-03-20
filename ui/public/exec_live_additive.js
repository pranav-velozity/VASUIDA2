/*
 * Pinpoint Exec — Cross-Week Executive Dashboard v3
 * Improvement Intelligence · 4 Charts · No scorecard
 */
(function ExecLiveAdditive() {
  'use strict';

  const BRAND    = '#990033';
  const BRAND_LT = 'rgba(153,0,51,0.08)';
  const AIR_COL  = '#4A90D9';
  const SEA_COL  = '#2E7D9E';
  const GREEN    = '#22C55E';
  const AMBER    = '#F59E0B';
  const GRAY     = '#E5E7EB';

  const _apiBase = (() => {
    const b = String(document.querySelector('meta[name="api-base"]')?.content || location.origin).replace(/\/+$/, '');
    return /\/api$/i.test(b) ? b : b + '/api';
  })();

  async function _getToken() {
    try { return await window.Clerk?.session?.getToken(); } catch { return null; }
  }

  async function _api(path) {
    const token = await _getToken();
    const res = await fetch(`${_apiBase}${path}`, {
      headers: Object.assign({ 'content-type': 'application/json' }, token ? { 'authorization': 'Bearer ' + token } : {})
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  let _weeks = [], _range = 8, _loading = false;
  const _charts = {};

  const fmtN   = (n, d) => { d=d||0; if(n==null||isNaN(n)) return '—'; return Number(n).toLocaleString('en-AU',{minimumFractionDigits:d,maximumFractionDigits:d}); };
  const fmtPct = n => n==null ? '—' : Math.round(n)+'%';
  const fmtDays= n => n==null ? '—' : Number(n).toFixed(1)+'d';
  const el     = id => document.getElementById(id);
  const shortWk= ws => { try{ const d=new Date(ws+'T00:00:00Z'); return d.toLocaleDateString('en-AU',{month:'short',day:'numeric',timeZone:'UTC'}); }catch{ return ws; } };
  const avg    = arr => arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : null;
  const orNull = v => (v==null||isNaN(v)||v===Infinity||v===-Infinity) ? null : v;

  function computeBaseline(weeks) {
    if (!weeks.length) return {};
    const s_days = weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null).sort((a,b)=>a-b);
    const s_otr  = weeks.map(w=>w.on_time_receiving_pct).filter(v=>v!=null).sort((a,b)=>b-a);
    const s_thru = weeks.map(w=>w.planned_units>0?w.applied_units/w.planned_units:0).sort((a,b)=>b-a);
    const q = arr => arr.length ? arr[Math.floor(arr.length*0.25)] : null;
    return { avg_days_to_apply:q(s_days), on_time_receiving_pct:q(s_otr), throughput_pct:q(s_thru) };
  }

  function _tryRender() {
    const host = el('page-exec');
    if (!host || host.style.display==='none' || host.classList.contains('hidden')) return;
    if (!el('exec-dashboard-root')) { host.innerHTML = _shellHTML(); _wireRangeBar(); }
    _loadAndRender();
  }

  function _shellHTML() {
    const btn = w => `<button class="exec-range-btn" data-weeks="${w}" style="font-size:11px;font-weight:500;padding:5px 12px;border-radius:6px;border:0.5px solid rgba(0,0,0,0.12);background:${w===8?'#1C1C1E':'#fff'};color:${w===8?'#fff':'#6E6E73'};cursor:pointer;">${w}W</button>`;
    const card = (id, title, sub, canH) => `<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;"><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">${title}</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:12px;">${sub}</div><canvas id="${id}" height="${canH}"></canvas></div>`;
    return `<div id="exec-dashboard-root" style="min-height:100vh;background:#F9F9FB;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:0 0 40px;">
<div id="exec-range-bar" style="position:sticky;top:0;z-index:100;background:#fff;border-bottom:0.5px solid rgba(0,0,0,0.08);padding:10px 24px;display:flex;align-items:center;gap:8px;">
  <span style="font-size:11px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-right:4px;">Range</span>
  ${[4,8,12,16].map(btn).join('')}
  <span id="exec-range-label" style="font-size:11px;color:#AEAEB2;margin-left:8px;"></span>
  <div id="exec-loading" style="margin-left:auto;font-size:11px;color:#AEAEB2;display:none;">Loading…</div>
</div>
<div style="padding:20px 24px;display:grid;grid-template-columns:1fr 320px;gap:20px;align-items:start;">
  <div>
    <div id="exec-kpis" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;"></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
      ${card('chart-throughput','Weekly Throughput','Planned vs applied units · cumulative %',160)}
      ${card('chart-receiving','Receiving Health','% POs received on time per week',160)}
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
      ${card('chart-pipeline','End-to-End Pipeline','Avg days: receiving → VAS complete',160)}
      ${card('chart-airvsea','Air vs Sea Trend','Unit volume by freight mode per week',160)}
    </div>
    <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;display:grid;grid-template-columns:260px 1fr;gap:20px;align-items:center;">
      <div>
        <div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Performance Radar</div>
        <div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">Actual vs best-week baseline</div>
        <canvas id="chart-radar" width="240" height="240" style="display:block;"></canvas>
      </div>
      <div id="exec-radar-legend" style="display:flex;flex-direction:column;gap:10px;"></div>
    </div>
  </div>
  <div id="exec-intelligence" style="position:sticky;top:60px;max-height:calc(100vh - 80px);overflow-y:auto;">
    <div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:12px;display:flex;align-items:center;gap:8px;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="${BRAND}" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      Improvement Intelligence
    </div>
    <div id="exec-insights-list" style="display:flex;flex-direction:column;gap:10px;"><div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">Loading…</div></div>
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
      const toDate = new Date();
      const fromDate = new Date(toDate);
      fromDate.setUTCDate(fromDate.getUTCDate() - (_range * 7));
      const from = fromDate.toISOString().slice(0,10);
      const to   = toDate.toISOString().slice(0,10);
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
      _renderError('Failed to load executive data: '+(e.message||e));
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
    if (!_weeks.length) { _renderError('No data found for this period.'); return; }
    const baseline = computeBaseline(_weeks);
    _renderKPIs(baseline);
    _renderCharts(baseline);
    _renderInsights(baseline);
  }

  // ── KPIs ──────────────────────────────────────────────────────
  function _kpiTile(label, value, badge, badgeCol, sub) {
    return `<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;">
      <div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">${label}</div>
      <div style="display:flex;align-items:baseline;gap:8px;">
        <div style="font-size:22px;font-weight:600;color:#1C1C1E;letter-spacing:-0.02em;">${value}</div>
        ${badge?`<div style="font-size:13px;font-weight:600;color:${badgeCol};">${badge}</div>`:''}
      </div>
      <div style="font-size:10px;color:#AEAEB2;margin-top:4px;">${sub}</div>
    </div>`;
  }

  function _renderKPIs(baseline) {
    const kpis = el('exec-kpis');
    if (!kpis) return;
    const totPl = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const pct   = totPl>0 ? Math.round(totAp/totPl*100) : 0;
    const daysA = avg(_weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null));
    const wtA   = avg(_weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null));
    const totAir= _weeks.reduce((s,w)=>s+(w.air_units||0),0);
    const totSea= _weeks.reduce((s,w)=>s+(w.sea_units||0),0);
    const airP  = (totAir+totSea)>0 ? Math.round(totAir/(totAir+totSea)*100) : 0;
    const last2 = _weeks.slice(-2);
    const daysArrow = last2.length===2&&last2[0].avg_days_to_apply!=null&&last2[1].avg_days_to_apply!=null
      ? (last2[1].avg_days_to_apply<last2[0].avg_days_to_apply?'↓':'↑') : '';
    const airArrow = last2.length===2 ? (()=>{
      const a0=last2[0].planned_units>0?last2[0].air_units/last2[0].planned_units:0;
      const a1=last2[1].planned_units>0?last2[1].air_units/last2[1].planned_units:0;
      return a1>a0?'↑':a1<a0?'↓':'';
    })() : '';
    kpis.innerHTML = [
      _kpiTile('Units Applied / Planned', `${fmtN(totAp)} / ${fmtN(totPl)}`, fmtPct(pct), pct>=90?GREEN:pct>=70?AMBER:BRAND, 'Season-to-date completion'),
      _kpiTile('Avg Days to Apply', fmtDays(daysA), daysArrow, daysArrow==='↓'?GREEN:daysArrow==='↑'?BRAND:'#AEAEB2', 'Receiving → VAS complete'+(baseline.avg_days_to_apply!=null?' · best '+fmtDays(baseline.avg_days_to_apply):'')),
      _kpiTile('Avg Carton Weight', wtA!=null?fmtN(wtA,1)+' kg':'—', '', '#6E6E73', 'Average weight per bin from manifest'),
      _kpiTile('Air vs Sea Split', airP+'% air', airArrow, airArrow==='↑'?AMBER:GREEN, `${fmtN(totAir)} air · ${fmtN(totSea)} sea units`),
    ].join('');
  }

  // ── Charts ────────────────────────────────────────────────────
  function _destroyCharts() {
    Object.keys(_charts).forEach(k=>{ try{_charts[k].destroy();}catch{} delete _charts[k]; });
  }
  function _mkChart(id, config) {
    const canvas = el(id);
    if (!canvas) return;
    if (!window.Chart) { console.warn('[exec] Chart.js not loaded'); return; }
    try { _charts[id] = new window.Chart(canvas, config); } catch(e){ console.warn('[exec] chart failed',id,e); }
  }
  function _baseOpts(extra) {
    return { plugins:{legend:{display:true,labels:{font:{size:10},boxWidth:10}}}, scales:Object.assign({x:{ticks:{font:{size:10}},grid:{display:false}},y:{ticks:{font:{size:10}},grid:{color:'rgba(0,0,0,0.04)'}}},extra||{}), responsive:true, maintainAspectRatio:true };
  }

  function _renderCharts(baseline) {
    _destroyCharts();
    const labels = _weeks.map(w=>shortWk(w.week_start));

    // Chart 1: Throughput
    _mkChart('chart-throughput', { type:'bar', data:{ labels, datasets:[
      {label:'Planned', data:_weeks.map(w=>w.planned_units), backgroundColor:GRAY, borderRadius:3, order:2},
      {label:'Applied', data:_weeks.map(w=>w.applied_units), backgroundColor:'#C8F902', borderRadius:3, order:2},
      {label:'Completion %', data:_weeks.map(w=>w.planned_units>0?Math.round(w.applied_units/w.planned_units*100):null),
        type:'line', borderColor:BRAND, backgroundColor:'transparent', borderWidth:2, pointBackgroundColor:BRAND, pointRadius:3, yAxisID:'y2', order:1, spanGaps:false},
    ]}, options:_baseOpts({y2:{position:'right',min:0,max:100,ticks:{font:{size:10},callback:v=>v+'%'},grid:{display:false}}}) });

    // Chart 2: Receiving Health
    const otrData = _weeks.map(w=>w.on_time_receiving_pct);
    const hasOTR  = otrData.some(v=>v!=null);
    const otrDs   = [{label:'On-time %', data:otrData.map(v=>v!=null?v:null), backgroundColor:otrData.map(v=>v==null?'rgba(0,0,0,0)':v>=85?GREEN:v>=60?AMBER:'#D61A3C'), borderRadius:3, spanGaps:false}];
    if (baseline.on_time_receiving_pct!=null) otrDs.push({label:'Best-week baseline', data:_weeks.map(()=>baseline.on_time_receiving_pct), type:'line', borderColor:'#6E6E73', borderDash:[4,4], borderWidth:1.5, pointRadius:0, backgroundColor:'transparent'});
    _mkChart('chart-receiving', { type:'bar', data:{labels, datasets:otrDs}, options:_baseOpts({y:{min:0,max:100,ticks:{font:{size:10},callback:v=>v+'%'},grid:{color:'rgba(0,0,0,0.04)'}}}) });
    if (!hasOTR) { const c=el('chart-receiving'); if(c){const ctx=c.getContext('2d');ctx.fillStyle='#AEAEB2';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.fillText('No receiving date data available yet',c.width/2,c.height/2);} }

    // Chart 3: Pipeline (only show avg_days_to_apply — transit is usually null)
    const pipeHasData = _weeks.some(w=>w.avg_days_to_apply!=null);
    _mkChart('chart-pipeline', { type:'bar', data:{labels, datasets:[
      {label:'Recv → VAS (days)', data:_weeks.map(w=>w.avg_days_to_apply), backgroundColor:'rgba(153,0,51,0.7)', borderRadius:3, spanGaps:false},
    ]}, options:_baseOpts({y:{ticks:{font:{size:10},callback:v=>v+'d'},grid:{color:'rgba(0,0,0,0.04)'}}}) });
    if (!pipeHasData) { const c=el('chart-pipeline'); if(c){const ctx=c.getContext('2d');ctx.fillStyle='#AEAEB2';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.fillText('Data populates as lanes are processed',c.width/2,c.height/2);} }

    // Chart 4: Air vs Sea
    const hasFreight = _weeks.some(w=>(w.air_units||0)+(w.sea_units||0)>0);
    _mkChart('chart-airvsea', { type:'bar', data:{labels, datasets:[
      {label:'Air', data:_weeks.map(w=>w.air_units||0), backgroundColor:AIR_COL, borderRadius:3, stack:'f'},
      {label:'Sea', data:_weeks.map(w=>w.sea_units||0), backgroundColor:SEA_COL, borderRadius:3, stack:'f'},
    ]}, options:Object.assign(_baseOpts(),{scales:{x:{stacked:true,ticks:{font:{size:10}},grid:{display:false}},y:{stacked:true,ticks:{font:{size:10}},grid:{color:'rgba(0,0,0,0.04)'}}}}) });

    // Radar
    _renderRadar(baseline);
  }

  function _renderRadar(baseline) {
    const daysA  = avg(_weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null));
    const otrA   = avg(_weeks.map(w=>w.on_time_receiving_pct).filter(v=>v!=null));
    const totPl  = _weeks.reduce((s,w)=>s+(w.planned_units||0),0);
    const totAp  = _weeks.reduce((s,w)=>s+(w.applied_units||0),0);
    const thruP  = totPl>0 ? totAp/totPl*100 : null;
    const totAir = _weeks.reduce((s,w)=>s+(w.air_units||0),0);
    const totAll = _weeks.reduce((s,w)=>s+(w.air_units||0)+(w.sea_units||0),0);
    const airP   = totAll>0 ? totAir/totAll*100 : null;
    const wtArr  = _weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null);
    const wtMean = avg(wtArr);
    const wtStd  = wtArr.length>1 ? Math.sqrt(wtArr.reduce((s,v)=>s+Math.pow(v-wtMean,2),0)/wtArr.length) : 0;
    const wtCons = wtMean ? Math.max(0,100-(wtStd/wtMean)*100) : null;

    function sc(actual, base, higherBetter) {
      if (actual==null) return 0;
      if (base==null) return Math.min(100,Math.round(actual));
      const r = higherBetter ? actual/base : base/actual;
      return Math.min(100,Math.max(0,Math.round(r*100)));
    }

    const actuals = [
      sc(otrA, baseline.on_time_receiving_pct, true),
      thruP!=null ? Math.min(100,Math.round(thruP)) : 0,
      sc(daysA, baseline.avg_days_to_apply, false),
      airP!=null ? Math.max(0,Math.round(100-airP)) : 50,
      wtCons!=null ? Math.round(wtCons) : 50,
    ];

    _mkChart('chart-radar', { type:'radar', data:{ labels:['On-time Receiving','VAS Throughput','Processing Speed','Sea Efficiency','Carton Consistency'], datasets:[
      {label:'Best-week baseline', data:[100,100,100,100,100], borderColor:'rgba(0,0,0,0.15)', backgroundColor:'rgba(0,0,0,0.04)', borderDash:[4,4], borderWidth:1.5, pointRadius:2},
      {label:'Current period', data:actuals, borderColor:BRAND, backgroundColor:BRAND_LT, borderWidth:2, pointBackgroundColor:BRAND, pointRadius:3},
    ]}, options:{plugins:{legend:{display:false}}, scales:{r:{min:0,max:100,ticks:{display:false},grid:{color:'rgba(0,0,0,0.06)'},pointLabels:{font:{size:10}}}}, responsive:false, maintainAspectRatio:false} });

    const legend = el('exec-radar-legend');
    if (!legend) return;
    const axes = [
      {label:'On-time Receiving', actual:otrA!=null?fmtPct(otrA):'—', base:baseline.on_time_receiving_pct!=null?fmtPct(baseline.on_time_receiving_pct):'—', score:actuals[0]},
      {label:'VAS Throughput', actual:thruP!=null?fmtPct(thruP):'—', base:baseline.throughput_pct!=null?fmtPct(baseline.throughput_pct*100):'—', score:actuals[1]},
      {label:'Processing Speed', actual:daysA!=null?fmtDays(daysA):'—', base:baseline.avg_days_to_apply!=null?fmtDays(baseline.avg_days_to_apply):'—', score:actuals[2]},
      {label:'Sea Efficiency', actual:airP!=null?fmtPct(airP)+' air':'—', base:'Lower = better', score:actuals[3]},
      {label:'Carton Consistency', actual:wtCons!=null?fmtPct(wtCons):'—', base:'Higher = better', score:actuals[4]},
    ];
    legend.innerHTML = axes.map(a => {
      const sc2 = a.score>=80?GREEN:a.score>=60?AMBER:BRAND;
      const bg2 = a.score>=80?'rgba(34,197,94,0.1)':a.score>=60?'rgba(245,158,11,0.1)':BRAND_LT;
      return `<div style="display:flex;align-items:center;gap:10px;">
        <div style="width:32px;height:32px;border-radius:8px;background:${bg2};display:flex;align-items:center;justify-content:center;flex-shrink:0;">
          <span style="font-size:11px;font-weight:700;color:${sc2};">${a.score}</span>
        </div>
        <div>
          <div style="font-size:11px;font-weight:500;color:#1C1C1E;">${a.label}</div>
          <div style="font-size:10px;color:#AEAEB2;">Actual: ${a.actual} · Best: ${a.base}</div>
        </div>
      </div>`;
    }).join('');
  }

  // ── Improvement Intelligence (dynamic, computed from real data) ─
  function _insightCard(ins) {
    const dotCol = ins.priority==='high'?'#D61A3C':ins.priority==='medium'?AMBER:GREEN;
    const bgCol  = ins.priority==='high'?'rgba(214,26,60,0.04)':ins.priority==='medium'?'rgba(245,158,11,0.04)':'rgba(34,197,94,0.04)';
    const catCol = ins.category==='time-to-live'?BRAND:ins.category==='throughput'?SEA_COL:AMBER;
    return `<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:12px;border-left:3px solid ${dotCol};">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
        <span style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${catCol};">${ins.category.replace(/-/g,' ')}</span>
        <span style="font-size:9px;font-weight:700;color:${dotCol};background:${bgCol};padding:2px 7px;border-radius:4px;">${ins.priority.toUpperCase()}</span>
      </div>
      <div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:4px;">${ins.title}</div>
      <div style="font-size:11px;color:#6E6E73;line-height:1.4;margin-bottom:6px;">${ins.observation}</div>
      <div style="font-size:11px;color:${BRAND};font-weight:500;margin-bottom:4px;">Impact: ${ins.impact}</div>
      <div style="font-size:10px;color:#6E6E73;background:#F9F9FB;border-radius:6px;padding:6px 8px;line-height:1.4;">${ins.action}</div>
    </div>`;
  }

  function _renderInsights(baseline) {
    const list = el('exec-insights-list');
    if (!list) return;
    const insights = _computeInsights(baseline);
    const pri = {high:3,medium:2,low:1};
    insights.sort((a,b)=>(pri[b.priority]||0)-(pri[a.priority]||0));
    list.innerHTML = insights.length
      ? insights.map(_insightCard).join('')
      : '<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">No insights available yet — check back as more weeks of data accumulate.</div>';
  }

  function _computeInsights(baseline) {
    const ins = [], weeks = _weeks;
    if (!weeks.length) return ins;

    // 1. Receiving lag impact on FC delivery
    const daysArr = weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null);
    const avgDays = avg(daysArr);
    if (avgDays!=null && baseline.avg_days_to_apply!=null) {
      const lag = Math.round((avgDays-baseline.avg_days_to_apply)*10)/10;
      if (lag>0.5) ins.push({ category:'time-to-live', priority:lag>3?'high':'medium',
        title:'Receiving-to-VAS processing gap affecting FC delivery',
        observation:`VelOzity is averaging ${fmtDays(avgDays)} from receiving to VAS complete, vs a best-week baseline of ${fmtDays(baseline.avg_days_to_apply)} — a ${fmtDays(lag)} gap.`,
        impact:`Closing this gap recovers ~${fmtDays(lag)} on ETA FC across all active lanes.`,
        action:`Review intake scanning throughput and VAS queue management. Identify which weeks achieved ${fmtDays(baseline.avg_days_to_apply)} and replicate those conditions.`
      });
    }

    // 2. VAS throughput vs best-week pace (VelOzity ops focus)
    const thruArr = weeks.map(w=>w.planned_units>0?w.applied_units/w.planned_units*100:null).filter(v=>v!=null);
    const avgThru = avg(thruArr);
    const bestThru = baseline.throughput_pct!=null ? baseline.throughput_pct*100 : null;
    if (avgThru!=null && bestThru!=null) {
      const gap = Math.round(bestThru-avgThru);
      if (gap>5) ins.push({ category:'throughput', priority:gap>20?'high':'medium',
        title:'VAS processing pace below best-week benchmark',
        observation:`VelOzity's average weekly completion is ${fmtPct(avgThru)} vs a best-week pace of ${fmtPct(bestThru)} across this period.`,
        impact:`A ${gap}% pace gap leaves ~${fmtN(Math.round(weeks.reduce((s,w)=>s+(w.planned_units||0),0)*gap/100/weeks.length))} units unprocessed per week on average.`,
        action:`Review staffing levels, equipment availability, and intake scanning rates on below-average weeks. The best-week pace is achievable — identify what was different.`
      });
    }

    // 3. Air freight creep
    const last4 = weeks.slice(-4);
    const airPcts = last4.map(w=>(w.air_units+w.sea_units)>0?w.air_units/(w.air_units+w.sea_units)*100:null).filter(v=>v!=null);
    if (airPcts.length>=3) {
      const trend = airPcts[airPcts.length-1]-airPcts[0];
      if (trend>5) ins.push({ category:'risk', priority:trend>15?'high':'medium',
        title:'Air freight reliance increasing',
        observation:`Air freight share has grown from ${fmtPct(airPcts[0])} to ${fmtPct(airPcts[airPcts.length-1])} over the last ${airPcts.length} weeks.`,
        impact:`Rising air dependency signals upstream lead-time pressure. Air freight typically costs 4–6× sea.`,
        action:`Identify which manufacturers are defaulting to air and engage them on earlier dispatch windows. Review plan upload lead times to enable sea freight selection.`
      });
    }

    // 4. Manufacturer performance impact (reframed — supplier = external manufacturer)
    const supMap = new Map();
    weeks.forEach(w=>{
      (w.suppliers||[]).forEach(s=>{
        if (!supMap.has(s.supplier)) supMap.set(s.supplier,{planned:0,applied:0});
        const m=supMap.get(s.supplier); m.planned+=(s.planned||0); m.applied+=(s.applied||0);
      });
    });
    const supRows = Array.from(supMap.entries())
      .map(([name,d])=>({name, pct:d.planned>0?Math.min(100,d.applied/d.planned*100):0, planned:d.planned}))
      .filter(s=>s.planned>500)
      .sort((a,b)=>a.pct-b.pct);
    if (supRows.length>=2) {
      const worst=supRows[0];
      const avgPct=avg(supRows.map(s=>s.pct));
      if (worst.pct < avgPct-15) ins.push({ category:'throughput', priority:worst.pct<50?'high':'medium',
        title:`${worst.name} volume is limiting throughput`,
        observation:`Units from ${worst.name} are at ${fmtPct(worst.pct)} completion — ${fmtPct(avgPct-worst.pct)} below the manufacturer average of ${fmtPct(avgPct)}.`,
        impact:`${fmtN(Math.round(worst.planned*(1-worst.pct/100)))} units from this manufacturer remain unprocessed, constraining VelOzity's overall throughput numbers.`,
        action:`Investigate whether this is a receiving delay, missing documentation, or VAS queue prioritisation issue. Coordinate with the manufacturer on dispatch timing.`
      });
    }

    // 5. Pipeline bottleneck identification
    const vasArr = weeks.map(w=>w.avg_days_to_apply).filter(v=>v!=null);
    const avgVas = avg(vasArr);
    if (avgVas!=null) {
      const target = baseline.avg_days_to_apply || 2;
      if (avgVas > target*1.5) ins.push({ category:'time-to-live', priority:'medium',
        title:'Receiving → VAS is the primary time constraint',
        observation:`The receiving-to-VAS-complete segment is averaging ${fmtDays(avgVas)}, which is the dominant factor in overall pipeline duration.`,
        impact:`Every day saved in this segment directly advances the FC arrival date.`,
        action:`Prioritise intake queue clearing at week start. Consider same-day VAS processing for high-volume POs received early in the week.`
      });
    }

    // 6. Carton weight anomaly
    const wtArr = weeks.map(w=>w.avg_weight_kg).filter(v=>v!=null&&v>0);
    if (wtArr.length>=3) {
      const wtAv=avg(wtArr), wtMx=Math.max(...wtArr), wtMn=Math.min(...wtArr);
      const vari=(wtMx-wtMn)/wtAv*100;
      if (vari>30) ins.push({ category:'risk', priority:'low',
        title:'Carton weight variance across weeks',
        observation:`Average bin weight ranges from ${fmtN(wtMn,1)} kg to ${fmtN(wtMx,1)} kg — a ${fmtPct(vari)} variance across weeks.`,
        impact:`High weight variance makes freight cost and CBM forecasting less reliable.`,
        action:`Review carton packing standards across manufacturers. Consistent packing improves both cost forecasting and container utilisation.`
      });
    }

    // 7. At-risk most recent week
    const recent = weeks.slice(-2);
    if (recent.length===2) {
      const recPct=recent[1].planned_units>0?recent[1].applied_units/recent[1].planned_units*100:null;
      if (recPct!=null && recPct<70) ins.push({ category:'risk', priority:'high',
        title:'Most recent week at risk of missing FC deadline',
        observation:`Last week completed only ${fmtPct(recPct)} of planned units, leaving ${fmtN(recent[1].planned_units-recent[1].applied_units)} units unprocessed.`,
        impact:`At current pace, these units may miss their planned ETA FC window.`,
        action:`Review VAS queue immediately. Prioritise highest-impact POs. Consider escalating to operations management for resource reallocation.`
      });
    }

    // 8. Late PO pattern
    const lateArr=weeks.map(w=>w.late_pos||0);
    const avgLate=avg(lateArr), maxLate=Math.max(...lateArr);
    if (avgLate>2) ins.push({ category:'risk', priority:avgLate>5?'high':'medium',
      title:'Persistent late receiving pattern',
      observation:`An average of ${fmtN(avgLate,1)} POs arrive late each week. The worst week had ${maxLate} late POs.`,
      impact:`Late arrivals create VAS queue backlog and delay FC delivery for downstream POs.`,
      action:`Identify manufacturers with recurring late dispatch. Implement earlier booking cut-offs and review transit time assumptions in the plan.`
    });

    // 9. Recovery opportunity
    if (avgDays!=null && baseline.avg_days_to_apply!=null && avgDays>baseline.avg_days_to_apply) {
      const halfLag=(avgDays-baseline.avg_days_to_apply)/2;
      if (halfLag>0.5) ins.push({ category:'time-to-live', priority:'low',
        title:`Recoverable: ${fmtDays(halfLag)} faster FC delivery within reach`,
        observation:`Halving the current processing gap (${fmtDays(avgDays)} → ${fmtDays(avgDays-halfLag)}) would move VelOzity halfway to best-week performance.`,
        impact:`~${fmtDays(halfLag)} earlier FC delivery achievable through incremental process improvements.`,
        action:`Set a 4-week improvement target. Use the Receiving Health chart to track week-on-week progress toward the baseline.`
      });
    }

    return ins;
  }

  // ── Listeners ─────────────────────────────────────────────────
  window.addEventListener('state:ready', () => {
    if ((location.hash||'').toLowerCase()==='#exec') _tryRender();
  });

  new MutationObserver(() => {
    const ep = el('page-exec');
    if (ep && ep.style.display!=='none' && !ep.classList.contains('hidden') && !el('exec-dashboard-root')) _tryRender();
  }).observe(document.querySelector('#page-exec') || document.body, {attributes:true, attributeFilter:['style','class']});

  if ((location.hash||'').toLowerCase()==='#exec') setTimeout(_tryRender, 150);

})();
