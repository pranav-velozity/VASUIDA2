/*
 * Pinpoint Exec — Cross-Week Executive Dashboard v2
 * Improvement Intelligence · Supplier Scorecard · 5 Charts
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

  function fmtN(n, dec) { dec = dec || 0; if (n == null || isNaN(n)) return '—'; return Number(n).toLocaleString('en-AU', { minimumFractionDigits: dec, maximumFractionDigits: dec }); }
  function fmtPct(n) { return n == null ? '—' : Math.round(n) + '%'; }
  function fmtDays(n) { return n == null ? '—' : Number(n).toFixed(1) + 'd'; }
  function el(id) { return document.getElementById(id); }
  function shortWk(ws) { try { const d = new Date(ws + 'T00:00:00Z'); return d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric', timeZone: 'UTC' }); } catch { return ws; } }
  function avg(arr) { return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null; }

  function computeBaseline(weeks) {
    if (!weeks.length) return {};
    const s_days = weeks.map(w => w.avg_days_to_apply).filter(v => v != null).sort((a, b) => a - b);
    const s_otr  = weeks.map(w => w.on_time_receiving_pct).filter(v => v != null).sort((a, b) => b - a);
    const s_thru = weeks.map(w => w.planned_units > 0 ? w.applied_units / w.planned_units : 0).sort((a, b) => b - a);
    const q = (arr) => arr.length ? arr[Math.floor(arr.length * 0.25)] : null;
    return { avg_days_to_apply: q(s_days), on_time_receiving_pct: q(s_otr), throughput_pct: q(s_thru) };
  }

  function _tryRender() {
    const host = el('page-exec');
    if (!host) return;
    if (!host.style.display || host.style.display === 'none') return;
    if (!el('exec-dashboard-root')) { host.innerHTML = _shellHTML(); _wireRangeBar(); }
    _loadAndRender();
  }

  function _shellHTML() {
    return '<div id="exec-dashboard-root" style="min-height:100vh;background:#F9F9FB;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:0 0 40px;">'
    + '<div id="exec-range-bar" style="position:sticky;top:0;z-index:100;background:#fff;border-bottom:0.5px solid rgba(0,0,0,0.08);padding:10px 24px;display:flex;align-items:center;gap:8px;">'
    + '<span style="font-size:11px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-right:4px;">Range</span>'
    + [4,8,12,16].map(w => '<button class="exec-range-btn" data-weeks="'+w+'" style="font-size:11px;font-weight:500;padding:5px 12px;border-radius:6px;border:0.5px solid rgba(0,0,0,0.12);background:'+(w===8?'#1C1C1E':'#fff')+';color:'+(w===8?'#fff':'#6E6E73')+';cursor:pointer;">'+w+'W</button>').join('')
    + '<span id="exec-range-label" style="font-size:11px;color:#AEAEB2;margin-left:8px;"></span>'
    + '<div id="exec-loading" style="margin-left:auto;font-size:11px;color:#AEAEB2;display:none;">Loading\u2026</div>'
    + '</div>'
    + '<div style="padding:20px 24px;display:grid;grid-template-columns:1fr 320px;gap:20px;align-items:start;">'
    + '<div>'
    + '<div id="exec-kpis" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;"></div>'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;"><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Weekly Throughput</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">Planned vs applied \xb7 completion %</div><canvas id="chart-throughput" height="160"></canvas></div>'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;"><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Receiving Health</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">% POs on time per week</div><canvas id="chart-receiving" height="160"></canvas></div>'
    + '</div>'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;"><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">End-to-End Pipeline</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">Avg days by segment</div><canvas id="chart-pipeline" height="160"></canvas></div>'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;"><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Air vs Sea Trend</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">Unit volume by freight mode</div><canvas id="chart-airvsea" height="160"></canvas></div>'
    + '</div>'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;display:grid;grid-template-columns:260px 1fr;gap:20px;align-items:center;margin-bottom:16px;">'
    + '<div><div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Performance Radar</div><div style="font-size:10px;color:#AEAEB2;margin-bottom:10px;">Actual vs best-week baseline</div><div style="position:relative;width:240px;height:240px;"><canvas id="chart-radar" width="240" height="240"></canvas></div></div>'
    + '<div id="exec-radar-legend" style="display:flex;flex-direction:column;gap:10px;"></div>'
    + '</div>'
    + '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;">'
    + '<div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:2px;">Supplier Scorecard</div>'
    + '<div style="font-size:10px;color:#AEAEB2;margin-bottom:14px;">Ranked by total planned units</div>'
    + '<div id="exec-scorecard"></div></div>'
    + '</div>'
    + '<div id="exec-intelligence" style="position:sticky;top:60px;max-height:calc(100vh - 80px);overflow-y:auto;">'
    + '<div style="font-size:13px;font-weight:600;color:#1C1C1E;margin-bottom:12px;display:flex;align-items:center;gap:8px;"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="'+BRAND+'" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>Improvement Intelligence</div>'
    + '<div id="exec-insights-list" style="display:flex;flex-direction:column;gap:10px;"><div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">Loading\u2026</div></div>'
    + '</div>'
    + '</div></div>';
  }

  function _wireRangeBar() {
    document.querySelectorAll('.exec-range-btn').forEach(function(btn) {
      btn.addEventListener('click', function() {
        _range = Number(btn.dataset.weeks);
        document.querySelectorAll('.exec-range-btn').forEach(function(b) {
          b.style.background = b === btn ? '#1C1C1E' : '#fff';
          b.style.color = b === btn ? '#fff' : '#6E6E73';
        });
        _loadAndRender();
      });
    });
  }

  async function _loadAndRender() {
    if (_loading) return;
    _loading = true;
    var loadEl = el('exec-loading');
    if (loadEl) loadEl.style.display = 'block';
    try {
      var toDate = new Date();
      var fromDate = new Date(toDate);
      fromDate.setUTCDate(fromDate.getUTCDate() - (_range * 7));
      var from = fromDate.toISOString().slice(0, 10);
      var to   = toDate.toISOString().slice(0, 10);
      var facility = ((window.state && Array.isArray(window.state.plan)) ? window.state.plan : []).map(function(p){ return String(p.facility_name || p.facility || '').trim(); }).find(Boolean)
        || String((window.state && window.state.facility) || '').trim() || '';
      if (!facility) { _renderError('No facility found. Please navigate to Week Hub and load a plan first.'); return; }
      var data = await _api('/exec/summary?from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to) + '&facility=' + encodeURIComponent(facility));
      _weeks = Array.isArray(data.weeks) ? data.weeks : [];
      var lbl = el('exec-range-label');
      if (lbl) lbl.textContent = from + ' \u2013 ' + to + ' \u00b7 ' + _weeks.length + ' weeks';
      _renderAll();
    } catch(e) {
      console.error('[exec] load failed', e);
      _renderError('Failed to load executive data: ' + (e.message || e));
    } finally {
      _loading = false;
      var loadEl2 = el('exec-loading');
      if (loadEl2) loadEl2.style.display = 'none';
    }
  }

  function _renderError(msg) {
    var kpis = el('exec-kpis');
    if (kpis) kpis.innerHTML = '<div style="grid-column:span 4;font-size:12px;color:#D61A3C;padding:16px;">' + msg + '</div>';
  }

  function _renderAll() {
    if (!_weeks.length) { _renderError('No data found for this period.'); return; }
    var baseline = computeBaseline(_weeks);
    _renderKPIs(baseline);
    _renderCharts(baseline);
    _renderScorecard();
    _renderInsights(baseline);
  }

  function _kpiTile(label, value, badge, badgeColor, sub) {
    return '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;padding:16px;">'
      + '<div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">' + label + '</div>'
      + '<div style="display:flex;align-items:baseline;gap:8px;">'
      + '<div style="font-size:22px;font-weight:600;color:#1C1C1E;letter-spacing:-0.02em;">' + value + '</div>'
      + (badge ? '<div style="font-size:13px;font-weight:600;color:' + badgeColor + ';">' + badge + '</div>' : '')
      + '</div><div style="font-size:10px;color:#AEAEB2;margin-top:4px;">' + sub + '</div></div>';
  }

  function _renderKPIs(baseline) {
    var kpis = el('exec-kpis');
    if (!kpis) return;
    var totalPlanned = _weeks.reduce(function(s,w){ return s+(w.planned_units||0); }, 0);
    var totalApplied = _weeks.reduce(function(s,w){ return s+(w.applied_units||0); }, 0);
    var pct = totalPlanned > 0 ? Math.round(totalApplied/totalPlanned*100) : 0;
    var daysArr = _weeks.map(function(w){ return w.avg_days_to_apply; }).filter(function(v){ return v!=null; });
    var avgDays = avg(daysArr);
    var wtArr = _weeks.map(function(w){ return w.avg_weight_kg; }).filter(function(v){ return v!=null; });
    var avgWt = avg(wtArr);
    var totalAir = _weeks.reduce(function(s,w){ return s+(w.air_units||0); }, 0);
    var totalSea = _weeks.reduce(function(s,w){ return s+(w.sea_units||0); }, 0);
    var airPct = (totalAir+totalSea)>0 ? Math.round(totalAir/(totalAir+totalSea)*100) : 0;
    var last2 = _weeks.slice(-2);
    var daysArrow = (last2.length===2 && last2[0].avg_days_to_apply!=null && last2[1].avg_days_to_apply!=null)
      ? (last2[1].avg_days_to_apply < last2[0].avg_days_to_apply ? '\u2193' : '\u2191') : '';
    var airArrow = last2.length===2 ? (function(){
      var a0=last2[0].planned_units>0?last2[0].air_units/last2[0].planned_units:0;
      var a1=last2[1].planned_units>0?last2[1].air_units/last2[1].planned_units:0;
      return a1>a0?'\u2191':(a1<a0?'\u2193':'');
    })() : '';
    kpis.innerHTML = [
      _kpiTile('Units Applied / Planned', fmtN(totalApplied)+' / '+fmtN(totalPlanned), fmtPct(pct), pct>=90?GREEN:pct>=70?AMBER:BRAND, 'Season-to-date completion'),
      _kpiTile('Avg Days to Apply', fmtDays(avgDays), daysArrow, daysArrow==='\u2193'?GREEN:daysArrow==='\u2191'?BRAND:'#AEAEB2', 'Receiving \u2192 VAS complete'+(baseline.avg_days_to_apply!=null?' \u00b7 best '+fmtDays(baseline.avg_days_to_apply):'')),
      _kpiTile('Avg Carton Weight', avgWt!=null?fmtN(avgWt,1)+' kg':'—', '', '#6E6E73', 'Average weight per bin from manifest'),
      _kpiTile('Air vs Sea Split', airPct+'% air', airArrow, airArrow==='\u2191'?AMBER:GREEN, fmtN(totalAir)+' air \u00b7 '+fmtN(totalSea)+' sea units'),
    ].join('');
  }

  function _destroyCharts() {
    Object.keys(_charts).forEach(function(k){ try{ _charts[k].destroy(); }catch{} delete _charts[k]; });
  }

  function _mkChart(id, config) {
    var canvas = el(id);
    if (!canvas || !window.Chart) return;
    try { _charts[id] = new window.Chart(canvas, config); } catch(e){ console.warn('[exec] chart', id, e); }
  }

  function _baseOpts(extra) {
    return Object.assign({ plugins: { legend: { display: true, labels: { font: { size: 10 }, boxWidth: 10 } } }, scales: Object.assign({ x: { ticks: { font: { size: 10 } }, grid: { display: false } }, y: { ticks: { font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } } }, extra||{}), responsive: true, maintainAspectRatio: true });
  }

  function _renderCharts(baseline) {
    _destroyCharts();
    var labels = _weeks.map(function(w){ return shortWk(w.week_start); });

    // Throughput
    _mkChart('chart-throughput', { type: 'bar', data: { labels: labels, datasets: [
      { label: 'Planned', data: _weeks.map(function(w){ return w.planned_units; }), backgroundColor: GRAY, borderRadius: 3, order: 2 },
      { label: 'Applied', data: _weeks.map(function(w){ return w.applied_units; }), backgroundColor: '#C8F902', borderRadius: 3, order: 2 },
      { label: 'Completion %', data: _weeks.map(function(w){ return w.planned_units>0?Math.round(w.applied_units/w.planned_units*100):0; }), type: 'line', borderColor: BRAND, backgroundColor: 'transparent', borderWidth: 2, pointBackgroundColor: BRAND, pointRadius: 3, yAxisID: 'y2', order: 1 },
    ]}, options: _baseOpts({ y2: { position: 'right', min: 0, max: 100, ticks: { font:{size:10}, callback: function(v){ return v+'%'; } }, grid: { display: false } } }) });

    // Receiving health
    var otrData = _weeks.map(function(w){ return w.on_time_receiving_pct; });
    var otrDs = [{ label: 'On-time %', data: otrData, backgroundColor: otrData.map(function(v){ return v==null?GRAY:v>=85?'#22C55E':v>=60?'#F59E0B':'#D61A3C'; }), borderRadius: 3 }];
    if (baseline.on_time_receiving_pct != null) otrDs.push({ label: 'Baseline', data: _weeks.map(function(){ return baseline.on_time_receiving_pct; }), type: 'line', borderColor: '#6E6E73', borderDash: [4,4], borderWidth: 1.5, pointRadius: 0, backgroundColor: 'transparent' });
    _mkChart('chart-receiving', { type: 'bar', data: { labels: labels, datasets: otrDs }, options: _baseOpts({ y: { min: 0, max: 100, ticks: { font:{size:10}, callback: function(v){ return v+'%'; } }, grid: { color: 'rgba(0,0,0,0.04)' } } }) });

    // Pipeline
    _mkChart('chart-pipeline', { type: 'bar', data: { labels: labels, datasets: [
      { label: 'Recv \u2192 VAS (days)', data: _weeks.map(function(w){ return w.avg_days_to_apply; }), backgroundColor: 'rgba(153,0,51,0.7)', borderRadius: 3 },
      { label: 'Transit (days)', data: _weeks.map(function(w){ return w.avg_transit_days; }), backgroundColor: 'rgba(46,125,158,0.7)', borderRadius: 3 },
    ]}, options: _baseOpts() });

    // Air vs Sea
    _mkChart('chart-airvsea', { type: 'bar', data: { labels: labels, datasets: [
      { label: 'Air', data: _weeks.map(function(w){ return w.air_units; }), backgroundColor: AIR_COL, borderRadius: 3, stack: 'f' },
      { label: 'Sea', data: _weeks.map(function(w){ return w.sea_units; }), backgroundColor: SEA_COL, borderRadius: 3, stack: 'f' },
    ]}, options: Object.assign(_baseOpts(), { scales: { x: { stacked: true, ticks:{font:{size:10}}, grid:{display:false} }, y: { stacked: true, ticks:{font:{size:10}}, grid:{color:'rgba(0,0,0,0.04)'} } } }) });

    // Radar
    var daysArr2 = _weeks.map(function(w){ return w.avg_days_to_apply; }).filter(function(v){ return v!=null; });
    var avgDays2 = avg(daysArr2);
    var otrArr2  = _weeks.map(function(w){ return w.on_time_receiving_pct; }).filter(function(v){ return v!=null; });
    var avgOTR   = avg(otrArr2);
    var totPl = _weeks.reduce(function(s,w){ return s+(w.planned_units||0); },0);
    var totAp = _weeks.reduce(function(s,w){ return s+(w.applied_units||0); },0);
    var thruPct2 = totPl>0 ? totAp/totPl*100 : null;
    var totAir2  = _weeks.reduce(function(s,w){ return s+(w.air_units||0); },0);
    var totAll   = _weeks.reduce(function(s,w){ return s+(w.air_units||0)+(w.sea_units||0); },0);
    var airPct2  = totAll>0 ? totAir2/totAll*100 : null;
    var wtArr2   = _weeks.map(function(w){ return w.avg_weight_kg; }).filter(function(v){ return v!=null; });
    var wtMean   = avg(wtArr2);
    var wtStd    = wtArr2.length>1 ? Math.sqrt(wtArr2.reduce(function(s,v){ return s+Math.pow(v-wtMean,2); },0)/wtArr2.length) : 0;
    var wtCons   = wtMean ? Math.max(0,100-(wtStd/wtMean)*100) : null;

    function sc(actual, base, higherBetter) {
      if (actual==null) return 0;
      if (base==null) return 50;
      var r = higherBetter ? actual/base : base/actual;
      return Math.min(100,Math.max(0,Math.round(r*100)));
    }

    var actuals = [
      sc(avgOTR, baseline.on_time_receiving_pct, true),
      thruPct2!=null ? Math.round(thruPct2) : 0,
      sc(avgDays2, baseline.avg_days_to_apply, false),
      airPct2!=null ? Math.max(0,Math.round(100-airPct2)) : 50,
      wtCons!=null ? Math.round(wtCons) : 50,
    ];
    var radarLabels = ['On-time Receiving','VAS Throughput','Processing Speed','Sea Efficiency','Carton Consistency'];

    _mkChart('chart-radar', { type: 'radar', data: { labels: radarLabels, datasets: [
      { label: 'Baseline', data: [100,100,100,100,100], borderColor: 'rgba(0,0,0,0.15)', backgroundColor: 'rgba(0,0,0,0.04)', borderDash:[4,4], borderWidth:1.5, pointRadius:2 },
      { label: 'Current', data: actuals, borderColor: BRAND, backgroundColor: BRAND_LT, borderWidth:2, pointBackgroundColor: BRAND, pointRadius:3 },
    ]}, options: { plugins:{ legend:{display:false} }, scales:{ r:{ min:0, max:100, ticks:{display:false}, grid:{color:'rgba(0,0,0,0.06)'}, pointLabels:{font:{size:10}} } }, responsive:false, maintainAspectRatio:false } });

    var legend = el('exec-radar-legend');
    if (legend) {
      var axisInfo = [
        { label:'On-time Receiving', actual: avgOTR!=null?fmtPct(avgOTR):'—', base: baseline.on_time_receiving_pct!=null?fmtPct(baseline.on_time_receiving_pct):'—', score:actuals[0] },
        { label:'VAS Throughput', actual: thruPct2!=null?fmtPct(thruPct2):'—', base:'100%', score:actuals[1] },
        { label:'Processing Speed', actual: avgDays2!=null?fmtDays(avgDays2):'—', base: baseline.avg_days_to_apply!=null?fmtDays(baseline.avg_days_to_apply):'—', score:actuals[2] },
        { label:'Sea Efficiency', actual: airPct2!=null?fmtPct(airPct2)+' air':'—', base:'—', score:actuals[3] },
        { label:'Carton Consistency', actual: wtCons!=null?fmtPct(wtCons):'—', base:'—', score:actuals[4] },
      ];
      legend.innerHTML = axisInfo.map(function(a){
        var sc2 = a.score>=80?GREEN:a.score>=60?AMBER:BRAND;
        var bg2 = a.score>=80?'rgba(34,197,94,0.1)':a.score>=60?'rgba(245,158,11,0.1)':BRAND_LT;
        return '<div style="display:flex;align-items:center;gap:10px;"><div style="width:32px;height:32px;border-radius:8px;background:'+bg2+';display:flex;align-items:center;justify-content:center;flex-shrink:0;"><span style="font-size:11px;font-weight:700;color:'+sc2+';">'+a.score+'</span></div><div><div style="font-size:11px;font-weight:500;color:#1C1C1E;">'+a.label+'</div><div style="font-size:10px;color:#AEAEB2;">Actual: '+a.actual+' \u00b7 Best: '+a.base+'</div></div></div>';
      }).join('');
    }
  }

  function _sparkline(data) {
    if (!data || data.length < 2) return '';
    var w=48,h=16;
    var pts = data.map(function(v,i){ return (i/(data.length-1)*w)+','+(h-(Math.max(0,Math.min(100,v))/100)*h); }).join(' ');
    var col = data[data.length-1]>=data[0]?GREEN:'#D61A3C';
    return '<svg width="'+w+'" height="'+h+'" viewBox="0 0 '+w+' '+h+'"><polyline points="'+pts+'" fill="none" stroke="'+col+'" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  }

  function _renderScorecard() {
    var sc = el('exec-scorecard');
    if (!sc) return;
    var supMap = new Map();
    _weeks.forEach(function(w){
      (w.suppliers||[]).forEach(function(s){
        if (!supMap.has(s.supplier)) supMap.set(s.supplier, {supplier:s.supplier,planned:0,applied:0,air:0,sea:0,weeks:0,wd:[]});
        var m=supMap.get(s.supplier);
        m.planned+=(s.planned||0); m.applied+=(s.applied||0); m.air+=(s.air||0); m.sea+=(s.sea||0);
        if((s.planned||0)>0){ m.weeks++; m.wd.push(s.applied/s.planned*100); }
      });
    });
    var rows = Array.from(supMap.values()).filter(function(s){ return s.planned>0; }).sort(function(a,b){ return b.planned-a.planned; });
    if (!rows.length) { sc.innerHTML='<div style="font-size:11px;color:#AEAEB2;padding:12px;">No supplier data.</div>'; return; }
    var maxPl = Math.max.apply(null, rows.map(function(r){ return r.planned; }));
    sc.innerHTML = '<div style="display:grid;grid-template-columns:2fr 160px 80px 80px 50px 80px;font-size:10px;font-weight:600;color:#AEAEB2;text-transform:uppercase;letter-spacing:.05em;padding:0 8px 8px;border-bottom:0.5px solid rgba(0,0,0,0.06);">'
      +'<div>Supplier</div><div>Units</div><div>Planned</div><div>Applied</div><div>Wks</div><div>Done</div></div>'
      + rows.map(function(r){
        var pct=r.planned>0?Math.round(r.applied/r.planned*100):0;
        var bw=Math.round(r.planned/maxPl*100);
        var aw=r.planned>0?Math.round(r.applied/r.planned*bw):0;
        var pill=pct>=90?'background:rgba(34,197,94,0.1);color:#16A34A':pct>=70?'background:rgba(245,158,11,0.1);color:#B45309':'background:'+BRAND_LT+';color:'+BRAND;
        var frt=r.air>r.sea?'<span style="font-size:9px;color:'+AIR_COL+';">\u2708 Air</span>':'<span style="font-size:9px;color:'+SEA_COL+';">\u26f4 Sea</span>';
        return '<div style="display:grid;grid-template-columns:2fr 160px 80px 80px 50px 80px;align-items:center;padding:10px 8px;border-bottom:0.5px solid rgba(0,0,0,0.04);">'
          +'<div><div style="font-size:12px;font-weight:500;color:#1C1C1E;">'+r.supplier+'</div><div style="margin-top:2px;">'+frt+'</div></div>'
          +'<div style="padding-right:12px;"><div style="height:6px;background:#F5F5F7;border-radius:3px;overflow:hidden;"><div style="display:flex;height:100%;"><div style="width:'+aw+'%;background:#C8F902;border-radius:3px;"></div><div style="width:'+(bw-aw)+'%;background:rgba(0,0,0,0.06);"></div></div></div>'
          +'<div style="display:flex;justify-content:flex-end;margin-top:3px;">'+_sparkline(r.wd)+'</div></div>'
          +'<div style="font-size:11px;color:#6E6E73;">'+fmtN(r.planned)+'</div>'
          +'<div style="font-size:11px;color:#1C1C1E;font-weight:500;">'+fmtN(r.applied)+'</div>'
          +'<div style="font-size:11px;color:#6E6E73;">'+r.weeks+'</div>'
          +'<div><span style="font-size:10px;font-weight:600;padding:3px 8px;border-radius:5px;'+pill+'">'+pct+'%</span></div>'
          +'</div>';
      }).join('');
  }

  function _insightCard(ins) {
    var dotCol = ins.priority==='high'?'#D61A3C':ins.priority==='medium'?AMBER:GREEN;
    var bgCol  = ins.priority==='high'?'rgba(214,26,60,0.04)':ins.priority==='medium'?'rgba(245,158,11,0.04)':'rgba(34,197,94,0.04)';
    var catCol = ins.category==='time-to-live'?BRAND:ins.category==='throughput'?SEA_COL:AMBER;
    return '<div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:12px;border-left:3px solid '+dotCol+';">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">'
      +'<span style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:'+catCol+';">'+ins.category.replace(/-/g,' ')+'</span>'
      +'<span style="font-size:9px;font-weight:700;color:'+dotCol+';background:'+bgCol+';padding:2px 7px;border-radius:4px;">'+ins.priority.toUpperCase()+'</span>'
      +'</div>'
      +'<div style="font-size:12px;font-weight:600;color:#1C1C1E;margin-bottom:4px;">'+ins.title+'</div>'
      +'<div style="font-size:11px;color:#6E6E73;line-height:1.4;margin-bottom:6px;">'+ins.observation+'</div>'
      +'<div style="font-size:11px;color:'+BRAND+';font-weight:500;margin-bottom:4px;">Impact: '+ins.impact+'</div>'
      +'<div style="font-size:10px;color:#6E6E73;background:#F9F9FB;border-radius:6px;padding:6px 8px;line-height:1.4;">'+ins.action+'</div>'
      +'</div>';
  }

  function _renderInsights(baseline) {
    var list = el('exec-insights-list');
    if (!list) return;
    var insights = _computeInsights(baseline);
    var pri = { high:3, medium:2, low:1 };
    insights.sort(function(a,b){ return (pri[b.priority]||0)-(pri[a.priority]||0); });
    list.innerHTML = insights.length ? insights.map(_insightCard).join('') : '<div style="font-size:11px;color:#AEAEB2;text-align:center;padding:20px;">No insights available yet.</div>';
  }

  function _computeInsights(baseline) {
    var ins = [], weeks = _weeks;
    if (!weeks.length) return ins;
    var last4 = weeks.slice(-4);

    // 1. Receiving lag
    var daysArr = weeks.map(function(w){ return w.avg_days_to_apply; }).filter(function(v){ return v!=null; });
    var avgDays = avg(daysArr);
    if (avgDays!=null && baseline.avg_days_to_apply!=null) {
      var lag = Math.round((avgDays-baseline.avg_days_to_apply)*10)/10;
      if (lag>0.5) ins.push({ category:'time-to-live', priority:lag>3?'high':'medium',
        title:'Receiving delays adding time to FC delivery',
        observation:'Avg '+fmtDays(avgDays)+' to process vs best-week baseline of '+fmtDays(baseline.avg_days_to_apply)+' — a '+fmtDays(lag)+' gap.',
        impact:'Closing this gap recovers ~'+fmtDays(lag)+' on ETA FC across all lanes.',
        action:'Review supplier dispatch schedules. Target receiving completion within 1 day of arrival.' });
    }

    // 2. Throughput gap
    var thruArr = weeks.map(function(w){ return w.planned_units>0?w.applied_units/w.planned_units*100:null; }).filter(function(v){ return v!=null; });
    var avgThru = avg(thruArr);
    var bestThru = baseline.throughput_pct!=null ? baseline.throughput_pct*100 : null;
    if (avgThru!=null && bestThru!=null) {
      var gap = Math.round(bestThru-avgThru);
      if (gap>5) ins.push({ category:'throughput', priority:gap>20?'high':'medium',
        title:'VAS throughput below best-week pace',
        observation:'Current avg '+fmtPct(avgThru)+' vs best-week baseline '+fmtPct(bestThru)+'.',
        impact:'A '+gap+'% gap = ~'+fmtN(Math.round(weeks.reduce(function(s,w){ return s+(w.planned_units||0); },0)*gap/100/weeks.length))+' units/week unprocessed.',
        action:'Identify best-performing weeks and replicate conditions. Check staffing and equipment constraints.' });
    }

    // 3. Air freight creep
    var airPcts = last4.map(function(w){ return (w.air_units+w.sea_units)>0?w.air_units/(w.air_units+w.sea_units)*100:null; }).filter(function(v){ return v!=null; });
    if (airPcts.length>=3) {
      var trend = airPcts[airPcts.length-1]-airPcts[0];
      if (trend>5) ins.push({ category:'risk', priority:trend>15?'high':'medium',
        title:'Air freight share increasing',
        observation:'Air % grew from '+fmtPct(airPcts[0])+' to '+fmtPct(airPcts[airPcts.length-1])+' over '+airPcts.length+' weeks.',
        impact:'Rising air dependency signals upstream planning delays. Air costs 4-6\xd7 sea freight.',
        action:'Review plan lead times. Identify air-default suppliers. Negotiate earlier dispatch windows.' });
    }

    // 4. Worst supplier
    var supMap = new Map();
    weeks.forEach(function(w){ (w.suppliers||[]).forEach(function(s){
      if (!supMap.has(s.supplier)) supMap.set(s.supplier,{planned:0,applied:0});
      var m=supMap.get(s.supplier); m.planned+=(s.planned||0); m.applied+=(s.applied||0);
    }); });
    var supRows = Array.from(supMap.entries()).map(function(e){ return {name:e[0],pct:e[1].planned>0?e[1].applied/e[1].planned*100:0,planned:e[1].planned}; }).filter(function(s){ return s.planned>500; }).sort(function(a,b){ return a.pct-b.pct; });
    if (supRows.length>=2) {
      var worst=supRows[0], avgPct2=avg(supRows.map(function(s){ return s.pct; }));
      if (worst.pct < avgPct2-15) ins.push({ category:'throughput', priority:worst.pct<50?'high':'medium',
        title:worst.name+' underperforming',
        observation:worst.name+' at '+fmtPct(worst.pct)+' vs supplier avg '+fmtPct(avgPct2)+'.',
        impact:fmtN(Math.round(worst.planned*(1-worst.pct/100)))+' units remain unprocessed.',
        action:'Escalate for root cause. Check receiving delays, docs issues or VAS queue backlog.' });
      var best=supRows[supRows.length-1];
      if (best.pct>=90) ins.push({ category:'throughput', priority:'low',
        title:best.name+' is your benchmark',
        observation:best.name+' achieved '+fmtPct(best.pct)+' — highest of all suppliers.',
        impact:'Their process represents the operational ceiling for this period.',
        action:'Document lead times, doc quality, dispatch patterns. Share with underperforming suppliers.' });
    }

    // 5. Pipeline bottleneck
    var vasArr2 = weeks.map(function(w){ return w.avg_days_to_apply; }).filter(function(v){ return v!=null; });
    var trArr2  = weeks.map(function(w){ return w.avg_transit_days; }).filter(function(v){ return v!=null; });
    var avgVas2=avg(vasArr2), avgTr2=avg(trArr2);
    if (avgVas2!=null && avgTr2!=null) {
      var bot = avgVas2>avgTr2?'receiving \u2192 VAS':'transit';
      ins.push({ category:'time-to-live', priority:'medium',
        title:'Pipeline bottleneck: '+bot,
        observation:'Recv\u2192VAS avg: '+fmtDays(avgVas2)+'. Transit avg: '+fmtDays(avgTr2)+'.',
        impact:'Reducing '+bot+' by 2 days recovers 2 days on ETA FC.',
        action:avgVas2>avgTr2?'Review VAS processing throughput and staffing.':'Review transit routes and customs clearance delays.' });
    }

    // 6. Carton weight anomaly
    var wtArr3 = weeks.map(function(w){ return w.avg_weight_kg; }).filter(function(v){ return v!=null; });
    if (wtArr3.length>=3) {
      var wtAv=avg(wtArr3), wtMx=Math.max.apply(null,wtArr3), wtMn=Math.min.apply(null,wtArr3);
      var vari=(wtMx-wtMn)/wtAv*100;
      if (vari>25) ins.push({ category:'risk', priority:'low',
        title:'Carton weight varies significantly',
        observation:'Avg bin weight: '+fmtN(wtMn,1)+'kg to '+fmtN(wtMx,1)+'kg — '+fmtPct(vari)+' variance.',
        impact:'High variance makes freight cost forecasting unreliable.',
        action:'Audit carton packing across suppliers. Set standard weight range per SKU category.' });
    }

    // 7. At-risk recent week
    var recent = weeks.slice(-2);
    if (recent.length===2) {
      var recPct=recent[1].planned_units>0?recent[1].applied_units/recent[1].planned_units*100:null;
      if (recPct!=null && recPct<70) ins.push({ category:'risk', priority:'high',
        title:'Most recent week at risk',
        observation:'Last week completed only '+fmtPct(recPct)+' with '+fmtN(recent[1].planned_units-recent[1].applied_units)+' units remaining.',
        impact:'If this pace continues these units will miss their FC arrival window.',
        action:'Escalate immediately. Review VAS queue. Consider priority processing for highest-value POs.' });
    }

    // 8. Late PO pattern
    var lateArr=weeks.map(function(w){ return w.late_pos||0; });
    var avgLate=avg(lateArr), maxLate=Math.max.apply(null,lateArr);
    if (avgLate>2) ins.push({ category:'risk', priority:avgLate>5?'high':'medium',
      title:'Persistent late PO receiving pattern',
      observation:'Avg '+fmtN(avgLate,1)+' POs late/week. Peak: '+maxLate+' in a single week.',
      impact:'Late receiving cascades into late VAS and delayed FC arrival.',
      action:'Identify which suppliers are consistently late. Implement earlier PO cut-off dates.' });

    // 9. Recovery opportunity
    if (avgDays!=null && baseline.avg_days_to_apply!=null) {
      var halfLag=(avgDays-baseline.avg_days_to_apply)/2;
      if (halfLag>0.5) ins.push({ category:'time-to-live', priority:'low',
        title:'Recovery opportunity: halve the processing lag',
        observation:'Improving from '+fmtDays(avgDays)+' to '+fmtDays(avgDays-halfLag)+' is halfway to best-week performance.',
        impact:'~'+fmtDays(halfLag)+' faster delivery to FC across all lanes.',
        action:'Set 4-week target to halve current lag. Track against best-week baseline on Receiving Health chart.' });
    }

    return ins;
  }

  window.addEventListener('state:ready', function() {
    if ((location.hash||'').toLowerCase() === '#exec') _tryRender();
  });

  var _obs = new MutationObserver(function() {
    var ep = el('page-exec');
    if (ep && ep.style.display !== 'none' && !ep.classList.contains('hidden') && !el('exec-dashboard-root')) _tryRender();
  });
  var _ep = document.querySelector('#page-exec') || document.body;
  _obs.observe(_ep, { attributes: true, attributeFilter: ['style','class'] });

  if ((location.hash||'').toLowerCase() === '#exec') setTimeout(_tryRender, 100);

})();
