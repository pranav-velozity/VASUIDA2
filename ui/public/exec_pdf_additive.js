/*
 * Pinpoint Exec — PDF Export (exec_pdf_additive.js)
 * --------------------------------------------------
 * Adds a "Download PDF" button to the Executive toolbar. Builds a print-only
 * overlay (A4 landscape, one chart per page) styled to mirror the Cost
 * Utilisation Report, then uses the browser's native print-to-PDF engine.
 *
 * Fully additive:
 *   - Reads the live, on-screen data from window.__execState (set by
 *     exec_live_additive.js after each render) so the PDF always matches the
 *     page the user is looking at. No second data path.
 *   - SVG charts (TTL, Throughput, On-Time Receiving, Planned vs Received POs)
 *     are cloned as vectors → crisp. Chart.js charts (Radar, Air vs Sea,
 *     Container Utilisation) are snapshotted via toBase64Image.
 *   - Per-chart Definition + Narrative + Insight are computed deterministically
 *     from the same data (never hallucinated). The heavier AI "Improvement
 *     Intelligence" section is opt-in at download time (one /ai/pulse call).
 */
(function ExecPdfAdditive() {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#22C55E', AMBER = '#F59E0B', RED = '#D61A3C';
  const COVER_BG = 'linear-gradient(135deg,#0d0010 0%,#1a0020 40%,#2d0035 70%,#990033 100%)';

  // ── API (mirrors exec_live_additive.js) ──────────────────────────────────
  const _apiBase = (() => {
    const b = String(document.querySelector('meta[name="api-base"]')?.content || location.origin).replace(/\/+$/, '');
    return /\/api$/i.test(b) ? b : b + '/api';
  })();
  async function _getToken() { try { return await window.Clerk?.session?.getToken(); } catch { return null; } }
  async function _fetchInsights(weeks, facility) {
    const token = await _getToken();
    const resp = await fetch(_apiBase + '/ai/pulse', {
      method: 'POST',
      headers: Object.assign({ 'content-type': 'application/json' }, token ? { 'authorization': 'Bearer ' + token } : {}),
      body: JSON.stringify({ weeks, facility })
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    return Array.isArray(data.insights) ? data.insights : [];
  }

  // ── Helpers ──────────────────────────────────────────────────────────────
  const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const fmtN = (n, d) => { d = d || 0; if (n == null || isNaN(n)) return '—'; return Number(n).toLocaleString('en-AU', { minimumFractionDigits: d, maximumFractionDigits: d }); };
  const fmtPct = n => (n == null || isNaN(n)) ? '—' : Math.round(n) + '%';
  const fmtDays = n => (n == null || isNaN(n)) ? '—' : Number(n).toFixed(1) + 'd';
  const mean = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const vals = (a, k) => a.map(w => w[k]).filter(v => v != null && !isNaN(v)).map(Number);
  const sum = (a, k) => a.reduce((s, w) => s + (Number(w[k]) || 0), 0);
  const shortWk = ws => { try { const d = new Date(ws + 'T00:00:00Z'); return d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric', timeZone: 'UTC' }); } catch { return ws; } };
  const chunk = (arr, n) => { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; };

  // ── Aggregate context (single deterministic source for all narratives) ────
  function buildCtx(weeks, baseline) {
    const n = weeks.length;
    const totPlU = sum(weeks, 'planned_units'), totApU = sum(weeks, 'applied_units');
    const completion = totPlU > 0 ? totApU / totPlU * 100 : null;

    const seg1 = mean(vals(weeks, 'avg_days_to_apply'));
    const seg2 = mean(vals(weeks, 'avg_days_vas_to_eta'));
    const seg3 = mean(vals(weeks, 'avg_days_eta_to_delivery'));
    const e2e = (seg1 != null || seg2 != null || seg3 != null) ? (seg1 || 0) + (seg2 || 0) + (seg3 || 0) : null;

    const otrArr = vals(weeks, 'on_time_receiving_pct');
    const otrAvg = mean(otrArr);

    const totPlP = sum(weeks, 'planned_pos'), totRcP = sum(weeks, 'received_pos');
    const receipt = totPlP > 0 ? totRcP / totPlP * 100 : null;

    const totAir = sum(weeks, 'air_units'), totSea = sum(weeks, 'sea_units');
    const airShare = (totAir + totSea) > 0 ? totAir / (totAir + totSea) * 100 : null;
    const firstAir = weeks.length && (weeks[0].air_units + weeks[0].sea_units) > 0 ? weeks[0].air_units / (weeks[0].air_units + weeks[0].sea_units) * 100 : null;
    const lastW = weeks[weeks.length - 1];
    const lastAir = lastW && (lastW.air_units + lastW.sea_units) > 0 ? lastW.air_units / (lastW.air_units + lastW.sea_units) * 100 : null;
    const airTrend = (firstAir != null && lastAir != null) ? lastAir - firstAir : null;

    const c40 = vals(weeks, 'avg_units_per_40ft'), c20 = vals(weeks, 'avg_units_per_20ft');

    // Radar scores — same formula as exec_live _renderRadar
    const daysA = seg1;
    const wtArr = vals(weeks, 'avg_weight_kg').filter(v => v > 0);
    const wtMean = mean(wtArr);
    const wtStd = wtArr.length > 1 ? Math.sqrt(wtArr.reduce((s, v) => s + Math.pow(v - wtMean, 2), 0) / wtArr.length) : 0;
    const wtCons = wtMean ? Math.max(0, 100 - (wtStd / wtMean) * 100) : null;
    const sc = (actual, base, high) => { if (actual == null) return 0; if (base == null) return Math.min(100, Math.round(actual)); const r = high ? actual / base : base / actual; return Math.min(100, Math.max(0, Math.round(r * 100))); };
    const radar = [
      { label: 'On-time Receiving', score: sc(otrAvg, baseline && baseline.on_time_receiving_pct, true) },
      { label: 'VAS Throughput', score: completion != null ? Math.min(100, Math.round(completion)) : 0 },
      { label: 'Processing Speed', score: sc(daysA, baseline && baseline.avg_days_to_apply, false) },
      { label: 'Sea Efficiency', score: airShare != null ? Math.max(0, Math.round(100 - airShare)) : 50 },
      { label: 'Carton Consistency', score: wtCons != null ? Math.round(wtCons) : 50 },
    ];

    return { n, totPlU, totApU, completion, seg1, seg2, seg3, e2e, otrAvg, otrArr,
      totPlP, totRcP, receipt, totAir, totSea, airShare, airTrend, c40, c20, radar };
  }

  // best/worst week helpers
  function bestWorst(weeks, fn) {
    let best = null, worst = null;
    weeks.forEach(w => {
      const v = fn(w);
      if (v == null || isNaN(v)) return;
      if (best == null || v > best.v) best = { v, ws: w.week_start };
      if (worst == null || v < worst.v) worst = { v, ws: w.week_start };
    });
    return { best, worst };
  }

  // ── Chart pages: definition + computed narrative + insight ────────────────
  const CHART_ORDER = [
    {
      id: 'chart-ttl', kind: 'svg', title: 'Avg Time — Door to Door', badge: 'Lead time',
      def: 'Average days from goods receipt to final-mile FC delivery, split into three segments: Receiving → VAS complete, VAS → ETA at FC, and ETA → delivery. Calculated per week from matched PO milestone dates and averaged across the window.',
      text: (w, c) => {
        const parts = [];
        if (c.seg1 != null) parts.push(`${fmtDays(c.seg1)} receiving→VAS`);
        if (c.seg2 != null) parts.push(`${fmtDays(c.seg2)} VAS→ETA`);
        if (c.seg3 != null) parts.push(`${fmtDays(c.seg3)} ETA→delivery`);
        const narrative = c.e2e != null
          ? `Across the ${c.n}-week window the average door-to-door time is ${fmtDays(c.e2e)}${parts.length ? ' (' + parts.join(', ') + ')' : ''}.`
          : 'Segment timings populate as receiving, VAS-completion and delivery dates are logged against each PO.';
        const segs = [['Receiving→VAS', c.seg1], ['VAS→ETA', c.seg2], ['ETA→delivery', c.seg3]].filter(s => s[1] != null);
        let insight;
        if (segs.length) {
          segs.sort((a, b) => b[1] - a[1]);
          insight = `${segs[0][0]} is the largest segment (${fmtDays(segs[0][1])}) — compressing it would shorten the end-to-end timeline most.`;
        } else { insight = 'Log milestone dates against POs to unlock segment-level lead-time analysis.'; }
        return { narrative, insight };
      }
    },
    {
      id: 'chart-receiving-dots', kind: 'svg', title: 'On-Time Receiving', badge: 'Timeliness',
      def: 'Share of planned POs that arrived on or before their due date each week. On-time % = (POs received − POs received late) ÷ POs planned. Distinct from receipt rate, which ignores timing.',
      text: (w, c) => {
        const bw = bestWorst(w, x => x.on_time_receiving_pct);
        const below = c.otrArr.filter(v => v < 85).length;
        const narrative = c.otrAvg != null
          ? `On-time receiving averages ${fmtPct(c.otrAvg)} across ${c.otrArr.length} week(s) with data`
            + (bw.best && bw.worst ? `, ranging from ${fmtPct(bw.worst.v)} (${shortWk(bw.worst.ws)}) to ${fmtPct(bw.best.v)} (${shortWk(bw.best.ws)}).` : '.')
          : 'On-time data populates once PO due dates and receiving dates are recorded.';
        const insight = c.otrAvg == null ? 'Record PO due dates and receiving dates to surface punctuality trends.'
          : below > 0 ? `${below} week(s) fell below 85% on-time — review suppliers with recurring late dispatch.`
          : 'Receiving punctuality is consistently strong across the window.';
        return { narrative, insight };
      }
    },
    {
      id: 'chart-po-dumbbell', kind: 'svg', title: 'Planned vs Received POs', badge: 'Receipt rate',
      def: 'Number of POs planned versus the number physically received each week, with the receipt rate above each week. Receipt rate = received ÷ planned. Measures whether planned POs arrived at all (regardless of timing).',
      text: (w, c) => {
        const bw = bestWorst(w, x => x.planned_pos > 0 ? x.received_pos / x.planned_pos * 100 : null);
        const shortfall = Math.max(0, c.totPlP - c.totRcP);
        const narrative = c.receipt != null
          ? `${fmtN(c.totRcP)} of ${fmtN(c.totPlP)} planned POs were received across the window — an overall receipt rate of ${fmtPct(c.receipt)}.`
            + (bw.worst ? ` Lowest week was ${shortWk(bw.worst.ws)} at ${fmtPct(bw.worst.v)}.` : '')
          : 'Receipt rate populates once planned and received PO counts are present.';
        const insight = c.receipt == null ? 'Ensure plan PO numbers and receiving records are captured to track receipt rate.'
          : shortfall > 0 ? `${fmtN(shortfall)} planned PO(s) did not arrive within their week — chase outstanding dispatches with those suppliers.`
          : 'Every planned PO was received within its week across the window.';
        return { narrative, insight };
      }
    },
    {
      id: 'chart-radar', kind: 'canvas', title: 'Performance Radar', badge: 'Overview',
      def: 'Five operational dimensions scored 0–100 against the best week achieved in the window (100 = best-week baseline): On-time Receiving, VAS Throughput, Processing Speed, Sea Efficiency and Carton Consistency.',
      text: (w, c) => {
        const r = c.radar.slice().sort((a, b) => b.score - a.score);
        const top = r[0], low = r[r.length - 1];
        const narrative = `Measured against best-week performance, the strongest dimension is ${top.label} (${top.score}/100) and the weakest is ${low.label} (${low.score}/100).`;
        const insight = `${low.label} is the clearest improvement opportunity — gains there move the overall profile closest to best-week performance.`;
        return { narrative, insight };
      }
    },
    {
      id: 'chart-airvsea', kind: 'canvas', title: 'Air vs Sea Mix', badge: 'Freight',
      def: 'Unit volume shipped by air versus sea each week. Air share = air units ÷ (air + sea units). Air freight typically costs several times more per unit than sea.',
      text: (w, c) => {
        const narrative = c.airShare != null
          ? `Air carried ${fmtPct(c.airShare)} of volume across the window (${fmtN(c.totAir)} air units vs ${fmtN(c.totSea)} sea units).`
            + (c.airTrend != null ? ` Air share moved ${c.airTrend >= 0 ? 'up' : 'down'} ${fmtPct(Math.abs(c.airTrend))} from first to last week.` : '')
          : 'Mode split populates once freight type is recorded on the plan.';
        const insight = c.airShare == null ? 'Tag plan rows with freight type to track the air/sea mix.'
          : (c.airTrend != null && c.airTrend > 5) ? 'Rising air reliance signals upstream lead-time pressure — earlier dispatch would enable lower-cost sea freight.'
          : 'Mode mix is stable; sea remains the cost-efficient default for the bulk of volume.';
        return { narrative, insight };
      }
    },
    {
      id: 'chart-container-util', kind: 'canvas', title: 'Container Utilisation', badge: 'Freight',
      def: 'Average units shipped per sea container by size (20ft vs 40ft), week over week, with container counts. Air containers are excluded. Higher units per container indicates better fill efficiency.',
      text: (w, c) => {
        const a40 = mean(c.c40), a20 = mean(c.c20);
        const parts = [];
        if (a40 != null) parts.push(`${fmtN(Math.round(a40))} units/40ft`);
        if (a20 != null) parts.push(`${fmtN(Math.round(a20))} units/20ft`);
        const narrative = parts.length
          ? `Average fill across the window is ${parts.join(' and ')}.`
          : 'Container utilisation populates as containers are assigned to weeks on the flow board.';
        const insight = parts.length
          ? 'Track fill against historical best to spot under-utilised containers and consolidation opportunities.'
          : 'Assign POs to containers on the flow board to unlock utilisation tracking.';
        return { narrative, insight };
      }
    }
  ];

  // ── Glossary ──────────────────────────────────────────────────────────────
  // ── Chart capture ──────────────────────────────────────────────────────────
  function captureSVG(id) {
    const host = document.getElementById(id);
    const svg = host && host.querySelector('svg');
    if (!svg) return '<div class="exec-pdf-empty">No data rendered for this chart.</div>';
    const clone = svg.cloneNode(true);
    clone.removeAttribute('width');
    clone.removeAttribute('height');
    clone.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    clone.style.width = '100%';
    clone.style.height = 'auto';
    clone.style.maxHeight = '100%';
    clone.style.display = 'block';
    return clone.outerHTML;
  }
  function canvasImg(id) {
    const cv = document.getElementById(id);
    if (!cv) return '';
    let url = '';
    try {
      const ch = (window.Chart && window.Chart.getChart) ? window.Chart.getChart(cv) : null;
      url = ch ? ch.toBase64Image('image/png', 1) : cv.toDataURL('image/png');
    } catch (e) { try { url = cv.toDataURL('image/png'); } catch (_) { url = ''; } }
    if (!url) return '';
    return '<img src="' + url + '" alt="" style="display:block;margin:0 auto;max-width:100%;max-height:100%;width:auto;height:auto;"/>';
  }
  function captureCanvas(id) {
    return canvasImg(id) || '<div class="exec-pdf-empty">No data rendered for this chart.</div>';
  }
  // Full radar = radar canvas + the score breakdown legend, mirroring the live page.
  function radarComposite() {
    const img = canvasImg('chart-radar');
    const legend = document.getElementById('exec-radar-legend');
    let legHTML = '';
    if (legend) {
      const clone = legend.cloneNode(true);
      // Compact the cloned cards so the full breakdown fits the fixed stage height
      Array.from(clone.children).forEach(ch => { if (ch.style) ch.style.padding = '7px 11px'; });
      legHTML = clone.innerHTML;
    }
    if (!img && !legHTML) return '<div class="exec-pdf-empty">No data rendered for this chart.</div>';
    return '<div class="exec-pdf-radar">'
      + '<div class="exec-pdf-radar-chart">' + (img || '') + '</div>'
      + '<div class="exec-pdf-radar-legend">' + legHTML + '</div>'
      + '</div>';
  }

  // ── Page frame (header strip + footer + page number) ───────────────────────
  function frame(num, total, periodLabel) {
    const head = `<div class="exec-pdf-strip">VelOzity Pinpoint — Executive Report</div>`
      + `<div class="exec-pdf-pageno">${num != null ? 'VelOzity Pinpoint • Page ' + num + ' of ' + total : ''}</div>`;
    const foot = `<div class="exec-pdf-footer"><span>Confidential</span><span>${esc(periodLabel)}</span></div>`;
    return { head, foot };
  }

  function chartPageHTML(c, weeks, ctx, num, total, periodLabel) {
    const body = c.id === 'chart-radar' ? radarComposite() : (c.kind === 'svg' ? captureSVG(c.id) : captureCanvas(c.id));
    const t = c.text(weeks, ctx);
    const f = frame(num, total, periodLabel);
    return `<div class="exec-pdf-page">
      ${f.head}
      <div class="exec-pdf-sechead">
        <div><div class="exec-pdf-title">${esc(c.title)}</div></div>
        <div class="exec-pdf-badge">${esc(c.badge)}</div>
      </div>
      <div class="exec-pdf-note"><strong>Definition</strong>${esc(c.def)}</div>
      <div class="exec-pdf-note"><strong>Actuals / Trend</strong>${esc(t.narrative)}</div>
      <div class="exec-pdf-stage">${body}</div>
      <div class="exec-pdf-note exec-pdf-note-insight"><strong>Improvement Insights</strong>${esc(t.insight)}</div>
      ${f.foot}
    </div>`;
  }

  function kpiPageHTML(ctx, num, total, periodLabel) {
    const f = frame(num, total, periodLabel);
    const card = (label, value, sub, accent) => `<div class="exec-pdf-kpi" style="border-top:3px solid ${accent};">
      <div class="exec-pdf-kpi-label">${esc(label)}</div>
      <div class="exec-pdf-kpi-value">${value}</div>
      <div class="exec-pdf-kpi-sub">${esc(sub)}</div></div>`;
    const cards = [
      card('Units Applied / Planned', `${fmtN(ctx.totApU)} / ${fmtN(ctx.totPlU)}`, `${fmtPct(ctx.completion)} completion`, BRAND),
      card('PO Receipt Rate', fmtPct(ctx.receipt), `${fmtN(ctx.totRcP)} received / ${fmtN(ctx.totPlP)} planned`, '#0EA5E9'),
      card('On-Time Receiving', fmtPct(ctx.otrAvg), 'Avg across weeks with data', GREEN),
      card('Processing Speed', fmtDays(ctx.seg1), 'Receiving → VAS', AMBER),
      card('Door-to-Door', fmtDays(ctx.e2e), 'Avg end-to-end days', '#8B5CF6'),
      card('Air vs Sea', `${fmtPct(ctx.airShare)} air`, `${fmtN(ctx.totAir)} air · ${fmtN(ctx.totSea)} sea`, '#F59E0B'),
    ].join('');
    return `<div class="exec-pdf-page">
      ${f.head}
      <div class="exec-pdf-sechead"><div class="exec-pdf-title">Executive Summary</div><div class="exec-pdf-badge">Overview</div></div>
      <div class="exec-pdf-sub">Cross-week snapshot · ${esc(periodLabel)}</div>
      <div class="exec-pdf-kpigrid">${cards}</div>
      <div class="exec-pdf-notes"><div class="exec-pdf-note"><strong>About this report</strong>All figures are computed from completed VAS records, weekly plans and receiving data for the selected facility and date range. Percentages are rounded. This report reflects the same data shown on the live Executive page at the time of export.</div></div>
      ${f.foot}
    </div>`;
  }

  function insightsPageHTML(items, part, parts, num, total, periodLabel) {
    const f = frame(num, total, periodLabel);
    const pri = p => p === 'high' ? RED : p === 'medium' ? AMBER : GREEN;
    const cards = items.map(ins => {
      const col = pri(String(ins.priority || 'low'));
      return `<div class="exec-pdf-ins" style="border-left:3px solid ${col};">
        <div class="exec-pdf-ins-head"><span class="exec-pdf-ins-cat">${esc(String(ins.category || '').replace(/-/g, ' '))}</span><span class="exec-pdf-ins-pri" style="color:${col};">${esc(String(ins.priority || '').toUpperCase())}</span></div>
        <div class="exec-pdf-ins-title">${esc(ins.title || '')}</div>
        ${ins.observation ? `<div class="exec-pdf-ins-body">${esc(ins.observation)}</div>` : ''}
        ${ins.impact ? `<div class="exec-pdf-ins-impact">↗ ${esc(ins.impact)}</div>` : ''}
        ${ins.action ? `<div class="exec-pdf-ins-action">${esc(ins.action)}</div>` : ''}
      </div>`;
    }).join('');
    return `<div class="exec-pdf-page">
      ${f.head}
      <div class="exec-pdf-sechead"><div class="exec-pdf-title">Improvement Intelligence${parts > 1 ? ' (' + part + '/' + parts + ')' : ''}</div><div class="exec-pdf-badge">AI</div></div>
      <div class="exec-pdf-sub">Generated analysis · priority-ranked</div>
      <div class="exec-pdf-insgrid">${cards}</div>
      ${f.foot}
    </div>`;
  }

  function coverHTML(periodLabel) {
    const now = new Date();
    return `<div class="exec-pdf-page exec-pdf-cover">
      <div class="exec-pdf-cover-brand">Vel<span style="color:#fff;">Ozity</span> Pinpoint</div>
      <div class="exec-pdf-cover-tag">Supply Chain Intelligence</div>
      <div class="exec-pdf-cover-title">Executive Report</div>
      <div class="exec-pdf-cover-dates">${esc(periodLabel)}</div>
      <div class="exec-pdf-cover-meta">CONFIDENTIAL • Generated ${esc(now.toLocaleString('en-AU'))}</div>
    </div>`;
  }

  function closingHTML(num, total, periodLabel) {
    return `<div class="exec-pdf-page exec-pdf-cover exec-pdf-back">
      <div class="exec-pdf-back-title">Report created</div>
      <div class="exec-pdf-cover-meta">${esc(new Date().toLocaleString('en-AU'))}</div>
      <div class="exec-pdf-cover-meta" style="margin-top:10px;">VelOzity Pinpoint • Executive Report • ${esc(periodLabel)}</div>
    </div>`;
  }

  // ── Print styles ───────────────────────────────────────────────────────────
  function styleHTML() {
    return `
    #exec-print-root{ font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:${DARK}; }
    #exec-print-root .exec-pdf-page{ width:297mm; min-height:210mm; max-height:210mm; overflow:hidden; position:relative; box-sizing:border-box; padding:16mm 18mm 14mm; background:#fff; }
    #exec-print-root .exec-pdf-strip{ position:absolute; top:7mm; left:18mm; font-size:9px; color:${LIGHT}; }
    #exec-print-root .exec-pdf-pageno{ position:absolute; top:7mm; right:18mm; font-size:9px; color:${LIGHT}; font-style:italic; }
    #exec-print-root .exec-pdf-footer{ position:absolute; bottom:8mm; left:18mm; right:18mm; display:flex; justify-content:space-between; font-size:9px; color:${LIGHT}; border-top:0.5px solid rgba(0,0,0,0.08); padding-top:5px; }
    #exec-print-root .exec-pdf-sechead{ display:flex; align-items:center; justify-content:space-between; margin-bottom:2px; }
    #exec-print-root .exec-pdf-title{ font-family:'DM Serif Display',Georgia,serif; font-size:28px; color:${BRAND}; }
    #exec-print-root .exec-pdf-badge{ font-size:10px; font-weight:700; padding:4px 12px; border-radius:20px; letter-spacing:0.05em; text-transform:uppercase; background:rgba(153,0,51,0.1); color:${BRAND}; }
    #exec-print-root .exec-pdf-sub{ font-size:13px; color:${MID}; margin-bottom:14px; }
    #exec-print-root .exec-pdf-stage{ height:82mm; display:flex; align-items:center; justify-content:center; overflow:hidden; margin:9px 0; }
    #exec-print-root .exec-pdf-empty{ font-size:13px; color:${LIGHT}; text-align:center; padding:40px 0; }
    #exec-print-root .exec-pdf-note{ background:#F5F5F7; border-radius:8px; padding:10px 16px; font-size:13.5px; color:${MID}; line-height:1.55; margin-bottom:8px; }
    #exec-print-root .exec-pdf-note:last-of-type{ margin-bottom:0; }
    #exec-print-root .exec-pdf-note strong{ color:${DARK}; font-size:14px; display:block; margin-bottom:3px; }
    #exec-print-root .exec-pdf-note-insight{ background:rgba(153,0,51,0.05); }
    #exec-print-root .exec-pdf-note-insight strong{ color:${BRAND}; }
    #exec-print-root .exec-pdf-radar{ display:flex; gap:22px; align-items:center; width:100%; height:100%; }
    #exec-print-root .exec-pdf-radar-chart{ flex:0 0 auto; height:100%; display:flex; align-items:center; justify-content:center; }
    #exec-print-root .exec-pdf-radar-chart img{ max-height:100%; max-width:100%; width:auto; height:auto; }
    #exec-print-root .exec-pdf-radar-legend{ flex:1 1 auto; max-height:100%; overflow:hidden; display:flex; flex-direction:column; gap:6px; }
    #exec-print-root .exec-pdf-kpigrid{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:14px; margin:10px 0 18px; }
    #exec-print-root .exec-pdf-kpi{ border:0.5px solid rgba(0,0,0,0.08); border-radius:12px; padding:16px 18px; }
    #exec-print-root .exec-pdf-kpi-label{ font-size:10px; font-weight:600; color:${MID}; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:7px; }
    #exec-print-root .exec-pdf-kpi-value{ font-family:'DM Serif Display',Georgia,serif; font-size:30px; color:${DARK}; margin-bottom:3px; }
    #exec-print-root .exec-pdf-kpi-sub{ font-size:11px; color:${MID}; }
    #exec-print-root .exec-pdf-glossgrid{ display:grid; grid-template-columns:1fr 1fr; gap:12px 26px; margin-top:8px; }
    #exec-print-root .exec-pdf-gloss-term{ font-size:13px; font-weight:600; color:${BRAND}; margin-bottom:2px; }
    #exec-print-root .exec-pdf-gloss-def{ font-size:11px; color:${MID}; line-height:1.5; }
    #exec-print-root .exec-pdf-insgrid{ display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:8px; }
    #exec-print-root .exec-pdf-ins{ background:#fff; border:0.5px solid rgba(0,0,0,0.08); border-radius:10px; padding:12px 14px; }
    #exec-print-root .exec-pdf-ins-head{ display:flex; justify-content:space-between; align-items:center; margin-bottom:5px; }
    #exec-print-root .exec-pdf-ins-cat{ font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.06em; color:${MID}; }
    #exec-print-root .exec-pdf-ins-pri{ font-size:10px; font-weight:700; }
    #exec-print-root .exec-pdf-ins-title{ font-size:13px; font-weight:600; color:${DARK}; margin-bottom:4px; line-height:1.3; }
    #exec-print-root .exec-pdf-ins-body{ font-size:11.5px; color:${MID}; line-height:1.45; margin-bottom:5px; }
    #exec-print-root .exec-pdf-ins-impact{ font-size:11.5px; color:${BRAND}; font-weight:500; margin-bottom:4px; }
    #exec-print-root .exec-pdf-ins-action{ font-size:11.5px; color:${MID}; background:#F9F9FB; border-radius:6px; padding:6px 9px; line-height:1.45; }
    #exec-print-root .exec-pdf-cover{ background:${COVER_BG}; color:#fff; display:flex; flex-direction:column; align-items:center; justify-content:center; text-align:center; }
    #exec-print-root .exec-pdf-cover-brand{ font-family:'DM Serif Display',Georgia,serif; font-size:48px; color:rgba(255,255,255,0.55); letter-spacing:-0.02em; }
    #exec-print-root .exec-pdf-cover-tag{ font-size:12px; color:rgba(255,255,255,0.5); letter-spacing:0.18em; text-transform:uppercase; margin:10px 0 40px; }
    #exec-print-root .exec-pdf-cover-title{ font-family:'DM Serif Display',Georgia,serif; font-style:italic; font-size:34px; color:#fff; margin-bottom:8px; }
    #exec-print-root .exec-pdf-cover-dates{ font-size:13px; color:rgba(255,255,255,0.6); margin-bottom:36px; }
    #exec-print-root .exec-pdf-cover-meta{ font-size:11px; color:rgba(255,255,255,0.45); }
    #exec-print-root .exec-pdf-back-title{ font-family:'DM Serif Display',Georgia,serif; font-size:44px; color:rgba(255,255,255,0.7); margin-bottom:14px; }
    @media screen { #exec-print-root{ display:none; } }
    @media print {
      @page { size:297mm 210mm; margin:0; }
      html, body { margin:0 !important; padding:0 !important; background:#fff !important; }
      body > *:not(#exec-print-root){ display:none !important; }
      #exec-print-root{ display:block !important; }
      #exec-print-root .exec-pdf-page{ break-after:page; }
      #exec-print-root .exec-pdf-page:last-child{ break-after:auto; }
    }`;
  }

  function periodLabelOf(st) {
    return `${st.from || '?'} → ${st.to || '?'} · ${st.weeks.length} weeks`;
  }

  let _building = false;
  async function buildAndPrint(st, includeAI, aiInsights) {
    const weeks = st.weeks, baseline = st.baseline || {};
    const ctx = buildCtx(weeks, baseline);
    const periodLabel = periodLabelOf(st);

    // Assemble page list to know the numbered total up front
    const pages = [{ type: 'cover' }, { type: 'kpi' }];
    CHART_ORDER.forEach(c => pages.push({ type: 'chart', chart: c }));
    const insChunks = (includeAI && aiInsights.length) ? chunk(aiInsights, 4) : [];
    insChunks.forEach((items, i) => pages.push({ type: 'insights', items, part: i + 1, parts: insChunks.length }));
    pages.push({ type: 'closing' });

    const total = pages.filter(p => p.type !== 'cover').length;
    let num = 0;
    const html = pages.map(p => {
      const numbered = p.type !== 'cover';
      if (numbered) num++;
      switch (p.type) {
        case 'cover': return coverHTML(periodLabel);
        case 'kpi': return kpiPageHTML(ctx, num, total, periodLabel);
        case 'chart': return chartPageHTML(p.chart, weeks, ctx, num, total, periodLabel);
        case 'insights': return insightsPageHTML(p.items, p.part, p.parts, num, total, periodLabel);
        case 'closing': return closingHTML(num, total, periodLabel);
        default: return '';
      }
    }).join('');

    // Fonts (best-effort, mirror cost report typography)
    if (!document.getElementById('exec-pdf-fonts')) {
      const link = document.createElement('link');
      link.id = 'exec-pdf-fonts';
      link.rel = 'stylesheet';
      link.href = 'https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@400;500;700&display=swap';
      document.head.appendChild(link);
    }
    const style = document.createElement('style');
    style.id = 'exec-pdf-style';
    style.textContent = styleHTML();
    document.head.appendChild(style);

    const root = document.createElement('div');
    root.id = 'exec-print-root';
    root.innerHTML = html;
    document.body.appendChild(root);

    const cleanup = () => {
      try { root.remove(); } catch (_) {}
      try { style.remove(); } catch (_) {}
      window.removeEventListener('afterprint', cleanup);
    };
    window.addEventListener('afterprint', cleanup);

    // Give fonts + raster images a beat to settle, then print
    try {
      if (document.fonts && document.fonts.ready) {
        await Promise.race([document.fonts.ready, new Promise(r => setTimeout(r, 1500))]);
      }
    } catch (_) {}
    await new Promise(r => setTimeout(r, 250));
    window.print();
    setTimeout(cleanup, 60000); // fallback if afterprint never fires
  }

  // ── Modal ────────────────────────────────────────────────────────────────
  function openModal() {
    const st = window.__execState;
    if (!st || !Array.isArray(st.weeks) || !st.weeks.length) {
      alert('Open the Executive page and let it finish loading before exporting.');
      return;
    }
    if (document.getElementById('exec-pdf-modal')) return;

    const overlay = document.createElement('div');
    overlay.id = 'exec-pdf-modal';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,0.4);display:flex;align-items:center;justify-content:center;font-family:-apple-system,BlinkMacSystemFont,sans-serif;';
    overlay.innerHTML = `
      <div style="background:#fff;border-radius:14px;width:420px;max-width:92vw;padding:24px;box-shadow:0 12px 48px rgba(0,0,0,0.25);">
        <div style="font-size:17px;font-weight:700;color:${DARK};margin-bottom:4px;">Download Executive Report</div>
        <div style="font-size:12px;color:${MID};margin-bottom:18px;line-height:1.5;">A4 landscape PDF, one chart per page, matching the period currently shown (${esc(periodLabelOf(st))}). Use your browser's “Save as PDF” in the print dialog.</div>
        <label style="display:flex;align-items:flex-start;gap:10px;cursor:pointer;padding:12px;border:0.5px solid rgba(0,0,0,0.12);border-radius:10px;margin-bottom:18px;">
          <input type="checkbox" id="exec-pdf-ai" style="margin-top:2px;width:16px;height:16px;accent-color:${BRAND};"/>
          <span style="font-size:12px;color:${DARK};line-height:1.45;">Include AI <strong>Improvement Intelligence</strong><br><span style="color:${MID};">Adds a generated analysis section — takes a few extra seconds.</span></span>
        </label>
        <div style="display:flex;justify-content:flex-end;gap:10px;">
          <button id="exec-pdf-cancel" style="font-size:13px;padding:8px 16px;border-radius:8px;border:0.5px solid rgba(0,0,0,0.15);background:#fff;color:${MID};cursor:pointer;">Cancel</button>
          <button id="exec-pdf-go" style="font-size:13px;font-weight:600;padding:8px 18px;border-radius:8px;border:none;background:${BRAND};color:#fff;cursor:pointer;">Generate PDF</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);

    const close = () => { try { overlay.remove(); } catch (_) {} };
    overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
    document.getElementById('exec-pdf-cancel').addEventListener('click', close);

    document.getElementById('exec-pdf-go').addEventListener('click', async () => {
      if (_building) return;
      _building = true;
      const includeAI = !!document.getElementById('exec-pdf-ai').checked;
      const goBtn = document.getElementById('exec-pdf-go');
      goBtn.textContent = includeAI ? 'Generating analysis…' : 'Preparing…';
      goBtn.style.opacity = '0.7';
      goBtn.style.cursor = 'default';
      let aiInsights = [];
      try {
        if (includeAI) {
          try { aiInsights = await _fetchInsights(st.weeks, st.facility); }
          catch (e) { console.warn('[exec-pdf] AI insights unavailable', e); aiInsights = []; }
        }
        close();
        await buildAndPrint(st, includeAI, aiInsights);
      } catch (e) {
        console.error('[exec-pdf] generate failed', e);
        alert('Could not generate the PDF: ' + (e && e.message ? e.message : e));
      } finally {
        _building = false;
      }
    });
  }

  // ── Button injection + mount ───────────────────────────────────────────────
  function injectButton() {
    const bar = document.getElementById('exec-range-bar');
    if (!bar || document.getElementById('exec-pdf-btn')) return;
    const btn = document.createElement('button');
    btn.id = 'exec-pdf-btn';
    btn.type = 'button';
    btn.textContent = 'Download PDF';
    btn.style.cssText = `margin-left:12px;font-size:11px;font-weight:600;padding:5px 14px;border-radius:6px;border:0.5px solid ${BRAND};background:${BRAND};color:#fff;cursor:pointer;transition:opacity .15s;`;
    btn.addEventListener('mouseenter', () => btn.style.opacity = '0.85');
    btn.addEventListener('mouseleave', () => btn.style.opacity = '1');
    btn.addEventListener('click', openModal);
    const loading = document.getElementById('exec-loading');
    if (loading && loading.parentNode === bar) bar.insertBefore(btn, loading);
    else bar.appendChild(btn);
  }

  function tryMount() {
    const ep = document.getElementById('page-exec');
    if (!ep || ep.style.display === 'none' || ep.classList.contains('hidden')) return;
    injectButton();
  }

  window.addEventListener('exec:rendered', injectButton);
  window.addEventListener('state:ready', () => { if ((location.hash || '').toLowerCase() === '#exec') setTimeout(tryMount, 150); });
  window.addEventListener('hashchange', () => { if ((location.hash || '').toLowerCase() === '#exec') setTimeout(tryMount, 150); });
  const _t = setInterval(tryMount, 600);
  setTimeout(() => clearInterval(_t), 30000);
  if ((location.hash || '').toLowerCase() === '#exec') setTimeout(tryMount, 300);

})();
