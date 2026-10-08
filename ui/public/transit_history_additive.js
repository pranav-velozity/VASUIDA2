/* ── VelOzity Pinpoint — transit performance, last 10 weeks v1 ──
   The middle of the chain. Receiving and VAS are covered by the weekly history report; the
   last mile by its own. This is what happens in between, and until now there was no report
   of it at all — because the dates it would have been built on were copies of the plan.

   It is a drill-down, not a new front door: the weekly report's Transit row opens it, and its
   last-mile column opens the Last Mile report. Same week rows, same vocabulary, same download
   behaviour, so nobody has to learn a third layout.

   What it measures, and why that phrasing matters:

     · on-time is against the plan as the team entered it, not against a rule default. A
       consignment quoted at 12 days is judged on 12 days.

     · a re-quote is a deviation, not a correction. Quoted 12, re-quoted 15, achieved 17 is
       three facts; collapsing them to "2 days late" hides where the time went.

     · a stage nobody confirmed is not on time. It is unconfirmed, and it is counted and shown
       as such — the whole point of the exercise is to stop assuming.

   window.__openTransitHistory() opens it.
*/
(function () {
  'use strict';

  const WEEKS = 10;
  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#8E8E93';
  const GREEN = '#1B7F3B', AMBER = '#B7791F', RED = '#990033';

  const STAGES = [
    { key: 'packing_list_ready', label: 'Packing list', owner: 'supplier' },
    { key: 'origin_cleared', label: 'Origin cleared', owner: 'origin' },
    { key: 'departed', label: 'Departed', owner: 'carrier' },
    { key: 'arrived', label: 'Arrived', owner: 'carrier' },
    { key: 'dest_cleared', label: 'Dest cleared', owner: 'destination' },
    { key: 'fc_receipt', label: 'FC receipt', owner: 'destination' },
  ];

  const esc = (v) => String(v == null ? '' : v)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const el = (id) => document.getElementById(id);
  const day = (d) => {
    if (!d) return '—';
    const x = new Date(String(d).slice(0, 10) + 'T00:00:00Z');
    return isNaN(x) ? String(d) : x.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  };
  const isoWeek = (ymd) => {
    const d = new Date(ymd + 'T00:00:00Z');
    const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    t.setUTCDate(t.getUTCDate() - ((t.getUTCDay() + 6) % 7) + 3);
    const f = new Date(Date.UTC(t.getUTCFullYear(), 0, 4));
    f.setUTCDate(f.getUTCDate() - ((f.getUTCDay() + 6) % 7) + 3);
    return 1 + Math.round((t - f) / (7 * 86400000));
  };
  const shiftWeek = (ws, n) => {
    const d = new Date(ws + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 7 * n);
    return d.toISOString().slice(0, 10);
  };
  const mondayOf = (d) => {
    const x = new Date(d);
    x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7));
    return x.toISOString().slice(0, 10);
  };

  async function api(path) {
    if (typeof window.api === 'function') return window.api(path);
    const base = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
    let token = null;
    if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
    const r = await fetch(base + path, { headers: token ? { Authorization: 'Bearer ' + token } : {} });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  }

  // ── What a week amounts to ──
  // Deliberately counts what is NOT known as well as what is. A week where nobody confirmed
  // anything should read as unmeasured, not as perfect.
  function summarise(consignments) {
    const out = {
      movements: consignments.length,
      lanes: consignments.reduce((n, c) => n + (c.lanes || []).length, 0),
      stages: {}, unverified: 0, requoted: 0, requoteDays: 0,
      transit: { quoted: [], achieved: [], variance: [] },
      fcDrift: [], unconfirmed: 0, recorded: 0,
    };
    for (const s of STAGES) out.stages[s.key] = { onTime: 0, late: 0, early: 0, unconfirmed: 0, slip: [] };

    for (const c of consignments) {
      if (!c.transit_confirmed) out.unverified++;
      const t = c.transit || {};
      if (t.requotes) { out.requoted++; out.requoteDays += (t.requote_days || 0); }
      if (t.baseline != null) out.transit.quoted.push(t.baseline);
      if (t.achieved != null) out.transit.achieved.push(t.achieved);
      if (t.variance != null) out.transit.variance.push(t.variance);

      const conf = c.confidence || {};
      if (typeof conf.drift === 'number') out.fcDrift.push(conf.drift);

      for (const ms of (c.milestones || [])) {
        const cell = out.stages[ms.stage];
        if (!cell) continue;
        // A rhythm-recorded sea stage is a schedule that held, not an observation. Counted as
        // on-time it would put packing and origin clearance at 100% every week forever — a
        // number that looks like performance and carries none. It belongs with the
        // unconfirmed, because unobserved is exactly what it is.
        if (ms.state === 'assumed' || ms.auto) { cell.unconfirmed++; out.unconfirmed++; continue; }
        out.recorded++;
        if (!ms.planned_at || !ms.actual_at) { cell.onTime++; continue; }
        const slip = Math.round(
          (new Date(ms.actual_at + 'T00:00:00Z') - new Date(ms.planned_at + 'T00:00:00Z')) / 86400000);
        cell.slip.push(slip);
        if (slip > 0) cell.late++; else if (slip < 0) cell.early++; else cell.onTime++;
      }
    }
    return out;
  }

  const median = (a) => {
    if (!a.length) return null;
    const s = a.slice().sort((x, y) => x - y);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : Math.round(((s[m - 1] + s[m]) / 2) * 10) / 10;
  };

  // Who owns each leg. The point of the report is not that things were late, but which party
  // the lateness belongs to — that is the difference between a complaint and a conversation.
  const OWNER_LABEL = { supplier: 'Supplier', origin: 'Origin landside',
                        carrier: 'Carrier', destination: 'Destination landside' };
  const OWNER_INK = { supplier: '#7C5CBF', origin: '#B7791F',
                      carrier: '#990033', destination: '#1E9BD7' };

  function styles() {
    if (el('th-css')) return;
    const st = document.createElement('style');
    st.id = 'th-css';
    st.textContent = `
      /* A full page, not a dialog. Ten weeks of six stages does not fit in a box in the
         middle of the screen, and shrinking it to fit is what made it look like a export. */
      #th-page{position:fixed;inset:0;z-index:9998;background:#F7F7F9;overflow-y:auto;}
      #th-inner{max-width:1180px;margin:0 auto;padding:30px 28px 60px;}
      .th-top{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;flex-wrap:wrap;}
      .th-h{font-size:24px;font-weight:700;color:${DARK};letter-spacing:-.02em;}
      .th-lede{font-size:13px;color:${MID};margin-top:4px;max-width:620px;line-height:1.55;}
      .th-card{background:#fff;border:.5px solid rgba(0,0,0,.07);border-radius:14px;padding:20px 22px;
        margin-top:16px;box-shadow:0 1px 2px rgba(16,18,27,.04),0 6px 18px rgba(16,18,27,.05);}
      .th-ch{font-size:13px;font-weight:600;color:${DARK};}
      .th-cs{font-size:11px;color:${LIGHT};margin-top:2px;line-height:1.5;}

      .th-heads{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:16px;}
      .th-stat{background:#fff;border:.5px solid rgba(0,0,0,.07);border-radius:14px;padding:16px 18px;}
      .th-sl{font-size:9.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;}
      .th-sv{font-size:26px;font-weight:600;letter-spacing:-.02em;margin-top:3px;}
      .th-ss{font-size:11px;color:${MID};margin-top:2px;line-height:1.45;}

      /* Where time is lost: one bar per stage, coloured by who owns it. */
      .th-bar{display:grid;grid-template-columns:130px 1fr 92px;gap:12px;align-items:center;padding:7px 0;}
      .th-bname{font-size:12px;color:${DARK};}
      .th-bown{display:block;font-size:9.5px;color:${LIGHT};}
      .th-btrack{height:10px;background:#F1F1F4;border-radius:5px;overflow:hidden;position:relative;}
      .th-bfill{display:block;height:10px;border-radius:5px;}
      .th-bval{font-family:ui-monospace,monospace;font-size:12px;text-align:right;}

      /* Week by week: a strip per week, not a spreadsheet row. */
      .th-week{display:grid;grid-template-columns:96px 1fr 128px 104px 92px;gap:14px;align-items:center;
        padding:11px 10px;border-radius:10px;cursor:pointer;transition:background .15s;}
      .th-week:hover{background:#FAFAFB;}
      .th-wk{font-family:ui-monospace,monospace;font-size:14px;font-weight:700;color:${DARK};}
      .th-wd{display:block;font-size:10px;color:${LIGHT};}
      .th-dots{display:flex;gap:4px;align-items:center;}
      .th-dot{width:100%;height:22px;border-radius:5px;position:relative;flex:1;min-width:0;}
      .th-dlabel{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
        font-size:9px;font-weight:700;}
      .th-none{color:#D1D1D6;font-size:12px;}
      .th-link{background:none;border:0;padding:0;font-size:11px;color:${BRAND};cursor:pointer;}
      .th-btn{background:${DARK};color:#fff;border:0;border-radius:9px;padding:8px 15px;
        font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;}
      .th-btn.ghost{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);font-weight:500;}
      .th-note{font-size:11.5px;color:${MID};line-height:1.6;}
      .th-legend{display:flex;gap:14px;flex-wrap:wrap;font-size:10.5px;color:${MID};margin-top:10px;}
      @media (max-width:820px){ .th-week{grid-template-columns:1fr;gap:6px;} }
    `;
    document.head.appendChild(st);
  }

  let _rows = [];

  function download() {
    const head = ['Week', 'Week start', 'Movements', 'Lanes',
      ...STAGES.map(s => s.label + ' on-time %'),
      ...STAGES.map(s => s.label + ' median slip'),
      ...STAGES.map(s => s.label + ' unconfirmed'),
      'Transit variance (median)', 'Re-quoted', 'Re-quote days', 'FC drift (median)', 'Unverified'];
    const lines = [head.join(',')];
    for (const r of _rows) {
      const s = r.summary;
      const pct = (k) => { const c = s.stages[k]; const m = c.onTime + c.late + c.early;
        return m ? Math.round((c.onTime + c.early) / m * 100) : ''; };
      lines.push([
        'W' + isoWeek(r.ws), r.ws, s.movements, s.lanes,
        ...STAGES.map(st => pct(st.key)),
        ...STAGES.map(st => { const m = median(s.stages[st.key].slip); return m == null ? '' : m; }),
        ...STAGES.map(st => s.stages[st.key].unconfirmed),
        median(s.transit.variance) ?? '', s.requoted, s.requoteDays,
        median(s.fcDrift) ?? '', s.unverified,
      ].join(','));
    }
    // A second sheet's worth: every consignment, so the summary can be checked rather than
    // taken on faith.
    lines.push('');
    lines.push(['Week', 'Reference', 'Mode', 'Lanes', 'Quoted', 'Re-quoted', 'Achieved',
                'Variance vs promise', 'ETA FC', 'Baseline FC', 'Confidence', 'Unconfirmed stages'].join(','));
    for (const r of _rows) {
      for (const c of r.consignments) {
        const t = c.transit || {}, conf = c.confidence || {};
        lines.push([
          'W' + isoWeek(r.ws), '"' + String(c.reference || '').replace(/"/g, '""') + '"', c.mode,
          (c.lanes || []).length, t.baseline ?? '', t.requotes ? t.quoted : '', t.achieved ?? '',
          t.variance ?? '', c.eta_fc || '', c.baseline_fc_at || '', conf.level || '',
          (c.milestones || []).filter(m => m.state === 'assumed').length,
        ].join(','));
      }
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'Transit_performance_last_' + WEEKS + '_weeks.csv';
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }

  // ── The reading ──
  // A sentence before the numbers. Somebody opening this wants to know what it says, not to
  // derive it from a grid.
  function headline(all) {
    const measured = [];
    for (const st of STAGES) {
      const c = all.stages[st.key];
      const n = c.onTime + c.late + c.early;
      if (n) measured.push({ st, n, med: median(c.slip) || 0, late: c.late });
    }
    if (!measured.length) {
      return all.unconfirmed
        ? `Nothing has been confirmed yet. ${all.unconfirmed} stage${all.unconfirmed === 1 ? '' : 's'} `
          + `across ${all.movements} movement${all.movements === 1 ? '' : 's'} are waiting on somebody, `
          + `so where the time goes cannot be established.`
        : 'No consignments recorded in these weeks.';
    }
    const worst = measured.slice().sort((a, b) => b.med - a.med)[0];
    if (worst.med <= 0) {
      return `Across ${all.movements} movements, no stage is adding time against the plan as entered.`;
    }
    const owner = OWNER_LABEL[worst.st.owner] || worst.st.owner;
    return `${worst.st.label} adds the most time — a median of ${worst.med} day${worst.med === 1 ? '' : 's'} `
      + `past plan across ${worst.n} recorded, which sits with ${owner.toLowerCase()}.`
      + (all.unconfirmed ? ` ${all.unconfirmed} stages remain unconfirmed and are excluded.` : '');
  }

  function stageBars(all) {
    const rows = STAGES.map(st => {
      const c = all.stages[st.key];
      const n = c.onTime + c.late + c.early;
      return { st, n, med: n ? (median(c.slip) || 0) : null, unconf: c.unconfirmed,
               pct: n ? Math.round((c.onTime + c.early) / n * 100) : null };
    });
    const peak = Math.max(1, ...rows.map(r => Math.abs(r.med || 0)));
    return rows.map(r => `
      <div class="th-bar">
        <span>
          <span class="th-bname">${esc(r.st.label)}</span>
          <span class="th-bown">${esc(OWNER_LABEL[r.st.owner])}</span>
        </span>
        <span class="th-btrack">
          ${r.med == null ? '' : `<span class="th-bfill" style="width:${Math.round(Math.max(0, r.med) / peak * 100)}%;
            background:${OWNER_INK[r.st.owner]};opacity:${r.med > 0 ? .85 : .25};"></span>`}
        </span>
        <span class="th-bval" style="color:${r.med == null ? '#D1D1D6' : (r.med > 0 ? RED : GREEN)};">
          ${r.med == null ? (r.unconf ? r.unconf + ' unconf' : '—')
            : (r.med > 0 ? '+' + r.med + 'd' : (r.med < 0 ? r.med + 'd' : 'on plan'))}
        </span>
      </div>`).join('');
  }

  function weekStrip(r) {
    const s = r.summary;
    const wk = isoWeek(r.ws);
    if (!s.movements) {
      return `<div class="th-week" data-week="${esc(r.ws)}">
        <span><span class="th-wk" style="color:#C7C7CC;">W${wk}</span>
          <span class="th-wd">${esc(day(r.ws))}</span></span>
        <span class="th-none">No consignments recorded</span>
        <span></span><span></span><span></span>
      </div>`;
    }
    const varMed = median(s.transit.variance), drift = median(s.fcDrift);
    return `
      <div class="th-week" data-week="${esc(r.ws)}">
        <span><span class="th-wk">W${wk}</span><span class="th-wd">${esc(day(r.ws))}</span></span>
        <span class="th-dots">
          ${STAGES.map(st => {
            const c = s.stages[st.key];
            const n = c.onTime + c.late + c.early;
            if (!n) return `<span class="th-dot" style="background:#F1F1F4;" title="${esc(st.label)} — ${c.unconfirmed} unconfirmed">
              <span class="th-dlabel" style="color:#C7C7CC;">${c.unconfirmed || ''}</span></span>`;
            const pct = Math.round((c.onTime + c.early) / n * 100);
            const ink = pct >= 90 ? GREEN : (pct >= 70 ? AMBER : RED);
            const bg = pct >= 90 ? 'rgba(27,127,59,.15)' : (pct >= 70 ? 'rgba(183,121,31,.16)' : 'rgba(153,0,51,.13)');
            return `<span class="th-dot" style="background:${bg};"
              title="${esc(st.label)} — ${pct}% on plan, ${n} recorded${c.unconfirmed ? ', ' + c.unconfirmed + ' unconfirmed' : ''}">
              <span class="th-dlabel" style="color:${ink};">${pct}</span></span>`;
          }).join('')}
        </span>
        <span style="font-size:11.5px;color:${MID};">
          ${varMed == null ? '<span class="th-none">no transit measured</span>'
            : `<b style="color:${varMed > 1 ? RED : GREEN};">${varMed > 0 ? '+' : ''}${varMed}d</b> vs quote`}
          ${s.requoted ? `<span class="th-wd">${s.requoted} re-quoted +${s.requoteDays}d</span>` : ''}
        </span>
        <span style="font-size:11.5px;color:${MID};">
          ${drift == null ? '<span class="th-none">—</span>'
            : `<b style="color:${drift > 1 ? RED : GREEN};">${drift > 0 ? '+' : ''}${drift}d</b> FC drift`}
          ${s.unverified ? `<span class="th-wd" style="color:${RED};">${s.unverified} unverified</span>` : ''}
        </span>
        <span style="text-align:right;">
          <button class="th-link" data-lastmile="${esc(r.ws)}">Last mile &rarr;</button>
        </span>
      </div>`;
  }

  async function open(opts) {
    styles();
    if (el('th-page')) return;

    const base = (opts && opts.week) || window.state?.weekStart || mondayOf(new Date());
    const weeks = [];
    for (let i = 0; i < WEEKS; i++) weeks.push(shiftWeek(base, -i));

    const page = document.createElement('div');
    page.id = 'th-page';
    page.innerHTML = `
      <div id="th-inner">
        <div class="th-top">
          <div>
            <div class="th-h">Transit performance</div>
            <div class="th-lede">The ten weeks from ${esc(day(weeks[weeks.length - 1]))} to
              ${esc(day(weeks[0]))}. Every figure is measured against the plan as your team
              entered it — the transit time quoted for that sailing, not a default.</div>
          </div>
          <div style="display:flex;gap:8px;">
            <button class="th-btn ghost" id="th-dl">Download</button>
            <button class="th-btn" id="th-close">Close</button>
          </div>
        </div>
        <div id="th-body" class="th-card">
          <div class="th-cs">Reading ${WEEKS} weeks…</div>
        </div>
      </div>`;
    document.body.appendChild(page);
    el('th-close').onclick = close;
    document.addEventListener('keydown', onKey);

    _rows = [];
    for (let i = 0; i < weeks.length; i += 3) {
      const got = await Promise.all(weeks.slice(i, i + 3).map(async ws => {
        try {
          const r = await api('/consignments?week=' + encodeURIComponent(ws));
          const list = (r && r.consignments) || [];
          return { ws, consignments: list, summary: summarise(list) };
        } catch (e) {
          return { ws, consignments: [], summary: summarise([]) };
        }
      }));
      _rows.push(...got);
    }

    // Everything, pooled, for the headline and the stage bars. A ten-week view of where time
    // goes is more use than ten separate weekly views of the same thing.
    const all = summarise(_rows.flatMap(r => r.consignments));
    const onTimeAll = (() => {
      let ok = 0, n = 0;
      for (const st of STAGES) { const c = all.stages[st.key];
        ok += c.onTime + c.early; n += c.onTime + c.late + c.early; }
      return n ? Math.round(ok / n * 100) : null;
    })();
    const varAll = median(all.transit.variance);
    const driftAll = median(all.fcDrift);

    el('th-body').outerHTML = `
      <div class="th-card" style="margin-top:18px;">
        <div class="th-ch">What this says</div>
        <div class="th-note" style="margin-top:6px;">${esc(headline(all))}</div>
      </div>

      <div class="th-heads">
        <div class="th-stat">
          <div class="th-sl">Movements</div>
          <div class="th-sv" style="color:${DARK};">${all.movements}</div>
          <div class="th-ss">${all.lanes} lanes carried</div>
        </div>
        <div class="th-stat">
          <div class="th-sl">Stages on plan</div>
          <div class="th-sv" style="color:${onTimeAll == null ? LIGHT : (onTimeAll >= 90 ? GREEN : onTimeAll >= 70 ? AMBER : RED)};">
            ${onTimeAll == null ? '—' : onTimeAll + '%'}</div>
          <div class="th-ss">${all.recorded} recorded${all.unconfirmed ? ` · ${all.unconfirmed} never confirmed` : ''}</div>
        </div>
        <div class="th-stat">
          <div class="th-sl">Transit vs quote</div>
          <div class="th-sv" style="color:${varAll == null ? LIGHT : (varAll > 1 ? RED : GREEN)};">
            ${varAll == null ? '—' : (varAll > 0 ? '+' : '') + varAll + 'd'}</div>
          <div class="th-ss">${all.requoted ? `${all.requoted} re-quoted, +${all.requoteDays}d added to plan` : 'no re-quotes'}</div>
        </div>
        <div class="th-stat">
          <div class="th-sl">FC date movement</div>
          <div class="th-sv" style="color:${driftAll == null ? LIGHT : (driftAll > 1 ? RED : GREEN)};">
            ${driftAll == null ? '—' : (driftAll > 0 ? '+' : '') + driftAll + 'd'}</div>
          <div class="th-ss">${all.unverified ? `${all.unverified} without a carrier quote` : 'all quoted'}</div>
        </div>
      </div>

      <div class="th-card">
        <div class="th-ch">Where the time goes</div>
        <div class="th-cs">Median days past plan at each stage, and who owns that leg. Stages
          nobody confirmed are excluded rather than counted as on time.</div>
        <div style="margin-top:12px;">${stageBars(all)}</div>
        <div class="th-legend">
          ${Object.entries(OWNER_LABEL).map(([k, v]) =>
            `<span><span style="display:inline-block;width:9px;height:9px;border-radius:2px;
              background:${OWNER_INK[k]};margin-right:5px;"></span>${esc(v)}</span>`).join('')}
        </div>
      </div>

      <div class="th-card">
        <div style="display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap;">
          <div>
            <div class="th-ch">Week by week</div>
            <div class="th-cs">Each block is a stage, left to right: packing list through FC
              receipt. The number is the percentage that happened on plan.</div>
          </div>
          <span class="th-cs">Click a week to open its consignments</span>
        </div>
        <div style="margin-top:10px;">${_rows.map(weekStrip).join('')}</div>
      </div>

      <div class="th-card">
        <div class="th-ch">How to read it</div>
        <div class="th-note" style="margin-top:6px;">
          A stage counts as on plan if it happened on or before the date the plan gave it.
          A stage nobody confirmed is not on time and not late — it is unconfirmed, and it is
          left out of the percentages rather than flattering them.
          Transit is judged against the first quote entered for that sailing; a later re-quote
          moves the plan but is reported separately, so a carrier cannot erase a slip by
          revising it.
        </div>
      </div>`;

    el('th-dl').onclick = download;
    document.querySelectorAll('#th-page [data-week]').forEach(b => b.onclick = (e) => {
      if (e.target.closest('[data-lastmile]')) return;
      const ws = b.getAttribute('data-week');
      close();
      if (window.__consignments && typeof window.__consignments.setWeek === 'function') {
        window.__consignments.setWeek(ws);
      }
      const t = document.getElementById('cg-root');
      if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    document.querySelectorAll('#th-page [data-lastmile]').forEach(b => b.onclick = (e) => {
      e.stopPropagation();
      if (typeof window.__openLastMileHistory === 'function') { close(); window.__openLastMileHistory(); }
      else alert('The Last Mile report is not loaded on this page.');
    });
  }

  function onKey(e) { if (e.key === 'Escape') close(); }

  function close() {
    const o = el('th-page');
    if (o) o.remove();
    document.removeEventListener('keydown', onKey);
  }

  // ── Getting in ──
  // Beside the other report buttons on Reports & Downloads, styled as they are: a secondary
  // action next to Download All, not a second primary competing with it. Copying the anchor's
  // styling made it black and it read as a duplicate of the download.
  function injectButton() {
    const anchor = el('btn-consolidated-download');
    if (!anchor || el('th-hist-btn')) return;
    const b = document.createElement('button');
    b.id = 'th-hist-btn';
    b.type = 'button';
    b.style.cssText = `display:flex;align-items:center;gap:8px;background:#fff;color:${DARK};`
      + `border:.5px solid rgba(0,0,0,.14);border-radius:9px;padding:10px 16px;font-size:12px;`
      + `font-weight:500;cursor:pointer;font-family:inherit;margin-right:8px;`;
    b.innerHTML = `<svg width="13" height="13" viewBox="0 0 12 12" fill="none" aria-hidden="true">`
      + `<path d="M1 9l3-3 2.5 2.5L11 3" stroke="${BRAND}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>`
      + `</svg>Transit performance`;
    b.onclick = (e) => { e.preventDefault(); open(); };
    // Before Download All, after Last Mile if it is there, so the reports read as a set.
    const lm = el('lm-hist-btn');
    anchor.parentElement.insertBefore(b, lm ? lm.nextSibling : anchor);
  }

  const obs = new MutationObserver(() => injectButton());
  obs.observe(document.body, { childList: true, subtree: true });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', injectButton);
  else injectButton();

  window.__openTransitHistory = open;
  console.log('[transit-history] v1 loaded — window.__openTransitHistory()');
})();
