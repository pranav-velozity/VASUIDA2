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
        if (ms.state === 'assumed') { cell.unconfirmed++; out.unconfirmed++; continue; }
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

  function stageCell(cell) {
    const measured = cell.onTime + cell.late + cell.early;
    if (!measured && !cell.unconfirmed) {
      return `<td class="th-cell"><span class="th-none">—</span></td>`;
    }
    if (!measured) {
      // Nothing recorded, so nothing can be claimed. Grey, and it says why.
      return `<td class="th-cell"><span class="th-pill" style="background:#F2F2F5;color:${LIGHT};"
        title="${cell.unconfirmed} stage(s) past their date with nobody confirming">${cell.unconfirmed} unconfirmed</span></td>`;
    }
    const pct = Math.round((cell.onTime + cell.early) / measured * 100);
    const med = median(cell.slip) || 0;
    const ink = pct >= 90 ? GREEN : (pct >= 70 ? AMBER : RED);
    const bg = pct >= 90 ? 'rgba(27,127,59,.12)' : (pct >= 70 ? 'rgba(183,121,31,.12)' : 'rgba(153,0,51,.10)');
    return `<td class="th-cell">
      <span class="th-pill" style="background:${bg};color:${ink};">${pct}%</span>
      <span class="th-sub">${med > 0 ? '+' + med + 'd' : (med < 0 ? med + 'd' : 'on plan')}${
        cell.unconfirmed ? ` · ${cell.unconfirmed} unconf` : ''}</span>
    </td>`;
  }

  function weekRow(ws, s) {
    const wk = isoWeek(ws);
    const varMed = median(s.transit.variance);
    const drift = median(s.fcDrift);
    return `
      <tr class="th-row" data-ws="${esc(ws)}">
        <td class="th-week">
          <button class="th-wk" data-week="${esc(ws)}">W${wk}</button>
          <span class="th-sub">${esc(day(ws))}</span>
        </td>
        <td class="th-cell">
          <span class="th-num">${s.movements}</span>
          <span class="th-sub">${s.lanes} lane${s.lanes === 1 ? '' : 's'}</span>
        </td>
        ${STAGES.map(st => stageCell(s.stages[st.key])).join('')}
        <td class="th-cell">
          ${varMed == null
            ? `<span class="th-none">—</span>`
            : `<span class="th-pill" style="background:${varMed > 1 ? 'rgba(153,0,51,.10)' : 'rgba(27,127,59,.12)'};
                 color:${varMed > 1 ? RED : GREEN};">${varMed > 0 ? '+' : ''}${varMed}d</span>`}
          <span class="th-sub">${s.requoted ? `${s.requoted} re-quoted +${s.requoteDays}d` : 'no re-quotes'}</span>
        </td>
        <td class="th-cell">
          ${drift == null ? `<span class="th-none">—</span>`
            : `<span class="th-pill" style="background:${drift > 1 ? 'rgba(153,0,51,.10)' : 'rgba(27,127,59,.12)'};
                 color:${drift > 1 ? RED : GREEN};">${drift > 0 ? '+' : ''}${drift}d</span>`}
          <span class="th-sub">${s.unverified ? `${s.unverified} unverified` : 'all quoted'}</span>
        </td>
        <td class="th-cell">
          <button class="th-link" data-lastmile="${esc(ws)}">Last mile &rarr;</button>
        </td>
      </tr>`;
  }

  function styles() {
    if (el('th-css')) return;
    const st = document.createElement('style');
    st.id = 'th-css';
    st.textContent = `
      #th-ov{position:fixed;inset:0;z-index:9998;background:rgba(16,18,27,.45);
        display:flex;align-items:flex-start;justify-content:center;padding:24px 16px;overflow-y:auto;}
      #th-card{background:#fff;border-radius:16px;width:100%;max-width:1440px;padding:26px 30px;}
      .th-h{font-size:17px;font-weight:700;color:${DARK};letter-spacing:-.01em;}
      .th-sub{display:block;font-size:10px;color:${LIGHT};margin-top:1px;}
      .th-tbl{width:100%;border-collapse:collapse;margin-top:14px;}
      .th-tbl th{text-align:left;padding:8px 9px;font-size:9px;color:${LIGHT};font-weight:700;
        text-transform:uppercase;letter-spacing:.06em;border-bottom:.5px solid rgba(0,0,0,.08);}
      .th-cell{padding:9px;border-top:.5px solid rgba(0,0,0,.05);vertical-align:top;}
      .th-week{padding:9px;border-top:.5px solid rgba(0,0,0,.05);white-space:nowrap;}
      .th-row:hover{background:#FBFBFC;}
      .th-pill{display:inline-block;font-size:11px;font-weight:600;border-radius:6px;padding:2px 7px;}
      .th-num{font-family:ui-monospace,monospace;font-size:13px;color:${DARK};}
      .th-none{color:#D1D1D6;font-size:12px;}
      .th-wk{background:none;border:0;padding:0;font-family:ui-monospace,monospace;font-size:13px;
        font-weight:700;color:${DARK};cursor:pointer;border-bottom:1px dashed rgba(0,0,0,.25);}
      .th-link{background:none;border:0;padding:0;font-size:11px;color:${BRAND};cursor:pointer;}
      .th-btn{background:${DARK};color:#fff;border:0;border-radius:8px;padding:7px 14px;
        font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;}
      .th-btn.ghost{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);font-weight:500;}
      .th-note{font-size:11px;color:${MID};line-height:1.55;margin-top:12px;
        border-top:.5px solid rgba(0,0,0,.06);padding-top:11px;}
    `;
    document.head.appendChild(st);
  }

  let _rows = [];   // [{ ws, summary, consignments }]

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

  async function open(opts) {
    styles();
    if (el('th-ov')) return;

    const base = (opts && opts.week) || window.state?.weekStart || mondayOf(new Date());
    const weeks = [];
    for (let i = 0; i < WEEKS; i++) weeks.push(shiftWeek(base, -i));

    const ov = document.createElement('div');
    ov.id = 'th-ov';
    ov.innerHTML = `
      <div id="th-card">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;">
          <div>
            <div class="th-h">Transit performance</div>
            <div style="font-size:11.5px;color:${MID};margin-top:2px;">
              Last ${WEEKS} weeks &middot; measured against the plan as entered, not a default</div>
          </div>
          <div style="display:flex;gap:8px;">
            <button class="th-btn ghost" id="th-dl">Download</button>
            <button class="th-btn ghost" id="th-close">Close</button>
          </div>
        </div>
        <div id="th-body"><div style="font-size:11.5px;color:${LIGHT};padding:16px 0;">Loading ${WEEKS} weeks…</div></div>
      </div>`;
    document.body.appendChild(ov);
    ov.addEventListener('click', (e) => { if (e.target === ov) close(); });
    el('th-close').onclick = close;

    // Fetched a few at a time: ten sequential round trips is a long wait, ten at once is a
    // burst the API does not need.
    _rows = [];
    for (let i = 0; i < weeks.length; i += 3) {
      const batch = weeks.slice(i, i + 3);
      const got = await Promise.all(batch.map(async ws => {
        try {
          const r = await api('/consignments?week=' + encodeURIComponent(ws));
          const list = (r && r.consignments) || [];
          return { ws, consignments: list, summary: summarise(list) };
        } catch (e) {
          return { ws, consignments: [], summary: summarise([]), error: String(e.message || e) };
        }
      }));
      _rows.push(...got);
    }

    const anyData = _rows.some(r => r.summary.movements);
    el('th-body').innerHTML = `
      <table class="th-tbl">
        <thead><tr>
          <th>Week</th><th>Movements</th>
          ${STAGES.map(s => `<th>${esc(s.label)}</th>`).join('')}
          <th>Transit vs quote</th><th>FC drift</th><th></th>
        </tr></thead>
        <tbody>${_rows.map(r => weekRow(r.ws, r.summary)).join('')}</tbody>
      </table>
      ${anyData ? '' : `<div class="th-note">No consignments recorded in these weeks yet.
        Bring a week across from the Transit &amp; Clearing panel and the rows fill in.</div>`}
      <div class="th-note">
        On-time counts a stage that happened on or before its planned date. A stage nobody
        confirmed is not counted as on-time — it is shown as unconfirmed, because an
        unrecorded date is not evidence of anything.
        Transit is measured against the first quote entered; a later re-quote moves the plan
        but is reported as a deviation rather than replacing the promise.
      </div>`;

    el('th-dl').onclick = download;
    el('th-body').querySelectorAll('[data-week]').forEach(b => b.onclick = () => {
      // Into that week's consignments, where the tiles live.
      close();
      if (window.__consignments && typeof window.__consignments.setWeek === 'function') {
        window.__consignments.setWeek(b.getAttribute('data-week'));
      }
      const t = document.getElementById('cg-root');
      if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    el('th-body').querySelectorAll('[data-lastmile]').forEach(b => b.onclick = () => {
      // Forward into the last mile: the same week, the next link in the chain.
      if (typeof window.__openLastMileHistory === 'function') { close(); window.__openLastMileHistory(); }
      else alert('The Last Mile report is not loaded on this page.');
    });
  }

  function close() { const o = el('th-ov'); if (o) o.remove(); }

  // ── Getting in ──
  // Injected beside the other report buttons rather than wired into the weekly report's
  // markup, so this file can be deployed on its own and does not depend on another module's
  // internals staying where they are.
  function injectButton() {
    if (el('th-btn-open')) return;
    const anchor = el('btn-consolidated-download') || el('lm-hist-btn') || el('wh-hist-btn');
    if (!anchor || !anchor.parentElement) return;
    const b = document.createElement('button');
    b.id = 'th-btn-open';
    b.textContent = 'Transit';
    b.className = anchor.className || '';
    b.style.cssText = anchor.style.cssText || '';
    b.onclick = (e) => { e.preventDefault(); open(); };
    anchor.parentElement.insertBefore(b, anchor.nextSibling);
  }

  const obs = new MutationObserver(() => injectButton());
  obs.observe(document.body, { childList: true, subtree: true });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', injectButton);
  else injectButton();

  window.__openTransitHistory = open;
  console.log('[transit-history] v1 loaded — window.__openTransitHistory()');
})();
