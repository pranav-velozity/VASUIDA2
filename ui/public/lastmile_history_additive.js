/* ── VelOzity Pinpoint — Last mile, last 10 weeks v1 ──
   Lives on Reports & Downloads, in its own file, so a fault here cannot reach the dashboard.

   It reads the same week data the dashboard computes (window.__FLOW_API__.computeWeek) and
   nothing else. Read-only: it records no status and writes no receipt.

   Two things it deliberately does NOT do:
   - It never invents a booked time. A container scheduled before the picker existed has only
     the stamp of when someone pressed the button, and the report says so rather than passing
     that off as a delivery slot.
   - It never marks anything on time when there is nothing to measure against. No booked slot
     means no variance, not a pass. */
;(function () {
  'use strict';
  if (window.__LM_HISTORY__) return;
  window.__LM_HISTORY__ = true;

  const WEEKS = 10, PARALLEL = 3;
  const BUFFER_MIN = 60;                    // an hour either side of the booked time is on time
  const DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#1B7F3B', AMBER = '#B7791F', RED = '#B33F40', BRAND = '#990033';

  const el = (id) => document.getElementById(id);
  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const api = () => window.__FLOW_API__ || null;

  function isoWeek(ymd) {
    const d = new Date(ymd + 'T00:00:00Z'); const day = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - day + 3);
    const f = new Date(Date.UTC(d.getUTCFullYear(), 0, 4)); const fd = (f.getUTCDay() + 6) % 7;
    f.setUTCDate(f.getUTCDate() - fd + 3);
    return 1 + Math.round((d - f) / (7 * 86400000));
  }
  const fmtDay = (ymd) => { try { return new Date(ymd + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); } catch (e) { return ymd; } };
  const ms = (v) => { const t = Date.parse(String(v || '').replace(' ', 'T')); return isFinite(t) ? t : null; };
  const fmtWhen = (v) => {
    const t = ms(v); if (t == null) return '';
    try {
      return new Date(t).toLocaleString('en-AU', { day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit', hour12: true });
    } catch (e) { return String(v).replace('T', ' '); }
  };
  const gap = (m) => { const a = Math.abs(m); return a >= 60 ? `${Math.floor(a / 60)}h ${a % 60}m` : `${a}m`; };

  // ── One row per container, per week ──
  function rowsFor(week) {
    const map = (week && week.receiptMap) || {};
    const conts = (week && week.containers) || [];
    const out = [];

    for (const c of conts) {
      const uid = String((c && (c.container_uid || c.uid || c.container_id)) || '').trim();
      const r = map[uid] || map[String(c && c.container_id || '').trim()] || {};
      const bookedFor = String(r.scheduled_for || '').trim();
      const enteredAt = String(r.scheduled_local || '').trim();
      const delivered = String(r.delivery_local || r.delivered_at || '').trim();

      // Variance only where there is a booked slot AND a delivery. Anything else is unknown,
      // which is not the same as on time.
      let variance = null;
      if (bookedFor && delivered) {
        const a = ms(bookedFor), b = ms(delivered);
        if (a != null && b != null) {
          const mins = Math.round((b - a) / 60000);
          variance = { mins, ok: Math.abs(mins) <= BUFFER_MIN };
        }
      }

      const state = delivered ? 'Delivered' : (enteredAt || bookedFor ? 'Scheduled' : 'Not scheduled');
      out.push({
        ws: week.ws,
        container: String((c && (c.container_id || c.container)) || '').trim() || '—',
        vessel: String((c && (c.vessel || c.mode)) || '').trim() || '—',
        size: String((c && c.size_ft) || '').trim(),
        bookedFor, enteredAt, delivered, variance, state,
        pod: !!r.pod_received,
        note: String(r.last_mile_note || '').trim(),
        legacy: !!(enteredAt && !bookedFor),        // scheduled before the picker existed
      });
    }
    return out;
  }

  const pill = (text, ink, bg) => `<span style="font-size:10.5px;font-weight:700;border-radius:6px;
    padding:2px 8px;color:${ink};background:${bg};white-space:nowrap;">${esc(text)}</span>`;

  function stateCell(r) {
    if (r.state === 'Delivered') {
      if (!r.variance) return pill('Delivered', GREEN, 'rgba(27,127,59,.10)') +
        `<div style="font-size:10px;color:${LIGHT};margin-top:3px;">no booked time to measure</div>`;
      return pill(r.variance.ok ? 'On time' : (r.variance.mins > 0 ? 'Late' : 'Early'),
                  r.variance.ok ? GREEN : (r.variance.mins > 0 ? RED : AMBER),
                  r.variance.ok ? 'rgba(27,127,59,.10)' : (r.variance.mins > 0 ? 'rgba(179,63,64,.10)' : 'rgba(183,121,31,.11)')) +
        (r.variance.ok ? '' : `<div style="font-size:10px;color:${MID};margin-top:3px;">${gap(r.variance.mins)} ${r.variance.mins > 0 ? 'late' : 'early'}</div>`);
    }
    if (r.state === 'Scheduled') return pill('Scheduled', '#1D5FA8', 'rgba(29,95,168,.10)');
    return pill('Not scheduled', AMBER, 'rgba(183,121,31,.11)');
  }

  // ── The panel ──
  function shell() {
    const ov = document.createElement('div');
    ov.id = 'lm-hist';
    ov.style.cssText = `position:fixed;inset:0;z-index:9500;background:#F7F8FA;overflow-y:auto;`;
    ov.innerHTML = `
      <div style="position:sticky;top:0;z-index:2;background:#fff;border-bottom:.5px solid rgba(0,0,0,.08);
           padding:14px 22px;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;">
        <div>
          <div style="font-size:15px;font-weight:700;color:${DARK};letter-spacing:-.01em;">Last mile &mdash; last ${WEEKS} weeks</div>
          <div style="font-size:11.5px;color:${MID};margin-top:2px;">
            Every container, when it was booked in, when it landed, and how that compares.
            On time means within ${BUFFER_MIN} minutes of the booked slot.</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
          <button id="lm-csv" style="background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.14);border-radius:9px;
            padding:9px 15px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;">Download CSV</button>
          <button id="lm-close" style="background:${DARK};color:#fff;border:0;border-radius:9px;
            padding:9px 16px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;">Close</button>
        </div>
      </div>
      <div id="lm-body" style="padding:18px 22px 40px;max-width:1500px;">
        <div style="font-size:12px;color:${MID};">Reading the last ${WEEKS} weeks&hellip;</div>
      </div>`;
    document.body.appendChild(ov);
    document.body.style.overflow = 'hidden';
    el('lm-close').onclick = () => { ov.remove(); document.body.style.overflow = ''; };
    const onKey = (e) => { if (e.key === 'Escape') { ov.remove(); document.body.style.overflow = ''; document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);
    return ov;
  }

  function summary(all) {
    const delivered = all.filter(r => r.state === 'Delivered');
    const measured = delivered.filter(r => r.variance);
    const onTime = measured.filter(r => r.variance.ok);
    const late = measured.filter(r => !r.variance.ok && r.variance.mins > 0);
    const notScheduled = all.filter(r => r.state === 'Not scheduled');
    const noPod = delivered.filter(r => !r.pod);
    const worst = late.slice().sort((a, b) => b.variance.mins - a.variance.mins)[0];

    const card = (label, value, note, ink) => `
      <div style="background:#fff;border:.5px solid rgba(0,0,0,.08);border-radius:12px;padding:12px 15px;">
        <div style="font-size:9.5px;color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;">${esc(label)}</div>
        <div style="font-size:22px;font-weight:700;color:${ink || DARK};line-height:1.2;font-family:ui-monospace,monospace;">${esc(String(value))}</div>
        <div style="font-size:10.5px;color:${MID};">${esc(note)}</div>
      </div>`;

    return `
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:18px;">
        ${card('Containers', all.length, `${delivered.length} delivered`)}
        ${card('On time', measured.length ? Math.round(onTime.length / measured.length * 100) + '%' : '—',
               measured.length ? `${onTime.length} of ${measured.length} measured` : 'no booked times yet',
               measured.length && onTime.length / measured.length < 0.8 ? RED : GREEN)}
        ${card('Late', late.length, worst ? `worst ${gap(worst.variance.mins)} — ${worst.container}` : 'none', late.length ? RED : DARK)}
        ${card('Not scheduled', notScheduled.length, notScheduled.length ? 'no slot booked' : 'all booked in', notScheduled.length ? AMBER : DARK)}
        ${card('Delivered, no POD', noPod.length, noPod.length ? 'chase the paperwork' : 'all signed off', noPod.length ? AMBER : DARK)}
      </div>
      ${measured.length < delivered.length ? `
        <div style="background:rgba(183,121,31,.08);border:.5px solid rgba(183,121,31,.25);border-radius:10px;
             padding:10px 14px;margin-bottom:16px;font-size:11.5px;color:${DARK};">
          <b>${delivered.length - measured.length} of ${delivered.length} deliveries cannot be measured.</b>
          They were scheduled before a delivery time was captured, so there is only a record of when
          someone pressed the button. They are shown as delivered, not as on time.
        </div>` : ''}`;
  }

  function table(rows) {
    if (!rows.length) return `<div style="font-size:12px;color:${MID};padding:14px 0;">Nothing in this week.</div>`;
    const cell = 'padding:9px 10px;font-size:11.5px;color:' + DARK + ';vertical-align:top;';
    return `
      <div style="background:#fff;border:.5px solid rgba(0,0,0,.08);border-radius:12px;overflow:hidden;">
        <table style="width:100%;border-collapse:collapse;">
          <thead><tr style="background:#FBFBFC;">
            ${['Container', 'Vessel / flight', 'Booked for', 'Delivered', 'How it went', 'POD', 'Note']
              .map((h, i) => `<th style="text-align:${i === 5 ? 'center' : 'left'};padding:9px 10px;font-size:9.5px;
                   color:${LIGHT};text-transform:uppercase;letter-spacing:.06em;font-weight:700;">${h}</th>`).join('')}
          </tr></thead>
          <tbody>
            ${rows.map(r => `
              <tr style="border-top:.5px solid rgba(0,0,0,.05);">
                <td style="${cell}font-family:ui-monospace,monospace;">${esc(r.container)}${r.size ? `<span style="color:${LIGHT};"> · ${esc(r.size)}ft</span>` : ''}</td>
                <td style="${cell}color:${MID};">${esc(r.vessel)}</td>
                <td style="${cell}">
                  ${r.bookedFor ? esc(fmtWhen(r.bookedFor))
                    : (r.legacy ? `<span style="color:${LIGHT};">not captured</span>
                         <div style="font-size:10px;color:${LIGHT};">entered ${esc(fmtWhen(r.enteredAt))}</div>`
                       : `<span style="color:${LIGHT};">—</span>`)}
                </td>
                <td style="${cell}">${r.delivered ? esc(fmtWhen(r.delivered)) : `<span style="color:${LIGHT};">—</span>`}</td>
                <td style="${cell}">${stateCell(r)}</td>
                <td style="${cell}text-align:center;">${r.delivered ? (r.pod ? '✓' : `<span style="color:${AMBER};">missing</span>`) : `<span style="color:${LIGHT};">—</span>`}</td>
                <td style="${cell}color:${MID};max-width:260px;">${esc(r.note)}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>`;
  }

  // What is booked in for the days ahead, so the dock knows what is coming.
  function comingUp(all) {
    const now = Date.now();
    const soon = all.filter(r => r.state === 'Scheduled' && r.bookedFor && ms(r.bookedFor) >= now - 12 * 3600000)
                    .sort((a, b) => ms(a.bookedFor) - ms(b.bookedFor));
    if (!soon.length) return '';
    const byDay = new Map();
    for (const r of soon) {
      const k = new Date(ms(r.bookedFor)).toDateString();
      if (!byDay.has(k)) byDay.set(k, []);
      byDay.get(k).push(r);
    }
    return `
      <div style="margin-bottom:22px;">
        <div style="font-size:13px;font-weight:600;color:${DARK};margin-bottom:8px;">Booked in</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;">
          ${[...byDay.entries()].map(([day, list]) => `
            <div style="background:#fff;border:.5px solid rgba(0,0,0,.08);border-radius:12px;padding:12px 14px;">
              <div style="font-size:11.5px;font-weight:600;color:${DARK};">${esc(new Date(day).toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' }))}</div>
              <div style="font-size:10px;color:${LIGHT};margin-bottom:7px;">${list.length} container${list.length === 1 ? '' : 's'}</div>
              ${list.map(r => `
                <div style="display:flex;justify-content:space-between;gap:8px;padding:4px 0;border-top:.5px solid rgba(0,0,0,.05);">
                  <span style="font-size:11px;font-family:ui-monospace,monospace;color:${DARK};">${esc(r.container)}</span>
                  <span style="font-size:11px;color:${MID};">${esc(new Date(ms(r.bookedFor)).toLocaleTimeString('en-AU', { hour: 'numeric', minute: '2-digit', hour12: true }))}</span>
                </div>`).join('')}
            </div>`).join('')}
        </div>
      </div>`;
  }

  function csv(all) {
    const head = ['Week', 'Week starting', 'Container', 'Vessel or flight', 'Booked for', 'Entered at', 'Delivered at', 'Minutes vs booked', 'Outcome', 'POD', 'Note'];
    const q = (v) => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
    const lines = [head.map(q).join(',')];
    for (const r of all) {
      lines.push([
        'W' + isoWeek(r.ws), r.ws, r.container, r.vessel,
        r.bookedFor || '', r.enteredAt || '', r.delivered || '',
        r.variance ? r.variance.mins : '',
        r.state === 'Delivered' ? (r.variance ? (r.variance.ok ? 'On time' : (r.variance.mins > 0 ? 'Late' : 'Early')) : 'Delivered, not measurable') : r.state,
        r.delivered ? (r.pod ? 'Yes' : 'No') : '',
        r.note,
      ].map(q).join(','));
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'last-mile-' + new Date().toISOString().slice(0, 10) + '.csv';
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 0);
  }

  async function open() {
    const A = api();
    shell();
    const body = el('lm-body');
    if (!A || typeof A.computeWeek !== 'function') {
      body.innerHTML = `<div style="font-size:12px;color:${BRAND};">The dashboard has not finished loading. Close this, wait a moment, and try again.</div>`;
      return;
    }

    // The same ten weeks the history report walks, and the same way of finding the first one.
    // toMonday parses `${ws}T00:00:00Z`, so it needs a YYYY-MM-DD string: handing it a Date
    // returns the Date untouched and every week after that is nonsense.
    const pick = (window._reportsWeek || (window.state && window.state.weekStart) || new Date().toISOString().slice(0, 10));
    const start = A.toMonday ? A.toMonday(pick) : pick;
    const list = [];
    for (let i = 0; i < WEEKS; i++) list.push(A.shiftWeek ? A.shiftWeek(start, -i) : start);

    const weeks = [];
    for (let i = 0; i < list.length; i += PARALLEL) {
      const batch = await Promise.all(list.slice(i, i + PARALLEL).map(async w => {
        try { return await A.computeWeek(w); } catch (e) { console.warn('[last-mile]', w, e); return null; }
      }));
      weeks.push(...batch.filter(Boolean));
    }

    const all = [];
    for (const wk of weeks) all.push(...rowsFor(wk));

    if (!all.length) {
      body.innerHTML = `
        <div style="background:#fff;border:.5px solid rgba(0,0,0,.08);border-radius:12px;padding:16px 18px;">
          <div style="font-size:13px;font-weight:600;color:${DARK};">No containers recorded in these weeks</div>
          <div style="font-size:11.5px;color:${MID};margin-top:6px;line-height:1.5;">
            Read ${list.length} weeks, ${esc(list[list.length - 1])} to ${esc(list[0])}.
            Containers appear here once they are on an international lane for the week.</div>
          <div style="font-size:10.5px;color:${LIGHT};margin-top:8px;font-family:ui-monospace,monospace;">
            ${list.map(x => esc(x)).join(' · ')}</div>
        </div>`;
      return;
    }

    const notScheduled = all.filter(r => r.state === 'Not scheduled');
    body.innerHTML = `
      ${summary(all)}
      ${comingUp(all)}
      ${notScheduled.length ? `
        <div style="margin-bottom:22px;">
          <div style="font-size:13px;font-weight:600;color:${DARK};margin-bottom:3px;">Waiting on a slot</div>
          <div style="font-size:11px;color:${MID};margin-bottom:8px;">Recorded against a week, with no delivery booked.</div>
          ${table(notScheduled)}
        </div>` : ''}
      ${weeks.map(wk => {
        const rows = rowsFor(wk);
        if (!rows.length) return '';
        const late = rows.filter(r => r.variance && !r.variance.ok && r.variance.mins > 0).length;
        return `
          <div style="margin-bottom:20px;">
            <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:8px;">
              <span style="font-size:12.5px;font-weight:700;color:${DARK};font-family:ui-monospace,monospace;">W${isoWeek(wk.ws)}</span>
              <span style="font-size:11.5px;color:${MID};">week of ${esc(fmtDay(wk.ws))}</span>
              <span style="flex:1;height:.5px;background:rgba(0,0,0,.08);"></span>
              <span style="font-size:11px;color:${late ? RED : LIGHT};">${rows.length} container${rows.length === 1 ? '' : 's'}${late ? ` · ${late} late` : ''}</span>
            </div>
            ${table(rows)}
          </div>`;
      }).join('')}`;

    el('lm-csv').onclick = () => csv(all);
  }

  // ── Button on Reports & Downloads, beside the others ──
  function inject() {
    const anchor = el('btn-consolidated-download');
    if (!anchor || el('lm-hist-btn')) return;
    const b = document.createElement('button');
    b.id = 'lm-hist-btn'; b.type = 'button';
    b.style.cssText = `display:flex;align-items:center;gap:8px;background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.14);border-radius:9px;padding:10px 16px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;margin-right:8px;`;
    b.innerHTML = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="${BRAND}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 3h15v13H1z"/><path d="M16 8h4l3 3v5h-7z"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>Last mile`;
    b.onclick = open;
    anchor.parentNode.insertBefore(b, anchor);
  }
  const mo = new MutationObserver(() => inject());
  const startObserving = () => { inject(); mo.observe(document.body, { childList: true, subtree: true }); };
  if (document.body) startObserving(); else document.addEventListener('DOMContentLoaded', startObserving);

  window.__openLastMileHistory = open;
  console.log('[last-mile] v2 loaded');
})();
