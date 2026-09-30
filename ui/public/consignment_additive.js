/* ── VelOzity Pinpoint — consignments panel v1 ──
   The Transit & Clearing screen stops being a grid of date fields and becomes a worklist.

   What it replaces: seven lanes each carrying seven date inputs, where the same departure is
   typed once per lane. Nobody does that honestly, and the record proves it — 157 departures
   with not one day of variance.

   What it is instead: one row per movement — a 20ft, a 40ft, two flights — with its six
   milestones. A stage that is due shows a tick to confirm and a field to amend. A stage
   nobody has touched stays visibly assumed rather than quietly becoming fact.

   Roughly 8 to 12 deliberate actions a week, against 72 fields today. On a quiet day the
   worklist is empty, and an empty worklist is the point: it means nothing needs you.

   Additive. It mounts itself onto the transit page and leaves the existing markup alone.
*/
(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#8E8E93';
  const LATE = '#990033', OK = '#5F6B0D', WARN = '#B8860B';

  const STAGE_LABEL = {
    packing_list_ready: 'Packing list',
    origin_cleared: 'Origin cleared',
    departed: 'Departed',
    arrived: 'Arrived',
    dest_cleared: 'Dest cleared',
    fc_receipt: 'FC receipt',
  };

  const esc = (v) => String(v == null ? '' : v)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  const day = (ymd) => {
    if (!ymd) return '—';
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(d)) return String(ymd);
    return d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  };

  const today = () => new Date().toISOString().slice(0, 10);

  let _week = null, _data = [], _busy = false;

  function styles() {
    if (document.getElementById('cg-css')) return;
    const el = document.createElement('style');
    el.id = 'cg-css';
    el.textContent = `
      .cg-wrap{font-family:inherit;}
      .cg-card{background:#fff;border:.5px solid rgba(0,0,0,.08);border-radius:14px;padding:18px 20px;
        margin-bottom:14px;box-shadow:0 1px 2px rgba(16,18,27,.04),0 4px 12px rgba(16,18,27,.05);}
      .cg-h{font-size:13px;font-weight:600;color:${DARK};}
      .cg-sub{font-size:10.5px;color:${LIGHT};margin-top:2px;line-height:1.5;}
      .cg-row{display:grid;grid-template-columns:168px repeat(6,minmax(0,1fr));gap:0 10px;
        align-items:start;padding:12px 0;border-top:.5px solid rgba(0,0,0,.06);}
      .cg-ref{font-family:ui-monospace,monospace;font-size:12.5px;color:${DARK};font-weight:600;}
      .cg-meta{font-size:10px;color:${LIGHT};margin-top:2px;line-height:1.45;}
      .cg-stage{min-width:0;}
      .cg-slabel{font-size:9px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;}
      .cg-date{font-family:ui-monospace,monospace;font-size:11.5px;margin-top:1px;}
      .cg-act{display:flex;gap:4px;margin-top:4px;align-items:center;}
      .cg-tick{border:.5px solid rgba(0,0,0,.18);background:#fff;border-radius:7px;
        padding:3px 8px;font-size:10.5px;cursor:pointer;font-family:inherit;color:${DARK};}
      .cg-tick:hover{background:#F5F5F7;}
      .cg-tick[disabled]{opacity:.4;cursor:default;}
      .cg-when{border:.5px solid rgba(0,0,0,.16);border-radius:7px;padding:2px 5px;
        font-size:10.5px;font-family:inherit;width:118px;}
      .cg-pill{display:inline-block;font-size:9px;border-radius:5px;padding:1px 5px;
        text-transform:uppercase;letter-spacing:.05em;font-weight:700;}
      .cg-work{display:grid;grid-template-columns:150px 1fr 96px 84px;gap:0 12px;
        padding:8px 0;border-top:.5px solid rgba(0,0,0,.05);font-size:11.5px;align-items:baseline;}
      .cg-btn{background:${DARK};color:#fff;border:0;border-radius:8px;padding:7px 14px;
        font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;}
      .cg-btn.ghost{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);font-weight:500;}
      .cg-field{border:.5px solid rgba(0,0,0,.16);border-radius:8px;padding:6px 9px;
        font-size:12px;font-family:inherit;width:100%;}
      @media (max-width:1100px){ .cg-row{grid-template-columns:1fr;} }
    `;
    document.head.appendChild(el);
  }

  async function call(path, opts) {
    const r = await window.api(path, opts);
    return r;
  }

  // window.api signatures vary across this codebase, so POSTs go through fetch directly.
  async function post(path, body) {
    const base = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
    let token = null;
    if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
    const res = await fetch(base + path, {
      method: 'POST',
      headers: Object.assign({ 'Content-Type': 'application/json' },
        token ? { Authorization: 'Bearer ' + token } : {}),
      body: JSON.stringify(body || {}),
    });
    const text = await res.text();
    let json = null; try { json = JSON.parse(text); } catch (_) {}
    if (!res.ok) throw new Error((json && json.error) || text.slice(0, 200));
    return json;
  }

  function stageCell(c, ms) {
    const due = ms.planned_at && ms.planned_at <= today();
    const confirmed = ms.state === 'confirmed' || ms.state === 'amended' || ms.state === 'carrier';

    // The distinction the whole model rests on: a planned date and a recorded one never look
    // the same, and an untouched stage is never dressed up as a fact.
    const ink = confirmed ? (ms.state === 'amended' ? WARN : OK) : (due ? LATE : MID);
    const shown = confirmed ? ms.actual_at : ms.planned_at;
    const badge = confirmed
      ? `<span class="cg-pill" style="background:${ms.state === 'amended' ? 'rgba(184,134,11,.12)' : 'rgba(95,107,13,.12)'};color:${ink};">${esc(ms.state)}</span>`
      : (due ? `<span class="cg-pill" style="background:rgba(153,0,51,.10);color:${LATE};">due</span>`
             : `<span class="cg-pill" style="background:#F2F2F5;color:${LIGHT};">planned</span>`);

    return `
      <div class="cg-stage">
        <div class="cg-slabel">${esc(STAGE_LABEL[ms.stage] || ms.stage)}</div>
        <div class="cg-date" style="color:${ink};">${esc(day(shown))}</div>
        <div style="margin-top:2px;">${badge}</div>
        <div class="cg-act">
          ${confirmed
            ? `<button class="cg-tick" data-clear="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}">undo</button>`
            : `<button class="cg-tick" data-ok="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}"
                 ${ms.planned_at ? '' : 'disabled'} title="Confirm it happened on the planned date">✓</button>
               <input class="cg-when" type="date" data-amend="${esc(c.consignment_uid)}"
                 data-stage="${esc(ms.stage)}" title="Or enter the date it actually happened">`}
        </div>
      </div>`;
  }

  function consignmentRow(c) {
    const t = c.transit || {};
    const variance = t.variance == null ? '' :
      `<span style="color:${t.variance > 0 ? LATE : OK};">${t.variance > 0 ? '+' : ''}${t.variance}d vs quote</span>`;
    return `
      <div class="cg-row">
        <div>
          <div class="cg-ref">${esc(c.reference || 'not advised')}</div>
          <div class="cg-meta">
            ${esc(c.mode)}${c.size_ft ? ' · ' + esc(c.size_ft) : ''}${c.vessel ? ' · ' + esc(c.vessel) : ''}<br>
            ${c.lanes.length} lane${c.lanes.length === 1 ? '' : 's'}
            ${t.quoted != null ? ' · quoted ' + t.quoted + 'd' : ''}
            ${t.achieved != null ? ' · achieved ' + t.achieved + 'd ' : ''}${variance}
          </div>
          ${c.split_lanes && c.split_lanes.length
            ? `<div class="cg-meta" style="color:${WARN};">a lane on this also rides another consignment</div>` : ''}
          ${c.needs.length
            ? `<div class="cg-meta" style="color:${LATE};">needs ${c.needs.map(esc).join(', ')}</div>` : ''}
          <button class="cg-tick" data-edit="${esc(c.consignment_uid)}" style="margin-top:5px;">details</button>
        </div>
        ${c.milestones.map(ms => stageCell(c, ms)).join('')}
      </div>`;
  }

  function editor(c) {
    const f = (label, key, val, ph) => `
      <label style="display:block;">
        <span class="cg-slabel">${esc(label)}</span>
        <input class="cg-field" data-f="${key}" value="${esc(val || '')}" placeholder="${esc(ph || '')}">
      </label>`;
    return `
      <div class="cg-card" id="cg-editor" data-uid="${esc(c.consignment_uid)}"
           style="border-left:3px solid ${BRAND};">
        <div class="cg-h">${esc(c.reference || 'not advised')}</div>
        <div class="cg-sub">Transit days are the carrier&rsquo;s quoted port-to-port for this sailing.
          They are compared against what the confirmed dates show, and never overwritten by an actual.</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:12px;">
          ${f('Container / AWB', 'reference', c.reference)}
          ${f('Vessel / flight', 'vessel', c.vessel)}
          ${f('Carrier', 'carrier', c.carrier)}
          ${f('Transit days (quoted)', 'transit_days', c.transit_days, 'e.g. 26')}
          ${c.mode === 'Air' ? f('Flight date', 'departure_planned', c.departure_planned, 'YYYY-MM-DD') : ''}
          ${f('Shipment #', 'shipment_ref', c.shipment_ref)}
          ${f('HBL', 'hbl', c.hbl)}
          ${f('MBL', 'mbl', c.mbl)}
        </div>
        <div style="display:flex;gap:8px;margin-top:12px;align-items:center;">
          <button class="cg-btn" data-save="1">Save</button>
          <button class="cg-btn ghost" data-close="1">Close</button>
          <span style="flex:1;"></span>
          <span class="cg-sub">HBL, MBL and shipment # belong to the movement, not to each lane —
            one number, entered once.</span>
        </div>
      </div>`;
  }

  function render(root) {
    const late = [];
    for (const c of _data) {
      for (const ms of c.milestones) {
        if (ms.state !== 'assumed' || !ms.planned_at || ms.planned_at > today()) continue;
        const over = Math.round((new Date(today() + 'T00:00:00Z') - new Date(ms.planned_at + 'T00:00:00Z')) / 86400000);
        late.push({ c, ms, over });
      }
    }
    late.sort((a, b) => b.over - a.over);

    root.innerHTML = `
      <div class="cg-wrap">
        <div class="cg-card">
          <div style="display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;">
            <span class="cg-h">Needs an update</span>
            <span class="cg-sub">${late.length ? late.length + ' stage' + (late.length === 1 ? '' : 's') + ' past their planned date with nobody confirming'
              : 'Nothing is waiting on you'}</span>
          </div>
          ${late.length ? late.slice(0, 12).map(x => `
            <div class="cg-work">
              <span class="cg-ref" style="font-size:11.5px;">${esc(x.c.reference || 'not advised')}</span>
              <span style="color:${MID};">${esc(STAGE_LABEL[x.ms.stage])} &middot; planned ${esc(day(x.ms.planned_at))}</span>
              <span style="color:${x.over >= 3 ? LATE : WARN};">${x.over}d ago</span>
              <span style="text-align:right;">
                <button class="cg-tick" data-ok="${esc(x.c.consignment_uid)}" data-stage="${esc(x.ms.stage)}">✓ on plan</button>
              </span>
            </div>`).join('')
            : `<div class="cg-sub" style="padding:10px 0 2px;">Every stage that has come due has been confirmed or amended.</div>`}
        </div>

        <div class="cg-card">
          <div style="display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;">
            <span class="cg-h">Consignments</span>
            <span class="cg-sub">${_data.length} movement${_data.length === 1 ? '' : 's'} this week &middot;
              dates belong to the movement, lanes inherit them</span>
          </div>
          ${_data.length ? _data.map(consignmentRow).join('')
            : `<div class="cg-sub" style="padding:12px 0;">No consignments for this week yet.
                 Assign containers in Container Manager, then migrate the week.</div>`}
        </div>
        <div id="cg-editor-slot"></div>
      </div>`;

    wire(root);
  }

  function wire(root) {
    const act = async (fn) => {
      if (_busy) return;
      _busy = true;
      try { await fn(); await load(root); }
      catch (e) { alert('Could not save: ' + (e.message || e)); }
      finally { _busy = false; }
    };

    root.querySelectorAll('[data-ok]').forEach(b => b.onclick = () => act(() =>
      post(`/consignments/${b.getAttribute('data-ok')}/milestone`, { stage: b.getAttribute('data-stage') })));

    root.querySelectorAll('[data-clear]').forEach(b => b.onclick = () => act(() =>
      post(`/consignments/${b.getAttribute('data-clear')}/milestone`,
        { stage: b.getAttribute('data-stage'), clear: true })));

    root.querySelectorAll('[data-amend]').forEach(inp => inp.onchange = () => {
      if (!inp.value) return;
      act(() => post(`/consignments/${inp.getAttribute('data-amend')}/milestone`,
        { stage: inp.getAttribute('data-stage'), actual_at: inp.value }));
    });

    root.querySelectorAll('[data-edit]').forEach(b => b.onclick = () => {
      const c = _data.find(x => x.consignment_uid === b.getAttribute('data-edit'));
      if (!c) return;
      const slot = root.querySelector('#cg-editor-slot');
      slot.innerHTML = editor(c);
      try { slot.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); } catch (_) {}

      slot.querySelector('[data-close]').onclick = () => { slot.innerHTML = ''; };
      slot.querySelector('[data-save]').onclick = () => act(async () => {
        const body = { consignment_uid: c.consignment_uid, week_start: c.week_start,
                       facility: c.facility, mode: c.mode, status: c.status };
        slot.querySelectorAll('[data-f]').forEach(i => {
          const k = i.getAttribute('data-f');
          body[k] = k === 'transit_days' ? (i.value === '' ? null : Number(i.value)) : i.value.trim();
        });
        await post('/consignments', body);
        slot.innerHTML = '';
      });
    });
  }

  async function load(root) {
    try {
      const ws = _week || window.state?.weekStart || window._reportsWeek;
      const r = await call(`/consignments?week=${encodeURIComponent(ws || '')}`);
      _data = (r && r.consignments) || [];
      _week = ws;
      render(root);
    } catch (e) {
      root.innerHTML = `<div class="cg-card"><div class="cg-h">Consignments unavailable</div>
        <div class="cg-sub">${esc(e.message || e)}</div></div>`;
    }
  }

  // Mounted beside the existing transit markup rather than replacing it, so the old screen
  // stays reachable until this has been used for a week or two.
  function mount() {
    styles();
    const host = document.getElementById('flow-transit-panel')
      || document.getElementById('page-flow')
      || document.querySelector('[data-node="transit"]')
      || document.querySelector('main');
    if (!host) return;
    if (document.getElementById('cg-root')) return;
    const root = document.createElement('div');
    root.id = 'cg-root';
    root.style.marginTop = '14px';
    host.appendChild(root);
    load(root);
  }

  window.__consignments = {
    mount,
    reload: () => { const r = document.getElementById('cg-root'); if (r) load(r); },
    setWeek: (ws) => { _week = ws; const r = document.getElementById('cg-root'); if (r) load(r); },
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else setTimeout(mount, 800);

  console.log('[consignments] panel v1 loaded — window.__consignments.mount()');
})();
