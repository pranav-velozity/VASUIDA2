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
      .cg-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap;}

      /* The worklist: one line per consignment, and the actions sit next to the content
         rather than pinned to the far edge with a corridor of white between. */
      .cg-wrow{display:flex;align-items:center;gap:10px;flex-wrap:wrap;
        padding:9px 0;border-top:.5px solid rgba(0,0,0,.05);}
      .cg-wstages{display:flex;gap:6px;flex-wrap:wrap;flex:1;min-width:0;}
      .cg-chip{display:inline-flex;align-items:baseline;gap:6px;border:.5px solid rgba(153,0,51,.25);
        background:rgba(153,0,51,.05);color:${DARK};border-radius:8px;padding:3px 9px;
        font-size:11px;font-family:inherit;cursor:pointer;}
      .cg-chip:hover{background:rgba(153,0,51,.10);}
      .cg-chipd{font-family:ui-monospace,monospace;font-size:10px;color:${MID};}
      .cg-wage{font-family:ui-monospace,monospace;font-size:11.5px;min-width:34px;text-align:right;}
      .cg-wall{padding:4px 10px;font-size:11px;}

      /* The rail: the journey, left to right. */
      .cg-item{padding:14px 0 6px;border-top:.5px solid rgba(0,0,0,.06);}
      .cg-itop{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:12px;}
      .cg-tag{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
        border-radius:5px;padding:2px 6px;}
      .cg-dim{font-size:10.5px;color:${MID};}
      .cg-need{font-size:10.5px;color:${LATE};}
      .cg-rail{display:flex;justify-content:space-between;align-items:flex-start;
        position:relative;padding:2px 0 4px;}
      .cg-line{position:absolute;left:6px;right:6px;top:5px;height:1.5px;background:#ECECEF;}
      .cg-node{position:relative;background:none;border:0;padding:0 4px;cursor:pointer;
        display:flex;flex-direction:column;align-items:center;gap:3px;flex:1;min-width:0;
        font-family:inherit;}
      .cg-dot{width:11px;height:11px;border-radius:50%;border:2px solid;display:block;
        position:relative;z-index:1;transition:box-shadow .2s;}
      .cg-nlabel{font-size:9.5px;text-transform:uppercase;letter-spacing:.04em;text-align:center;}
      .cg-ndate{font-family:ui-monospace,monospace;font-size:11px;}
      .cg-ndue{font-size:8.5px;font-weight:700;color:${LATE};text-transform:uppercase;letter-spacing:.05em;}
      .cg-namend{font-size:8.5px;font-weight:700;color:${WARN};text-transform:uppercase;letter-spacing:.05em;}
      .cg-node:hover .cg-dot{box-shadow:0 0 0 5px rgba(28,28,30,.07);}

      .cg-pop{margin-top:10px;background:#FAFAFB;border:.5px solid rgba(0,0,0,.08);
        border-radius:10px;padding:12px 14px;}
      .cg-poph{font-size:12px;font-weight:600;color:${DARK};}
      .cg-pops{font-size:11.5px;color:${MID};margin:3px 0 10px;}
      .cg-popact{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
      .cg-or{font-size:10.5px;color:${LIGHT};}

      @media (max-width:900px){
        .cg-rail{flex-wrap:wrap;gap:12px;} .cg-line{display:none;}
        .cg-node{flex:0 0 30%;}
      }
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
  // ── A consignment reads as a journey, not a spreadsheet row ──
  // Six date inputs across six columns is the thing we are trying to leave behind. A rail with
  // a node per stage says the same in a glance: how far along, what is next, what is late.
  function rail(c) {
    const n = c.milestones.length;
    return `
      <div class="cg-rail">
        <div class="cg-line"></div>
        ${c.milestones.map((ms, i) => {
          const done = ms.state !== 'assumed';
          const due = !done && ms.planned_at && ms.planned_at <= today();
          const amended = ms.state === 'amended';
          const ink = done ? (amended ? WARN : OK) : (due ? LATE : '#C7C7CC');
          const shown = done ? ms.actual_at : ms.planned_at;
          return `
            <button class="cg-node" data-open="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}"
                    title="${esc(STAGE_LABEL[ms.stage])} — ${esc(day(shown))}">
              <span class="cg-dot" style="background:${done ? ink : '#fff'};border-color:${ink};
                    ${due ? 'box-shadow:0 0 0 4px rgba(153,0,51,.12);' : ''}"></span>
              <span class="cg-nlabel" style="color:${done || due ? DARK : LIGHT};">${esc(STAGE_LABEL[ms.stage])}</span>
              <span class="cg-ndate" style="color:${ink};">${esc(day(shown))}</span>
              ${due ? '<span class="cg-ndue">due</span>' : ''}
              ${amended ? '<span class="cg-namend">amended</span>' : ''}
            </button>`;
        }).join('')}
      </div>`;
  }

  // Opened from a node rather than sitting on screen for every stage at once. Thirty date
  // inputs competing for attention when one or two need anything is how the old screen taught
  // people to stop looking.
  function stagePopover(c, ms) {
    const done = ms.state !== 'assumed';
    return `
      <div class="cg-pop">
        <div class="cg-poph">${esc(STAGE_LABEL[ms.stage])} &middot; ${esc(c.reference || 'not advised')}</div>
        <div class="cg-pops">${done
          ? `Recorded as <b>${esc(day(ms.actual_at))}</b> (${esc(ms.state)}). Planned ${esc(day(ms.planned_at))}.`
          : `Planned for <b>${esc(day(ms.planned_at))}</b>. Nothing recorded yet.`}</div>
        <div class="cg-popact">
          ${done
            ? `<button class="cg-btn ghost" data-clear="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}">Undo</button>`
            : `<button class="cg-btn" data-ok="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}"
                 ${ms.planned_at ? '' : 'disabled'}>Happened on plan</button>
               <span class="cg-or">or</span>
               <input class="cg-when" type="date" data-amend="${esc(c.consignment_uid)}"
                      data-stage="${esc(ms.stage)}" value="${esc(ms.planned_at || '')}">`}
          <span style="flex:1;"></span>
          <button class="cg-btn ghost" data-popclose="1">Close</button>
        </div>
      </div>`;
  }

  function consignmentRow(c) {
    const t = c.transit || {};
    // A defaulted transit is not a quote. Until the carrier's figure is in, every date after
    // departure is a guess, and the row says so rather than looking complete.
    const quoteUnset = c.needs.includes('transit_days') || c.transit_defaulted;
    const variance = t.variance == null ? '' :
      ` · <b style="color:${t.variance > 0 ? LATE : OK};">${t.variance > 0 ? '+' : ''}${t.variance}d vs quote</b>`;

    return `
      <div class="cg-item" data-row="${esc(c.consignment_uid)}">
        <div class="cg-itop">
          <span class="cg-ref">${esc(c.reference || 'not advised')}</span>
          <span class="cg-tag" style="background:${c.mode === 'Air' ? 'rgba(30,155,215,.12)' : 'rgba(153,0,51,.10)'};
                color:${c.mode === 'Air' ? '#15618F' : BRAND};">${esc(c.mode)}</span>
          ${c.size_ft ? `<span class="cg-dim">${esc(c.size_ft)}</span>` : ''}
          ${c.vessel ? `<span class="cg-dim">${esc(c.vessel)}</span>` : ''}
          <span class="cg-dim">${c.lanes.length} lane${c.lanes.length === 1 ? '' : 's'}</span>
          <span class="cg-dim">${quoteUnset
            ? `<span style="color:${LATE};">transit not confirmed${t.quoted != null ? ` (using ${t.quoted}d)` : ''}</span>`
            : `quoted ${t.quoted}d${t.achieved != null ? ` · achieved ${t.achieved}d` : ''}${variance}`}</span>
          <span style="flex:1;"></span>
          ${c.needs.filter(x => x !== 'transit_days').length
            ? `<span class="cg-need">needs ${c.needs.filter(x => x !== 'transit_days').map(esc).join(', ')}</span>` : ''}
          <button class="cg-tick" data-edit="${esc(c.consignment_uid)}">details</button>
        </div>
        ${rail(c)}
        <div class="cg-popslot" data-slot="${esc(c.consignment_uid)}"></div>
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
    // Grouped by consignment. Twenty-four separate rows is a list nobody reads; four rows
    // saying "this box needs three things" is a morning's work.
    const groups = [];
    for (const c of _data) {
      const dueStages = c.milestones.filter(ms =>
        ms.state === 'assumed' && ms.planned_at && ms.planned_at <= today());
      if (!dueStages.length) continue;
      const worst = Math.max(...dueStages.map(ms =>
        Math.round((new Date(today() + 'T00:00:00Z') - new Date(ms.planned_at + 'T00:00:00Z')) / 86400000)));
      groups.push({ c, dueStages, worst });
    }
    groups.sort((a, b) => b.worst - a.worst);
    const totalDue = groups.reduce((n, g) => n + g.dueStages.length, 0);

    root.innerHTML = `
      <div class="cg-wrap">
        <div class="cg-card">
          <div class="cg-head">
            <span class="cg-h">Needs an update</span>
            <span class="cg-sub">${totalDue
              ? `${totalDue} stage${totalDue === 1 ? '' : 's'} across ${groups.length} consignment${groups.length === 1 ? '' : 's'}`
              : 'Nothing is waiting on you'}</span>
          </div>
          ${groups.length ? groups.map(g => `
            <div class="cg-wrow">
              <span class="cg-ref" style="font-size:12px;">${esc(g.c.reference || 'not advised')}</span>
              <span class="cg-wstages">
                ${g.dueStages.map(ms => `
                  <button class="cg-chip" data-ok="${esc(g.c.consignment_uid)}" data-stage="${esc(ms.stage)}"
                          title="Confirm it happened on ${esc(day(ms.planned_at))}">
                    ${esc(STAGE_LABEL[ms.stage])}<span class="cg-chipd">${esc(day(ms.planned_at))}</span>
                  </button>`).join('')}
              </span>
              <span class="cg-wage" style="color:${g.worst >= 3 ? LATE : WARN};">${g.worst}d</span>
              <button class="cg-btn ghost cg-wall" data-allok="${esc(g.c.consignment_uid)}">all on plan</button>
            </div>`).join('')
            : `<div class="cg-sub" style="padding:8px 0 0;">Every stage that has come due has been confirmed or amended.</div>`}
        </div>

        <div class="cg-card">
          <div class="cg-head">
            <span class="cg-h">Consignments</span>
            <span class="cg-sub">${_data.length} movement${_data.length === 1 ? '' : 's'} &middot;
              dates belong to the movement, lanes inherit them</span>
          </div>
          ${_data.length ? _data.map(consignmentRow).join('')
            : `<div class="cg-sub" style="padding:10px 0 0;">No consignments for this week yet.
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

    // A node opens its stage rather than the row carrying six inputs at all times.
    root.querySelectorAll('[data-open]').forEach(b => b.onclick = () => {
      const uid = b.getAttribute('data-open'), stage = b.getAttribute('data-stage');
      const c = _data.find(x => x.consignment_uid === uid);
      const ms = c && c.milestones.find(m => m.stage === stage);
      if (!ms) return;
      root.querySelectorAll('.cg-popslot').forEach(el => { if (el.getAttribute('data-slot') !== uid) el.innerHTML = ''; });
      const slot = root.querySelector(`.cg-popslot[data-slot="${uid}"]`);
      if (!slot) return;
      const already = slot.getAttribute('data-stage') === stage && slot.innerHTML.trim();
      slot.innerHTML = already ? '' : stagePopover(c, ms);
      slot.setAttribute('data-stage', already ? '' : stage);
      if (!already) wire(root);
    });

    root.querySelectorAll('[data-popclose]').forEach(b => b.onclick = () => {
      const slot = b.closest('.cg-popslot');
      if (slot) { slot.innerHTML = ''; slot.setAttribute('data-stage', ''); }
    });

    // One action for a consignment whose stages all ran to plan, which is the common case and
    // was four separate clicks.
    root.querySelectorAll('[data-allok]').forEach(b => b.onclick = () => act(async () => {
      const uid = b.getAttribute('data-allok');
      const c = _data.find(x => x.consignment_uid === uid);
      if (!c) return;
      const due = c.milestones.filter(ms =>
        ms.state === 'assumed' && ms.planned_at && ms.planned_at <= today());
      for (const ms of due) {
        await post(`/consignments/${uid}/milestone`, { stage: ms.stage });
      }
    }));

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

  const currentWeek = () => _week || window.state?.weekStart || window._reportsWeek || null;

  async function load(root) {
    try {
      const ws = currentWeek();
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
    const existing = document.getElementById('cg-root');
    // A root left behind in a host that is no longer on the page is not a reason to skip:
    // that is exactly the case where the panel silently disappears.
    if (existing && existing.isConnected && host.contains(existing)) return;
    if (existing) existing.remove();
    const root = document.createElement('div');
    root.id = 'cg-root';
    root.style.marginTop = '14px';
    host.appendChild(root);
    load(root);
    watchWeek();
  }

  // The Week Hub changes weeks without a page load and emits no event, so the panel watches
  // for the value to change rather than being told. Cheap, and it means the panel can never
  // be showing one week's consignments under another week's heading.
  let _watching = null;
  function watchWeek() {
    if (_watching) clearInterval(_watching);
    let last = window.state?.weekStart || null;
    _watching = setInterval(() => {
      const now = window.state?.weekStart || null;
      if (now && now !== last) {
        last = now;
        _week = null;                       // follow the page again after a manual setWeek
        const r = document.getElementById('cg-root');
        if (r) load(r);
      }
    }, 1200);
    if (_watching.unref) _watching.unref();
  }

  window.__consignments = {
    mount,
    reload: () => { const r = document.getElementById('cg-root'); if (r) load(r); },
    setWeek: (ws) => {
      _week = ws;
      const r = document.getElementById('cg-root');
      if (!r) { console.warn('[consignments] not mounted yet — run window.__consignments.mount()'); return; }
      return load(r);                       // returns the promise, so `await` actually waits
    },
    week: () => currentWeek(),
  };

  // The transit section renders after its data arrives, so a fixed delay either fires too
  // early or waits longer than it needs to. Watch for it instead, and give up after a while
  // rather than polling forever on a page that will never have one.
  function findHost() {
    return document.getElementById('flow-transit-panel')
        || document.getElementById('page-flow')
        || document.querySelector('[data-node="transit"]');
  }

  function whenReady() {
    if (findHost()) mount();                                 // already rendered: mount now

    // And watch regardless. The Week Hub swaps its content in and out as the user moves
    // between sections, so a panel that mounted once is not a panel that stays mounted.
    if (window.__consignmentsObserver) return;
    const obs = new MutationObserver(() => {
      const root = document.getElementById('cg-root');
      if (root && root.isConnected) return;                  // still on the page, nothing to do
      if (findHost()) mount();
    });
    obs.observe(document.body, { childList: true, subtree: true });
    window.__consignmentsObserver = obs;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', whenReady);
  else whenReady();

  console.log('[consignments] panel v1 loaded — window.__consignments.mount()');
})();
