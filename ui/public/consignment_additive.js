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
  const _expanded = {};        // which tiles are showing their full journey
  const _open = {};            // which stage each tile has open

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

      /* Tiles: one per movement, wide enough to read, narrow enough that a week fits on a
         screen without scrolling. */
      .cg-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:12px;margin-top:12px;}
      .cg-tile{background:#fff;border:.5px solid rgba(0,0,0,.09);border-radius:12px;padding:14px 15px;
        display:flex;flex-direction:column;gap:10px;transition:border-color .2s,box-shadow .2s;}
      .cg-tile:hover{box-shadow:0 2px 10px rgba(16,18,27,.07);}
      /* Only lateness gets a border. If every state had one, none of them would read. */
      .cg-tile.is-late{border-color:rgba(153,0,51,.35);}
      .cg-thead{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;}
      .cg-tmeta{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:3px;
        font-size:10.5px;color:${MID};}
      .cg-ell{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:130px;}

      /* The spine: six dots down the right, newest state at a glance. */
      .cg-spine{display:flex;flex-direction:column;gap:5px;align-items:center;flex-shrink:0;padding-top:2px;}
      .cg-sdot{width:9px;height:9px;border-radius:50%;border:1.5px solid;padding:0;cursor:pointer;
        transition:transform .15s,box-shadow .2s;}
      .cg-sdot:hover{transform:scale(1.35);}

      .cg-tbody{min-height:62px;display:flex;flex-direction:column;gap:7px;align-items:flex-start;}
      .cg-tnext{display:flex;flex-direction:column;gap:1px;}
      .cg-tnlabel{font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .cg-tnstage{font-size:15px;font-weight:600;letter-spacing:-.01em;}
      .cg-tndate{font-family:ui-monospace,monospace;font-size:12px;color:${MID};}
      .cg-tconfirm{padding:6px 12px;font-size:11.5px;}

      .cg-tfoot{display:flex;align-items:center;gap:8px;font-size:10px;color:${LIGHT};}
      .cg-bar{flex:1;height:3px;background:#EFF1F4;border-radius:2px;overflow:hidden;}
      .cg-barfill{display:block;height:3px;background:${OK};border-radius:2px;transition:width .4s;}
      .cg-track{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
        border-radius:5px;padding:2px 7px;border:0;cursor:default;font-family:inherit;}
      button.cg-track{cursor:pointer;}
      .cg-alert{display:flex;align-items:flex-start;gap:12px;padding:10px 0;
        border-top:.5px solid rgba(0,0,0,.05);}
      .cg-aheadline{font-size:12.5px;font-weight:600;color:${DARK};line-height:1.35;}
      .cg-adetail{font-size:10.5px;color:${MID};margin-top:2px;}
      .cg-aaction{font-size:11px;margin-top:3px;}
      .cg-empty{display:flex;flex-direction:column;gap:9px;align-items:flex-start;padding:10px 0 2px;}
      .cg-report{background:#FAFAFB;border:.5px solid rgba(0,0,0,.08);border-radius:11px;
        padding:14px 16px;margin-top:12px;display:flex;flex-direction:column;gap:10px;}
      .cg-mlist{display:flex;flex-direction:column;gap:6px;}
      .cg-mrow{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;}
      .cg-warn{font-size:11px;color:${DARK};background:rgba(184,134,11,.10);border-radius:8px;
        padding:8px 10px;line-height:1.5;}
      .cg-eta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
      .cg-etad{font-family:ui-monospace,monospace;font-size:13px;font-weight:600;color:${DARK};}
      .cg-conf{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
        border-radius:5px;padding:2px 7px;cursor:default;}
      .cg-duebox{display:flex;flex-direction:column;gap:5px;align-items:flex-start;
        background:rgba(153,0,51,.04);border-radius:9px;padding:9px 11px;width:100%;}
      .cg-dues{font-size:12px;color:${DARK};}
      .cg-lock{font-size:11px;color:${MID};background:#FAFAFB;border:.5px dashed rgba(0,0,0,.14);
        border-radius:9px;padding:10px 11px;line-height:1.5;width:100%;}
      .cg-lock .cg-btn{margin-top:7px;}
      .cg-tquote{display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:10.5px;color:${MID};
        border-top:.5px solid rgba(0,0,0,.05);padding-top:9px;}

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
      /* Quieter than 'amended' deliberately: a note on provenance, not a thing to act on. */
      .cg-nauto{font-size:8.5px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;}
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
    if (!res.ok) {
      throw new Error((json && (json.message || json.error)) || text.slice(0, 200));
    }
    return json;
  }
  // ── A consignment reads as a journey, not a spreadsheet row ──
  // Six date inputs across six columns is the thing we are trying to leave behind. A rail with
  // a node per stage says the same in a glance: how far along, what is next, what is late.
  // ── One tile per movement ──
  // Four to six movements a week is exactly the count where tiles beat rows: each one is a
  // thing you act on, not a record you scan. The tile gives most of its space to the only
  // question that matters day to day — what is next, and did it happen.
  //
  // Colour is reserved for lateness. Six stages in three colours across five tiles is
  // eighteen states competing for attention, and colour that means everything means nothing.
  // Red here always means "this needs you".

  const stageState = (ms) => {
    if (ms.state === 'amended') return 'amended';
    // Recorded by the sea rhythm rather than by a person. Same ink, hollow dot: the stage
    // genuinely is recorded, but a filled dot would say somebody looked and nobody did.
    if (ms.auto && ms.state === 'confirmed') return 'auto';
    if (ms.state !== 'assumed') return 'done';
    if (ms.planned_at && ms.planned_at <= today()) return 'due';
    return 'future';
  };
  const STATE_INK = { done: OK, auto: OK, amended: WARN, due: LATE, future: '#C7C7CC' };
  const HOLLOW    = { auto: 1, future: 1 };     // drawn as an outline, not a filled dot
  const RECORDED  = { done: 1, auto: 1, amended: 1 };  // has an actual_at worth showing

  // Where it has actually got to. The last stage recorded, not the next one pending: a tile
  // should say what is true before it says what is expected.
  function lastAchieved(c) {
    let last = null;
    for (const ms of c.milestones) if (ms.state !== 'assumed') last = ms;
    return last;
  }
  function firstDue(c) {
    return c.milestones.find(ms => stageState(ms) === 'due') || null;
  }

  // The vertical column, top right. Same dot vocabulary as the expanded view, so the two
  // never need learning twice.
  function spine(c, activeStage) {
    return `
      <div class="cg-spine">
        ${c.milestones.map(ms => {
          const st = stageState(ms);
          const ink = STATE_INK[st];
          const is = ms.stage === activeStage;
          return `
            <button class="cg-sdot" data-open="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}"
              title="${esc(STAGE_LABEL[ms.stage])} — ${esc(day(RECORDED[st] ? ms.actual_at : ms.planned_at))}${st === 'auto' ? ' · on the rhythm, not observed' : ''}"
              style="background:${HOLLOW[st] ? '#fff' : ink};border-color:${ink};
                     ${is ? 'transform:scale(1.35);' : ''}
                     ${st === 'due' ? 'box-shadow:0 0 0 3px rgba(153,0,51,.14);' : ''}"></button>`;
        }).join('')}
      </div>`;
  }

  const CONF = {
    'on plan':    { ink: OK,    bg: 'rgba(95,107,13,.12)' },
    'ahead':      { ink: OK,    bg: 'rgba(95,107,13,.12)' },
    'slipping':   { ink: WARN,  bg: 'rgba(184,134,11,.14)' },
    'at risk':    { ink: LATE,  bg: 'rgba(153,0,51,.10)' },
    'unverified': { ink: LIGHT, bg: '#F2F2F5' },
    'unknown':    { ink: LIGHT, bg: '#F2F2F5' },
  };

  // Terminal49 covers ocean only, and only once there is something to track with. The chip
  // says which of those is true rather than leaving a silent gap.
  function trackingChip(c) {
    if (c.mode === 'Air') return `<span class="cg-dim" title="Terminal49 tracks ocean freight">air · manual</span>`;
    const t = _tracking[c.consignment_uid];
    if (t && t.state === 'tracking') {
      // Catch up is offered even while tracking: a box subscribed mid-voyage has history the
      // feed will never send, and this is the only way to get it.
      return `<span class="cg-track" style="background:rgba(27,127,59,.12);color:${OK};">tracking</span>
        <button class="cg-tick" data-backfill="${esc(c.consignment_uid)}"
          title="Fetch what Terminal49 already knows about this shipment">catch up</button>`;
    }
    if (t && t.state === 'requested') {
      return `<span class="cg-track" style="background:#F2F2F5;color:${MID};"
          title="Terminal49 is looking for this shipment">requested</span>
        <button class="cg-tick" data-backfill="${esc(c.consignment_uid)}"
          title="Fetch what Terminal49 already knows about this shipment">catch up</button>`;
    }
    if (t && t.state === 'failed') {
      return `<button class="cg-track" style="background:rgba(153,0,51,.10);color:${LATE};"
        data-track="${esc(c.consignment_uid)}" title="${esc(t.failed_reason || 'The carrier could not find it')}">retry tracking</button>`;
    }
    if (!c.mbl) {
      return `<span class="cg-track" style="background:rgba(184,134,11,.14);color:${WARN};"
        title="Terminal49 needs the MBL to track this">MBL needed</span>`;
    }
    return `<button class="cg-track" style="background:#fff;border:.5px solid rgba(0,0,0,.16);color:${DARK};"
      data-track="${esc(c.consignment_uid)}">track it</button>`;
  }

  function tile(c) {
    const t = c.transit || {};
    const conf = c.confidence || { level: 'unknown', why: '' };
    const cf = CONF[conf.level] || CONF.unknown;
    const last = lastAchieved(c);
    const due = firstDue(c);
    const done = c.milestones.filter(ms => ms.state !== 'assumed').length;
    const pct = Math.round((done / c.milestones.length) * 100);
    const locked = !c.transit_confirmed;

    return `
      <div class="cg-tile ${due ? 'is-late' : ''}" data-row="${esc(c.consignment_uid)}">
        <div class="cg-thead">
          <div style="min-width:0;">
            <div class="cg-ref">${esc(c.reference || 'not advised')}</div>
            <div class="cg-tmeta">
              <span class="cg-tag" style="background:${c.mode === 'Air' ? 'rgba(30,155,215,.12)' : 'rgba(153,0,51,.10)'};
                    color:${c.mode === 'Air' ? '#15618F' : BRAND};">${esc(c.mode)}</span>
              ${c.size_ft ? `<span>${esc(c.size_ft)}</span>` : ''}
              ${c.vessel ? `<span class="cg-ell">${esc(c.vessel)}</span>` : ''}
            </div>
          </div>
          ${spine(c, _open[c.consignment_uid] || null)}
        </div>

        <div class="cg-tbody">
          <div class="cg-tnext">
            <span class="cg-tnlabel">Current status</span>
            <span class="cg-tnstage" style="color:${last ? DARK : LIGHT};">
              ${last ? esc(STAGE_LABEL[last.stage]) : 'Nothing recorded yet'}</span>
            <span class="cg-tndate">${last ? esc(day(last.actual_at)) : 'no milestone achieved'}</span>
          </div>

          ${/* The date everyone downstream is planning around, and how much it has moved. */ ''}
          <div class="cg-eta">
            <span class="cg-tnlabel">ETA FC</span>
            <span class="cg-etad">${esc(day(c.eta_fc))}</span>
            <span class="cg-conf" style="background:${cf.bg};color:${cf.ink};"
                  title="${esc(conf.why || '')}">${esc(conf.level)}${
                    conf.drift ? ` ${conf.drift > 0 ? '+' : ''}${conf.drift}d` : ''}</span>
          </div>

          ${locked
            ? `<div class="cg-lock">Dates use a default transit, not the carrier&rsquo;s quote.
                 <button class="cg-btn cg-tconfirm" data-edit="${esc(c.consignment_uid)}">Add details</button></div>`
            : due
              ? `<div class="cg-duebox">
                   <span class="cg-tnlabel" style="color:${LATE};">Awaiting confirmation</span>
                   <span class="cg-dues">${esc(STAGE_LABEL[due.stage])} &middot; planned ${esc(day(due.planned_at))}</span>
                   <span class="cg-act">
                     <button class="cg-btn cg-tconfirm" data-ok="${esc(c.consignment_uid)}"
                             data-stage="${esc(due.stage)}">Happened on plan</button>
                     <button class="cg-tick" data-open="${esc(c.consignment_uid)}"
                             data-stage="${esc(due.stage)}">different date</button>
                   </span>
                 </div>`
              : `<div class="cg-sub">Nothing due. ${done === c.milestones.length ? 'All six recorded.' : ''}</div>`}
        </div>

        <div class="cg-tfoot">
          <span>${done}/${c.milestones.length} recorded</span>
          <span class="cg-bar"><span class="cg-barfill" style="width:${pct}%;"></span></span>
          <span>${c.lanes.length} lane${c.lanes.length === 1 ? '' : 's'}</span>
        </div>

        <div class="cg-tquote">
          ${locked
            ? `<span style="color:${LATE};">transit not confirmed${t.quoted != null ? ` — using ${t.quoted}d` : ''}</span>`
            : `quoted ${t.quoted}d${t.achieved != null ? ` · achieved ${t.achieved}d` : ''}${
                t.variance == null ? '' : ` · <b style="color:${t.variance > 0 ? LATE : OK};">${t.variance > 0 ? '+' : ''}${t.variance}d</b>`}`}
          ${c.needs.filter(x => x !== 'transit_days').length
            ? `<span style="color:${LATE};"> · needs ${c.needs.filter(x => x !== 'transit_days').map(esc).join(', ')}</span>` : ''}
          <span style="flex:1;"></span>
          ${trackingChip(c)}
          <button class="cg-tick" data-edit="${esc(c.consignment_uid)}">details</button>
          <button class="cg-tick" data-expand="${esc(c.consignment_uid)}">${_expanded[c.consignment_uid] ? 'hide' : 'history'}</button>
        </div>

        ${_expanded[c.consignment_uid] ? rail(c) : ''}
        <div class="cg-popslot" data-slot="${esc(c.consignment_uid)}"></div>
      </div>`;
  }

  // The full journey, for when someone asks what happened to this container. Same dots, laid
  // out along a line rather than down a column.
  function rail(c) {
    return `
      <div class="cg-rail">
        <div class="cg-line"></div>
        ${c.milestones.map(ms => {
          const st = stageState(ms);
          const ink = STATE_INK[st];
          const shown = RECORDED[st] ? ms.actual_at : ms.planned_at;
          return `
            <button class="cg-node" data-open="${esc(c.consignment_uid)}" data-stage="${esc(ms.stage)}">
              <span class="cg-dot" style="background:${HOLLOW[st] ? '#fff' : ink};border-color:${ink};
                    ${st === 'due' ? 'box-shadow:0 0 0 4px rgba(153,0,51,.12);' : ''}"></span>
              <span class="cg-nlabel" style="color:${st === 'future' ? LIGHT : DARK};">${esc(STAGE_LABEL[ms.stage])}</span>
              <span class="cg-ndate" style="color:${ink};">${esc(day(shown))}</span>
              ${st === 'due' ? '<span class="cg-ndue">due</span>' : ''}
              ${st === 'amended' ? '<span class="cg-namend">amended</span>' : ''}
              ${st === 'auto' ? '<span class="cg-nauto" title="Recorded from the sea rhythm. Nobody observed it — amend if it ran differently.">rhythm</span>' : ''}
            </button>`;
        }).join('')}
      </div>`;
  }

  function stagePopover(c, ms) {
    const st = stageState(ms);
    const done = !!RECORDED[st];
    return `
      <div class="cg-pop">
        <div class="cg-poph">${esc(STAGE_LABEL[ms.stage])} &middot; ${esc(c.reference || 'not advised')}</div>
        <div class="cg-pops">${
          st === 'auto'
            ? `Recorded as <b>${esc(day(ms.actual_at))}</b> from the sea rhythm — nobody observed it.
               Give the real day below if it ran differently.`
            : done
          ? `Recorded as <b>${esc(day(ms.actual_at))}</b> (${esc(ms.state)}). Planned ${esc(day(ms.planned_at))}.`
          : `Planned for <b>${esc(day(ms.planned_at))}</b>. Nothing recorded yet.`}</div>
        <div class="cg-popact">
          ${st === 'auto'
            // Amend only. There is no Undo here on purpose: clearing a rhythm stage returns
            // it to `assumed`, and the next sweep records it again — a button that undoes
            // nothing is worse than no button. Correcting the date is the real action.
            ? `<input class="cg-when" type="date" data-amend="${esc(c.consignment_uid)}"
                      data-stage="${esc(ms.stage)}" value="${esc(ms.actual_at || ms.planned_at || '')}"
                      title="Give the day it actually ran">
               <span class="cg-or">it ran on a different day</span>`
            : done
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
          ${c.mode === 'Air' ? '' : f('Carrier code (SCAC)', 'scac', c.scac, 'EGLV, MAEU, CMDU…')}
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

  // What the dry run found, in the order it matters: what would be created, then what would
  // be left behind. A lane no container claims would silently get no dates at all, so it is
  // named here rather than discovered three weeks later.
  function migrationReport(r) {
    const made = r.consignments || [];
    if (!made.length) {
      return `<div class="cg-report">
        <div class="cg-h">Nothing to bring across</div>
        <div class="cg-sub">No containers are assigned for this week. Assign them in Container
          Manager first.</div>
        <button class="cg-btn ghost" data-migrate-cancel="1">Close</button>
      </div>`;
    }
    return `<div class="cg-report">
      <div class="cg-h">${made.length} movement${made.length === 1 ? '' : 's'} would be created</div>
      <div class="cg-mlist">
        ${made.map(c => `<div class="cg-mrow">
          <span class="cg-ref" style="font-size:11.5px;">${esc(c.reference)}</span>
          <span class="cg-dim">${esc(c.mode)} &middot; ${c.lanes} lane${c.lanes === 1 ? '' : 's'}</span>
          <span class="cg-dim" style="color:${LATE};">transit defaults to ${c.transit_days_defaulted}d
            until a quote is entered</span>
        </div>`).join('')}
      </div>
      ${r.unassigned_lanes && r.unassigned_lanes.length ? `
        <div class="cg-warn">
          <b>${r.unassigned_lanes.length} lane${r.unassigned_lanes.length === 1 ? '' : 's'} on no container.</b>
          They will have no dates until assigned:
          ${r.unassigned_lanes.slice(0, 4).map(k => esc(String(k).split('||')[0])).join(', ')}${
            r.unassigned_lanes.length > 4 ? ` +${r.unassigned_lanes.length - 4} more` : ''}
        </div>` : ''}
      ${r.split_lanes && r.split_lanes.length ? `
        <div class="cg-warn">${r.split_lanes.length} lane${r.split_lanes.length === 1 ? '' : 's'}
          ride more than one consignment — allowed, but worth a look.</div>` : ''}
      <div class="cg-sub">${esc(r.note || '')}</div>
      <div style="display:flex;gap:8px;">
        <button class="cg-btn" data-migrate-go="1">Bring them across</button>
        <button class="cg-btn ghost" data-migrate-cancel="1">Cancel</button>
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
        ${alertCard()}
        <div class="cg-card">
          <div class="cg-head">
            <span class="cg-h">Consignments</span>
            <span class="cg-sub">${_data.length} movement${_data.length === 1 ? '' : 's'} &middot;
              dates belong to the movement, lanes inherit them</span>
          </div>
          ${_data.length ? `<div class="cg-grid">${_data.map(tile).join('')}</div>`
            : `<div class="cg-empty">
                 <div class="cg-sub">No consignments for this week. If containers are assigned in
                   Container Manager, bring them across — dates start unconfirmed either way.</div>
                 <button class="cg-btn" data-migrate="1">Check this week</button>
               </div>`}
          <div id="cg-admin"></div>
        </div>
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

        <div id="cg-editor-slot"></div>
      </div>`;

    wire(root);
  }


  function wire(root) {
    const act = async (fn) => {
      if (_busy) return;
      _busy = true;
      try { await fn(); await load(root); }
      catch (e) {
        const msg = String(e.message || e);
        alert(/details_required/.test(msg)
          ? 'Add the details first \u2014 the dates are still a guess without the carrier\u2019s quote.'
          : 'Could not save: ' + msg);
      }
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
      _open[uid] = already ? null : stage;
      if (!already) wire(root);
    });

    // ── Bring a week across ──
    const admin = root.querySelector('#cg-admin');
    const mig = root.querySelector('[data-migrate]');
    if (mig) mig.onclick = async () => {
      mig.disabled = true; mig.textContent = 'Checking…';
      try {
        const dry = await post('/consignments/migrate', { week_start: currentWeek(), dry_run: true });
        admin.innerHTML = migrationReport(dry);
        const go = admin.querySelector('[data-migrate-go]');
        if (go) go.onclick = async () => {
          go.disabled = true; go.textContent = 'Bringing across…';
          try {
            await post('/consignments/migrate', { week_start: currentWeek(), dry_run: false });
            await load(root);
          } catch (e) { alert('Could not migrate: ' + (e.message || e)); go.disabled = false; }
        };
        const cancel = admin.querySelector('[data-migrate-cancel]');
        if (cancel) cancel.onclick = () => { admin.innerHTML = ''; };
      } catch (e) {
        admin.innerHTML = `<div class="cg-sub" style="color:${LATE};">${esc(e.message || e)}</div>`;
      }
      mig.disabled = false; mig.textContent = 'Check this week';
    };

    root.querySelectorAll('[data-backfill]').forEach(b => b.onclick = () => act(async () => {
      const uid = b.getAttribute('data-backfill');
      b.textContent = 'fetching…';
      const r = await post('/t49/backfill', { consignment_uid: uid });
      const did = (r && r.applied || []).map(a => `${a.stage.replace(/_/g, ' ')} ${a.date}`);
      alert(did.length
        ? 'Recorded from Terminal49:\n\n' + did.join('\n')
          + (r.carrier_eta ? `\n\nTheir ETA: ${r.carrier_eta}` : '')
          + (r.pickup_lfd ? `\nLast free day: ${r.pickup_lfd}` : '')
        : (r && r.note) || 'Terminal49 has nothing recorded for this shipment yet.');
    }));

    root.querySelectorAll('[data-track]').forEach(b => b.onclick = () => act(async () => {
      const uid = b.getAttribute('data-track');
      b.textContent = 'asking…';
      try {
        const r = await post('/t49/subscribe', { consignment_uid: uid });
        if (r && r.note) alert(r.note);
      } catch (e) {
        // Terminal49's own words. An error that says only "refused" leaves somebody guessing
        // at a carrier code they could have typed in ten seconds.
        alert(String(e.message || e));
        throw e;
      }
    }));

    root.querySelectorAll('[data-ack]').forEach(b => b.onclick = () => act(() =>
      post('/alerts/ack', { consignment_uid: b.getAttribute('data-ack'),
                            kind: b.getAttribute('data-kind'),
                            signature: b.getAttribute('data-sig') })));

    root.querySelectorAll('[data-expand]').forEach(b => b.onclick = () => {
      const uid = b.getAttribute('data-expand');
      _expanded[uid] = !_expanded[uid];
      render(root);
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
        const saved = await post('/consignments', body);

        // Subscribing is the point of entering the MBL, so it happens here rather than
        // waiting for somebody to remember. Guarded: only ocean, only with an MBL, and only
        // when nothing is tracking yet — a correction to a tracked consignment must not
        // create a second tracking request.
        const sc = (saved && saved.consignment) || {};
        const already = _tracking[c.consignment_uid];
        if (sc.mode !== 'Air' && body.mbl && !already) {
          try { await post('/t49/subscribe', { consignment_uid: c.consignment_uid }); }
          catch (e) { console.warn('[consignments] could not subscribe:', e.message); }
        }
        slot.innerHTML = '';
      });
    });
  }

  const currentWeek = () => _week || window.state?.weekStart || window._reportsWeek || null;

  let _alerts = [];
  let _tracking = {};        // consignment_uid -> { state, failed_reason, container_number }

  function alertCard() {
    const open = _alerts.filter(a => !a.acknowledged_at && a.kind === 'fc_moved');
    if (!open.length) return '';
    return `
      <div class="cg-card" style="border-left:3px solid ${LATE};">
        <div class="cg-head">
          <span class="cg-h">Delivery dates have moved</span>
          <span class="cg-sub">${open.length} consignment${open.length === 1 ? '' : 's'} &middot;
            dock bookings may need changing</span>
        </div>
        ${open.map(a => `
          <div class="cg-alert">
            <div style="min-width:0;flex:1;">
              <div class="cg-aheadline">${esc(a.headline)}</div>
              <div class="cg-adetail">${esc(a.detail)}</div>
              <div class="cg-aaction" style="color:${a.severity === 'high' ? LATE : WARN};">${esc(a.action)}</div>
            </div>
            <button class="cg-tick" data-ack="${esc(a.consignment_uid)}"
                    data-kind="${esc(a.kind)}" data-sig="${esc(a.signature)}"
                    title="Silence this until the date moves again">Seen</button>
          </div>`).join('')}
      </div>`;
  }

  async function load(root) {
    try {
      const ws = currentWeek();
      const r = await call(`/consignments?week=${encodeURIComponent(ws || '')}`);
      _data = (r && r.consignments) || [];
      _week = ws;
      // Shared with the Control Tower exception strip, which would otherwise repeat the fetch.
      window.__cgConsignments = _data;
      window.__cgWeek = ws;

      // Alerts are a separate reading of the same data. A failure here must not take the
      // panel down with it — the tiles are useful on their own.
      try {
        const al = await call(`/alerts?week=${encodeURIComponent(ws || '')}`);
        _alerts = (al && al.alerts) || [];
      } catch (_) { _alerts = []; }
      window.__cgAlerts = _alerts;

      // Who is being tracked, so the tile can say so rather than leaving people to guess
      // whether the carrier feed is on for this box.
      try {
        const t = await call('/t49/status');
        _tracking = {};
        for (const sub of ((t && t.subscriptions) || [])) _tracking[sub.consignment_uid] = sub;
      } catch (_) { _tracking = {}; }
      render(root);
    } catch (e) {
      root.innerHTML = `<div class="cg-card"><div class="cg-h">Consignments unavailable</div>
        <div class="cg-sub">${esc(e.message || e)}</div></div>`;
    }
  }

  // Mounted beside the existing transit markup rather than replacing it, so the old screen
  // stays reachable until this has been used for a week or two.
  // Render into an element the caller owns. Nothing is appended to the page on its own.
  function renderInto(el, opts) {
    if (!el) return;
    styles();
    if (opts && opts.week) _week = opts.week;
    el.id = el.id || 'cg-root';
    el.dataset.cgOwned = '1';          // the caller placed this; the observer leaves it alone
    return load(el);
  }

  function mount() {
    styles();
    // One place decides where this belongs. mount() having its own lookup meant the observer
    // and the mount disagreed, and the panel silently never appeared.
    const host = findHost();
    if (!host) return;
    const existing = document.getElementById('cg-root');
    // A root left behind in a host that is no longer on the page is not a reason to skip:
    // that is exactly the case where the panel silently disappears.
    if (existing && existing.isConnected && host.contains(existing)) return;
    if (existing) existing.remove();
    const root = document.createElement('div');
    root.id = 'cg-root';
    root.style.marginBottom = '14px';

    // Inserted before the Transit & Clearing card rather than appended after it.
    const anchor = document.getElementById('flow-containers-btn');
    const card = anchor && (anchor.closest('.rounded-2xl') || anchor.closest('.rounded-xl'));
    if (card && card.parentElement === host) host.insertBefore(root, card);
    else host.insertBefore(root, host.firstChild);
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
    mount, renderInto,
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
    const named = document.getElementById('flow-transit-panel')
               || document.querySelector('[data-node="transit"]');
    if (named) return named;

    // No id on the section itself. The Manage Containers button sits inside it, so its card
    // is a reliable way in — and lands the panel beside the lanes it describes rather than
    // at the bottom of the Week Hub, which is where appending to #page-flow put it.
    const btn = document.getElementById('flow-containers-btn');
    if (!btn) return null;
    const card = btn.closest('.rounded-2xl') || btn.closest('.rounded-xl') || btn.parentElement;
    return (card && card.parentElement) || null;
  }

  function whenReady() {
    if (findHost()) mount();                                 // already rendered: mount now

    // And watch regardless. The Week Hub swaps its content in and out as the user moves
    // between sections, so a panel that mounted once is not a panel that stays mounted.
    if (window.__consignmentsObserver) return;
    const obs = new MutationObserver(() => {
      const root = document.getElementById('cg-root');
      const host = findHost();

      // The section is gone: so is the panel. Rendered-on-request hosts are exempt — those
      // belong to whoever asked for them and are theirs to clear.
      if (root && root.isConnected && !host && !root.dataset.cgOwned) {
        root.remove();
        return;
      }

      if (root && root.isConnected) {
        // Still here, but the section may have been rebuilt around it.
        if (host && !host.contains(root) && !root.dataset.cgOwned) { root.remove(); mount(); }
        return;
      }
      if (host) mount();
    });
    obs.observe(document.body, { childList: true, subtree: true });
    window.__consignmentsObserver = obs;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', whenReady);
  else whenReady();

  console.log('[consignments] panel v1 loaded — window.__consignments.mount()');
})();
