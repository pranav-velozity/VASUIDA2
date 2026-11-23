/* ── VelOzity Pinpoint — Pulse rail v2 ──
   Pulse moves from the bottom bar to a right-hand rail and speaks first: a briefing surface at
   the top (built from the Week Hub insights already fetched — no extra model call), four verb
   tiles for the page you are on, then the conversation, then the composer.

   Nothing functional is rebuilt. The existing #pulse-panel (chat, cards, tasks and meetings
   strips) and #pulse-bar (input, Pulse on/off, Spoken, voice pill and modes) are MOVED into the
   rail and restyled; every id, handler and module keeps working as before. The server switch
   PULSE_RAIL=off (read from GET /pulse/ui after sign-in) puts everything back in the bottom bar.

   Pinning: on Receiving Ops the ticked POs (and the supplier) are pinned; on Transit Movements
   the movement you open is. The pin is sent with every question as `pin`, so "these" and "it"
   mean what is on screen, and the verb tiles change to match.

   Collapsed: a 48 px strip with the orb, the listening dot and the task count; your choice is
   remembered. Under 1300 px the open rail floats over the page instead of narrowing it.

   Test hook: window._pulseRail { state, setCollapsed, setPin, refresh, page } */
;(function () {
  'use strict';
  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E', OK = '#1F7A4D', AMBER = '#B07000';
  const W = 380, WC = 48, OVERLAY_BELOW = 1300;

  const css = `
    :root{--pr-w:${W}px;--pr-top:52px}
    /* Pages that nest their own .vo-wrap (Receiving, others) size it to 96vw; inside the narrowed main they must follow the main instead. */
    body.pr-on .vo-wrap{width:auto!important;max-width:none!important;margin:0!important;}
    body.pr-on main.vo-wrap{padding-right:calc(24px + var(--pr-w))!important;box-sizing:border-box;}
    body.pr-on main.vo-wrap .vo-wrap{padding-left:0!important;padding-right:0!important;}
    body.pr-on.pr-overlay main.vo-wrap{padding-right:calc(24px + ${WC}px)!important;}
    #pr-rail{position:fixed;top:var(--pr-top);right:0;bottom:0;width:var(--pr-w);box-sizing:border-box;z-index:45;background:#fff;border-left:.5px solid rgba(0,0,0,.1);display:flex;flex-direction:column;font-family:Inter,-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:${DARK};transition:width .2s ease;}
    body.pr-overlay #pr-rail:not(.collapsed){box-shadow:-16px 0 40px rgba(0,0,0,.12);}
    #pr-rail.hidden{display:none!important;}
    #pr-rail .pr-scroll{flex:1;min-height:0;overflow-y:auto;display:flex;flex-direction:column;gap:12px;padding:12px 12px 0;}
    #pr-rail .pr-scroll::-webkit-scrollbar{width:6px} #pr-rail .pr-scroll::-webkit-scrollbar-thumb{background:rgba(0,0,0,.12);border-radius:3px}
    /* briefing surface */
    .pr-surface{background:radial-gradient(120% 90% at 100% 0%,#5B0020 0%,#2A0612 55%,#17060B 100%);color:#fff;border-radius:18px;padding:14px 16px 12px;position:relative;overflow:hidden;flex:none;}
    .pr-surface::after{content:'';position:absolute;right:-90px;top:-110px;width:260px;height:260px;border-radius:50%;background:radial-gradient(closest-side,rgba(255,80,130,.28),rgba(255,80,130,0));pointer-events:none}
    .pr-row{display:flex;align-items:center;gap:10px;position:relative}
    .pr-orb{width:40px;height:40px;border-radius:50%;position:relative;flex:none;cursor:pointer}
    .pr-orb .c{position:absolute;inset:9px;border-radius:50%;background:radial-gradient(circle at 35% 30%,#FF9BBB,#B8003F 55%,#4A0019);box-shadow:0 0 22px rgba(255,90,140,.6),inset -3px -4px 8px rgba(0,0,0,.35),inset 3px 3px 6px rgba(255,255,255,.35);animation:prBreathe 3.2s ease-in-out infinite}
    .pr-orb .r{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(255,120,160,.55);animation:prRipple 3.2s ease-out infinite}
    .pr-orb .r.two{animation-delay:1.6s}
    .pr-orb.listening .c{box-shadow:0 0 30px rgba(255,120,160,.9),inset -3px -4px 8px rgba(0,0,0,.35),inset 3px 3px 6px rgba(255,255,255,.35)}
    .pr-orb.speaking .r{animation-duration:1.1s}
    @keyframes prBreathe{0%,100%{transform:scale(.92)}50%{transform:scale(1.08)}}
    @keyframes prRipple{0%{transform:scale(.6);opacity:.9}100%{transform:scale(1.35);opacity:0}}
    @media (prefers-reduced-motion:reduce){.pr-orb .c,.pr-orb .r{animation:none}}
    .pr-eyebrow{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:rgba(255,255,255,.55);font-weight:600;flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .pr-spk{display:inline-flex;align-items:center;gap:6px;font-size:10.5px;color:rgba(255,255,255,.7);cursor:pointer;user-select:none;flex:none}
    .pr-spk .sw{width:26px;height:15px;border-radius:8px;background:rgba(255,255,255,.2);position:relative;display:inline-block;transition:background .15s}
    .pr-spk .sw::after{content:'';position:absolute;top:2px;left:2px;width:11px;height:11px;border-radius:50%;background:#fff;transition:left .15s}
    .pr-spk.on .sw{background:#fff} .pr-spk.on .sw::after{left:13px;background:${BRAND}}
    .pr-fold{width:24px;height:24px;border-radius:7px;border:.5px solid rgba(255,255,255,.22);background:rgba(255,255,255,.08);color:#fff;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;padding:0;flex:none}
    .pr-head{font-family:'Instrument Serif',Georgia,'Times New Roman',serif;font-size:24px;line-height:1.14;letter-spacing:-.01em;margin:10px 0 2px;position:relative;cursor:pointer}
    .pr-head em{font-style:italic;color:#FF9BBB}
    .pr-sub{font-size:12px;color:rgba(255,255,255,.62);position:relative;margin-bottom:6px;cursor:pointer}
    .pr-sig{display:flex;align-items:center;gap:10px;padding:7px 0;border-top:.5px solid rgba(255,255,255,.1);font-size:12.5px;position:relative}
    .pr-sig .t{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .pr-sig button{height:26px;padding:0 11px;border-radius:8px;border:.5px solid rgba(255,255,255,.22);background:rgba(255,255,255,.08);color:#fff;font:600 11px Inter,sans-serif;cursor:pointer;white-space:nowrap}
    .pr-sig button:hover{background:#fff;color:#5B0020}
    /* glass icon tiles */
    .pr-g{width:30px;height:30px;border-radius:9px;flex:none;display:inline-flex;align-items:center;justify-content:center;position:relative;box-shadow:0 4px 10px rgba(0,0,0,.25),inset 0 1px 0 rgba(255,255,255,.45),inset 0 -2px 4px rgba(0,0,0,.25)}
    .pr-g::before{content:'';position:absolute;inset:1px;border-radius:8px;background:linear-gradient(180deg,rgba(255,255,255,.35),rgba(255,255,255,0) 55%);pointer-events:none}
    .pr-g.rose{background:linear-gradient(145deg,#FF7FA6,#B8003F)} .pr-g.amber{background:linear-gradient(145deg,#FFD27A,#D98A00)} .pr-g.sea{background:linear-gradient(145deg,#7FD1FF,#0F6FB0)} .pr-g.mint{background:linear-gradient(145deg,#8FEBC2,#17885A)} .pr-g.ink{background:linear-gradient(145deg,#6B6B75,#1C1C1E)} .pr-g.red{background:linear-gradient(145deg,#FF9A8F,#B42318)}
    .pr-g svg{position:relative}
    /* verbs */
    .pr-verbs{display:grid;grid-template-columns:1fr 1fr;gap:8px;flex:none}
    .pr-verb{display:flex;flex-direction:column;align-items:flex-start;gap:9px;padding:11px 12px;border-radius:14px;border:.5px solid rgba(0,0,0,.08);background:#fff;cursor:pointer;text-align:left;color:${DARK};position:relative;font-family:inherit}
    .pr-verb:hover{border-color:rgba(153,0,51,.4);background:#FFF7F9}
    .pr-verb .pr-g{width:34px;height:34px;border-radius:11px}
    .pr-verb b{font-size:12.5px;font-weight:600;line-height:1.25} .pr-verb small{font-size:10.5px;color:#8E8E93}
    .pr-verb .n{position:absolute;top:10px;right:10px;min-width:20px;height:20px;border-radius:10px;background:${BRAND};color:#fff;font-size:11px;font-weight:600;display:inline-flex;align-items:center;justify-content:center;padding:0 6px}
    .pr-pin{display:flex;align-items:center;gap:10px;padding:8px 10px;border:.5px solid rgba(153,0,51,.3);background:#FFF7F9;border-radius:12px;font-size:12px;flex:none}
    .pr-pin b{font-weight:600} .pr-pin a{margin-left:auto;font-size:11px;color:${BRAND};cursor:pointer}
    .pr-minis{display:grid;grid-template-columns:1fr 1fr;gap:8px;flex:none}
    .pr-mini{display:flex;align-items:center;gap:8px;padding:8px 10px;border:.5px solid rgba(0,0,0,.08);border-radius:12px;font-size:11px;color:${MID};cursor:pointer;background:#fff}
    .pr-mini:hover{border-color:rgba(153,0,51,.4)} .pr-mini b{color:${DARK};font-weight:600;margin-left:auto} .pr-mini .pr-g{width:22px;height:22px;border-radius:7px}
    /* the moved panel + bar */
    body.pr-on #pulse-panel{position:static!important;max-height:none!important;flex:1 1 auto!important;min-height:120px;border:0!important;border-radius:0!important;box-shadow:none!important;overflow:visible!important;display:flex!important;flex-direction:column;background:transparent!important;margin:0 -12px;padding:0 12px;}
    body.pr-on #pulse-panel .pulse-chat{padding:4px 0 10px!important;flex:1 1 auto;overflow:visible!important}
    body.pr-on #pulse-panel .pulse-msg{max-width:92%!important}
    body.pr-on #pulse-panel .pulse-suggestions{display:none!important}   /* the verb tiles replace the stock questions */
    body.pr-on #pulse-panel #pt-strip,body.pr-on #pulse-panel #pm-strip{margin:0 0 8px!important}
    body.pr-on #pulse-minimize{display:none!important}
    body.pr-on #pulse-bar{position:static!important;left:auto!important;right:auto!important;bottom:auto!important;width:auto!important;margin:0!important;border:0!important;border-top:.5px solid rgba(0,0,0,.08)!important;border-radius:0!important;box-shadow:none!important;padding:8px 12px 12px!important;display:none;flex-wrap:wrap;gap:8px!important;justify-content:flex-start!important;background:#fff!important;flex:none}
    body.pr-on #pulse-bar.visible{display:flex!important}
    body.pr-on #pulse-bar > svg,body.pr-on #pulse-bar .pulse-name,body.pr-on #pulse-bar .pulse-divider,body.pr-on #pulse-bar .pulse-quick{display:none!important}
    body.pr-on #pulse-bar #ptg-wrap{order:1;margin-right:auto} body.pr-on #pulse-bar #pa-spoken{order:2} body.pr-on #pulse-bar #pv-pill{order:3} body.pr-on #pulse-bar #pv-mode{order:4;margin-right:0}
    body.pr-on #pulse-bar #pulse-input-display{order:10;flex:1 1 200px!important;min-width:0!important;height:40px;border-radius:12px!important;padding:0 14px!important}
    body.pr-on #pulse-bar #pulse-open-btn{order:11;width:40px!important;height:40px!important;border-radius:12px!important}
    /* collapsed strip */
    #pr-rail.collapsed{width:${WC}px} #pr-rail.collapsed .pr-scroll,#pr-rail.collapsed #pulse-bar,#pr-rail.collapsed #pulse-panel{display:none!important}
    .pr-strip{display:none;flex-direction:column;align-items:center;padding:10px 0;gap:14px;height:100%}
    #pr-rail.collapsed .pr-strip{display:flex}
    .pr-strip .pr-orb{width:34px;height:34px} .pr-strip .pr-orb .c{inset:8px}
    .pr-strip .vt{writing-mode:vertical-rl;transform:rotate(180deg);font-size:10px;color:${MID};letter-spacing:.04em}
    .pr-strip .dot{width:8px;height:8px;border-radius:50%;background:#D1D1D6} .pr-strip .dot.on{background:${OK};box-shadow:0 0 0 4px rgba(31,122,77,.15)}
    .pr-strip .badge{min-width:20px;height:20px;border-radius:10px;background:${BRAND};color:#fff;font-size:11px;font-weight:600;display:inline-flex;align-items:center;justify-content:center;padding:0 6px}
    .pr-strip .exp{width:30px;height:30px;border-radius:8px;border:.5px solid rgba(0,0,0,.1);background:#fff;display:inline-flex;align-items:center;justify-content:center;padding:0;cursor:pointer}
    /* compact header */
    body.pr-hdr .pn-brand > span[style*="border-left"]{display:none!important}
    body.pr-hdr .pn-user-meta,body.pr-hdr .pn-signout{display:none!important}
    body.pr-hdr .pn-user-info{cursor:pointer;position:relative;display:flex;align-items:center;gap:6px}
    body.pr-hdr .pn-user-info::after{content:'▾';font-size:10px;color:${LIGHT}}
    #pr-user-menu{position:absolute;right:0;top:calc(100% + 8px);background:#fff;border:.5px solid rgba(0,0,0,.1);border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.12);padding:10px 12px;display:none;flex-direction:column;gap:8px;min-width:200px;z-index:60;text-align:left}
    #pr-user-menu.open{display:flex} #pr-user-menu .pn-user-meta,#pr-user-menu .pn-signout{display:flex!important} #pr-user-menu .pn-user-meta{justify-content:flex-start;font-size:11px;color:${MID}}
    body.pr-hdr .pn-nav-item{font-size:12.5px}
  `;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);

  const ICO = {
    warning: '<path d="M12 3l10 18H2zM12 10v5M12 18h.01"/>', check: '<path d="M4 12l5 5L20 6"/>', clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    truck: '<path d="M3 17h18l-2 4H5zM5 14l2-8h10l2 8M12 6V3"/>', box: '<path d="M3 7l9-4 9 4v10l-9 4-9-4zM3 7l9 4 9-4M12 11v10"/>', hold: '<circle cx="12" cy="12" r="9"/><path d="M5 5l14 14"/>',
    mail: '<path d="M4 6h16v12H4zM4 7l8 6 8-6"/>', search: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/>', globe: '<path d="M3 12h18M12 3a14 14 0 010 18M12 3a14 14 0 000 18"/>',
    chart: '<path d="M4 19V5M4 19h16M8 15l4-6 4 3 4-7"/>', plus: '<path d="M12 4v16M4 12h16"/>', note: '<path d="M5 4h11l3 3v13H5zM8 12h8M8 16h5"/>', move: '<path d="M4 12h12M12 6l6 6-6 6"/>', list: '<path d="M8 6h12M8 12h12M8 18h12M4 6h.01M4 12h.01M4 18h.01"/>',
  };
  const ico = (k, size = 15) => `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICO[k] || ICO.clock}</svg>`;
  const tone = (k) => ({ warning: 'amber', check: 'mint', clock: 'ink', truck: 'sea', box: 'ink', hold: 'red', mail: 'amber', search: 'ink', globe: 'sea', chart: 'amber', plus: 'rose', note: 'ink', move: 'sea', list: 'ink' })[k] || 'ink';
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  // The serif headline gets one italic flourish: the first number, or the last word.
  const flourish = (t) => { const s = esc(t); const m = s.match(/\d[\d,.%]*\s?[A-Za-z%]*/); if (m) return s.slice(0, m.index) + '<em>' + m[0] + '</em>' + s.slice(m.index + m[0].length); return s.replace(/(\S+)([.!?]?)$/, '<em>$1</em>$2'); };

  const R = window._pulseRail = { state: { on: false, collapsed: false, page: 'weekhub', pin: null, insights: [], ui: null, overlay: false }, el: null };
  try { R.state.collapsed = localStorage.getItem('pulse_rail_collapsed') === '1'; } catch (_) {}

  // ── where are we ──
  function pageKey() {
    const a = document.querySelector('.pn-nav-item.active'); const id = a ? a.id : '';
    if (id === 'nav-receiving') return 'receiving'; if (id === 'nav-map') return 'transit';
    if (id === 'nav-weekhub' || !id) return 'weekhub'; return 'other';
  }
  const PAGE_LABEL = { weekhub: 'Week Hub', receiving: 'Receiving', transit: 'Transit', other: 'Pinpoint' };
  const weekLabel = () => { const ws = window.state && window.state.weekStart; if (!ws) return ''; const d = new Date(ws + 'T00:00:00Z'); if (isNaN(d)) return ''; const x = new Date(d); x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7) + 3); const f = new Date(Date.UTC(x.getUTCFullYear(), 0, 4)); return 'W' + (1 + Math.round(((x - f) / 86400000 - 3 + ((f.getUTCDay() + 6) % 7)) / 7)); };

  // ── ask Pulse (same path as typing) ──
  function fire(q) {
    const input = document.getElementById('pulse-input-display'); if (!input) return;
    if (typeof window.pulseIsOn === 'function' && !window.pulseIsOn()) { const sw = document.getElementById('ptg-sw'); if (sw) sw.click(); }
    input.value = q;
    if (typeof window.pulseSend === 'function') window.pulseSend();
  }
  function prefill(q) { const input = document.getElementById('pulse-input-display'); if (!input) return; input.value = q; input.focus(); try { input.setSelectionRange(q.length, q.length); } catch (_) {} }

  // ── briefing ──
  function brief() {
    const ins = R.state.insights || [], page = R.state.page, pin = R.state.pin, wk = weekLabel();
    const pick = (f) => ins.find(f);
    let head = '', sub = '', sigs = [], eyebrow = 'Pulse · ' + PAGE_LABEL[page] + (wk ? ' · ' + wk : '');
    if (pin && pin.page === 'receiving') {
      head = pin.ids.length + (pin.ids.length === 1 ? ' PO pinned.' : ' POs pinned.'); sub = (pin.supplier ? pin.supplier + ' · ' : '') + 'Ask about them, chase, or move them.'; eyebrow = 'Pulse · Receiving · ' + (pin.supplier || wk);
    } else if (pin && pin.page === 'transit') {
      head = pin.label + '.'; sub = 'Ask why, confirm a milestone, or note something on it.'; eyebrow = 'Pulse · Transit · ' + (pin.week || wk);
    } else if (page === 'receiving') {
      const plan = Array.isArray(window.state && window.state.plan) ? window.state.plan : [], rec = Array.isArray(window.state && window.state.receiving) ? window.state.receiving : [];
      const pos = new Set(plan.map(p => String(p.po_number || '').trim()).filter(Boolean)), got = new Set(rec.map(r => String(r.po_number || '').trim()).filter(Boolean));
      if (pos.size) { const inn = [...pos].filter(p => got.has(p)).length; head = pos.size + ' POs. ' + (inn ? inn + ' in.' : 'None in yet.'); sub = inn >= pos.size ? 'Everything planned has landed.' : (pos.size - inn) + ' still to come. Tick POs to pin them.'; }
      else { head = 'Receiving.'; sub = 'Tick POs in the table to pin them here.'; }
      const r = pick(i => /receiv|PO/i.test(i.text)); if (r) sigs.push(r);
    } else if (page === 'transit') {
      const t = pick(i => i.icon === 'truck' || i.icon === 'hold' || /transit|lane|container|customs/i.test(i.text));
      head = t ? t.text : 'Transit movements.'; sub = 'Open a movement to pin it here.';
      sigs = ins.filter(i => i !== t && /transit|lane|container|customs|depart|arriv/i.test(i.text)).slice(0, 2);
      if (t) R._headQ = t.pulse_question;
    } else {
      head = ins[0] ? ins[0].text : (wk ? 'Week ' + wk.slice(1) + '.' : 'Pulse.'); sub = ins[1] ? ins[1].text : (ins.length ? '' : 'Loading the week…');
      sigs = ins.slice(2, 5);
      R._headQ = ins[0] && ins[0].pulse_question; R._subQ = ins[1] && ins[1].pulse_question;
    }
    return { eyebrow, head, sub, sigs };
  }
  function verbs() {
    const page = R.state.page, pin = R.state.pin, wk = weekLabel(), wkN = Number((wk || 'W0').slice(1)) || 0;
    const V = (icon, b, small, run, n) => ({ icon, b, small, run, n });
    if (pin && pin.page === 'receiving') {
      const ids = pin.ids.join(', '), who = pin.supplier ? pin.supplier : 'the supplier';
      return [V('check', 'Which are in?', `of the ${pin.ids.length} pinned`, () => fire(`Which of these POs have been received, and which are still outstanding: ${ids}?`), pin.ids.length),
        V('mail', `Chase ${who}`, 'email, held 60 s', () => fire(`Email ${who} to chase these POs${pin.supplier ? '' : ''}: ${ids}. Ask for the delivery date and keep it short.`)),
        V('move', 'Move to next week', `W${wkN + 1}`, () => fire(`Move these POs to next week: ${ids}.`)),
        V('chart', `${pin.supplier || 'Supplier'} history`, 'last 8 weeks', () => fire(`How has ${who} performed on receiving over the last 8 weeks — on time, late, how late?`))];
    }
    if (pin && pin.page === 'transit') {
      return [V('search', 'Where is it?', 'and why, if late', () => fire(`Where is ${pin.label} right now, and if it is late, why?`)),
        V('truck', 'Confirm departure', 'if tracking has seen it', () => fire(`Confirm departure for ${pin.label}.`)),
        V('note', 'Add a note', 'on the movement', () => prefill(`Note on ${pin.label}: `)),
        V('mail', 'Tell the client', 'draft a delay note', () => fire(`Draft an email to the client about the status of ${pin.label} — what happened, the new ETA, and what we are doing.`))];
    }
    if (page === 'receiving') {
      return [V('warning', "Who's behind", 'receiving, and why', () => fire('Which suppliers are behind on receiving this week, and by how much?')),
        V('list', 'Not received yet', 'this week', () => fire('Which POs have not been received yet this week?')),
        V('mail', 'Chase a supplier', 'type the name', () => prefill('Email ')),
        V('chart', 'Receiving summary', 'two lines', () => fire('Summarise receiving for this week in two lines.'))];
    }
    if (page === 'transit') {
      return [V('hold', "What's late", 'and how late', () => fire('Which movements are late or at risk, and by how much?')),
        V('truck', 'Departures to confirm', 'tracking has seen them', () => fire('Which departures has tracking seen that we have not confirmed yet?')),
        V('warning', 'Customs holds', 'open right now', () => fire('Which movements are on customs hold right now, and for how long?')),
        V('chart', 'Transit in 2 lines', 'for the client', () => fire('Summarise transit for the client in two lines.'))];
    }
    return [V('warning', "What's at risk", 'this week', () => fire("What's at risk this week?")),
      V('search', "Who's behind", 'receiving, and why', () => fire('Which suppliers are behind on receiving this week, and why?')),
      V('globe', 'Where is…', 'a container or PO', () => prefill('Where is ')),
      V('chart', 'Week in 2 lines', 'for the stand-up', () => fire('Give me this week in two lines for the stand-up.'))];
  }

  // ── build ──
  function build() {
    if (R.el) return R.el;
    const rail = document.createElement('aside'); rail.id = 'pr-rail'; rail.className = 'hidden'; rail.setAttribute('aria-label', 'Pulse');
    rail.innerHTML = `
      <div class="pr-strip">
        <div class="pr-orb" role="button" aria-label="Open Pulse" title="Open Pulse"><span class="r"></span><span class="r two"></span><span class="c"></span></div>
        <span class="dot" data-voice></span><span class="vt" data-voice-txt>Pulse</span>
        <div style="flex:1"></div>
        <div style="display:flex;flex-direction:column;align-items:center;gap:4px"><span class="badge" data-tasks>0</span><span class="vt" style="font-size:9.5px">Tasks</span></div>
        <button class="exp" aria-label="Expand Pulse" title="Expand"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="${MID}" stroke-width="2" aria-hidden="true"><path d="M15 6l-6 6 6 6"/></svg></button>
      </div>
      <div class="pr-scroll">
        <div class="pr-surface">
          <div class="pr-row">
            <div class="pr-orb" title="Pulse"><span class="r"></span><span class="r two"></span><span class="c"></span></div>
            <div class="pr-eyebrow" data-eyebrow>Pulse</div>
            <label class="pr-spk" data-spk title="Spoken answers"><span class="sw"></span>Spoken</label>
            <button class="pr-fold" aria-label="Collapse Pulse" title="Collapse"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.2" aria-hidden="true"><path d="M9 6l6 6-6 6"/></svg></button>
          </div>
          <div class="pr-head" data-head></div>
          <div class="pr-sub" data-sub></div>
          <div data-sigs></div>
        </div>
        <div class="pr-pin" data-pin style="display:none"></div>
        <div class="pr-verbs" data-verbs></div>
        <div class="pr-minis">
          <div class="pr-mini" data-mini="tasks"><span class="pr-g rose">${ico('check', 12)}</span><span>Tasks</span><b data-tasks>0</b></div>
          <div class="pr-mini" data-mini="meetings"><span class="pr-g ink">${ico('clock', 12)}</span><span>Meetings</span><b data-meetings>—</b></div>
        </div>
      </div>`;
    document.body.appendChild(rail);
    R.el = rail;
    rail.querySelector('.pr-fold').onclick = () => setCollapsed(true);
    rail.querySelector('.pr-strip .exp').onclick = () => setCollapsed(false);
    rail.querySelector('.pr-strip .pr-orb').onclick = () => setCollapsed(false);
    rail.querySelector('[data-spk]').onclick = (e) => { e.preventDefault(); const s = document.getElementById('pa-spoken'); if (s) s.click(); };
    rail.querySelector('[data-head]').onclick = () => { if (R._headQ) fire(R._headQ); };
    rail.querySelector('[data-sub]').onclick = () => { if (R._subQ) fire(R._subQ); };
    rail.querySelectorAll('[data-mini]').forEach(m => { m.onclick = () => { const strip = document.getElementById(m.dataset.mini === 'tasks' ? 'pt-strip' : 'pm-strip'); if (!strip) return; const head = strip.querySelector('.pt-head, .pm-head'); if (head && !strip.classList.contains('open')) head.click(); strip.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); }; });
    return rail;
  }
  // Move the existing panel and bar into the rail (and back).
  function adopt() {
    const panel = document.getElementById('pulse-panel'), bar = document.getElementById('pulse-bar'), rail = R.el;
    if (!panel || !bar || !rail) return false;
    const scroll = rail.querySelector('.pr-scroll');
    if (panel.parentNode !== scroll) scroll.appendChild(panel);
    if (bar.parentNode !== rail) rail.appendChild(bar);
    // The rail scrolls as one column, so a new message must bring the column down with it.
    const chat = document.getElementById('pulse-chat');
    if (chat && !chat._prObs) { chat._prObs = new MutationObserver(() => { requestAnimationFrame(() => { scroll.scrollTop = scroll.scrollHeight; }); }); chat._prObs.observe(chat, { childList: true, subtree: true }); }
    return true;
  }
  function release() {
    const panel = document.getElementById('pulse-panel'), bar = document.getElementById('pulse-bar');
    if (panel && panel.parentNode !== document.body) document.body.appendChild(panel);
    if (bar && bar.parentNode !== document.body) document.body.appendChild(bar);
  }

  function paint() {
    const rail = R.el; if (!rail) return;
    const b = brief();
    rail.querySelector('[data-eyebrow]').textContent = b.eyebrow;
    rail.querySelector('[data-head]').innerHTML = flourish(b.head);
    rail.querySelector('[data-sub]').textContent = b.sub;
    rail.querySelector('[data-sigs]').innerHTML = b.sigs.map((s, i) => `<div class="pr-sig"${i === b.sigs.length - 1 ? ' style="padding-bottom:0"' : ''}><span class="pr-g ${tone(s.icon)}">${ico(s.icon, 14)}</span><span class="t" title="${esc(s.text)}">${esc(s.text)}</span><button data-q="${esc(s.pulse_question || s.text)}">Ask</button></div>`).join('');
    rail.querySelectorAll('[data-sigs] button').forEach(bt => { bt.onclick = () => fire(bt.dataset.q); });
    const pin = R.state.pin, pinEl = rail.querySelector('[data-pin]');
    if (pin) { pinEl.style.display = ''; pinEl.innerHTML = `<span class="pr-g rose" style="width:22px;height:22px;border-radius:7px"></span><span>About <b>${esc(pin.page === 'receiving' ? (pin.ids.length + (pin.ids.length === 1 ? ' PO' : ' POs')) : pin.label)}</b>${pin.supplier ? ' · ' + esc(pin.supplier) : (pin.week ? ' · ' + esc(pin.week) : '')}</span> <a data-unpin>unpin</a>`; pinEl.querySelector('[data-unpin]').onclick = () => setPin(null, true); }
    else pinEl.style.display = 'none';
    const vs = verbs(); const vEl = rail.querySelector('[data-verbs]');
    vEl.innerHTML = vs.map((v, i) => `<button class="pr-verb" data-v="${i}"><span class="pr-g ${tone(v.icon)}">${ico(v.icon)}</span><b>${esc(v.b)}</b><small>${esc(v.small)}</small>${v.n ? `<span class="n">${v.n}</span>` : ''}</button>`).join('');
    vEl.querySelectorAll('.pr-verb').forEach(bt => { bt.onclick = () => vs[Number(bt.dataset.v)].run(); });
    // mirrors
    const spk = document.getElementById('pa-spoken'); rail.querySelector('[data-spk]').classList.toggle('on', !!(spk && spk.classList.contains('on')));
    const tn = (document.querySelector('#pt-strip .pt-n') || {}).textContent || ''; const tcount = (tn.match(/\d+/) || ['0'])[0];
    rail.querySelectorAll('[data-tasks]').forEach(e => { e.textContent = tcount; });
    const mn = (document.querySelector('#pm-strip .pm-n') || {}).textContent || ''; rail.querySelector('[data-meetings]').textContent = mn.trim() || '—';
    rail.querySelector('[data-tasks].badge').style.visibility = tcount === '0' ? 'hidden' : '';
  }

  function setCollapsed(c) {
    R.state.collapsed = !!c; try { localStorage.setItem('pulse_rail_collapsed', c ? '1' : '0'); } catch (_) {}
    if (R.el) R.el.classList.toggle('collapsed', !!c);
    layout();
  }
  function layout() {
    const on = R.state.on; document.body.classList.toggle('pr-on', on);
    if (!on) return;
    const hdr = document.querySelector('.pn-nav'); const top = hdr ? Math.round(hdr.getBoundingClientRect().bottom) : 52;
    document.documentElement.style.setProperty('--pr-top', Math.max(0, top) + 'px');
    const overlay = window.innerWidth < OVERLAY_BELOW; R.state.overlay = overlay; document.body.classList.toggle('pr-overlay', overlay);
    // On a narrow screen the open rail floats over the page, so start it folded unless the person has chosen otherwise.
    let chosen = null; try { chosen = localStorage.getItem('pulse_rail_collapsed'); } catch (_) {}
    if (overlay && chosen == null && !R.state.collapsed) { R.state.collapsed = true; R.el && R.el.classList.add('collapsed'); }
    document.documentElement.style.setProperty('--pr-w', (R.state.collapsed ? WC : W) + 'px');
  }

  // ── pins ──
  function setPin(pin, byUser) {
    R.state.pin = pin; window._pulsePin = pin ? { page: pin.page, label: pin.label, ids: pin.ids || [] } : null;
    if (byUser && pin == null) R._unpinned = true;
    paint();
  }
  function readReceivingPin() {
    if (R.state.page !== 'receiving') return;
    const boxes = [...document.querySelectorAll('.recv-row-check[data-po]:checked')];
    const ids = [...new Set(boxes.map(b => b.dataset.po).filter(Boolean))];
    if (!ids.length) { if (R.state.pin && R.state.pin.page === 'receiving') setPin(null); return; }
    const sel = document.getElementById('recv-supplier'); const supplier = sel && sel.value ? sel.value : '';
    setPin({ page: 'receiving', label: `${ids.length} selected PO${ids.length === 1 ? '' : 's'}${supplier ? ' from ' + supplier : ''}`, ids, supplier });
  }
  document.addEventListener('change', (e) => { const t = e.target; if (t && (t.classList.contains('recv-row-check') || t.id === 'recv-check-all' || t.id === 'recv-supplier')) setTimeout(readReceivingPin, 0); });
  document.addEventListener('click', (e) => {
    const pick = e.target.closest && e.target.closest('[data-act="pick"][data-uid]');
    if (pick && R.state.page === 'transit') {
      const uid = pick.dataset.uid; const monos = pick.querySelectorAll('.tm-mono'); const short = monos.length ? monos[monos.length - 1].textContent.trim() : uid; const wk = pick.querySelector('.tm-wk') ? pick.querySelector('.tm-wk').textContent.trim() : '';
      setPin({ page: 'transit', label: short, ids: [uid], week: wk }); return;
    }
    if (e.target.closest && e.target.closest('[data-act="panel-close"]') && R.state.pin && R.state.pin.page === 'transit') setPin(null);
  }, true);

  // ── wiring ──
  function onPage() {
    const p = pageKey(); if (p === R.state.page) return;
    R.state.page = p; if (R.state.pin && R.state.pin.page !== p) setPin(null); paint();
  }
  function observe() {
    const nav = document.getElementById('pn-nav-items'); if (nav) new MutationObserver(onPage).observe(nav, { attributes: true, subtree: true, attributeFilter: ['class'] });
    window.addEventListener('hashchange', () => setTimeout(onPage, 0));
    window.addEventListener('pulse:insights', (e) => { R.state.insights = (e.detail && e.detail.insights) || []; paint(); });
    if (Array.isArray(window._whInsights)) R.state.insights = window._whInsights;
    window.addEventListener('pulse:voice', (e) => { const s = (e.detail || {}).state; R.el && R.el.querySelectorAll('.pr-orb').forEach(o => { o.classList.toggle('listening', s === 'listening' || s === 'hearing'); o.classList.toggle('speaking', s === 'speaking' || s === 'thinking'); }); const d = R.el && R.el.querySelector('[data-voice]'), t = R.el && R.el.querySelector('[data-voice-txt]'); if (d) d.classList.toggle('on', !!s && s !== 'off' && s !== 'error'); if (t) t.textContent = s && s !== 'off' ? (s === 'speaking' ? 'Speaking' : 'Listening') : 'Pulse'; });
    const spk = document.getElementById('pa-spoken'); if (spk) new MutationObserver(paint).observe(spk, { attributes: true, attributeFilter: ['class'] });
    const bar = document.getElementById('pulse-bar'); if (bar) new MutationObserver(() => { if (R.el) R.el.classList.toggle('hidden', !bar.classList.contains('visible')); }).observe(bar, { attributes: true, attributeFilter: ['class'] });
    const strips = () => { const t = document.getElementById('pt-strip'), m = document.getElementById('pm-strip'); if (t && !t._prObs) { t._prObs = new MutationObserver(paint); t._prObs.observe(t, { childList: true, subtree: true, characterData: true }); } if (m && !m._prObs) { m._prObs = new MutationObserver(paint); m._prObs.observe(m, { childList: true, subtree: true, characterData: true }); } };
    strips(); setInterval(strips, 2000);
    window.addEventListener('resize', layout);
    window.addEventListener('tenancy:ready', () => setTimeout(paint, 0));
    document.addEventListener('visibilitychange', () => { if (!document.hidden) paint(); });
  }
  function compactHeader() {
    if (!R.state.ui || R.state.ui.header === false) return;
    document.body.classList.add('pr-hdr');
    const info = document.querySelector('.pn-user-info'); if (!info || document.getElementById('pr-user-menu')) return;
    const menu = document.createElement('div'); menu.id = 'pr-user-menu';
    const meta = info.querySelector('.pn-user-meta'), out = document.querySelector('.pn-signout');
    if (meta) menu.appendChild(meta); if (out) menu.appendChild(out);
    info.appendChild(menu);
    info.onclick = (e) => { if (e.target.closest('#pr-user-menu')) { setTimeout(() => menu.classList.remove('open'), 0); return; } menu.classList.toggle('open'); };
    document.addEventListener('click', (e) => { if (!e.target.closest('.pn-user-info')) menu.classList.remove('open'); });
  }

  async function uiSwitches() {
    try {
      const base = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
      let token = null; if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
      if (!token) return null;
      const r = await fetch(base + '/pulse/ui', { headers: { Authorization: 'Bearer ' + token } });
      return r.ok ? await r.json() : null;
    } catch (_) { return null; }
  }
  function enable() {
    if (R.state.on) return;
    build(); if (!adopt()) return;
    R.state.on = true; R.state.page = pageKey();
    R.el.classList.toggle('collapsed', R.state.collapsed);
    // Tasks and meetings start folded in the rail; the minis above open them.
    ['pt-strip', 'pm-strip'].forEach(id => { const el = document.getElementById(id); if (el && el.classList.contains('open')) { const h = el.querySelector('.pt-head, .pm-head'); if (h) h.click(); else el.classList.remove('open'); } });
    const bar = document.getElementById('pulse-bar'); R.el.classList.toggle('hidden', !(bar && bar.classList.contains('visible')));
    layout(); paint(); compactHeader();
    console.log('[pulse-rail] v2 on');
  }
  function disable() { if (!R.state.on) return; release(); R.state.on = false; document.body.classList.remove('pr-on', 'pr-overlay', 'pr-hdr'); if (R.el) R.el.classList.add('hidden'); }

  let tries = 0;
  async function init() {
    // Wait for the host page's bar and the modules that decorate it, then for sign-in (the
    // switch endpoint needs a token). Default is on; PULSE_RAIL=off turns it off after the
    // first successful read, so a server-side off never leaves the bottom bar unusable.
    const ready = document.getElementById('pulse-bar') && document.getElementById('pulse-panel') && document.getElementById('ptg-wrap');
    if (!ready) { if (++tries < 80) return setTimeout(init, 250); }
    observe();
    let ui = null; for (let i = 0; i < 40 && !ui; i++) { ui = await uiSwitches(); if (!ui) await new Promise(r => setTimeout(r, 1500)); }
    R.state.ui = ui || { rail: true, header: true };
    let localOff = false; try { localOff = localStorage.getItem('pulse_rail') === 'off'; } catch (_) {}
    if (R.state.ui.rail === false || localOff) { console.log('[pulse-rail] off (' + (localOff ? 'local' : 'PULSE_RAIL') + ')'); return; }
    enable();
  }
  Object.assign(R, { setCollapsed, setPin, refresh: paint, page: pageKey, enable, disable, fire });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
