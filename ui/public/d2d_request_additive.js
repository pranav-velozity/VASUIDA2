/* ── VelOzity Pinpoint — Simulator · requests v3 (Phase 5b.2) ──
   v3: units can be typed when the period has no plan yet (and overridden when it does).
   v4: Delete a request (Hide for an approved one — its quotes stay).
   v2: the mock's dark canvas (the one dark surface in Pinpoint), options as cards, and no
   internal review on this page at all — that moved to Quote Review › Requests.
   The client side of door-to-door options: raise a request (where from, where to, when, how
   much), see the options VelOzity releases, pick, and approve once the price is firm. The
   simulation boards (5d) replace the options table on this page; the request and the states
   stay as they are here.

   This page reads client endpoints only (/rq/requests, /rq/prefill): nothing on it can carry
   cost, margin or a partner's name, whoever is logged in.

   Wording on client surfaces: Price, Ask VelOzity. Never sell, never partner. */
(function () {
  if (window.__d2dReqLoaded) return; window.__d2dReqLoaded = true;
  const BUILD = 'd2dreq-4';
  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const nf = n => n == null ? '—' : Number(n).toLocaleString();
  const money = (n, cur) => n == null ? '—' : (cur || 'USD') + ' ' + Number(n).toLocaleString('en-AU', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  const dmy = s => { if (!s) return '—'; const d = new Date(String(s).slice(0, 10) + 'T00:00:00Z'); return isNaN(d) ? String(s) : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); };
  const todayYmd = () => new Date().toISOString().slice(0, 10);
  const mondayOf = ymd => { const d = new Date(ymd + 'T00:00:00Z'); if (isNaN(d)) return null; d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7)); return d.toISOString().slice(0, 10); };
  const addDays = (ymd, n) => { const d = new Date(ymd + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');
  async function tok() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts) {
    const t = await tok(), o = opts || {}; const headers = { 'Content-Type': 'application/json', ...(o.headers || {}) };
    if (t) headers.Authorization = 'Bearer ' + t; if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers }); const txt = await r.text(); let d = null; try { d = txt ? JSON.parse(txt) : null; } catch (e) { d = txt; }
    if (!r.ok) { const e = new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); e.status = r.status; e.body = d; throw e; } return d;
  }
  // The simulator's own palette — the dark ground from the mock. Nothing else in Pinpoint uses it.
  const C = { ground: '#010B13', card: '#121A21', card2: '#0F171E', raised: '#1A232B', line: 'rgba(255,255,255,.09)', line2: 'rgba(255,255,255,.16)', text: '#E8ECEF', mute: '#9AA4AD', dim: '#7E8891', faint: '#5F6973', wine: '#990033', rose: '#FF6B9A', rose2: '#FF9BBB', mint: '#5FD39A', sky: '#7FD1FF', amber: '#F5B85B', mintBg: '#0F2E22', skyBg: '#0F2A3C', amberBg: '#3A2A10', roseBg: '#2A1620', tint: '#1A232B' };
  const STATE = { requested: ['Asked', C.tint, C.mute], with_velozity: ['With VelOzity', C.amberBg, C.amber], options_ready: ['Options ready', C.skyBg, C.sky], picked: ['Picked · confirming', C.amberBg, C.amber], firm: ['Firm · approve', C.mintBg, C.mint], approved: ['Approved', C.mintBg, C.mint], declined: ['Declined', C.tint, C.mute], expired: ['Expired', C.roseBg, C.rose] };
  const OSTATE = { approximate: ['approximate', C.tint, C.mute], picked: ['picked', C.amberBg, C.amber], confirmed: ['confirmed', C.amberBg, C.amber], firm: ['firm', C.mintBg, C.mint], approved: ['approved', C.mintBg, C.mint], expired: ['expired', C.roseBg, C.rose], withdrawn: ['withdrawn', C.tint, C.faint] };

  let _list = null, _open = null, _detail = null, _pf = null, _form = null, _isOpen = false, _err = null, _showForm = false, _picks = new Set();

  function styles() {
    if (el('d2r-css')) return; const s = document.createElement('style'); s.id = 'd2r-css';
    if (!el('d2r-fonts')) { const l = document.createElement('link'); l.id = 'd2r-fonts'; l.rel = 'stylesheet'; l.href = 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Instrument+Serif:ital@0;1&display=swap'; document.head.appendChild(l); }
    s.textContent = `
      #page-simulator{padding:0;}
      .d2r{background:${C.ground};color:${C.text};border-radius:18px;padding:26px 28px 34px;min-height:78vh;font-family:Inter,-apple-system,"Segoe UI",system-ui,sans-serif;-webkit-font-smoothing:antialiased;} .d2r *{box-sizing:border-box;}
      .d2r-eyebrow{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:${C.dim};font-weight:600;}
      .d2r-h1{font-family:"Instrument Serif",Georgia,serif;font-weight:400;font-size:38px;line-height:1.1;letter-spacing:-.01em;margin:6px 0 0;} .d2r-h1 em{font-style:italic;color:${C.rose2};}
      .d2r-sub{font-size:13px;color:${C.mute};margin-top:8px;max-width:720px;line-height:1.5;}
      .d2r-head{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap;margin-bottom:22px;}
      .d2r-btn{border:0;border-radius:999px;padding:10px 18px;font:600 12.5px inherit;cursor:pointer;background:${C.wine};color:#fff;} .d2r-btn:hover{background:#B0003B;} .d2r-btn.sec{background:${C.card};color:${C.text};border:0.5px solid ${C.line2};} .d2r-btn.sec:hover{border-color:${C.rose};background:${C.card};} .d2r-btn.sm{padding:7px 13px;font-size:11.5px;} .d2r-btn[disabled]{opacity:.45;cursor:default;}
      .d2r-pill{display:inline-block;font-size:9.5px;letter-spacing:.06em;text-transform:uppercase;font-weight:700;border-radius:999px;padding:3px 8px;}
      .d2r-card{background:${C.card};border:0.5px solid ${C.line};border-radius:16px;padding:16px 18px;}
      .d2r-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:10px;}
      .d2r-rq{cursor:pointer;transition:border-color .15s;} .d2r-rq:hover{border-color:rgba(255,107,154,.5);} .d2r-rq.on{border:1.5px solid ${C.rose};padding:15px 17px;background:#1A121A;}
      .d2r-rq .t{display:flex;gap:8px;align-items:center;font-size:13.5px;font-weight:600;} .d2r-rq .m{font-size:11.5px;color:${C.mute};margin-top:7px;line-height:1.55;}
      .d2r-sec{display:flex;align-items:baseline;gap:12px;margin:26px 0 12px;flex-wrap:wrap;} .d2r-sec h2{font-size:15px;font-weight:600;margin:0;letter-spacing:-.01em;} .d2r-sec span{font-size:11.5px;color:${C.dim};}
      .d2r-form{display:grid;grid-template-columns:1fr 1fr;gap:14px;} @media (max-width:900px){.d2r-form{grid-template-columns:1fr;}}
      .d2r-f label{display:block;font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:${C.dim};font-weight:600;margin-bottom:6px;}
      .d2r-f .in{min-height:46px;border:0.5px solid ${C.line2};border-radius:12px;background:${C.card2};display:flex;align-items:center;padding:0 14px;font-size:14px;gap:10px;flex-wrap:wrap;color:${C.text};}
      .d2r-f .in input,.d2r-f .in select{border:0;background:none;font:inherit;font-size:14px;outline:none;min-width:0;flex:1;color:${C.text};color-scheme:dark;} .d2r-f .in input[type=number]{text-align:right;max-width:110px;} .d2r-f .in input::placeholder{color:${C.faint};}
      .d2r-f .in .m{color:${C.faint};font-size:12px;margin-left:auto;}
      .d2r-seg{display:inline-flex;border:0.5px solid ${C.line2};border-radius:999px;padding:2px;background:${C.card};} .d2r-seg button{background:none;border:0;color:${C.mute};font-size:11px;padding:4px 12px;border-radius:999px;cursor:pointer;font-family:inherit;} .d2r-seg button.on{background:${C.wine};color:#fff;}
      .d2r-chip{display:inline-flex;align-items:center;gap:6px;border:0.5px solid ${C.line2};border-radius:999px;padding:5px 11px;font-size:12px;background:${C.card};cursor:pointer;color:${C.mute};} .d2r-chip.on{border-color:${C.rose};background:#2A1620;color:${C.rose2};}
      .d2r-way{flex:1;border:0.5px solid ${C.line};border-radius:18px;padding:20px 22px;background:${C.card};cursor:pointer;position:relative;display:flex;flex-direction:column;gap:10px;} .d2r-way:hover{border-color:rgba(255,107,154,.5);} .d2r-way.on{border:1.5px solid ${C.rose};background:#1A121A;}
      .d2r-way h3{margin:0;font-size:16px;font-weight:600;} .d2r-way p{margin:0;font-size:12.5px;color:${C.mute};line-height:1.5;} .d2r-way .when{font-size:11px;color:${C.dim};margin-top:auto;}
      .d2r-ogrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:10px;}
      .d2r-opt{background:${C.card};border:0.5px solid ${C.line};border-radius:16px;padding:16px 18px;position:relative;display:flex;flex-direction:column;gap:8px;transition:border-color .15s;} .d2r-opt.dead{opacity:.45;} .d2r-opt.pk{border:1.5px solid ${C.rose};background:#1A121A;padding:15px 17px;} .d2r-opt.can{cursor:pointer;} .d2r-opt.can:hover{border-color:rgba(255,107,154,.5);}
      .d2r-opt .top{display:flex;gap:10px;align-items:center;padding-right:28px;} .d2r-opt .nm{font-size:14px;font-weight:600;letter-spacing:-.01em;} .d2r-opt .nm small{display:block;font-size:11px;color:${C.dim};font-weight:400;margin-top:1px;}
      .d2r-opt .price{font-size:26px;font-weight:600;letter-spacing:-.02em;font-variant-numeric:tabular-nums;line-height:1;margin-top:4px;} .d2r-opt .price small{display:block;font-size:11px;color:${C.dim};font-weight:400;letter-spacing:0;margin-top:5px;}
      .d2r-opt .facts{display:flex;gap:14px;flex-wrap:wrap;font-size:11.5px;color:${C.mute};font-variant-numeric:tabular-nums;} .d2r-opt .facts b{color:${C.text};font-weight:600;}
      .d2r-opt .pickmark{position:absolute;top:14px;right:14px;width:22px;height:22px;border-radius:50%;border:1.5px solid ${C.line2};display:flex;align-items:center;justify-content:center;font-size:12px;color:#fff;} .d2r-opt.pk .pickmark{background:${C.wine};border-color:${C.wine};}
      .d2r-g{width:30px;height:30px;border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:9.5px;font-weight:700;letter-spacing:.06em;flex:none;} .d2r-g.sea{background:${C.mintBg};color:${C.mint};} .d2r-g.air{background:${C.skyBg};color:${C.sky};}
      .d2r-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:12px;} @media (max-width:1100px){.d2r-kpis{grid-template-columns:repeat(2,minmax(0,1fr));}}
      .d2r-kpi{background:${C.card};border:0.5px solid ${C.line};border-radius:14px;padding:13px 15px;} .d2r-kpi small{display:block;font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:${C.dim};font-weight:600;} .d2r-kpi b{display:block;font-size:22px;font-weight:600;margin-top:4px;letter-spacing:-.02em;font-variant-numeric:tabular-nums;} .d2r-kpi i{display:block;font-style:normal;font-size:11px;color:${C.mute};margin-top:2px;}
      .d2r-note{font-size:11px;color:${C.faint};margin-top:10px;line-height:1.5;}
      .d2r-empty{background:${C.card};border:0.5px solid ${C.line};border-radius:16px;padding:28px;text-align:center;color:${C.mute};font-size:13px;}
      .d2r-toast{position:fixed;left:50%;bottom:26px;transform:translateX(-50%);background:#fff;color:#1C1C1E;padding:10px 16px;border-radius:999px;font-size:12.5px;z-index:9999;opacity:0;transition:opacity .2s;pointer-events:none;box-shadow:0 6px 24px rgba(0,0,0,.35);} .d2r-toast.on{opacity:1;}
      body.pr-on #page-simulator .d2r-ogrid,body.pr-on #page-simulator .d2r-grid{grid-template-columns:repeat(auto-fill,minmax(250px,1fr));}
    `; document.head.appendChild(s);
  }
  let _toastT; function toast(m) { let t = el('d2r-toast'); if (!t) { t = document.createElement('div'); t.id = 'd2r-toast'; t.className = 'd2r-toast'; document.body.appendChild(t); } t.textContent = m; t.classList.add('on'); clearTimeout(_toastT); _toastT = setTimeout(() => t.classList.remove('on'), 2600); }

  // ── nav + route ──
  function injectNav() { if (el('nav-simulator')) return; const nav = el('pn-nav-items'), after = el('nav-performance') || el('nav-map'); if (!nav || !after) return; const a = document.createElement('a'); a.className = 'pn-nav-item'; a.id = 'nav-simulator'; a.href = '#simulator'; a.textContent = 'Simulator'; a.onclick = e => { e.preventDefault(); window.show('#simulator'); }; after.insertAdjacentElement('afterend', a); }
  function section() { let s = el('page-simulator'); if (!s) { s = document.createElement('section'); s.id = 'page-simulator'; s.className = 'hidden'; s.style.display = 'none'; const main = document.querySelector('main.vo-wrap') || document.querySelector('main'); if (main) main.appendChild(s); } return s; }
  const PAGES = ['page-dashboard', 'page-intake', 'page-exec', 'page-receiving', 'page-reports', 'page-map', 'page-finance', 'page-d2d', 'page-elevate', 'page-uid-labels', 'page-flow', 'page-performance'];
  function wrapRouter() {
    if (window.__d2rRouted) return; window.__d2rRouted = true; const orig = window.show;
    window.show = function (hash) {
      let h = String(hash || location.hash || '').trim(); if (h.startsWith('#/')) h = '#' + h.slice(2);
      if (h === '#simulator' || h.startsWith('#simulator/')) {
        PAGES.forEach(id => { const p = el(id); if (p) { p.classList.add('hidden'); p.style.display = 'none'; } }); if (typeof window.hideReceivingPage === 'function') { try { window.hideReceivingPage(); } catch (_) {} }
        if (location.hash !== h) history.replaceState(null, '', h);
        const s = section(); s.classList.remove('hidden'); s.style.display = 'block'; document.querySelectorAll('.pn-nav-item').forEach(x => x.classList.remove('active')); const n = el('nav-simulator'); if (n) n.classList.add('active');
        _isOpen = true; const ref = h.startsWith('#simulator/') ? decodeURIComponent(h.slice(11)) : null; open(false, ref).catch(e => console.error('[simulator] open failed', e)); return;
      }
      _isOpen = false; const s = el('page-simulator'); if (s) { s.classList.add('hidden'); s.style.display = 'none'; } if (typeof orig === 'function') return orig.apply(this, arguments);
    };
  }

  // ── data ──
  async function open(force, ref) {
    styles(); _err = null;
    if (!_list || force) { render(); try { _list = (await req('/rq/requests')).requests || []; } catch (e) { _err = e; _list = []; } }
    if (ref) { const x = _list.find(r => r.ref === ref); if (x) _open = x.id; }
    if (_open) await loadDetail(_open);
    render();
  }
  async function loadDetail(id) {
    try { _detail = await req('/rq/requests/' + encodeURIComponent(id)); } catch (e) { _detail = null; }
    _picks = new Set((_detail && _detail.options || []).filter(o => ['picked', 'confirmed', 'firm', 'approved'].includes(o.state)).map(o => o.id));
  }
  async function refreshAll() { try { _list = (await req('/rq/requests')).requests || []; } catch (_) {} if (_open) await loadDetail(_open); render(); }
  async function loadPrefill() {
    const f = _form; try { _pf = await req(`/rq/prefill?period_type=${f.period_type}&period_start=${encodeURIComponent(f.period_start)}`); if (!f.origins_set) f.origins = _pf.origins.slice(); if (!f.need_by) f.need_by = f.period_type === 'week' ? addDays(_pf.period_start, 13) : addDays(_pf.period_start, 27); if (!f.destination) f.destination = _pf.destination; } catch (e) { _pf = { error: e.message }; }
    render();
  }

  // ── render ──
  function render() {
    const s = section(); styles();
    const open = _detail && _detail.id === _open ? _detail : null;
    const live = (_list || []).filter(r => !['approved', 'declined', 'expired'].includes(r.state));
    const ready = live.filter(r => r.client_state === 'options_ready' || r.client_state === 'firm');
    let html = `<div class="d2r" data-build="${BUILD}">
      <div class="d2r-head"><div><div class="d2r-eyebrow">Door-to-door simulator</div><h1 class="d2r-h1">Where from, <em>where to,</em> and when.</h1>
        <div class="d2r-sub">Ask for options for a week or a month, compare them on price and days against the date you need stock in the Sydney FC, pick, and approve once the price is firm.</div></div>
        <div style="margin-left:auto;display:flex;gap:8px;align-items:center;"><button class="d2r-btn" id="d2r-new">${_showForm ? 'Close' : 'New request'}</button></div></div>`;
    if (_showForm) html += formHtml();
    if (!_list) html += `<div class="d2r-empty">Loading…</div>`;
    else if (_err) html += `<div class="d2r-empty">${esc(_err.status === 403 ? 'The simulator is not available for this account.' : _err.message)}</div>`;
    else {
      html += `<div class="d2r-sec"><h2>Requests</h2><span>${live.length} open · ${ready.length} waiting on you</span></div>`;
      if (!_list.length) html += `<div class="d2r-empty">No requests yet. Start one — where from, where to, and when.</div>`;
      else html += `<div class="d2r-grid">${_list.map(r => { const st = STATE[r.client_state] || STATE.requested; const vol = r.volume || {}; return `<div class="d2r-card d2r-rq${_open === r.id ? ' on' : ''}" data-open="${esc(r.id)}">
          <div class="t">${esc(r.period_label)} <span style="color:${C.dim};font-weight:500">${esc(r.ref)}</span><span class="d2r-pill" style="margin-left:auto;background:${st[1]};color:${st[2]}">${esc(st[0])}</span></div>
          <div class="m">${esc((r.origins || []).join(' · ') || '—')} → ${esc(r.destination || 'Sydney')}<br>${nf(vol.units)} units${vol.expected_units && vol.expected_units !== vol.units ? ` · expected ${nf(vol.expected_units)} after supplier history` : ''}${r.need_by ? ` · in FC by <b style="color:${C.text}">${esc(dmy(r.need_by))}</b>` : ''}<br>${r.options_count ? `${r.options_count} option${r.options_count === 1 ? '' : 's'}${r.picked_count ? ` · ${r.picked_count} picked` : ''}` : (r.client_state === 'requested' ? (r.source === 'history' ? 'No history on these lanes yet — Ask VelOzity' : 'Waiting for options') : '')}</div></div>`; }).join('')}</div>`;
      if (open) html += detailHtml(open);
    }
    html += `</div>`; s.innerHTML = html; bind(s);
  }
  function formHtml() {
    const f = _form; const pf = _pf || {};
    const originList = pf.origin_list || []; const unmapped = pf.unmapped || [];
    return `<div class="d2r-card" style="margin-bottom:18px;padding:20px 22px;">
      <div class="d2r-note" style="margin:0 0 12px;color:${C.mute}">Everything else is pre-filled from your plan and can be changed later.</div>
      <div class="d2r-form">
        <div class="d2r-f"><label>Period</label><div class="in"><span class="d2r-seg"><button data-pt="week" class="${f.period_type === 'week' ? 'on' : ''}">Week</button><button data-pt="month" class="${f.period_type === 'month' ? 'on' : ''}">Month</button></span>
          ${f.period_type === 'week' ? `<input type="date" id="d2r-ps" value="${esc(f.period_start)}">` : `<input type="month" id="d2r-ps" value="${esc(f.period_start.slice(0, 7))}">`}<span class="m">${pf.period_label ? esc(pf.period_label) + ' · ' + nf(pf.pos) + ' POs in plan' : ''}</span></div></div>
        <div class="d2r-f"><label>In Sydney FC by</label><div class="in"><input type="date" id="d2r-nb" value="${esc(f.need_by || '')}"><span class="m">the date every option is judged against</span></div></div>
        <div class="d2r-f"><label>From</label><div class="in" style="padding:6px 12px;">${originList.length ? originList.map(o => `<span class="d2r-chip${f.origins.includes(o.name) ? ' on' : ''}" data-origin="${esc(o.name)}">${esc(o.name)}</span>`).join('') : `<span style="color:${C.dim};font-size:12px">No origins set up yet — ask VelOzity to add your origin ports.</span>`}${pf.origins && pf.origins.length ? `<span class="m">${esc(pf.origins.length)} from the plan</span>` : ''}</div>
          ${unmapped.length && originList.length ? `<div class="d2r-note">Suppliers without an origin: ${unmapped.map(u => `<span style="white-space:nowrap">${esc(u)} → <select data-map="${esc(u)}" style="font:inherit;font-size:11px;border:0.5px solid ${C.line2};border-radius:6px;padding:2px 4px;background:${C.card};color:${C.text}"><option value="">pick</option>${originList.map(o => `<option value="${esc(o.id)}">${esc(o.name)}</option>`).join('')}</select></span>`).join(' · ')}</div>` : ''}</div>
        <div class="d2r-f"><label>To</label><div class="in"><input id="d2r-dest" value="${esc(f.destination || '')}" placeholder="Sydney · VOZ_KY"><span class="m">facility</span></div></div>
        <div class="d2r-f" style="grid-column:1 / -1;"><label>Volume</label><div class="in"><input type="number" id="d2r-units" min="0" style="max-width:120px;font-weight:600;font-size:15px" value="${esc(f.units != null && f.units !== '' ? f.units : (pf.units || ''))}" placeholder="units"><span style="color:${C.dim};font-size:12px">${pf.units ? (pf.expected_units != null && pf.expected_units !== pf.units ? `units · from the ${esc(pf.period_label || '')} plan · expected ${nf(pf.expected_units)} after supplier history` : `units · from the ${esc(pf.period_label || '')} plan`) : (pf.error ? 'plan could not be read — enter units' : `no plan in Pinpoint for ${esc(pf.period_label || 'this period')} yet — enter units`)}</span>
          <span style="margin-left:auto;display:flex;gap:10px;align-items:center;font-size:12px;color:${C.mute}">cartons <input type="number" id="d2r-ctn" min="0" value="${esc(f.cartons || '')}" placeholder="—"> CBM <input type="number" id="d2r-cbm" min="0" step="0.1" value="${esc(f.cbm || '')}" placeholder="—"> kg <input type="number" id="d2r-kg" min="0" value="${esc(f.kg || '')}" placeholder="—"></span></div>
          ${(pf.suppliers || []).length ? `<div class="d2r-note">${pf.suppliers.slice(0, 6).map(s => `${esc(s.supplier)} ${nf(s.units)}${s.signal && s.signal.late_days_p50 ? ` <span style="color:${C.rose2}">${esc(s.signal.late_days_p50)} d late, ${Math.round(s.signal.fill_rate)}% of plan</span>` : ''}`).join(' · ')}${pf.suppliers.length > 6 ? ` · +${pf.suppliers.length - 6} more` : ''}</div>` : ''}</div>
      </div>
      <div style="display:flex;gap:14px;margin-top:16px;">
        <div class="d2r-way${f.source === 'history' ? ' on' : ''}" data-src="history"><span class="d2r-pill" style="position:absolute;top:14px;right:14px;background:${C.tint};color:${C.mute}">instant</span><h3>Use what we know</h3><p>Options we've shipped or quoted on these lanes in the last six months, priced at the average approved rate and timed on what the lanes actually achieved.</p><div class="when">Ready now · labelled <i>from history</i></div></div>
        <div class="d2r-way${f.source === 'ask' ? ' on' : ''}" data-src="ask"><h3>Ask VelOzity</h3><p>Fresh carrier options for this period — sailings, flights, transit and price — checked by VelOzity before you see them. Upgrade a history run to this any time.</p><div class="when">Usually within a business day · labelled <i>approximate</i>, with a validity date</div></div>
      </div>
      <div style="display:flex;gap:12px;align-items:center;margin-top:16px;"><button class="d2r-btn" id="d2r-go" style="height:44px;padding:0 22px;font-size:13px" ${!_pf ? 'disabled' : ''}>${f.source === 'history' ? 'Build the options' : 'Ask VelOzity'}</button>
        <input id="d2r-note" placeholder="Note for VelOzity (optional)" style="flex:1;border:0.5px solid ${C.line2};border-radius:999px;padding:10px 14px;font:inherit;font-size:12.5px;background:${C.card2};color:${C.text}" value="${esc(f.note || '')}">
        ${(pf.supplier_notes || []).length ? `<span class="d2r-note" style="margin:0">Accounting for: ${esc(pf.supplier_notes.join(' · '))}</span>` : ''}</div></div>`;
  }
  function detailHtml(r) {
    const st = STATE[r.client_state] || STATE.requested; const vol = r.volume || {}; const opts = (r.options || []).filter(o => o.state !== 'withdrawn');
    const pickable = r.client_state === 'options_ready' || r.client_state === 'picked'; const firm = r.client_state === 'firm';
    const canPick = pickable && opts.some(o => o.state === 'approximate');
    const live = opts.filter(o => !['expired', 'withdrawn'].includes(o.state)); const sea = live.filter(o => o.mode === 'Sea'), air = live.filter(o => o.mode === 'Air');
    const card = o => { const os = OSTATE[o.state] || OSTATE.approximate; const dead = ['expired', 'withdrawn'].includes(o.state); const pk = _picks.has(o.id) || ['picked', 'confirmed', 'firm', 'approved'].includes(o.state); const can = canPick && !dead && ['approximate', 'picked'].includes(o.state);
      const basis = o.price_basis === 'per_container' ? 'per container' : o.price_basis === 'per_kg' ? 'per chargeable kg' : 'total';
      return `<div class="d2r-opt${dead ? ' dead' : ''}${pk ? ' pk' : ''}${can ? ' can' : ''}" ${can ? `data-pick="${esc(o.id)}"` : ''}>
        ${can || pk ? `<div class="pickmark">${pk ? '✓' : ''}</div>` : ''}
        <div class="top"><span class="d2r-g ${o.mode.toLowerCase()}">${esc(o.mode.toUpperCase())}</span><div class="nm">${esc([o.carrier, o.service].filter(Boolean).join(' · ') || (o.kind === 'history' ? 'From history' : 'Option'))}<small>${o.kind === 'history' ? 'from what we know' : (o.firm ? 'firm' : 'approximate')}${o.container_size ? ` · ${esc(o.container_size)}'` : ''}${o.origin ? ' · ' + esc(o.origin) : ''}</small></div></div>
        <div class="price">${o.price != null ? esc(money(o.price, o.currency)) : `<span style="font-size:14px;color:${C.dim};font-weight:400">${r.client_state === 'with_velozity' || r.client_state === 'requested' ? 'pricing with VelOzity' : '—'}</span>`}<small>${o.price != null ? basis + ' · ex GST' : ''}</small></div>
        <div class="facts"><span>departs <b>${esc(dmy(o.departs))}</b></span><span>transit <b>${o.transit_days != null ? esc(o.transit_days) + ' d' : '—'}</b> <span style="color:${C.faint}">port to port</span></span><span>valid to <b>${esc(dmy(o.valid_until))}</b></span>${o.cutoff ? `<span>cut-off <b>${esc(dmy(o.cutoff))}</b></span>` : ''}</div>
        <div><span class="d2r-pill" style="background:${os[1]};color:${os[2]}">${esc(os[0])}</span></div></div>`; };
    let html = `<div class="d2r-sec"><h2>${esc(r.period_label)} · ${esc(r.ref)}</h2><span class="d2r-pill" style="background:${st[1]};color:${st[2]}">${esc(st[0])}</span><span>${esc((r.origins || []).join(' · ') || '—')} → ${esc(r.destination || 'Sydney')}${r.need_by ? ` · in FC by <b style="color:${C.text}">${esc(dmy(r.need_by))}</b>` : ''} · ${nf(vol.units)} units${vol.expected_units && vol.expected_units !== vol.units ? ` · expected ${nf(vol.expected_units)}` : ''}</span>
      <span style="margin-left:auto;display:flex;gap:8px;">${firm ? `<button class="d2r-btn sm" data-act="approve">Approve</button><button class="d2r-btn sm sec" data-act="decline">Decline</button>` : ''}${canPick ? `<button class="d2r-btn sm" data-act="pick" ${_picks.size ? '' : 'disabled'}>Pick ${_picks.size || ''} &amp; ask VelOzity to confirm</button>` : ''}${r.client_state === 'expired' || (r.client_state === 'requested' && r.source === 'history') ? `<button class="d2r-btn sm" data-act="refresh">Ask VelOzity${r.client_state === 'expired' ? ' to refresh' : ''}</button>` : ''}${!['approved', 'declined'].includes(r.client_state) && !firm ? `<button class="d2r-btn sm sec" data-act="decline">Withdraw</button>` : ''}<button class="d2r-btn sm sec" data-act="delete" title="${r.client_state === 'approved' ? 'Hide from the list — the approved quotes stay' : 'Delete this request'}">${r.client_state === 'approved' ? 'Hide' : 'Delete'}</button></span></div>`;
    if ((vol.supplier_notes || []).length) html += `<div class="d2r-note" style="margin:-4px 0 10px;color:${C.mute}">Accounting for supplier history: ${esc(vol.supplier_notes.join(' · '))}</div>`;
    if (!opts.length) html += `<div class="d2r-empty">${r.client_state === 'requested' ? (r.source === 'history' ? 'Nothing on these lanes in the last six months — Ask VelOzity for fresh options.' : 'VelOzity has asked the carriers. You\'ll get an email when the options are ready.') : 'No options on this request.'}</div>`;
    else {
      html += `<div class="d2r-kpis"><div class="d2r-kpi"><small>Options</small><b>${live.length}</b><i>${sea.length} sea · ${air.length} air</i></div>
        <div class="d2r-kpi"><small>Fastest</small><b>${live.some(o => o.transit_days != null) ? Math.min(...live.filter(o => o.transit_days != null).map(o => o.transit_days)) + ' d' : '—'}</b><i>port to port · our stages are added in the simulation</i></div>
        <div class="d2r-kpi"><small>Lowest sea price</small><b>${sea.some(o => o.price != null) ? esc(money(Math.min(...sea.filter(o => o.price != null).map(o => o.price)), sea.find(o => o.price != null).currency)) : '—'}</b><i>per container</i></div>
        <div class="d2r-kpi"><small>In Sydney FC by</small><b>${esc(dmy(r.need_by))}</b><i>${r.need_by ? 'every option is judged against this date' : 'no date set'}</i></div></div>`;
      html += `<div class="d2r-ogrid">${opts.map(card).join('')}</div>
      <div class="d2r-note">${firm ? 'Firm prices carry a validity date. Approving saves these as the period\'s approved quotes and VelOzity instructs the carrier.' : r.client_state === 'picked' ? 'VelOzity is confirming the price for your pick. Approval opens once it is firm.' : 'Prices are ex GST. Approximate prices become firm when VelOzity confirms them for your pick. Pick two options to split the period across them. Simulation boards — days to FC, the chance of making your date, splits — arrive next.'}</div>`;
    }
    return html;
  }

  // ── events ──
  function bind(s) {
    const nb = el('d2r-new'); if (nb) nb.onclick = () => { _showForm = !_showForm; if (_showForm && !_form) { _form = { period_type: 'week', period_start: addDays(mondayOf(todayYmd()), 14), origins: [], origins_set: false, source: 'ask', units: '', need_by: '', destination: '', cartons: '', cbm: '', kg: '', note: '' }; render(); loadPrefill(); } else render(); };
    s.querySelectorAll('[data-pt]').forEach(b => b.onclick = () => { _form.period_type = b.dataset.pt; _form.period_start = _form.period_type === 'month' ? _form.period_start.slice(0, 7) + '-01' : mondayOf(_form.period_start); _form.need_by = ''; _form.units = ''; loadPrefill(); });
    const ps = el('d2r-ps'); if (ps) ps.onchange = () => { const v = ps.value; _form.period_start = _form.period_type === 'month' ? v + '-01' : (mondayOf(v) || _form.period_start); _form.need_by = ''; _form.units = ''; loadPrefill(); };
    const nbb = el('d2r-nb'); if (nbb) nbb.onchange = () => { _form.need_by = nbb.value; };
    const dest = el('d2r-dest'); if (dest) dest.onchange = () => { _form.destination = dest.value; };
    ['ctn', 'cbm', 'kg', 'note', 'units'].forEach(k => { const i = el('d2r-' + k); if (i) i.onchange = () => { _form[k === 'ctn' ? 'cartons' : k] = i.value; }; });
    s.querySelectorAll('[data-origin]').forEach(c => c.onclick = () => { const n = c.dataset.origin; _form.origins_set = true; _form.origins = _form.origins.includes(n) ? _form.origins.filter(x => x !== n) : _form.origins.concat([n]); render(); });
    s.querySelectorAll('[data-map]').forEach(sel => sel.onchange = async () => { if (!sel.value) return; try { await req('/rq/supplier-origin', { method: 'PUT', body: JSON.stringify({ supplier: sel.dataset.map, origin_id: sel.value }) }); toast('Origin saved'); _form.origins_set = false; loadPrefill(); } catch (e) { toast(e.message); } });
    s.querySelectorAll('[data-src]').forEach(w => w.onclick = () => { _form.source = w.dataset.src; render(); });
    const go = el('d2r-go'); if (go) go.onclick = async () => { const f = _form; if (!f.need_by) { toast('Set the date you need it in the Sydney FC'); return; } go.disabled = true; go.textContent = 'Sending…';
      if (!(Number(f.units) || (_pf && _pf.units))) { toast('Enter the units for the period'); return; }
      try { const r = await req('/rq/requests', { method: 'POST', body: JSON.stringify({ period_type: f.period_type, period_start: f.period_start, need_by: f.need_by, origins: f.origins, destination: f.destination, units: f.units || null, cartons: f.cartons || null, cbm: f.cbm || null, kg: f.kg || null, source: f.source, note: (el('d2r-note') || {}).value || null }) });
        toast(r.no_history ? `${r.ref} created — no history on these lanes, ask VelOzity` : (f.source === 'history' ? `${r.ref} · ${r.options_count} option(s) ready` : `${r.ref} sent to VelOzity`)); _showForm = false; _form = null; _pf = null; _open = r.id; await refreshAll(); }
      catch (e) { toast(e.message); go.disabled = false; go.textContent = 'Try again'; } };
    s.querySelectorAll('[data-open]').forEach(c => c.onclick = async () => { _open = _open === c.dataset.open ? null : c.dataset.open; if (_open) { history.replaceState(null, '', '#simulator/' + encodeURIComponent((_list.find(r => r.id === _open) || {}).ref || '')); await loadDetail(_open); pin(); } else { history.replaceState(null, '', '#simulator'); pin(); } render(); });
    s.querySelectorAll('[data-act]').forEach(b => b.onclick = async () => {
      const act = b.dataset.act, id = _open; if (!id) return;
      try {
        if (act === 'pick') { const ids = [..._picks]; if (!ids.length) { toast('Choose the option(s) to pick'); return; } b.disabled = true; const r = await req(`/rq/requests/${encodeURIComponent(id)}/pick`, { method: 'POST', body: JSON.stringify({ option_ids: ids }) }); toast(`Picked · VelOzity is confirming the price${r.confirm_links_sent ? '' : ' by hand'}`); }
        else if (act === 'approve') { if (!confirm('Approve the firm option(s)? This becomes the period\'s approved quote and VelOzity instructs the carrier.')) return; b.disabled = true; await req(`/rq/requests/${encodeURIComponent(id)}/approve`, { method: 'POST', body: '{}' }); toast('Approved'); }
        else if (act === 'decline') { const note = prompt('Decline or withdraw this request — a note for VelOzity (optional):'); if (note === null) return; b.disabled = true; await req(`/rq/requests/${encodeURIComponent(id)}/decline`, { method: 'POST', body: JSON.stringify({ note }) }); toast('Declined'); }
        else if (act === 'delete') { const r0 = _list.find(x => x.id === id) || {}; if (!confirm(r0.client_state === 'approved' ? `Hide ${r0.ref} from the list? Its approved quotes stay on record.` : `Delete ${r0.ref}? Any open options are withdrawn. This cannot be undone.`)) return; b.disabled = true; await req(`/rq/requests/${encodeURIComponent(id)}`, { method: 'DELETE' }); toast(r0.client_state === 'approved' ? 'Hidden' : 'Deleted'); _open = null; history.replaceState(null, '', '#simulator'); pin(); }
        else if (act === 'refresh') { b.disabled = true; const r = await req(`/rq/requests/${encodeURIComponent(id)}/refresh`, { method: 'POST', body: '{}' }); toast(r.partner_links_sent ? 'VelOzity asked for fresh options' : 'Request re-opened — no carrier link could be sent'); }
        await refreshAll();
      } catch (e) { toast(e.message); b.disabled = false; }
    });
    s.querySelectorAll('.d2r-opt[data-pick]').forEach(c => c.onclick = () => { const id = c.dataset.pick; if (_picks.has(id)) _picks.delete(id); else _picks.add(id); render(); });
  }
  function pin() { const r = _open && (_list || []).find(x => x.id === _open); if (window._pulseRail && typeof window._pulseRail.setPin === 'function') window._pulseRail.setPin(r ? { page: 'simulator', label: `${r.ref} · ${r.period_label}`, ids: [], ref: r.ref } : null); }

  // ── boot ──
  const boot = () => { styles(); injectNav(); section(); wrapRouter(); if (/^#simulator/.test(String(location.hash || ''))) setTimeout(() => { try { window.show(location.hash); } catch (_) {} }, 120); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);
  new MutationObserver(() => { try { injectNav(); } catch (_) {} }).observe(document.documentElement, { childList: true, subtree: true });
  window.addEventListener('tenancy:ready', () => { _list = null; _open = null; _detail = null; _form = null; _pf = null; if (_isOpen) open(true).catch(() => {}); });
  window.renderSimulator = () => open(true); window.__d2dReqBuild = BUILD;
  console.log('[simulator] build ' + BUILD + ' loaded');
})();
