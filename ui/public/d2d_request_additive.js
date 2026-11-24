/* ── VelOzity Pinpoint — Simulator · requests v1 (Phase 5b) ──
   The client side of door-to-door options: raise a request (where from, where to, when, how
   much), see the options VelOzity releases, pick, and approve once the price is firm. The
   simulation boards (5d) replace the options table on this page; the request and the states
   stay as they are here.

   Internal admins see, on the same page, the review panel for each request: partner cost,
   margin, pricing by markup or by price, release, confirm by hand, partner links. That panel
   reads /rq/internal/* only — the client endpoints never carry cost or partner.

   Wording on client surfaces: Price, Ask VelOzity. Never sell, never partner. */
(function () {
  if (window.__d2dReqLoaded) return; window.__d2dReqLoaded = true;
  const BUILD = 'd2dreq-1';
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
  const C = { text: '#1C1C1E', mute: '#6E6E73', dim: '#8E8E93', faint: '#AEAEB2', wine: '#990033', mint: '#2E7D4F', sky: '#2F6FED', amber: '#C77A00', good: '#4A6A00', goodBg: '#EEF6D6', warnBg: '#FFF4D6', badBg: '#FBE9EE', tint: '#F2F2F0', line: 'rgba(0,0,0,.08)' };
  const STATE = { requested: ['Asked', C.tint, C.mute], with_velozity: ['With VelOzity', C.warnBg, C.amber], options_ready: ['Options ready', '#E6F0FF', C.sky], picked: ['Picked · confirming', C.warnBg, C.amber], firm: ['Firm · approve', C.goodBg, C.good], approved: ['Approved', C.goodBg, C.good], declined: ['Declined', C.tint, C.mute], expired: ['Expired', C.badBg, C.wine] };
  const ISTATE = { requested: 'Requested', options_offered: 'Options in · price & release', released: 'Released to client', picked: 'Picked · confirm cost', cost_confirmed: 'Cost confirmed · release firm', firm_released: 'Firm · awaiting approval', approved: 'Approved', declined: 'Declined', expired: 'Expired' };
  const OSTATE = { approximate: ['approximate', C.tint, C.mute], picked: ['picked', C.warnBg, C.amber], confirmed: ['confirmed', C.warnBg, C.amber], firm: ['firm', C.goodBg, C.good], approved: ['approved', C.goodBg, C.good], expired: ['expired', C.badBg, C.wine], withdrawn: ['withdrawn', C.tint, C.faint] };

  let _list = null, _open = null, _detail = null, _internal = null, _pf = null, _form = null, _isOpen = false, _err = null, _isAdmin = false, _showForm = false;

  function styles() {
    if (el('d2r-css')) return; const s = document.createElement('style'); s.id = 'd2r-css';
    s.textContent = `
      #page-simulator{padding:0;} .d2r{color:${C.text};padding:4px 0 28px;} .d2r *{box-sizing:border-box;}
      .d2r-eyebrow{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:${C.mute};font-weight:600;}
      .d2r-h1{font-size:24px;font-weight:700;letter-spacing:-.02em;margin:4px 0 0;} .d2r-sub{font-size:12.5px;color:${C.mute};margin-top:6px;max-width:760px;}
      .d2r-head{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap;margin-bottom:16px;}
      .d2r-btn{border:0;border-radius:999px;padding:9px 16px;font:600 12.5px inherit;cursor:pointer;background:${C.wine};color:#fff;} .d2r-btn.sec{background:#fff;color:${C.text};border:0.5px solid rgba(0,0,0,.18);} .d2r-btn.sm{padding:6px 12px;font-size:11.5px;} .d2r-btn[disabled]{opacity:.5;cursor:default;}
      .d2r-pill{display:inline-block;font-size:9.5px;letter-spacing:.06em;text-transform:uppercase;font-weight:700;border-radius:999px;padding:2px 8px;}
      .d2r-card{background:#fff;border:0.5px solid ${C.line};border-radius:14px;padding:14px 16px;} .d2r-card + .d2r-card{margin-top:10px;}
      .d2r-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:10px;}
      .d2r-rq{cursor:pointer;transition:border-color .15s,box-shadow .15s;} .d2r-rq:hover{border-color:rgba(153,0,51,.45);box-shadow:0 2px 10px rgba(0,0,0,.05);} .d2r-rq.on{border:1.5px solid ${C.wine};padding:13px 15px;}
      .d2r-rq .t{display:flex;gap:8px;align-items:center;font-size:13px;font-weight:700;} .d2r-rq .m{font-size:11.5px;color:${C.mute};margin-top:6px;line-height:1.5;}
      .d2r-sec{display:flex;align-items:baseline;gap:12px;margin:22px 0 10px;flex-wrap:wrap;} .d2r-sec h2{font-size:14px;font-weight:700;margin:0;} .d2r-sec span{font-size:11.5px;color:${C.mute};}
      .d2r-form{display:grid;grid-template-columns:1fr 1fr;gap:12px;} @media (max-width:900px){.d2r-form{grid-template-columns:1fr;}}
      .d2r-f label{display:block;font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:${C.mute};font-weight:600;margin-bottom:5px;}
      .d2r-f .in{min-height:42px;border:0.5px solid rgba(0,0,0,.16);border-radius:11px;background:#fff;display:flex;align-items:center;padding:0 12px;font-size:13.5px;gap:8px;flex-wrap:wrap;}
      .d2r-f .in input,.d2r-f .in select{border:0;background:none;font:inherit;font-size:13.5px;outline:none;min-width:0;flex:1;} .d2r-f .in input[type=number]{text-align:right;max-width:110px;}
      .d2r-f .in .m{color:${C.dim};font-size:11.5px;margin-left:auto;}
      .d2r-seg{display:inline-flex;border:0.5px solid rgba(0,0,0,.14);border-radius:999px;padding:2px;background:#fff;} .d2r-seg button{background:none;border:0;color:${C.mute};font-size:11px;padding:4px 12px;border-radius:999px;cursor:pointer;font-family:inherit;} .d2r-seg button.on{background:${C.wine};color:#fff;}
      .d2r-chip{display:inline-flex;align-items:center;gap:6px;border:0.5px solid rgba(0,0,0,.14);border-radius:999px;padding:4px 10px;font-size:12px;background:#fff;cursor:pointer;} .d2r-chip.on{border-color:${C.wine};background:#FBE9EE;color:${C.wine};}
      .d2r-way{border:0.5px solid rgba(0,0,0,.12);border-radius:16px;padding:16px 18px;background:#fff;cursor:pointer;position:relative;} .d2r-way:hover{border-color:rgba(153,0,51,.5);} .d2r-way.on{border:1.5px solid ${C.wine};background:#FFF8FA;}
      .d2r-way h3{margin:0;font-size:14px;font-weight:700;} .d2r-way p{margin:6px 0 0;font-size:12px;color:${C.mute};line-height:1.5;} .d2r-way .when{font-size:11px;color:${C.dim};margin-top:8px;}
      .d2r-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;} @media (max-width:1100px){.d2r-kpis{grid-template-columns:repeat(2,minmax(0,1fr));}}
      .d2r-kpi{background:#fff;border:0.5px solid ${C.line};border-radius:14px;padding:12px 14px;} .d2r-kpi small{display:block;font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:${C.mute};font-weight:600;} .d2r-kpi b{display:block;font-size:20px;font-weight:700;margin-top:3px;letter-spacing:-.02em;} .d2r-kpi i{display:block;font-style:normal;font-size:11px;color:${C.mute};margin-top:2px;}
      .d2r-opts{width:100%;border-collapse:separate;border-spacing:0;font-size:12.5px;background:#fff;border:0.5px solid ${C.line};border-radius:14px;overflow:hidden;font-variant-numeric:tabular-nums;}
      .d2r-opts th{font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;color:${C.mute};font-weight:600;text-align:left;padding:9px 11px;border-bottom:0.5px solid ${C.line};background:#FAFAF8;white-space:nowrap;}
      .d2r-opts td{padding:9px 11px;border-bottom:0.5px solid rgba(0,0,0,.05);vertical-align:middle;} .d2r-opts tr.dead td{color:${C.faint};} .d2r-opts .r{text-align:right;} .d2r-opts tr.pick td{background:#FFF8FA;}
      .d2r-mode{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;border-radius:6px;padding:2px 7px;} .d2r-mode.sea{background:#E3F2EA;color:${C.mint};} .d2r-mode.air{background:#E6F0FF;color:${C.sky};}
      .d2r-opts input{width:84px;border:0.5px solid rgba(0,0,0,.18);border-radius:7px;padding:4px 6px;font:inherit;font-size:12px;text-align:right;} .d2r-opts input[type=date]{width:130px;text-align:left;}
      .d2r-int{margin-top:12px;border:1px dashed rgba(153,0,51,.35);border-radius:14px;padding:12px 14px;background:#FFFBFC;} .d2r-int .t{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:${C.wine};font-weight:700;display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
      .d2r-ev{font-size:11px;color:${C.mute};line-height:1.6;max-height:160px;overflow:auto;margin-top:8px;}
      .d2r-note{font-size:11px;color:${C.dim};margin-top:8px;line-height:1.5;}
      .d2r-empty{background:#fff;border:0.5px solid ${C.line};border-radius:14px;padding:26px;text-align:center;color:${C.mute};font-size:13px;}
      .d2r-toast{position:fixed;left:50%;bottom:26px;transform:translateX(-50%);background:${C.text};color:#fff;padding:10px 16px;border-radius:999px;font-size:12.5px;z-index:9999;opacity:0;transition:opacity .2s;pointer-events:none;} .d2r-toast.on{opacity:1;}
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
    styles(); _err = null; _isAdmin = (window._pnRoleKey || '') === 'org:admin_auth';
    if (!_list || force) { render(); try { _list = (await req('/rq/requests')).requests || []; } catch (e) { _err = e; _list = []; } }
    if (ref) { const x = _list.find(r => r.ref === ref); if (x) _open = x.id; }
    if (_open) await loadDetail(_open);
    render();
  }
  async function loadDetail(id) {
    try { _detail = await req('/rq/requests/' + encodeURIComponent(id)); } catch (e) { _detail = null; }
    _internal = null; if (_isAdmin) { try { _internal = await req('/rq/internal/requests/' + encodeURIComponent(id)); } catch (e) { _internal = null; } }
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
      <div class="d2r-head"><div><div class="d2r-eyebrow">Door to door · simulator</div><h1 class="d2r-h1">Simulator</h1>
        <div class="d2r-sub">Ask for options for a week or a month, compare them on price and days against the date you need stock in the Sydney FC, pick, and approve once the price is firm. Simulation boards land here next; the options table does the comparing for now.</div></div>
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
    return `<div class="d2r-card" style="margin-bottom:16px;padding:18px 20px;">
      <div style="font-size:15px;font-weight:700;letter-spacing:-.01em;">Where from, where to, and when.</div><div class="d2r-note" style="margin-top:2px;">Everything else is pre-filled from your plan and can be changed.</div>
      <div class="d2r-form" style="margin-top:14px;">
        <div class="d2r-f"><label>Period</label><div class="in"><span class="d2r-seg"><button data-pt="week" class="${f.period_type === 'week' ? 'on' : ''}">Week</button><button data-pt="month" class="${f.period_type === 'month' ? 'on' : ''}">Month</button></span>
          ${f.period_type === 'week' ? `<input type="date" id="d2r-ps" value="${esc(f.period_start)}">` : `<input type="month" id="d2r-ps" value="${esc(f.period_start.slice(0, 7))}">`}<span class="m">${pf.period_label ? esc(pf.period_label) + ' · ' + nf(pf.pos) + ' POs in plan' : ''}</span></div></div>
        <div class="d2r-f"><label>In Sydney FC by</label><div class="in"><input type="date" id="d2r-nb" value="${esc(f.need_by || '')}"><span class="m">the date every option is judged against</span></div></div>
        <div class="d2r-f"><label>From</label><div class="in" style="padding:6px 12px;">${originList.length ? originList.map(o => `<span class="d2r-chip${f.origins.includes(o.name) ? ' on' : ''}" data-origin="${esc(o.name)}">${esc(o.name)}</span>`).join('') : `<span style="color:${C.dim};font-size:12px">No origins set up yet — ask VelOzity to add your origin ports.</span>`}${pf.origins && pf.origins.length ? `<span class="m">${esc(pf.origins.length)} from the plan</span>` : ''}</div>
          ${unmapped.length && originList.length ? `<div class="d2r-note">Suppliers without an origin: ${unmapped.map(u => `<span style="white-space:nowrap">${esc(u)} → <select data-map="${esc(u)}" style="font:inherit;font-size:11px;border:0.5px solid rgba(0,0,0,.18);border-radius:6px;padding:2px 4px;"><option value="">pick</option>${originList.map(o => `<option value="${esc(o.id)}">${esc(o.name)}</option>`).join('')}</select></span>`).join(' · ')}</div>` : ''}</div>
        <div class="d2r-f"><label>To</label><div class="in"><input id="d2r-dest" value="${esc(f.destination || '')}" placeholder="Sydney · VOZ_KY"><span class="m">facility</span></div></div>
        <div class="d2r-f" style="grid-column:1 / -1;"><label>Volume</label><div class="in"><b>${nf(pf.units)} units</b><span style="color:${C.dim};font-size:12px">${pf.expected_units != null && pf.expected_units !== pf.units ? `expected ${nf(pf.expected_units)} after supplier history` : ''}</span>
          <span style="margin-left:auto;display:flex;gap:10px;align-items:center;font-size:12px;color:${C.mute}">cartons <input type="number" id="d2r-ctn" min="0" value="${esc(f.cartons || '')}" placeholder="—"> CBM <input type="number" id="d2r-cbm" min="0" step="0.1" value="${esc(f.cbm || '')}" placeholder="—"> kg <input type="number" id="d2r-kg" min="0" value="${esc(f.kg || '')}" placeholder="—"></span></div>
          ${(pf.suppliers || []).length ? `<div class="d2r-note">${pf.suppliers.slice(0, 6).map(s => `${esc(s.supplier)} ${nf(s.units)}${s.signal && s.signal.late_days_p50 ? ` <span style="color:${C.wine}">${esc(s.signal.late_days_p50)} d late, ${Math.round(s.signal.fill_rate)}% of plan</span>` : ''}`).join(' · ')}${pf.suppliers.length > 6 ? ` · +${pf.suppliers.length - 6} more` : ''}</div>` : ''}</div>
      </div>
      <div style="display:flex;gap:12px;margin-top:14px;">
        <div class="d2r-way${f.source === 'history' ? ' on' : ''}" data-src="history"><span class="d2r-pill" style="position:absolute;top:12px;right:12px;background:${C.tint};color:${C.mute}">instant</span><h3>Use what we know</h3><p>Options we've shipped or quoted on these lanes in the last six months, priced at the average approved rate and timed on what the lanes actually achieved.</p><div class="when">Ready now · labelled <i>from history</i></div></div>
        <div class="d2r-way${f.source === 'ask' ? ' on' : ''}" data-src="ask"><h3>Ask VelOzity</h3><p>Fresh carrier options for this period — sailings, flights, transit and price — checked by VelOzity before you see them. Upgrade a history run to this any time.</p><div class="when">Usually within a business day · labelled <i>approximate</i>, with a validity date</div></div>
      </div>
      <div style="display:flex;gap:12px;align-items:center;margin-top:14px;"><button class="d2r-btn" id="d2r-go" ${pf.units == null ? 'disabled' : ''}>${f.source === 'history' ? 'Build the options' : 'Ask VelOzity'}</button>
        <input id="d2r-note" placeholder="Note for VelOzity (optional)" style="flex:1;border:0.5px solid rgba(0,0,0,.16);border-radius:999px;padding:8px 14px;font:inherit;font-size:12.5px;" value="${esc(f.note || '')}">
        ${(pf.supplier_notes || []).length ? `<span class="d2r-note" style="margin:0">Accounting for: ${esc(pf.supplier_notes.join(' · '))}</span>` : ''}</div></div>`;
  }
  function detailHtml(r) {
    const st = STATE[r.client_state] || STATE.requested; const vol = r.volume || {}; const opts = (r.options || []).filter(o => o.state !== 'withdrawn');
    const pickable = r.client_state === 'options_ready' || (r.client_state === 'picked'); const firm = r.client_state === 'firm';
    const canPick = pickable && opts.some(o => o.state === 'approximate');
    const row = o => { const os = OSTATE[o.state] || OSTATE.approximate; const dead = ['expired', 'withdrawn'].includes(o.state); const pk = ['picked', 'confirmed', 'firm', 'approved'].includes(o.state);
      return `<tr class="${dead ? 'dead' : ''}${pk ? ' pick' : ''}"><td>${canPick && !dead ? `<input type="checkbox" data-pick="${esc(o.id)}" ${pk ? 'checked' : ''} style="width:auto">` : ''}</td><td><span class="d2r-mode ${o.mode.toLowerCase()}">${esc(o.mode)}</span></td><td>${esc([o.carrier, o.service].filter(Boolean).join(' · ') || (o.kind === 'history' ? 'From history' : 'Option'))}${o.kind === 'history' ? `<div style="font-size:10.5px;color:${C.dim}">from history</div>` : ''}</td><td>${o.container_size ? esc(o.container_size) + "'" : '—'}</td><td>${esc(dmy(o.departs))}${o.cutoff ? `<div style="font-size:10.5px;color:${C.dim}">cut-off ${esc(dmy(o.cutoff))}</div>` : ''}</td><td class="r">${o.transit_days != null ? esc(o.transit_days) + ' d' : '—'}</td><td>${esc(dmy(o.valid_until))}</td>
        <td class="r"><b>${o.price != null ? esc(money(o.price, o.currency)) : (r.client_state === 'with_velozity' || r.client_state === 'requested' ? '<span style="color:' + C.dim + ';font-weight:400">pricing</span>' : '—')}</b>${o.price != null ? `<div style="font-size:10.5px;color:${C.dim}">${o.price_basis === 'per_container' ? 'per container' : o.price_basis === 'per_kg' ? 'per kg' : 'total'}${o.firm ? '' : ' · approximate'}</div>` : ''}</td><td><span class="d2r-pill" style="background:${os[1]};color:${os[2]}">${esc(os[0])}</span></td></tr>`; };
    let html = `<div class="d2r-sec"><h2>${esc(r.period_label)} · ${esc(r.ref)}</h2><span class="d2r-pill" style="background:${st[1]};color:${st[2]}">${esc(st[0])}</span><span>${esc((r.origins || []).join(' · ') || '—')} → ${esc(r.destination || 'Sydney')}${r.need_by ? ` · in FC by ${esc(dmy(r.need_by))}` : ''} · ${nf(vol.units)} units</span>
      <span style="margin-left:auto;display:flex;gap:8px;">${firm ? `<button class="d2r-btn sm" data-act="approve">Approve</button><button class="d2r-btn sm sec" data-act="decline">Decline</button>` : ''}${canPick ? `<button class="d2r-btn sm" data-act="pick">Pick & ask VelOzity to confirm</button>` : ''}${r.client_state === 'expired' || (r.client_state === 'requested' && r.source === 'history') ? `<button class="d2r-btn sm" data-act="refresh">Ask VelOzity${r.client_state === 'expired' ? ' to refresh' : ''}</button>` : ''}${!['approved', 'declined'].includes(r.client_state) && !firm ? `<button class="d2r-btn sm sec" data-act="decline">Withdraw</button>` : ''}</span></div>`;
    if ((vol.supplier_notes || []).length) html += `<div class="d2r-note" style="margin:-4px 0 8px">Accounting for supplier history: ${esc(vol.supplier_notes.join(' · '))}</div>`;
    if (!opts.length) html += `<div class="d2r-empty">${r.client_state === 'requested' ? (r.source === 'history' ? 'Nothing on these lanes in the last six months — Ask VelOzity for fresh options.' : 'VelOzity has asked the carriers. You\'ll get an email when the options are ready.') : 'No options on this request.'}</div>`;
    else html += `<table class="d2r-opts"><tr><th></th><th>Mode</th><th>Option</th><th>Container</th><th>Departs</th><th class="r">Transit</th><th>Valid to</th><th class="r">Price</th><th>State</th></tr>${opts.map(row).join('')}</table>
      <div class="d2r-note">${firm ? 'Firm prices carry a validity date. Approving saves these as the period\'s approved quotes and VelOzity instructs the carrier.' : r.client_state === 'picked' ? 'VelOzity is confirming the price for your pick. Approval opens once it is firm.' : 'Prices are ex GST. Approximate prices become firm when VelOzity confirms them for your pick. Pick two options to split the period across them.'}</div>`;
    if (_internal && _internal.id === r.id) html += internalHtml(_internal);
    return html;
  }
  function internalHtml(x) {
    const opts = x.options || []; const mk = x.markup || {}; const can = { release: ['requested', 'options_offered', 'released'].includes(x.state) && opts.some(o => o.state === 'approximate'), firm: ['picked', 'cost_confirmed'].includes(x.state) };
    const row = o => { const dead = ['expired', 'withdrawn', 'approved'].includes(o.state); const cost = o.confirmed_cost != null ? o.confirmed_cost : o.cost;
      return `<tr class="${dead ? 'dead' : ''}"><td><span class="d2r-mode ${o.mode.toLowerCase()}">${esc(o.mode)}</span></td><td>${esc([o.carrier, o.service].filter(Boolean).join(' · ') || (o.kind === 'history' ? 'From history' : '—'))}<div style="font-size:10.5px;color:${C.dim}">${esc(o.partner_name || (o.kind === 'history' ? 'no partner yet' : '—'))}${o.validity_days != null && o.validity_days < 3 && !dead ? ` · <span style="color:${C.amber};font-weight:700">valid ${o.validity_days} d</span>` : ''}</div></td>
        <td class="r">${cost != null ? esc(money(cost, o.cost_currency)) : '—'}${o.confirmed_cost != null ? `<div style="font-size:10.5px;color:${C.good}">confirmed${o.confirmed_cost !== o.cost ? ' · was ' + esc(money(o.cost, o.cost_currency)) : ''}</div>` : (o.local_charges ? `<div style="font-size:10.5px;color:${C.dim}">incl. ${esc(money(o.local_charges, o.cost_currency))} local</div>` : '')}</td>
        <td class="r">${dead ? (o.markup_pct != null ? esc(o.markup_pct) + '%' : '—') : `<input type="number" step="0.5" min="0" data-mk="${esc(o.id)}" value="${o.markup_pct != null ? esc(o.markup_pct) : ''}" placeholder="${esc(mk[o.mode] != null ? mk[o.mode] : '')}">%`}</td>
        <td class="r">${dead ? (o.price != null ? esc(money(o.price, o.currency)) : '—') : `<input type="number" step="1" min="0" data-px="${esc(o.id)}" value="${o.price != null ? esc(o.price) : ''}" placeholder="price">`}</td>
        <td class="r" style="color:${o.margin != null && o.margin < 0 ? C.wine : C.text}">${o.margin != null ? esc(money(o.margin, o.currency)) + ` <span style="color:${C.dim}">${esc(o.margin_pct)}%</span>` : '—'}</td>
        <td><span class="d2r-pill" style="background:${(OSTATE[o.state] || OSTATE.approximate)[1]};color:${(OSTATE[o.state] || OSTATE.approximate)[2]}">${esc(o.state)}</span></td>
        <td style="white-space:nowrap">${o.state === 'picked' ? `<button class="d2r-btn sm sec" data-iact="confirm" data-oid="${esc(o.id)}">Confirm cost</button> ` : ''}${!dead ? `<button class="d2r-btn sm sec" data-iact="withdraw" data-oid="${esc(o.id)}">Withdraw</button>` : ''}</td></tr>`; };
    const links = (x.links || []).slice(0, 6).map(l => `${esc(l.partner_name || 'partner')} · ${l.purpose === 'partner_options' ? 'options' : 'confirm'} link ${l.used_at ? 'used ' + esc(dmy(l.used_at)) : 'sent ' + esc(dmy(l.created_at))}`).join(' · ');
    return `<div class="d2r-int"><div class="t">Internal · ${esc(ISTATE[x.state] || x.state)}<span style="margin-left:auto;display:flex;gap:6px;font-weight:400;text-transform:none;letter-spacing:0;">
        <button class="d2r-btn sm sec" data-iact="rfq">Re-send carrier links</button><button class="d2r-btn sm sec" data-iact="add">Add option by hand</button>
        ${can.release ? `<button class="d2r-btn sm" data-iact="release">Release to client</button>` : ''}${can.firm ? `<button class="d2r-btn sm" data-iact="release">Release firm price</button>` : ''}</span></div>
      <div class="d2r-note" style="margin-top:4px">Partner cost and margin — never shown to the client. Markup defaults: Air ${esc(mk.Air)}% · Sea ${esc(mk.Sea)}% (Finance › Rates). Type a markup or a price; the other follows. ${x.state === 'picked' ? 'Confirm by hand if the carrier phoned it in.' : ''}</div>
      ${opts.length ? `<table class="d2r-opts" style="margin-top:8px"><tr><th>Mode</th><th>Option · partner</th><th class="r">Cost</th><th class="r">Markup</th><th class="r">Price</th><th class="r">Margin</th><th>State</th><th></th></tr>${opts.map(row).join('')}</table>` : `<div class="d2r-note">No options yet. ${links ? '' : 'No carrier partner is configured for this client — add one under Finance › Quotes.'}</div>`}
      ${links ? `<div class="d2r-note">${links}</div>` : ''}
      <div class="d2r-ev">${(x.events || []).slice(-12).reverse().map(e => `${esc(String(e.created_at).slice(0, 16).replace('T', ' '))} · ${esc(e.actor || 'system')} · ${esc(e.to_state ? '→ ' + e.to_state + (e.detail ? ' · ' + e.detail : '') : e.detail || '')}`).join('<br>')}</div></div>`;
  }

  // ── events ──
  function bind(s) {
    const nb = el('d2r-new'); if (nb) nb.onclick = () => { _showForm = !_showForm; if (_showForm && !_form) { _form = { period_type: 'week', period_start: addDays(mondayOf(todayYmd()), 14), origins: [], origins_set: false, source: 'ask', need_by: '', destination: '', cartons: '', cbm: '', kg: '', note: '' }; render(); loadPrefill(); } else render(); };
    s.querySelectorAll('[data-pt]').forEach(b => b.onclick = () => { _form.period_type = b.dataset.pt; _form.period_start = _form.period_type === 'month' ? _form.period_start.slice(0, 7) + '-01' : mondayOf(_form.period_start); _form.need_by = ''; loadPrefill(); });
    const ps = el('d2r-ps'); if (ps) ps.onchange = () => { const v = ps.value; _form.period_start = _form.period_type === 'month' ? v + '-01' : (mondayOf(v) || _form.period_start); _form.need_by = ''; loadPrefill(); };
    const nbb = el('d2r-nb'); if (nbb) nbb.onchange = () => { _form.need_by = nbb.value; };
    const dest = el('d2r-dest'); if (dest) dest.onchange = () => { _form.destination = dest.value; };
    ['ctn', 'cbm', 'kg', 'note'].forEach(k => { const i = el('d2r-' + k); if (i) i.onchange = () => { _form[k === 'ctn' ? 'cartons' : k] = i.value; }; });
    s.querySelectorAll('[data-origin]').forEach(c => c.onclick = () => { const n = c.dataset.origin; _form.origins_set = true; _form.origins = _form.origins.includes(n) ? _form.origins.filter(x => x !== n) : _form.origins.concat([n]); render(); });
    s.querySelectorAll('[data-map]').forEach(sel => sel.onchange = async () => { if (!sel.value) return; try { await req('/rq/supplier-origin', { method: 'PUT', body: JSON.stringify({ supplier: sel.dataset.map, origin_id: sel.value }) }); toast('Origin saved'); _form.origins_set = false; loadPrefill(); } catch (e) { toast(e.message); } });
    s.querySelectorAll('[data-src]').forEach(w => w.onclick = () => { _form.source = w.dataset.src; render(); });
    const go = el('d2r-go'); if (go) go.onclick = async () => { const f = _form; if (!f.need_by) { toast('Set the date you need it in the Sydney FC'); return; } go.disabled = true; go.textContent = 'Sending…';
      try { const r = await req('/rq/requests', { method: 'POST', body: JSON.stringify({ period_type: f.period_type, period_start: f.period_start, need_by: f.need_by, origins: f.origins, destination: f.destination, cartons: f.cartons || null, cbm: f.cbm || null, kg: f.kg || null, source: f.source, note: (el('d2r-note') || {}).value || null }) });
        toast(r.no_history ? `${r.ref} created — no history on these lanes, ask VelOzity` : (f.source === 'history' ? `${r.ref} · ${r.options_count} option(s) ready` : `${r.ref} sent to VelOzity`)); _showForm = false; _form = null; _pf = null; _open = r.id; await refreshAll(); }
      catch (e) { toast(e.message); go.disabled = false; go.textContent = 'Try again'; } };
    s.querySelectorAll('[data-open]').forEach(c => c.onclick = async () => { _open = _open === c.dataset.open ? null : c.dataset.open; if (_open) { history.replaceState(null, '', '#simulator/' + encodeURIComponent((_list.find(r => r.id === _open) || {}).ref || '')); await loadDetail(_open); pin(); } else { history.replaceState(null, '', '#simulator'); pin(); } render(); });
    s.querySelectorAll('[data-act]').forEach(b => b.onclick = async () => {
      const act = b.dataset.act, id = _open; if (!id) return;
      try {
        if (act === 'pick') { const ids = [...s.querySelectorAll('[data-pick]:checked')].map(i => i.dataset.pick); if (!ids.length) { toast('Tick the option(s) to pick'); return; } b.disabled = true; const r = await req(`/rq/requests/${encodeURIComponent(id)}/pick`, { method: 'POST', body: JSON.stringify({ option_ids: ids }) }); toast(`Picked · VelOzity is confirming the price${r.confirm_links_sent ? '' : ' by hand'}`); }
        else if (act === 'approve') { if (!confirm('Approve the firm option(s)? This becomes the period\'s approved quote and VelOzity instructs the carrier.')) return; b.disabled = true; await req(`/rq/requests/${encodeURIComponent(id)}/approve`, { method: 'POST', body: '{}' }); toast('Approved'); }
        else if (act === 'decline') { const note = prompt('Decline or withdraw this request — a note for VelOzity (optional):'); if (note === null) return; b.disabled = true; await req(`/rq/requests/${encodeURIComponent(id)}/decline`, { method: 'POST', body: JSON.stringify({ note }) }); toast('Declined'); }
        else if (act === 'refresh') { b.disabled = true; const r = await req(`/rq/requests/${encodeURIComponent(id)}/refresh`, { method: 'POST', body: '{}' }); toast(r.partner_links_sent ? 'VelOzity asked for fresh options' : 'Request re-opened — no carrier link could be sent'); }
        await refreshAll();
      } catch (e) { toast(e.message); b.disabled = false; }
    });
    // internal
    s.querySelectorAll('[data-mk]').forEach(i => i.onchange = () => price(i.dataset.mk, { markup_pct: Number(i.value) }));
    s.querySelectorAll('[data-px]').forEach(i => i.onchange = () => price(i.dataset.px, { price: Number(i.value) }));
    s.querySelectorAll('[data-iact]').forEach(b => b.onclick = async () => {
      const act = b.dataset.iact, id = _open, oid = b.dataset.oid;
      try {
        if (act === 'release') { if (!confirm('Release to the client? They get an email with the options.')) return; b.disabled = true; await req(`/rq/internal/requests/${encodeURIComponent(id)}/release`, { method: 'POST', body: '{}' }); toast('Released'); }
        else if (act === 'rfq') { const note = prompt('Note to the carrier partners (optional):'); if (note === null) return; const r = await req(`/rq/internal/requests/${encodeURIComponent(id)}/rfq`, { method: 'POST', body: JSON.stringify({ note }) }); toast(r.sent.length ? `Links sent to ${r.sent.map(x => x.partner).join(', ')}` : 'No carrier partner configured — Finance › Quotes'); }
        else if (act === 'withdraw') { if (!confirm('Withdraw this option?')) return; await req(`/rq/internal/requests/${encodeURIComponent(id)}/options/${encodeURIComponent(oid)}/withdraw`, { method: 'POST', body: '{}' }); toast('Withdrawn'); }
        else if (act === 'confirm') { const c = prompt('Confirmed freight cost (partner currency):'); if (c === null || c === '') return; const l = prompt('Local charges (0 if none):', '0'); if (l === null) return; await req(`/rq/internal/requests/${encodeURIComponent(id)}/options/${encodeURIComponent(oid)}/confirm`, { method: 'POST', body: JSON.stringify({ cost_amount: Number(c), local_charges: Number(l) || 0, note: 'confirmed by hand' }) }); toast('Cost confirmed'); }
        else if (act === 'add') { const mode = prompt('Mode: Sea or Air', 'Sea'); if (!mode) return; const carrier = prompt('Carrier'); if (carrier === null) return; const service = prompt('Service / loop'); if (service === null) return; const cs = /air/i.test(mode) ? null : prompt("Container size: 20, 40 or 40HC", '40'); const dep = prompt('Departs (YYYY-MM-DD)', ''); const tr = prompt('Transit days', ''); const cost = prompt(/air/i.test(mode) ? 'Cost per chargeable kg' : 'Cost per container (freight)', ''); if (cost === null) return; const loc = prompt('Local charges', '0'); const vu = prompt('Valid to (YYYY-MM-DD)', addDays(todayYmd(), 7));
          await req(`/rq/internal/requests/${encodeURIComponent(id)}/options`, { method: 'POST', body: JSON.stringify({ mode, carrier, service, container_size: cs, departs: dep || null, transit_days: tr === '' ? null : Number(tr), cost_amount: Number(cost), local_charges: Number(loc) || 0, valid_until: vu || null }) }); toast('Option added'); }
        await refreshAll();
      } catch (e) { toast(e.message); b.disabled = false; }
    });
  }
  async function price(oid, body) { try { await req(`/rq/internal/requests/${encodeURIComponent(_open)}/options/${encodeURIComponent(oid)}/price`, { method: 'POST', body: JSON.stringify(body) }); await refreshAll(); } catch (e) { toast(e.message); } }
  function pin() { const r = _open && (_list || []).find(x => x.id === _open); if (window._pulseRail && typeof window._pulseRail.setPin === 'function') window._pulseRail.setPin(r ? { page: 'simulator', label: `${r.ref} · ${r.period_label}`, ids: [], ref: r.ref } : null); }

  // ── boot ──
  const boot = () => { styles(); injectNav(); section(); wrapRouter(); if (/^#simulator/.test(String(location.hash || ''))) setTimeout(() => { try { window.show(location.hash); } catch (_) {} }, 120); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);
  new MutationObserver(() => { try { injectNav(); } catch (_) {} }).observe(document.documentElement, { childList: true, subtree: true });
  window.addEventListener('tenancy:ready', () => { _list = null; _open = null; _detail = null; _form = null; _pf = null; if (_isOpen) open(true).catch(() => {}); });
  window.renderSimulator = () => open(true); window.__d2dReqBuild = BUILD;
  console.log('[simulator] build ' + BUILD + ' loaded');
})();
