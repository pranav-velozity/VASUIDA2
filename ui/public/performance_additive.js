/* ── VelOzity Pinpoint — Performance v4 ──
   v4: 'Supplier performance', opened from Reports & Downloads rather than the top nav.
   v3: no volume & price block — this page is for clients too, and nothing here touches cost or margin.
   v2: Pinpoint's own light look (only the simulator is dark); delivery accuracy (over / under
   against plan, tolerance per client) and compliance from the Exception log on every card.
   The signals layer, made visible. Everything on this page is read from GET /signals/overview,
   which the nightly job fills from Pinpoint's own records (plans, receiving, transit milestones,
   invoices, approved quotes). Nothing here is estimated or modelled; the page shows what was
   measured, with the sample behind each number.

   Additive: injects a nav item after Transit Movements, owns #performance, and wraps
   window.show the same way the Shipments hub does. The server strips cost and margin for
   client orgs, and this page shows neither — nothing on it is cost- or margin-derived.

   Pulse: the rail gets a 'performance' page and a supplier pin; the chips at the top fire the
   same question a person would type, so answers come through get_signals, never arithmetic. */
(function () {
  if (window.__perfLoaded) return; window.__perfLoaded = true;
  const BUILD = 'perf-4';
  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const nf = n => n == null ? '—' : Number(n).toLocaleString();
  const d1 = n => n == null ? '—' : (Math.round(Number(n) * 10) / 10).toLocaleString(undefined, { maximumFractionDigits: 1 });
  const pct = n => n == null ? '—' : Math.round(Number(n)) + '%';
  const money = n => n == null ? '—' : '$' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');
  async function tok() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts) {
    const t = await tok(), o = opts || {};
    const headers = { 'Content-Type': 'application/json', ...(o.headers || {}) };
    if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers });
    const txt = await r.text(); let d = null;
    try { d = txt ? JSON.parse(txt) : null; } catch (e) { d = txt; }
    if (!r.ok) { const e = new Error((d && d.error) || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return d;
  }
  const wk = (ws) => { const d = new Date(ws + 'T00:00:00Z'); if (isNaN(d)) return ws; const x = new Date(d); x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7) + 3); const f = new Date(Date.UTC(x.getUTCFullYear(), 0, 4)); return 'W' + (1 + Math.round(((x - f) / 86400000 - 3 + ((f.getUTCDay() + 6) % 7)) / 7)); };
  const dmy = (iso) => { if (!iso) return '—'; const d = new Date(iso); return isNaN(d) ? String(iso).slice(0, 10) : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short' }); };

  // Palette — the same ground the simulator mock stands on.
  // Pinpoint's palette — the same ink, brand and card treatment as the rest of the app.
  const C = { ground: 'transparent', card: '#fff', card2: '#FAFAF8', line: 'rgba(0,0,0,.08)', text: '#1C1C1E', mute: '#6E6E73', dim: '#8E8E93', faint: '#AEAEB2', rose: '#990033', wine: '#990033', mint: '#2E7D4F', sky: '#2F6FED', amber: '#C77A00', good: '#4A6A00', goodBg: '#EEF6D6', warnBg: '#FFF4D6', badBg: '#FBE9EE', tint: '#F2F2F0' };
  const STAGE_LABEL = { origin_rhythm: 'Packing list', origin_cleared: 'Origin cleared', departed: 'Departed', arrived: 'Arrived', dest_cleared: 'Dest. cleared', fc_receipt: 'FC receipt' };

  let _data = null, _loading = false, _err = null, _mode = 'Sea', _sel = null, _isOpen = false;

  function styles() {
    if (el('perf-css')) return;
    const s = document.createElement('style'); s.id = 'perf-css';
    s.textContent = `
      #page-performance{padding:0;}
      .perf{color:${C.text};padding:4px 0 28px;}
      .perf *{box-sizing:border-box;}
      .perf a{color:${C.sky};text-decoration:none;}
      .perf-eyebrow{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:${C.mute};font-weight:600;}
      .perf-h1{font-size:24px;font-weight:700;letter-spacing:-.02em;line-height:1.15;margin:4px 0 0;}
      .perf-h1 em{font-style:normal;color:${C.wine};}
      .perf-sub{font-size:12.5px;color:${C.mute};margin-top:6px;max-width:760px;}
      .perf-head{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap;margin-bottom:16px;}
      .perf-chips{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap;align-items:center;}
      .perf-chip{border:0.5px solid rgba(0,0,0,.14);background:#fff;color:${C.text};border-radius:999px;padding:7px 12px;font-size:11.5px;cursor:pointer;font-family:inherit;display:inline-flex;gap:7px;align-items:center;}
      .perf-chip:hover{border-color:${C.wine};color:${C.wine};}
      .perf-chip i{width:7px;height:7px;border-radius:50%;background:${C.wine};display:inline-block;}
      .perf-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:14px;}
      .perf-kpi{background:${C.card};border:0.5px solid ${C.line};border-radius:14px;padding:13px 15px;min-height:84px;}
      .perf-kpi small{display:block;font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:${C.mute};font-weight:600;}
      .perf-kpi b{display:block;font-size:26px;font-weight:700;letter-spacing:-.02em;margin-top:4px;font-variant-numeric:tabular-nums;}
      .perf-kpi i{display:block;font-style:normal;font-size:11px;color:${C.mute};margin-top:2px;}
      .perf-sec{display:flex;align-items:baseline;gap:12px;margin:22px 0 10px;flex-wrap:wrap;}
      .perf-sec h2{font-size:14px;font-weight:700;margin:0;letter-spacing:-.01em;}
      .perf-sec span{font-size:11.5px;color:${C.mute};}
      .perf-sec .tol{margin-left:auto;font-size:11.5px;color:${C.mute};display:inline-flex;gap:6px;align-items:center;}
      .perf-sec .tol input{width:52px;height:24px;border:0.5px solid rgba(0,0,0,.18);border-radius:7px;padding:0 6px;font:inherit;font-size:11.5px;text-align:right;}
      .perf-sec .tol button{height:24px;border:0;background:${C.text};color:#fff;border-radius:7px;padding:0 9px;font:inherit;font-size:11px;cursor:pointer;}
      .perf-seg{display:inline-flex;border:0.5px solid rgba(0,0,0,.14);border-radius:999px;padding:2px;margin-left:auto;background:#fff;}
      .perf-seg button{background:none;border:0;color:${C.mute};font-size:11px;padding:4px 12px;border-radius:999px;cursor:pointer;font-family:inherit;}
      .perf-seg button.on{background:${C.wine};color:#fff;}
      .perf-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:10px;}
      .perf-card{background:${C.card};border:0.5px solid ${C.line};border-radius:14px;padding:14px 15px;cursor:pointer;transition:border-color .15s,box-shadow .15s;position:relative;}
      .perf-card:hover{border-color:rgba(153,0,51,.45);box-shadow:0 2px 10px rgba(0,0,0,.05);}
      .perf-card.on{border:1.5px solid ${C.wine};padding:13px 14px;}
      .perf-card .nm{font-size:13px;font-weight:700;letter-spacing:-.01em;display:flex;gap:8px;align-items:center;}
      .perf-card .big{font-size:30px;font-weight:700;letter-spacing:-.02em;margin-top:6px;font-variant-numeric:tabular-nums;line-height:1;}
      .perf-card .big small{font-size:11px;color:${C.mute};font-weight:500;letter-spacing:0;margin-left:6px;}
      .perf-card .row{display:flex;gap:12px;font-size:11px;color:${C.mute};margin-top:8px;flex-wrap:wrap;font-variant-numeric:tabular-nums;align-items:center;}
      .perf-card .row b{color:${C.text};font-weight:600;}
      .perf-card .row.sep{border-top:0.5px solid rgba(0,0,0,.06);padding-top:8px;}
      .perf-card svg{display:block;margin-top:10px;width:100%;height:34px;}
      .perf-pill{font-size:9.5px;letter-spacing:.06em;text-transform:uppercase;font-weight:700;border-radius:999px;padding:2px 7px;background:${C.tint};color:${C.mute};}
      .perf-pill.ok{background:${C.goodBg};color:${C.good};} .perf-pill.thin{background:${C.warnBg};color:${C.amber};} .perf-pill.none{background:${C.badBg};color:${C.wine};}
      .perf-pill.improving{background:${C.goodBg};color:${C.good};} .perf-pill.worsening{background:${C.badBg};color:${C.wine};}
      .perf-detail{grid-column:1 / -1;background:${C.card};border:0.5px solid ${C.line};border-radius:14px;padding:14px 16px;overflow:auto;}
      .perf-detail table{width:100%;border-collapse:collapse;font-size:11.5px;font-variant-numeric:tabular-nums;}
      .perf-detail th{font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;color:${C.mute};font-weight:600;text-align:left;padding:4px 8px;border-bottom:0.5px solid ${C.line};white-space:nowrap;}
      .perf-detail td{padding:5px 8px;border-bottom:0.5px solid rgba(0,0,0,.05);color:${C.text};white-space:nowrap;}
      .perf-detail .r,.perf-lanes .r{text-align:right;}
      .perf-detail tr.immature td{color:${C.faint};}
      .perf-lanes{width:100%;border-collapse:separate;border-spacing:0;font-size:12px;font-variant-numeric:tabular-nums;background:${C.card};border:0.5px solid ${C.line};border-radius:14px;overflow:hidden;}
      .perf-lanes th{font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;color:${C.mute};font-weight:600;text-align:left;padding:10px 12px;border-bottom:0.5px solid ${C.line};background:${C.card2};}
      .perf-lanes td{padding:10px 12px;border-bottom:0.5px solid rgba(0,0,0,.05);vertical-align:middle;}
      .perf-lanes tr.mode td{font-weight:600;}
      .perf-lanes .lab{color:${C.text};} .perf-lanes .lab small{display:block;color:${C.mute};font-weight:400;font-size:10.5px;}
      .perf-band{position:relative;height:18px;min-width:180px;}
      .perf-band .ax{position:absolute;left:0;right:0;top:8px;height:2px;background:rgba(0,0,0,.08);border-radius:2px;}
      .perf-band .bd{position:absolute;top:5px;height:8px;border-radius:4px;background:${C.mint};opacity:.8;}
      .perf-band .bd.late{background:${C.wine};}
      .perf-band .q{position:absolute;top:1px;width:2px;height:16px;background:${C.sky};border-radius:1px;}
      .perf-band .p{position:absolute;top:3px;width:2px;height:12px;background:${C.text};border-radius:1px;}
      .perf-stages{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 0;}
      .perf-stage{background:${C.card};border:0.5px solid ${C.line};border-radius:10px;padding:8px 11px;font-size:11px;color:${C.mute};min-width:120px;}
      .perf-stage b{display:block;color:${C.text};font-size:14px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums;}
      .perf-stage.slow{border-color:rgba(153,0,51,.5);}
      .perf-foot{font-size:10.5px;color:${C.dim};margin-top:22px;line-height:1.6;}
      .perf-empty{background:${C.card};border:0.5px solid ${C.line};border-radius:14px;padding:28px;text-align:center;color:${C.mute};font-size:13px;}
      .perf-btn{background:${C.wine};color:#fff;border:0;border-radius:999px;padding:8px 16px;font-size:12px;cursor:pointer;font-family:inherit;margin-top:12px;}
      .perf-btn[disabled]{opacity:.5;cursor:default;}
      body.pr-on #page-performance .perf-grid{grid-template-columns:repeat(auto-fill,minmax(240px,1fr));}
    `;
    document.head.appendChild(s);
  }

  // ── nav + route ──
  // v4: no top-nav item. Supplier performance lives under Reports & Downloads as its own section card.
  function injectNav() {
    const stale = el('nav-performance'); if (stale) stale.remove();
    const page = el('page-reports'); if (!page || el('rp-supplier-performance')) return;
    const anchor = page.querySelector('#rp-week-header'); if (!anchor) return;
    const card = document.createElement('div'); card.id = 'rp-supplier-performance';
    card.style.cssText = 'background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:12px;overflow:hidden;margin-bottom:14px;';
    card.innerHTML = `<div style="padding:12px 18px;border-bottom:0.5px solid rgba(0,0,0,0.06);display:flex;align-items:center;justify-content:space-between;">
        <div style="font-size:12px;font-weight:500;color:#1C1C1E;display:flex;align-items:center;gap:8px;"><span>Supplier performance</span><span style="font-size:9px;background:#F5F5F7;color:#AEAEB2;padding:2px 8px;border-radius:10px;">live page</span></div>
        <span style="font-size:10px;color:#AEAEB2;">measured from Pinpoint records · last 26 weeks</span></div>
      <div style="padding:14px 18px;display:flex;align-items:center;gap:14px;">
        <div style="flex-shrink:0;width:36px;height:36px;border-radius:9px;background:#F5F5F7;display:flex;align-items:center;justify-content:center;"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#990033" stroke-width="1.6" stroke-linecap="round"><path d="M3 3v18h18"/><polyline points="18 9 12 15 8 11 3 16"/></svg></div>
        <div style="flex:1;min-width:0;"><div style="font-size:12px;font-weight:500;color:#1C1C1E;">Fill rate, on-time, accuracy and Exception-log compliance per supplier; achieved transit per lane against the quote.</div><div style="font-size:10px;color:#6E6E73;margin-top:3px;">Computed nightly from plans, receiving, transit milestones and the Exception log. Click a supplier on the page for its week-by-week table.</div></div>
        <button id="rp-open-performance" style="flex-shrink:0;background:#1C1C1E;color:#fff;border:none;border-radius:9px;padding:9px 16px;font-size:11.5px;font-weight:500;cursor:pointer;font-family:inherit;">Open</button></div>`;
    anchor.insertAdjacentElement('afterend', card); el('rp-open-performance').onclick = () => window.show('#performance');
  }
  function section() {
    let s = el('page-performance');
    if (!s) { s = document.createElement('section'); s.id = 'page-performance'; s.className = 'hidden'; s.style.display = 'none'; const main = document.querySelector('main.vo-wrap') || document.querySelector('main'); if (main) main.appendChild(s); }
    return s;
  }
  const PAGES = ['page-dashboard', 'page-intake', 'page-exec', 'page-receiving', 'page-reports', 'page-map', 'page-finance', 'page-d2d', 'page-elevate', 'page-uid-labels', 'page-flow'];
  function wrapRouter() {
    if (window.__perfRouted) return; window.__perfRouted = true;
    const orig = window.show;
    window.show = function (hash) {
      let h = String(hash || location.hash || '').trim(); if (h.startsWith('#/')) h = '#' + h.slice(2);
      if (h === '#performance') {
        PAGES.forEach(id => { const p = el(id); if (p) { p.classList.add('hidden'); p.style.display = 'none'; } });
        if (typeof window.hideReceivingPage === 'function') { try { window.hideReceivingPage(); } catch (_) {} }
        if (location.hash !== '#performance') history.replaceState(null, '', '#performance');
        const s = section(); s.classList.remove('hidden'); s.style.display = 'block';
        document.querySelectorAll('.pn-nav-item').forEach(x => x.classList.remove('active')); const n = el('nav-reports'); if (n) n.classList.add('active');
        _isOpen = true; open().catch(e => console.error('[performance] open failed', e));
        return;
      }
      _isOpen = false; const s = el('page-performance'); if (s) { s.classList.add('hidden'); s.style.display = 'none'; }
      if (typeof orig === 'function') return orig.apply(this, arguments);
    };
  }

  // ── data ──
  async function open(force) {
    styles(); const s = section();
    if (!_data || force) {
      _loading = true; _err = null; render();
      try { _data = await req('/signals/overview'); } catch (e) { _err = e; _data = null; }
      _loading = false;
    }
    render();
  }

  // ── drawing helpers ──
  function bars(vals, opts) {
    // vals: [{v, mature, label}] — v is 0..100. Immature weeks are drawn hollow.
    const o = opts || {}, w = o.w || 240, h = o.h || 34, n = vals.length || 1, gap = 2, bw = Math.max(2, (w - gap * (n - 1)) / n);
    const base = o.base == null ? 100 : o.base;
    return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">` +
      `<line x1="0" x2="${w}" y1="${h - 1}" y2="${h - 1}" stroke="rgba(0,0,0,.1)"/>` +
      (o.goal ? `<line x1="0" x2="${w}" y1="${h - 1 - (o.goal / base) * (h - 4)}" y2="${h - 1 - (o.goal / base) * (h - 4)}" stroke="rgba(0,0,0,.2)" stroke-dasharray="2 3"/>` : '') +
      vals.map((p, i) => { const v = Math.max(0, Math.min(base, Number(p.v) || 0)); const bh = (v / base) * (h - 4); const col = p.v == null ? 'rgba(0,0,0,.06)' : (v >= 95 ? C.mint : v >= 85 ? C.amber : C.rose); return `<rect x="${(i * (bw + gap)).toFixed(1)}" y="${(h - 1 - bh).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" rx="1" fill="${col}" ${p.mature ? '' : 'opacity=".35"'}><title>${esc(p.label || '')}</title></rect>`; }).join('') +
      `</svg>`;
  }
  function band(l, scale) {
    // Achieved P50–P90 band on a shared scale, quoted as a blue tick, P50 as a white tick.
    if (l.transit_p50 == null) return '<span style="color:' + C.faint + '">no measured transit</span>';
    const x = v => Math.max(0, Math.min(100, (v / scale) * 100));
    const a = x(l.transit_p50), b = x(l.transit_p90 == null ? l.transit_p50 : l.transit_p90), late = l.quoted_p50 != null && l.transit_p50 > l.quoted_p50;
    return `<div class="perf-band" title="P50 ${d1(l.transit_p50)} d · P90 ${d1(l.transit_p90)} d${l.quoted_p50 != null ? ' · quoted ' + d1(l.quoted_p50) : ''}"><div class="ax"></div><div class="bd${late ? ' late' : ''}" style="left:${a}%;width:${Math.max(1.5, b - a)}%"></div><div class="p" style="left:calc(${a}% - 1px)"></div>${l.quoted_p50 != null ? `<div class="q" style="left:calc(${x(l.quoted_p50)}% - 1px)"></div>` : ''}</div>`;
  }
  // ── Pulse ──
  function ask(q) { if (window._pulseRail && typeof window._pulseRail.fire === 'function') return window._pulseRail.fire(q); const i = el('pulse-input-display'); if (i) { i.value = q; if (typeof window.pulseSend === 'function') window.pulseSend(); } }
  function pinSupplier(name) { if (window._pulseRail && typeof window._pulseRail.setPin === 'function') window._pulseRail.setPin(name ? { page: 'performance', label: name, ids: [], supplier: name } : null); }

  // ── render ──
  function render() {
    const s = section(); styles();
    if (_loading && !_data) { s.innerHTML = `<div class="perf"><div class="perf-eyebrow">Performance</div><div class="perf-sub" style="margin-top:14px">Reading the signals…</div></div>`; return; }
    if (_err || !_data) { s.innerHTML = `<div class="perf"><div class="perf-eyebrow">Performance</div><div class="perf-empty" style="margin-top:14px">${esc(_err ? (_err.status === 403 ? 'Performance is not available for this account.' : _err.message) : 'No signals yet.')}</div></div>`; return; }
    const d = _data, sup = d.suppliers || [], lanes = d.lanes || [];
    const isAdmin = (window._pnRoleKey || '') === 'org:admin_auth';
    if (!d.computed_at && !sup.length) {
      s.innerHTML = `<div class="perf"><div class="perf-eyebrow">Supplier performance</div><h1 class="perf-h1">Nothing measured yet</h1><div class="perf-empty" style="margin-top:16px">The signals job runs nightly over the last ${d.window_weeks} weeks of plans, receiving and transit. ${isAdmin ? 'You can run it now for this client.' : 'Check back after the next run.'}${isAdmin ? '<br><button class="perf-btn" id="perf-run">Compute now</button>' : ''}</div></div>`;
      const b = el('perf-run'); if (b) b.onclick = async () => { b.disabled = true; b.textContent = 'Computing…'; try { await req('/signals/run', { method: 'POST', body: '{}' }); await open(true); } catch (e) { b.disabled = false; b.textContent = 'Compute now'; alert('Run failed: ' + e.message); } };
      return;
    }

    // KPIs
    const rated = sup.filter(x => x.confidence !== 'none');
    const onTime = rated.filter(x => (x.on_time_rate || 0) >= 90).length;
    const recent = sup.map(x => (x.history || []).filter(h => h.mature).slice(-8)).flat();
    const plan8 = recent.reduce((a, h) => a + (h.planned_units || 0), 0), app8 = recent.reduce((a, h) => a + (h.applied_units || 0), 0);
    const laneMode = m => lanes.find(l => l.grain === 'mode' && l.mode === m);
    const sea = laneMode('Sea'), air = laneMode('Air');
    const slipTxt = l => !l || l.transit_p50 == null ? '—' : (l.quoted_p50 == null ? d1(l.transit_p50) + ' d' : `${d1(l.transit_p50)} d <span style="color:${l.transit_p50 > l.quoted_p50 ? C.rose : C.mint}">${l.transit_p50 > l.quoted_p50 ? '+' : ''}${d1(l.transit_p50 - l.quoted_p50)}</span>`);
    const top = sup[0] && sup[0].supplier;
    const chips = [top ? ['How reliable is ' + top + '?', `How reliable is ${top} — fill, on time, how late, and is it getting better or worse?`] : null,
      sea ? ['How long does sea really take?', 'How long does sea freight really take door to door, against what was quoted, and which stage slips most?'] : null,
      ['Who over- or under-delivers?', 'Which suppliers deliver over or under plan beyond the tolerance, and which have the most Exception log incidents? Use measured figures with sample sizes.']].filter(Boolean);

    let html = `<div class="perf" data-build="${BUILD}">
      <div class="perf-head"><div><div class="perf-eyebrow"><a href="#reports" data-back style="color:${C.dim};text-decoration:none">← Reports &amp; Downloads</a> &nbsp;·&nbsp; last ${d.window_weeks} weeks</div>
        <h1 class="perf-h1">Supplier performance</h1>
        <div class="perf-sub">Suppliers and lanes from Pinpoint's own records — computed ${d.computed_at ? dmy(d.computed_at) : '—'}, never estimated. Every figure carries its sample; thin samples say so.</div></div>
        <div class="perf-chips">${chips.map(c => `<button class="perf-chip" data-q="${esc(c[1])}"><i></i>${esc(c[0])}</button>`).join('')}</div></div>
      <div class="perf-kpis">
        <div class="perf-kpi"><small>Suppliers on time</small><b>${onTime} <span style="font-size:14px;color:${C.dim};font-weight:500">of ${rated.length}</span></b><i>≥ 90% of POs received by plan date</i></div>
        <div class="perf-kpi"><small>Fill rate · last 8 weeks</small><b>${plan8 ? Math.round(app8 / plan8 * 100) + '%' : '—'}</b><i>${nf(app8)} applied of ${nf(plan8)} planned</i></div>
        <div class="perf-kpi"><small>Sea transit · achieved</small><b>${slipTxt(sea)}</b><i>${sea ? `P50 vs quoted ${sea.quoted_p50 == null ? '—' : d1(sea.quoted_p50) + ' d'} · ${sea.transit_n || sea.n} sailings` : 'no sea history'}</i></div>
        <div class="perf-kpi"><small>Air transit · achieved</small><b>${slipTxt(air)}</b><i>${air ? `P50 vs quoted ${air.quoted_p50 == null ? '—' : d1(air.quoted_p50) + ' d'} · ${air.transit_n || air.n} flights` : 'no air history'}</i></div>
      </div>`;

    // Suppliers
    const tol = (d.settings && d.settings.accuracy_tolerance_pct != null) ? d.settings.accuracy_tolerance_pct : 2;
    html += `<div class="perf-sec"><h2>Suppliers</h2><span>fill, timing, accuracy against plan and the Exception log · click a supplier for its weeks</span>${isAdmin ? `<span class="tol">accuracy tolerance ±<input id="perf-tol" type="number" min="0" max="50" step="0.5" value="${esc(tol)}">%<button id="perf-tol-save">Save</button></span>` : `<span style="margin-left:auto">accuracy tolerance ±${esc(tol)}%</span>`}</div><div class="perf-grid">`;
    if (!sup.length) html += `<div class="perf-empty" style="grid-column:1/-1">No supplier weeks in the window.</div>`;
    sup.forEach(x => {
      const hist = (x.history || []); const vals = hist.map(h => ({ v: h.fill_rate, mature: !!h.mature, label: `${wk(h.week_start)} · ${pct(h.fill_rate)} · ${nf(h.applied_units)} of ${nf(h.planned_units)}` }));
      const on = _sel === x.supplier;
      html += `<div class="perf-card${on ? ' on' : ''}" data-sup="${esc(x.supplier)}">
        <div class="nm">${esc(x.supplier)}<span class="perf-pill ${esc(x.confidence)}" style="margin-left:auto">${esc(x.confidence === 'ok' ? x.weeks + ' wks' : x.confidence)}</span></div>
        <div class="big">${pct(x.fill_rate)}<small>fill · last 8 ${pct(x.fill_rate_recent)}</small></div>
        <div class="row"><span>on time <b>${pct(x.on_time_rate)}</b></span><span>late <b>${x.late_days_p50 == null ? '—' : d1(x.late_days_p50) + ' d'}</b>${x.late_days_p90 != null ? ` <span style="color:${C.faint}">P90 ${d1(x.late_days_p90)}</span>` : ''}</span><span>short <b>${pct(x.short_rate)}</b></span></div>
        <div class="row"><span>packing list on time <b>${x.ready_n ? pct(x.ready_on_time_rate) : '—'}</b></span>${x.ready_late_p50 != null && x.ready_late_p50 > 0 ? `<span>ready late <b>${d1(x.ready_late_p50)} d</b></span>` : ''}${x.trend ? `<span class="perf-pill ${esc(x.trend)}">${esc(x.trend)}</span>` : ''}</div>
        <div class="row sep"><span>accuracy <b>${x.accuracy_n ? pct(x.accuracy_rate) : '—'}</b> <span style="color:${C.dim}">within ±${esc(tol)}%</span></span><span>over <b style="color:${x.over_pct > tol ? C.wine : C.text}">${x.accuracy_n ? pct(x.over_pct) : '—'}</b></span><span>under <b style="color:${x.under_pct > tol ? C.wine : C.text}">${x.accuracy_n ? pct(x.under_pct) : '—'}</b></span></div>
        <div class="row"><span>compliance <b>${x.nc_incidents ? pct(x.nc_rate) + ' of POs' : (x.nc_incidents === 0 ? 'clean' : '—')}</b></span>${x.nc_incidents ? `<span>${nf(x.nc_incidents)} incident${x.nc_incidents === 1 ? '' : 's'}${x.nc_open ? ` · <b style="color:${C.wine}">${nf(x.nc_open)} open</b>` : ''}</span>` : ''}${x.nc_top_category ? `<span class="perf-pill" title="${esc(x.nc_top_count)} of ${esc(x.nc_incidents)}">${esc(x.nc_top_category)}</span>` : ''}</div>
        ${bars(vals, { goal: 95 })}</div>`;
      if (on) {
        html += `<div class="perf-detail"><div style="display:flex;gap:12px;align-items:baseline;margin-bottom:8px"><b style="font-size:13px">${esc(x.supplier)} · week by week</b><span style="font-size:11px;color:${C.dim}">${nf(x.pos)} POs · ${nf(x.applied_units)} of ${nf(x.planned_units)} units · dimmed weeks are still open</span><button class="perf-chip" style="margin-left:auto" data-q="${esc(`How reliable is ${x.supplier} — fill, on time, how late, and is it getting better or worse?`)}"><i></i>Ask Pulse</button></div>
          <table><tr><th>Week</th><th class="r">Planned</th><th class="r">Applied</th><th class="r">Fill</th><th class="r">POs</th><th class="r">On time</th><th class="r">Late</th><th class="r">Short</th><th class="r">Pending</th><th class="r">Packing list late</th><th class="r">Over</th><th class="r">Under</th><th class="r">Within ±${esc(tol)}%</th><th class="r">Incidents</th></tr>
          ${hist.slice().reverse().map(h => `<tr class="${h.mature ? '' : 'immature'}"><td>${wk(h.week_start)} <span style="color:${C.faint}">${dmy(h.week_start)}</span></td><td class="r">${nf(h.planned_units)}</td><td class="r">${nf(h.applied_units)}</td><td class="r">${pct(h.fill_rate)}</td><td class="r">${nf(h.pos)}</td><td class="r">${nf(h.on_time_pos)}</td><td class="r">${nf(h.late_pos)}</td><td class="r">${nf(h.short_pos)}</td><td class="r">${nf(h.pending_pos)}</td><td class="r">${h.ready_n ? (h.ready_late_days_p50 == null ? '0 d' : d1(h.ready_late_days_p50) + ' d') : '—'}</td><td class="r">${h.accuracy_n ? '+' + nf(h.over_units) : '—'}</td><td class="r">${h.accuracy_n ? '−' + nf(h.under_units) : '—'}</td><td class="r">${h.accuracy_n ? `${nf(h.within_pos)} of ${nf(h.accuracy_n)}` : '—'}</td><td class="r">${h.nc_incidents ? `${nf(h.nc_incidents)}${h.nc_units ? ' · ' + nf(h.nc_units) + ' u' : ''}${h.nc_cartons ? ' · ' + nf(h.nc_cartons) + ' ctn' : ''}` : '—'}</td></tr>`).join('')}</table></div>`;
      }
    });
    html += `</div>`;

    // Lanes
    const modes = [...new Set(lanes.map(l => l.mode).filter(Boolean))]; if (!modes.includes(_mode)) _mode = modes[0] || 'Sea';
    const ml = lanes.filter(l => l.mode === _mode); const modeRow = ml.find(l => l.grain === 'mode');
    const scale = Math.max(1, ...ml.flatMap(l => [l.transit_p90 || 0, l.quoted_p50 || 0, l.transit_p50 || 0])) * 1.12;
    html += `<div class="perf-sec"><h2>Lanes</h2><span>achieved transit against what was quoted · band is P50–P90, blue tick is quoted</span>${modes.length > 1 ? `<div class="perf-seg">${modes.map(m => `<button class="${m === _mode ? 'on' : ''}" data-mode="${esc(m)}">${esc(m)}</button>`).join('')}</div>` : ''}</div>`;
    if (!ml.length) html += `<div class="perf-empty">No ${esc(_mode).toLowerCase()} movements with milestone dates in the window.</div>`;
    else {
      const order = { mode: 0, carrier: 1, supplier: 2 };
      html += `<table class="perf-lanes"><tr><th>Lane</th><th class="r">Movements</th><th style="min-width:200px">Transit · P50–P90 vs quoted</th><th class="r">P50</th><th class="r">P90</th><th class="r">Quoted</th><th class="r">Slip</th><th class="r">Door to door</th><th>Slowest stage</th><th></th></tr>`;
      ml.slice().sort((a, b) => order[a.grain] - order[b.grain] || (b.n - a.n)).forEach(l => {
        const lab = l.grain === 'mode' ? `All ${l.mode.toLowerCase()}` : (l.grain === 'carrier' ? (l.carrier || 'Unknown carrier') : (l.supplier || '—'));
        const sub = l.grain === 'mode' ? `${l.transit_n || 0} with transit · ${l.d2d_n || 0} door to door${l.legacy_n ? ' · ' + l.legacy_n + ' from legacy dates' : ''}` : (l.grain === 'carrier' ? 'carrier' : 'supplier');
        const slow = l.slowest_stage && l.stage && l.stage[l.slowest_stage] ? `${STAGE_LABEL[l.slowest_stage] || l.slowest_stage} <span style="color:${C.rose}">+${d1(l.stage[l.slowest_stage].slip_p50)} d</span>` : `<span style="color:${C.faint}">on plan</span>`;
        html += `<tr class="${l.grain}"><td class="lab">${esc(lab)}<small>${esc(sub)}</small></td><td class="r">${nf(l.n)}</td><td>${band(l, scale)}</td><td class="r">${l.transit_p50 == null ? '—' : d1(l.transit_p50)}</td><td class="r">${l.transit_p90 == null ? '—' : d1(l.transit_p90)}</td><td class="r">${l.quoted_p50 == null ? '—' : d1(l.quoted_p50)}</td><td class="r" style="color:${l.slip_p50 > 0 ? C.rose : l.slip_p50 < 0 ? C.mint : C.dim}">${l.slip_p50 == null ? '—' : (l.slip_p50 > 0 ? '+' : '') + d1(l.slip_p50)}</td><td class="r">${l.d2d_p50 == null ? '—' : (l.d2d_p90 == null || l.d2d_p90 === l.d2d_p50 ? d1(l.d2d_p50) + ' d' : `${d1(l.d2d_p50)}–${d1(l.d2d_p90)} d`)}</td><td>${slow}</td><td><span class="perf-pill ${esc(l.confidence)}">${esc(l.confidence)}</span></td></tr>`;
      });
      html += `</table>`;
      if (modeRow && modeRow.stage && Object.keys(modeRow.stage).length) {
        html += `<div class="perf-stages">${Object.entries(modeRow.stage).map(([k, v]) => `<div class="perf-stage${modeRow.slowest_stage === k ? ' slow' : ''}">${esc(STAGE_LABEL[k] || k)} <span style="color:${C.faint}">· ${nf(v.n)}</span><b>${v.slip_p50 == null ? '—' : (v.slip_p50 > 0 ? '+' : '') + d1(v.slip_p50) + ' d'}${v.slip_p90 != null ? ` <span style="font-size:10px;color:${C.dim};font-weight:400">P90 ${(v.slip_p90 > 0 ? '+' : '') + d1(v.slip_p90)}</span>` : ''}</b></div>`).join('')}</div>
        <div style="font-size:10.5px;color:${C.faint};margin-top:6px">Slip per stage: actual date against the planned date for that stage, median across ${esc(_mode.toLowerCase())} movements. Outliers beyond 2.5× the median are excluded from transit (${nf(modeRow.outliers || 0)} here).</div>`;
      }
    }

    html += `<div class="perf-foot">Definitions · <b>Fill rate</b> units applied in Pinpoint against units planned, counted once a week is four weeks old (mature). <b>On time</b> PO received by its planned due date; <b>late</b> is days past it. <b>Packing list on time</b> the supplier's packing list date against the plan. <b>Transit</b> departed → arrived on the movement's milestones; <b>quoted</b> is the transit on the quote or the lane rule. <b>Door to door</b> packing list → FC receipt. <b>Accuracy</b> received POs in mature weeks whose applied units land within the tolerance of plan; <b>over</b> / <b>under</b> are the units above or below plan as a share of those POs' plan. <b>Compliance</b> incidents from the Exception log joined to this client's plan by week and PO: share of received POs with an incident, units reworked per 1,000 applied, carton failures per 100 cartons received. <b>Confidence</b> ok from ${d.minimums ? d.minimums.supplier_weeks : 8} weeks or ${d.minimums ? d.minimums.lane_movements : 5} movements; thin below that; none when there is too little to say. Definitions v${esc(d.definitions_version || '')}.</div>
    </div>`;
    s.innerHTML = html;

    s.querySelectorAll('[data-q]').forEach(b => b.onclick = (e) => { e.stopPropagation(); ask(b.dataset.q); });
    const back = s.querySelector('[data-back]'); if (back) back.onclick = (e) => { e.preventDefault(); window.show('#reports'); };
    s.querySelectorAll('[data-mode]').forEach(b => b.onclick = () => { _mode = b.dataset.mode; render(); });
    s.querySelectorAll('.perf-card[data-sup]').forEach(c => c.onclick = () => { _sel = _sel === c.dataset.sup ? null : c.dataset.sup; pinSupplier(_sel); render(); });
    const tb = el('perf-tol-save'); if (tb) tb.onclick = async () => { const v = Number(el('perf-tol').value); if (!isFinite(v) || v < 0 || v > 50) return alert('Tolerance must be between 0 and 50 %.'); tb.disabled = true; tb.textContent = 'Saving…'; try { await req('/signals/settings', { method: 'PUT', body: JSON.stringify({ accuracy_tolerance_pct: v }) }); await open(true); } catch (e) { tb.disabled = false; tb.textContent = 'Save'; alert('Could not save: ' + e.message); } };
  }

  // ── boot ──
  const boot = () => { styles(); injectNav(); section(); wrapRouter(); if (String(location.hash || '') === '#performance') setTimeout(() => { try { window.show('#performance'); } catch (_) {} }, 120); };
  if (document.body) boot(); else document.addEventListener('DOMContentLoaded', boot);
  new MutationObserver(() => { try { injectNav(); } catch (_) {} }).observe(document.documentElement, { childList: true, subtree: true });
  window.addEventListener('tenancy:ready', () => { _data = null; _sel = null; if (_isOpen) open(true).catch(() => {}); });
  window.renderPerformance = () => open(true);
  window.__perfBuild = BUILD;
  console.log('[performance] build ' + BUILD + ' loaded');
})();
