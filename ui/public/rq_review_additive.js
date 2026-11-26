/* ── VelOzity Pinpoint — Door-to-door request review (internal) v3 ──
   v3: history options show the pricing basis (weeks, containers, 26-wk vs 8-wk figure).
   v2: Delete / Hide a request.
   The Requests tab inside Quote Review: the one place partner cost, markup and margin on a
   door-to-door request are visible. Reads /rq/internal/* only; the client Simulator page never
   loads this module's data. Mounted by air_quotes_internal_additive.js as a third tab. */
(function () {
  if (window._rqReview) return;
  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const nf = n => n == null ? '—' : Number(n).toLocaleString();
  const money = (n, cur) => n == null ? '—' : (cur || 'USD') + ' ' + Number(n).toLocaleString('en-AU', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  const dmy = s => { if (!s) return '—'; const d = new Date(String(s).slice(0, 10) + 'T00:00:00Z'); return isNaN(d) ? String(s) : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); };
  const todayYmd = () => new Date().toISOString().slice(0, 10);
  const addDays = (ymd, n) => { const d = new Date(ymd + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');
  async function tok() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts) {
    const t = await tok(), o = opts || {}; const headers = { 'Content-Type': 'application/json', ...(o.headers || {}) };
    if (t) headers.Authorization = 'Bearer ' + t; if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers }); const txt = await r.text(); let d = null; try { d = txt ? JSON.parse(txt) : null; } catch (e) { d = txt; }
    if (!r.ok) { const e = new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); e.status = r.status; throw e; } return d;
  }
  const C = { text: '#1C1C1E', mute: '#6E6E73', dim: '#8E8E93', faint: '#AEAEB2', wine: '#990033', mint: '#2E7D4F', sky: '#2F6FED', amber: '#C77A00', good: '#4A6A00', goodBg: '#EEF6D6', warnBg: '#FFF4D6', badBg: '#FBE9EE', tint: '#F2F2F0', line: 'rgba(0,0,0,.08)' };
  const ISTATE = { requested: ['Requested', C.tint, C.mute], options_offered: ['Options in · price & release', C.warnBg, C.amber], released: ['Released to client', '#E6F0FF', C.sky], picked: ['Picked · confirm cost', C.warnBg, C.amber], cost_confirmed: ['Cost confirmed · release firm', C.warnBg, C.amber], firm_released: ['Firm · awaiting approval', C.goodBg, C.good], approved: ['Approved', C.goodBg, C.good], declined: ['Declined', C.tint, C.mute], expired: ['Expired', C.badBg, C.wine] };
  const OSTATE = { approximate: [C.tint, C.mute], picked: [C.warnBg, C.amber], confirmed: [C.warnBg, C.amber], firm: [C.goodBg, C.good], approved: [C.goodBg, C.good], expired: [C.badBg, C.wine], withdrawn: [C.tint, C.faint] };
  let _host = null, _data = null, _open = null, _showDone = false;

  function styles() {
    if (el('rqr-css')) return; const s = document.createElement('style'); s.id = 'rqr-css';
    s.textContent = `
      .rqr{color:${C.text};font-size:12.5px;} .rqr *{box-sizing:border-box;}
      .rqr-top{display:flex;gap:12px;align-items:baseline;margin-bottom:12px;flex-wrap:wrap;} .rqr-top h3{margin:0;font-size:14px;font-weight:700;} .rqr-top span{font-size:11.5px;color:${C.mute};}
      .rqr-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:10px;margin-bottom:16px;}
      .rqr-card{background:#fff;border:0.5px solid ${C.line};border-radius:14px;padding:13px 15px;cursor:pointer;} .rqr-card:hover{border-color:rgba(153,0,51,.45);} .rqr-card.on{border:1.5px solid ${C.wine};padding:12px 14px;}
      .rqr-card .t{display:flex;gap:8px;align-items:center;font-size:13px;font-weight:700;} .rqr-card .m{font-size:11.5px;color:${C.mute};margin-top:5px;line-height:1.5;}
      .rqr-pill{display:inline-block;font-size:9.5px;letter-spacing:.06em;text-transform:uppercase;font-weight:700;border-radius:999px;padding:2px 8px;}
      .rqr-btn{border:0;border-radius:999px;padding:7px 13px;font:600 12px inherit;cursor:pointer;background:${C.wine};color:#fff;} .rqr-btn.sec{background:#fff;color:${C.text};border:0.5px solid rgba(0,0,0,.18);} .rqr-btn.sm{padding:5px 10px;font-size:11px;} .rqr-btn[disabled]{opacity:.5;}
      .rqr-panel{background:#fff;border:0.5px solid ${C.line};border-radius:14px;padding:14px 16px;}
      .rqr-panel .hd{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:6px;} .rqr-panel .hd b{font-size:13px;}
      .rqr-note{font-size:11px;color:${C.dim};line-height:1.5;margin-top:6px;}
      .rqr-opts{width:100%;border-collapse:separate;border-spacing:0;font-size:12px;border:0.5px solid ${C.line};border-radius:12px;overflow:hidden;font-variant-numeric:tabular-nums;margin-top:8px;}
      .rqr-opts th{font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;color:${C.mute};font-weight:600;text-align:left;padding:8px 10px;border-bottom:0.5px solid ${C.line};background:#FAFAF8;white-space:nowrap;}
      .rqr-opts td{padding:8px 10px;border-bottom:0.5px solid rgba(0,0,0,.05);vertical-align:middle;} .rqr-opts tr.dead td{color:${C.faint};} .rqr-opts .r{text-align:right;}
      .rqr-opts input{width:84px;border:0.5px solid rgba(0,0,0,.18);border-radius:7px;padding:4px 6px;font:inherit;font-size:12px;text-align:right;}
      .rqr-mode{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;border-radius:6px;padding:2px 7px;} .rqr-mode.sea{background:#E3F2EA;color:${C.mint};} .rqr-mode.air{background:#E6F0FF;color:${C.sky};}
      .rqr-ev{font-size:11px;color:${C.mute};line-height:1.6;max-height:180px;overflow:auto;margin-top:8px;border-top:0.5px solid ${C.line};padding-top:8px;}
      .rqr-toast{position:fixed;left:50%;bottom:26px;transform:translateX(-50%);background:${C.text};color:#fff;padding:10px 16px;border-radius:999px;font-size:12.5px;z-index:99999;opacity:0;transition:opacity .2s;pointer-events:none;} .rqr-toast.on{opacity:1;}
    `; document.head.appendChild(s);
  }
  let _tt; function toast(m) { let t = el('rqr-toast'); if (!t) { t = document.createElement('div'); t.id = 'rqr-toast'; t.className = 'rqr-toast'; document.body.appendChild(t); } t.textContent = m; t.classList.add('on'); clearTimeout(_tt); _tt = setTimeout(() => t.classList.remove('on'), 2600); }

  async function load(host) {
    styles(); _host = host || _host; if (!_host) return;
    if (!_data) _host.innerHTML = '<div style="padding:20px;color:' + C.mute + '">Loading…</div>';
    try { _data = await req('/rq/internal/requests'); } catch (e) { _host.innerHTML = `<div style="padding:20px;color:${C.wine}">Could not load: ${esc(e.message)}</div>`; return; }
    if (_open && !_data.requests.some(r => r.id === _open)) _open = null;
    render();
  }
  function render() {
    if (!_host || !_data) return; const all = _data.requests || []; const live = all.filter(r => !['approved', 'declined', 'expired'].includes(r.state)); const list = _showDone ? all : live;
    const act = live.filter(r => ['options_offered', 'cost_confirmed'].includes(r.state)).length;
    let html = `<div class="rqr"><div class="rqr-top"><h3>Door-to-door requests</h3><span>${live.length} open · ${act} need VelOzity · partner cost and margin live here and nowhere else</span><span style="margin-left:auto"><button class="rqr-btn sm sec" data-r="toggle">${_showDone ? 'Hide' : 'Show'} decided</button></span></div>`;
    if (!list.length) html += `<div class="rqr-note">No requests${_showDone ? '' : ' open'}. Partners: ${(_data.partners || []).map(p => esc(p.name)).join(', ') || '<b style="color:' + C.wine + '">none configured — Finance › Quotes</b>'}.</div>`;
    html += `<div class="rqr-grid">${list.map(r => { const st = ISTATE[r.state] || ISTATE.requested; const vol = r.volume || {}; return `<div class="rqr-card${_open === r.id ? ' on' : ''}" data-open="${esc(r.id)}"><div class="t">${esc(r.period_label)} <span style="color:${C.dim};font-weight:500">${esc(r.ref)}</span><span class="rqr-pill" style="margin-left:auto;background:${st[1]};color:${st[2]}">${esc(st[0])}</span></div>
      <div class="m">${esc((r.origins || []).join(' · ') || '—')} → ${esc(r.destination || 'Sydney')} · ${nf(vol.units)} units${r.need_by ? ` · FC by <b style="color:${C.text}">${esc(dmy(r.need_by))}</b>` : ''}<br>${r.options_count} option${r.options_count === 1 ? '' : 's'}${r.picked_count ? ` · ${r.picked_count} picked` : ''} · ${esc(r.source === 'history' ? 'from history' : 'ask')} · by ${esc(r.created_by || '—')}</div></div>`; }).join('')}</div>`;
    const x = _open && all.find(r => r.id === _open); if (x) html += panel(x);
    html += '</div>'; _host.innerHTML = html; bind();
  }
  function panel(x) {
    const opts = x.options || []; const mk = _data.markup || {}; const can = { release: ['requested', 'options_offered', 'released'].includes(x.state) && opts.some(o => o.state === 'approximate'), firm: ['picked', 'cost_confirmed'].includes(x.state) };
    const st = ISTATE[x.state] || ISTATE.requested;
    const row = o => { const dead = ['expired', 'withdrawn', 'approved'].includes(o.state); const cost = o.confirmed_cost != null ? o.confirmed_cost : o.cost; const os = OSTATE[o.state] || OSTATE.approximate;
      return `<tr class="${dead ? 'dead' : ''}"><td><span class="rqr-mode ${o.mode.toLowerCase()}">${esc(o.mode)}</span></td><td>${esc([o.carrier, o.service].filter(Boolean).join(' · ') || (o.kind === 'history' ? 'From history' : '—'))}${o.container_size ? ` · ${esc(o.container_size)}'` : ''}<div style="font-size:10.5px;color:${C.dim}">${esc(o.partner_name || (o.kind === 'history' ? 'no partner yet' : '—'))}${o.kind === 'history' && o.cost_note ? ` · <span title="${esc(o.cost_note)}">${esc(o.cost_note)}</span>` : ''}${o.departs ? ' · departs ' + esc(dmy(o.departs)) : ''}${o.transit_days != null ? ' · ' + esc(o.transit_days) + ' d' : ''} · valid ${esc(dmy(o.valid_until))}${o.validity_days != null && o.validity_days < 3 && !dead ? ` <span style="color:${C.amber};font-weight:700">(${o.validity_days} d)</span>` : ''}</div></td>
        <td class="r">${cost != null ? esc(money(cost, o.cost_currency)) : '—'}${o.confirmed_cost != null ? `<div style="font-size:10.5px;color:${C.good}">confirmed${o.confirmed_cost !== o.cost ? ' · was ' + esc(money(o.cost, o.cost_currency)) : ''}</div>` : (o.local_charges ? `<div style="font-size:10.5px;color:${C.dim}">incl. ${esc(money(o.local_charges, o.cost_currency))} local</div>` : '')}</td>
        <td class="r">${dead ? (o.markup_pct != null ? esc(o.markup_pct) + '%' : '—') : `<input type="number" step="0.5" min="0" data-mk="${esc(o.id)}" value="${o.markup_pct != null ? esc(o.markup_pct) : ''}" placeholder="${esc(mk[o.mode] != null ? mk[o.mode] : '')}">%`}</td>
        <td class="r">${dead ? (o.price != null ? esc(money(o.price, o.currency)) : '—') : `<input type="number" step="1" min="0" data-px="${esc(o.id)}" value="${o.price != null ? esc(o.price) : ''}" placeholder="price">`}<div style="font-size:10.5px;color:${C.dim}">${o.price_basis === 'per_container' ? 'per container' : o.price_basis === 'per_kg' ? 'per kg' : 'total'}</div></td>
        <td class="r" style="color:${o.margin != null && o.margin < 0 ? C.wine : C.text}">${o.margin != null ? esc(money(o.margin, o.currency)) + ` <span style="color:${C.dim}">${esc(o.margin_pct)}%</span>` : '—'}</td>
        <td><span class="rqr-pill" style="background:${os[0]};color:${os[1]}">${esc(o.state)}</span></td>
        <td style="white-space:nowrap;text-align:right">${o.state === 'picked' ? `<button class="rqr-btn sm sec" data-iact="confirm" data-oid="${esc(o.id)}">Confirm cost</button> ` : ''}${!dead ? `<button class="rqr-btn sm sec" data-iact="withdraw" data-oid="${esc(o.id)}">Withdraw</button>` : ''}</td></tr>`; };
    const links = (x.links || []).slice(0, 6).map(l => `${esc(l.partner_name || 'partner')} · ${l.purpose === 'partner_options' ? 'options' : 'confirm'} link ${l.used_at ? 'used ' + esc(dmy(l.used_at)) : 'sent ' + esc(dmy(l.created_at))}`).join(' · ');
    const vol = x.volume || {};
    return `<div class="rqr-panel"><div class="hd"><b>${esc(x.period_label)} · ${esc(x.ref)}</b><span class="rqr-pill" style="background:${st[1]};color:${st[2]}">${esc(st[0])}</span><span style="color:${C.mute}">${esc((x.origins || []).join(' · ') || '—')} → ${esc(x.destination || 'Sydney')} · ${nf(vol.units)} units${vol.cbm ? ' · ' + vol.cbm + ' CBM' : ''}${vol.kg ? ' · ' + vol.kg + ' kg' : ''}${x.need_by ? ' · FC by ' + esc(dmy(x.need_by)) : ''}</span>
        <span style="margin-left:auto;display:flex;gap:6px;"><button class="rqr-btn sm sec" data-iact="delete">${x.state === 'approved' ? 'Hide' : 'Delete'}</button><button class="rqr-btn sm sec" data-iact="rfq">Re-send carrier links</button><button class="rqr-btn sm sec" data-iact="add">Add option by hand</button>${can.release ? `<button class="rqr-btn sm" data-iact="release">Release to client</button>` : ''}${can.firm ? `<button class="rqr-btn sm" data-iact="release">Release firm price</button>` : ''}</span></div>
      <div class="rqr-note">Markup defaults: Air ${esc(mk.Air)}% · Sea ${esc(mk.Sea)}% (Finance › Rates). Type a markup or a price; the other follows. ${x.state === 'picked' ? 'Confirm by hand if the carrier phoned it in.' : ''}${x.client_note ? ` · Client note: ${esc(x.client_note)}` : ''}</div>
      ${opts.length ? `<table class="rqr-opts"><tr><th>Mode</th><th>Option · partner</th><th class="r">Cost</th><th class="r">Markup</th><th class="r">Price</th><th class="r">Margin</th><th>State</th><th></th></tr>${opts.map(row).join('')}</table>` : `<div class="rqr-note">No options yet. ${links ? '' : 'No carrier partner is configured for this client — add one under Finance › Quotes.'}</div>`}
      ${links ? `<div class="rqr-note">${links}</div>` : ''}
      <div class="rqr-ev">${(x.events || []).slice(-14).reverse().map(e => `${esc(String(e.created_at).slice(0, 16).replace('T', ' '))} · ${esc(e.actor || 'system')} · ${esc(e.to_state ? '→ ' + e.to_state + (e.detail ? ' · ' + e.detail : '') : e.detail || '')}`).join('<br>')}</div></div>`;
  }
  function bind() {
    const h = _host; const id = _open;
    h.querySelectorAll('[data-r=toggle]').forEach(b => b.onclick = () => { _showDone = !_showDone; render(); });
    h.querySelectorAll('[data-open]').forEach(c => c.onclick = () => { _open = _open === c.dataset.open ? null : c.dataset.open; render(); });
    h.querySelectorAll('[data-mk]').forEach(i => i.onchange = () => price(i.dataset.mk, { markup_pct: Number(i.value) }));
    h.querySelectorAll('[data-px]').forEach(i => i.onchange = () => price(i.dataset.px, { price: Number(i.value) }));
    h.querySelectorAll('[data-iact]').forEach(b => b.onclick = async () => {
      const act = b.dataset.iact, oid = b.dataset.oid;
      try {
        if (act === 'release') { if (!confirm('Release to the client? They get an email with the options.')) return; b.disabled = true; await req(`/rq/internal/requests/${encodeURIComponent(id)}/release`, { method: 'POST', body: '{}' }); toast('Released'); }
        else if (act === 'delete') { const x = (_data.requests || []).find(r => r.id === id) || {}; if (!confirm(x.state === 'approved' ? `Hide ${x.ref} from the lists? Its approved quotes stay.` : `Delete ${x.ref}? Open options are withdrawn; partner links stop working.`)) return; await req(`/rq/requests/${encodeURIComponent(id)}`, { method: 'DELETE' }); toast(x.state === 'approved' ? 'Hidden' : 'Deleted'); _open = null; }
        else if (act === 'rfq') { const note = prompt('Note to the carrier partners (optional):'); if (note === null) return; const r = await req(`/rq/internal/requests/${encodeURIComponent(id)}/rfq`, { method: 'POST', body: JSON.stringify({ note }) }); toast(r.sent.length ? `Links sent to ${r.sent.map(x => x.partner).join(', ')}` : 'No carrier partner configured — Finance › Quotes'); }
        else if (act === 'withdraw') { if (!confirm('Withdraw this option?')) return; await req(`/rq/internal/requests/${encodeURIComponent(id)}/options/${encodeURIComponent(oid)}/withdraw`, { method: 'POST', body: '{}' }); toast('Withdrawn'); }
        else if (act === 'confirm') { const c = prompt('Confirmed freight cost (partner currency):'); if (c === null || c === '') return; const l = prompt('Local charges (0 if none):', '0'); if (l === null) return; await req(`/rq/internal/requests/${encodeURIComponent(id)}/options/${encodeURIComponent(oid)}/confirm`, { method: 'POST', body: JSON.stringify({ cost_amount: Number(c), local_charges: Number(l) || 0, note: 'confirmed by hand' }) }); toast('Cost confirmed'); }
        else if (act === 'add') { const mode = prompt('Mode: Sea or Air', 'Sea'); if (!mode) return; const carrier = prompt('Carrier'); if (carrier === null) return; const service = prompt('Service / loop'); if (service === null) return; const cs = /air/i.test(mode) ? null : prompt("Container size: 20, 40 or 40HC", '40'); const dep = prompt('Departs (YYYY-MM-DD)', ''); const tr = prompt('Transit days (port to port)', ''); const cost = prompt(/air/i.test(mode) ? 'Cost per chargeable kg' : 'Cost per container (freight)', ''); if (cost === null) return; const loc = prompt('Local charges', '0'); const vu = prompt('Valid to (YYYY-MM-DD)', addDays(todayYmd(), 7));
          await req(`/rq/internal/requests/${encodeURIComponent(id)}/options`, { method: 'POST', body: JSON.stringify({ mode, carrier, service, container_size: cs, departs: dep || null, transit_days: tr === '' ? null : Number(tr), cost_amount: Number(cost), local_charges: Number(loc) || 0, valid_until: vu || null }) }); toast('Option added'); }
        await load();
      } catch (e) { toast(e.message); b.disabled = false; }
    });
  }
  async function price(oid, body) { try { await req(`/rq/internal/requests/${encodeURIComponent(_open)}/options/${encodeURIComponent(oid)}/price`, { method: 'POST', body: JSON.stringify(body) }); await load(); } catch (e) { toast(e.message); } }
  window._rqReview = { load, open: (id) => { _open = id; } };
})();
