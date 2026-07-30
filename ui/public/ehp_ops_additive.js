/* ── VelOzity Pinpoint — EHP Ops v1 ──
   Fulfilment workspace for EHP at VOZ_TX: inbound receipts (pallets + lines),
   assembly queue → batches → assembled → dispatched, inventory + cycle counts,
   and the versioned kit recipe. Capability-gated: only appears when the active
   client has envelope_fulfilment. Rendered as an overlay, so it does not touch
   the page router. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#34C759', AMBER = '#C8860A', RED = '#D7263D';
  let _enabled = false, _tab = 'queue', _cache = {};

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const el = id => document.getElementById(id);
  const nfmt = n => Number(n || 0).toLocaleString();
  // Timestamps are stored in UTC. VOZ_TX operates on America/Chicago, so show facility-local.
  const FACILITY_TZ = 'America/Chicago';
  function localTs(v) {
    if (!v) return '—';
    const iso = String(v).includes('T') ? v : String(v).replace(' ', 'T') + 'Z';
    const d = new Date(iso);
    if (isNaN(d)) return String(v);
    return d.toLocaleString('en-US', { timeZone: FACILITY_TZ, year:'numeric', month:'short', day:'2-digit',
                                       hour:'2-digit', minute:'2-digit', hour12: true });
  }
  function apiBase(){ return (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/,''); }
  async function tok(){ if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch(e){} } return null; }

  async function req(path, opts) {
    const t = await tok(); const o = opts || {};
    const headers = { 'Content-Type': 'application/json', ...(o.headers||{}) };
    if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers });
    const txt = await r.text(); let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch(e){ data = txt; }
    if (!r.ok) { const err = new Error((data && data.message) || (data && data.error) || ('HTTP '+r.status)); err.status = r.status; err.body = data; throw err; }
    return data;
  }

  // ── styles ──
  function styles() {
    if (el('ehp-styles')) return;
    const s = document.createElement('style'); s.id = 'ehp-styles';
    s.textContent = `
      .ehp-ov{position:fixed;inset:0;z-index:9400;background:rgba(0,0,0,0.35);display:flex;align-items:flex-start;justify-content:center;padding:20px;overflow:auto;}
      .ehp-panel{background:#fff;border-radius:16px;width:min(1180px,97vw);box-shadow:0 24px 64px rgba(0,0,0,0.28);overflow:hidden;}
      .ehp-ov.full{padding:0;align-items:stretch;}
      .ehp-ov.full .ehp-panel{width:100vw;min-height:100vh;border-radius:0;}
      .ehp-max{background:none;border:none;font-size:15px;color:${MID};cursor:pointer;line-height:1;margin-right:10px;}
      .ehp-head{position:sticky;top:0;background:#fff;border-bottom:0.5px solid rgba(0,0,0,0.08);padding:14px 20px;display:flex;align-items:center;justify-content:space-between;z-index:3;}
      .ehp-t{font-size:16px;font-weight:700;color:${DARK};letter-spacing:-0.02em;}
      .ehp-s{font-size:11px;color:${LIGHT};margin-top:2px;}
      .ehp-x{background:none;border:none;font-size:22px;color:${MID};cursor:pointer;line-height:1;}
      .ehp-tabs{display:flex;gap:6px;padding:10px 20px 0;flex-wrap:wrap;}
      .ehp-tab{border:0.5px solid rgba(0,0,0,0.12);background:#fff;border-radius:999px;padding:5px 12px;font:600 11px/1 inherit;color:${MID};cursor:pointer;}
      .ehp-tab.on{border-color:${BRAND};color:${BRAND};}
      .ehp-body{padding:16px 20px 22px;}
      .ehp-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:14px;}
      .ehp-kpi{border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:11px 13px;}
      .ehp-kl{font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .ehp-kv{font-size:22px;font-weight:700;color:${DARK};margin-top:3px;}
      .ehp-ks{font-size:10px;color:${MID};}
      .ehp-btn{background:${BRAND};color:#fff;border:none;border-radius:8px;padding:8px 14px;font:600 12px/1 inherit;cursor:pointer;}
      .ehp-btn.g{background:#fff;color:${DARK};border:0.5px solid rgba(0,0,0,0.14);}
      .ehp-btn:disabled{opacity:.5;cursor:default;}
      .ehp-in,.ehp-sel{border:0.5px solid rgba(0,0,0,0.14);border-radius:8px;padding:7px 9px;font:400 12px/1.2 inherit;color:${DARK};box-sizing:border-box;background:#fff;}
      .ehp-lbl{font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};display:block;margin-bottom:3px;}
      table.ehp{width:100%;border-collapse:collapse;margin-top:8px;}
      table.ehp th,table.ehp td{text-align:left;padding:7px 8px;border-bottom:0.5px solid rgba(0,0,0,0.07);font-size:11px;}
      table.ehp th{font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};}
      table.ehp td.n,table.ehp th.n{text-align:right;}
      .ehp-chip{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;border-radius:999px;}
      .ehp-msg{border-radius:8px;padding:8px 10px;font-size:11px;margin:8px 0;}
      .ehp-msg.e{background:rgba(215,38,61,.08);border:0.5px solid rgba(215,38,61,.3);color:${RED};}
      .ehp-msg.w{background:rgba(200,134,10,.1);border:0.5px solid rgba(200,134,10,.3);color:${AMBER};}
      .ehp-msg.k{background:rgba(52,199,89,.1);border:0.5px solid rgba(52,199,89,.3);color:#1f7a34;}
      .ehp-drop{border:1.5px dashed rgba(0,0,0,0.18);border-radius:10px;padding:12px;text-align:center;font-size:11px;color:${MID};cursor:pointer;}
    `;
    document.head.appendChild(s);
  }

  // ── nav visibility ──
  async function refreshEnabled() {
    try {
      await req('/ehp/queue');           // 409 when the active client lacks the capability
      _enabled = true;
    } catch (e) { _enabled = (e.status !== 409 && e.status !== 403); if (e.status === 409) _enabled = false; }
    const sub = el('nav-vas-fulfilment');
    if (sub) sub.style.display = _enabled ? '' : 'none';
    const legacy = el('nav-ehp');                      // pre-restructure nav item, if still present
    if (legacy) legacy.style.display = _enabled ? '' : 'none';
  }

  // ── shell ──
  function close(){ const o = document.querySelector('.ehp-ov'); if (o) o.remove(); }
  async function open() {
    styles();
    let o = document.querySelector('.ehp-ov');
    if (!o) {
      o = document.createElement('div'); o.className = 'ehp-ov';
      o.addEventListener('click', e => { if (e.target === o) close(); });
      o.innerHTML = `<div class="ehp-panel">
        <div class="ehp-head">
          <div><div class="ehp-t">EHP Fulfilment — VOZ_TX</div>
          <div class="ehp-s">Inbound · assembly · dispatch · inventory</div></div>
          <div style="display:flex;align-items:center;">
            <button class="ehp-max" id="ehp-max" title="Expand to full screen">⛶</button>
            <button class="ehp-x" id="ehp-close">×</button>
          </div>
        </div>
        <div class="ehp-tabs" id="ehp-tabs"></div>
        <div class="ehp-body" id="ehp-body">Loading…</div>
      </div>`;
      document.body.appendChild(o);
      el('ehp-close').addEventListener('click', close);
      el('ehp-max').addEventListener('click', () => {
        const on = o.classList.toggle('full');
        try { localStorage.setItem('pinpoint.ehpFull', on ? '1' : '0'); } catch (e) {}
        el('ehp-max').title = on ? 'Exit full screen' : 'Expand to full screen';
      });
      try { if (localStorage.getItem('pinpoint.ehpFull') === '1') o.classList.add('full'); } catch (e) {}
    }
    renderTabs(); render();
  }

  function renderTabs() {
    const tabs = [['queue','Assembly'],['inbound','Inbound'],['inventory','Inventory'],['recipe','Recipe'],['shopify','Shopify']];
    const c = el('ehp-tabs'); if (!c) return;
    c.innerHTML = tabs.map(([k,l]) => `<button class="ehp-tab ${_tab===k?'on':''}" data-tab="${k}">${l}</button>`).join('');
    c.querySelectorAll('[data-tab]').forEach(b => b.addEventListener('click', () => { _tab = b.getAttribute('data-tab'); renderTabs(); render(); }));
  }

  function msg(kind, text){ return `<div class="ehp-msg ${kind}">${esc(text)}</div>`; }

  async function render() {
    const body = el('ehp-body'); if (!body) return;
    body.innerHTML = 'Loading…';
    try {
      if (_tab === 'queue')     return await renderQueue(body);
      if (_tab === 'inbound')   return await renderInbound(body);
      if (_tab === 'inventory') return await renderInventory(body);
      if (_tab === 'recipe')    return await renderRecipe(body);
      if (_tab === 'shopify')   return await renderShopify(body);
    } catch (e) {
      body.innerHTML = msg('e', e.status === 409
        ? 'Switch the Client selector to EHP to use this workspace.'
        : ('Could not load: ' + (e.message || e)));
    }
  }

  // ── Assembly: queue + batches ──
  async function renderQueue(body) {
    const [q, batches] = await Promise.all([req('/ehp/queue'), req('/ehp/batches')]);
    _cache.recipe = q.active_recipe;
    const b = batches.batches || [];
    body.innerHTML = `
      <div class="ehp-kpis">
        <div class="ehp-kpi"><div class="ehp-kl">Queued orders</div><div class="ehp-kv">${nfmt(q.queued_orders)}</div><div class="ehp-ks">awaiting a batch</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Queued envelopes</div><div class="ehp-kv">${nfmt(q.queued_envelopes)}</div><div class="ehp-ks">billable units</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Flagged orders</div><div class="ehp-kv" style="color:${q.flagged_high_qty?AMBER:DARK}">${nfmt(q.flagged_high_qty)}</div><div class="ehp-ks">unusually large</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Active recipe</div><div class="ehp-kv" style="font-size:14px;">${q.active_recipe ? esc(q.active_recipe.name) : '—'}</div><div class="ehp-ks">${q.active_recipe ? q.active_recipe.lines.map(l=>l.qty_per_envelope+'× '+l.sku).join(' · ') : 'none set'}</div></div>
      </div>
      ${!q.active_recipe ? msg('w','No active kit recipe — set one on the Recipe tab before assembling, or consumption cannot be derived.') : ''}
      <div style="display:flex;gap:8px;align-items:flex-end;margin:10px 0 4px;">
        <div><span class="ehp-lbl">Envelopes in this batch</span><input class="ehp-in" id="ehp-target" type="number" min="1" step="1" value="500" style="width:130px;"></div>
        <button class="ehp-btn" id="ehp-mkbatch" ${q.queued_orders?'':'disabled'}>Create batch</button>
        <div style="font-size:10px;color:${LIGHT};padding-bottom:8px;">Whole orders only — the actual count may land slightly above your target.</div>
      </div>
      <div id="ehp-batchmsg"></div>
      <table class="ehp"><thead><tr><th>Batch</th><th class="n">Target</th><th class="n">Envelopes</th><th class="n">Orders</th><th>State</th><th class="n">Shopify</th><th>Assembled (CT)</th><th>Dispatched (CT)</th><th></th></tr></thead>
      <tbody>${b.length ? b.map(x => `<tr>
        <td style="font-family:monospace;font-size:10px;">${esc(String(x.id).slice(-8))}</td>
        <td class="n">${nfmt(x.target_envelopes)}</td><td class="n"><b>${nfmt(x.actual_envelopes)}</b></td><td class="n">${nfmt(x.order_count)}</td>
        <td><span class="ehp-chip" style="background:${x.state==='dispatched'?'rgba(52,199,89,.14)':x.state==='assembled'?'rgba(200,134,10,.14)':'rgba(0,0,0,.06)'};color:${x.state==='dispatched'?GREEN:x.state==='assembled'?AMBER:MID};">${esc(x.state)}</span></td>
        <td class="n" style="font-size:10px;">${x.state==='dispatched'
          ? `<span style="color:${GREEN}">${(x.fulfilment&&x.fulfilment.fulfilled)||0} ok</span>` +
            (((x.fulfilment&&x.fulfilment.not_fulfilled)||0) ? ` · <span style="color:${AMBER}">${x.fulfilment.not_fulfilled} not sent</span>` : '')
          : `<span style="color:${LIGHT}">—</span>`}</td>
        <td style="font-size:10px;color:${MID}">${esc(localTs(x.assembled_at))}</td>
        <td style="font-size:10px;color:${MID}">${esc(localTs(x.dispatched_at))}</td>
        <td style="white-space:nowrap;">
          ${x.state==='queued' ? `<button class="ehp-btn g" data-asm="${esc(x.id)}">Assemble</button>` : ''}
          ${x.state==='assembled' ? `<button class="ehp-btn g" data-dis="${esc(x.id)}">Dispatch</button>` : ''}
          <button class="ehp-btn g" data-pick="${esc(x.id)}">Pick list</button>
          ${x.state==='dispatched' && ((x.fulfilment&&x.fulfilment.not_fulfilled)||0) ? `<button class="ehp-btn g" data-why="${esc(x.id)}">Why?</button>` : ''}
        </td></tr>`).join('') : `<tr><td colspan="8" style="color:${LIGHT};text-align:center;padding:18px;">No batches yet.</td></tr>`}
      </tbody></table>`;

    el('ehp-mkbatch')?.addEventListener('click', async () => {
      const target = parseInt(el('ehp-target').value, 10) || 0;
      if (target < 1) return;
      el('ehp-mkbatch').disabled = true;
      try {
        const r = await req('/ehp/batch', { method:'POST', body: JSON.stringify({ target_envelopes: target, facility_code:'VOZ_TX' }) });
        el('ehp-batchmsg').innerHTML = msg('k', `Batch created — ${r.actual_envelopes} envelopes across ${r.order_count} orders (target ${r.target_envelopes}).`);
        setTimeout(render, 900);
      } catch (e) { el('ehp-batchmsg').innerHTML = msg('e', e.message || String(e)); el('ehp-mkbatch').disabled = false; }
    });
    body.querySelectorAll('[data-asm]').forEach(x => x.addEventListener('click', async () => {
      x.disabled = true;
      try {
        const r = await req('/ehp/batch/'+x.getAttribute('data-asm')+'/assemble', { method:'POST', body:'{}' });
        el('ehp-batchmsg').innerHTML = msg('k', `Assembled ${nfmt(r.envelopes)} envelope(s) · ${nfmt(r.sticks_consumed_total||0)} sticks used in total. Per-flavour usage is derived at the next stock count.`);
        setTimeout(render, 900);
      } catch (e) { el('ehp-batchmsg').innerHTML = msg('e', e.message || String(e)); x.disabled = false; }
    }));
    body.querySelectorAll('[data-dis]').forEach(x => x.addEventListener('click', async () => {
      x.disabled = true;
      try {
        const r = await req('/ehp/batch/'+x.getAttribute('data-dis')+'/dispatch', { method:'POST', body:'{}' });
        const sh = r.shopify || {};
        let shTxt, shKind = 'k';
        if (sh.error)            { shTxt = 'Shopify write-back FAILED: ' + sh.error; shKind = 'e'; }
        else if (sh.skipped)     { shTxt = 'Shopify write-back skipped (' + sh.skipped + ')'; shKind = 'w'; }
        else if (sh.failed || sh.skipped) {
          shTxt = `Shopify: ${sh.ok||0} fulfilled` + (sh.skipped ? `, ${sh.skipped} skipped` : '') + (sh.failed ? `, ${sh.failed} failed` : '');
          if (sh.skips && sh.skips[0]) shTxt += ' — reason: ' + sh.skips[0].reason;
          else if (sh.errors && sh.errors[0]) shTxt += ' — ' + sh.errors[0].error;
          shKind = 'w';
        }
        else                     { shTxt = `Shopify: ${sh.ok || 0} order(s) marked fulfilled`; }
        el('ehp-batchmsg').innerHTML = msg(shKind, `Dispatched ${nfmt(r.envelopes)} envelope(s) across ${nfmt(r.orders)} order(s). ` + shTxt);
        console.log('[ehp] dispatch result', r);
        setTimeout(render, 900);
      } catch (e) { el('ehp-batchmsg').innerHTML = msg('e', e.message || String(e)); x.disabled = false; }
    }));
    body.querySelectorAll('[data-why]').forEach(x => x.addEventListener('click', async () => {
      try {
        const r = await req('/ehp/batch/'+x.getAttribute('data-why')+'/orders');
        const nf = r.not_fulfilled || [];
        el('ehp-batchmsg').innerHTML = msg('w',
          `${(r.fulfilled||[]).length} order(s) fulfilled in Shopify. ${nf.length} not sent — ` +
          nf.map(o=>`${o.order_number}: ${o.reason}`).join(' · '));
      } catch (e) { el('ehp-batchmsg').innerHTML = msg('e', e.message || String(e)); }
    }));
    body.querySelectorAll('[data-pick]').forEach(x => x.addEventListener('click', async () => {
      try {
        const r = await req('/ehp/batch/'+x.getAttribute('data-pick')+'/picklist');
        const w = window.open('', '_blank');
        const rows = (r.orders||[]);
        const totalEnv = rows.reduce((a,o)=>a+(o.envelope_qty||0),0);
        w.document.write(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Pick list</title><style>
          body{font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;color:#1C1C1E;margin:28px;}
          h2{font-size:18px;margin:0 0 4px;color:#990033;} .meta{color:#6E6E73;font-size:12px;margin-bottom:16px;}
          table{width:100%;border-collapse:collapse;table-layout:auto;}
          th,td{border:1px solid #ddd;padding:7px 9px;vertical-align:top;text-align:left;
                white-space:normal;word-break:break-word;overflow-wrap:anywhere;}
          th{background:#F5F5F7;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:#6E6E73;}
          td.n{text-align:right;white-space:nowrap;} .flag{color:#C8860A;font-weight:600;}
          col.c-order{width:11%} col.c-env{width:6%} col.c-name{width:20%} col.c-addr{width:31%}
          col.c-city{width:11%} col.c-state{width:5%} col.c-zip{width:11%} col.c-addr2{width:13%}
          td.zip{white-space:nowrap;}
          @media print{body{margin:10mm} th{background:#eee !important;-webkit-print-color-adjust:exact}}
        </style></head><body>
        <h2>Pick list</h2>
        <div class="meta">Batch ${esc(x.getAttribute('data-pick'))} &middot; ${rows.length} order(s) &middot; ${totalEnv} envelope(s) &middot; ${new Date().toLocaleString()}</div>
        <table>
          <colgroup><col class="c-order"><col class="c-env"><col class="c-name"><col class="c-addr"><col class="c-addr2"><col class="c-city"><col class="c-state"><col class="c-zip"></colgroup>
          <thead><tr><th>Order</th><th>Env</th><th>Name</th><th>Address 1</th><th>Address 2</th><th>City</th><th>State</th><th>Zip</th></tr></thead>
          <tbody>${rows.map(o=>`<tr>
            <td>${esc(o.order_number)}${o.flagged_high_qty?' <span class="flag">&#9873;</span>':''}</td>
            <td class="n">${o.envelope_qty}</td>
            <td>${esc(o.recipient_name)}</td>
            <td>${esc(o.recipient_address)}</td>
            <td>${esc(o.recipient_address2 || '')}</td>
            <td>${esc(o.recipient_city)}</td>
            <td>${esc(o.recipient_state)}</td>
            <td class="zip">${esc(o.recipient_postcode)}</td></tr>`).join('')}</tbody>
        </table></body></html>`);
        w.document.close();
      } catch (e) { alert('Pick list failed: ' + (e.message||e)); }
    }));
  }

  // ── Inbound ──
  let _lines = [];
  async function renderInbound(body) {
    const today = new Date().toISOString().slice(0,10);
    body.innerHTML = `
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-bottom:10px;">
        <div><span class="ehp-lbl">Received date (facility local)</span><input class="ehp-in" id="ehp-rdate" type="date" value="${today}"></div>
        <div><span class="ehp-lbl">Pallets received (billing)</span><input class="ehp-in" id="ehp-pallets" type="number" min="0" step="1" value="0" style="width:120px;"></div>
        <div><span class="ehp-lbl">Reference</span><input class="ehp-in" id="ehp-ref" placeholder="ASN / DO number"></div>
      </div>
      <div style="font-size:10px;color:${LIGHT};margin-bottom:8px;"><b>Pallets drive billing</b> ($35 each) and are counted independently of the product lines below — leave at 0 for a non-palletised receipt. A partial pallet bills as one.</div>
      <div id="ehp-lines"></div>
      <button class="ehp-btn g" id="ehp-addline" style="margin-top:6px;">＋ Add product line</button>
      <div class="ehp-drop" id="ehp-drop" style="margin-top:12px;">Or drop a receipt spreadsheet (columns: sku, qty, uom, lot, expiry) <span id="ehp-fname" style="color:${BRAND};font-weight:600;"></span></div>
      <div id="ehp-inmsg"></div>
      <div style="display:flex;justify-content:flex-end;margin-top:12px;"><button class="ehp-btn" id="ehp-save">Save receipt</button></div>`;
    if (!_lines.length) _lines = [{}];
    drawLines();
    el('ehp-addline').addEventListener('click', () => { _lines.push({}); drawLines(); });

    const drop = el('ehp-drop');
    const pick = document.createElement('input'); pick.type='file'; pick.accept='.xlsx,.xls,.csv'; pick.style.display='none'; drop.appendChild(pick);
    drop.addEventListener('click', () => pick.click());
    pick.addEventListener('change', () => pick.files[0] && parseFile(pick.files[0]));
    ['dragover','dragenter'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); drop.style.borderColor = BRAND; }));
    ['dragleave','drop'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); drop.style.borderColor = 'rgba(0,0,0,0.18)'; }));
    drop.addEventListener('drop', e => { const f = e.dataTransfer?.files?.[0]; if (f) parseFile(f); });

    el('ehp-save').addEventListener('click', saveReceipt);
  }

  function drawLines() {
    const c = el('ehp-lines'); if (!c) return;
    c.innerHTML = `<table class="ehp"><thead><tr><th>SKU</th><th class="n">Qty</th><th>UoM</th><th>Lot (optional)</th><th>Expiry (optional)</th><th></th></tr></thead><tbody>
      ${_lines.map((l,i)=>`<tr>
        <td><input class="ehp-in" data-i="${i}" data-f="sku" value="${esc(l.sku||'')}" placeholder="FIZZ-KIWI" style="width:100%"></td>
        <td><input class="ehp-in" data-i="${i}" data-f="qty" type="number" min="0" step="any" value="${esc(l.qty||'')}" style="width:90px;text-align:right"></td>
        <td><select class="ehp-sel" data-i="${i}" data-f="uom">${['each','inner','carton','pallet'].map(u=>`<option ${l.uom===u?'selected':''}>${u}</option>`).join('')}</select></td>
        <td><input class="ehp-in" data-i="${i}" data-f="lot_code" value="${esc(l.lot_code||'')}" style="width:110px"></td>
        <td><input class="ehp-in" data-i="${i}" data-f="expiry_date" type="date" value="${esc(l.expiry_date||'')}"></td>
        <td><button class="ehp-btn g" data-del="${i}" style="padding:5px 8px;">✕</button></td></tr>`).join('')}
    </tbody></table>`;
    c.querySelectorAll('[data-f]').forEach(inp => inp.addEventListener('input', () => {
      _lines[+inp.getAttribute('data-i')][inp.getAttribute('data-f')] = inp.value;
    }));
    c.querySelectorAll('[data-del]').forEach(b => b.addEventListener('click', () => { _lines.splice(+b.getAttribute('data-del'),1); if(!_lines.length)_lines=[{}]; drawLines(); }));
  }

  function parseFile(file) {
    el('ehp-fname').textContent = file.name;
    if (!window.XLSX) { el('ehp-inmsg').innerHTML = msg('e','Spreadsheet library not loaded — enter lines manually.'); return; }
    const fr = new FileReader();
    fr.onload = e => {
      try {
        const wb = XLSX.read(new Uint8Array(e.target.result), { type:'array' });
        const rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { defval:'' });
        const norm = k => String(k||'').trim().toLowerCase().replace(/\s+/g,'_');
        const out = [];
        for (const r of rows) {
          const o = {}; for (const k of Object.keys(r)) o[norm(k)] = r[k];
          const sku = String(o.sku || o.sku_code || '').trim(); if (!sku) continue;
          out.push({ sku, qty: o.qty ?? o.quantity ?? 0, uom: String(o.uom || 'each').toLowerCase(),
                     lot_code: o.lot || o.lot_code || '', expiry_date: o.expiry || o.expiry_date || '' });
        }
        if (!out.length) { el('ehp-inmsg').innerHTML = msg('e','No usable rows — expected a "sku" column.'); return; }
        _lines = out; drawLines();
        el('ehp-inmsg').innerHTML = msg('k', `${out.length} line(s) loaded from ${esc(file.name)} — review, then Save.`);
      } catch (err) { el('ehp-inmsg').innerHTML = msg('e','Could not read file: ' + (err.message||err)); }
    };
    fr.readAsArrayBuffer(file);
  }

  async function saveReceipt() {
    const lines = _lines.filter(l => l.sku && Number(l.qty) > 0);
    const pallets = parseInt(el('ehp-pallets').value,10) || 0;
    if (!pallets && !lines.length) { el('ehp-inmsg').innerHTML = msg('e','Enter pallets received and/or at least one product line.'); return; }
    el('ehp-save').disabled = true;
    try {
      const r = await req('/ehp/receipt', { method:'POST', body: JSON.stringify({
        received_date_local: el('ehp-rdate').value, pallets, reference: el('ehp-ref').value || null,
        facility_code: 'VOZ_TX', lines }) });
      let out = msg('k', `Receipt saved — ${r.pallets} pallet(s), ${r.lines.length} line(s).`);
      if (r.warnings?.length) out += msg('w', 'No pack conversion set for: ' + r.warnings.map(w=>w.sku+' ('+w.uom+')').join(', ') + '. Quantity recorded at face value — set the conversion on the Inventory tab.');
      el('ehp-inmsg').innerHTML = out;
      _lines = [{}]; drawLines();
    } catch (e) { el('ehp-inmsg').innerHTML = msg('e', e.message || String(e)); }
    el('ehp-save').disabled = false;
  }

  // ── Inventory ──
  async function renderInventory(body) {
    const d = await req('/ehp/skus');
    const all = d.skus || [];
    const rows = all.filter(r => r.is_component);          // stocked items
    const others = all.filter(r => !r.is_component);       // Shopify product codes, not stock
    body.innerHTML = `
      <div style="font-size:10px;color:${LIGHT};margin-bottom:6px;">Periodic inventory: <b>estimated</b> on-hand between counts (each flavour averages sticks ÷ flavours per envelope). The fortnightly count replaces the estimate with truth.</div>
      <div id="ehp-period"></div>
      <div id="ehp-invmsg"></div>
      <table class="ehp"><thead><tr><th>SKU</th><th>Flavour</th><th class="n">Ledger</th><th class="n">Est. used</th><th class="n">Est. on hand</th><th class="n">Per inner</th><th class="n">Inner/carton</th><th class="n">Carton/pallet</th><th></th></tr></thead>
      <tbody>${rows.length ? rows.map(r=>`<tr>
        <td><b>${esc(r.sku)}</b></td><td>${esc(r.flavour||'—')}</td>
        <td class="n">${nfmt(r.ledger_on_hand)}</td>
        <td class="n" style="color:${MID}">${nfmt(r.estimated_used)}</td>
        <td class="n"><b style="color:${(r.estimated_on_hand||0) < 0 ? RED : DARK}">${nfmt(r.estimated_on_hand)}</b></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="eaches_per_inner" type="number" min="0" value="${r.eaches_per_inner??''}" style="width:70px;text-align:right"></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="inners_per_carton" type="number" min="0" value="${r.inners_per_carton??''}" style="width:70px;text-align:right"></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="cartons_per_pallet" type="number" min="0" value="${r.cartons_per_pallet??''}" style="width:70px;text-align:right"></td>
        <td style="white-space:nowrap"><button class="ehp-btn g" data-save="${esc(r.sku)}">Save</button></td>
      </tr>`).join('') : `<tr><td colspan="9" style="color:${LIGHT};text-align:center;padding:18px;">No stocked SKUs yet — they appear automatically on first receipt or order.</td></tr>`}
      </tbody></table>
      ${others.length ? `<div style="margin-top:14px;font-size:10px;color:${LIGHT};line-height:1.6;">
        <b>${others.length} SKU(s) seen on Shopify orders but never received into stock</b> — not consumed and not counted:
        ${others.map(o=>esc(o.sku)).join(', ')}.<br>They become stocked items the first time they appear on an inbound receipt.
      </div>` : ''}`;
    try {
      const cp = await req('/ehp/count-period');
      el('ehp-period').innerHTML = `<div class="ehp-msg k" style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
        <span>Open count period from <b>${esc(cp.open_period.period_start)}</b> — ${nfmt(cp.envelopes_in_period)} envelopes assembled, ${nfmt(cp.expected_sticks)} sticks expected.</span>
        <button class="ehp-btn" id="ehp-closeper">Record count &amp; close period</button></div>`;
      el('ehp-closeper').addEventListener('click', async () => {
        const counts = [];
        for (const r of rows) {
          const v = prompt(`Counted quantity (each) for ${r.sku}:`, '');
          if (v === null) return;
          const n = parseInt(v,10); if (Number.isFinite(n)) counts.push({ sku: r.sku, counted_each: n });
        }
        if (!counts.length) return;
        try {
          const res = await req('/ehp/count-period/'+cp.open_period.id+'/close', { method:'POST', body: JSON.stringify({ counts }) });
          el('ehp-invmsg').innerHTML = msg(res.variance_sticks === 0 ? 'k' : 'w',
            `Period closed. Derived usage ${nfmt(res.derived_sticks)} sticks vs ${nfmt(res.expected_sticks)} expected — variance ${res.variance_sticks>0?'+':''}${nfmt(res.variance_sticks)}. ${res.note}`);
          setTimeout(render, 1400);
        } catch (e) { el('ehp-invmsg').innerHTML = msg('e', e.message||String(e)); }
      });
    } catch (e) { /* period panel is optional */ }

    body.querySelectorAll('[data-save]').forEach(b => b.addEventListener('click', async () => {
      const sku = b.getAttribute('data-save'); const patch = {};
      body.querySelectorAll(`[data-sku="${sku}"]`).forEach(i => { patch[i.getAttribute('data-f')] = i.value === '' ? '' : parseInt(i.value,10); });
      b.disabled = true;
      try { await req('/ehp/sku/'+encodeURIComponent(sku), { method:'PATCH', body: JSON.stringify(patch) });
            el('ehp-invmsg').innerHTML = msg('k', `Conversions saved for ${esc(sku)}.`); }
      catch (e) { el('ehp-invmsg').innerHTML = msg('e', e.message||String(e)); }
      b.disabled = false;
    }));
  }

  // ── Recipe (structure only — which flavour fills which slot is decided on the floor) ──
  async function renderRecipe(body) {
    const d = await req('/ehp/recipe');
    const a = d.active;
    body.innerHTML = `
      <div class="ehp-kpis"><div class="ehp-kpi" style="grid-column:span 2;">
        <div class="ehp-kl">Active structure</div>
        <div class="ehp-kv" style="font-size:15px;">${a ? esc(a.sticks_per_envelope + ' sticks · ' + a.distinct_flavours + ' flavours · ' + (a.split_pattern||'')) : 'None'}</div>
        <div class="ehp-ks">${a ? esc(a.name) + ' — from ' + esc(a.effective_from) : 'Set the envelope structure before assembling.'}</div>
      </div></div>
      ${msg('w','Flavours are not fixed. The recipe defines the pattern only — 5 sticks across 3 distinct flavours, split 2/2/1. Actual per-flavour usage is derived at each stock count.')}
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-top:10px;">
        <div><span class="ehp-lbl">Sticks per envelope</span><input class="ehp-in" id="ehp-sticks" type="number" min="1" value="${a?a.sticks_per_envelope:5}" style="width:110px"></div>
        <div><span class="ehp-lbl">Distinct flavours</span><input class="ehp-in" id="ehp-flav" type="number" min="1" value="${a?a.distinct_flavours:3}" style="width:110px"></div>
        <div><span class="ehp-lbl">Split pattern</span><input class="ehp-in" id="ehp-split" value="${a?esc(a.split_pattern||'2,2,1'):'2,2,1'}" placeholder="2,2,1" style="width:120px"></div>
        <div><span class="ehp-lbl">Name</span><input class="ehp-in" id="ehp-rname" placeholder="Standard sample envelope"></div>
        <div><span class="ehp-lbl">Effective from</span><input class="ehp-in" id="ehp-rfrom" type="date" value="${new Date().toISOString().slice(0,10)}"></div>
        <button class="ehp-btn" id="ehp-rsave">Save new version</button>
      </div>
      <div id="ehp-rmsg"></div>
      ${(d.history||[]).length ? `<table class="ehp" style="margin-top:16px"><thead><tr><th>Version</th><th>Structure</th><th>From</th><th>To</th></tr></thead><tbody>${d.history.map(h=>`<tr><td>${esc(h.name)}</td><td>${esc((h.sticks_per_envelope||5)+' / '+(h.distinct_flavours||3)+' / '+(h.split_pattern||''))}</td><td>${esc(h.effective_from)}</td><td>${esc(h.effective_to||'current')}</td></tr>`).join('')}</tbody></table>`:''}`;
    el('ehp-rsave').addEventListener('click', async () => {
      try {
        await req('/ehp/recipe', { method:'POST', body: JSON.stringify({
          name: el('ehp-rname').value || null, effective_from: el('ehp-rfrom').value,
          sticks_per_envelope: parseInt(el('ehp-sticks').value,10),
          distinct_flavours: parseInt(el('ehp-flav').value,10),
          split_pattern: el('ehp-split').value }) });
        el('ehp-rmsg').innerHTML = msg('k','Structure saved.');
        setTimeout(render, 900);
      } catch (e) { el('ehp-rmsg').innerHTML = msg('e', e.message || String(e)); }
    });
  }

  // ── Shopify integration ──
  async function renderShopify(body) {
    const st = await req('/shopify/status');
    const dot = (on) => `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${on?GREEN:LIGHT};margin-right:6px;"></span>`;
    body.innerHTML = `
      ${!st.configured ? msg('e','The Shopify app is not configured on the server. Set SHOPIFY_API_KEY, SHOPIFY_API_SECRET and SHOPIFY_APP_URL in Render.') : ''}
      <div class="ehp-kpis">
        <div class="ehp-kpi"><div class="ehp-kl">Connection</div><div class="ehp-kv" style="font-size:15px;">${dot(st.connected)}${st.connected?'Connected':'Not connected'}</div><div class="ehp-ks">${esc(st.shop_domain||'no store linked')}</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Queued orders</div><div class="ehp-kv">${nfmt(st.queued_orders)}</div><div class="ehp-ks">awaiting a batch</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Last webhook</div><div class="ehp-kv" style="font-size:13px;">${esc(st.last_webhook_at||'—')}</div><div class="ehp-ks">orders/create</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Unfulfilled</div><div class="ehp-kv" style="color:${st.unfulfilled_dispatched?AMBER:DARK}">${nfmt(st.unfulfilled_dispatched)}</div><div class="ehp-ks">dispatched, not yet in Shopify</div></div>
      </div>
      ${st.last_error ? msg('e','Last error: ' + st.last_error) : ''}
      <div style="border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:14px;margin-top:6px;">
        <div style="font-size:12px;font-weight:700;color:${DARK};margin-bottom:8px;">1 · Link the store</div>
        <div style="display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;">
          <div><span class="ehp-lbl">Shopify store domain</span><input class="ehp-in" id="ehp-shop" value="${esc(st.shop_domain||'')}" placeholder="ehplabs.myshopify.com" style="width:280px"></div>
          <button class="ehp-btn" id="ehp-genlink">Generate install link</button>
        </div>
        <div id="ehp-linkout"></div>
        <div style="font-size:10px;color:${LIGHT};margin-top:8px;">Send the link to EHP. They click Install in their own Shopify admin — no tokens are copied or emailed. The redirect URL registered in the Partner dashboard must be exactly <code>${esc(st.callback_url||'(SHOPIFY_APP_URL not set)')}</code>.</div>
      </div>
      <div style="border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:14px;margin-top:10px;">
        <div style="font-size:12px;font-weight:700;color:${DARK};margin-bottom:8px;">2 · Keep it in sync</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <button class="ehp-btn g" id="ehp-poll" ${st.connected?'':'disabled'}>Poll for missed orders</button>
          <button class="ehp-btn g" id="ehp-retry" ${st.unfulfilled_dispatched?'':'disabled'}>Retry ${nfmt(st.unfulfilled_dispatched)} fulfilment(s)</button>
        </div>
        <div id="ehp-syncout"></div>
        <div style="font-size:10px;color:${LIGHT};margin-top:8px;">Last poll: ${esc(st.last_poll_at||'never')}. Webhooks are the primary path; polling catches anything missed.</div>
      </div>`;

    el('ehp-genlink')?.addEventListener('click', async () => {
      try {
        const r = await req('/shopify/connect', { method:'POST', body: JSON.stringify({ shop_domain: el('ehp-shop').value }) });
        el('ehp-linkout').innerHTML = `<div class="ehp-msg k" style="margin-top:10px;">
          <div style="margin-bottom:6px;">Install link for <b>${esc(r.shop_domain)}</b> — send this to EHP:</div>
          <input class="ehp-in" style="width:100%;font-family:monospace;font-size:10px;" readonly value="${esc(r.install_url)}" onclick="this.select()">
          <div style="margin-top:6px;font-size:10px;">Requests: ${esc(r.scopes)}</div></div>`;
      } catch (e) { el('ehp-linkout').innerHTML = msg('e', e.message || String(e)); }
    });
    el('ehp-poll')?.addEventListener('click', async () => {
      el('ehp-poll').disabled = true;
      try { const r = await req('/shopify/poll', { method:'POST', body: JSON.stringify({}) });
            el('ehp-syncout').innerHTML = msg('k', `Fetched ${r.fetched} order(s) — ${r.created} new, ${r.already_present} already present.`);
            setTimeout(render, 1200); }
      catch (e) { el('ehp-syncout').innerHTML = msg('e', e.message || String(e)); el('ehp-poll').disabled = false; }
    });
    el('ehp-retry')?.addEventListener('click', async () => {
      el('ehp-retry').disabled = true;
      try { const r = await req('/shopify/retry-fulfilments', { method:'POST', body: JSON.stringify({}) });
            el('ehp-syncout').innerHTML = msg(r.failed ? 'w' : 'k', `Retried ${r.attempted} — ${r.ok} succeeded, ${r.failed} failed.` + (r.errors?.length ? ' e.g. ' + esc(r.errors[0].error) : ''));
            setTimeout(render, 1200); }
      catch (e) { el('ehp-syncout').innerHTML = msg('e', e.message || String(e)); el('ehp-retry').disabled = false; }
    });
  }

  // ── init ──
  function init() {
    styles();
    window.openEhpOps = open;
    refreshEnabled();
    window.addEventListener('state:ready', refreshEnabled);
    setInterval(refreshEnabled, 15000);   // active client can change via the picker
    console.log('[ehp-ops] module v9 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
