/* ── VelOzity Pinpoint — EHP Ops v1 ──
   Fulfilment workspace for EHP at VOZ_TX: inbound receipts (pallets + lines),
   assembly queue → batches → assembled → dispatched, inventory + cycle counts,
   and the versioned kit recipe. Capability-gated: only appears when the active
   client has envelope_fulfilment. Rendered as an overlay, so it does not touch
   the page router. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#34C759', AMBER = '#FFD014', RED = '#B33F40';
  const AMBER_TXT = '#8A6D00';   // #FFD014 is too light for text on white
  let _enabled = false, _tab = 'queue', _cache = {};
  let _capClient = null;                         // client the capability answer applies to

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
      .ehp-kpi{border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:11px 13px;
               background:#fff;box-shadow:0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06);}
      /* Raised by default, matching the Week Hub and Geography pages. */
      .ehp-card{border:0.5px solid rgba(0,0,0,0.1);border-radius:12px;padding:14px 16px;background:#fff;
                box-shadow:0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06);}
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
      .ehp-msg.e{background:rgba(179,63,64,.09);border:0.5px solid rgba(179,63,64,.32);color:${RED};}
      .ehp-msg.w{background:rgba(255,208,20,.16);border:0.5px solid rgba(255,208,20,.55);color:${AMBER_TXT};}
      .ehp-msg.k{background:rgba(52,199,89,.1);border:0.5px solid rgba(52,199,89,.3);color:#1f7a34;}
      .ehp-drop{border:1.5px dashed rgba(0,0,0,0.18);border-radius:10px;padding:12px;text-align:center;font-size:11px;color:${MID};cursor:pointer;}
    `;
    document.head.appendChild(s);
  }

  // ── nav visibility ──
  async function refreshEnabled() {
    const activeClient = window.pinpointClient || 'unknown';
    if (_capClient === activeClient) {           // already known — don't re-probe
      const sub0 = el('nav-vas-fulfilment');
      if (sub0) sub0.style.display = _enabled ? '' : 'none';
      return;
    }
    try {
      await req('/ehp/queue');           // 409 when the active client lacks the capability
      _enabled = true;
    } catch (e) {
      // Fail closed: a 401 on a cold load must not be read as "capability present".
      if (e.status === 409 || e.status === 403) _enabled = false;
      else return;                       // transient — leave the nav as it is and retry
    }
    _capClient = activeClient;
    const sub = el('nav-vas-fulfilment');
    if (sub) sub.style.display = _enabled ? '' : 'none';
    const legacy = el('nav-ehp');                      // pre-restructure nav item, if still present
    if (legacy) legacy.style.display = _enabled ? '' : 'none';
  }

  // ── shell ──
  function close(){ const o = document.querySelector('.ehp-ov'); if (o) o.remove(); stopLive(); }
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
    renderTabs(); render(); startLive();
  }

  function renderTabs() {
    const tabs = [['queue','Assembly'],['inbound','Inbound'],['inventory','Inventory'],['recipe','Recipe'],['shopify','Shopify']];
    const c = el('ehp-tabs'); if (!c) return;
    c.innerHTML = tabs.map(([k,l]) => `<button class="ehp-tab ${_tab===k?'on':''}" data-tab="${k}">${l}</button>`).join('');
    c.querySelectorAll('[data-tab]').forEach(b => b.addEventListener('click', () => { _tab = b.getAttribute('data-tab'); renderTabs(); render(); }));
  }

  function msg(kind, text){ return `<div class="ehp-msg ${kind}">${esc(text)}</div>`; }

  // True while a render is in flight, so a background tick cannot start a second one on
  // top of the first — overlapping renders were part of why this felt like it was
  // constantly reloading.
  let _rendering = false;

  // opts.background: fetch first and leave the current content on screen until the new
  // content is ready. Only a first open, or a tab change, shows "Loading…" — a background
  // refresh that blanks the panel reads as the page hanging.
  async function render(opts) {
    const background = !!(opts && opts.background);
    const body = el('ehp-body'); if (!body) return;
    if (_rendering) return;
    _rendering = true;
    // Never redraw under someone's cursor: the inventory tab has editable fields, and a
    // refresh mid-entry would discard what they had typed.
    if (background && document.activeElement &&
        /INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName) &&
        body.contains(document.activeElement)) { _rendering = false; return; }

    // Rendered into the real panel, never a detached node: the renderers wire their
    // buttons with getElementById, which returns null outside the document, so a detached
    // build would swap in dead controls. Each renderer awaits its data before writing, so
    // the existing content already survives the fetch — the only thing that blanked the
    // panel was the "Loading…" line, and a background pass simply skips it.
    if (!background) body.innerHTML = 'Loading…';
    try {
      if (_tab === 'queue')     await renderQueue(body);
      else if (_tab === 'inbound')   await renderInbound(body);
      else if (_tab === 'inventory') await renderInventory(body);
      else if (_tab === 'recipe')    await renderRecipe(body);
      else if (_tab === 'shopify')   await renderShopify(body);
    } catch (e) {
      // A background failure leaves the last good content in place rather than replacing
      // it with an error the user did not ask for.
      if (!background) {
        body.innerHTML = msg('e', e.status === 409
          ? 'Switch the Client selector to EHP to use this workspace.'
          : ('Could not load: ' + (e.message || e)));
      } else {
        console.warn('[ehp-ops] background refresh failed:', e.message || e);
      }
    } finally { _rendering = false; }
  }

  // ── Assembly: queue + batches ──
  async function renderQueue(body) {
    const [q, batches] = await Promise.all([req('/ehp/queue'), req('/ehp/batches')]);
    _cache.recipe = q.active_recipe;
    const b = batches.batches || [];
    const lines = q.lines || [];
    const unmapped = q.unmapped || [];
    // A batch is one product line, so the queue is presented per line rather than as one
    // pooled number — the pooled figure would suggest a batch that cannot be built.
    const lineCards = lines.map(l => `
        <div class="ehp-kpi">
          <div class="ehp-kl">${esc(l.product_line)}</div>
          <div class="ehp-kv">${nfmt(l.queued_envelopes)}</div>
          <div class="ehp-ks">${nfmt(l.queued_orders)} order(s) · ${l.recipe ? esc(l.recipe.name) : '<span style="color:'+RED+'">no recipe</span>'}</div>
        </div>`).join('');
    body.innerHTML = `
      <div class="ehp-kpis">
        ${lineCards || `<div class="ehp-kpi"><div class="ehp-kl">Queued orders</div><div class="ehp-kv">${nfmt(q.queued_orders)}</div><div class="ehp-ks">awaiting a batch</div></div>`}
        <div class="ehp-kpi"><div class="ehp-kl">Queued envelopes</div><div class="ehp-kv">${nfmt(q.queued_envelopes)}</div><div class="ehp-ks">billable units</div></div>
        <div class="ehp-kpi"><div class="ehp-kl">Flagged orders</div><div class="ehp-kv" style="color:${q.flagged_high_qty?AMBER_TXT:DARK}">${nfmt(q.flagged_high_qty)}</div><div class="ehp-ks">unusually large</div></div>
      </div>
      ${unmapped.length ? msg('w','<b>'+unmapped.reduce((a,u)=>a+u.orders,0)+' queued order(s) cannot be batched</b> — their Shopify SKU is not mapped to a product line: '
          + unmapped.map(u=>'<code>'+esc(u.product_sku||'(no SKU)')+'</code> ('+u.orders+')').join(', ')
          + '. Map them on the Recipe tab.') : ''}
      ${lines.length && lines.some(l=>!l.ready) ? msg('w','A product line has no flavour pool on its recipe — dispatch will be refused until the flavours are set.') : ''}
      ${!lines.length && !unmapped.length ? msg('w','No queued orders carry a product line yet. Map the Shopify SKUs on the Recipe tab.') : ''}
      <div style="display:flex;gap:8px;align-items:flex-end;margin:10px 0 4px;">
        <div><span class="ehp-lbl">Product line</span><select class="ehp-sel" id="ehp-bline" style="width:150px;">
          ${lines.length ? lines.map(l=>`<option value="${esc(l.product_line)}" ${l.ready?'':'disabled'}>${esc(l.product_line)} (${nfmt(l.queued_envelopes)})</option>`).join('')
                         : '<option value="">none available</option>'}
        </select></div>
        <div><span class="ehp-lbl">Envelopes in this batch</span><input class="ehp-in" id="ehp-target" type="number" min="1" step="1" value="500" style="width:130px;"></div>
        <button class="ehp-btn" id="ehp-mkbatch" ${lines.some(l=>l.ready)?'':'disabled'}>Create batch</button>
        <div style="font-size:10px;color:${LIGHT};padding-bottom:8px;">Whole orders only — the actual count may land slightly above your target.</div>
      </div>
      <div id="ehp-batchmsg"></div>
      <table class="ehp"><thead><tr><th>Batch</th><th>Line</th><th class="n">Target</th><th class="n">Envelopes</th><th class="n">Orders</th><th>State</th><th class="n">Labels</th><th class="n">Shopify</th><th>Assembled (CT)</th><th>Dispatched (CT)</th><th></th></tr></thead>
      <tbody>${b.length ? b.map(x => `<tr>
        <td style="font-family:monospace;font-size:10px;"><b>${esc(x.batch_ref || String(x.id).slice(-8))}</b></td>
        <td style="font-size:10px;color:${MID}">${esc(x.product_line || '—')}</td>
        <td class="n">${nfmt(x.target_envelopes)}</td><td class="n"><b>${nfmt(x.actual_envelopes)}</b></td><td class="n">${nfmt(x.order_count)}</td>
        <td><span class="ehp-chip" style="background:${x.state==='dispatched'?'rgba(52,199,89,.14)':x.state==='assembled'?'rgba(255,208,20,.28)':'rgba(0,0,0,.06)'};color:${x.state==='dispatched'?GREEN:x.state==='assembled'?AMBER_TXT:MID};">${esc(x.state)}</span></td>
        <td class="n" style="font-size:10px;">${(() => {
          const f = x.fulfilment || {};
          const done = f.labelled || 0, tot = f.total || x.order_count || 0;
          if (!tot) return `<span style="color:${LIGHT}">—</span>`;
          if (done === 0)   return `<span style="color:${LIGHT}">not printed</span>`;
          if (done >= tot)  return `<span style="color:${GREEN}">✓ ${nfmt(f.labelled_envelopes||0)}</span>`;
          return `<span style="color:${AMBER_TXT}">${done}/${tot}</span>`;
        })()}</td>
        <td class="n" style="font-size:10px;">${x.state==='dispatched'
          ? `<span style="color:${GREEN}">${(x.fulfilment&&x.fulfilment.fulfilled)||0} ok</span>` +
            (((x.fulfilment&&x.fulfilment.not_fulfilled)||0) ? ` · <span style="color:${AMBER_TXT}">${x.fulfilment.not_fulfilled} not sent</span>` : '')
          : `<span style="color:${LIGHT}">—</span>`}</td>
        <td style="font-size:10px;color:${MID}">${esc(localTs(x.assembled_at))}</td>
        <td style="font-size:10px;color:${MID}">${esc(localTs(x.dispatched_at))}</td>
        <td style="white-space:nowrap;">
          ${x.state==='queued' ? `<button class="ehp-btn g" data-asm="${esc(x.id)}">Assemble</button>` : ''}
          ${x.state==='assembled' ? `<button class="ehp-btn g" data-dis="${esc(x.id)}">Dispatch</button>` : ''}
          <button class="ehp-btn g" data-pick="${esc(x.id)}">Pick list</button>
          ${x.state==='dispatched' && ((x.fulfilment&&x.fulfilment.not_fulfilled)||0) ? `<button class="ehp-btn g" data-why="${esc(x.id)}">Why?</button>` : ''}
        </td></tr>`).join('') : `<tr><td colspan="11" style="color:${LIGHT};text-align:center;padding:18px;">No batches yet.</td></tr>`}
      </tbody></table>`;

    el('ehp-mkbatch')?.addEventListener('click', async () => {
      const target = parseInt(el('ehp-target').value, 10) || 0;
      const line = el('ehp-bline') ? el('ehp-bline').value : '';
      if (target < 1) return;
      if (!line) { el('ehp-batchmsg').innerHTML = msg('e','Choose a product line — a batch cannot mix OxyShred and Fizz Stix.'); return; }
      el('ehp-mkbatch').disabled = true;
      try {
        const r = await req('/ehp/batch', { method:'POST', body: JSON.stringify({ target_envelopes: target, facility_code:'VOZ_TX', product_line: line }) });
        el('ehp-batchmsg').innerHTML = msg('k', `Batch ${esc(r.batch_ref||'')} created — ${r.actual_envelopes} ${esc(r.product_line)} envelopes across ${r.order_count} orders (target ${r.target_envelopes}).`);
        window.dispatchEvent(new CustomEvent('ehp:changed'));
        setTimeout(render, 900);
      } catch (e) { el('ehp-batchmsg').innerHTML = msg('e', e.message || String(e)); el('ehp-mkbatch').disabled = false; }
    });
    body.querySelectorAll('[data-asm]').forEach(x => x.addEventListener('click', async () => {
      x.disabled = true;
      try {
        const r = await req('/ehp/batch/'+x.getAttribute('data-asm')+'/assemble', { method:'POST', body:'{}' });
        el('ehp-batchmsg').innerHTML = msg('k', `Assembled ${nfmt(r.envelopes)} envelope(s) · ${nfmt(r.sticks_consumed_total||0)} sticks. Stock is deducted on dispatch.`);
        window.dispatchEvent(new CustomEvent('ehp:changed'));
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
        const cons = r.consumed || {};
        const consTxt = (cons.by_flavour && cons.by_flavour.length)
          ? ` Deducted ${nfmt(cons.total_sticks)} sticks — ` + cons.by_flavour.map(f=>esc(f.sku)+' '+nfmt(f.qty)).join(', ') + '.'
          : '';
        el('ehp-batchmsg').innerHTML = msg(shKind, `Dispatched ${nfmt(r.envelopes)} ${esc(r.product_line||'')} envelope(s) across ${nfmt(r.orders)} order(s).${consTxt} ` + shTxt);
        window.dispatchEvent(new CustomEvent('ehp:changed'));
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
      const batchId = x.getAttribute('data-pick');
      try {
        const r = await req('/ehp/batch/' + batchId + '/picklist');
        openPickList(batchId, r);
      } catch (e) { alert('Pick list failed: ' + (e.message || e)); }
    }));
  }

  // ── Pick list + envelope labels ──
  // Labels are 76 x 36 mm, one per envelope: an order for three envelopes prints three
  // identical labels. Addresses are set in caps, which is the USPS convention for
  // machine-readable mail.
  // 30% smaller than the original 76 x 36mm. Type and padding scale with it: the label is
  // overflow:hidden, so text that no longer fits is silently cut off rather than shrinking —
  // a truncated address looks like a data problem, not a layout one.
  const LABEL_W = '53mm', LABEL_H = '25mm';

  function labelLines(o) {
    const cityLine = [o.recipient_city, o.recipient_state, o.recipient_postcode].filter(Boolean).join(' ');
    const country = String(o.recipient_country || '').toUpperCase();
    return [
      o.recipient_name || '',
      o.recipient_address || '',
      o.recipient_address2 || '',
      cityLine,
      (country && country !== 'US' && country !== 'USA') ? country : '',
    ].filter(Boolean);
  }

  function openPickList(batchId, data) {
    const orders = data.orders || [];
    const w = window.open('', '_blank');
    if (!w) { alert('Pop-up blocked. Allow pop-ups for Pinpoint to print labels.'); return; }

    const rows = orders.map(o => {
      const done = !!o.labels_generated_at;
      return '<tr data-id="' + esc(o.id) + '" data-qty="' + (o.envelope_qty || 1) + '">'
        + '<td class="c"><input type="checkbox" class="sel" checked></td>'
        + '<td>' + esc(o.order_number || '') + (o.flagged_high_qty ? ' <span class="flag">&#9873;</span>' : '') + '</td>'
        + '<td class="n">' + (o.envelope_qty || 1) + '</td>'
        + '<td>' + esc(o.recipient_name || '') + '</td>'
        + '<td>' + esc(o.recipient_address || '') + '</td>'
        + '<td>' + esc(o.recipient_address2 || '') + '</td>'
        + '<td>' + esc(o.recipient_city || '') + '</td>'
        + '<td>' + esc(o.recipient_state || '') + '</td>'
        + '<td class="zip">' + esc(o.recipient_postcode || '') + '</td>'
        + '<td class="c">' + (done ? '<span class="ok">printed</span>' : '<span class="pend">&mdash;</span>') + '</td>'
        + '</tr>';
    }).join('');

    const payload = JSON.stringify(orders.map(o => ({
      id: o.id, order_number: o.order_number, qty: o.envelope_qty || 1, lines: labelLines(o),
    })));

    const doc = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Pick list &amp; labels</title><style>'
      + 'body{font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;color:#1C1C1E;margin:24px;}'
      + 'h2{font-size:17px;margin:0 0 3px;color:#990033;} .meta{color:#6E6E73;font-size:12px;margin-bottom:14px;}'
      + '.bar{position:sticky;top:0;background:#fff;padding:10px 0;border-bottom:1px solid #eee;display:flex;gap:10px;align-items:center;z-index:5;}'
      + 'button{border:none;border-radius:8px;padding:9px 15px;font:600 12px inherit;cursor:pointer;}'
      + '.primary{background:#990033;color:#fff;} .ghost{background:#F5F5F7;color:#6E6E73;}'
      + 'button:disabled{opacity:.45;cursor:default;}'
      + 'table{width:100%;border-collapse:collapse;margin-top:14px;}'
      + 'th,td{border:1px solid #ddd;padding:6px 8px;vertical-align:top;text-align:left;font-size:12px;word-break:break-word;}'
      + 'th{background:#F5F5F7;font-size:10px;text-transform:uppercase;letter-spacing:.04em;color:#6E6E73;}'
      + 'td.n,th.n{text-align:right;} td.c,th.c{text-align:center;} td.zip{white-space:nowrap;}'
      + '.flag{color:#8A6D00;font-weight:600;} .ok{color:#1f7a34;font-weight:600;font-size:11px;} .pend{color:#AEAEB2;}'
      + '.hint{font-size:11px;color:#6E6E73;}'
      + '.pull{margin-top:14px;padding:10px 14px;background:#F5F5F7;border-radius:10px;font-size:12px;}'
      // labels: exact page size, one label per page
      + '#labels{display:none;}'
      + '@media screen{#labels{margin-top:18px;} #labels.show{display:block;}'
      + '  .label{width:' + LABEL_W + ';height:' + LABEL_H + ';border:1px dashed #bbb;margin:0 0 6px;}}'
      + '.label{box-sizing:border-box;padding:2.4mm 2.8mm;overflow:hidden;'
      + '  font-family:Helvetica,Arial,sans-serif;text-transform:uppercase;}'
      // The reference is held at 4.5pt rather than scaled the full 30% — below about 4pt it
      // stops being readable on a thermal printer, and it is there to be read on the floor.
      + '.label .ref{font-size:4.5pt;color:#888;letter-spacing:.03em;margin-bottom:0.4mm;}'
      + '.label .nm{font-size:6.8pt;font-weight:700;line-height:1.22;}'
      + '.label .ln{font-size:6.4pt;line-height:1.25;}'
      + '@media print{'
      + '  @page{size:' + LABEL_W + ' ' + LABEL_H + ';margin:0;}'
      + '  body{margin:0;} .noprint{display:none !important;}'
      + '  #labels{display:block !important;}'
      + '  .label{border:none;page-break-after:always;break-after:page;width:' + LABEL_W + ';height:' + LABEL_H + ';}'
      + '  .label:last-child{page-break-after:auto;break-after:auto;}'
      + '}</style></head><body>'
      + '<div class="noprint">'
      + '<h2>Pick list &amp; envelope labels</h2>'
      + '<div class="meta">Batch ' + esc(data.batch_ref || batchId)
      +   (data.product_line ? ' &middot; <b>' + esc(data.product_line) + '</b>' : '')
      +   ' &middot; ' + orders.length + ' order(s) &middot; '
      +   (data.envelope_count || 0) + ' envelope(s) &middot; ' + new Date().toLocaleString() + '</div>'
      // What to pull off the shelf. Identical to the deduction dispatch will post, so the
      // floor and the ledger are asked for the same quantities.
      + ((data.pull && data.pull.length) ? '<div class="pull"><b>Pull from stock</b>'
          + ' &middot; ' + esc((data.recipe && data.recipe.name) || '')
          + ' &middot; ' + ((data.recipe && data.recipe.sticks_per_envelope) || 5) + ' sticks per envelope'
          + '<table style="width:auto;margin-top:6px;"><thead><tr><th>Flavour SKU</th><th class="n">Sticks</th></tr></thead><tbody>'
          + data.pull.map(f => '<tr><td>' + esc(f.sku) + '</td><td class="n"><b>' + f.qty + '</b></td></tr>').join('')
          + '</tbody></table>'
          + '<div class="hint">Any split across these three is fine, so long as every envelope gets all three flavours and five sticks in total.</div>'
          + '</div>' : '')
      + '<div class="bar">'
      +   '<button class="ghost" id="all">Select all</button>'
      +   '<button class="ghost" id="none">Clear</button>'
      +   '<button class="primary" id="print">Print labels</button>'
      +   '<span class="hint" id="count"></span>'
      + '</div>'
      + '<table><thead><tr><th class="c">Print</th><th>Order</th><th class="n">Env</th><th>Name</th>'
      +   '<th>Address 1</th><th>Address 2</th><th>City</th><th>State</th><th>Zip</th><th class="c">Labels</th></tr></thead>'
      + '<tbody>' + rows + '</tbody></table>'
      + '<div class="hint" style="margin-top:10px;">Labels are ' + LABEL_W + ' &times; ' + LABEL_H
      +   ', one per envelope. <strong>Print labels</strong> downloads a PDF where every page is exactly one label, '
      +   'so it prints correctly on label stock without changing any print settings.</div>'
      + '</div>'
      + '<div id="labels"></div>'
      + '<script>(function(){'
      + 'var DATA=' + payload + ';'
      + 'var BATCH=' + JSON.stringify(batchId) + ';'
      + 'function sel(){return Array.prototype.filter.call(document.querySelectorAll("tbody tr"),function(tr){return tr.querySelector(".sel").checked;});}'
      + 'function refresh(){var t=sel(),e=0;t.forEach(function(tr){e+=parseInt(tr.getAttribute("data-qty"),10)||1;});'
      + '  document.getElementById("count").textContent=t.length+" order(s) selected \\u00b7 "+e+" label(s) to print";'
      + '  document.getElementById("print").disabled=!t.length;'
      + '  document.getElementById("print").textContent="Print "+e+" label"+(e===1?"":"s");}'
      + 'document.addEventListener("change",function(ev){if(ev.target.classList.contains("sel"))refresh();});'
      + 'document.getElementById("all").onclick=function(){document.querySelectorAll(".sel").forEach(function(c){c.checked=true;});refresh();};'
      + 'document.getElementById("none").onclick=function(){document.querySelectorAll(".sel").forEach(function(c){c.checked=false;});refresh();};'
      + 'document.getElementById("print").onclick=function(){'
      + '  var ids=sel().map(function(tr){return tr.getAttribute("data-id");});'
      + '  window.opener.__ehpLabelPdf(BATCH, ids);'
      + '  var box=document.getElementById("labels"),html="";'
      + '  ids.forEach(function(id){var o=DATA.filter(function(d){return String(d.id)===String(id);})[0];if(!o)return;'
      + '    for(var i=1;i<=o.qty;i++){'
      + '      html+="<div class=\\"label\\"><div class=\\"ref\\">"+(o.order_number||"")+(o.qty>1?(" \\u00b7 "+i+"/"+o.qty):"")+"</div>";'
      + '      html+="<div class=\\"nm\\">"+(o.lines[0]||"")+"</div>";'
      + '      for(var k=1;k<o.lines.length;k++){html+="<div class=\\"ln\\">"+o.lines[k]+"</div>";}'
      + '      html+="</div>";}});'
      + '  box.innerHTML=html;box.classList.add("show");'
      + '  document.getElementById("count").textContent="PDF generated \\u2014 check your downloads";'
      + '};'
      + 'refresh();'
      + '})();<\/script></body></html>';

    w.document.write(doc);
    w.document.close();
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
      window.dispatchEvent(new CustomEvent('ehp:changed'));
    } catch (e) { el('ehp-inmsg').innerHTML = msg('e', e.message || String(e)); }
    el('ehp-save').disabled = false;
  }

  // ── Inventory ──
  async function renderInventory(body) {
    const d = await req('/ehp/skus');
    const all = d.skus || [];
    const rows = all.filter(r => r.is_component);          // stocked items
    const others = all.filter(r => !r.is_component);       // Shopify product codes, not stock
    // Reference counts drive the delete confirmation, so the operator sees what goes
    // with the SKU before agreeing to it. Optional: an older API just omits the panel.
    let audit = null; try { audit = await req('/ehp/sku-audit'); } catch (e) {}
    const refsOf = (sku) => { const a = (audit && audit.skus || []).find(x => x.sku === sku); return a ? a.refs : null; };
    body.innerHTML = `
      <div style="font-size:10px;color:${LIGHT};margin-bottom:6px;">Periodic inventory: <b>estimated</b> on-hand between counts (each flavour averages sticks ÷ flavours per envelope). The fortnightly count replaces the estimate with truth.</div>
      <div id="ehp-period"></div>
      <div id="ehp-invmsg"></div>
      <table class="ehp"><thead><tr><th>SKU</th><th>Line</th><th>Flavour</th><th class="n">Ledger</th><th class="n">Est. used</th><th class="n">Est. on hand</th><th class="n">Per inner</th><th class="n">Inner/carton</th><th class="n">Carton/pallet</th><th></th></tr></thead>
      <tbody>${rows.length ? rows.map(r=>`<tr>
        <td><b>${esc(r.sku)}</b></td>
        <td><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="product_line" value="${esc(r.product_line||'')}" placeholder="line" style="width:96px"></td>
        <td>${esc(r.flavour||'—')}</td>
        <td class="n">${nfmt(r.ledger_on_hand)}</td>
        <td class="n" style="color:${MID}">${nfmt(r.estimated_used)}</td>
        <td class="n"><b style="color:${(r.estimated_on_hand||0) < 0 ? RED : DARK}">${nfmt(r.estimated_on_hand)}</b></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="eaches_per_inner" type="number" min="0" value="${r.eaches_per_inner??''}" style="width:70px;text-align:right"></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="inners_per_carton" type="number" min="0" value="${r.inners_per_carton??''}" style="width:70px;text-align:right"></td>
        <td class="n"><input class="ehp-in" data-sku="${esc(r.sku)}" data-f="cartons_per_pallet" type="number" min="0" value="${r.cartons_per_pallet??''}" style="width:70px;text-align:right"></td>
        <td style="white-space:nowrap"><button class="ehp-btn g" data-save="${esc(r.sku)}">Save</button>
          <button class="ehp-btn g" data-del="${esc(r.sku)}" style="color:${RED};margin-left:4px;">Delete</button></td>
      </tr>`).join('') : `<tr><td colspan="10" style="color:${LIGHT};text-align:center;padding:18px;">No stocked SKUs yet — they appear on their first inbound receipt.</td></tr>`}
      </tbody></table>
      ${others.length ? `<div style="margin-top:14px;">
        <div style="font-size:10px;color:${LIGHT};line-height:1.6;margin-bottom:6px;">
          <b>${others.length} SKU(s) seen on Shopify orders but never received into stock</b> — not consumed and not counted.
          They become stocked items the first time they appear on an inbound receipt.
        </div>
        <table class="ehp"><tbody>${others.map(o=>`<tr>
          <td><b>${esc(o.sku)}</b></td>
          <td style="color:${MID}">${esc(o.name||'—')}</td>
          <td style="color:${LIGHT}">${esc(o.source||'')}</td>
          <td style="text-align:right;white-space:nowrap"><button class="ehp-btn g" data-del="${esc(o.sku)}" style="color:${RED}">Delete</button></td>
        </tr>`).join('')}</tbody></table>
      </div>` : ''}
      ${(audit && (audit.suppressed||[]).length) ? `<div style="margin-top:14px;font-size:10px;color:${LIGHT};">
        <b>Deleted SKUs</b> — these will not re-register from Shopify orders:
        <table class="ehp"><tbody>${audit.suppressed.map(s=>`<tr>
          <td><b>${esc(s.sku)}</b></td><td style="color:${MID}">${esc(s.reason||'')}</td>
          <td style="text-align:right"><button class="ehp-btn g" data-restore="${esc(s.sku)}">Restore</button></td>
        </tr>`).join('')}</tbody></table>
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
      body.querySelectorAll(`[data-sku="${sku}"]`).forEach(i => {
        const f = i.getAttribute('data-f');
        // product_line is text; the pack conversions are integers.
        patch[f] = f === 'product_line' ? i.value.trim().toUpperCase()
                 : (i.value === '' ? '' : parseInt(i.value,10));
      });
      b.disabled = true;
      try { await req('/ehp/sku/'+encodeURIComponent(sku), { method:'PATCH', body: JSON.stringify(patch) });
            el('ehp-invmsg').innerHTML = msg('k', `Conversions saved for ${esc(sku)}.`); }
      catch (e) { el('ehp-invmsg').innerHTML = msg('e', e.message||String(e)); }
      b.disabled = false;
    }));

    // Delete: two steps always. The first click reports what is attached to the SKU;
    // only a second, explicit confirmation removes anything.
    body.querySelectorAll('[data-del]').forEach(b => b.addEventListener('click', async () => {
      const sku = b.getAttribute('data-del');
      const rf = refsOf(sku);
      const attached = rf ? rf.total : null;
      let cascade = false;
      if (attached === null) {
        if (!confirm('Delete SKU ' + sku + '?\n\nReference check unavailable — the server will refuse if records point at it.')) return;
      } else if (attached === 0) {
        if (!confirm('Delete SKU ' + sku + '?\n\nNothing references it. The registry row is removed and the code will not re-register from Shopify orders.')) return;
      } else {
        const detail = ['receipt lines: ' + rf.receipt_lines, 'inventory txns: ' + rf.inventory_txns,
                        'recipe lines: ' + rf.recipe_lines, 'stock counts: ' + rf.stock_counts].join('\n');
        if (!confirm('SKU ' + sku + ' is referenced by ' + attached + ' record(s):\n\n' + detail +
                     '\n\nDeleting it removes ALL of the above. This cannot be undone. Continue?')) return;
        if (!confirm('Confirm again: permanently delete ' + sku + ' and its ' + attached + ' referencing record(s)?')) return;
        cascade = true;
      }
      b.disabled = true;
      try {
        const r = await req('/ehp/sku/' + encodeURIComponent(sku),
          { method: 'DELETE', body: JSON.stringify({ confirm: 'DELETE', cascade: cascade ? '1' : '0' }) });
        el('ehp-invmsg').innerHTML = msg('k', 'Deleted ' + esc(sku) + '.' +
          (r.deleted && r.deleted.inventory_txns ? ' Removed ' + r.deleted.inventory_txns + ' ledger entr(ies).' : ''));
        setTimeout(render, 900);
      } catch (e) { el('ehp-invmsg').innerHTML = msg('e', e.message||String(e)); b.disabled = false; }
    }));

    body.querySelectorAll('[data-restore]').forEach(b => b.addEventListener('click', async () => {
      const sku = b.getAttribute('data-restore');
      b.disabled = true;
      try { await req('/ehp/sku/' + encodeURIComponent(sku) + '/restore', { method: 'POST', body: '{}' });
            el('ehp-invmsg').innerHTML = msg('k', esc(sku) + ' restored — it re-registers on its next receipt or order.');
            setTimeout(render, 900); }
      catch (e) { el('ehp-invmsg').innerHTML = msg('e', e.message||String(e)); b.disabled = false; }
    }));
  }

  // ── Recipe (structure only — which flavour fills which slot is decided on the floor) ──
  async function renderRecipe(body) {
    const [d, pm] = await Promise.all([req('/ehp/recipe'), req('/ehp/product-map').catch(()=>({}))]);
    const byLine = d.by_line || {};
    const lineNames = d.product_lines || [];
    const comps = d.components || [];
    const map = pm.map || [], unmappedSkus = pm.unmapped || [];

    // One card per product line: its active recipe and its flavour pool.
    const lineCard = (pl) => {
      const a = byLine[pl];
      const pool = (a && a.pool) || [];
      return `<div class="ehp-card" style="margin-top:12px;">
        <div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px;">
          <div style="font-size:13px;font-weight:700;color:${DARK};">${esc(pl)}</div>
          <div style="font-size:10px;color:${LIGHT};">${a ? esc(a.name)+' — from '+esc(a.effective_from) : 'no active recipe'}</div>
        </div>
        <div style="font-size:12px;color:${MID};margin-top:3px;">
          ${a ? esc((a.sticks_per_envelope||5)+' sticks · '+(a.distinct_flavours||3)+' flavours · nominal split '+(a.split_pattern||'')) : '—'}
        </div>
        <div style="margin-top:6px;font-size:11px;">Flavour pool:
          ${pool.length ? pool.map(x=>`<span class="ehp-chip" style="background:rgba(0,0,0,.05);color:${DARK};margin-right:4px;">${esc(x)}</span>`).join('')
                        : `<span style="color:${RED}">not set — dispatch will be refused</span>`}
        </div>
      </div>`;
    };

    const flavourPicker = (idx) => `<select class="ehp-sel" data-pool="${idx}" style="width:190px;">
        <option value="">— choose flavour SKU —</option>
        ${comps.map(c=>`<option value="${esc(c.sku)}">${esc(c.sku)}${c.flavour?' ('+esc(c.flavour)+')':''}</option>`).join('')}
      </select>`;

    body.innerHTML = `
      ${msg('w','Within an envelope the split rotates — all three of the line\'s flavours appear and the sticks total five, but not always in the same proportion. Stock is therefore deducted evenly across the pool (5 ÷ 3) and trued up at each cycle count.')}
      ${lineNames.length ? lineNames.map(lineCard).join('') : msg('w','No product lines yet — map a Shopify SKU below, then create a recipe for that line.')}

      <div class="ehp-card" style="margin-top:16px;">
        <div style="font-size:12px;font-weight:700;color:${DARK};margin-bottom:8px;">New recipe version</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
          <div><span class="ehp-lbl">Product line</span><input class="ehp-in" id="ehp-rline" list="ehp-lines" placeholder="OXYSHRED" style="width:140px">
            <datalist id="ehp-lines">${lineNames.map(l=>`<option value="${esc(l)}">`).join('')}</datalist></div>
          <div><span class="ehp-lbl">Sticks per envelope</span><input class="ehp-in" id="ehp-sticks" type="number" min="1" value="5" style="width:110px"></div>
          <div><span class="ehp-lbl">Distinct flavours</span><input class="ehp-in" id="ehp-flav" type="number" min="1" value="3" style="width:110px"></div>
          <div><span class="ehp-lbl">Nominal split</span><input class="ehp-in" id="ehp-split" value="2,2,1" placeholder="2,2,1" style="width:110px"></div>
          <div><span class="ehp-lbl">Name</span><input class="ehp-in" id="ehp-rname" placeholder="OxyShred sample envelope"></div>
          <div><span class="ehp-lbl">Effective from</span><input class="ehp-in" id="ehp-rfrom" type="date" value="${new Date().toISOString().slice(0,10)}"></div>
        </div>
        <div style="margin-top:10px;">
          <span class="ehp-lbl">Flavour pool — the three flavours this line consumes</span>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:4px;">${[0,1,2].map(flavourPicker).join('')}</div>
          <div style="font-size:10px;color:${LIGHT};margin-top:4px;">Flavours must already exist as stocked SKUs. Add them on the Inventory tab or via an inbound receipt first.</div>
        </div>
        <button class="ehp-btn" id="ehp-rsave" style="margin-top:12px;">Save new version</button>
        <div id="ehp-rmsg"></div>
      </div>

      <div class="ehp-card" style="margin-top:16px;">
        <div style="font-size:12px;font-weight:700;color:${DARK};">Shopify SKU &rarr; product line</div>
        <div style="font-size:10px;color:${LIGHT};margin-bottom:8px;">Shopify sends a product SKU (e.g. <code>oxyshredstickpack-us</code>), never a flavour. This map is the only thing that tells Pinpoint which recipe an order needs.</div>
        ${unmappedSkus.length ? msg('w','Seen on orders but not mapped: '+unmappedSkus.map(u=>'<code>'+esc(u.product_sku)+'</code> ('+u.orders+' order(s))').join(', ')) : ''}
        <table class="ehp"><thead><tr><th>Shopify SKU</th><th>Product line</th><th class="n">Orders seen</th><th></th></tr></thead><tbody>
          ${map.map(m=>{
            const seen = (pm.seen_on_orders||[]).find(x=>x.product_sku===m.shopify_sku);
            return `<tr><td><b>${esc(m.shopify_sku)}</b></td><td>${esc(m.product_line)}</td>
              <td class="n">${nfmt(seen?seen.orders:0)}</td>
              <td style="text-align:right"><button class="ehp-btn g" data-unmap="${esc(m.shopify_sku)}" style="color:${RED}">Remove</button></td></tr>`;
          }).join('')}
          ${unmappedSkus.map(u=>`<tr><td><b>${esc(u.product_sku)}</b></td>
            <td><input class="ehp-in" data-mapline="${esc(u.product_sku)}" list="ehp-lines" placeholder="OXYSHRED" style="width:130px"></td>
            <td class="n">${nfmt(u.orders)}</td>
            <td style="text-align:right"><button class="ehp-btn g" data-map="${esc(u.product_sku)}">Map</button></td></tr>`).join('')}
          ${(!map.length && !unmappedSkus.length) ? `<tr><td colspan="4" style="color:${LIGHT};text-align:center;padding:14px;">No Shopify SKUs seen yet.</td></tr>` : ''}
        </tbody></table>
        <div style="display:flex;gap:8px;align-items:flex-end;margin-top:10px;">
          <div><span class="ehp-lbl">Shopify SKU</span><input class="ehp-in" id="ehp-newsku" placeholder="oxyshredstickpack-us" style="width:210px"></div>
          <div><span class="ehp-lbl">Product line</span><input class="ehp-in" id="ehp-newline" list="ehp-lines" placeholder="OXYSHRED" style="width:140px"></div>
          <button class="ehp-btn g" id="ehp-addmap">Add mapping</button>
        </div>
        <div id="ehp-mapmsg"></div>
      </div>

      ${(d.history||[]).length ? `<table class="ehp" style="margin-top:16px"><thead><tr><th>Version</th><th>Line</th><th>Structure</th><th>From</th><th>To</th></tr></thead><tbody>${d.history.map(h=>`<tr><td>${esc(h.name)}</td><td>${esc(h.product_line||'—')}</td><td>${esc((h.sticks_per_envelope||5)+' / '+(h.distinct_flavours||3)+' / '+(h.split_pattern||''))}</td><td>${esc(h.effective_from)}</td><td>${esc(h.effective_to||'current')}</td></tr>`).join('')}</tbody></table>`:''}`;

    el('ehp-rsave').addEventListener('click', async () => {
      const pool = Array.from(body.querySelectorAll('[data-pool]')).map(x=>x.value).filter(Boolean);
      const line = (el('ehp-rline').value || '').trim().toUpperCase();
      if (!line) { el('ehp-rmsg').innerHTML = msg('e','Product line is required — a recipe serves one line.'); return; }
      const want = parseInt(el('ehp-flav').value,10) || 3;
      if (pool.length && pool.length !== want) {
        el('ehp-rmsg').innerHTML = msg('e', `Pick exactly ${want} flavour(s) — ${pool.length} chosen.`); return;
      }
      if (new Set(pool).size !== pool.length) { el('ehp-rmsg').innerHTML = msg('e','The same flavour is selected twice.'); return; }
      try {
        await req('/ehp/recipe', { method:'POST', body: JSON.stringify({
          product_line: line,
          name: el('ehp-rname').value || null, effective_from: el('ehp-rfrom').value,
          sticks_per_envelope: parseInt(el('ehp-sticks').value,10),
          distinct_flavours: want,
          split_pattern: el('ehp-split').value,
          flavours: pool }) });
        el('ehp-rmsg').innerHTML = msg('k','Recipe saved for ' + esc(line) + '.');
        setTimeout(render, 900);
      } catch (e) { el('ehp-rmsg').innerHTML = msg('e', e.message || String(e)); }
    });

    async function saveMap(sku, line) {
      if (!sku || !line) { el('ehp-mapmsg').innerHTML = msg('e','Both the Shopify SKU and the product line are required.'); return; }
      try {
        const r = await req('/ehp/product-map', { method:'POST',
          body: JSON.stringify({ shopify_sku: sku, product_line: line.toUpperCase() }) });
        el('ehp-mapmsg').innerHTML = msg('k', `Mapped ${esc(sku)} to ${esc(r.product_line)}.` +
          (r.orders_backfilled ? ` ${r.orders_backfilled} queued order(s) updated.` : ''));
        setTimeout(render, 900);
      } catch (e) { el('ehp-mapmsg').innerHTML = msg('e', e.message || String(e)); }
    }
    body.querySelectorAll('[data-map]').forEach(b => b.addEventListener('click', () => {
      const sku = b.getAttribute('data-map');
      const inp = body.querySelector('[data-mapline="' + sku.replace(/"/g,'\\"') + '"]');
      saveMap(sku, inp ? inp.value.trim() : '');
    }));
    el('ehp-addmap')?.addEventListener('click', () =>
      saveMap((el('ehp-newsku').value||'').trim(), (el('ehp-newline').value||'').trim()));
    body.querySelectorAll('[data-unmap]').forEach(b => b.addEventListener('click', async () => {
      const sku = b.getAttribute('data-unmap');
      if (!confirm('Remove the mapping for ' + sku + '?\n\nNew orders with this SKU will not be batchable until it is mapped again.')) return;
      try { await req('/ehp/product-map/' + encodeURIComponent(sku), { method:'DELETE' });
            setTimeout(render, 700); }
      catch (e) { el('ehp-mapmsg').innerHTML = msg('e', e.message || String(e)); }
    }));
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
        <div class="ehp-kpi"><div class="ehp-kl">Unfulfilled</div><div class="ehp-kv" style="color:${st.unfulfilled_dispatched?AMBER_TXT:DARK}">${nfmt(st.unfulfilled_dispatched)}</div><div class="ehp-ks">dispatched, not yet in Shopify</div></div>
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

  // Fetch the generated label PDF and hand it to the browser as a download. Server-side
  // generation means the page size is exactly 76 x 36 mm regardless of print settings.
  window.__ehpLabelPdf = async function (batchId, orderIds) {
    try {
      const t = await tok();
      const h = {}; if (t) h.Authorization = 'Bearer ' + t;
      if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
      const qs = (orderIds && orderIds.length) ? '?order_ids=' + encodeURIComponent(orderIds.join(',')) : '';
      const r = await fetch(apiBase() + '/ehp/batch/' + batchId + '/labels.pdf' + qs, { headers: h });
      if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + (await r.text()).slice(0, 200));
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = 'labels-' + batchId + '.pdf';
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 4000);
      window.__ehpMarkLabels(batchId, orderIds);
    } catch (e) {
      const m = el('ehp-batchmsg');
      if (m) m.innerHTML = msg('e', 'Could not generate labels: ' + (e.message || e));
      else alert('Could not generate labels: ' + (e.message || e));
    }
  };

  // Records which orders have had labels produced, so the floor can see it at a glance.
  window.__ehpMarkLabels = async function (batchId, orderIds) {
    try {
      const r = await req('/ehp/batch/' + batchId + '/labels-generated',
        { method: 'POST', body: JSON.stringify({ order_ids: orderIds || [] }) });
      const m = el('ehp-batchmsg');
      if (m) m.innerHTML = msg('k', `Labels marked as printed for ${r.marked} order(s) — ${r.with_labels} of ${r.orders} in this batch now have labels.`);
      if (_tab === 'queue') setTimeout(render, 600);
    } catch (e) {
      const m = el('ehp-batchmsg');
      if (m) m.innerHTML = msg('w', 'Labels printed, but marking them failed: ' + (e.message || e));
    }
  };

  // ── init ──
  let _liveTimer = null;
  function startLive() {
    stopLive();
    _liveTimer = setInterval(() => {
      // Only the read-mostly tabs; never re-render a form mid-entry.
      if (!document.querySelector('.ehp-ov')) return stopLive();
      if (document.visibilityState !== 'visible') return;
      if (_rendering) return;
      if (_tab === 'queue' || _tab === 'inventory') render({ background: true });
    }, 60000);
  }
  function stopLive() { if (_liveTimer) { clearInterval(_liveTimer); _liveTimer = null; } }

  function init() {
    styles();
    window.openEhpOps = open;
    refreshEnabled();
    window.addEventListener('state:ready', refreshEnabled);
    setInterval(() => { if (document.visibilityState === 'visible') refreshEnabled(); }, 15000);
    console.log('[ehp-ops] module v14 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
