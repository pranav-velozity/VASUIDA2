/* ── VelOzity Pinpoint — Fulfilment Week Hub v1 ──
   Renders a fulfilment-shaped Week Hub for ANY client with the envelope_fulfilment
   capability — not EHP-specific. Keeps the shared week capsules and replaces the
   ICONIC-shaped KPI row and Control Tower/Insights with fulfilment content. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#34C759', AMBER = '#C8860A', RED = '#D7263D';
  let _on = false, _busy = false, _lastWeek = '';

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const el = id => document.getElementById(id);
  const nf = n => Number(n || 0).toLocaleString();
  function apiBase(){ return (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/,''); }
  async function tok(){ if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch(e){} } return null; }
  async function req(path) {
    const t = await tok();
    const h = {}; if (t) h.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { headers: h });
    const txt = await r.text(); let d = null; try { d = txt ? JSON.parse(txt) : null; } catch(e){ d = txt; }
    if (!r.ok) { const e = new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return d;
  }

  function styles() {
    if (el('fwh-styles')) return;
    const s = document.createElement('style'); s.id = 'fwh-styles';
    s.textContent = `
      #fwh{margin:14px 0 20px;}
      .fwh-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:14px;}
      .fwh-kpi{background:#fff;border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;padding:13px 15px;}
      .fwh-kl{font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .fwh-kv{font-size:26px;font-weight:700;color:${DARK};margin-top:4px;letter-spacing:-0.02em;}
      .fwh-ks{font-size:10px;color:${MID};margin-top:1px;}
      .fwh-grid{display:grid;grid-template-columns:1.4fr 1fr;gap:12px;align-items:start;}
      @media (max-width:1100px){.fwh-grid{grid-template-columns:1fr;}}
      .fwh-card{background:#fff;border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;padding:15px 17px;}
      .fwh-t{font-size:13px;font-weight:700;color:${DARK};}
      .fwh-s{font-size:10px;color:${LIGHT};margin-bottom:12px;}
      .fwh-flow{display:flex;align-items:flex-start;justify-content:space-between;gap:6px;margin:16px 0 6px;}
      .fwh-node{flex:1;text-align:center;position:relative;}
      .fwh-dot{width:34px;height:34px;border-radius:50%;border:1.5px solid rgba(0,0,0,0.14);background:#fff;
               display:flex;align-items:center;justify-content:center;margin:0 auto 6px;font-size:14px;}
      .fwh-dot.on{border-color:${BRAND};box-shadow:0 2px 10px rgba(153,0,51,.18);}
      .fwh-nl{font-size:10px;font-weight:600;color:${DARK};}
      .fwh-nv{font-size:15px;font-weight:700;color:${DARK};}
      .fwh-nb{font-size:9px;color:${LIGHT};}
      .fwh-bar{position:absolute;top:17px;left:calc(50% + 22px);right:calc(-50% + 22px);height:1.5px;
               background:repeating-linear-gradient(90deg,rgba(0,0,0,.16) 0 5px,transparent 5px 10px);}
      .fwh-ex{display:flex;gap:9px;align-items:flex-start;padding:9px 0;border-bottom:0.5px solid rgba(0,0,0,0.06);}
      .fwh-ex:last-child{border-bottom:none;}
      .fwh-ei{width:3px;align-self:stretch;border-radius:2px;flex-shrink:0;}
      .fwh-eh{font-size:12px;font-weight:600;color:${DARK};}
      .fwh-eb{font-size:10px;color:${MID};margin-top:1px;}
      .fwh-none{font-size:11px;color:${LIGHT};text-align:center;padding:18px;}
    `;
    document.head.appendChild(s);
  }

  function onWeekHub() {
    const h = (location.hash || '').toLowerCase();
    return h === '' || h === '#' || h.includes('week-hub') || h.includes('flow') || h.includes('dashboard');
  }

  // Hide ICONIC-shaped sections; keep the shared week capsules.
  function toggleIconicSections(hide) {
    for (const id of ['wh-kpi-row', 'page-flow']) {
      const n = el(id); if (n) n.style.display = hide ? 'none' : '';
    }
  }

  function ensureHost() {
    let host = el('fwh');
    if (!host) {
      const dash = el('page-dashboard'); if (!dash) return null;
      host = document.createElement('div'); host.id = 'fwh';
      const hdr = el('wh-header');
      if (hdr && hdr.parentNode === dash) dash.insertBefore(host, hdr.nextSibling);
      else dash.insertBefore(host, dash.firstChild);
    }
    return host;
  }

  function weekRange() {
    const ws = (window.state && window.state.weekStart) || '';
    if (!ws) return null;
    const d = new Date(ws + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + 6);
    return { from: ws, to: d.toISOString().slice(0, 10) };
  }

  async function render(force) {
    const host = ensureHost(); if (!host) return;
    const rng = weekRange(); if (!rng) return;
    if (_busy) return;
    if (!force && rng.from === _lastWeek) return;
    _busy = true;
    try {
      const [sum, queue, batches] = await Promise.all([
        req(`/ehp/summary?from=${rng.from}&to=${rng.to}`),
        req('/ehp/queue').catch(() => null),
        req('/ehp/batches').catch(() => ({ batches: [] })),
      ]);
      _lastWeek = rng.from;
      let inv = null, billing = null;
      try { inv = await req('/ehp/inventory'); } catch (e) {}
      try { billing = await req(`/finance/fulfilment-billing?from=${rng.from}&to=${rng.to}`); } catch (e) {}
      draw(host, sum, queue, batches.batches || [], inv, billing, rng);
    } catch (e) {
      host.innerHTML = `<div class="fwh-card" style="color:${RED};font-size:12px;">Could not load fulfilment summary: ${esc(e.message || e)}</div>`;
    }
    _busy = false;
  }

  function draw(host, s, q, batches, inv, billing, rng) {
    const queuedOrders = q ? q.queued_orders : s.queued_orders;
    const queuedEnv = q ? q.queued_envelopes : s.queued_envelopes;
    const flagged = q ? q.flagged_high_qty : 0;
    const recipe = q ? q.active_recipe : null;

    // exceptions — the reason this panel exists
    const ex = [];
    const unsent = batches.filter(b => b.state === 'dispatched' && b.fulfilment && b.fulfilment.not_fulfilled > 0);
    const unsentCount = unsent.reduce((a, b) => a + (b.fulfilment.not_fulfilled || 0), 0);
    if (unsentCount) ex.push({ c: RED, h: `${unsentCount} order(s) dispatched but not fulfilled in Shopify`,
      b: 'Use Retry fulfilments on the Shopify tab, or Why? on the batch to see the reason.' });
    if (flagged) ex.push({ c: AMBER, h: `${flagged} order(s) flagged as unusually large`, b: 'Review before assembling — free-sample abuse control.' });
    if (!recipe) ex.push({ c: RED, h: 'No active kit recipe', b: 'Assembly is blocked until an envelope structure is set.' });
    const low = (inv && inv.inventory || []).filter(r => r.is_component && (r.estimated_on_hand || 0) <= 0);
    if (low.length) ex.push({ c: RED, h: `${low.length} component(s) estimated at or below zero`,
      b: low.map(r => r.sku).join(', ') + ' — receive stock or run a count.' });
    const assembledNotDispatched = batches.filter(b => b.state === 'assembled');
    if (assembledNotDispatched.length) ex.push({ c: AMBER,
      h: `${assembledNotDispatched.length} batch(es) assembled but not dispatched`,
      b: `${nf(assembledNotDispatched.reduce((a,b)=>a+(b.actual_envelopes||0),0))} envelope(s) waiting to be lodged.` });
    if (queuedEnv > 0 && !assembledNotDispatched.length) ex.push({ c: MID,
      h: `${nf(queuedEnv)} envelope(s) queued`, b: 'Create a batch on the Fulfilment screen to begin assembly.' });
    if (billing && !billing.complete) ex.push({ c: AMBER, h: 'Billing rate not set',
      b: 'A service rate is missing — the weekly invoice cannot be calculated in full.' });

    const node = (icon, label, value, sub, on, last) => `
      <div class="fwh-node">
        ${last ? '' : '<div class="fwh-bar"></div>'}
        <div class="fwh-dot ${on ? 'on' : ''}">${icon}</div>
        <div class="fwh-nl">${label}</div>
        <div class="fwh-nv">${value}</div>
        <div class="fwh-nb">${sub}</div>
      </div>`;

    host.innerHTML = `
      <div class="fwh-kpis">
        <div class="fwh-kpi"><div class="fwh-kl">Pallets received</div><div class="fwh-kv">${nf(s.pallets_received)}</div><div class="fwh-ks">billable inbound</div></div>
        <div class="fwh-kpi"><div class="fwh-kl">Orders received</div><div class="fwh-kv">${nf(s.orders_received)}</div><div class="fwh-ks">${nf(s.envelopes_ordered)} envelopes</div></div>
        <div class="fwh-kpi"><div class="fwh-kl">Envelopes assembled</div><div class="fwh-kv">${nf(s.envelopes_assembled)}</div><div class="fwh-ks">this week</div></div>
        <div class="fwh-kpi"><div class="fwh-kl">Envelopes dispatched</div><div class="fwh-kv" style="color:${BRAND}">${nf(s.envelopes_dispatched)}</div><div class="fwh-ks">billable outbound</div></div>
        <div class="fwh-kpi"><div class="fwh-kl">Queue depth</div><div class="fwh-kv">${nf(queuedEnv)}</div><div class="fwh-ks">${nf(queuedOrders)} order(s) awaiting a batch</div></div>
        ${billing ? `<div class="fwh-kpi"><div class="fwh-kl">Week to invoice</div>
          <div class="fwh-kv" style="font-size:20px;">${billing.complete ? ('$' + nf(billing.total)) : '—'}</div>
          <div class="fwh-ks">${billing.complete ? 'pallets + envelopes' : 'rate not set'}</div></div>` : ''}
      </div>

      <div class="fwh-grid">
        <div class="fwh-card">
          <div class="fwh-t">Fulfilment flow</div>
          <div class="fwh-s">Week of ${esc(rng.from)} &middot; ${esc(rng.to)}</div>
          <div class="fwh-flow">
            ${node('📦', 'Receipt', nf(s.pallets_received), 'pallets in', s.pallets_received > 0)}
            ${node('🧾', 'Queue', nf(queuedEnv), 'envelopes', queuedEnv > 0)}
            ${node('🧰', 'Assembly', nf(s.envelopes_assembled), 'assembled', s.envelopes_assembled > 0)}
            ${node('🚚', 'Dispatch', nf(s.envelopes_dispatched), 'lodged', s.envelopes_dispatched > 0)}
            ${node('📮', 'USPS', '—', 'in transit', false, true)}
          </div>
          ${recipe ? `<div style="font-size:10px;color:${LIGHT};margin-top:10px;">
            Active structure: ${recipe.sticks_per_envelope || 5} sticks &middot; ${recipe.distinct_flavours || 3} flavours &middot; ${esc(recipe.split_pattern || '')}
          </div>` : ''}
        </div>

        <div class="fwh-card">
          <div class="fwh-t">Needs attention</div>
          <div class="fwh-s">Exceptions for this week</div>
          ${ex.length ? ex.map(e => `<div class="fwh-ex">
            <div class="fwh-ei" style="background:${e.c}"></div>
            <div><div class="fwh-eh">${esc(e.h)}</div><div class="fwh-eb">${esc(e.b)}</div></div>
          </div>`).join('') : `<div class="fwh-none">Nothing needs attention.</div>`}
          <div style="margin-top:12px;text-align:right;">
            <button onclick="window.openEhpOps&&window.openEhpOps()" style="background:${BRAND};color:#fff;border:none;border-radius:8px;padding:7px 13px;font:600 11px/1 inherit;cursor:pointer;">Open Fulfilment →</button>
          </div>
        </div>
      </div>`;
  }

  async function check() {
    // Capability-driven: any client with envelope fulfilment gets this Week Hub.
    let enabled = false;
    try { await req('/ehp/queue'); enabled = true; }
    catch (e) { enabled = !(e.status === 409 || e.status === 403); if (e.status === 409) enabled = false; }
    _on = enabled;
    const host = el('fwh');
    if (!_on) { if (host) host.remove(); toggleIconicSections(false); return; }
    if (!onWeekHub()) { if (host) host.style.display = 'none'; return; }
    if (host) host.style.display = '';
    toggleIconicSections(true);
    render(false);
  }

  function init() {
    styles();
    check();
    window.addEventListener('state:ready', () => { _lastWeek = ''; check(); });
    window.addEventListener('hashchange', check);
    setInterval(check, 4000);                 // client switch / week change
    window.refreshFulfilmentWeekHub = () => render(true);
    console.log('[fulfilment-weekhub] module v1 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
