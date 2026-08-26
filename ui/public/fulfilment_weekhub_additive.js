/* ── VelOzity Pinpoint — Fulfilment Week Hub v1 ──
   Renders a fulfilment-shaped Week Hub for ANY client with the envelope_fulfilment
   capability — not EHP-specific. Keeps the shared week capsules and replaces the
   ICONIC-shaped KPI row and Control Tower/Insights with fulfilment content. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#34C759', AMBER = '#FFD014', RED = '#B33F40';
  const AMBER_TXT = '#8A6D00';        // #FFD014 is too light for text on white
  // Keyed per client: a cached flag from EHP must not decide the layout for ICONIC.
  const lsKey = () => 'pinpoint.fulfilmentClient.' + (window.pinpointClient || 'unknown');
  // Remembered from the last session so the ICONIC layout is hidden on the very first
  // paint, instead of flashing while the capability check round-trips.
  let _on = (() => { try { return localStorage.getItem(lsKey()) === '1'; } catch (e) { return false; } })();
  let _busy = false, _lastWeek = '', _conn = null;
  // Once the answer is definitive for a client, stop re-asking every few seconds.
  let _capClient = null;

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
      .fwh-kpi{background:#fff;border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;padding:13px 15px;
               box-shadow:0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06);}
      .fwh-kl{font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .fwh-kv{font-size:26px;font-weight:700;color:${DARK};margin-top:4px;letter-spacing:-0.02em;}
      .fwh-ks{font-size:10px;color:${MID};margin-top:1px;}
      .fwh-grid{display:grid;grid-template-columns:1.4fr 1fr;gap:12px;align-items:start;}
      @media (max-width:1100px){.fwh-grid{grid-template-columns:1fr;}}
      /* Tiles sit raised rather than flat on the page background — the flat treatment read
         as loosely grouped rectangles rather than as distinct cards. */
      .fwh-card{background:#fff;border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;padding:15px 17px;
                box-shadow:0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06);}
      .fwh-t{font-size:13px;font-weight:700;color:${DARK};display:flex;align-items:center;justify-content:space-between;gap:8px;}
      .fwh-cfg{background:none;border:none;color:${LIGHT};cursor:pointer;font-size:12px;padding:2px 4px;border-radius:5px;}
      .fwh-cfg:hover{background:#F5F5F7;color:${BRAND};}
      .fwh-pop{position:absolute;z-index:40;background:#fff;border:0.5px solid rgba(0,0,0,.14);border-radius:10px;
        box-shadow:0 12px 32px rgba(0,0,0,.16);padding:12px 14px;min-width:240px;}
      .fwh-pop label{display:block;font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};margin:8px 0 3px;}
      .fwh-pop input{width:100%;border:0.5px solid rgba(0,0,0,.15);border-radius:7px;padding:6px 8px;font:400 12px inherit;box-sizing:border-box;}
      .fwh-pop .row{display:flex;gap:8px;justify-content:flex-end;margin-top:11px;}
      .fwh-pop button{border:none;border-radius:7px;padding:6px 12px;font:600 11px inherit;cursor:pointer;}
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
      .fwh-exwrap{max-height:186px;overflow:hidden;position:relative;}
      .fwh-exwrap.scrolling::after{content:'';position:absolute;left:0;right:0;bottom:0;height:22px;
        background:linear-gradient(transparent,#fff);pointer-events:none;}
      .fwh-exscroll{will-change:transform;}
      .fwh-excount{font-size:9px;color:${LIGHT};margin-left:6px;font-weight:500;}
      @keyframes fwhFlash{0%{background:rgba(153,0,51,.20);}55%{background:rgba(153,0,51,.14);}100%{background:transparent;}}
      .fwh-kpi.flash{animation:fwhFlash 3.6s ease-out;}
      @keyframes fwhPulse{0%,100%{opacity:1;transform:scale(1);}50%{opacity:.35;transform:scale(.8);}}
      .fwh-live{display:inline-block;width:8px;height:8px;border-radius:50%;background:${GREEN};margin-right:6px;
                animation:fwhPulse 1.6s ease-in-out infinite;}
      .fwh-live.off{background:${RED};animation:none;}
      .fwh-conn{display:flex;align-items:center;justify-content:space-between;gap:10px;background:#fff;
                border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;padding:9px 14px;margin-bottom:12px;font-size:11px;}
      .fwh-gauges{display:flex;gap:10px;flex-wrap:wrap;justify-content:space-around;align-items:center;height:100%;}
      .fwh-gauge{width:118px;text-align:center;}
      .fwh-gauge canvas{max-height:104px;}
      .fwh-gsku{font-size:10px;font-weight:600;color:${DARK};margin-top:2px;
                white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
      .fwh-backlog{display:flex;align-items:center;gap:16px;margin-top:14px;padding:13px 16px;
                   border-radius:11px;border:0.5px solid rgba(0,0,0,.08);background:#FAFAFA;}
      .fwh-bnum{font-size:34px;font-weight:700;letter-spacing:-.02em;line-height:1;}
      .fwh-bmeta{flex:1;}
      .fwh-btrack{height:6px;border-radius:3px;background:rgba(0,0,0,.07);overflow:hidden;margin-top:7px;}
      .fwh-bfill{height:100%;border-radius:3px;transition:width .5s ease;}
    `;
    document.head.appendChild(s);
  }

  function onWeekHub() {
    const h = (location.hash || '').toLowerCase();
    return h === '' || h === '#' || h.includes('week-hub') || h.includes('flow') || h.includes('dashboard');
  }

  // Hide ICONIC-shaped sections; keep the shared week capsules.
  // Never guess at another module's layout: remember the exact inline value we replaced
  // and put it back. Writing '' here previously wiped a display the app had set itself.
  function toggleIconicSections(hide) {
    for (const id of ['wh-kpi-row', 'page-flow']) {
      const n = el(id); if (!n) continue;
      if (hide) {
        if (n.dataset.fwhPrev === undefined) n.dataset.fwhPrev = n.style.display || '';
        n.style.display = 'none';
      } else if (n.dataset.fwhPrev !== undefined) {
        n.style.display = n.dataset.fwhPrev;
        delete n.dataset.fwhPrev;
      }
      // If we never hid it, leave it entirely alone.
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
      let inv = null, billing = null, ts = null;
      try { inv = await req('/ehp/inventory'); } catch (e) {}
      try { billing = await req(`/finance/fulfilment-billing?from=${rng.from}&to=${rng.to}`); } catch (e) {}
      try { ts = await req(`/ehp/timeseries?from=${rng.from}&to=${rng.to}`); } catch (e) {}
      await loadConn();
      draw(host, sum, queue, batches.batches || [], inv, billing, rng, ts);
      host.dataset.loaded = '1';
    } catch (e) {
      host.innerHTML = `<div class="fwh-card" style="color:${RED};font-size:12px;">Could not load fulfilment summary: ${esc(e.message || e)}</div>`;
    }
    _busy = false;
  }

  // render() skips a week it has already drawn, so a receipt saved on another tab left the
  // tile showing a stale figure. Other EHP modules raise this after a write.
  window.addEventListener('ehp:changed', () => { _lastWeek = null; render(true); });

  const _prev = {};
  function shortTs(v) {
    if (!v) return '';
    const iso = String(v).includes('T') ? v : String(v).replace(' ', 'T') + 'Z';
    const d = new Date(iso); if (isNaN(d)) return String(v);
    return d.toLocaleString('en-US', { timeZone: 'America/Chicago', month:'short', day:'2-digit', hour:'2-digit', minute:'2-digit', hour12:true });
  }
  // Highlight a tile whose value moved since the last refresh.
  function flashChanged(vals) {
    for (const [k, v] of Object.entries(vals)) {
      if (_prev[k] !== undefined && _prev[k] !== v) {
        const n = document.querySelector(`.fwh-kpi[data-k="${k}"]`);
        if (n) { n.classList.remove('flash'); void n.offsetWidth; n.classList.add('flash'); }
      }
      _prev[k] = v;
    }
  }

  function draw(host, s, q, batches, inv, billing, rng, ts) {
    ts = ts || {};
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
    const crit = (ts && ts.inventory || []).filter(i => i.status === 'critical');
    if (crit.length) ex.push({ c: RED, h: `${crit.length} flavour(s) below the critical cover threshold`,
      b: crit.map(i => `${i.sku} (${i.days_cover}d)`).join(', ') + ` — under a ${ts.settings.replenishment_lead_days}-day lead time, reorder now.` });
    const warnLow = (ts && ts.inventory || []).filter(i => i.status === 'warning');
    if (warnLow.length) ex.push({ c: AMBER, h: `${warnLow.length} flavour(s) approaching reorder point`,
      b: warnLow.map(i => `${i.sku} (${i.days_cover}d)`).join(', ') });
    if (ts && ts.backlog_limit > 0 && ts.backlog_now > ts.backlog_limit) ex.push({ c: RED,
      h: `Backlog ${nf(ts.backlog_now)} envelopes exceeds the SLA limit of ${nf(ts.backlog_limit)}`,
      b: 'Dispatch is falling behind intake — orders risk breaching the lodgement SLA.' });
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

    const conn = _conn || {};
    const liveOk = !!conn.connected;
    host.innerHTML = `
      <div class="fwh-conn">
        <div><span class="fwh-live ${liveOk?'':'off'}"></span>
          <b>${liveOk ? 'Shopify connected' : 'Shopify not connected'}</b>
          <span style="color:${LIGHT}"> &middot; ${esc(conn.shop_domain || 'no store linked')}</span></div>
        <div style="color:${MID}">
          ${conn.last_webhook_at ? 'Last order received ' + esc(shortTs(conn.last_webhook_at)) : 'No orders received yet'}
          ${conn.unfulfilled_dispatched ? ` &middot; <span style="color:${AMBER_TXT};font-weight:600;">${nf(conn.unfulfilled_dispatched)} unfulfilled</span>` : ''}
        </div>
      </div>
      <div class="fwh-kpis">
        <div class="fwh-kpi" data-k="pallets"><div class="fwh-kl">Pallets received</div><div class="fwh-kv">${nf(s.pallets_received)}</div><div class="fwh-ks">billable inbound</div></div>
        <div class="fwh-kpi" data-k="orders"><div class="fwh-kl">Orders received</div><div class="fwh-kv">${nf(s.orders_received)}</div><div class="fwh-ks">${nf(s.envelopes_ordered)} envelopes</div></div>
        <div class="fwh-kpi" data-k="assembled"><div class="fwh-kl">Envelopes assembled</div><div class="fwh-kv">${nf(s.envelopes_assembled)}</div><div class="fwh-ks">this week</div></div>
        <div class="fwh-kpi" data-k="dispatched"><div class="fwh-kl">Envelopes dispatched</div><div class="fwh-kv" style="color:${BRAND}">${nf(s.envelopes_dispatched)}</div><div class="fwh-ks">billable outbound</div></div>
        <div class="fwh-kpi" data-k="queue"><div class="fwh-kl">Queue depth</div><div class="fwh-kv">${nf(queuedEnv)}</div><div class="fwh-ks">${nf(queuedOrders)} order(s) awaiting a batch</div></div>

      </div>

      <div class="fwh-grid">
        <div class="fwh-card">
          <div class="fwh-t">Fulfilment flow</div>
          <div class="fwh-s">Week of ${esc(rng.from)} &middot; ${esc(rng.to)}</div>
          <div class="fwh-flow">
            ${node('📦', 'Received', nf(s.pallets_received), 'pallets', s.pallets_received > 0)}
            ${node('🧾', 'Ordered', nf(s.envelopes_ordered), 'envelopes', s.envelopes_ordered > 0)}
            ${node('🧰', 'Assembled', nf(s.envelopes_assembled), 'envelopes', s.envelopes_assembled > 0)}
            ${node('📮', 'Lodged with USPS', nf(s.envelopes_dispatched), 'envelopes', s.envelopes_dispatched > 0, true)}
          </div>
          <div style="font-size:10px;color:${LIGHT};margin-top:2px;">
            All four are totals for this week. USPS transit is not shown: letter-mail samples carry no tracking, so there is no signal after lodgement.
          </div>
          ${(() => {
            const bl = ts.backlog_now != null ? ts.backlog_now : queuedEnv;
            const lim = ts.backlog_limit || 0;
            const over = lim > 0 && bl > lim;
            const pct = lim > 0 ? Math.min(100, Math.round(bl / lim * 100)) : (bl > 0 ? 100 : 0);
            const col = over ? RED : (pct >= 70 ? AMBER : GREEN);
            return `<div class="fwh-backlog">
              <div><div class="fwh-kl">Current backlog</div>
                <div class="fwh-bnum" style="color:${col}">${nf(bl)}</div>
                <div class="fwh-ks">envelopes ordered, not yet lodged</div></div>
              <div class="fwh-bmeta">
                <div style="font-size:11px;color:${over ? RED : MID};font-weight:${over ? '600' : '400'};">
                  ${lim > 0 ? (over
                      ? `Above the ${nf(lim)}-envelope SLA limit — dispatch is behind intake.`
                      : `Within the ${nf(lim)}-envelope SLA limit (${pct}%).`)
                    : 'No SLA limit yet — dispatch some batches to establish a rate.'}
                </div>
                <div class="fwh-btrack"><div class="fwh-bfill" style="width:${pct}%;background:${col}"></div></div>
                <div style="font-size:10px;color:${LIGHT};margin-top:5px;">
                  ${nf(queuedOrders)} order(s) awaiting a batch · ${nf(queuedEnv)} envelope(s) live in the queue
                </div>
              </div>
            </div>`;
          })()}
          ${recipe ? `<div style="font-size:10px;color:${LIGHT};margin-top:10px;">
            Active structure: ${recipe.sticks_per_envelope || 5} sticks &middot; ${recipe.distinct_flavours || 3} flavours &middot; ${esc(recipe.split_pattern || '')}
          </div>` : ''}
        </div>

        <div class="fwh-card">
          <div class="fwh-t"><span>Needs attention${ex.length > 3 ? `<span class="fwh-excount">${ex.length} items &middot; scrolling</span>` : ''}</span></div>
          <div class="fwh-s">Exceptions for this week</div>
          <div class="fwh-exwrap ${ex.length > 3 ? 'scrolling' : ''}" id="fwh-exwrap">
            <div class="fwh-exscroll" id="fwh-exscroll">
            ${ex.length ? ex.map(e => `<div class="fwh-ex">
              <div class="fwh-ei" style="background:${e.c}"></div>
              <div><div class="fwh-eh">${esc(e.h)}</div><div class="fwh-eb">${esc(e.b)}</div></div>
            </div>`).join('') : `<div class="fwh-none">Nothing needs attention.</div>`}
            </div>
          </div>
          <div style="margin-top:12px;text-align:right;">
            <button onclick="window.openEhpOps&&window.openEhpOps()" style="background:${BRAND};color:#fff;border:none;border-radius:8px;padding:7px 13px;font:600 11px/1 inherit;cursor:pointer;">Open Fulfilment →</button>
          </div>
        </div>
      </div>

      <div class="fwh-grid" style="margin-top:12px;">
        <div class="fwh-card">
          <div class="fwh-t"><span>Throughput</span><button class="fwh-cfg" data-cfg="sla" title="Edit SLA">&#9881;</button></div>
          <div class="fwh-s">Cumulative across the week. <b>Backlog</b> is ordered but not yet lodged &mdash; above the dashed SLA limit means dispatch is falling behind intake.</div>
          <div style="height:210px;position:relative;"><canvas id="fwh-c-flow"></canvas></div>
        </div>
        <div class="fwh-card">
          <div class="fwh-t"><span>Days of cover</span><button class="fwh-cfg" data-cfg="cover" title="Edit thresholds">&#9881;</button></div>
          <div class="fwh-s">At the current burn rate. Red under ${ts && ts.settings ? ts.settings.cover_critical_days : 10} days, amber under ${ts && ts.settings ? ts.settings.cover_warning_days : 30} &mdash; set against a ${ts && ts.settings ? ts.settings.replenishment_lead_days : 30}-day replenishment lead time.</div>
          <div style="height:210px;" id="fwh-cover-wrap"><div class="fwh-gauges" id="fwh-gauges"></div></div>
        </div>
      </div>

      <div class="fwh-card" style="margin-top:12px;">
        <div class="fwh-t"><span>Stock position by flavour</span><button class="fwh-cfg" data-cfg="cover" title="Edit thresholds">&#9881;</button></div>
        <div class="fwh-s">Estimated on-hand over the week. Consumption is derived and replaced by truth at each count. Crossing the <b>reorder point</b> means stock ordered today would land after you run out.</div>
        <div style="height:230px;position:relative;"><canvas id="fwh-c-stock"></canvas></div>
      </div>`;

    drawCharts(ts);
    startExScroll(ex.length);
    wireSettings(ts.settings || {});
    flashChanged({ pallets: s.pallets_received, orders: s.orders_received,
                   assembled: s.envelopes_assembled, dispatched: s.envelopes_dispatched, queue: queuedEnv });
  }

  // ── needs-attention auto-scroll (pauses on hover) ──
  let _exTimer = null;
  function startExScroll(count) {
    if (_exTimer) { clearInterval(_exTimer); _exTimer = null; }
    const wrap = el('fwh-exwrap'), inner = el('fwh-exscroll');
    if (!wrap || !inner || count <= 3) return;
    let y = 0, paused = false, holdUntil = Date.now() + 2500;   // read the top item first
    wrap.onmouseenter = () => { paused = true; };
    wrap.onmouseleave = () => { paused = false; };
    _exTimer = setInterval(() => {
      if (paused || Date.now() < holdUntil) return;
      const max = inner.scrollHeight - wrap.clientHeight;
      if (max <= 0) return;
      y += 0.5;                                   // slow enough to read
      if (y >= max) { y = 0; holdUntil = Date.now() + 2500; }
      inner.style.transform = `translateY(${-y}px)`;
    }, 40);
  }

  // ── inline threshold editing ──
  function wireSettings(cur) {
    document.querySelectorAll('.fwh-cfg').forEach(btn => {
      btn.onclick = (ev) => {
        ev.stopPropagation();
        document.querySelectorAll('.fwh-pop').forEach(p => p.remove());
        const which = btn.getAttribute('data-cfg');
        const fields = which === 'cover'
          ? [['replenishment_lead_days', 'Replenishment lead time (days)'],
             ['cover_critical_days', 'Critical below (days)'],
             ['cover_warning_days', 'Warning below (days)']]
          : [['expected_daily_envelopes', 'Expected envelopes per working day (0 = auto)'],
             ['sla_lodge_days', 'Lodgement SLA (business days)'],
             ['sla_inbound_days', 'Inbound put-away SLA (business days)']];
        const pop = document.createElement('div');
        pop.className = 'fwh-pop';
        pop.innerHTML = fields.map(([k, l]) =>
          `<label>${l}</label><input type="number" min="0" step="1" data-k="${k}" value="${cur[k] != null ? cur[k] : ''}">`).join('')
          + `<div class="row"><button data-x="c" style="background:#F5F5F7;color:${MID}">Cancel</button>
             <button data-x="s" style="background:${BRAND};color:#fff">Save</button></div>`;
        const card = btn.closest('.fwh-card');
        card.style.position = 'relative';
        pop.style.top = '38px'; pop.style.right = '12px';
        card.appendChild(pop);
        pop.onclick = e => e.stopPropagation();
        pop.querySelector('[data-x="c"]').onclick = () => pop.remove();
        pop.querySelector('[data-x="s"]').onclick = async () => {
          const settings = {};
          pop.querySelectorAll('input[data-k]').forEach(i => { if (i.value !== '') settings[i.getAttribute('data-k')] = Number(i.value); });
          try {
            const t = await tok();
            const h = { 'Content-Type': 'application/json' };
            if (t) h.Authorization = 'Bearer ' + t;
            if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
            const r = await fetch(apiBase() + '/ehp/settings', { method: 'POST', headers: h, body: JSON.stringify({ settings }) });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            pop.remove(); render(true);
          } catch (e) { alert('Could not save: ' + (e.message || e)); }
        };
        const off = () => { pop.remove(); document.removeEventListener('click', off); };
        setTimeout(() => document.addEventListener('click', off), 0);
      };
    });
  }

  // ── charts ──
  const _charts = {};
  const PALETTE = ['#990033', '#0EA5E9', '#F59E0B', '#34C759', '#8B5CF6', '#EC4899'];
  function mkChart(id, cfg) {
    if (!window.Chart) return;
    const cv = el(id); if (!cv) return;
    try { if (_charts[id]) { _charts[id].destroy(); delete _charts[id]; } } catch (e) {}
    try { _charts[id] = new Chart(cv.getContext('2d'), cfg); } catch (e) { console.warn('[fwh] chart', id, e.message); }
  }
  const baseOpts = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { labels: { boxWidth: 10, font: { size: 10 } } } },
    scales: { x: { grid: { display: false }, ticks: { font: { size: 9 } } },
              y: { beginAtZero: true, ticks: { font: { size: 9 } }, grid: { display: false }, border: { display: false } } },
  };

  function drawCharts(ts) {
    if (!ts || !window.Chart) return;
    const labels = (ts.series || []).map(d => d.date.slice(5));
    const cum = (key) => { let a = 0; return (ts.series || []).map(d => (a += d[key] || 0)); };

    // 1. running throughput — lines, not bars
    const backlog = ts.backlog || [];
    const limit = ts.backlog_limit || 0;
    mkChart('fwh-c-flow', { type: 'line', data: { labels, datasets: [
      { label: 'Ordered',   data: cum('envelopes_ordered'),   borderColor: PALETTE[1], backgroundColor: 'rgba(14,165,233,.10)', fill: true, tension: .32, pointRadius: 2, borderWidth: 2 },
      { label: 'Assembled', data: cum('envelopes_assembled'), borderColor: PALETTE[2], backgroundColor: 'transparent', tension: .32, pointRadius: 2, borderWidth: 2 },
      { label: 'Lodged',    data: cum('envelopes_dispatched'),borderColor: PALETTE[0], backgroundColor: 'transparent', tension: .32, pointRadius: 2, borderWidth: 2 },
      { label: 'Backlog',   data: backlog, borderColor: RED, backgroundColor: 'rgba(179,63,64,.10)',
        fill: true, tension: .32, pointRadius: 0, borderWidth: 1.5, borderDash: [4,3] },
      ...(limit > 0 ? [{ label: `SLA limit (${nf(limit)})`, data: labels.map(()=>limit),
        borderColor: 'rgba(179,63,64,.5)', borderDash: [2,4], pointRadius: 0, borderWidth: 1, fill: false }] : []),
    ] }, options: baseOpts });

    // 2. days of cover — radial gauges, one per flavour
    // Keep flavours with no burn rate. Filtering them out meant freshly received stock
    // showed nothing at all, which is the moment you most want to see it.
    const cover = (ts.inventory || []).filter(i => (i.days_cover !== null) || (i.on_hand || 0) > 0).slice(0, 6);
    const wrap = el('fwh-gauges');
    if (wrap && cover.length) {
      const _cfg = ts.settings || {};
      // Scale to the warning threshold so the arc means something absolute, not relative.
      const FULL = Math.max(_cfg.cover_warning_days || 30, ...cover.map(i => i.days_cover || 0));
      wrap.innerHTML = cover.map((i, k) => `<div class="fwh-gauge">
          <canvas id="fwh-g-${k}"></canvas>
          <div class="fwh-gsku" title="${esc(i.sku)}">${esc(i.sku)}</div>
        </div>`).join('');
      const cfg = ts.settings || {};
      const crit = cfg.cover_critical_days || 10, warn = cfg.cover_warning_days || 30;
      cover.forEach((i, k) => {
        const noBurn = i.days_cover === null;
        const days = Math.max(0, i.days_cover || 0);
        const col = noBurn ? '#34C759'
                  : i.status === 'critical' ? RED : i.status === 'warning' ? AMBER
                  : days <= crit ? RED : days <= warn ? AMBER : '#34C759';
        mkChart('fwh-g-' + k, {
          type: 'doughnut',
          data: { labels: ['Days of cover', 'Remaining scale'],
                  datasets: [{ data: noBurn ? [FULL, 0] : [Math.min(days, FULL), Math.max(0, FULL - days)],
                    backgroundColor: [col, 'rgba(0,0,0,.06)'], borderWidth: 0 }] },
          options: { responsive: true, maintainAspectRatio: false, cutout: '72%',
            rotation: -110, circumference: 220,
            plugins: { legend: { display: false }, tooltip: { enabled: false } } },
          plugins: [{
            id: 'centre' + k,
            afterDraw(ch) {
              const { ctx, chartArea } = ch; if (!chartArea) return;
              const x = (chartArea.left + chartArea.right) / 2;
              const y = (chartArea.top + chartArea.bottom) / 2 + 4;
              ctx.save();
              ctx.textAlign = 'center'; ctx.fillStyle = col;
              ctx.font = '700 21px -apple-system,Segoe UI,Roboto,sans-serif';
              ctx.fillText(noBurn ? nf(i.on_hand || 0) : String(days), x, y);
              ctx.fillStyle = '#AEAEB2'; ctx.font = '500 9px -apple-system,Segoe UI,Roboto,sans-serif';
              ctx.fillText(noBurn ? 'in stock' : 'days', x, y + 13);
              ctx.restore();
            },
          }],
        });
      });
    } else if (wrap) {
      wrap.innerHTML = `<div class="fwh-none">No stocked flavours yet &mdash; record an inbound receipt to establish stock.</div>`;
    }

    // 3. stock burn-down per flavour
    const inv = ts.inventory || [];
    if (inv.length) {
      mkChart('fwh-c-stock', { type: 'line', data: { labels,
        datasets: [
          ...inv.map((i, k) => ({ label: i.sku, data: i.points.map(p => p.on_hand),
            borderColor: PALETTE[(k + 1) % PALETTE.length], backgroundColor: 'transparent',
            tension: .3, pointRadius: 2, borderWidth: 2 })),
          // One reorder line at the highest reorder point — below it, an order placed today lands late.
          ...(() => { const rp = Math.max(0, ...inv.map(i => i.reorder_point || 0));
              return rp > 0 ? [{ label: `Reorder point (${nf(rp)})`, data: labels.map(()=>rp),
                borderColor: 'rgba(179,63,64,.55)', borderDash: [3,4], pointRadius: 0, borderWidth: 1.5, fill: false }] : []; })(),
        ] },
        options: { ...baseOpts, scales: { ...baseOpts.scales, y: { ...baseOpts.scales.y, beginAtZero: false } } } });
    } else {
      const cv = el('fwh-c-stock');
      if (cv && cv.parentNode) cv.parentNode.innerHTML = `<div class="fwh-none">No stocked components yet &mdash; record an inbound receipt.</div>`;
    }
  }

  async function check() {
    const activeClient = window.pinpointClient || 'unknown';
    if (_capClient === activeClient) {           // already known for this client
      if (!_on) { toggleIconicSections(false); const h0 = el('fwh'); if (h0) h0.remove(); return; }
      if (!onWeekHub()) { const h1 = el('fwh'); if (h1) h1.style.display = 'none'; return; }
      const h2 = el('fwh'); if (h2) h2.style.display = '';
      toggleIconicSections(true);
      return render(false);
    }
    // Capability-driven, and it must FAIL CLOSED. A cold load returns 401 before the Clerk
    // token is ready; treating that as "enabled" would hide the ICONIC layout and leave a
    // blank page until the user happened to navigate away and back.
    let enabled;
    try {
      await req('/ehp/queue');
      enabled = true;                                   // definitive: fulfilment client
    } catch (e) {
      if (e.status === 409 || e.status === 403) enabled = false;   // definitive: not one
      else return;                                      // transient — change nothing, retry later
    }
    _on = enabled;
    _capClient = activeClient;                   // definitive for this client
    try { localStorage.setItem(lsKey(), enabled ? '1' : '0'); } catch (e) {}
    const host = el('fwh');
    if (!_on) { if (host) host.remove(); toggleIconicSections(false); return; }
    if (!onWeekHub()) { if (host) host.style.display = 'none'; return; }
    if (host) host.style.display = '';
    toggleIconicSections(true);
    render(false);
  }

  function applyImmediately() {
    if (!_on || !onWeekHub()) return;
    styles();
    toggleIconicSections(true);
    const host = ensureHost();
    if (host && !host.dataset.loaded) host.innerHTML = skeleton();
  }

  function skeleton() {
    const box = 'background:#fff;border:0.5px solid rgba(0,0,0,0.09);border-radius:12px;'
                + 'box-shadow:0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06);';
    return `<div class="fwh-kpis">${Array(5).fill(0).map(()=>`<div class="fwh-kpi" style="${box}">
      <div class="fwh-kl" style="opacity:.35">&nbsp;</div>
      <div class="fwh-kv" style="color:${LIGHT};opacity:.35">&mdash;</div>
      <div class="fwh-ks" style="opacity:.35">&nbsp;</div></div>`).join('')}</div>
      <div class="fwh-grid"><div class="fwh-card" style="height:190px"></div><div class="fwh-card" style="height:190px"></div></div>`;
  }

  async function loadConn() {
    try { _conn = await req('/shopify/status'); } catch (e) { _conn = null; }
  }

  function init() {
    styles();
    applyImmediately();          // before any network call
    check();
    // Live refresh — the panel previously only updated when you navigated away and back.
    setInterval(() => {
      if (!_on || !onWeekHub() || document.visibilityState !== 'visible') return;
      if (document.visibilityState === 'visible') render(true);
    }, 20000);
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && _on && onWeekHub()) render(true);
    });
    window.addEventListener('state:ready', () => { _lastWeek = ''; check(); });
    window.addEventListener('hashchange', () => { applyImmediately(); check(); });
    // Client and week changes both raise events, so this is only a backstop. Four seconds
    // meant a needless pass fifteen times a minute.
    setInterval(() => { if (document.visibilityState === 'visible') check(); }, 15000);
    window.refreshFulfilmentWeekHub = () => render(true);
    console.log('[fulfilment-weekhub] module v10 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
