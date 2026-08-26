/* ── VelOzity Pinpoint — EHP Geography & Reports v1 ──
   Where sample envelopes are going, and the client's self-service downloads.
   Capability-gated on fulfilment_geo. Rendered as an overlay, so it never touches
   the page router or any ICONIC surface.

   Design note: this is a tile-grid cartogram, not a geographic map. Every state is
   the same size, which is the point — on a real map the shading tracks land area and
   population, and California is dark forever. Equal tiles plus a per-capita toggle
   make an under-served state as visible as a large one. */
;(function () {
  'use strict';

  const DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const AMBER = '#FFD014', RED = '#B33F40', GREEN = '#34C759';
  // One hue per product line. Two hues on ONE map cannot be read where both ship to the
  // same state, so the two lines get their own grid and share a scale instead.
  const LINE_HUES = [
    { h: 350, s: 72 },   // first line  — magenta/red family
    { h: 196, s: 64 },   // second line — teal/blue family
  ];
  const NEUTRAL = { h: 220, s: 8 };
  // Cards sit raised by default rather than flat-on-white. Kept soft and low so the page
  // still reads as one surface — hover adds travel on top of this, not a first shadow.
  const LIFT = '0 1px 2px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06)';

  let _enabled = false, _capClient = null;
  let _month = null, _mode = 'volume', _sel = null, _data = null;

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const el = id => document.getElementById(id);
  const nfmt = n => Number(n || 0).toLocaleString();
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

  // ── tile-grid layout: [col, row] on a 12 × 8 grid ──
  const GRID = {
    AK:[0,0], ME:[11,0],
    VT:[10,1], NH:[11,1],
    WA:[0,2], ID:[1,2], MT:[2,2], ND:[3,2], MN:[4,2], IL:[5,2], WI:[6,2], MI:[7,2], NY:[9,2], RI:[10,2], MA:[11,2],
    OR:[0,3], NV:[1,3], WY:[2,3], SD:[3,3], IA:[4,3], IN:[5,3], OH:[6,3], PA:[7,3], NJ:[8,3], CT:[9,3],
    CA:[0,4], UT:[1,4], CO:[2,4], NE:[3,4], MO:[4,4], KY:[5,4], WV:[6,4], VA:[7,4], MD:[8,4], DE:[9,4],
    AZ:[1,5], NM:[2,5], KS:[3,5], AR:[4,5], TN:[5,5], NC:[6,5], SC:[7,5], DC:[8,5],
    OK:[2,6], LA:[3,6], MS:[4,6], AL:[5,6], GA:[6,6],
    HI:[0,7], TX:[2,7], FL:[7,7]
  };
  const CELL = 30, GAP = 4;

  // ── styles ──
  function styles() {
    if (el('ehpgeo-styles')) return;
    const s = document.createElement('style'); s.id = 'ehpgeo-styles';
    s.textContent = `
            .eg-ov{position:fixed;inset:0;background:#fff;z-index:9500;
             display:flex;align-items:stretch;justify-content:center;}
      .eg-panel{background:#fff;width:100%;height:100%;display:flex;flex-direction:column;overflow:hidden;}
      .eg-head{display:flex;justify-content:space-between;align-items:center;gap:14px;
               padding:16px 28px;border-bottom:0.5px solid rgba(0,0,0,0.08);flex:0 0 auto;}
      .eg-t{font-size:15px;font-weight:700;color:${DARK};letter-spacing:-0.01em;}
      .eg-s{font-size:11px;color:${LIGHT};margin-top:2px;}
      .eg-x{background:none;border:none;font-size:22px;color:${LIGHT};cursor:pointer;line-height:1;padding:0 4px;}
      .eg-x:hover{color:${DARK};}
      /* The overlay is an opaque full-viewport surface at z-index 9500, which paints over the
         fixed Pulse bar (z-index 50). Rather than raising .pulse-bar globally — shared CSS that
         would change layering on every ICONIC overlay too — lift it only while this page is open,
         and reserve room at the foot of the scroll so the bar never sits on top of a report tile. */
      /* The page behind keeps its own scrollbar otherwise, so the overlay shows two. */
      body.eg-open{overflow:hidden;}
      /* Pulse layering. #pulse-panel is fixed at bottom:57px with a 2px border and
         max-height:0 when closed — normally an invisible sliver tucked behind the bar.
         Lift it ABOVE the bar and that sliver becomes a second red outline, which is
         exactly what was happening. Bar on top, panel just beneath it, both above the
         overlay, and both docked flush so the floating treatment does not read as a box
         against this flat white sheet. */
      body.eg-open #pulse-bar{z-index:9600;margin:0;width:100%;border-radius:0;
                              border-left:none;border-right:none;box-shadow:none;}
      body.eg-open #pulse-panel{z-index:9599;left:0;right:0;border-radius:0;
                                border-left:none;border-right:none;}
      .eg-body{padding:20px 28px 104px;overflow:auto;flex:1 1 auto;max-width:1560px;width:100%;margin:0 auto;}
      .eg-grid2{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:22px;align-items:start;}
      @media (max-width:1080px){ .eg-grid2{grid-template-columns:1fr;} }
      /* Analytics strip — sits under the maps so the layout holds whether one product
         line is present or two. */
      .eg-strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:14px;}
      @media (max-width:900px){ .eg-strip{grid-template-columns:repeat(2,minmax(0,1fr));} }
      @media (max-width:520px){ .eg-strip{grid-template-columns:1fr;} }
      .eg-stat{border:0.5px solid rgba(0,0,0,0.1);border-radius:12px;padding:12px 14px;min-height:82px;
               display:flex;flex-direction:column;justify-content:space-between;box-shadow:${LIFT};}
      .eg-sl{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .eg-sv{font-size:21px;font-weight:700;color:${DARK};letter-spacing:-0.02em;margin-top:2px;
             font-variant-numeric:tabular-nums;line-height:1.1;}
      .eg-ss{font-size:10px;color:${LIGHT};margin-top:4px;line-height:1.35;}
      /* Top row: cartogram(s) on the left at a fixed sensible size, live metrics filling the
         space beside them. Unbounded width made the state tiles 90px and the map ate the page. */
      .eg-top{display:grid;grid-template-columns:minmax(0,auto) minmax(0,1fr);gap:14px;align-items:stretch;}
      @media (max-width:1100px){ .eg-top{grid-template-columns:1fr;} }
      .eg-maps{display:flex;gap:14px;flex-wrap:wrap;}
      .eg-maps > .eg-card{width:404px;max-width:100%;}
      .eg-maps svg{width:100%;max-width:372px;height:auto;display:block;margin-top:auto;
                   margin-bottom:auto;align-self:center;}
      .eg-two{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:14px;align-items:stretch;}
      @media (max-width:900px){ .eg-two{grid-template-columns:1fr;} }
      /* Every panel on the page is this card. One border weight, one radius, one padding —
         the previous mix of sizes is what made the layout read as loose. */
      .eg-card{border:0.5px solid rgba(0,0,0,0.1);border-radius:12px;padding:14px 16px;
               background:#fff;display:flex;flex-direction:column;box-shadow:${LIFT};}
      .eg-ml{font-size:12px;font-weight:700;color:${DARK};letter-spacing:-0.005em;}
      .eg-mv{font-size:10px;color:${LIGHT};margin-top:2px;}
      .eg-sec{}
      .eg-cell{cursor:pointer;transition:opacity .12s;}
      .eg-cell:hover{opacity:.72;}
      .eg-ab{font-size:8.5px;font-weight:600;pointer-events:none;}
      .eg-sel{stroke:${DARK};stroke-width:1.6;}
      .eg-seg{display:inline-flex;border:0.5px solid rgba(0,0,0,0.14);border-radius:8px;overflow:hidden;}
      .eg-seg button{border:none;background:#fff;font-size:11px;padding:5px 11px;cursor:pointer;color:${MID};}
      .eg-seg button.on{background:${DARK};color:#fff;}
      .eg-sel-in{border:0.5px solid rgba(0,0,0,0.16);border-radius:8px;padding:5px 9px;font-size:11px;color:${DARK};background:#fff;}
      .eg-ins{display:flex;flex-direction:column;gap:9px;}
      .eg-ic{border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:12px 13px;background:#fff;
             box-shadow:${LIFT};}
      .eg-icat{font-size:8.5px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;}
      .eg-it{font-size:12px;font-weight:600;color:${DARK};margin:5px 0 4px;line-height:1.3;}
      .eg-io{font-size:11px;color:${MID};line-height:1.45;}
      .eg-ia{font-size:10px;color:${MID};background:#F9F9FB;border-radius:7px;padding:7px 9px;margin-top:7px;line-height:1.45;}
      /* Four across at every usable width, so eight reports are always two even rows. */
      .eg-tiles{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:8px;}
      @media (max-width:1180px){ .eg-tiles{grid-template-columns:repeat(2,minmax(0,1fr));} }
      @media (max-width:620px){ .eg-tiles{grid-template-columns:1fr;} }
      .eg-tile{text-align:left;border:0.5px solid rgba(0,0,0,0.1);border-radius:12px;padding:14px 15px;
               background:#fff;cursor:pointer;display:flex;flex-direction:column;height:128px;
               box-shadow:${LIFT};
               transition:border-color .16s ease,transform .16s ease,box-shadow .16s ease;}
      .eg-tile:hover{border-color:rgba(0,0,0,0.22);transform:translateY(-4px);
                     box-shadow:0 12px 26px rgba(0,0,0,0.11);}
      .eg-tile:active{transform:translateY(-1px);}
      .eg-tile:disabled{opacity:.5;cursor:default;transform:none;box-shadow:none;}
      .eg-thead{display:flex;align-items:flex-start;gap:9px;}
      .eg-ico{flex:0 0 auto;width:26px;height:26px;border-radius:7px;background:#F5F5F7;
              display:flex;align-items:center;justify-content:center;color:${MID};}
      .eg-tile:hover .eg-ico{background:${DARK};color:#fff;}
      .eg-tn{font-size:12px;font-weight:600;color:${DARK};line-height:1.3;flex:1;}
      /* Clamped so a long description can never make one tile taller than its neighbour. */
      .eg-td{font-size:10px;color:${LIGHT};margin-top:9px;line-height:1.45;flex:1;overflow:hidden;
             display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;}
      .eg-thead{min-height:34px;}
      .eg-fmt{font-size:8.5px;font-weight:700;color:${LIGHT};letter-spacing:.05em;}
      .eg-tbl{width:100%;border-collapse:collapse;font-size:11px;margin-top:8px;}
      .eg-tbl th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};
                 font-weight:600;padding:5px 6px;border-bottom:0.5px solid rgba(0,0,0,0.08);}
      .eg-tbl td{padding:5px 6px;border-bottom:0.5px solid rgba(0,0,0,0.05);color:${DARK};}
      .eg-tbl td.n{text-align:right;font-variant-numeric:tabular-nums;}
      .eg-note{font-size:10px;color:${LIGHT};line-height:1.5;}
      .eg-sec{font-size:10px;font-weight:700;color:${LIGHT};margin:20px 0 9px;
              text-transform:uppercase;letter-spacing:.07em;}
    `;
    document.head.appendChild(s);
  }

  // ── nav visibility ──
  async function refreshEnabled() {
    const active = window.pinpointClient || 'unknown';
    const nav = el('nav-ehp-geo');
    if (_capClient === active) { if (nav) nav.style.display = _enabled ? '' : 'none'; return; }
    try { await req('/ehp/geo'); _enabled = true; }
    catch (e) {
      // Fail closed on a definite refusal; leave the nav alone on a transient error.
      if (e.status === 409 || e.status === 403) _enabled = false; else return;
    }
    _capClient = active;
    if (nav) nav.style.display = _enabled ? '' : 'none';
  }

  // Injected next to Live Map so it reads as the client-facing map for EHP.
  function injectNav() {
    if (el('nav-ehp-geo')) return;
    const after = el('nav-map') || el('nav-exec');
    if (!after || !after.parentNode) return;
    const a = document.createElement('a');
    a.className = after.className; a.id = 'nav-ehp-geo'; a.href = '#ehp-geo';
    a.textContent = 'Geography & Reports';
    a.style.display = 'none';                       // until the capability check passes
    a.addEventListener('click', e => { e.preventDefault(); open(); });
    after.parentNode.insertBefore(a, after.nextSibling);
  }

  // ── colour ──
  function shade(v, max, hue) {
    if (!max || !v) return `hsl(${NEUTRAL.h} ${NEUTRAL.s}% 96%)`;
    // sqrt keeps the mid-range readable; a linear ramp collapses everything but the top.
    const t = Math.sqrt(Math.min(1, v / max));
    return `hsl(${hue.h} ${hue.s}% ${Math.round(92 - t * 52)}%)`;
  }

  function cartogram(line, hue, idx) {
    const vals = {}, byState = {};
    for (const st of (_data.states || [])) {
      const v = line ? (st.by_line[line] || 0) : st.envelopes;
      if (!v) continue;
      byState[st.state] = st;
      vals[st.state] = _mode === 'per_capita' && st.per_100k != null && st.envelopes
        ? Math.round(v / st.envelopes * st.per_100k * 10) / 10   // that line's share, per 100k
        : v;
    }
    const max = Math.max(0, ...Object.values(vals));
    const W = 12 * (CELL + GAP), H = 8 * (CELL + GAP);
    const cells = Object.entries(GRID).map(([ab, [c, r]]) => {
      const v = vals[ab] || 0;
      const x = c * (CELL + GAP), y = r * (CELL + GAP);
      const fill = shade(v, max, hue);
      const dark = max && v && Math.sqrt(v / max) > 0.55;
      return `<g class="eg-cell" data-st="${ab}" data-line="${esc(line||'')}">
        <rect x="${x}" y="${y}" width="${CELL}" height="${CELL}" rx="5" fill="${fill}"
          ${_sel === ab ? 'class="eg-sel"' : 'stroke="rgba(0,0,0,0.05)" stroke-width="0.5"'}></rect>
        <text class="eg-ab" x="${x + CELL/2}" y="${y + CELL/2 + 3}" text-anchor="middle"
          fill="${dark ? '#fff' : (v ? DARK : LIGHT)}">${ab}</text>
        <title>${ab}: ${nfmt(v)}${_mode==='per_capita' ? ' per 100k' : ' envelopes'}</title>
      </g>`;
    }).join('');
    const total = Object.values(vals).reduce((a, b) => a + b, 0);
    return `<div class="eg-card">
      <div class="eg-ml">${esc(line || 'All lines')}</div>
      <div class="eg-mv">${_mode === 'per_capita'
        ? 'Envelopes per 100,000 residents'
        : nfmt(Math.round(total)) + ' envelopes'}</div>
      <svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto;margin-top:10px;" data-map="${idx}">${cells}</svg>
    </div>`;
  }

  // ── insights: computed from the aggregates already on screen ──
  // Deterministic on purpose. A client-facing panel that says "AI unavailable" is worse
  // than one showing plainer numbers, so this is the floor, not the fallback.
  // Below this, geographic statements are arithmetic on noise — two envelopes will always
  // be "100% concentrated in the top five states". Integrity findings are exempt: those
  // are true at any volume.
  const MIN_FOR_GEO = 50;

  function insights() {
    const st = _data.states || [], out = [];
    const total = _data.totals.envelopes || 0;
    const rep = _data.repeat || {};

    if (!total) return [{ cat: 'coverage', col: LIGHT, title: 'No dispatches in this period',
      obs: 'Nothing was lodged with USPS in the selected month.',
      act: 'Pick a different month, or check the Assembly tab for batches still awaiting dispatch.' }];

    if (total < MIN_FOR_GEO) {
      const early = [];
      const unmappedEarly = st.reduce((a, s) => a + (s.by_line.UNMAPPED || 0), 0);
      if (unmappedEarly) early.push({ cat: 'integrity', col: RED,
        title: `${nfmt(unmappedEarly)} envelope(s) have no product line`,
        obs: 'These dispatched before their Shopify SKU was mapped, so they are absent from the per-line views.',
        act: 'Map the SKU on EHP Ops → Recipe. Historic orders keep the line they were processed under.' });
      early.push({ cat: 'coverage', col: LIGHT,
        title: 'Too little volume for geographic analysis',
        obs: `${nfmt(total)} envelope(s) across ${st.length} state(s). Share and concentration figures at this volume describe the sample, not the market.`,
        act: `Geographic insights start once around ${MIN_FOR_GEO} envelopes have dispatched in a month.` });
      return early;
    }

    const ranked = st.filter(s => s.per_100k != null).sort((a, b) => b.per_100k - a.per_100k);
    const natlPer100k = ranked.length
      ? total / (ranked.reduce((a, s) => a + (s.envelopes / (s.per_100k || 1)) * 1, 0) || 1) : null;

    if (ranked.length >= 3) {
      const top = ranked[0];
      out.push({ cat: 'geography', col: '#2E7D9E',
        title: `${top.state} is the strongest per-head market`,
        obs: `${nfmt(top.envelopes)} envelopes, or ${top.per_100k} per 100,000 residents — the highest of ${ranked.length} states with volume.`,
        act: 'Worth checking against ad spend by region: a high per-head figure with low spend is an organic pocket to lean into.' });

      const big = st.slice(0, 8).filter(s => s.per_100k != null).sort((a, b) => a.per_100k - b.per_100k)[0];
      if (big && big.state !== top.state) {
        out.push({ cat: 'geography', col: AMBER,
          title: `${big.state} under-indexes for its size`,
          obs: `${nfmt(big.envelopes)} envelopes places it in the top eight by volume, but only ${big.per_100k} per 100,000 residents.`,
          act: 'Large raw numbers here are a population effect rather than genuine traction.' });
      }
    }

    const lines = _data.product_lines || [];
    if (lines.length >= 2) {
      const totals = {}; lines.forEach(l => totals[l] = 0);
      st.forEach(s => lines.forEach(l => totals[l] += (s.by_line[l] || 0)));
      const skew = st.filter(s => s.envelopes >= 25).map(s => {
        const a = s.by_line[lines[0]] || 0;
        return { state: s.state, share: a / s.envelopes, env: s.envelopes };
      });
      const natl = totals[lines[0]] / Math.max(1, totals[lines[0]] + totals[lines[1]]);
      const skewed = skew.sort((a, b) => Math.abs(b.share - natl) - Math.abs(a.share - natl))[0];
      if (skewed && Math.abs(skewed.share - natl) > 0.12) {
        const favours = skewed.share > natl ? lines[0] : lines[1];
        out.push({ cat: 'product mix', col: '#7B5EA7',
          title: `${skewed.state} skews toward ${favours}`,
          obs: `${Math.round(skewed.share * 100)}% of ${skewed.state} volume is ${lines[0]}, against ${Math.round(natl * 100)}% nationally.`,
          act: 'Regional preference like this is usually actionable for ad creative and retail placement before it is visible in sales data.' });
      }
    }

    const unmapped = st.reduce((a, s) => a + (s.by_line.UNMAPPED || 0), 0);
    if (unmapped) out.push({ cat: 'integrity', col: RED,
      title: `${nfmt(unmapped)} envelopes have no product line`,
      obs: 'These dispatched before their Shopify SKU was mapped, so they are absent from the per-line views.',
      act: 'Map the SKU on EHP Ops → Recipe. Historic orders keep the line they were processed under.' });

    if (rep.rate_pct >= 8 && rep.distinct_addresses >= 40) {
      out.push({ cat: 'repeat demand', col: rep.rate_pct >= 20 ? AMBER : '#2E7D9E',
        title: `${rep.rate_pct}% of addresses have sampled more than once`,
        obs: `${nfmt(rep.addresses_repeating)} of ${nfmt(rep.distinct_addresses)} distinct addresses have taken more than one free sample.`,
        act: rep.rate_pct >= 20
          ? 'At this level it is worth checking whether the campaign is being gamed as well as read as demand.'
          : 'Repeat requests are the closest signal to intent available here — Pinpoint holds no paid-order data, so this is not a conversion rate.' });
    }

    if (st.length >= 5) {
      const conc = st.slice(0, 5).reduce((a, s) => a + s.envelopes, 0) / total;
      out.push({ cat: 'coverage', col: conc > 0.6 ? AMBER : GREEN,
        title: conc > 0.6 ? 'Volume is concentrated in five states' : 'Volume is well spread',
        obs: `The top five of ${st.length} states with volume account for ${Math.round(conc * 100)}% of envelopes.`,
        act: conc > 0.6 ? 'A narrow footprint makes the campaign sensitive to a single region going quiet.'
                        : 'Broad distribution suggests national reach rather than a few concentrated pockets.' });
    }

    return out.slice(0, 5);
  }

  function insightCard(i) {
    return `<div class="eg-ic" style="border-left:3px solid ${i.col};">
      <div class="eg-icat" style="color:${i.col};">${esc(i.cat)}</div>
      <div class="eg-it">${esc(i.title)}</div>
      <div class="eg-io">${esc(i.obs)}</div>
      <div class="eg-ia">${esc(i.act)}</div>
    </div>`;
  }

  // ── report icons ──
  // Line icons only; a filled or coloured set would compete with the map for attention.
  const ICONS = {
    'reconciliation':    '<path d="M4 3h11l5 5v13H4z"/><path d="M15 3v5h5"/><path d="M8 13h8M8 17h5"/>',
    'dispatch-register': '<path d="M3 7h13v9H3z"/><path d="M16 10h3l2 3v3h-5z"/><circle cx="7" cy="18" r="1.6"/><circle cx="18" cy="18" r="1.6"/>',
    'inventory-cover':   '<path d="M3 8l9-5 9 5v8l-9 5-9-5z"/><path d="M3 8l9 5 9-5M12 13v8"/>',
    'count-variance':    '<path d="M4 20V10M10 20V4M16 20v-7M22 20h-20"/>',
    'geography':         '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c2.6 2.7 4 5.8 4 9s-1.4 6.3-4 9c-2.6-2.7-4-5.8-4-9s1.4-6.3 4-9z"/>',
    'sla':               '<circle cx="12" cy="13" r="8"/><path d="M12 9v4l3 2M9 2h6"/>',
    'inbound-receipts':  '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><path d="M7 10l5 5 5-5M12 15V3"/>',
    'stock-ledger':      '<path d="M4 4h16v16H4z"/><path d="M4 9h16M4 15h16M9 4v16"/>',
  };
  const icon = (id) => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
      ${ICONS[id] || ICONS['reconciliation']}</svg>`;

  // ── downloads ──
  let _reports = [];
  async function download(id, name, fmt) {
    const t = await tok();
    const headers = {}; if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + '/ehp/report/' + id + '?month=' + encodeURIComponent(_month), { headers });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const blob = await r.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `EHP_${name.replace(/[^A-Za-z0-9]+/g, '_')}_${_month}.${fmt}`;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1500);
  }

  // ── dispatch trend ──
  // A plain area sparkline per line. Enough to see shape and cadence; anything more
  // elaborate would compete with the cartogram.
  function trendChart() {
    const t = _data.trend || [];
    if (!t.length) return `<div class="eg-note" style="padding:16px 2px;">No dispatches yet this month.</div>`;
    const days = Array.from(new Set(t.map(r => r.d))).sort();
    const lines = (_data.product_lines || []).length ? _data.product_lines.slice(0, 2) : [null];
    const series = lines.map((l, i) => ({
      line: l, hue: LINE_HUES[i] || LINE_HUES[0],
      pts: days.map(d => t.filter(r => r.d === d && (l == null || r.pl === l))
                          .reduce((a, r) => a + r.envelopes, 0))
    }));
    const max = Math.max(1, ...series.flatMap(x => x.pts));
    const W = 560, H = 132, pad = 8;
    const Y = v => H - pad - (v / max) * (H - pad * 2 - 8);
    let body;
    if (days.length < 3) {
      // A line through one or two points draws nothing useful. Bars read correctly from
      // the first day of activity.
      // Cap the bar width: one day of data spread across the full card reads as a slab,
      // not a chart.
      const slot = (W - pad * 2) / Math.max(days.length, 6);
      const bw = Math.min(26, slot / (series.length + 0.4));
      body = days.map((d, i) => series.map((sr, k) => {
        const v = sr.pts[i]; if (!v) return '';
        const x = pad + (i + 0.5) * slot - (series.length * bw) / 2 + k * bw;
        const y = Y(v);
        return `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${(bw-3).toFixed(1)}" height="${(H-pad-y).toFixed(1)}"
                  rx="3" fill="hsl(${sr.hue.h} ${sr.hue.s}% 46%)"><title>${esc(d)} — ${nfmt(v)}</title></rect>`;
      }).join('')).join('');
    } else {
      const X = i => pad + i * (W - pad * 2) / (days.length - 1);
      body = series.map(sr => {
        const d = sr.pts.map((v, i) => `${i ? 'L' : 'M'}${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join('');
        const area = `${d}L${X(sr.pts.length-1).toFixed(1)},${H-pad}L${X(0).toFixed(1)},${H-pad}Z`;
        const col = `hsl(${sr.hue.h} ${sr.hue.s}% 46%)`;
        return `<path d="${area}" fill="${col}" opacity="0.10"/><path d="${d}" fill="none" stroke="${col}" stroke-width="1.8" stroke-linejoin="round"/>`;
      }).join('');
    }
    const paths = body + `<line x1="${pad}" y1="${H-pad}" x2="${W-pad}" y2="${H-pad}" stroke="rgba(0,0,0,0.10)" stroke-width="1"/>`;
    const legend = series.filter(x => x.line).map(sr =>
      `<span style="display:inline-flex;align-items:center;gap:5px;margin-right:12px;">
        <span style="width:8px;height:8px;border-radius:2px;background:hsl(${sr.hue.h} ${sr.hue.s}% 46%);"></span>
        <span style="font-size:10px;color:${MID};">${esc(sr.line)}</span></span>`).join('');
    return `<div style="display:flex;justify-content:space-between;align-items:baseline;">
        <div class="eg-ml">Dispatch trend</div><div>${legend}</div></div>
      <div class="eg-mv">${days.length} day(s) with activity · peak ${nfmt(max)} envelopes</div>
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMax meet"
           style="width:100%;height:148px;margin-top:auto;">${paths}</svg>`;
  }

  function strip() {
    const tot = _data.totals || {}, prev = _data.previous || {}, rep = _data.repeat || {};
    const delta = prev.envelopes ? Math.round((tot.envelopes - prev.envelopes) / prev.envelopes * 100) : null;
    return `
      <div class="eg-strip">
        <div class="eg-stat">
          <div class="eg-sl">Envelopes</div>
          <div class="eg-sv">${nfmt(tot.envelopes)}</div>
          <div class="eg-ss">${delta == null ? 'No prior month to compare'
            : `${delta >= 0 ? '+' : ''}${delta}% vs ${esc(prev.month)}`}</div>
        </div>
        <div class="eg-stat">
          <div class="eg-sl">States reached</div>
          <div class="eg-sv">${nfmt(tot.states)}</div>
          <div class="eg-ss">${prev.states ? `${prev.states} last month` : 'of 51 possible'}</div>
        </div>
        <div class="eg-stat">
          <div class="eg-sl">Repeat requests</div>
          <div class="eg-sv">${rep.rate_pct != null ? rep.rate_pct + '%' : '—'}</div>
          <div class="eg-ss">${nfmt(rep.addresses_repeating)} of ${nfmt(rep.distinct_addresses)} addresses have sampled more than once</div>
        </div>
        <div class="eg-stat">
          <div class="eg-sl">Avg envelopes / order</div>
          <div class="eg-sv">${tot.orders ? (Math.round(tot.envelopes / tot.orders * 100) / 100) : '—'}</div>
          <div class="eg-ss">${nfmt(tot.orders)} orders dispatched</div>
        </div>
      </div>`;
  }

  function topCitiesCard() {
    const topCities = (_data.cities || []).sort((a, b) => b.envelopes - a.envelopes).slice(0, 8);
    return `<div class="eg-card">
        <div class="eg-ml">Top cities</div>
        <div class="eg-mv">Cities under ${_data.small_cell_min} envelopes are grouped</div>
        <table class="eg-tbl"><thead><tr><th>City</th><th>State</th><th>Line</th><th class="n">Envelopes</th></tr></thead>
        <tbody>${topCities.length ? topCities.map(c => `<tr>
          <td>${esc(c.recipient_city || '—')}</td><td style="color:${MID}">${esc(c.recipient_state || '')}</td>
          <td style="color:${MID}">${esc(c.product_line || '—')}</td>
          <td class="n">${nfmt(c.envelopes)}</td></tr>`).join('')
          : `<tr><td colspan="4" style="color:${LIGHT};text-align:center;padding:12px;">No city detail yet.</td></tr>`}</tbody></table>
      </div>`;
  }

  // ── render ──
  function statePanel() {
    if (!_sel) return `<div class="eg-note" style="padding:14px 2px;">Click any state to see its cities, orders and product mix.</div>`;
    const s = (_data.states || []).find(x => x.state === _sel);
    if (!s) return `<div class="eg-note" style="padding:14px 2px;">No dispatches to ${esc(_sel)} in this month.</div>`;
    const cities = (_data.cities || []).filter(c => c.recipient_state === _sel)
      .sort((a, b) => b.envelopes - a.envelopes).slice(0, 12);
    return `
      <div style="display:flex;gap:16px;flex-wrap:wrap;margin:10px 0 6px;">
        <div><div class="eg-mv">Envelopes</div><div style="font-size:18px;font-weight:700;color:${DARK};">${nfmt(s.envelopes)}</div></div>
        <div><div class="eg-mv">Orders</div><div style="font-size:18px;font-weight:700;color:${DARK};">${nfmt(s.orders)}</div></div>
        <div><div class="eg-mv">Per 100k</div><div style="font-size:18px;font-weight:700;color:${DARK};">${s.per_100k != null ? s.per_100k : '—'}</div></div>
        <div><div class="eg-mv">Leading line</div><div style="font-size:13px;font-weight:600;color:${DARK};padding-top:4px;">${esc(s.dominant_line || '—')}</div></div>
      </div>
      <table class="eg-tbl"><thead><tr><th>City</th><th>Line</th><th class="n">Orders</th><th class="n">Envelopes</th></tr></thead>
      <tbody>${cities.length ? cities.map(c => `<tr>
        <td>${esc(c.recipient_city || '—')}</td><td style="color:${MID}">${esc(c.product_line || '—')}</td>
        <td class="n">${nfmt(c.orders)}</td><td class="n">${nfmt(c.envelopes)}</td></tr>`).join('')
        : `<tr><td colspan="4" style="color:${LIGHT};padding:12px;text-align:center;">No city detail.</td></tr>`}</tbody></table>
      <div class="eg-note" style="margin-top:6px;">Cities under ${_data.small_cell_min} envelopes are grouped so a row can never identify one household.</div>`;
  }

  function paint() {
    const body = el('eg-body'); if (!body || !_data) return;
    const lines = _data.product_lines || [];
    const maps = lines.length >= 2
      ? lines.slice(0, 2).map((l, i) => cartogram(l, LINE_HUES[i], i)).join('')
      : cartogram(lines[0] || null, LINE_HUES[0], 0);

    body.innerHTML = `
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px;">
        <select class="eg-sel-in" id="eg-month">
          ${(_data.available_months || [_month]).map(m => `<option value="${esc(m)}" ${m===_month?'selected':''}>${esc(m)}</option>`).join('')}
        </select>
        <div class="eg-seg">
          <button data-mode="volume" class="${_mode==='volume'?'on':''}">Volume</button>
          <button data-mode="per_capita" class="${_mode==='per_capita'?'on':''}">Per 100k</button>
        </div>
        <div class="eg-note">${nfmt(_data.totals.envelopes)} envelopes · ${nfmt(_data.totals.orders)} orders · ${_data.totals.states} states</div>
      </div>

      <div class="eg-grid2">
        <div>
          ${strip()}
          <div class="eg-top">
            <div class="eg-maps">${maps}</div>
            <div class="eg-card">${trendChart()}</div>
          </div>
          <div class="eg-sec">Detail</div>
          <div class="eg-two">
            ${topCitiesCard()}
            <div class="eg-card">
              <div class="eg-ml">${_sel ? esc(_sel) : 'State detail'}</div>
              <div class="eg-mv">${_sel ? 'Cities and product mix' : 'Select a state on the map'}</div>
              ${statePanel()}
            </div>
          </div>
        </div>
        <div>
          <div class="eg-sec" style="margin:0 0 9px;">Insights</div>
          <div class="eg-ins">${insights().map(insightCard).join('')}</div>
        </div>
      </div>

      <div class="eg-sec">Reports &amp; downloads</div>
      <div class="eg-tiles">
        ${_reports.map(r => `<button class="eg-tile" data-rep="${esc(r.id)}" data-rn="${esc(r.name)}" data-rf="${esc(r.format)}">
          <div class="eg-thead">
            <span class="eg-ico">${icon(r.id)}</span>
            <span class="eg-tn">${esc(r.name)}</span>
            <span class="eg-fmt">${esc(r.format.toUpperCase())}</span>
          </div>
          <div class="eg-td">${esc(r.desc)}</div>
        </button>`).join('')}
      </div>
      <div class="eg-note" style="margin-top:10px;">
        All reports cover the calendar month selected above and contain quantities only — no rates or values.
      </div>`;

    el('eg-month').addEventListener('change', e => { _month = e.target.value; _sel = null; load(); });
    body.querySelectorAll('[data-mode]').forEach(b => b.addEventListener('click', () => { _mode = b.getAttribute('data-mode'); paint(); }));
    body.querySelectorAll('.eg-cell').forEach(g => g.addEventListener('click', () => {
      const st = g.getAttribute('data-st');
      _sel = (_sel === st) ? null : st;
      paint();
    }));
    body.querySelectorAll('[data-rep]').forEach(b => b.addEventListener('click', async () => {
      const label = b.querySelector('.eg-tn');
      const orig = label.textContent;
      b.disabled = true; label.textContent = 'Preparing…';
      try { await download(b.getAttribute('data-rep'), b.getAttribute('data-rn'), b.getAttribute('data-rf')); }
      catch (e) { label.textContent = 'Failed — retry'; setTimeout(() => { label.textContent = orig; }, 2500); }
      finally { setTimeout(() => { b.disabled = false; if (label.textContent === 'Preparing…') label.textContent = orig; }, 400); }
    }));
  }

  async function load() {
    const body = el('eg-body'); if (body && !_data) body.innerHTML = `<div style="color:${LIGHT};font-size:12px;padding:30px;text-align:center;">Loading…</div>`;
    try {
      const [g, r] = await Promise.all([
        req('/ehp/geo' + (_month ? '?month=' + encodeURIComponent(_month) : '')),
        _reports.length ? Promise.resolve({ reports: _reports }) : req('/ehp/reports')
      ]);
      _data = g; _month = g.month; _reports = r.reports || _reports;
      paint();
    } catch (e) {
      if (body) body.innerHTML = `<div style="color:${RED};font-size:12px;padding:30px;text-align:center;">Could not load: ${esc(e.message || e)}</div>`;
    }
  }

  function close(){
    const o = document.querySelector('.eg-ov'); if (o) o.remove();
    document.body.classList.remove('eg-open');
  }
  function open() {
    styles();
    if (document.querySelector('.eg-ov')) return;
    const o = document.createElement('div'); o.className = 'eg-ov';
    o.addEventListener('click', e => { if (e.target === o) close(); });
    o.innerHTML = `<div class="eg-panel">
      <div class="eg-head">
        <div><div class="eg-t">EHP Geography &amp; Reports</div>
        <div class="eg-s">Where sample envelopes are going · monthly downloads</div></div>
        <button class="eg-x" id="eg-close">×</button>
      </div>
      <div class="eg-body" id="eg-body">Loading…</div>
    </div>`;
    document.body.appendChild(o);
    document.body.classList.add('eg-open');
    el('eg-close').addEventListener('click', close);
    document.addEventListener('keydown', function esc2(ev){
      if (ev.key === 'Escape') { close(); document.removeEventListener('keydown', esc2); }
    });
    load();
  }

  function init() {
    styles(); injectNav();
    window.openEhpGeo = open;
    refreshEnabled();
    window.addEventListener('state:ready', refreshEnabled);
    // Capability re-check only; it early-returns unless the client actually changed.
    setInterval(() => { if (document.visibilityState === 'visible') refreshEnabled(); }, 15000);
    if (location.hash === '#ehp-geo') setTimeout(open, 600);
    console.log('[ehp-geo] module v8 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
