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
      .eg-ov{position:fixed;inset:0;background:rgba(0,0,0,0.34);z-index:9500;display:flex;
             align-items:center;justify-content:center;padding:24px;
             -webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px);}
      .eg-panel{background:#fff;border-radius:16px;width:min(1280px,100%);max-height:100%;
                display:flex;flex-direction:column;overflow:hidden;
                box-shadow:0 18px 60px rgba(0,0,0,0.22);}
      .eg-head{display:flex;justify-content:space-between;align-items:center;gap:14px;
               padding:16px 20px;border-bottom:0.5px solid rgba(0,0,0,0.08);}
      .eg-t{font-size:15px;font-weight:700;color:${DARK};letter-spacing:-0.01em;}
      .eg-s{font-size:11px;color:${LIGHT};margin-top:2px;}
      .eg-x{background:none;border:none;font-size:22px;color:${LIGHT};cursor:pointer;line-height:1;padding:0 4px;}
      .eg-x:hover{color:${DARK};}
      .eg-body{padding:18px 20px 22px;overflow:auto;}
      .eg-grid2{display:grid;grid-template-columns:1fr 300px;gap:20px;align-items:start;}
      @media (max-width:1080px){ .eg-grid2{grid-template-columns:1fr;} }
      .eg-maps{display:grid;grid-template-columns:1fr 1fr;gap:16px;}
      @media (max-width:760px){ .eg-maps{grid-template-columns:1fr;} }
      .eg-card{border:0.5px solid rgba(0,0,0,0.1);border-radius:12px;padding:14px 16px;}
      .eg-ml{font-size:12px;font-weight:700;color:${DARK};}
      .eg-mv{font-size:10px;color:${LIGHT};margin-top:1px;}
      .eg-cell{cursor:pointer;transition:opacity .12s;}
      .eg-cell:hover{opacity:.72;}
      .eg-ab{font-size:8.5px;font-weight:600;pointer-events:none;}
      .eg-sel{stroke:${DARK};stroke-width:1.6;}
      .eg-seg{display:inline-flex;border:0.5px solid rgba(0,0,0,0.14);border-radius:8px;overflow:hidden;}
      .eg-seg button{border:none;background:#fff;font-size:11px;padding:5px 11px;cursor:pointer;color:${MID};}
      .eg-seg button.on{background:${DARK};color:#fff;}
      .eg-sel-in{border:0.5px solid rgba(0,0,0,0.16);border-radius:8px;padding:5px 9px;font-size:11px;color:${DARK};background:#fff;}
      .eg-ins{display:flex;flex-direction:column;gap:9px;}
      .eg-ic{border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:12px 13px;background:#fff;}
      .eg-icat{font-size:8.5px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;}
      .eg-it{font-size:12px;font-weight:600;color:${DARK};margin:5px 0 4px;line-height:1.3;}
      .eg-io{font-size:11px;color:${MID};line-height:1.45;}
      .eg-ia{font-size:10px;color:${MID};background:#F9F9FB;border-radius:7px;padding:7px 9px;margin-top:7px;line-height:1.45;}
      .eg-tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px;margin-top:8px;}
      .eg-tile{text-align:left;border:0.5px solid rgba(0,0,0,0.1);border-radius:11px;padding:12px 14px;
               background:#fff;cursor:pointer;transition:border-color .12s,transform .12s;}
      .eg-tile:hover{border-color:rgba(0,0,0,0.28);transform:translateY(-1px);}
      .eg-tile:disabled{opacity:.5;cursor:default;transform:none;}
      .eg-tn{font-size:12px;font-weight:600;color:${DARK};display:flex;justify-content:space-between;gap:6px;}
      .eg-td{font-size:10px;color:${LIGHT};margin-top:4px;line-height:1.4;}
      .eg-fmt{font-size:8.5px;font-weight:700;color:${LIGHT};letter-spacing:.05em;}
      .eg-tbl{width:100%;border-collapse:collapse;font-size:11px;margin-top:8px;}
      .eg-tbl th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};
                 font-weight:600;padding:5px 6px;border-bottom:0.5px solid rgba(0,0,0,0.08);}
      .eg-tbl td{padding:5px 6px;border-bottom:0.5px solid rgba(0,0,0,0.05);color:${DARK};}
      .eg-tbl td.n{text-align:right;font-variant-numeric:tabular-nums;}
      .eg-note{font-size:10px;color:${LIGHT};line-height:1.5;}
      .eg-sec{font-size:11px;font-weight:700;color:${DARK};margin:20px 0 8px;
              text-transform:uppercase;letter-spacing:.06em;}
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
  function insights() {
    const st = _data.states || [], out = [];
    const total = _data.totals.envelopes || 0;
    if (!total) return [{ cat: 'coverage', col: LIGHT, title: 'No dispatches in this period',
      obs: 'Nothing was lodged with USPS in the selected month.',
      act: 'Pick a different month, or check the Assembly tab for batches still awaiting dispatch.' }];

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

    const conc = st.slice(0, 5).reduce((a, s) => a + s.envelopes, 0) / total;
    out.push({ cat: 'coverage', col: conc > 0.6 ? AMBER : GREEN,
      title: conc > 0.6 ? 'Volume is concentrated in five states' : 'Volume is well spread',
      obs: `The top five states account for ${Math.round(conc * 100)}% of envelopes across ${st.length} states in total.`,
      act: conc > 0.6 ? 'A narrow footprint makes the campaign sensitive to a single region going quiet.'
                      : 'Broad distribution suggests national reach rather than a few concentrated pockets.' });

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

  // ── render ──
  function statePanel() {
    if (!_sel) return `<div class="eg-note" style="padding:10px 2px;">Select a state to see its cities and product mix.</div>`;
    const s = (_data.states || []).find(x => x.state === _sel);
    if (!s) return `<div class="eg-note" style="padding:10px 2px;">No dispatches to ${esc(_sel)} in this month.</div>`;
    const cities = (_data.cities || []).filter(c => c.recipient_state === _sel)
      .sort((a, b) => b.envelopes - a.envelopes).slice(0, 12);
    return `
      <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:6px;">
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
          <div class="eg-maps">${maps}</div>
          <div class="eg-sec">${_sel ? esc(_sel) : 'State detail'}</div>
          <div class="eg-card">${statePanel()}</div>
        </div>
        <div>
          <div class="eg-sec" style="margin-top:0;">Insights</div>
          <div class="eg-ins">${insights().map(insightCard).join('')}</div>
        </div>
      </div>

      <div class="eg-sec">Reports &amp; downloads</div>
      <div class="eg-tiles">
        ${_reports.map(r => `<button class="eg-tile" data-rep="${esc(r.id)}" data-rn="${esc(r.name)}" data-rf="${esc(r.format)}">
          <div class="eg-tn"><span>${esc(r.name)}</span><span class="eg-fmt">${esc(r.format.toUpperCase())}</span></div>
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
      const label = b.querySelector('.eg-tn span');
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

  function close(){ const o = document.querySelector('.eg-ov'); if (o) o.remove(); }
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
    setInterval(refreshEnabled, 15000);      // the active client can change via the picker
    if (location.hash === '#ehp-geo') setTimeout(open, 600);
    console.log('[ehp-geo] module v1 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
