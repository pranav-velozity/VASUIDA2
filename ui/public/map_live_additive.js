/* map_live_additive.js — VelOzity Pinpoint Live Map
   Additive module — mounts into #page-map
   Phase 1: calculated vessel position from lane dates
   Phase 2 hook: getVesselPosition() / getFlightPosition() — swap for API calls
*/
(function () {
  'use strict';

  // ── Fixed location coordinates (Phase 1 — hardcoded, Phase 2 via lookup table) ──
  const LOCATIONS = {
    supplier:       { name: 'Shenzhen',       lat: 22.54,  lon: 114.06 },
    origin_port:    { name: 'Shenzhen Port',  lat: 22.49,  lon: 113.87 },
    sydney_port:    { name: 'Port Botany',    lat: -33.97, lon: 151.19 },
    sydney_airport: { name: 'Sydney Airport', lat: -33.94, lon: 151.18 },
    client_wh:      { name: 'Sydney WH',      lat: -33.87, lon: 151.20 },
  };

  // ── Stage colors — matte palette ──
  const STAGE_COLOR = {
    at_supplier:  '#9E9689',
    origin_port:  '#B5956A',
    transit:      '#6B8FA8',
    clearing:     '#A89070',
    customs_hold: '#B07070',
    vas:          '#7A9E7E',
    last_mile:    '#8B82B0',
  };

  const STAGE_LABEL = {
    at_supplier:  'At Supplier',
    origin_port:  'At Origin Port',
    transit:      'In Transit',
    clearing:     'At Port / Clearing',
    customs_hold: 'Customs Hold',
    vas:          'At Facility / VAS',
    last_mile:    'Last Mile',
  };

  const W = 900, H = 480;

  // ── Map land polygon data (simplified Natural Earth) ──
  const LAND = [
    [[-140,60],[-130,54],[-124,48],[-124,34],[-117,32],[-97,26],[-87,30],[-81,25],[-80,32],[-76,34],[-75,38],[-70,42],[-66,44],[-64,48],[-66,50],[-64,52],[-66,56],[-78,54],[-82,48],[-84,46],[-88,48],[-94,48],[-100,50],[-104,50],[-110,50],[-120,50],[-128,52],[-136,58],[-140,60]],
    [[-80,12],[-76,8],[-74,4],[-70,0],[-70,-4],[-74,-8],[-76,-12],[-70,-18],[-66,-22],[-64,-26],[-62,-30],[-58,-34],[-56,-38],[-58,-40],[-62,-42],[-66,-44],[-68,-48],[-68,-52],[-66,-54],[-60,-52],[-52,-46],[-48,-28],[-44,-24],[-40,-20],[-36,-12],[-38,-8],[-46,0],[-52,4],[-58,8],[-62,10],[-68,12],[-72,12],[-76,10],[-80,12]],
    [[0,51],[4,52],[8,54],[10,56],[14,56],[18,58],[22,56],[26,60],[28,62],[26,64],[28,66],[30,68],[28,70],[24,68],[20,64],[16,62],[12,60],[8,58],[4,54],[2,52],[0,51],[-2,50],[-4,48],[-6,44],[-2,44],[2,44],[4,48],[0,51]],
    [[-18,16],[-16,12],[-14,8],[-12,4],[-8,4],[-4,4],[0,4],[4,6],[8,4],[12,4],[16,4],[20,0],[24,-4],[28,-8],[32,-12],[34,-20],[36,-24],[34,-28],[30,-32],[28,-34],[22,-34],[18,-34],[16,-30],[14,-26],[12,-22],[10,-18],[8,-14],[8,-4],[8,4],[10,8],[12,14],[14,22],[8,18],[4,14],[0,12],[-4,14],[-8,16],[-12,18],[-16,18],[-18,16]],
    [[26,42],[30,46],[34,48],[38,50],[42,54],[46,56],[50,58],[54,60],[58,60],[62,58],[66,56],[70,54],[74,52],[78,52],[82,54],[86,52],[90,50],[94,48],[98,46],[102,44],[106,42],[110,40],[114,40],[118,40],[122,40],[126,38],[130,36],[134,34],[130,32],[126,30],[122,28],[118,24],[116,22],[118,20],[116,18],[112,14],[108,10],[104,6],[100,4],[100,0],[104,-4],[106,-6],[108,-8],[110,-8],[116,-4],[116,0],[120,4],[124,8],[128,12],[132,14],[136,14],[140,12],[142,10],[142,6],[138,2],[134,-2],[130,-6],[126,-8],[122,-8],[118,-6],[114,-4],[110,-2],[106,2],[102,4],[98,4],[94,8],[90,8],[86,8],[82,8],[78,8],[74,6],[70,8],[66,8],[62,10],[58,12],[54,12],[50,14],[46,12],[42,10],[38,10],[34,10],[30,12],[26,14],[22,12],[18,12],[14,14],[14,18],[18,20],[22,22],[26,24],[26,30],[26,36],[26,42]],
    [[114,-22],[116,-20],[118,-18],[122,-16],[126,-14],[130,-12],[136,-12],[140,-16],[142,-18],[144,-22],[146,-24],[148,-26],[150,-28],[152,-30],[152,-34],[150,-38],[148,-38],[146,-38],[144,-36],[142,-38],[140,-36],[138,-36],[136,-36],[134,-34],[132,-32],[128,-32],[124,-32],[120,-34],[116,-34],[112,-32],[110,-30],[110,-26],[112,-22],[114,-22]],
    [[130,32],[132,34],[134,36],[136,38],[138,40],[140,42],[142,44],[140,44],[138,44],[136,42],[134,38],[132,36],[130,34],[130,32]],
    [[-6,50],[-4,50],[-2,52],[0,52],[2,52],[2,54],[0,56],[-2,58],[-4,58],[-6,56],[-6,54],[-4,52],[-6,50]],
    [[168,-44],[170,-44],[172,-42],[174,-40],[174,-38],[172,-36],[170,-36],[168,-38],[166,-42],[168,-44]],
    [[14,56],[16,58],[18,60],[20,62],[22,64],[24,68],[26,70],[28,70],[30,68],[28,66],[26,64],[24,62],[22,60],[20,58],[18,58],[16,56],[14,56]],
  ];

  // ── Utilities ──
  function project(lon, lat) {
    return [(lon + 180) / 360 * W, (90 - lat) / 180 * H];
  }

  function insidePoly(px, py, poly) {
    let inside = false;
    for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const [xi,yi] = poly[i], [xj,yj] = poly[j];
      if (((yi>py)!==(yj>py)) && (px < (xj-xi)*(py-yi)/(yj-yi)+xi)) inside = !inside;
    }
    return inside;
  }

  function lerp(a, b, t) { return a + (b - a) * t; }

  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

  function arcPoint(x1, y1, x2, y2, t) {
    const mx = (x1+x2)/2, my = (y1+y2)/2 - Math.abs(x2-x1)*0.28;
    return [lerp(lerp(x1,mx,t),lerp(mx,x2,t),t), lerp(lerp(y1,my,t),lerp(my,y2,t),t)];
  }

  function fmtDate(v) {
    if (!v) return '—';
    try { return new Date(v).toLocaleDateString('en-AU', {day:'numeric',month:'short',year:'numeric'}); } catch { return String(v); }
  }

  function ns(tag) { return document.createElementNS('http://www.w3.org/2000/svg', tag); }

  // ── Phase 1: calculated position — Phase 2: swap these for real API calls ──
  function getVesselPosition(vesselData) {
    // vesselData: { departed_at, eta_port, arrived_at }
    // Returns progress 0→1 along sea arc
    if (!vesselData.departed_at) return 0.02;
    if (vesselData.arrived_at) return 1;
    const dep = new Date(vesselData.departed_at).getTime();
    const eta = vesselData.eta_port ? new Date(vesselData.eta_port).getTime() : null;
    if (!eta || eta <= dep) return 0.5;
    const now = Date.now();
    return clamp((now - dep) / (eta - dep), 0.05, 0.95);
  }

  function getFlightPosition(flightData) {
    // Same structure, same logic for air
    return getVesselPosition(flightData);
  }

  // ── Determine stage from lane milestone dates ──
  function getLaneStage(lane, hasReceiving) {
    if (lane.customs_hold) return 'customs_hold';
    if (hasReceiving) return 'vas';
    if (lane.dest_customs_cleared_at) return 'last_mile';
    if (lane.arrived_at) return 'clearing';
    if (lane.departed_at) return 'transit';
    if (lane.packing_list_ready_at) return 'origin_port';
    return 'at_supplier';
  }

  // ── Build vessel groups from lanes + containers ──
  function buildVesselGroups(lanes, containers, plan, receiving, appliedByPO, binsByPO) {
    const groups = new Map(); // vesselKey → group object
    const noVesselLanes = [];

    // Build container → vessel lookup
    const contVessel = new Map();
    for (const c of containers) {
      const vessel = String(c.vessel||'').trim();
      const cid = String(c.container_id||c.container||'').trim();
      if (vessel && cid) contVessel.set(cid, vessel);
    }

    // Plan lookup: zendesk → [PO records]
    const planByZendesk = new Map();
    for (const p of plan) {
      const zd = String(p.zendesk_ticket||'').trim();
      if (!zd) continue;
      if (!planByZendesk.has(zd)) planByZendesk.set(zd, []);
      planByZendesk.get(zd).push(p);
    }

    // Receiving: set of received PO numbers
    const receivedPOs = new Set((receiving||[]).map(r => String(r.po_number||'').trim()).filter(Boolean));

    for (const lane of lanes) {
      const vessel = String(lane.manual?.vessel || lane.vessel || '').trim();
      const freight = String(lane.freight||'').trim().toLowerCase();
      const zendesk = String(lane.zendesk||'').trim();
      const isAir = freight === 'air';

      // Find POs for this lane via zendesk
      const lanePoRows = planByZendesk.get(zendesk) || [];
      const lanePos = [...new Set(lanePoRows.map(p => String(p.po_number||'').trim()).filter(Boolean))];
      const laneSkus = [...new Set(lanePoRows.map(p => String(p.sku_code||'').trim()).filter(Boolean))];

      // Planned / applied / bins for this lane
      let planned = 0, applied = 0;
      const laneBins = new Set();
      for (const po of lanePos) {
        planned += lanePoRows.filter(p=>String(p.po_number||'').trim()===po).reduce((s,p)=>s+Number(p.target_qty||0),0);
        applied += appliedByPO.get(po) || 0;
        for (const bin of (binsByPO.get(po)||new Set())) laneBins.add(bin);
      }

      // Receiving status for this lane's POs
      const hasReceiving = lanePos.some(po => receivedPOs.has(po));

      // Stage
      const manual = lane.manual || {};
      const stageData = {
        customs_hold: manual.customs_hold || false,
        packing_list_ready_at: manual.packing_list_ready_at || null,
        departed_at: manual.departed_at || null,
        arrived_at: manual.arrived_at || null,
        dest_customs_cleared_at: manual.dest_customs_cleared_at || null,
        eta_port: manual.eta_fc || manual.latest_arrival_date || null,
        eta_fc: manual.eta_fc || null,
        latest_arrival_date: manual.latest_arrival_date || null,
        hbl: manual.hbl || null,
        mbl: manual.mbl || null,
        shipment: manual.shipmentNumber || manual.shipment || null,
      };
      const stage = getLaneStage(stageData, hasReceiving);

      // Skip delivered
      if (stage === 'delivered') continue;

      const laneObj = {
        zendesk, freight: lane.freight, stage, stageData, isAir,
        pos: lanePos, skus: laneSkus, planned, applied,
        bins: Array.from(laneBins),
        supplier: lane.supplier || '',
      };

      if (!vessel) {
        noVesselLanes.push({ vessel: 'NO_VESSEL_' + zendesk, lanes: [laneObj], isAir, stage, stageData });
        continue;
      }

      const key = vessel;
      if (!groups.has(key)) {
        groups.set(key, {
          vessel, isAir,
          lanes: [],
          // Merge dates across lanes — earliest departure, latest ETA
          departed_at: null, eta_port: null, arrived_at: null,
          dest_customs_cleared_at: null,
          stage: 'at_supplier',
        });
      }
      const g = groups.get(key);
      g.lanes.push(laneObj);

      // Merge dates
      const dep = stageData.departed_at;
      const eta = stageData.eta_port;
      const arr = stageData.arrived_at;
      const clr = stageData.dest_customs_cleared_at;
      if (dep && (!g.departed_at || dep < g.departed_at)) g.departed_at = dep;
      if (eta && (!g.eta_port || eta > g.eta_port)) g.eta_port = eta;
      if (arr && (!g.arrived_at || arr > g.arrived_at)) g.arrived_at = arr;
      if (clr && (!g.dest_customs_cleared_at || clr > g.dest_customs_cleared_at)) g.dest_customs_cleared_at = clr;

      // Group stage = most advanced stage across lanes
      const STAGE_ORDER = ['at_supplier','origin_port','transit','clearing','customs_hold','vas','last_mile'];
      const stageIdx = s => STAGE_ORDER.indexOf(s);
      if (stageIdx(stage) > stageIdx(g.stage)) g.stage = stage;
    }

    // Combine
    const result = Array.from(groups.values());
    for (const nl of noVesselLanes) result.push(nl);
    return result;
  }

  // ── Get pin coordinates for a vessel group ──
  function getGroupPosition(g) {
    const stage = g.stage;
    const isAir = g.isAir;

    if (stage === 'at_supplier') return project(LOCATIONS.supplier.lon, LOCATIONS.supplier.lat);
    if (stage === 'origin_port') return project(LOCATIONS.origin_port.lon, LOCATIONS.origin_port.lat);
    if (stage === 'vas') return project(LOCATIONS.client_wh.lon, LOCATIONS.client_wh.lat);

    if (stage === 'transit') {
      const progress = isAir
        ? getFlightPosition({ departed_at: g.departed_at, eta_port: g.eta_port, arrived_at: g.arrived_at })
        : getVesselPosition({ departed_at: g.departed_at, eta_port: g.eta_port, arrived_at: g.arrived_at });
      const [ox, oy] = project(LOCATIONS.origin_port.lon, LOCATIONS.origin_port.lat);
      const dest = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dx, dy] = project(dest.lon, dest.lat);
      return arcPoint(ox, oy, dx, dy, progress);
    }

    if (stage === 'clearing' || stage === 'customs_hold') {
      const dest = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      return project(dest.lon, dest.lat);
    }

    if (stage === 'last_mile') {
      const dep = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dx, dy] = project(dep.lon, dep.lat);
      const [wx, wy] = project(LOCATIONS.client_wh.lon, LOCATIONS.client_wh.lat);
      const prog = g.dest_customs_cleared_at ? clamp((Date.now() - new Date(g.dest_customs_cleared_at).getTime()) / (2*24*60*60*1000), 0.1, 0.9) : 0.5;
      return arcPoint(dx, dy, wx, wy, prog);
    }

    return project(LOCATIONS.supplier.lon, LOCATIONS.supplier.lat);
  }

  // ── Inject skeleton into page ──
  function injectSkeleton(host) {
    host.innerHTML = `
<div style="padding:0;height:calc(100vh - 100px);min-height:500px;position:relative;display:flex;flex-direction:column;gap:8px;">
  <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:10px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="font-size:15px;font-weight:500;color:#1C1C1E;letter-spacing:-0.01em;">Live Map</div>
      <div style="width:0.5px;height:20px;background:rgba(0,0,0,0.08);"></div>
      <input id="map-search" type="text" placeholder="Search vessel, PO, SKU, Zendesk, mobile bin..." style="border:0.5px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 12px;font-size:12px;width:300px;outline:none;font-family:inherit;color:#1C1C1E;background:#fff;"/>
    </div>
    <div style="display:flex;align-items:center;gap:16px;">
      <div id="map-counter" style="font-size:11px;color:#6E6E73;"></div>
      <div style="display:flex;gap:12px;align-items:center;" id="map-legend"></div>
    </div>
  </div>
  <div style="flex:1;position:relative;border-radius:14px;overflow:hidden;border:0.5px solid rgba(0,0,0,0.08);">
    <canvas id="map-canvas" style="display:block;width:100%;height:100%;"></canvas>
    <svg id="map-pins" style="position:absolute;top:0;left:0;width:100%;height:100%;overflow:visible;"></svg>
    <div id="map-tooltip" style="position:absolute;background:#1C1C1E;color:#fff;border-radius:6px;padding:5px 10px;font-size:11px;pointer-events:none;display:none;white-space:nowrap;z-index:10;"></div>
    <div id="map-detail" style="position:absolute;top:0;right:0;width:300px;height:100%;background:#fff;border-left:0.5px solid rgba(0,0,0,0.08);transform:translateX(100%);transition:transform .3s cubic-bezier(0.4,0,0.2,1);z-index:20;overflow-y:auto;padding:20px 18px 20px;"></div>
  </div>
</div>`;
  }

  // ── Draw dot matrix base map ──
  function drawBaseMap(canvas) {
    const ctx = canvas.getContext('2d');
    const w = canvas.width, h = canvas.height;
    ctx.fillStyle = '#FAFAFA';
    ctx.fillRect(0, 0, w, h);
    const scaleX = w / W, scaleY = h / H;
    const step = 7;
    for (let px = 0; px < W; px += step) {
      for (let py = 0; py < H; py += step) {
        let inside = false;
        for (const poly of LAND) {
          if (insidePoly(px, py, poly.map(([lo,la])=>project(lo,la)))) { inside=true; break; }
        }
        if (inside) {
          ctx.beginPath();
          ctx.arc(px*scaleX, py*scaleY, 1.5*Math.min(scaleX,scaleY), 0, Math.PI*2);
          ctx.fillStyle = '#D8D8D8';
          ctx.fill();
        }
      }
    }
  }

  // ── Render fixed location labels ──
  function renderLocationLabels(svgEl, cw, ch) {
    const scaleX = cw / W, scaleY = ch / H;
    const locs = Object.values(LOCATIONS);
    for (const loc of locs) {
      const [x, y] = project(loc.lon, loc.lat);
      const sx = x * scaleX, sy = y * scaleY;
      const dot = ns('circle');
      dot.setAttribute('cx', sx); dot.setAttribute('cy', sy);
      dot.setAttribute('r', '3'); dot.setAttribute('fill', '#C8C8C8');
      dot.setAttribute('stroke', '#fff'); dot.setAttribute('stroke-width', '1');
      svgEl.appendChild(dot);
      const label = ns('text');
      label.setAttribute('x', sx + 6); label.setAttribute('y', sy + 3);
      label.setAttribute('font-size', '9'); label.setAttribute('fill', '#AEAEB2');
      label.setAttribute('font-family', '-apple-system,sans-serif');
      label.textContent = loc.name;
      svgEl.appendChild(label);
    }
  }

  // ── Render legend ──
  function renderLegend() {
    const el = document.getElementById('map-legend');
    if (!el) return;
    el.innerHTML = Object.entries(STAGE_LABEL).map(([k, v]) =>
      `<div style="display:flex;align-items:center;gap:5px;">
        <div style="width:8px;height:8px;border-radius:50%;background:${STAGE_COLOR[k]};flex-shrink:0;"></div>
        <span style="font-size:10px;color:#6E6E73;">${v}</span>
      </div>`
    ).join('');
  }

  // ── Detail panel ──
  function openDetail(g, panel) {
    const color = STAGE_COLOR[g.stage];
    const allPos = [...new Set(g.lanes.flatMap(l=>l.pos))];
    const allSkus = [...new Set(g.lanes.flatMap(l=>l.skus))];
    const allBins = [...new Set(g.lanes.flatMap(l=>l.bins))];
    const allZendesks = [...new Set(g.lanes.map(l=>l.zendesk).filter(Boolean))];
    const totalPlanned = g.lanes.reduce((s,l)=>s+l.planned,0);
    const totalApplied = g.lanes.reduce((s,l)=>s+l.applied,0);
    const pct = totalPlanned > 0 ? Math.round(totalApplied/totalPlanned*100) : 0;
    const hbls = [...new Set(g.lanes.flatMap(l=>l.stageData?.hbl ? [l.stageData.hbl] : []))];
    const mbls = [...new Set(g.lanes.flatMap(l=>l.stageData?.mbl ? [l.stageData.mbl] : []))];
    const etaPort = g.lanes[0]?.stageData?.eta_port;
    const etaFC = g.lanes[0]?.stageData?.eta_fc;
    const latestArr = g.lanes[0]?.stageData?.latest_arrival_date;

    const row = (label, value) => value && value !== '—'
      ? `<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:8px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
          <span style="font-size:11px;color:#AEAEB2;flex-shrink:0;">${label}</span>
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;text-align:right;max-width:160px;">${value}</span>
        </div>` : '';

    const section = (title, rows) => `
      <div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin:14px 0 6px;">${title}</div>
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;">${rows}</div>`;

    panel.innerHTML = `
      <button onclick="document.getElementById('map-detail').style.transform='translateX(100%)'" style="position:absolute;top:14px;right:14px;width:26px;height:26px;border-radius:7px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:13px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>
      <div style="padding-right:32px;margin-bottom:14px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:6px;">${g.vessel && !g.vessel.startsWith('NO_VESSEL') ? g.vessel : 'Unassigned vessel'}</div>
        <div style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:6px;background:${color}18;">
          <div style="width:6px;height:6px;border-radius:50%;background:${color};"></div>
          <span style="font-size:10px;font-weight:500;color:${color};">${STAGE_LABEL[g.stage]}</span>
        </div>
      </div>
      ${section('Shipment', [
        row('Vessel', g.vessel && !g.vessel.startsWith('NO_VESSEL') ? g.vessel : null),
        row('Freight', g.isAir ? 'Air' : 'Sea'),
        row('Zendesk(s)', allZendesks.map(z=>'#'+z).join(', ')),
        row('HBL', hbls.join(', ')),
        row('MBL', mbls.join(', ')),
      ].join(''))}
      ${section('POs & SKUs', [
        row('POs', allPos.join(', ')),
        row('SKUs', allSkus.join(', ')),
      ].join(''))}
      ${section('Units', [
        row('Planned', totalPlanned.toLocaleString()),
        row('Applied', totalApplied.toLocaleString()),
        row('Completion', pct + '%'),
      ].join(''))}
      ${allBins.length ? section('Mobile Bins', [
        row('Count', allBins.length.toLocaleString()),
        row('Bins', allBins.slice(0,8).join(', ') + (allBins.length > 8 ? ` +${allBins.length-8} more` : '')),
      ].join('')) : ''}
      ${section('Dates', [
        row('ETA Port', fmtDate(etaPort)),
        row('ETA FC / WH', fmtDate(etaFC || latestArr)),
        row('Departed', fmtDate(g.departed_at)),
        row('Arrived', fmtDate(g.arrived_at)),
        row('Customs cleared', fmtDate(g.dest_customs_cleared_at)),
        g.lanes.some(l=>l.stageData?.customs_hold) ? row('Customs Hold', 'YES — contact ops') : '',
      ].join(''))}`;

    panel.style.transform = 'translateX(0)';
  }

  // ── Main render function ──
  function renderMap(groups, filter) {
    const svgEl = document.getElementById('map-pins');
    const canvas = document.getElementById('map-canvas');
    const tooltip = document.getElementById('map-tooltip');
    const detail = document.getElementById('map-detail');
    const wrap = svgEl.parentElement;
    if (!svgEl || !canvas) return;

    const cw = canvas.offsetWidth || W;
    const ch = canvas.offsetHeight || H;
    canvas.width = cw; canvas.height = ch;
    const scaleX = cw / W, scaleY = ch / H;

    drawBaseMap(canvas);
    svgEl.innerHTML = '';
    svgEl.setAttribute('viewBox', `0 0 ${cw} ${ch}`);

    renderLocationLabels(svgEl, cw, ch);

    const q = (filter||'').toLowerCase().trim();
    const matched = q ? groups.filter(g => {
      const allPos = g.lanes.flatMap(l=>l.pos);
      const allSkus = g.lanes.flatMap(l=>l.skus);
      const allZd = g.lanes.map(l=>l.zendesk);
      const allBins = g.lanes.flatMap(l=>l.bins);
      const allSup = g.lanes.map(l=>l.supplier);
      return (
        g.vessel?.toLowerCase().includes(q) ||
        allPos.some(p=>p.toLowerCase().includes(q)) ||
        allSkus.some(s=>s.toLowerCase().includes(q)) ||
        allZd.some(z=>z.includes(q)) ||
        allBins.some(b=>b.toLowerCase().includes(q)) ||
        allSup.some(s=>s.toLowerCase().includes(q))
      );
    }) : groups;

    const matchedIds = new Set(matched.map(g=>g.vessel));
    const isFiltering = q.length > 0;

    // Draw arcs first (behind pins)
    for (const g of groups) {
      const isMatch = matchedIds.has(g.vessel);
      const opacity = isFiltering ? (isMatch ? 0.7 : 0.06) : 0.45;
      const color = STAGE_COLOR[g.stage];
      if (g.stage !== 'transit' && g.stage !== 'last_mile') continue;

      let x1, y1, x2, y2;
      if (g.stage === 'transit') {
        [x1, y1] = project(LOCATIONS.origin_port.lon, LOCATIONS.origin_port.lat);
        const dest = g.isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
        [x2, y2] = project(dest.lon, dest.lat);
      } else {
        const dep = g.isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
        [x1, y1] = project(dep.lon, dep.lat);
        [x2, y2] = project(LOCATIONS.client_wh.lon, LOCATIONS.client_wh.lat);
      }
      const mx = (x1+x2)/2*scaleX, my = ((y1+y2)/2 - Math.abs(x2-x1)*0.28)*scaleY;
      const pathD = `M${x1*scaleX},${y1*scaleY} Q${mx},${my} ${x2*scaleX},${y2*scaleY}`;
      const bg = ns('path'); bg.setAttribute('d',pathD); bg.setAttribute('stroke',color);
      bg.setAttribute('stroke-width','1.5'); bg.setAttribute('fill','none');
      bg.setAttribute('stroke-opacity', isFiltering?(isMatch?'0.3':'0.04'):'0.18');
      svgEl.appendChild(bg);
      const fg = ns('path'); fg.setAttribute('d',pathD); fg.setAttribute('stroke',color);
      fg.setAttribute('stroke-width','1.5'); fg.setAttribute('fill','none');
      fg.setAttribute('stroke-dasharray','5 4');
      fg.setAttribute('stroke-opacity', isFiltering?(isMatch?'0.8':'0.06'):'0.5');
      svgEl.appendChild(fg);
    }

    // Draw pins — matched on top
    const sorted = [...groups].sort((a,b) => {
      const am = matchedIds.has(a.vessel), bm = matchedIds.has(b.vessel);
      return am === bm ? 0 : am ? 1 : -1;
    });

    let activeCount = 0;
    for (const g of sorted) {
      const isMatch = matchedIds.has(g.vessel);
      const pinOpacity = isFiltering ? (isMatch ? 1 : 0.1) : 1;
      const pinScale = isFiltering && isMatch ? 1.8 : 1;
      const color = STAGE_COLOR[g.stage];
      const [rawX, rawY] = getGroupPosition(g);
      const px = rawX * scaleX, py = rawY * scaleY;
      activeCount++;

      const gEl = ns('g');
      gEl.setAttribute('opacity', pinOpacity);
      gEl.style.cursor = 'pointer';
      gEl.style.pointerEvents = 'all';

      // Sonar rings
      for (let ri = 0; ri < 3; ri++) {
        const ring = ns('circle');
        ring.setAttribute('cx', px); ring.setAttribute('cy', py); ring.setAttribute('r', '4');
        ring.setAttribute('fill', 'none'); ring.setAttribute('stroke', color);
        ring.style.animation = `mapSonar 2.4s ease-out infinite ${ri*0.8}s`;
        gEl.appendChild(ring);
      }

      // Outer glow
      const outer = ns('circle');
      outer.setAttribute('cx', px); outer.setAttribute('cy', py);
      outer.setAttribute('r', String(7*pinScale)); outer.setAttribute('fill', color);
      outer.setAttribute('opacity', '0.15');
      gEl.appendChild(outer);

      // Core dot
      const dot = ns('circle');
      dot.setAttribute('cx', px); dot.setAttribute('cy', py);
      dot.setAttribute('r', String(4.5*pinScale)); dot.setAttribute('fill', color);
      gEl.appendChild(dot);

      // Customs hold X mark
      if (g.stage === 'customs_hold') {
        const sz = 3*pinScale;
        const x = ns('path');
        x.setAttribute('d', `M${px-sz},${py-sz} L${px+sz},${py+sz} M${px+sz},${py-sz} L${px-sz},${py+sz}`);
        x.setAttribute('stroke', '#fff'); x.setAttribute('stroke-width', '1.5');
        x.setAttribute('stroke-linecap', 'round');
        gEl.appendChild(x);
      }

      const vesselLabel = g.vessel && !g.vessel.startsWith('NO_VESSEL') ? g.vessel : 'Unassigned';
      gEl.addEventListener('mouseenter', e => {
        tooltip.style.display = 'block';
        tooltip.textContent = vesselLabel + ' · ' + STAGE_LABEL[g.stage];
      });
      gEl.addEventListener('mousemove', e => {
        const r = wrap.getBoundingClientRect();
        tooltip.style.left = (e.clientX - r.left + 14) + 'px';
        tooltip.style.top = (e.clientY - r.top - 32) + 'px';
      });
      gEl.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
      gEl.addEventListener('click', () => { openDetail(g, detail); });

      svgEl.appendChild(gEl);
    }

    const counter = document.getElementById('map-counter');
    if (counter) counter.textContent = activeCount + ' active shipment' + (activeCount !== 1 ? 's' : '');
  }

  // ── CSS for sonar animation ──
  function injectStyles() {
    const s = document.createElement('style');
    s.textContent = `@keyframes mapSonar{0%{r:4;stroke-opacity:.65;stroke-width:2}100%{r:20;stroke-opacity:0;stroke-width:.5}}`;
    document.head.appendChild(s);
  }

  // ── Fetch all data needed for the map ──
  async function loadMapData() {
    const apiBase = (document.querySelector('meta[name="api-base"]')?.content||'').replace(/\/+$/,'');
    let token = null;
    if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch(_) {} }
    const headers = Object.assign({}, token ? {'Authorization':'Bearer '+token} : {});
    const api = async (path) => {
      const r = await fetch(apiBase + path, { headers });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    };

    // Get last 8 weeks of plan data to catch slow sea freight
    const planRows = Array.isArray(window.state?.plan) ? window.state.plan : [];
    const weekStart = window.state?.weekStart || '';

    // Fetch flow data for last 8 weeks
    const weeksToFetch = [];
    for (let i = 0; i < 8; i++) {
      const d = new Date(weekStart + 'T00:00:00');
      d.setDate(d.getDate() - i*7);
      weeksToFetch.push(d.toISOString().slice(0,10));
    }

    const flowResults = await Promise.allSettled(
      weeksToFetch.map(ws => api(`/flow/week/${encodeURIComponent(ws)}/all`))
    );

    // Merge all flow data
    const allLanes = [];
    const allContainers = [];
    for (const result of flowResults) {
      if (result.status !== 'fulfilled') continue;
      const d = result.value;
      const flowData = {};
      for (const facVal of Object.values(d.facilities || {})) {
        const fd = (facVal?.data && typeof facVal.data === 'object') ? facVal.data : {};
        for (const [k, v] of Object.entries(fd)) {
          if (k === 'intl_lanes' && v && typeof v === 'object' && !Array.isArray(v)) {
            flowData.intl_lanes = Object.assign({}, flowData.intl_lanes||{}, v);
          } else { flowData[k] = v; }
        }
      }
      const intl = (flowData.intl_lanes && typeof flowData.intl_lanes === 'object') ? flowData.intl_lanes : {};
      for (const [lk, manual] of Object.entries(intl)) {
        const parts = lk.split('||');
        allLanes.push({ key: lk, supplier: parts[0]||'', zendesk: parts[1]||'', freight: parts[2]||'', manual: manual||{} });
      }
      const wc = flowData.intl_weekcontainers;
      const conts = Array.isArray(wc) ? wc : (Array.isArray(wc?.containers) ? wc.containers : []);
      allContainers.push(...conts);
    }

    // Fetch receiving + bins for current week
    const [receivingData, binsData, summaryData] = await Promise.allSettled([
      api(`/receiving?weekStart=${encodeURIComponent(weekStart)}`),
      api(`/bins/weeks/${encodeURIComponent(weekStart)}`),
      api(`/summary/po_sku?from=${encodeURIComponent(weekStart)}&to=${encodeURIComponent(weekStart)}&status=complete`),
    ]);

    const receiving = receivingData.status === 'fulfilled' ? (Array.isArray(receivingData.value) ? receivingData.value : []) : [];
    const bins = binsData.status === 'fulfilled' ? (Array.isArray(binsData.value) ? binsData.value : []) : [];
    const summary = summaryData.status === 'fulfilled' ? (summaryData.value?.rows || []) : [];

    // Build applied by PO
    const appliedByPO = new Map();
    for (const r of summary) appliedByPO.set(String(r.po||'').trim(), Number(r.units||0));

    // Build bins by PO
    const binsByPO = new Map();
    for (const b of bins) {
      const po = String(b.po_number||'').trim();
      if (!po) continue;
      if (!binsByPO.has(po)) binsByPO.set(po, new Set());
      binsByPO.get(po).add(String(b.mobile_bin||'').trim());
    }

    return { lanes: allLanes, containers: allContainers, plan: planRows, receiving, appliedByPO, binsByPO };
  }

  // ── Main entry point ──
  async function initMap(host) {
    injectSkeleton(host);
    injectStyles();
    renderLegend();

    const canvas = document.getElementById('map-canvas');
    const svgEl = document.getElementById('map-pins');
    if (!canvas) return;

    // Draw base map immediately while data loads
    canvas.width = canvas.offsetWidth || W;
    canvas.height = canvas.offsetHeight || H;
    drawBaseMap(canvas);

    let groups = [];

    try {
      const { lanes, containers, plan, receiving, appliedByPO, binsByPO } = await loadMapData();
      groups = buildVesselGroups(lanes, containers, plan, receiving, appliedByPO, binsByPO);
      renderMap(groups, '');
    } catch(e) {
      console.error('[Map] load failed', e);
      renderMap([], '');
    }

    // Wire search
    const searchInput = document.getElementById('map-search');
    if (searchInput) {
      searchInput.addEventListener('input', function() {
        renderMap(groups, this.value);
      });
    }

    // Re-render on resize
    window.addEventListener('resize', () => { renderMap(groups, searchInput?.value||''); });
  }

  // ── Show function exposed to nav routing ──
  window.showMapPage = function() {
    let pg = document.getElementById('page-map');
    if (!pg) {
      pg = document.createElement('section');
      pg.id = 'page-map';
      pg.style.padding = '16px';
      const main = document.querySelector('main.vo-wrap') || document.querySelector('main');
      if (main) main.appendChild(pg);
      initMap(pg);
    }
    pg.classList.remove('hidden');
    pg.style.display = 'block';
  };

  window.hideMapPage = function() {
    const pg = document.getElementById('page-map');
    if (pg) { pg.classList.add('hidden'); pg.style.display = 'none'; }
  };

})();
