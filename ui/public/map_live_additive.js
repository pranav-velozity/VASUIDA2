/* map_live_additive.js v2 — VelOzity Pinpoint Live Map
   Uses D3 + Natural Earth GeoJSON for proper world map
   Phase 1: calculated vessel position | Phase 2 hook: getVesselPosition() API swap
*/
(function () {
  'use strict';

  // ── Locations — spread apart for visual clarity ──
  // Shenzhen cluster spread north-south, Sydney cluster spread apart
  const LOCATIONS = {
    supplier:       { name: 'Supplier (Shenzhen)',  lat: 23.50,  lon: 116.00 },
    vas_facility:   { name: 'VAS Facility',         lat: 22.55,  lon: 114.10 },
    origin_port:    { name: 'Shenzhen Port',        lat: 21.50,  lon: 111.50 },
    sydney_port:    { name: 'Port Botany',          lat: -34.00, lon: 151.22 },
    sydney_airport: { name: 'Sydney Airport',       lat: -33.94, lon: 151.17 },
    client_wh:      { name: 'Sydney WH (Client)',   lat: -31.80, lon: 148.50 },
  };

  // ── Strong distinct colors per stage ──
  const STAGE_COLOR = {
    at_supplier:  '#888780',   // neutral grey — not yet moving
    vas:          '#990033',   // brand red — your VAS facility
    origin_port:  '#3B82F6',   // blue — Shenzhen port
    transit:      '#0EA5E9',   // sky blue — at sea / in air
    clearing:     '#3B82F6',   // blue — destination port
    customs_hold: '#DC2626',   // red alert — blocked
    last_mile:    '#1C1C1E',   // black — final delivery
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

  const STAGE_ORDER = ['at_supplier','origin_port','transit','clearing','customs_hold','vas','last_mile'];

  // ── Utilities ──
  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
  function lerp(a, b, t) { return a + (b-a)*t; }
  function ns(tag) { return document.createElementNS('http://www.w3.org/2000/svg', tag); }

  function fmtDate(v) {
    if (!v) return '—';
    try { return new Date(v).toLocaleDateString('en-AU', {day:'numeric',month:'short',year:'numeric'}); }
    catch { return String(v); }
  }

  // ── Phase 1 position — Phase 2: replace body with API call ──
  function getVesselPosition(departed_at, eta_port, arrived_at) {
    if (arrived_at) return 1;
    if (!departed_at) return 0.02;
    const dep = new Date(departed_at).getTime();
    const eta = eta_port ? new Date(eta_port).getTime() : 0;
    if (!eta || eta <= dep) return 0.5;
    return clamp((Date.now() - dep) / (eta - dep), 0.05, 0.95);
  }

  // ── Stage derivation from lane milestone dates ──
  function getLaneStage(m, hasReceiving) {
    if (!m) return 'at_supplier';
    if (m.customs_hold) return 'customs_hold';
    if (hasReceiving) return 'vas';
    if (m.dest_customs_cleared_at) return 'last_mile';
    if (m.arrived_at) return 'clearing';
    if (m.departed_at) return 'transit';
    if (m.packing_list_ready_at) return 'origin_port';
    return 'at_supplier';
  }

  // ── Build vessel groups ──
  function buildVesselGroups(lanes, containers, plan, receiving, appliedByPO, binsByPO) {
    const planByZendesk = new Map();
    for (const p of plan) {
      const zd = String(p.zendesk_ticket||'').trim();
      if (!zd) continue;
      if (!planByZendesk.has(zd)) planByZendesk.set(zd, []);
      planByZendesk.get(zd).push(p);
    }
    const receivedPOs = new Set((receiving||[]).map(r=>String(r.po_number||'').trim()).filter(Boolean));

    // Container → vessel lookup
    const contVessel = new Map();
    for (const c of containers) {
      const v = String(c.vessel||'').trim();
      const cid = String(c.container_id||c.container||'').trim();
      if (v && cid) contVessel.set(cid, v);
    }

    const groups = new Map();

    for (const lane of lanes) {
      const m = lane.manual || {};
      // Try vessel from manual data, then from containers matching this lane
      let vessel = String(m.vessel||m.shipmentNumber||'').trim();
      if (!vessel) {
        // Look for a container whose lane_keys include this lane
        for (const c of containers) {
          const lks = Array.isArray(c.lane_keys) ? c.lane_keys : [];
          if (lks.includes(lane.key)) {
            vessel = String(c.vessel||'').trim();
            if (vessel) break;
          }
        }
      }

      const freight = String(lane.freight||'').trim().toLowerCase();
      const zendesk = String(lane.zendesk||'').trim();
      const isAir = freight === 'air';

      const lanePoRows = planByZendesk.get(zendesk) || [];
      const lanePos = [...new Set(lanePoRows.map(p=>String(p.po_number||'').trim()).filter(Boolean))];
      const laneSkus = [...new Set(lanePoRows.map(p=>String(p.sku_code||'').trim()).filter(Boolean))];
      let planned = 0, applied = 0;
      const laneBins = new Set();
      for (const po of lanePos) {
        planned += lanePoRows.filter(p=>String(p.po_number||'').trim()===po).reduce((s,p)=>s+Number(p.target_qty||0),0);
        applied += appliedByPO.get(po) || 0;
        for (const b of (binsByPO.get(po)||new Set())) laneBins.add(b);
      }
      const hasReceiving = lanePos.some(po=>receivedPOs.has(po));
      const stage = getLaneStage(m, hasReceiving);
      if (stage === 'delivered') continue;

      const key = vessel || ('NO_VESSEL_' + zendesk);

      if (!groups.has(key)) {
        groups.set(key, {
          vessel: vessel || '',
          isAir,
          lanes: [],
          departed_at: null,
          eta_port: null,
          arrived_at: null,
          dest_customs_cleared_at: null,
          stage: 'at_supplier',
        });
      }
      const g = groups.get(key);

      g.lanes.push({
        zendesk, freight: lane.freight, stage, manual: m, isAir,
        pos: lanePos, skus: laneSkus, planned, applied,
        bins: Array.from(laneBins),
        supplier: lane.supplier||'',
      });

      // Merge dates — earliest departure, latest ETA
      const dep = m.departed_at, eta = m.eta_fc||m.latest_arrival_date;
      const arr = m.arrived_at, clr = m.dest_customs_cleared_at;
      if (dep && (!g.departed_at || dep < g.departed_at)) g.departed_at = dep;
      if (eta && (!g.eta_port || eta > g.eta_port)) g.eta_port = eta;
      if (arr && (!g.arrived_at || arr > g.arrived_at)) g.arrived_at = arr;
      if (clr && (!g.dest_customs_cleared_at || clr > g.dest_customs_cleared_at)) g.dest_customs_cleared_at = clr;

      // Most advanced stage wins
      if (STAGE_ORDER.indexOf(stage) > STAGE_ORDER.indexOf(g.stage)) g.stage = stage;
    }

    return Array.from(groups.values());
  }

  // ── Project lon/lat using D3 projection (set after map draws) ──
  // Fallback to equirectangular if D3 not ready yet
  let _proj = null;
  function project(lon, lat) {
    if (_proj) return _proj(lon, lat);
    // Equirectangular fallback (same as D3 default but may not match dots exactly)
    return [(lon+180)/360*900, (90-lat)/180*480];
  }

  // ── Arc midpoint (curved upward for sea routes) ──
  function arcMid(x1, y1, x2, y2, bend) {
    const dx = x2-x1, dy = y2-y1;
    const dist = Math.sqrt(dx*dx+dy*dy);
    // Perpendicular offset — rotated 90° from the direction of travel
    const nx = -dy/dist, ny = dx/dist;
    const mx = (x1+x2)/2 + nx*dist*(bend||0.28);
    const my = (y1+y2)/2 + ny*dist*(bend||0.28);
    return [mx, my];
  }

  function arcPoint(x1, y1, x2, y2, t, bend) {
    const [mx, my] = arcMid(x1, y1, x2, y2, bend);
    const ax = lerp(x1,mx,t), ay = lerp(y1,my,t);
    const bx = lerp(mx,x2,t), by = lerp(my,y2,t);
    return [lerp(ax,bx,t), lerp(ay,by,t)];
  }

  function getGroupXY(g) {
    const stage = g.stage;
    const isAir = g.isAir;
    const P = (loc) => project(loc.lon, loc.lat);

    if (stage === 'at_supplier') return P(LOCATIONS.supplier);
    if (stage === 'vas')         return P(LOCATIONS.vas_facility);
    if (stage === 'origin_port') return P(LOCATIONS.origin_port);
    if (stage === 'clearing' || stage === 'customs_hold') {
      return P(isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port);
    }
    if (stage === 'transit') {
      const progress = getVesselPosition(g.departed_at, g.eta_port, g.arrived_at);
      const [ox, oy] = P(LOCATIONS.origin_port);
      const dest = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dx, dy] = P(dest);
      return arcPoint(ox, oy, dx, dy, progress, 0.22);
    }
    if (stage === 'last_mile') {
      const dep = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dpx, dpy] = P(dep);
      const [wx, wy] = P(LOCATIONS.client_wh);
      const prog = g.dest_customs_cleared_at
        ? clamp((Date.now()-new Date(g.dest_customs_cleared_at).getTime())/(2*24*60*60*1000),0.1,0.9)
        : 0.5;
      return arcPoint(dpx, dpy, wx, wy, prog, 0.15);
    }
    return P(LOCATIONS.supplier);
  }

  // ── Detail panel ──
  function openDetail(g, panel) {
    const color = STAGE_COLOR[g.stage];
    const allPos = [...new Set(g.lanes.flatMap(l=>l.pos))];
    const allSkus = [...new Set(g.lanes.flatMap(l=>l.skus))];
    const allBins = [...new Set(g.lanes.flatMap(l=>l.bins))];
    const allZd = [...new Set(g.lanes.map(l=>l.zendesk).filter(Boolean))];
    const totalPlanned = g.lanes.reduce((s,l)=>s+l.planned,0);
    const totalApplied = g.lanes.reduce((s,l)=>s+l.applied,0);
    const pct = totalPlanned > 0 ? Math.round(totalApplied/totalPlanned*100) : 0;
    const hbls = [...new Set(g.lanes.map(l=>l.manual?.hbl).filter(Boolean))];
    const mbls = [...new Set(g.lanes.map(l=>l.manual?.mbl).filter(Boolean))];
    const eta_port = g.eta_port;
    const eta_fc = g.lanes[0]?.manual?.eta_fc;
    const lat_arr = g.lanes[0]?.manual?.latest_arrival_date;
    const onHold = g.lanes.some(l=>l.manual?.customs_hold);

    const row = (label, value) => (!value || value==='—') ? '' :
      `<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:8px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <span style="font-size:11px;color:#AEAEB2;flex-shrink:0;margin-right:8px;">${label}</span>
        <span style="font-size:11px;font-weight:500;color:#1C1C1E;text-align:right;">${value}</span>
      </div>`;

    const section = (title, content) => content.trim()
      ? `<div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin:14px 0 6px;">${title}</div>
         <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;">${content}</div>` : '';

    const vesselName = g.vessel || 'Unassigned vessel';

    panel.innerHTML = `
      <button onclick="this.closest('#map-detail').style.transform='translateX(100%)'"
        style="position:absolute;top:14px;right:14px;width:26px;height:26px;border-radius:7px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:13px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>
      <div style="padding-right:32px;margin-bottom:14px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:6px;">${vesselName}</div>
        <div style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:6px;background:${color}18;">
          <div style="width:6px;height:6px;border-radius:50%;background:${color};"></div>
          <span style="font-size:10px;font-weight:500;color:${color};">${STAGE_LABEL[g.stage]}</span>
        </div>
        ${onHold ? `<div style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:6px;background:#B0707018;margin-left:6px;">
          <span style="font-size:10px;font-weight:500;color:#B07070;">Customs Hold</span></div>` : ''}
      </div>
      ${section('Shipment', [
        row('Freight', g.isAir ? 'Air' : 'Sea'),
        row('Zendesk(s)', allZd.map(z=>'#'+z).join(', ')),
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
        row('Bins', allBins.slice(0,6).join(', ')+(allBins.length>6?` +${allBins.length-6} more`:'')),
      ].join('')) : ''}
      ${section('Dates & ETA', [
        row('Departed', fmtDate(g.departed_at)),
        row('ETA Port', fmtDate(eta_port)),
        row('ETA FC / WH', fmtDate(eta_fc||lat_arr)),
        row('Arrived', fmtDate(g.arrived_at)),
        row('Customs Cleared', fmtDate(g.dest_customs_cleared_at)),
      ].join(''))}`;

    panel.style.transform = 'translateX(0)';
  }

  // ── Render all pins and arcs onto the SVG ──
  function renderPins(svgEl, groups, filter) {
    while (svgEl.firstChild) svgEl.removeChild(svgEl.firstChild);

    const q = (filter||'').toLowerCase().trim();
    const matched = q ? new Set(groups.filter(g => {
      const allPos = g.lanes.flatMap(l=>l.pos);
      const allSkus = g.lanes.flatMap(l=>l.skus);
      const allZd = g.lanes.map(l=>l.zendesk);
      const allBins = g.lanes.flatMap(l=>l.bins);
      const allSup = g.lanes.map(l=>l.supplier);
      return g.vessel?.toLowerCase().includes(q) ||
        allPos.some(p=>p.toLowerCase().includes(q)) ||
        allSkus.some(s=>s.toLowerCase().includes(q)) ||
        allZd.some(z=>z.includes(q)) ||
        allBins.some(b=>b.toLowerCase().includes(q)) ||
        allSup.some(s=>s.toLowerCase().includes(q));
    }).map(g=>g.vessel)) : null;

    const isFiltering = q.length > 0;

    // Fixed location pins — each with its own distinct color
    const LOC_STYLE = {
      supplier:       { color: '#888780', r: 5, labelColor: '#6E6E73' },
      vas_facility:   { color: '#990033', r: 6, labelColor: '#990033' },
      origin_port:    { color: '#3B82F6', r: 5, labelColor: '#3B82F6' },
      sydney_port:    { color: '#3B82F6', r: 5, labelColor: '#3B82F6' },
      sydney_airport: { color: '#6B8FA8', r: 4, labelColor: '#6B8FA8' },
      client_wh:      { color: '#1C1C1E', r: 6, labelColor: '#1C1C1E' },
    };
    for (const [key, loc] of Object.entries(LOCATIONS)) {
      const [lx, ly] = project(loc.lon, loc.lat);
      const style = LOC_STYLE[key] || { color: '#CACACA', r: 4, labelColor: '#AEAEB2' };
      const outer = ns('circle');
      outer.setAttribute('cx',lx); outer.setAttribute('cy',ly);
      outer.setAttribute('r', String(style.r+4)); outer.setAttribute('fill', style.color);
      outer.setAttribute('opacity','0.12');
      svgEl.appendChild(outer);
      const dot = ns('circle');
      dot.setAttribute('cx',lx); dot.setAttribute('cy',ly);
      dot.setAttribute('r', String(style.r)); dot.setAttribute('fill', style.color);
      dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','1.5');
      svgEl.appendChild(dot);
      const txt = ns('text');
      txt.setAttribute('x', lx+style.r+5); txt.setAttribute('y', ly+4);
      txt.setAttribute('font-size','10'); txt.setAttribute('fill', style.labelColor);
      txt.setAttribute('font-family','-apple-system,sans-serif');
      txt.setAttribute('font-weight','500');
      txt.textContent = loc.name;
      svgEl.appendChild(txt);
    }

    // Draw one shared ghost route line per freight type (sea / air) — not one per vessel
    const drawnRoutes = new Set();
    groups.forEach((g) => {
      const routeKey = g.isAir ? 'air' : 'sea';
      if (drawnRoutes.has(routeKey)) return;
      drawnRoutes.add(routeKey);
      const [ox,oy] = project(LOCATIONS.origin_port.lon, LOCATIONS.origin_port.lat);
      const destPort = g.isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dx,dy] = project(destPort.lon, destPort.lat);
      const [mx,my] = arcMid(ox,oy,dx,dy,0.22);
      const ghostPath = ns('path');
      ghostPath.setAttribute('d',`M${ox},${oy} Q${mx},${my} ${dx},${dy}`);
      ghostPath.setAttribute('stroke','#C8C8C8');
      ghostPath.setAttribute('stroke-width','1');
      ghostPath.setAttribute('fill','none');
      ghostPath.setAttribute('stroke-opacity','0.4');
      svgEl.appendChild(ghostPath);
    });

    // Draw animated progress arc per vessel — offset perpendicular to route
    const seaGroups = groups.filter(g=>!g.isAir);
    const airGroups = groups.filter(g=>g.isAir);

    groups.forEach((g, idx) => {
      const isMatch = matched ? matched.has(g.vessel) : true;
      const color = STAGE_COLOR[g.stage];
      const isAir = g.isAir;
      const [ox,oy] = project(LOCATIONS.origin_port.lon, LOCATIONS.origin_port.lat);
      const destPort = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
      const [dx,dy] = project(destPort.lon, destPort.lat);

      // Perpendicular offset: vessels in same freight type spread out
      const sameType = isAir ? airGroups : seaGroups;
      const typeIdx = sameType.indexOf(g);
      const typeCount = sameType.length;
      const offsetFactor = typeCount > 1 ? (typeIdx - (typeCount-1)/2) * 0.12 : 0;
      const bend = 0.22 + offsetFactor;

      if (g.stage === 'transit') {
        const progress = getVesselPosition(g.departed_at, g.eta_port, g.arrived_at);
        const [vx,vy] = arcPoint(ox,oy,dx,dy,progress,bend);
        const [pmx,pmy] = arcMid(ox,oy,vx,vy,bend);
        const animId = 'mf' + idx;
        const styleEl = document.createElement('style');
        styleEl.textContent = `@keyframes ${animId}{from{stroke-dashoffset:14}to{stroke-dashoffset:0}}`;
        document.head.appendChild(styleEl);
        const fgPath = ns('path');
        fgPath.setAttribute('d',`M${ox},${oy} Q${pmx},${pmy} ${vx},${vy}`);
        fgPath.setAttribute('stroke',color);
        fgPath.setAttribute('stroke-width','2.5');
        fgPath.setAttribute('fill','none');
        fgPath.setAttribute('stroke-dasharray','8 6');
        fgPath.setAttribute('stroke-linecap','round');
        fgPath.setAttribute('stroke-opacity', isFiltering?(isMatch?'0.9':'0.05'):'0.7');
        fgPath.style.animation = `${animId} 1.4s linear infinite`;
        svgEl.appendChild(fgPath);
      }

      if (g.stage === 'last_mile') {
        const dep = isAir ? LOCATIONS.sydney_airport : LOCATIONS.sydney_port;
        const [lpx,lpy] = project(dep.lon, dep.lat);
        const [wx,wy] = project(LOCATIONS.client_wh.lon, LOCATIONS.client_wh.lat);
        const [lmx,lmy] = arcMid(lpx,lpy,wx,wy,0.15+offsetFactor*0.5);
        const lmId = 'ml' + idx;
        const lmStyle = document.createElement('style');
        lmStyle.textContent = `@keyframes ${lmId}{from{stroke-dashoffset:14}to{stroke-dashoffset:0}}`;
        document.head.appendChild(lmStyle);
        const lmPath = ns('path');
        lmPath.setAttribute('d',`M${lpx},${lpy} Q${lmx},${lmy} ${wx},${wy}`);
        lmPath.setAttribute('stroke',color);
        lmPath.setAttribute('stroke-width','2');
        lmPath.setAttribute('fill','none');
        lmPath.setAttribute('stroke-dasharray','6 5');
        lmPath.setAttribute('stroke-linecap','round');
        lmPath.setAttribute('stroke-opacity', isFiltering?(isMatch?'0.8':'0.04'):'0.6');
        lmPath.style.animation = `${lmId} 1s linear infinite`;
        svgEl.appendChild(lmPath);
      }

      // VAS — short arc from supplier to VAS facility (both in Shenzhen)
      if (g.stage === 'vas') {
        const [sx,sy] = project(LOCATIONS.supplier.lon, LOCATIONS.supplier.lat);
        const [vfx,vfy] = project(LOCATIONS.vas_facility.lon, LOCATIONS.vas_facility.lat);
        const [vmx,vmy] = arcMid(sx,sy,vfx,vfy,0.2);
        const vasPath = ns('path');
        vasPath.setAttribute('d',`M${sx},${sy} Q${vmx},${vmy} ${vfx},${vfy}`);
        vasPath.setAttribute('stroke',color);
        vasPath.setAttribute('stroke-width','1.5');
        vasPath.setAttribute('fill','none');
        vasPath.setAttribute('stroke-dasharray','4 4');
        vasPath.setAttribute('stroke-opacity', isFiltering?(isMatch?'0.5':'0.04'):'0.4');
        svgEl.appendChild(vasPath);
      }
    });

    // Pins — dimmed first, matched on top
    const sorted = [...groups].sort((a,b)=>{
      const am=matched?matched.has(a.vessel):true, bm=matched?matched.has(b.vessel):true;
      return (am===bm)?0:(am?1:-1);
    });

    const detail = document.getElementById('map-detail');
    const tooltip = document.getElementById('map-tooltip');
    const wrap = svgEl.parentElement;

    for (const g of sorted) {
      const isMatch = matched?matched.has(g.vessel):true;
      const pinOpacity = isFiltering?(isMatch?1:0.08):1;
      const pinScale = isFiltering&&isMatch?1.7:1;
      const color = STAGE_COLOR[g.stage];
      const [px,py] = getGroupXY(g);

      const gEl = ns('g');
      gEl.setAttribute('opacity',pinOpacity);
      gEl.style.cursor = 'pointer';
      gEl.style.pointerEvents = 'all';

      for (let ri=0; ri<3; ri++) {
        const ring = ns('circle');
        ring.setAttribute('cx',px); ring.setAttribute('cy',py); ring.setAttribute('r','4');
        ring.setAttribute('fill','none'); ring.setAttribute('stroke',color);
        ring.style.animation = `mapSonar 2.4s ease-out infinite ${ri*0.8}s`;
        gEl.appendChild(ring);
      }
      const outer = ns('circle');
      outer.setAttribute('cx',px); outer.setAttribute('cy',py);
      outer.setAttribute('r',String(7*pinScale)); outer.setAttribute('fill',color);
      outer.setAttribute('opacity','0.15'); gEl.appendChild(outer);
      const dot = ns('circle');
      dot.setAttribute('cx',px); dot.setAttribute('cy',py);
      dot.setAttribute('r',String(4.5*pinScale)); dot.setAttribute('fill',color);
      gEl.appendChild(dot);

      if (g.stage==='customs_hold') {
        const sz=2.5*pinScale, xPath=ns('path');
        xPath.setAttribute('d',`M${px-sz},${py-sz}L${px+sz},${py+sz}M${px+sz},${py-sz}L${px-sz},${py+sz}`);
        xPath.setAttribute('stroke','#fff'); xPath.setAttribute('stroke-width','1.5');
        xPath.setAttribute('stroke-linecap','round'); gEl.appendChild(xPath);
      }

      const label = g.vessel||'Unassigned';
      gEl.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent=label+' · '+STAGE_LABEL[g.stage]; });
      gEl.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
      gEl.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
      gEl.addEventListener('click',()=>{ openDetail(g,detail); });
      svgEl.appendChild(gEl);
    }

    const counter = document.getElementById('map-counter');
    if (counter) counter.textContent=groups.length+' active shipment'+(groups.length!==1?'s':'');
    console.log('[Map] rendered',groups.length,'groups:',groups.map(g=>(g.vessel||'NO_VES')+'='+g.stage).join(', '));
  }

  // ── Draw world map using D3 + TopoJSON ──  }

  // ── Draw world map using D3 + TopoJSON ──
  async function drawWorldMap(canvas) {
    const w = canvas.offsetWidth || 900;
    const h = canvas.offsetHeight || 480;
    canvas.width = w; canvas.height = h;

    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#FAFAFA';
    ctx.fillRect(0, 0, w, h);

    // Asia-Pacific focused projection — covers China to Australia
    // Centered on ~130°E, 0° lat with enough zoom to see both Shenzhen and Sydney clearly
    const projection = d3.geoMercator()
      .center([130, -10])
      .scale(w * 0.55)
      .translate([w * 0.42, h * 0.52]);

    const topo = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r=>r.json());
    const features = topojson.feature(topo, topo.objects.countries).features;

    const DOT_STEP = 6, DOT_R = 1.4;
    ctx.fillStyle = '#D4D4D4';

    const offscreen = document.createElement('canvas');
    offscreen.width = w; offscreen.height = h;
    const octx = offscreen.getContext('2d');

    for (const feature of features) {
      octx.clearRect(0, 0, w, h);
      octx.beginPath();
      d3.geoPath(projection, octx)(feature);
      octx.fillStyle = '#000';
      octx.fill();
      const imageData = octx.getImageData(0, 0, w, h).data;
      for (let px = DOT_STEP/2; px < w; px += DOT_STEP) {
        for (let py = DOT_STEP/2; py < h; py += DOT_STEP) {
          const idx = (Math.floor(py)*w + Math.floor(px))*4;
          if (imageData[idx+3] > 128) {
            ctx.beginPath();
            ctx.arc(px, py, DOT_R, 0, Math.PI*2);
            ctx.fill();
          }
        }
      }
    }

    // Return projection so pins use the same coordinate system as the map
    return { w, h, project: (lon, lat) => projection([lon, lat]) };
  }

  // ── Inject page skeleton ──
  function injectSkeleton(host) {
    host.innerHTML = `
<div style="padding:0;height:calc(100vh - 100px);min-height:520px;display:flex;flex-direction:column;gap:8px;">
  <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:10px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="font-size:15px;font-weight:500;color:#1C1C1E;letter-spacing:-0.01em;">Live Map</div>
      <div style="width:0.5px;height:20px;background:rgba(0,0,0,0.08);"></div>
      <input id="map-search" type="text" placeholder="Search vessel, PO, SKU, Zendesk, mobile bin..." style="border:0.5px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 12px;font-size:12px;width:310px;outline:none;font-family:inherit;color:#1C1C1E;background:#fff;"/>
    </div>
    <div style="display:flex;align-items:center;gap:16px;">
      <div id="map-counter" style="font-size:11px;color:#6E6E73;"></div>
      <div id="map-legend" style="display:flex;gap:12px;flex-wrap:wrap;"></div>
    </div>
  </div>
  <div id="map-wrap" style="flex:1;position:relative;border-radius:14px;overflow:hidden;border:0.5px solid rgba(0,0,0,0.08);background:#FAFAFA;">
    <div id="map-loading" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:12px;color:#AEAEB2;z-index:5;">Loading map…</div>
    <canvas id="map-canvas" style="display:block;width:100%;height:100%;"></canvas>
    <svg id="map-pins" style="position:absolute;top:0;left:0;width:100%;height:100%;overflow:visible;"></svg>
    <div id="map-tooltip" style="position:absolute;background:#1C1C1E;color:#fff;border-radius:6px;padding:5px 10px;font-size:11px;pointer-events:none;display:none;white-space:nowrap;z-index:30;"></div>
    <div id="map-detail" style="position:absolute;top:0;right:0;width:300px;height:100%;background:#fff;border-left:0.5px solid rgba(0,0,0,0.08);transform:translateX(100%);transition:transform .3s cubic-bezier(0.4,0,0.2,1);z-index:20;overflow-y:auto;padding:20px 18px;"></div>
  </div>
</div>`;
  }

  function renderLegend() {
    const el = document.getElementById('map-legend');
    if (!el) return;
    el.innerHTML = Object.entries(STAGE_LABEL).map(([k,v]) =>
      `<div style="display:flex;align-items:center;gap:4px;">
        <div style="width:8px;height:8px;border-radius:50%;background:${STAGE_COLOR[k]};flex-shrink:0;"></div>
        <span style="font-size:10px;color:#6E6E73;">${v}</span>
      </div>`
    ).join('');
  }

  function injectStyles() {
    if (document.getElementById('map-sonar-style')) return;
    const s = document.createElement('style');
    s.id = 'map-sonar-style';
    s.textContent = `@keyframes mapSonar{0%{r:4;stroke-opacity:.6;stroke-width:2}100%{r:20;stroke-opacity:0;stroke-width:.5}}`;
    document.head.appendChild(s);
  }

  // ── Fetch operational data ──
  async function loadMapData() {
    const apiBase = (document.querySelector('meta[name="api-base"]')?.content||'').replace(/\/+$/,'');
    let token = null;
    if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch(_){} }
    const headers = Object.assign({}, token?{'Authorization':'Bearer '+token}:{});
    const api = async (path) => {
      const r = await fetch(apiBase+path, {headers});
      if (!r.ok) throw new Error('HTTP '+r.status);
      return r.json();
    };

    const planRows = Array.isArray(window.state?.plan) ? window.state.plan : [];
    const weekStart = window.state?.weekStart || '';

    // Fetch 8 weeks of flow data to catch slow sea freight
    const weeks = [];
    for (let i=0; i<8; i++) {
      const d = new Date(weekStart+'T00:00:00');
      d.setDate(d.getDate()-i*7);
      weeks.push(d.toISOString().slice(0,10));
    }

    const flowResults = await Promise.allSettled(
      weeks.map(ws=>api(`/flow/week/${encodeURIComponent(ws)}/all`))
    );

    const allLanes=[], allContainers=[];
    for (const res of flowResults) {
      if (res.status!=='fulfilled') continue;
      const flowData={};
      for (const facVal of Object.values(res.value.facilities||{})) {
        const fd=(facVal?.data&&typeof facVal.data==='object')?facVal.data:{};
        for (const [k,v] of Object.entries(fd)) {
          if (k==='intl_lanes'&&v&&typeof v==='object'&&!Array.isArray(v)) {
            flowData.intl_lanes=Object.assign({},flowData.intl_lanes||{},v);
          } else { flowData[k]=v; }
        }
      }
      const intl=(flowData.intl_lanes&&typeof flowData.intl_lanes==='object')?flowData.intl_lanes:{};
      for (const [lk,manual] of Object.entries(intl)) {
        const parts=lk.split('||');
        allLanes.push({key:lk,supplier:parts[0]||'',zendesk:parts[1]||'',freight:parts[2]||'',manual:manual||{}});
      }
      const wc=flowData.intl_weekcontainers;
      const conts=Array.isArray(wc)?wc:(Array.isArray(wc?.containers)?wc.containers:[]);
      allContainers.push(...conts);
    }

    const [recvRes, binsRes, summaryRes] = await Promise.allSettled([
      api(`/receiving?weekStart=${encodeURIComponent(weekStart)}`),
      api(`/bins/weeks/${encodeURIComponent(weekStart)}`),
      api(`/summary/po_sku?from=${encodeURIComponent(weekStart)}&to=${encodeURIComponent(weekStart)}&status=complete`),
    ]);

    const receiving = recvRes.status==='fulfilled'?(Array.isArray(recvRes.value)?recvRes.value:[]):[];
    const bins = binsRes.status==='fulfilled'?(Array.isArray(binsRes.value)?binsRes.value:[]):[];
    const summary = summaryRes.status==='fulfilled'?(summaryRes.value?.rows||[]):[];

    const appliedByPO=new Map();
    for (const r of summary) appliedByPO.set(String(r.po||'').trim(), Number(r.units||0));
    const binsByPO=new Map();
    for (const b of bins) {
      const po=String(b.po_number||'').trim();
      if (!po) continue;
      if (!binsByPO.has(po)) binsByPO.set(po,new Set());
      binsByPO.get(po).add(String(b.mobile_bin||'').trim());
    }

    return {lanes:allLanes, containers:allContainers, plan:planRows, receiving, appliedByPO, binsByPO};
  }

  // ── Main init ──
  async function initMap(host) {
    injectSkeleton(host);
    injectStyles();
    renderLegend();

    const canvas = document.getElementById('map-canvas');
    const svgEl = document.getElementById('map-pins');
    const loading = document.getElementById('map-loading');
    const searchInput = document.getElementById('map-search');
    const wrap = document.getElementById('map-wrap');

    let groups = [];

    // Load D3 + TopoJSON from CDN
    async function loadLibs() {
      if (!window.d3) {
        await new Promise((res,rej)=>{
          const s=document.createElement('script');
          s.src='https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js';
          s.onload=res; s.onerror=rej;
          document.head.appendChild(s);
        });
      }
      if (!window.topojson) {
        await new Promise((res,rej)=>{
          const s=document.createElement('script');
          s.src='https://cdn.jsdelivr.net/npm/topojson@3/dist/topojson.min.js';
          s.onload=res; s.onerror=rej;
          document.head.appendChild(s);
        });
      }
    }

    try {
      await loadLibs();
      const dims = await drawWorldMap(canvas);
      // Set module-level projection so all pins use same coordinate system as map dots
      _proj = dims.project;
      if (loading) loading.style.display='none';
    } catch(e) {
      console.error('[Map] world map draw failed', e);
      if (loading) loading.textContent = 'Map unavailable — check connection';
    }

    try {
      const data = await loadMapData();
      groups = buildVesselGroups(data.lanes, data.containers, data.plan, data.receiving, data.appliedByPO, data.binsByPO);
    } catch(e) {
      console.error('[Map] data load failed', e);
    }

    renderPins(svgEl, groups, '');

    if (searchInput) {
      searchInput.addEventListener('input', function() {
        renderPins(svgEl, groups, this.value);
      });
    }

    // Redraw on resize
    let resizeTimer;
    window.addEventListener('resize', ()=>{
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(async ()=>{
        try {
          const dims = await drawWorldMap(canvas);
          _proj = dims.project;
          renderPins(svgEl, groups, searchInput?.value||'');
        } catch(e){}
      }, 300);
    });
  }

  // ── Exposed show/hide ──
  window.showMapPage = function() {
    let pg = document.getElementById('page-map');
    if (!pg) {
      pg = document.createElement('section');
      pg.id = 'page-map';
      pg.style.cssText = 'padding:16px;display:block;';
      const main = document.querySelector('main.vo-wrap')||document.querySelector('main');
      if (main) main.appendChild(pg);
      initMap(pg);
    }
    pg.classList.remove('hidden');
    pg.style.display = 'block';
  };

  window.hideMapPage = function() {
    const pg = document.getElementById('page-map');
    if (pg) { pg.classList.add('hidden'); pg.style.display='none'; }
  };

})();
