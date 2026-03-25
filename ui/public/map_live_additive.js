/* map_live_additive.js v5 — VelOzity Pinpoint Live Map
   Fixed field names from source: pack/departed/arrived/destClr/hold/etaFC
   One arc per vessel. Clickable location pins. Sea arc goes east.
*/
(function () {
  'use strict';

  const LOCATIONS = {
    supplier:       { name: 'Supplier (Shenzhen)',  lat: 26.00, lon: 118.00 },
    vas_facility:   { name: 'VAS Facility',         lat: 23.50, lon: 114.50 },
    origin_port:    { name: 'Shenzhen Port',        lat: 20.50, lon: 110.00 },
    sydney_port:    { name: 'Port Botany',          lat: -34.20, lon: 153.50 },
    sydney_airport: { name: 'Sydney Airport',       lat: -31.50, lon: 153.00 },
    client_wh:      { name: 'Sydney WH (Client)',   lat: -29.00, lon: 147.50 },
  };

  const LOC_COLOR = {
    supplier:       '#888780',
    vas_facility:   '#990033',
    origin_port:    '#3B82F6',
    sydney_port:    '#3B82F6',
    sydney_airport: '#6B8FA8',
    client_wh:      '#1C1C1E',
  };

  const STAGE_COLOR = {
    at_supplier:  '#888780',
    vas:          '#990033',
    origin_port:  '#3B82F6',
    transit:      '#990033',
    clearing:     '#3B82F6',
    customs_hold: '#DC2626',
    last_mile:    '#CC4466',
  };

  const STAGE_LABEL = {
    at_supplier:  'At Supplier',
    vas:          'At Facility / VAS',
    origin_port:  'At Origin Port',
    transit:      'In Transit',
    clearing:     'At Port / Clearing',
    customs_hold: 'Customs Hold',
    last_mile:    'Last Mile',
  };

  // ── Utilities ──
  function clamp(v,lo,hi){return Math.max(lo,Math.min(hi,v));}
  function lerp(a,b,t){return a+(b-a)*t;}
  function ns(tag){return document.createElementNS('http://www.w3.org/2000/svg',tag);}
  function fmtDate(v){
    if(!v||v==='undefined'||v==='null') return '—';
    try{ return new Date(v).toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'}); }
    catch{ return String(v); }
  }

  let _proj = null;
  function project(lon,lat){
    if(_proj) return _proj(lon,lat);
    return [(lon+180)/360*900,(90-lat)/180*480];
  }

  // ── Manual field accessors — handles both short (pack) and long (packing_list_ready_at) names ──
  function mGet(m, ...keys){
    for(const k of keys){ if(m[k]!==undefined && m[k]!==null && m[k]!=='') return m[k]; }
    return null;
  }
  function getPackingReady(m){ return mGet(m,'pack','packing_list_ready_at','packingListReadyAt','packing_list_ready'); }
  function getDeparted(m){     return mGet(m,'departed','departed_at','departedAt'); }
  function getArrived(m){      return mGet(m,'arrived','arrived_at','arrivedAt'); }
  function getDestClr(m){      return mGet(m,'destClr','dest_customs_cleared_at','destClearedAt'); }
  function getCustomsHold(m){  return !!(m.hold || m.customs_hold); }
  function getEtaFC(m){        return mGet(m,'etaFC','eta_fc','etaFc'); }
  function getLatestArrival(m){ return mGet(m,'latestArrivalDate','latest_arrival_date','latestArrival'); }
  function getHbl(m){          return mGet(m,'hbl','HBL','hblNumber')||''; }
  function getMbl(m){          return mGet(m,'mbl','MBL','mblNumber')||''; }
  function getVesselFromManual(m){ return String(m.vessel||'').trim(); }

  // ── Phase 1 position — Phase 2: replace body with API call ──
  function getVesselPosition(departed, etaPort, arrived){
    if(arrived) return 1;
    if(!departed) return 0.02;
    const dep=new Date(departed).getTime();
    const eta=etaPort?new Date(etaPort).getTime():0;
    if(!eta||eta<=dep) return 0.5;
    return clamp((Date.now()-dep)/(eta-dep),0.05,0.95);
  }

  // ── Arc geometry — perpendicular bend ──
  function arcMid(x1,y1,x2,y2,bend){
    const dx=x2-x1, dy=y2-y1;
    const dist=Math.sqrt(dx*dx+dy*dy)||1;
    const nx=-dy/dist, ny=dx/dist;
    return [(x1+x2)/2+nx*dist*(bend||0.22),(y1+y2)/2+ny*dist*(bend||0.22)];
  }
  function arcPoint(x1,y1,x2,y2,t,bend){
    const [mx,my]=arcMid(x1,y1,x2,y2,bend);
    const ax=lerp(x1,mx,t), ay=lerp(y1,my,t);
    const bx=lerp(mx,x2,t), by=lerp(my,y2,t);
    return [lerp(ax,bx,t),lerp(ay,by,t)];
  }

  // ── Sea arc control point — curves EAST to avoid landmass ──
  // Goes via ~155°E (Philippine Sea / Coral Sea) not over Indonesia
  function seaArcMid(x1,y1,x2,y2){
    // Fixed eastern waypoint: ~10°N, 155°E (open ocean east of Philippines)
    const [wx,wy]=project(155,10);
    // Use the waypoint as a quadratic control by averaging
    const cmx=(x1+wx*2+x2)/4;
    const cmy=(y1+wy*2+y2)/4;
    return [cmx*2-x1/2-x2/2, cmy*2-y1/2-y2/2];
  }
  function seaArcPoint(x1,y1,x2,y2,t){
    const [mx,my]=seaArcMid(x1,y1,x2,y2);
    const ax=lerp(x1,mx,t), ay=lerp(y1,my,t);
    const bx=lerp(mx,x2,t), by=lerp(my,y2,t);
    return [lerp(ax,bx,t),lerp(ay,by,t)];
  }

  // ── Stage derivation — uses correct short field names ──
  function getLaneStage(m, hasReceiving){
    if(!m) return 'at_supplier';
    if(getCustomsHold(m)) return 'customs_hold';
    if(getDestClr(m)) return 'last_mile';
    if(getArrived(m)) return 'clearing';
    if(getDeparted(m)) return 'transit';
    if(hasReceiving) return 'vas';
    if(getPackingReady(m)) return 'origin_port';
    return 'at_supplier';
  }

  // ── Build all map data ──
  function buildMapData(lanes, containers, plan, receiving, appliedByPO){

    // ── Diagnostics — visible in DevTools console ──
    console.log('[Map:build] inputs — plan:',plan.length,'receiving:',receiving.length,'appliedByPO:',appliedByPO.size,'lanes:',lanes.length);
    console.log('[Map:build] plan sample:',plan.slice(0,3).map(p=>({zd:p.zendesk_ticket,po:p.po_number,qty:p.target_qty,freq:p.freight_type})));
    console.log('[Map:build] appliedByPO sample:',Array.from(appliedByPO.entries()).slice(0,4));
    console.log('[Map:build] receiving sample:',receiving.slice(0,3).map(r=>({po:r.po_number,wk:r.week_start})));
    console.log('[Map:build] lane sample:',lanes.slice(0,3).map(l=>({zd:l.zendesk,sup:l.supplier,freight:l.freight,manual_keys:Object.keys(l.manual||{})})));

    // Plan lookup by zendesk_ticket — coerce both to string, handle numeric values
    const planByZendesk = new Map();
    const zdByPO = new Map(); // PO → zendesk (reverse map)
    for(const p of plan){
      const zd = String(p.zendesk_ticket ?? p.zendesk ?? '').trim();
      const po = String(p.po_number ?? '').trim().toUpperCase();
      if(!zd || zd === 'undefined') continue;
      if(!planByZendesk.has(zd)) planByZendesk.set(zd, []);
      planByZendesk.get(zd).push(p);
      if(po) zdByPO.set(po, zd);
    }
    console.log('[Map:build] planByZendesk keys:',Array.from(planByZendesk.keys()).slice(0,8));

    // Received POs — uppercase for consistent comparison
    const receivedPOs = new Set((receiving||[]).map(r => String(r.po_number||'').trim().toUpperCase()).filter(Boolean));
    console.log('[Map:build] receivedPOs:',Array.from(receivedPOs).slice(0,5));

    // Applied units by zendesk — two approaches to maximise match rate
    const appliedByZendesk = new Map();
    // Approach 1: via planByZendesk → POs → appliedByPO
    for(const [zd, pRows] of planByZendesk){
      const pos = [...new Set(pRows.map(p => String(p.po_number||'').trim().toUpperCase()).filter(Boolean))];
      let total = 0;
      for(const po of pos) total += appliedByPO.get(po) || 0;
      if(total > 0) appliedByZendesk.set(zd, total);
    }
    // Approach 2: reverse map — appliedByPO → zdByPO (catches any missed by approach 1)
    for(const [po, units] of appliedByPO){
      if(!units) continue;
      const zd = zdByPO.get(po.toUpperCase()) || zdByPO.get(po);
      if(zd) appliedByZendesk.set(zd, (appliedByZendesk.get(zd)||0) + units);
    }
    console.log('[Map:build] appliedByZendesk non-zero:',Array.from(appliedByZendesk.entries()).filter(([,v])=>v>0).slice(0,6));

    // Container → vessel lookup
    const contVessel = new Map();
    for(const c of containers){
      const v = String(c.vessel||'').trim();
      const cid = String(c.container_id||c.container||'').trim();
      if(v && cid) contVessel.set(cid, v);
    }

    // Vessel groups (in transit — have departed date)
    const vesselGroups = new Map();

    // Location groups (static pins)
    const locationGroups = {
      at_supplier: [],
      vas: [],
      origin_port: [],
      clearing: [],
      customs_hold: [],
      last_mile: [],
    };

    for(const lane of lanes){
      const m = lane.manual || {};
      const zendesk = String(lane.zendesk||'').trim();
      const freight = String(lane.freight||'').trim().toLowerCase();
      const isAir = freight === 'air';

      // Lookup plan rows for this zendesk — try exact match then numeric coercion
      const lanePoRows = planByZendesk.get(zendesk) ||
                         planByZendesk.get(String(Number(zendesk))) || [];
      const lanePos = [...new Set(lanePoRows.map(p => String(p.po_number||'').trim().toUpperCase()).filter(Boolean))];
      const hasReceiving = lanePos.some(po => receivedPOs.has(po));
      const stage = getLaneStage(m, hasReceiving);
      if(stage === 'delivered') continue;

      const applied = appliedByZendesk.get(zendesk) || 0;
      const hbl = getHbl(m);
      const mbl = getMbl(m);
      const etaPort = getEtaFC(m) || getLatestArrival(m);

      const zdEntry = {zendesk, applied, hbl, mbl, supplier: lane.supplier||'', freight: lane.freight||'', stage, manual: m, isAir, etaPort};

      // Static location
      if(stage !== 'transit'){
        if(locationGroups[stage]) locationGroups[stage].push(zdEntry);
        continue;
      }

      // Vessel group
      let vessel = getVesselFromManual(m);
      if(!vessel){
        for(const c of containers){
          const lks = Array.isArray(c.lane_keys) ? c.lane_keys : [];
          if(lks.includes(lane.key)){ vessel = String(c.vessel||'').trim(); if(vessel) break; }
        }
      }
      const key = vessel || ('NO_VESSEL_' + zendesk);

      if(!vesselGroups.has(key)){
        vesselGroups.set(key, {vessel, isAir, zdentrys: [],
          departed: null, etaPort: null, arrived: null, destClr: null});
      }
      const vg = vesselGroups.get(key);
      vg.zdentrys.push(zdEntry);

      const dep = getDeparted(m), eta = getEtaFC(m) || getLatestArrival(m);
      const arr = getArrived(m), clr = getDestClr(m);
      if(dep && (!vg.departed || dep < vg.departed)) vg.departed = dep;
      if(eta && (!vg.etaPort || eta > vg.etaPort)) vg.etaPort = eta;
      if(arr && (!vg.arrived || arr > vg.arrived)) vg.arrived = arr;
      if(clr && (!vg.destClr || clr > vg.destClr)) vg.destClr = clr;
    }

    console.log('[Map:build] result — vesselGroups:',vesselGroups.size,'locationGroups:',Object.entries(locationGroups).map(([k,v])=>k+':'+v.length).join(' '));

    return {
      vesselGroups: Array.from(vesselGroups.values()),
      locationGroups,
      appliedByZendesk,
    };
  }

  // ── Detail panel helpers ──
  function dRow(label,value){
    if(!value||value==='—') return '';
    return `<div style="display:flex;justify-content:space-between;padding:8px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
      <span style="font-size:11px;color:#AEAEB2;">${label}</span>
      <span style="font-size:11px;font-weight:500;color:#1C1C1E;text-align:right;max-width:180px;">${value}</span>
    </div>`;
  }
  function sectionHeader(title){
    return `<div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin:14px 0 6px;">${title}</div>`;
  }
  function closeBtn(){
    return `<button onclick="document.getElementById('map-detail').style.transform='translateX(100%)'"
      style="position:absolute;top:14px;right:14px;width:26px;height:26px;border-radius:7px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:13px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>`;
  }

  // ── Vessel detail panel ──
  function openVesselDetail(vg,panel){
    const color='#990033';
    const totalApplied=vg.zdentrys.reduce((s,z)=>s+z.applied,0);
    const allHBLs=[...new Set(vg.zdentrys.map(z=>z.hbl).filter(Boolean))];
    const name=vg.vessel||'Unassigned vessel';
    const zdRows=vg.zdentrys.map(z=>`
      <div style="display:flex;justify-content:space-between;align-items:center;padding:9px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <div>
          <div style="font-size:11px;font-weight:500;color:#1C1C1E;">#${z.zendesk}</div>
          ${z.supplier?`<div style="font-size:10px;color:#AEAEB2;">${z.supplier}</div>`:''}
        </div>
        <div style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} applied</div>
      </div>`).join('');
    panel.innerHTML=`${closeBtn()}
      <div style="padding-right:32px;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:6px;">${name}</div>
        <div style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:6px;background:${color}18;">
          <div style="width:6px;height:6px;border-radius:50%;background:${color};"></div>
          <span style="font-size:10px;font-weight:500;color:${color};">In Transit · ${vg.isAir?'Air':'Sea'}</span>
        </div>
      </div>
      ${sectionHeader('Shipment')}
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:4px;">
        ${dRow('Freight',vg.isAir?'Air':'Sea')}
        ${dRow('HBL',allHBLs.join(', ')||'—')}
        ${dRow('Departed',fmtDate(vg.departed))}
        ${dRow('ETA Port / FC',fmtDate(vg.etaPort))}
        ${vg.arrived?dRow('Arrived',fmtDate(vg.arrived)):''}
        ${vg.destClr?dRow('Customs Cleared',fmtDate(vg.destClr)):''}
      </div>
      ${sectionHeader('Zendesks on vessel')}
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:4px;">
        ${zdRows||'<div style="padding:12px;font-size:11px;color:#AEAEB2;">No Zendesks found</div>'}
        ${vg.zdentrys.length>0?`<div style="display:flex;justify-content:space-between;padding:8px 12px;background:#F5F5F7;">
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">Total applied</span>
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalApplied.toLocaleString()} units</span>
        </div>`:''}
      </div>`;
    panel.style.transform='translateX(0)';
  }

  // ── Location detail panel ──
  function openLocationDetail(locKey,entries,panel){
    const color=LOC_COLOR[locKey];
    const locName=LOCATIONS[locKey].name;
    const stageDesc={
      at_supplier:'Plan loaded — not yet received',
      vas:'In VAS processing',
      origin_port:'Packing list ready — awaiting departure',
      clearing:'Arrived — in customs',
      customs_hold:'Customs Hold — action required',
      last_mile:'Cleared — last mile delivery',
    }[locKey]||'';
    const rows=entries.map(z=>`
      <div style="display:flex;justify-content:space-between;align-items:center;padding:9px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <div>
          <div style="font-size:11px;font-weight:500;color:#1C1C1E;">#${z.zendesk}</div>
          ${z.supplier?`<div style="font-size:10px;color:#AEAEB2;">${z.supplier}</div>`:''}
        </div>
        <div style="text-align:right;">
          <div style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} applied</div>
          ${z.hbl?`<div style="font-size:10px;color:#AEAEB2;">HBL: ${z.hbl}</div>`:''}
        </div>
      </div>`).join('');
    const totalApplied=entries.reduce((s,z)=>s+z.applied,0);
    panel.innerHTML=`${closeBtn()}
      <div style="padding-right:32px;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:4px;">${locName}</div>
        <div style="font-size:11px;color:#6E6E73;">${stageDesc}</div>
      </div>
      ${sectionHeader(entries.length+' Zendesk'+(entries.length!==1?'s':''))}
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:4px;">
        ${rows||'<div style="padding:12px;font-size:11px;color:#AEAEB2;">Nothing here right now</div>'}
        ${entries.length>0?`<div style="display:flex;justify-content:space-between;padding:8px 12px;background:#F5F5F7;">
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">Total applied</span>
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalApplied.toLocaleString()} units</span>
        </div>`:''}
      </div>`;
    panel.style.transform='translateX(0)';
  }

  // ── Ship and airplane SVG icons ──
  function shipIcon(x,y,color){
    // Simple ship hull
    const g=ns('g');
    g.setAttribute('transform',`translate(${x-8},${y-6})`);
    const hull=ns('path');
    hull.setAttribute('d','M2,6 L4,2 L12,2 L14,6 Q8,10 2,6 Z');
    hull.setAttribute('fill',color); hull.setAttribute('opacity','0.9');
    const mast=ns('line');
    mast.setAttribute('x1','8'); mast.setAttribute('y1','2');
    mast.setAttribute('x2','8'); mast.setAttribute('y2','-2');
    mast.setAttribute('stroke',color); mast.setAttribute('stroke-width','1.5');
    g.appendChild(hull); g.appendChild(mast);
    return g;
  }
  function planeIcon(x,y,color){
    const g=ns('g');
    g.setAttribute('transform',`translate(${x},${y}) rotate(-45)`);
    const body=ns('path');
    body.setAttribute('d','M0,-7 L2,0 L7,2 L2,2 L2,7 L0,5 L-2,7 L-2,2 L-7,2 L-2,0 Z');
    body.setAttribute('fill',color); body.setAttribute('opacity','0.9');
    g.appendChild(body);
    return g;
  }

  // ── Render ──
  function renderMap(svgEl,vesselGroups,locationGroups,filter){
    while(svgEl.firstChild) svgEl.removeChild(svgEl.firstChild);
    const q=(filter||'').toLowerCase().trim();
    const detail=document.getElementById('map-detail');
    const tooltip=document.getElementById('map-tooltip');
    const wrap=svgEl.parentElement;

    // ── Location pins ──
    for(const [locKey,loc] of Object.entries(LOCATIONS)){
      const [lx,ly]=project(loc.lon,loc.lat);
      const color=LOC_COLOR[locKey];
      const entries=locationGroups[locKey]||[];
      const active=entries.length>0;

      const glow=ns('circle');
      glow.setAttribute('cx',lx); glow.setAttribute('cy',ly);
      glow.setAttribute('r','10'); glow.setAttribute('fill',color);
      glow.setAttribute('opacity',active?'0.12':'0.05');
      svgEl.appendChild(glow);

      if(active){
        for(let ri=0;ri<3;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',lx); ring.setAttribute('cy',ly); ring.setAttribute('r','6');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke',color);
          ring.setAttribute('stroke-width','1.5');
          ring.style.animation=`mapSonar 3.5s ease-out infinite ${ri*1.4}s`;
          svgEl.appendChild(ring);
        }
      }

      const dot=ns('circle');
      dot.setAttribute('cx',lx); dot.setAttribute('cy',ly);
      dot.setAttribute('r',active?'6':'4');
      dot.setAttribute('fill',color);
      dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','1.5');
      dot.setAttribute('opacity',active?'1':'0.4');
      svgEl.appendChild(dot);

      const txt=ns('text');
      txt.setAttribute('x',lx+9); txt.setAttribute('y',ly+4);
      txt.setAttribute('font-size','9.5'); txt.setAttribute('font-weight','500');
      txt.setAttribute('fill',active?color:'#BCBCBC');
      txt.setAttribute('font-family','-apple-system,sans-serif');
      txt.textContent=loc.name;
      svgEl.appendChild(txt);

      if(active){
        const bdg=ns('text');
        bdg.setAttribute('x',lx+9); bdg.setAttribute('y',ly+15);
        bdg.setAttribute('font-size','8'); bdg.setAttribute('font-weight','500');
        bdg.setAttribute('fill',color); bdg.setAttribute('opacity','0.8');
        bdg.setAttribute('font-family','-apple-system,sans-serif');
        bdg.textContent=entries.length+' ZD';
        svgEl.appendChild(bdg);
      }

      const hit=ns('circle');
      hit.setAttribute('cx',lx); hit.setAttribute('cy',ly);
      hit.setAttribute('r','18'); hit.setAttribute('fill','transparent');
      hit.style.cursor='pointer'; hit.style.pointerEvents='all';
      hit.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent=loc.name+(active?` — ${entries.length} Zendesk${entries.length!==1?'s':''}`:''); });
      hit.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
      hit.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
      hit.addEventListener('click',()=>{ openLocationDetail(locKey,entries,detail); });
      svgEl.appendChild(hit);
    }

    // ── Last mile arcs (from location groups) ──
    const lastMileEntries=locationGroups.last_mile||[];
    if(lastMileEntries.length>0){
      const seaLM=lastMileEntries.filter(z=>!z.isAir);
      const airLM=lastMileEntries.filter(z=>z.isAir);
      [[seaLM,'sydney_port'],[airLM,'sydney_airport']].forEach(([arr,depKey])=>{
        if(!arr.length) return;
        const [dpx,dpy]=project(LOCATIONS[depKey].lon,LOCATIONS[depKey].lat);
        const [wx,wy]=project(LOCATIONS.client_wh.lon,LOCATIONS.client_wh.lat);
        const [lmx,lmy]=arcMid(dpx,dpy,wx,wy,0.2);
        const lmArc=ns('path');
        lmArc.setAttribute('d',`M${dpx},${dpy} Q${lmx},${lmy} ${wx},${wy}`);
        lmArc.setAttribute('stroke','#CC4466');
        lmArc.setAttribute('stroke-width','2');
        lmArc.setAttribute('fill','none');
        lmArc.setAttribute('stroke-opacity','0.6');
        svgEl.appendChild(lmArc);
        // Arrow at midpoint
        const [mx2,my2]=arcPoint(dpx,dpy,wx,wy,0.55,0.2);
        const arrowG=ns('g');
        arrowG.setAttribute('transform',`translate(${mx2},${my2})`);
        const arrowPath=ns('path');
        arrowPath.setAttribute('d','M-4,-4 L0,0 L-4,4');
        arrowPath.setAttribute('stroke','#CC4466'); arrowPath.setAttribute('stroke-width','1.5');
        arrowPath.setAttribute('fill','none'); arrowPath.setAttribute('stroke-linecap','round');
        arrowG.appendChild(arrowPath);
        svgEl.appendChild(arrowG);
      });
    }

    // ── Vessel arcs ──
    const seaVessels=vesselGroups.filter(v=>!v.isAir);
    const airVessels=vesselGroups.filter(v=>v.isAir);

    vesselGroups.forEach((vg,idx)=>{
      const sameType=vg.isAir?airVessels:seaVessels;
      const typeIdx=sameType.indexOf(vg);
      const typeCount=sameType.length;
      const offsetFactor=typeCount>1?(typeIdx-(typeCount-1)/2)*1.0:0;

      const allZd=vg.zdentrys.map(z=>z.zendesk);
      const allHBL=vg.zdentrys.map(z=>z.hbl);
      const isMatch=!q||vg.vessel?.toLowerCase().includes(q)||allZd.some(z=>z.includes(q))||allHBL.some(h=>h?.toLowerCase().includes(q));
      const arcOpacity=q?(isMatch?1:0.05):1;
      if(arcOpacity<0.06) return;

      const [ox,oy]=project(LOCATIONS.origin_port.lon,LOCATIONS.origin_port.lat);
      const destLoc=vg.isAir?LOCATIONS.sydney_airport:LOCATIONS.sydney_port;
      const [dx,dy]=project(destLoc.lon,destLoc.lat);

      const progress=getVesselPosition(vg.departed,vg.etaPort,vg.arrived);

      if(vg.isAir){
        // Air: direct curved arc with slight perpendicular offset
        const bend=0.22+(offsetFactor*0.15);
        const [mx,my]=arcMid(ox,oy,dx,dy,bend);
        const arcD=`M${ox},${oy} Q${mx},${my} ${dx},${dy}`;

        // Faint full route
        const fullArc=ns('path');
        fullArc.setAttribute('d',arcD);
        fullArc.setAttribute('stroke','#990033'); fullArc.setAttribute('stroke-width','1');
        fullArc.setAttribute('fill','none');
        fullArc.setAttribute('stroke-opacity',String(arcOpacity*0.18));
        fullArc.setAttribute('stroke-dasharray','4 3');
        svgEl.appendChild(fullArc);

        // Travelled portion
        const [vx,vy]=arcPoint(ox,oy,dx,dy,progress,bend);
        const [tmx,tmy]=arcMid(ox,oy,vx,vy,bend);
        const travelArc=ns('path');
        travelArc.setAttribute('d',`M${ox},${oy} Q${tmx},${tmy} ${vx},${vy}`);
        travelArc.setAttribute('stroke','#990033'); travelArc.setAttribute('stroke-width','2');
        travelArc.setAttribute('fill','none');
        travelArc.setAttribute('stroke-opacity',String(arcOpacity*0.65));
        svgEl.appendChild(travelArc);

        // Plane icon at 30%
        const [ix,iy]=arcPoint(ox,oy,dx,dy,0.3,bend);
        svgEl.appendChild(planeIcon(ix,iy,'#990033'));

        // Vessel label tag above dot
        if(vg.vessel&&(!q||isMatch)){
          const tagW=Math.min(vg.vessel.length*5+14,160);
          const tagBg=ns('rect');
          tagBg.setAttribute('x',vx-tagW/2); tagBg.setAttribute('y',vy-22);
          tagBg.setAttribute('width',tagW); tagBg.setAttribute('height','13');
          tagBg.setAttribute('rx','3'); tagBg.setAttribute('fill','#fff');
          tagBg.setAttribute('stroke','#990033'); tagBg.setAttribute('stroke-width','0.5');
          tagBg.setAttribute('opacity','0.95');
          svgEl.appendChild(tagBg);
          const tagTxt=ns('text');
          tagTxt.setAttribute('x',vx); tagTxt.setAttribute('y',vy-12);
          tagTxt.setAttribute('text-anchor','middle');
          tagTxt.setAttribute('font-size','8'); tagTxt.setAttribute('font-weight','500');
          tagTxt.setAttribute('fill','#990033');
          tagTxt.setAttribute('font-family','-apple-system,sans-serif');
          tagTxt.textContent=vg.vessel.length>24?vg.vessel.slice(0,22)+'\u2026':vg.vessel;
          svgEl.appendChild(tagTxt);
        }

        // Pulse rings + dot at vessel tip
        for(let ri=0;ri<2;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',vx); ring.setAttribute('cy',vy); ring.setAttribute('r','5');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke','#990033');
          ring.setAttribute('stroke-width','1.5');
          ring.setAttribute('opacity',String(arcOpacity));
          ring.style.animation=`mapSonar 3.5s ease-out infinite ${ri*1.4}s`;
          svgEl.appendChild(ring);
        }
        const gEl=ns('g');
        gEl.setAttribute('opacity',String(arcOpacity));
        gEl.style.cursor='pointer'; gEl.style.pointerEvents='all';
        const dot=ns('circle');
        dot.setAttribute('cx',vx); dot.setAttribute('cy',vy);
        dot.setAttribute('r','5.5'); dot.setAttribute('fill','#990033');
        dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','2');
        gEl.appendChild(dot);
        const label=vg.vessel||'Unassigned vessel';
        gEl.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent=label+' · Air · In Transit'; });
        gEl.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
        gEl.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
        gEl.addEventListener('click',()=>{ openVesselDetail(vg,detail); });
        svgEl.appendChild(gEl);

      } else {
        // Sea: arc curves EAST around landmass
        // Apply offset by shifting the eastern waypoint slightly
        const wayptLon=160+(offsetFactor*18);
        const wayptLat=8;
        const [wx2,wy2]=project(wayptLon,wayptLat);
        // Cubic-like: use two quadratic segments via eastern waypoint
        const seaD=`M${ox},${oy} Q${wx2},${wy2} ${dx},${dy}`;

        // Faint full route
        const fullArc=ns('path');
        fullArc.setAttribute('d',seaD);
        fullArc.setAttribute('stroke','#990033'); fullArc.setAttribute('stroke-width','1');
        fullArc.setAttribute('fill','none');
        fullArc.setAttribute('stroke-opacity',String(arcOpacity*0.18));
        svgEl.appendChild(fullArc);

        // Travelled portion — approximate position on this path
        // t interpolation on the quadratic bezier
        const t=progress;
        const vx=lerp(lerp(ox,wx2,t),lerp(wx2,dx,t),t);
        const vy=lerp(lerp(oy,wy2,t),lerp(wy2,dy,t),t);

        // Draw from origin to vessel using same control point
        const travelD=`M${ox},${oy} Q${wx2},${wy2} ${vx},${vy}`;
        // For partial bezier we need a De Casteljau split
        // Split at t: new control = lerp(P0,P1,t), new end = lerp(lerp(P0,P1,t),lerp(P1,P2,t),t)
        const c1x=lerp(ox,wx2,t), c1y=lerp(oy,wy2,t);
        const splitD=`M${ox},${oy} Q${c1x},${c1y} ${vx},${vy}`;

        const travelArc=ns('path');
        travelArc.setAttribute('d',splitD);
        travelArc.setAttribute('stroke','#990033'); travelArc.setAttribute('stroke-width','2');
        travelArc.setAttribute('fill','none');
        travelArc.setAttribute('stroke-opacity',String(arcOpacity*0.65));
        svgEl.appendChild(travelArc);

        // Ship icon at ~30% along sea arc
        const t30=0.3;
        const ix=lerp(lerp(ox,wx2,t30),lerp(wx2,dx,t30),t30);
        const iy=lerp(lerp(oy,wy2,t30),lerp(wy2,dy,t30),t30);
        svgEl.appendChild(shipIcon(ix,iy,'#990033'));

        // Vessel label tag
        if(vg.vessel&&(!q||isMatch)){
          const tagW=Math.min(vg.vessel.length*5+14,160);
          const tagBg=ns('rect');
          tagBg.setAttribute('x',vx-tagW/2); tagBg.setAttribute('y',vy-22);
          tagBg.setAttribute('width',tagW); tagBg.setAttribute('height','13');
          tagBg.setAttribute('rx','3'); tagBg.setAttribute('fill','#fff');
          tagBg.setAttribute('stroke','#990033'); tagBg.setAttribute('stroke-width','0.5');
          tagBg.setAttribute('opacity','0.95');
          svgEl.appendChild(tagBg);
          const tagTxt=ns('text');
          tagTxt.setAttribute('x',vx); tagTxt.setAttribute('y',vy-12);
          tagTxt.setAttribute('text-anchor','middle');
          tagTxt.setAttribute('font-size','8'); tagTxt.setAttribute('font-weight','500');
          tagTxt.setAttribute('fill','#990033');
          tagTxt.setAttribute('font-family','-apple-system,sans-serif');
          tagTxt.textContent=vg.vessel.length>24?vg.vessel.slice(0,22)+'\u2026':vg.vessel;
          svgEl.appendChild(tagTxt);
        }

        // Pulse rings + clickable dot
        for(let ri=0;ri<2;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',vx); ring.setAttribute('cy',vy); ring.setAttribute('r','5');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke','#990033');
          ring.setAttribute('stroke-width','1.5');
          ring.setAttribute('opacity',String(arcOpacity));
          ring.style.animation=`mapSonar 3.5s ease-out infinite ${ri*1.4}s`;
          svgEl.appendChild(ring);
        }
        const gEl=ns('g');
        gEl.setAttribute('opacity',String(arcOpacity));
        gEl.style.cursor='pointer'; gEl.style.pointerEvents='all';
        const dot=ns('circle');
        dot.setAttribute('cx',vx); dot.setAttribute('cy',vy);
        dot.setAttribute('r','5.5'); dot.setAttribute('fill','#990033');
        dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','2');
        gEl.appendChild(dot);
        const label=vg.vessel||'Unassigned vessel';
        gEl.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent=label+' · Sea · In Transit'; });
        gEl.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
        gEl.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
        gEl.addEventListener('click',()=>{ openVesselDetail(vg,detail); });
        svgEl.appendChild(gEl);
      }
    });

    const counter=document.getElementById('map-counter');
    if(counter) counter.textContent=vesselGroups.length+' vessel'+(vesselGroups.length!==1?'s':'')+' in transit';
    console.log('[Map] vessels:',vesselGroups.length,'| locations:',Object.entries(locationGroups).map(([k,v])=>k+':'+v.length).join(' '));
  }

  // ── Legend ──
  function renderLegend(){
    const el=document.getElementById('map-legend');
    if(!el) return;
    el.innerHTML=Object.entries(STAGE_LABEL).map(([k,v])=>
      `<div style="display:flex;align-items:center;gap:4px;">
        <div style="width:7px;height:7px;border-radius:50%;background:${STAGE_COLOR[k]};flex-shrink:0;"></div>
        <span style="font-size:10px;color:#6E6E73;">${v}</span>
      </div>`).join('');
  }

  // ── Skeleton ──
  function injectSkeleton(host){
    host.innerHTML=`
<div style="padding:0;height:calc(100vh - 100px);min-height:520px;display:flex;flex-direction:column;gap:8px;">
  <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:10px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="font-size:15px;font-weight:500;color:#1C1C1E;letter-spacing:-0.01em;">Live Map</div>
      <div style="width:0.5px;height:20px;background:rgba(0,0,0,0.08);"></div>
      <input id="map-search" type="text" placeholder="Search vessel, Zendesk, HBL..." style="border:0.5px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 12px;font-size:12px;width:560px;outline:none;font-family:inherit;color:#1C1C1E;background:#fff;"/>
    </div>
    <div style="display:flex;align-items:center;gap:16px;">
      <div id="map-counter" style="font-size:11px;color:#6E6E73;"></div>
      <div id="map-legend" style="display:flex;gap:10px;flex-wrap:wrap;"></div>
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

  function injectStyles(){
    if(document.getElementById('map-sonar-style')) return;
    const s=document.createElement('style');
    s.id='map-sonar-style';
    s.textContent=`@keyframes mapSonar{0%{r:5;stroke-opacity:.55;stroke-width:1.5}100%{r:26;stroke-opacity:0;stroke-width:.3}}`;
    document.head.appendChild(s);
  }

  // ── World map — shifted right to use full canvas ──
  async function drawWorldMap(canvas){
    const w=canvas.offsetWidth||900, h=canvas.offsetHeight||480;
    canvas.width=w; canvas.height=h;
    const ctx=canvas.getContext('2d');
    ctx.fillStyle='#FAFAFA'; ctx.fillRect(0,0,w,h);

    // Centered more to the right to use full canvas — action is between 110°E and 155°E
    const projection=d3.geoMercator()
      .center([133,-8])
      .scale(w*0.30)
      .translate([w*0.45,h*0.46]);

    const topo=await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r=>r.json());
    const features=topojson.feature(topo,topo.objects.countries).features;
    const DOT_STEP=6, DOT_R=1.4;
    ctx.fillStyle='#D4D4D4';
    const off=document.createElement('canvas'); off.width=w; off.height=h;
    const octx=off.getContext('2d');
    for(const feature of features){
      octx.clearRect(0,0,w,h); octx.beginPath();
      d3.geoPath(projection,octx)(feature);
      octx.fillStyle='#000'; octx.fill();
      const imgData=octx.getImageData(0,0,w,h).data;
      for(let px=DOT_STEP/2;px<w;px+=DOT_STEP){
        for(let py=DOT_STEP/2;py<h;py+=DOT_STEP){
          const idx=(Math.floor(py)*w+Math.floor(px))*4;
          if(imgData[idx+3]>128){ ctx.beginPath(); ctx.arc(px,py,DOT_R,0,Math.PI*2); ctx.fill(); }
        }
      }
    }
    return {w,h,project:(lon,lat)=>projection([lon,lat])};
  }

  // ── Fetch data ──
  async function loadMapData(){
    const apiBase=(document.querySelector('meta[name="api-base"]')?.content||'').replace(/\/+$/,'');
    let token=null;
    if(window.Clerk?.session){ try{ token=await window.Clerk.session.getToken(); }catch(_){} }
    const headers=Object.assign({},token?{'Authorization':'Bearer '+token}:{});
    const api=async(path)=>{ const r=await fetch(apiBase+path,{headers}); if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); };

    const weekStart=window.state?.weekStart||'';
    const planRows=Array.isArray(window.state?.plan)?window.state.plan:[];

    // ── appliedByPO: use window.state.records (already loaded, avoids duplicate API call) ──
    // state.records = by_po array: [{po, units}] from /records/summary
    // Build appliedByPO — normalise PO to UPPERCASE to match plan's po_number
    const appliedByPO=new Map();
    const stateRecords=Array.isArray(window.state?.records)?window.state.records:[];
    for(const r of stateRecords){
      const po=String(r.po||r.po_number||'').trim().toUpperCase();
      if(po) appliedByPO.set(po,(appliedByPO.get(po)||0)+Number(r.units||0));
    }
    console.log('[Map] appliedByPO from state.records — entries:',appliedByPO.size,'sample:',Array.from(appliedByPO.entries()).slice(0,3));

    // ── receiving: use window.state.receiving (already loaded) ──
    const receiving=Array.isArray(window.state?.receiving)?window.state.receiving:[];
    console.log('[Map] receiving from state.receiving — rows:',receiving.length,'sample:',receiving.slice(0,2));

    // ── Flow data: 8 weeks (needed for lane/vessel data not in state) ──
    const weeks=[];
    for(let i=0;i<8;i++){
      const d=new Date(weekStart+'T00:00:00'); d.setDate(d.getDate()-i*7);
      weeks.push(d.toISOString().slice(0,10));
    }
    const flowResults=await Promise.allSettled(weeks.map(ws=>api(`/flow/week/${encodeURIComponent(ws)}/all`)));
    const allLanes=[], allContainers=[];
    for(const res of flowResults){
      if(res.status!=='fulfilled') continue;
      const fd={};
      for(const facVal of Object.values(res.value.facilities||{})){
        const d=(facVal?.data&&typeof facVal.data==='object')?facVal.data:{};
        for(const [k,v] of Object.entries(d)){
          if(k==='intl_lanes'&&v&&typeof v==='object'&&!Array.isArray(v)) fd.intl_lanes=Object.assign({},fd.intl_lanes||{},v);
          else fd[k]=v;
        }
      }
      const intl=(fd.intl_lanes&&typeof fd.intl_lanes==='object')?fd.intl_lanes:{};
      for(const [lk,manual] of Object.entries(intl)){
        const parts=lk.split('||');
        allLanes.push({key:lk,supplier:parts[0]||'',zendesk:parts[1]||'',freight:parts[2]||'',manual:manual||{}});
      }
      const wc=fd.intl_weekcontainers;
      const conts=Array.isArray(wc)?wc:(Array.isArray(wc?.containers)?wc.containers:[]);
      allContainers.push(...conts);
    }
    console.log('[Map] lanes:',allLanes.length,'containers:',allContainers.length,'plan rows:',planRows.length);
    console.log('[Map] plan sample zendesk fields:',planRows.slice(0,3).map(p=>p.zendesk_ticket));
    console.log('[Map] lane sample zendesks:',allLanes.slice(0,3).map(l=>l.zendesk));

    return {lanes:allLanes,containers:allContainers,plan:planRows,receiving,appliedByPO};
  }

  // ── Init ──
  async function initMap(host){
    injectSkeleton(host);
    injectStyles();
    renderLegend();

    const canvas=document.getElementById('map-canvas');
    const svgEl=document.getElementById('map-pins');
    const loading=document.getElementById('map-loading');
    const searchInput=document.getElementById('map-search');
    let mapData=null;

    async function loadLibs(){
      if(!window.d3) await new Promise((res,rej)=>{ const s=document.createElement('script'); s.src='https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js'; s.onload=res; s.onerror=rej; document.head.appendChild(s); });
      if(!window.topojson) await new Promise((res,rej)=>{ const s=document.createElement('script'); s.src='https://cdn.jsdelivr.net/npm/topojson@3/dist/topojson.min.js'; s.onload=res; s.onerror=rej; document.head.appendChild(s); });
    }

    try{ await loadLibs(); const dims=await drawWorldMap(canvas); _proj=dims.project; if(loading) loading.style.display='none'; }
    catch(e){ console.error('[Map] world map failed',e); if(loading) loading.textContent='Map unavailable'; }

    try{
      const raw=await loadMapData();
      mapData=buildMapData(raw.lanes,raw.containers,raw.plan,raw.receiving,raw.appliedByPO);
    }catch(e){ console.error('[Map] data failed',e); }

    if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,'');

    if(searchInput){
      searchInput.addEventListener('input',function(){
        if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,this.value);
      });
    }

    let resizeTimer;
    window.addEventListener('resize',()=>{
      clearTimeout(resizeTimer);
      resizeTimer=setTimeout(async()=>{
        try{ const dims=await drawWorldMap(canvas); _proj=dims.project; if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,searchInput?.value||''); }catch(e){}
      },300);
    });
  }

  window.showMapPage=function(){
    let pg=document.getElementById('page-map');
    if(!pg){
      pg=document.createElement('section');
      pg.id='page-map'; pg.style.cssText='padding:16px;display:block;';
      const main=document.querySelector('main.vo-wrap')||document.querySelector('main');
      if(main) main.appendChild(pg);
      initMap(pg);
    }
    pg.classList.remove('hidden'); pg.style.display='block';
  };

  window.hideMapPage=function(){
    const pg=document.getElementById('page-map');
    if(pg){ pg.classList.add('hidden'); pg.style.display='none'; }
  };

})();
