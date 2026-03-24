/* map_live_additive.js v4 — VelOzity Pinpoint Live Map
   One arc per vessel. Location pins clickable. Clean visual model.
   Phase 1: calculated position | Phase 2: swap getVesselPosition() for API
*/
(function () {
  'use strict';

  // ── Fixed locations — spread for visual clarity at Asia-Pacific zoom ──
  const LOCATIONS = {
    supplier:       { name: 'Supplier (Shenzhen)',  lat: 26.00,  lon: 118.00 },
    vas_facility:   { name: 'VAS Facility',         lat: 23.50,  lon: 114.50 },
    origin_port:    { name: 'Shenzhen Port',        lat: 20.50,  lon: 110.00 },
    sydney_port:    { name: 'Port Botany',          lat: -34.20, lon: 153.50 },
    sydney_airport: { name: 'Sydney Airport',       lat: -31.50, lon: 153.00 },
    client_wh:      { name: 'Sydney WH (Client)',   lat: -29.00, lon: 147.50 },
  };

  // ── Location pin colors ──
  const LOC_COLOR = {
    supplier:       '#888780',
    vas_facility:   '#990033',
    origin_port:    '#3B82F6',
    sydney_port:    '#3B82F6',
    sydney_airport: '#6B8FA8',
    client_wh:      '#1C1C1E',
  };

  // ── Stage colors ──
  const STAGE_COLOR = {
    at_supplier:  '#888780',
    vas:          '#990033',
    origin_port:  '#3B82F6',
    transit:      '#0EA5E9',
    clearing:     '#3B82F6',
    customs_hold: '#DC2626',
    last_mile:    '#1C1C1E',
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

  const STAGE_ORDER = ['at_supplier','origin_port','vas','transit','clearing','customs_hold','last_mile'];

  // ── Utilities ──
  function clamp(v,lo,hi){return Math.max(lo,Math.min(hi,v));}
  function lerp(a,b,t){return a+(b-a)*t;}
  function ns(tag){return document.createElementNS('http://www.w3.org/2000/svg',tag);}
  function fmtDate(v){if(!v)return'—';try{return new Date(v).toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'});}catch{return String(v);}}

  let _proj = null;
  function project(lon,lat){
    if(_proj) return _proj(lon,lat);
    return [(lon+180)/360*900,(90-lat)/180*480];
  }

  // ── Phase 1 position — Phase 2: replace with API call ──
  function getVesselPosition(departed_at, eta_port, arrived_at){
    if(arrived_at) return 1;
    if(!departed_at) return 0.02;
    const dep=new Date(departed_at).getTime();
    const eta=eta_port?new Date(eta_port).getTime():0;
    if(!eta||eta<=dep) return 0.5;
    return clamp((Date.now()-dep)/(eta-dep),0.05,0.95);
  }

  // ── Arc geometry ──
  function arcMid(x1,y1,x2,y2,bend){
    const dx=x2-x1,dy=y2-y1;
    const dist=Math.sqrt(dx*dx+dy*dy)||1;
    const nx=-dy/dist, ny=dx/dist;
    return [(x1+x2)/2+nx*dist*(bend||0.22),(y1+y2)/2+ny*dist*(bend||0.22)];
  }
  function arcPoint(x1,y1,x2,y2,t,bend){
    const [mx,my]=arcMid(x1,y1,x2,y2,bend);
    const ax=lerp(x1,mx,t),ay=lerp(y1,my,t);
    const bx=lerp(mx,x2,t),by=lerp(my,y2,t);
    return [lerp(ax,bx,t),lerp(ay,by,t)];
  }

  // ── Stage derivation ──
  function getLaneStage(m, hasReceiving){
    if(!m) return 'at_supplier';
    if(m.customs_hold) return 'customs_hold';
    if(m.dest_customs_cleared_at) return 'last_mile';
    if(m.arrived_at) return 'clearing';
    if(m.departed_at) return 'transit';
    if(hasReceiving) return 'vas';
    if(m.packing_list_ready_at) return 'origin_port';
    return 'at_supplier';
  }

  // ── Build data structures ──
  function buildMapData(lanes, containers, plan, receiving, appliedByPO, binsByPO){
    // Plan lookup by zendesk
    const planByZendesk = new Map();
    for(const p of plan){
      const zd=String(p.zendesk_ticket||'').trim();
      if(!zd) continue;
      if(!planByZendesk.has(zd)) planByZendesk.set(zd,[]);
      planByZendesk.get(zd).push(p);
    }

    // Received POs
    const receivedPOs=new Set((receiving||[]).map(r=>String(r.po_number||'').trim()).filter(Boolean));

    // Container → vessel lookup
    const contVessel=new Map();
    for(const c of containers){
      const v=String(c.vessel||'').trim();
      const cid=String(c.container_id||c.container||'').trim();
      if(v&&cid) contVessel.set(cid,v);
    }

    // Applied units by zendesk (sum across POs)
    const appliedByZendesk=new Map();
    for(const [zd, pRows] of planByZendesk){
      const pos=[...new Set(pRows.map(p=>String(p.po_number||'').trim()).filter(Boolean))];
      let total=0;
      for(const po of pos) total+=appliedByPO.get(po)||0;
      appliedByZendesk.set(zd, total);
    }

    // ── Build vessel groups (for arcs — only lanes with departed_at) ──
    const vesselGroups=new Map();

    // ── Build location groups (for static pins) ──
    const locationGroups={
      at_supplier:[],  // zendesks with plan but no receiving
      vas:[],          // zendesks in VAS
      origin_port:[],  // zendesks packing list ready, not departed
      clearing:[],     // zendesks arrived, in customs
      customs_hold:[], // zendesks on hold
      last_mile:[],    // zendesks in last mile
    };

    for(const lane of lanes){
      const m=lane.manual||{};
      const zendesk=String(lane.zendesk||'').trim();
      const freight=String(lane.freight||'').trim().toLowerCase();
      const isAir=freight==='air';
      const hasReceiving=[...new Set((planByZendesk.get(zendesk)||[]).map(p=>String(p.po_number||'').trim()).filter(Boolean))].some(po=>receivedPOs.has(po));
      const stage=getLaneStage(m,hasReceiving);

      if(stage==='delivered') continue;

      // Applied units for this zendesk
      const applied=appliedByZendesk.get(zendesk)||0;
      const hbl=m.hbl||'';
      const mbl=m.mbl||'';
      const supplier=lane.supplier||'';

      const zdEntry={zendesk, applied, hbl, mbl, supplier, freight:lane.freight, stage, manual:m, isAir};

      // ── Static location groups — things NOT moving ──
      if(stage!=='transit'){
        if(locationGroups[stage]) locationGroups[stage].push(zdEntry);
        continue;
      }

      // ── Vessel groups — things IN TRANSIT (have departed_at) ──
      let vessel=String(m.vessel||'').trim();
      if(!vessel){
        for(const c of containers){
          const lks=Array.isArray(c.lane_keys)?c.lane_keys:[];
          if(lks.includes(lane.key)){ vessel=String(c.vessel||'').trim(); if(vessel) break; }
        }
      }
      const key=vessel||('NO_VESSEL_'+zendesk);

      if(!vesselGroups.has(key)){
        vesselGroups.set(key,{
          vessel, isAir,
          zdentrys:[],
          departed_at:null, eta_port:null, arrived_at:null, dest_customs_cleared_at:null,
        });
      }
      const vg=vesselGroups.get(key);
      vg.zdentrys.push(zdEntry);

      const dep=m.departed_at, eta=m.eta_fc||m.latest_arrival_date;
      const arr=m.arrived_at, clr=m.dest_customs_cleared_at;
      if(dep&&(!vg.departed_at||dep<vg.departed_at)) vg.departed_at=dep;
      if(eta&&(!vg.eta_port||eta>vg.eta_port)) vg.eta_port=eta;
      if(arr&&(!vg.arrived_at||arr>vg.arrived_at)) vg.arrived_at=arr;
      if(clr&&(!vg.dest_customs_cleared_at||clr>vg.dest_customs_cleared_at)) vg.dest_customs_cleared_at=clr;
    }

    return {
      vesselGroups: Array.from(vesselGroups.values()),
      locationGroups,
      appliedByZendesk,
    };
  }

  // ── Detail panel — vessel ──
  function openVesselDetail(vg, panel){
    const color=STAGE_COLOR['transit'];
    const totalApplied=vg.zdentrys.reduce((s,z)=>s+z.applied,0);
    const allHBLs=[...new Set(vg.zdentrys.map(z=>z.hbl).filter(Boolean))];
    const allZd=[...new Set(vg.zdentrys.map(z=>z.zendesk).filter(Boolean))];
    const freightLabel=vg.isAir?'Air':'Sea';
    const vesselName=vg.vessel||'Unassigned vessel';

    const zdRows=vg.zdentrys.map(z=>
      `<div style="display:flex;justify-content:space-between;padding:8px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <span style="font-size:11px;color:#1C1C1E;font-weight:500;">#${z.zendesk}</span>
        <span style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} units applied</span>
      </div>`
    ).join('');

    panel.innerHTML=`
      <button onclick="this.closest('#map-detail').style.transform='translateX(100%)'"
        style="position:absolute;top:14px;right:14px;width:26px;height:26px;border-radius:7px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:13px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>
      <div style="padding-right:32px;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:6px;">${vesselName}</div>
        <div style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:6px;background:${color}18;">
          <div style="width:6px;height:6px;border-radius:50%;background:${color};"></div>
          <span style="font-size:10px;font-weight:500;color:${color};">In Transit</span>
        </div>
      </div>
      <div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">Shipment</div>
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:14px;">
        ${dRow('Freight',freightLabel)}
        ${dRow('HBL',allHBLs.join(', ')||'—')}
        ${dRow('Departed',fmtDate(vg.departed_at))}
        ${dRow('ETA Port',fmtDate(vg.eta_port))}
        ${vg.arrived_at?dRow('Arrived',fmtDate(vg.arrived_at)):''}
        ${vg.dest_customs_cleared_at?dRow('Customs Cleared',fmtDate(vg.dest_customs_cleared_at)):''}
      </div>
      <div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">Zendesks on vessel</div>
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:14px;">
        ${zdRows}
        <div style="display:flex;justify-content:space-between;padding:8px 12px;background:#F5F5F7;">
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">Total applied</span>
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalApplied.toLocaleString()} units</span>
        </div>
      </div>`;
    panel.style.transform='translateX(0)';
  }

  // ── Detail panel — location ──
  function openLocationDetail(locKey, entries, appliedByZendesk, panel){
    const color=LOC_COLOR[locKey];
    const locName=LOCATIONS[locKey].name;
    const stageLabel={
      at_supplier:'At Supplier — plan loaded, not yet received',
      vas:'At Facility / VAS — in processing',
      origin_port:'At Origin Port — packing list ready',
      clearing:'At Port / Clearing',
      customs_hold:'Customs Hold',
      last_mile:'Last Mile',
    }[locKey]||locKey;

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
      </div>`
    ).join('');

    const totalApplied=entries.reduce((s,z)=>s+z.applied,0);

    panel.innerHTML=`
      <button onclick="this.closest('#map-detail').style.transform='translateX(100%)'"
        style="position:absolute;top:14px;right:14px;width:26px;height:26px;border-radius:7px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:13px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>
      <div style="padding-right:32px;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:6px;">${locName}</div>
        <div style="font-size:11px;color:#6E6E73;">${stageLabel}</div>
      </div>
      <div style="font-size:10px;font-weight:500;color:#AEAEB2;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">${entries.length} Zendesk${entries.length!==1?'s':''}</div>
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:14px;">
        ${rows||'<div style="padding:12px;font-size:11px;color:#AEAEB2;">Nothing here right now</div>'}
        ${entries.length>0?`<div style="display:flex;justify-content:space-between;padding:8px 12px;background:#F5F5F7;">
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">Total applied</span>
          <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalApplied.toLocaleString()} units</span>
        </div>`:''}
      </div>`;
    panel.style.transform='translateX(0)';
  }

  function dRow(label,value){
    if(!value||value==='—') return '';
    return `<div style="display:flex;justify-content:space-between;padding:8px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
      <span style="font-size:11px;color:#AEAEB2;">${label}</span>
      <span style="font-size:11px;font-weight:500;color:#1C1C1E;text-align:right;">${value}</span>
    </div>`;
  }

  // ── Render map ──
  function renderMap(svgEl, vesselGroups, locationGroups, appliedByZendesk, filter){
    while(svgEl.firstChild) svgEl.removeChild(svgEl.firstChild);

    const q=(filter||'').toLowerCase().trim();
    const detail=document.getElementById('map-detail');
    const tooltip=document.getElementById('map-tooltip');
    const wrap=svgEl.parentElement;

    // ── Location pins — always visible, always clickable ──
    for(const [locKey, loc] of Object.entries(LOCATIONS)){
      const [lx,ly]=project(loc.lon,loc.lat);
      const color=LOC_COLOR[locKey];
      const entries=locationGroups[locKey]||[];
      const hasEntries=entries.length>0;

      // Outer glow
      const glow=ns('circle');
      glow.setAttribute('cx',lx); glow.setAttribute('cy',ly);
      glow.setAttribute('r','10'); glow.setAttribute('fill',color);
      glow.setAttribute('opacity',hasEntries?'0.12':'0.06');
      svgEl.appendChild(glow);

      // Pin dot
      const dot=ns('circle');
      dot.setAttribute('cx',lx); dot.setAttribute('cy',ly);
      dot.setAttribute('r', hasEntries?'6':'4');
      dot.setAttribute('fill',color);
      dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','1.5');
      dot.setAttribute('opacity', hasEntries?'1':'0.45');
      svgEl.appendChild(dot);

      // Pulse rings if has entries
      if(hasEntries){
        for(let ri=0;ri<3;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',lx); ring.setAttribute('cy',ly); ring.setAttribute('r','6');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke',color);
          ring.setAttribute('stroke-width','1.5');
          ring.style.animation=`mapSonar 2.4s ease-out infinite ${ri*0.8}s`;
          svgEl.appendChild(ring);
        }
      }

      // Label
      const txt=ns('text');
      txt.setAttribute('x',lx+9); txt.setAttribute('y',ly+4);
      txt.setAttribute('font-size','9.5'); txt.setAttribute('font-weight','500');
      txt.setAttribute('fill',hasEntries?color:'#BCBCBC');
      txt.setAttribute('font-family','-apple-system,sans-serif');
      txt.textContent=loc.name;
      svgEl.appendChild(txt);

      // Count badge if entries
      if(hasEntries){
        const badgeBg=ns('rect');
        badgeBg.setAttribute('x',lx+9); badgeBg.setAttribute('y',ly+7);
        badgeBg.setAttribute('width','28'); badgeBg.setAttribute('height','11');
        badgeBg.setAttribute('rx','3'); badgeBg.setAttribute('fill',color);
        badgeBg.setAttribute('opacity','0.15');
        svgEl.appendChild(badgeBg);
        const badgeTxt=ns('text');
        badgeTxt.setAttribute('x',lx+23); badgeTxt.setAttribute('y',ly+16);
        badgeTxt.setAttribute('text-anchor','middle');
        badgeTxt.setAttribute('font-size','8'); badgeTxt.setAttribute('font-weight','500');
        badgeTxt.setAttribute('fill',color);
        badgeTxt.setAttribute('font-family','-apple-system,sans-serif');
        badgeTxt.textContent=entries.length+' ZD';
        svgEl.appendChild(badgeTxt);
      }

      // Click area
      const hit=ns('circle');
      hit.setAttribute('cx',lx); hit.setAttribute('cy',ly);
      hit.setAttribute('r','16'); hit.setAttribute('fill','transparent');
      hit.style.cursor='pointer';
      hit.style.pointerEvents='all';
      hit.addEventListener('mouseenter',()=>{
        tooltip.style.display='block';
        tooltip.textContent=loc.name+(entries.length?` — ${entries.length} Zendesk${entries.length!==1?'s':''}`:' — nothing active');
      });
      hit.addEventListener('mousemove',e=>{
        const r=wrap.getBoundingClientRect();
        tooltip.style.left=(e.clientX-r.left+14)+'px';
        tooltip.style.top=(e.clientY-r.top-36)+'px';
      });
      hit.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
      hit.addEventListener('click',()=>{ openLocationDetail(locKey,entries,appliedByZendesk,detail); });
      svgEl.appendChild(hit);
    }

    // ── Vessel arcs — one per vessel, only for transit vessels ──
    const seaVessels=vesselGroups.filter(vg=>!vg.isAir);
    const airVessels=vesselGroups.filter(vg=>vg.isAir);

    vesselGroups.forEach((vg,idx)=>{
      const sameType=vg.isAir?airVessels:seaVessels;
      const typeIdx=sameType.indexOf(vg);
      const typeCount=sameType.length;
      const offsetFactor=typeCount>1?(typeIdx-(typeCount-1)/2)*0.10:0;
      const bend=0.22+offsetFactor;

      const [ox,oy]=project(LOCATIONS.origin_port.lon,LOCATIONS.origin_port.lat);
      const destLoc=vg.isAir?LOCATIONS.sydney_airport:LOCATIONS.sydney_port;
      const [dx,dy]=project(destLoc.lon,destLoc.lat);
      const [mx,my]=arcMid(ox,oy,dx,dy,bend);

      const progress=getVesselPosition(vg.departed_at,vg.eta_port,vg.arrived_at);
      const [vx,vy]=arcPoint(ox,oy,dx,dy,progress,bend);

      // Search filter
      const allZd=vg.zdentrys.map(z=>z.zendesk);
      const allHBL=vg.zdentrys.map(z=>z.hbl);
      const isMatch=!q||
        vg.vessel?.toLowerCase().includes(q)||
        allZd.some(z=>z.includes(q))||
        allHBL.some(h=>h?.toLowerCase().includes(q));
      const arcOpacity=q?(isMatch?1:0.05):1;

      // Full route arc (faint — shows destination)
      const fullArc=ns('path');
      fullArc.setAttribute('d',`M${ox},${oy} Q${mx},${my} ${dx},${dy}`);
      fullArc.setAttribute('stroke','#0EA5E9');
      fullArc.setAttribute('stroke-width','1');
      fullArc.setAttribute('fill','none');
      fullArc.setAttribute('stroke-opacity', String(arcOpacity*0.15));
      svgEl.appendChild(fullArc);

      // Travelled portion (solid, from origin to vessel position)
      const [tmx,tmy]=arcMid(ox,oy,vx,vy,bend);
      const travelArc=ns('path');
      travelArc.setAttribute('d',`M${ox},${oy} Q${tmx},${tmy} ${vx},${vy}`);
      travelArc.setAttribute('stroke','#0EA5E9');
      travelArc.setAttribute('stroke-width','2');
      travelArc.setAttribute('fill','none');
      travelArc.setAttribute('stroke-opacity', String(arcOpacity*0.6));
      svgEl.appendChild(travelArc);

      // Vessel name tag above dot
      if(vg.vessel && (!q||isMatch)){
        const tagW=Math.min(vg.vessel.length*5+14,160);
        const tagBg=ns('rect');
        tagBg.setAttribute('x',vx-tagW/2); tagBg.setAttribute('y',vy-22);
        tagBg.setAttribute('width',tagW); tagBg.setAttribute('height','13');
        tagBg.setAttribute('rx','3'); tagBg.setAttribute('fill','#fff');
        tagBg.setAttribute('stroke','#0EA5E9'); tagBg.setAttribute('stroke-width','0.5');
        tagBg.setAttribute('opacity','0.95');
        svgEl.appendChild(tagBg);
        const tagTxt=ns('text');
        tagTxt.setAttribute('x',vx); tagTxt.setAttribute('y',vy-12);
        tagTxt.setAttribute('text-anchor','middle');
        tagTxt.setAttribute('font-size','8'); tagTxt.setAttribute('font-weight','500');
        tagTxt.setAttribute('fill','#0EA5E9');
        tagTxt.setAttribute('font-family','-apple-system,sans-serif');
        tagTxt.textContent=vg.vessel.length>24?vg.vessel.slice(0,22)+'\u2026':vg.vessel;
        svgEl.appendChild(tagTxt);
      }

      // Pulse rings at vessel tip
      for(let ri=0;ri<3;ri++){
        const ring=ns('circle');
        ring.setAttribute('cx',vx); ring.setAttribute('cy',vy); ring.setAttribute('r','5');
        ring.setAttribute('fill','none'); ring.setAttribute('stroke','#0EA5E9');
        ring.setAttribute('stroke-width','1.5');
        ring.setAttribute('opacity', String(arcOpacity));
        ring.style.animation=`mapSonar 2.2s ease-out infinite ${ri*0.75}s`;
        svgEl.appendChild(ring);
      }

      // Vessel dot — clickable
      const gEl=ns('g');
      gEl.setAttribute('opacity', String(arcOpacity));
      gEl.style.cursor='pointer'; gEl.style.pointerEvents='all';
      const dot=ns('circle');
      dot.setAttribute('cx',vx); dot.setAttribute('cy',vy);
      dot.setAttribute('r','5.5'); dot.setAttribute('fill','#0EA5E9');
      dot.setAttribute('stroke','#fff'); dot.setAttribute('stroke-width','2');
      gEl.appendChild(dot);

      const vesselLabel=vg.vessel||'Unassigned vessel';
      gEl.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent=vesselLabel+' · In Transit'; });
      gEl.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
      gEl.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
      gEl.addEventListener('click',()=>{ openVesselDetail(vg,detail); });
      svgEl.appendChild(gEl);
    });

    // Update counter
    const counter=document.getElementById('map-counter');
    if(counter){
      const total=vesselGroups.length+Object.values(locationGroups).reduce((s,a)=>s+(a.length>0?1:0),0);
      counter.textContent=vesselGroups.length+' vessel'+(vesselGroups.length!==1?'s':'')+' in transit';
    }

    console.log('[Map] vessels:',vesselGroups.length,'| locations:',Object.entries(locationGroups).map(([k,v])=>k+':'+v.length).join(' '));
  }

  // ── Render legend ──
  function renderLegend(){
    const el=document.getElementById('map-legend');
    if(!el) return;
    el.innerHTML=Object.entries(STAGE_LABEL).map(([k,v])=>
      `<div style="display:flex;align-items:center;gap:4px;">
        <div style="width:7px;height:7px;border-radius:50%;background:${STAGE_COLOR[k]};flex-shrink:0;"></div>
        <span style="font-size:10px;color:#6E6E73;">${v}</span>
      </div>`
    ).join('');
  }

  // ── Inject skeleton ──
  function injectSkeleton(host){
    host.innerHTML=`
<div style="padding:0;height:calc(100vh - 100px);min-height:520px;display:flex;flex-direction:column;gap:8px;">
  <div style="background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:10px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="font-size:15px;font-weight:500;color:#1C1C1E;letter-spacing:-0.01em;">Live Map</div>
      <div style="width:0.5px;height:20px;background:rgba(0,0,0,0.08);"></div>
      <input id="map-search" type="text" placeholder="Search vessel, Zendesk, HBL..." style="border:0.5px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 12px;font-size:12px;width:280px;outline:none;font-family:inherit;color:#1C1C1E;background:#fff;"/>
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
    s.textContent=`@keyframes mapSonar{0%{r:5;stroke-opacity:.65;stroke-width:2}100%{r:22;stroke-opacity:0;stroke-width:.5}}`;
    document.head.appendChild(s);
  }

  // ── Draw world map ──
  async function drawWorldMap(canvas){
    const w=canvas.offsetWidth||900, h=canvas.offsetHeight||480;
    canvas.width=w; canvas.height=h;
    const ctx=canvas.getContext('2d');
    ctx.fillStyle='#FAFAFA'; ctx.fillRect(0,0,w,h);

    const projection=d3.geoMercator()
      .center([128,-5])
      .scale(w*0.28)
      .translate([w*0.38,h*0.48]);

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

    const [recvRes,binsRes,summaryRes]=await Promise.allSettled([
      api(`/receiving?weekStart=${encodeURIComponent(weekStart)}`),
      api(`/bins/weeks/${encodeURIComponent(weekStart)}`),
      api(`/summary/po_sku?from=${encodeURIComponent(weekStart)}&to=${encodeURIComponent(weekStart)}&status=complete`),
    ]);

    const receiving=recvRes.status==='fulfilled'?(Array.isArray(recvRes.value)?recvRes.value:[]):[];
    const bins=binsRes.status==='fulfilled'?(Array.isArray(binsRes.value)?binsRes.value:[]):[];
    const summary=summaryRes.status==='fulfilled'?(summaryRes.value?.rows||[]):[];

    const appliedByPO=new Map();
    for(const r of summary){
      const po=String(r.po_number||r.po||'').trim();
      if(po) appliedByPO.set(po,(appliedByPO.get(po)||0)+Number(r.units||r.applied_qty||0));
    }
    const binsByPO=new Map();
    for(const b of bins){
      const po=String(b.po_number||'').trim(); if(!po) continue;
      if(!binsByPO.has(po)) binsByPO.set(po,new Set());
      binsByPO.get(po).add(String(b.mobile_bin||'').trim());
    }

    return {lanes:allLanes,containers:allContainers,plan:planRows,receiving,appliedByPO,binsByPO};
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

    try{
      await loadLibs();
      const dims=await drawWorldMap(canvas);
      _proj=dims.project;
      if(loading) loading.style.display='none';
    }catch(e){ console.error('[Map] world map failed',e); if(loading) loading.textContent='Map unavailable'; }

    try{
      const raw=await loadMapData();
      mapData=buildMapData(raw.lanes,raw.containers,raw.plan,raw.receiving,raw.appliedByPO,raw.binsByPO);
    }catch(e){ console.error('[Map] data failed',e); }

    if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,mapData.appliedByZendesk,'');

    if(searchInput){
      searchInput.addEventListener('input',function(){
        if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,mapData.appliedByZendesk,this.value);
      });
    }

    let resizeTimer;
    window.addEventListener('resize',()=>{
      clearTimeout(resizeTimer);
      resizeTimer=setTimeout(async()=>{
        try{ const dims=await drawWorldMap(canvas); _proj=dims.project; if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,mapData.appliedByZendesk,searchInput?.value||''); }catch(e){}
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
