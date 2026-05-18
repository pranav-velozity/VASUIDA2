/* map_live_additive.js v5 — VelOzity Pinpoint Live Map
   Fixed field names from source: pack/departed/arrived/destClr/hold/etaFC
   One arc per vessel. Clickable location pins. Sea arc goes east.
*/
(function () {
  'use strict';

  const LOCATIONS = {
    supplier:       { name: 'Supplier',  lat: 26.00, lon: 118.00 },
    vas_facility:   { name: 'SZ VAS Facility',         lat: 23.50, lon: 114.50 },
    origin_port:    { name: 'Shenzhen Port',        lat: 20.50, lon: 110.00 },
    sydney_port:    { name: 'Port Botany',          lat: -34.20, lon: 153.50 },
    sydney_airport: { name: 'Sydney Airport',       lat: -31.50, lon: 153.00 },
    client_wh:      { name: 'TIC FC',   lat: -29.00, lon: 147.50 },
  };

  const LOC_COLOR = {
    supplier:       '#C8860A',
    vas_facility:   '#990033',
    origin_port:    '#3B82F6',
    sydney_port:    '#3B82F6',
    sydney_airport: '#6B8FA8',
    client_wh:      '#1C1C1E',
  };

  const AIR_COLOR = '#4A9B8E';  // matte teal for air freight — distinct from sea red
  const STAGE_COLOR = {
    at_supplier:  '#C8860A',
    vas:          '#990033',
    origin_port:  '#3B82F6',
    transit:      '#990033',
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

  // ── Utilities ──
  function clamp(v,lo,hi){return Math.max(lo,Math.min(hi,v));}
  function lerp(a,b,t){return a+(b-a)*t;}
  function ns(tag){return document.createElementNS('http://www.w3.org/2000/svg',tag);}
  function fmtDate(v){
    if(!v||v==='undefined'||v==='null') return '—';
    try{ return new Date(v).toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'}); }
    catch{ return String(v); }
  }
  function fmtWeek(ws){
    // Convert week_start date to "Wk Mar 16" format
    if(!ws) return '';
    try{
      const d=new Date(ws+'T00:00:00');
      return 'Wk '+d.toLocaleDateString('en-AU',{day:'numeric',month:'short'});
    }catch{ return ws; }
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
    // pack date = goods ready to ship, at origin port
    if(getPackingReady(m)) return 'origin_port';
    // VAS = received at facility, packing list NOT yet raised
    if(hasReceiving) return 'vas';
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
    const zdWeek = new Map(); // zendesk → latest week_start
    for(const p of plan){
      const zd = String(p.zendesk_ticket ?? p.zendesk ?? '').trim();
      const po = String(p.po_number ?? '').trim().toUpperCase();
      if(!zd || zd === 'undefined') continue;
      if(!planByZendesk.has(zd)) planByZendesk.set(zd, []);
      planByZendesk.get(zd).push(p);
      if(po) zdByPO.set(po, zd);
      // Track latest week for this zendesk
      const ws = p._week_start || p.start_date || '';
      if(ws && (!zdWeek.has(zd) || ws > zdWeek.get(zd))) zdWeek.set(zd, ws);
    }
    console.log('[Map:build] planByZendesk keys:',Array.from(planByZendesk.keys()).slice(0,8));
    // Find plan rows for lane zendesks specifically
    const laneZDs=[...new Set(lanes.map(l=>String(l.zendesk||'').trim()).filter(Boolean))];
    console.log('[Map:build] lane zendesks:',laneZDs.slice(0,8));
    console.log('[Map:build] lane ZDs in plan?',laneZDs.slice(0,5).map(zd=>zd+':'+(planByZendesk.has(zd)?'YES':'NO')));
    // Sample older plan rows to see if zendesk_ticket is populated
    const olderRows=plan.filter(p=>!['77665','77664','77669','77671','77750','77736','77748','77754'].includes(String(p.zendesk_ticket||'').trim()));
    console.log('[Map:build] older plan row sample:',olderRows.slice(0,3).map(p=>({zd:p.zendesk_ticket,po:p.po_number,sup:p.supplier_name})));

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
    // Approach 3: for lanes whose zendesk has no applied yet — 
    // use receiving rows to find their POs, then look up applied
    // receiving has po_number, and those POs may be in appliedByPO
    const receivingByPO = new Map();
    for(const r of (receiving||[])){
      const po = String(r.po_number||'').trim().toUpperCase();
      if(po) receivingByPO.set(po, true);
    }
    // Build zendesk→POs from ALL plan rows (not just planByZendesk lookup)
    // This catches zendesks whose plan rows have empty zendesk_ticket field
    // by falling back to the lane key's zendesk
    for(const lane of lanes){
      const laneZD = String(lane.zendesk||'').trim();
      if(!laneZD || appliedByZendesk.get(laneZD)) continue; // already has units
      // Find plan rows that match this lane's supplier+freight
      const sup = String(lane.supplier||'').trim().toLowerCase();
      const frt = String(lane.freight||'').trim().toLowerCase();
      let total = 0;
      for(const p of plan){
        const pSup = String(p.supplier_name||'').trim().toLowerCase();
        const pFrt = String(p.freight_type||'').trim().toLowerCase();
        const po = String(p.po_number||'').trim().toUpperCase();
        if(pSup===sup && pFrt===frt && po){
          total += appliedByPO.get(po) || 0;
        }
      }
      if(total > 0) appliedByZendesk.set(laneZD, total);
    }
    console.log('[Map:build] appliedByZendesk non-zero:',Array.from(appliedByZendesk.entries()).filter(([,v])=>v>0).slice(0,8));
    console.log('[Map:build] appliedByPO keys sample:',Array.from(appliedByPO.keys()).slice(0,6));
    console.log('[Map:build] zdByPO sample (PO→zendesk):',Array.from(zdByPO.entries()).slice(0,6));

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
      
      // hasReceiving: goods physically at your facility (receiving table only)
      // Do NOT use hasApplied as proxy — in-transit goods also have applied units
      const sup = String(lane.supplier||'').trim().toLowerCase();
      const frt = String(lane.freight||'').trim().toLowerCase();
      const hasReceivingViaPlan = lanePos.some(po => receivedPOs.has(po));
      const hasApplied = (appliedByZendesk.get(zendesk)||0) > 0;
      // Fallback: match by supplier+freight when plan PO lookup fails
      const hasReceivingViaSup = !hasReceivingViaPlan && Array.from(receivedPOs).some(po => {
        const planRow = plan.find(p => String(p.po_number||'').trim().toUpperCase() === po);
        if(!planRow) return false;
        return String(planRow.supplier_name||'').trim().toLowerCase() === sup &&
               String(planRow.freight_type||'').trim().toLowerCase() === frt;
      });
      const hasReceiving = hasReceivingViaPlan || hasReceivingViaSup;
      const stage = getLaneStage(m, hasReceiving);
      if(stage === 'delivered') continue;
      // Detailed log for specific zendesks to diagnose stage assignment
      if(['77664','77671','77736','77665','77669','77750','77748','77754'].includes(zendesk)){
        console.log(`[Map:build] ZD ${zendesk} → stage:${stage} | manual keys:`,Object.keys(m).filter(k=>m[k]), '| departed:',getDeparted(m),'pack:',getPackingReady(m),'arrived:',getArrived(m));
      }

      const applied = appliedByZendesk.get(zendesk) || 0;
      // Planned units for this zendesk from plan rows
      const lanePoRowsForPlanned = planByZendesk.get(zendesk) ||
                                   planByZendesk.get(String(Number(zendesk))) || [];
      const planned = lanePoRowsForPlanned.reduce((s,p)=>s+Number(p.target_qty||0),0);
      const hbl = getHbl(m);
      const mbl = getMbl(m);
      const etaPort = getEtaFC(m) || getLatestArrival(m);

      const weekLabel = zdWeek.get(zendesk) || '';
      const zdEntry = {zendesk, applied, planned, hbl, mbl, supplier: lane.supplier||'', freight: lane.freight||'', stage, manual: m, isAir, etaPort, weekLabel};

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
      // Group unassigned vessels by freight type so they show as one arc
      // instead of many overlapping dots at origin port
      const key = vessel || (isAir ? 'NO_VESSEL_AIR' : 'NO_VESSEL_SEA');

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

    // Add plan-only zendesks that have no lane data yet
    // Check receiving to determine correct stage:
    // - received POs exist → VAS (goods at facility, processing in progress)
    // - no receiving → at_supplier (plan loaded, not yet received)
    const processedZDs = new Set([
      ...Array.from(vesselGroups.values()).flatMap(v=>v.zdentrys.map(z=>z.zendesk)),
      ...Object.values(locationGroups).flatMap(e=>e.map(z=>z.zendesk))
    ]);
    for(const [zd, pRows] of planByZendesk){
      if(processedZDs.has(zd)) continue;
      const applied = appliedByZendesk.get(zd)||0;
      const planned = pRows.reduce((s,p)=>s+Number(p.target_qty||0),0);
      const supplier = String(pRows[0]?.supplier_name||'').trim();
      const freight = String(pRows[0]?.freight_type||'').trim();
      const isAir = freight.toLowerCase()==='air';
      // Check if any POs for this zendesk have been received
      const zdPos = [...new Set(pRows.map(p=>String(p.po_number||'').trim().toUpperCase()).filter(Boolean))];
      const hasRecv = zdPos.some(po=>receivedPOs.has(po));
      const stage = hasRecv ? 'vas' : 'at_supplier';
      const weekLabel = zdWeek.get(zd) || '';
      locationGroups[stage].push({zendesk:zd, applied, planned, hbl:'', mbl:'', supplier, freight, stage, manual:{}, isAir, etaPort:null, weekLabel});
      console.log('[Map:build] Plan-only ZD →',stage,':',zd,supplier,'received:',hasRecv);
    }
    console.log('[Map:build] result — vesselGroups:',vesselGroups.size,'locationGroups:',Object.entries(locationGroups).map(([k,v])=>k+':'+v.length).join(' '));
    // Check where specific zendesks landed
    const targetZDs=['77664','77671','77748','77754'];
    for(const [stage,entries] of Object.entries(locationGroups)){
      const found=entries.filter(e=>targetZDs.includes(e.zendesk));
      if(found.length) console.log('[Map:build] Found in '+stage+':',found.map(e=>e.zendesk));
    }
    const inVessel=Array.from(vesselGroups.values()).flatMap(v=>v.zdentrys.map(z=>z.zendesk));
    const foundInVessel=targetZDs.filter(z=>inVessel.includes(z));
    if(foundInVessel.length) console.log('[Map:build] Found in vesselGroups:',foundInVessel);
    const allFound=[...Object.values(locationGroups).flatMap(e=>e.map(z=>z.zendesk)),...inVessel];
    const notFound=targetZDs.filter(z=>!allFound.includes(z));
    if(notFound.length) console.log('[Map:build] NOT FOUND ANYWHERE:',notFound);
    console.log('[Map:build] vessel breakdown:',Array.from(vesselGroups.values()).map(v=>`${v.vessel||'NO_VES'}(${v.isAir?'air':'sea'}):${v.zdentrys.length}ZD`).join(' '));
    console.log('[Map:build] air transit lanes:',lanes.filter(l=>String(l.freight||'').toLowerCase()==='air'&&getDeparted(l.manual||{})).map(l=>l.zendesk).join(','));
    // Diagnose why VAS / supplier / last_mile may be empty
    console.log('[Map:build] receivedPOs count:',receivedPOs.size,'sample:',Array.from(receivedPOs).slice(0,3));
    console.log('[Map:build] lanes without departure (should be VAS candidates):',
      lanes.filter(l=>!getDeparted(l.manual||{})).map(l=>({zd:l.zendesk,pack:getPackingReady(l.manual||{}),recv:receivedPOs.size>0})).slice(0,5)
    );
    console.log('[Map:build] last_mile entries:',locationGroups.last_mile.slice(0,3).map(z=>z.zendesk));
    console.log('[Map:build] last_mile entries in locationGroups:',locationGroups.last_mile.length);

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
    const color = vg.isAir ? AIR_COLOR : '#990033';
    const totalApplied=vg.zdentrys.reduce((s,z)=>s+z.applied,0);
    const allHBLs=[...new Set(vg.zdentrys.map(z=>z.hbl).filter(Boolean))];
    const name=vg.vessel||'Unassigned vessel';
    // ETA FC: use actual etaPort or baseline from first lane's manual
    const etaFCActual = vg.etaPort;
    const etaFCBaseline = vg.zdentrys[0]?.manual ? getEtaFC(vg.zdentrys[0].manual) : null;
    const latestArrBaseline = vg.zdentrys[0]?.manual ? getLatestArrival(vg.zdentrys[0].manual) : null;
    const etaDisplay = etaFCActual || etaFCBaseline || latestArrBaseline;
    const etaLabel = etaFCActual ? 'ETA Port / FC' : (etaFCBaseline||latestArrBaseline) ? 'ETA FC (baseline)' : null;

    const zdRows=vg.zdentrys.map(z=>{
      const wkLabel = z.weekLabel ? ` (${fmtWeek(z.weekLabel)})` : '';
      return `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:9px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <div>
          <div style="font-size:11px;font-weight:500;color:#1C1C1E;">#${z.zendesk}<span style="font-size:10px;font-weight:400;color:#AEAEB2;">${wkLabel}</span></div>
          ${z.supplier?`<div style="font-size:10px;color:#AEAEB2;">${z.supplier}</div>`:''}
        </div>
        <div style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} applied</div>
      </div>`;
    }).join('');
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
        ${etaLabel?dRow(etaLabel,fmtDate(etaDisplay)):''}
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
      </div>
      ${vg.zdentrys.length>0?`<button type="button" data-action="open-detail-modal" data-scope="vessel" data-vessel="${(vg.vessel||'').replace(/"/g,'&quot;')}" data-isair="${vg.isAir?'1':'0'}"
        style="width:100%;margin-top:12px;background:#fff;color:#990033;border:1px solid rgba(153,0,51,0.3);border-radius:9px;padding:9px 12px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;display:flex;align-items:center;justify-content:center;gap:6px;transition:background .15s;"
        onmouseover="this.style.background='rgba(153,0,51,0.05)'"
        onmouseout="this.style.background='#fff'">
        View full detail (POs · SKUs · days)
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>
      </button>`:''}`;
    panel.style.transform='translateX(0)';
    // Wire the "View full detail" button
    const vDetailLink = panel.querySelector('[data-action="open-detail-modal"]');
    if(vDetailLink){
      const zds = vg.zdentrys.map(z=>z.zendesk);
      vDetailLink.addEventListener('click', ()=> openDetailModal({scope:'vessel', zendesks: zds, vessel: vg.vessel||'Unassigned'}));
    }
  }

  // ── Location detail panel ──
  function openLocationDetail(locKey,entries,panel){
    const color=LOC_COLOR[locKey];
    const stageToName={
      at_supplier:'Supplier',
      vas:'SZ VAS Facility',
      origin_port:'Shenzhen Port',
      clearing:'At Port / Clearing',
      customs_hold:'Customs Hold',
      last_mile:'TIC FC',
    };
    const locName=stageToName[locKey]||locKey;
    const stageDesc={
      at_supplier:'Plan loaded — not yet received',
      vas:'Received — VAS processing in progress',
      origin_port:'Packing list ready — awaiting departure',
      clearing:'Arrived — in customs clearing',
      customs_hold:'Customs Hold — action required',
      last_mile:'Customs cleared — last mile delivery',
    }[locKey]||'';
    const rows=entries.map(z=>{
      const pct=z.planned>0?Math.round(z.applied/z.planned*100):null;
      const showPct=pct!==null&&(locKey==='vas'||locKey==='last_mile');
      const pctColor=pct>=100?'#3B82F6':pct>50?'#B5956A':'#990033';
      const wkLabel = z.weekLabel ? ` (${fmtWeek(z.weekLabel)})` : '';
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:9px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);">
        <div>
          <div style="font-size:11px;font-weight:500;color:#1C1C1E;">#${z.zendesk}<span style="font-size:10px;font-weight:400;color:#AEAEB2;">${wkLabel}</span></div>
          ${z.supplier?`<div style="font-size:10px;color:#AEAEB2;">${z.supplier}</div>`:''}
        </div>
        <div style="text-align:right;">
          ${z.planned>0
            ?`<div style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} / ${z.planned.toLocaleString()}</div>
              ${showPct?`<div style="font-size:10px;color:${pctColor};">${pct}% done</div>`:''}`
            :`<div style="font-size:11px;color:#6E6E73;">${z.applied.toLocaleString()} applied</div>`}
          ${z.hbl?`<div style="font-size:10px;color:#AEAEB2;">HBL: ${z.hbl}</div>`:''}
        </div>
      </div>`;
    }).join('');
    const totalApplied=entries.reduce((s,z)=>s+z.applied,0);
    const totalPlanned=entries.reduce((s,z)=>s+(z.planned||0),0);
    const totalPct=totalPlanned>0?Math.round(totalApplied/totalPlanned*100):null;
    const pctBarColor=totalPct>=100?'#3B82F6':totalPct>50?'#B5956A':'#990033';
    panel.innerHTML=`${closeBtn()}
      <div style="padding-right:32px;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:500;color:#1C1C1E;margin-bottom:4px;">${locName}</div>
        <div style="font-size:11px;color:#6E6E73;">${stageDesc}</div>
      </div>
      ${sectionHeader(entries.length+' Zendesk'+(entries.length!==1?'s':''))}
      <div style="border:0.5px solid rgba(0,0,0,0.07);border-radius:10px;overflow:hidden;margin-bottom:4px;">
        ${rows||'<div style="padding:12px;font-size:11px;color:#AEAEB2;">Nothing here right now</div>'}
        ${entries.length>0?`<div style="padding:8px 12px;background:#F5F5F7;">
          <div style="display:flex;justify-content:space-between;margin-bottom:${totalPct!==null?'5px':'0'};">
            <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalPlanned>0?'Applied / Planned':'Total applied'}</span>
            <span style="font-size:11px;font-weight:500;color:#1C1C1E;">${totalPlanned>0?totalApplied.toLocaleString()+' / '+totalPlanned.toLocaleString()+' units':totalApplied.toLocaleString()+' units'}</span>
          </div>
          ${totalPct!==null?`<div style="height:3px;background:rgba(0,0,0,0.08);border-radius:2px;overflow:hidden;"><div style="height:100%;width:${Math.min(totalPct,100)}%;background:${pctBarColor};border-radius:2px;"></div></div>`:''}
        </div>`:''}
      </div>
      ${entries.length>0?`<button type="button" data-action="open-detail-modal" data-scope="location" data-loc="${locKey}"
        style="width:100%;margin-top:12px;background:#fff;color:#990033;border:1px solid rgba(153,0,51,0.3);border-radius:9px;padding:9px 12px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;display:flex;align-items:center;justify-content:center;gap:6px;transition:background .15s;"
        onmouseover="this.style.background='rgba(153,0,51,0.05)'"
        onmouseout="this.style.background='#fff'">
        View full detail (POs · SKUs · days)
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>
      </button>`:''}`;
    panel.style.transform='translateX(0)';
    // Wire the "View full detail" button (delegated since panel innerHTML resets)
    const detailLink = panel.querySelector('[data-action="open-detail-modal"]');
    if(detailLink){
      detailLink.addEventListener('click', ()=> openDetailModal({scope:'location', locKey}));
    }
  }

  // ── Ship and airplane SVG icons ──
  function shipIcon(x,y,color){
    // Container ship side profile
    const g=ns('g');
    g.setAttribute('transform',`translate(${x},${y})`);
    // Hull — wide flat bottom, pointed bow on right
    const hull=ns('path');
    hull.setAttribute('d','M-16,5 L-16,2 L-12,2 L12,2 L17,-1 L18,5 Z');
    hull.setAttribute('fill',color); hull.setAttribute('opacity','1');
    // Deck boxes (cargo containers stacked)
    const deck=ns('path');
    deck.setAttribute('d','M-12,2 L-12,-4 L-2,-4 L-2,2 Z');
    deck.setAttribute('fill',color); deck.setAttribute('opacity','0.7');
    // Bridge/superstructure
    const bridge=ns('path');
    bridge.setAttribute('d','M2,2 L2,-7 L8,-7 L8,2 Z');
    bridge.setAttribute('fill',color); bridge.setAttribute('opacity','0.85');
    // Funnel on top of bridge
    const funnel=ns('rect');
    funnel.setAttribute('x','4'); funnel.setAttribute('y','-10');
    funnel.setAttribute('width','3'); funnel.setAttribute('height','4');
    funnel.setAttribute('rx','1');
    funnel.setAttribute('fill',color); funnel.setAttribute('opacity','0.9');
    // Waterline
    const water=ns('line');
    water.setAttribute('x1','-16'); water.setAttribute('y1','5');
    water.setAttribute('x2','18'); water.setAttribute('y2','5');
    water.setAttribute('stroke',color); water.setAttribute('stroke-width','1.5');
    water.setAttribute('opacity','0.5');
    g.appendChild(hull); g.appendChild(deck); g.appendChild(bridge);
    g.appendChild(funnel); g.appendChild(water);
    return g;
  }
  function planeIcon(x,y,color){
    // Clean airplane silhouette — top-down view pointing toward destination
    const g=ns('g');
    g.setAttribute('transform',`translate(${x},${y}) rotate(135)`);
    const plane=ns('path');
    // Standard top-down airplane: fuselage + swept wings + tail
    plane.setAttribute('d','M0,-12 L2,-4 L10,2 L8,4 L2,1 L1,8 L4,10 L3,12 L0,11 L-3,12 L-4,10 L-1,8 L-2,1 L-8,4 L-10,2 L-2,-4 Z');
    plane.setAttribute('fill',color);
    plane.setAttribute('opacity','0.92');
    g.appendChild(plane);
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
    // Map LOCATIONS keys → locationGroups stage keys
    const locToStage = {
      supplier:       'at_supplier',
      vas_facility:   'vas',
      origin_port:    'origin_port',
      sydney_port:    'clearing',
      sydney_airport: 'clearing',
      client_wh:      'last_mile',
    };

    for(const [locKey,loc] of Object.entries(LOCATIONS)){
      const [lx,ly]=project(loc.lon,loc.lat);
      const color=LOC_COLOR[locKey];
      const stageKey=locToStage[locKey]||locKey;
      // For sydney_port and sydney_airport split clearing by freight type
      let entries=[];
      if(locKey==='sydney_port'){
        entries=(locationGroups['clearing']||[]).filter(z=>!z.isAir);
        entries=entries.concat((locationGroups['customs_hold']||[]).filter(z=>!z.isAir));
      } else if(locKey==='sydney_airport'){
        entries=(locationGroups['clearing']||[]).filter(z=>z.isAir);
        entries=entries.concat((locationGroups['customs_hold']||[]).filter(z=>z.isAir));
      } else {
        entries=locationGroups[stageKey]||[];
      }
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

      // Labels on left for Shenzhen cluster + Sydney WH, right for ports/airport
      const labelLeft = ['supplier','vas_facility','origin_port','client_wh'].includes(locKey);
      const dotR = active ? 6 : 4;
      const txtX = labelLeft ? lx - dotR - 18 : lx + dotR + 5;
      const txtAnchor = labelLeft ? 'end' : 'start';
      const labelW = loc.name.length * 5.8 + 8;
      const labelH = active ? 24 : 14;
      // White background pill behind label for readability over map dots
      const labelBg = ns('rect');
      labelBg.setAttribute('x', labelLeft ? txtX - labelW : txtX - 3);
      labelBg.setAttribute('y', ly - 13);
      labelBg.setAttribute('width', labelW + 6);
      labelBg.setAttribute('height', labelH);
      labelBg.setAttribute('rx', '3');
      labelBg.setAttribute('fill', '#FAFAFA');
      labelBg.setAttribute('opacity', '0.82');
      svgEl.appendChild(labelBg);
      const txt=ns('text');
      txt.setAttribute('x',txtX); txt.setAttribute('y',ly-2);
      txt.setAttribute('text-anchor',txtAnchor);
      txt.setAttribute('font-size','9.5'); txt.setAttribute('font-weight','500');
      txt.setAttribute('fill',active?color:'#BCBCBC');
      txt.setAttribute('font-family','-apple-system,sans-serif');
      txt.textContent=loc.name;
      svgEl.appendChild(txt);

      if(active){
        const bdg=ns('text');
        bdg.setAttribute('x',txtX); bdg.setAttribute('y',ly+9);
        bdg.setAttribute('text-anchor',txtAnchor);
        bdg.setAttribute('font-size','8'); bdg.setAttribute('font-weight','500');
        bdg.setAttribute('fill',color); bdg.setAttribute('opacity','0.9');
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
      hit.addEventListener('click',()=>{ openLocationDetail(stageKey,entries,detail); });
      svgEl.appendChild(hit);
    }

    // ── Extra pin: SH VAS Facility (same color as vas, above supplier) ──
    {
      // Position SH VAS Facility relative to Supplier pin — offset up and slightly right
      const [supX,supY] = project(118.00, 26.00);
      const shx = supX + 28;
      const shy = supY - 28;
      console.log('[Map] SH VAS screen pos:',shx,shy,'Supplier was:',supX,supY);
      const shColor = LOC_COLOR['vas_facility']; // same #990033
      const shEntries = locationGroups['vas'] || [];
      const shActive = shEntries.length > 0;
      const shGlow = ns('circle');
      shGlow.setAttribute('cx',shx); shGlow.setAttribute('cy',shy);
      shGlow.setAttribute('r','10'); shGlow.setAttribute('fill',shColor);
      shGlow.setAttribute('opacity',shActive?'0.12':'0.05');
      svgEl.appendChild(shGlow);
      if(shActive){
        for(let ri=0;ri<3;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',shx); ring.setAttribute('cy',shy); ring.setAttribute('r','6');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke',shColor);
          ring.setAttribute('stroke-width','1.5');
          ring.style.animation=`mapSonar 3.5s ease-out infinite ${ri*1.4}s`;
          svgEl.appendChild(ring);
        }
      }
      const shDot=ns('circle');
      shDot.setAttribute('cx',shx); shDot.setAttribute('cy',shy);
      shDot.setAttribute('r','5');
      shDot.setAttribute('fill',shColor);
      shDot.setAttribute('stroke','#fff'); shDot.setAttribute('stroke-width','1.5');
      shDot.setAttribute('opacity','0.5');
      svgEl.appendChild(shDot);
      const shLabelW = 'SH VAS Facility'.length * 5.8 + 8;
      const shLabelBg = ns('rect');
      shLabelBg.setAttribute('x', shx - 23 - shLabelW);
      shLabelBg.setAttribute('y', shy - 13);
      shLabelBg.setAttribute('width', shLabelW + 6);
      shLabelBg.setAttribute('height', shActive ? 24 : 14);
      shLabelBg.setAttribute('rx', '3');
      shLabelBg.setAttribute('fill', '#FAFAFA');
      shLabelBg.setAttribute('opacity', '0.82');
      svgEl.appendChild(shLabelBg);
      const shTxt=ns('text');
      shTxt.setAttribute('x',shx-23); shTxt.setAttribute('y',shy-2);
      shTxt.setAttribute('text-anchor','end');
      shTxt.setAttribute('font-size','9.5'); shTxt.setAttribute('font-weight','500');
      shTxt.setAttribute('fill',shColor);
      shTxt.setAttribute('font-family','-apple-system,sans-serif');
      shTxt.textContent='SH VAS Facility';
      svgEl.appendChild(shTxt);
      if(shActive){
        const shBdg=ns('text');
        shBdg.setAttribute('x',shx-23); shBdg.setAttribute('y',shy+9);
        shBdg.setAttribute('text-anchor','end');
        shBdg.setAttribute('font-size','8'); shBdg.setAttribute('font-weight','500');
        shBdg.setAttribute('fill',shColor); shBdg.setAttribute('opacity','0.8');
        shBdg.setAttribute('font-family','-apple-system,sans-serif');
        shBdg.textContent=shEntries.length+' ZD';
        svgEl.appendChild(shBdg);
      }
      const shHit=ns('circle');
      shHit.setAttribute('cx',shx); shHit.setAttribute('cy',shy);
      shHit.setAttribute('r','18'); shHit.setAttribute('fill','transparent');
      shHit.style.cursor='pointer'; shHit.style.pointerEvents='all';
      shHit.addEventListener('mouseenter',()=>{ tooltip.style.display='block'; tooltip.textContent='SH VAS Facility'+(shActive?` — ${shEntries.length} Zendesk${shEntries.length!==1?'s':''}`:''); });
      shHit.addEventListener('mousemove',e=>{ const r=wrap.getBoundingClientRect(); tooltip.style.left=(e.clientX-r.left+14)+'px'; tooltip.style.top=(e.clientY-r.top-36)+'px'; });
      shHit.addEventListener('mouseleave',()=>{ tooltip.style.display='none'; });
      shHit.addEventListener('click',()=>{ openLocationDetail('vas',shEntries,detail); });
      svgEl.appendChild(shHit);
    }

    // ── Last mile arcs (from location groups) ──
    const lastMileEntries=locationGroups.last_mile||[];
    console.log('[Map:render] lastMileEntries:',lastMileEntries.length,'airLM:',lastMileEntries.filter(z=>z.isAir).map(z=>z.zendesk),'seaLM:',lastMileEntries.filter(z=>!z.isAir).map(z=>z.zendesk).slice(0,3));
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
        lmArc.setAttribute('stroke','#1C1C1E');
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
        arrowPath.setAttribute('stroke','#1C1C1E'); arrowPath.setAttribute('stroke-width','1.5');
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
        travelArc.setAttribute('stroke',AIR_COLOR); travelArc.setAttribute('stroke-width','2');
        travelArc.setAttribute('fill','none');
        travelArc.setAttribute('stroke-opacity',String(arcOpacity*0.65));
        svgEl.appendChild(travelArc);

        // Plane icon at 30%
        const [ix,iy]=arcPoint(ox,oy,dx,dy,0.3,bend);
        svgEl.appendChild(planeIcon(ix,iy,AIR_COLOR));

        // Vessel label tag above dot
        if(vg.vessel&&(!q||isMatch)){
          const tagW=Math.min(vg.vessel.length*5+14,160);
          const tagBg=ns('rect');
          tagBg.setAttribute('x',vx-tagW/2); tagBg.setAttribute('y',vy-22);
          tagBg.setAttribute('width',tagW); tagBg.setAttribute('height','13');
          tagBg.setAttribute('rx','3'); tagBg.setAttribute('fill','#fff');
          tagBg.setAttribute('stroke','rgba(0,0,0,0.15)'); tagBg.setAttribute('stroke-width','0.5');
          tagBg.setAttribute('opacity','0.95');
          svgEl.appendChild(tagBg);
          const tagTxt=ns('text');
          tagTxt.setAttribute('x',vx); tagTxt.setAttribute('y',vy-12);
          tagTxt.setAttribute('text-anchor','middle');
          tagTxt.setAttribute('font-size','8'); tagTxt.setAttribute('font-weight','500');
          tagTxt.setAttribute('fill','#1C1C1E');
          tagTxt.setAttribute('font-family','-apple-system,sans-serif');
          tagTxt.textContent=vg.vessel.length>24?vg.vessel.slice(0,22)+'\u2026':vg.vessel;
          svgEl.appendChild(tagTxt);
        }

        // Pulse rings + dot at vessel tip
        for(let ri=0;ri<2;ri++){
          const ring=ns('circle');
          ring.setAttribute('cx',vx); ring.setAttribute('cy',vy); ring.setAttribute('r','5');
          ring.setAttribute('fill','none'); ring.setAttribute('stroke',AIR_COLOR);
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
        dot.setAttribute('r','5.5'); dot.setAttribute('fill',AIR_COLOR);
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
          tagBg.setAttribute('stroke','rgba(0,0,0,0.15)'); tagBg.setAttribute('stroke-width','0.5');
          tagBg.setAttribute('opacity','0.95');
          svgEl.appendChild(tagBg);
          const tagTxt=ns('text');
          tagTxt.setAttribute('x',vx); tagTxt.setAttribute('y',vy-12);
          tagTxt.setAttribute('text-anchor','middle');
          tagTxt.setAttribute('font-size','8'); tagTxt.setAttribute('font-weight','500');
          tagTxt.setAttribute('fill','#1C1C1E');
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
      <div style="font-size:15px;font-weight:500;color:#1C1C1E;letter-spacing:-0.01em;">Live Tracking</div>
      <div style="width:0.5px;height:20px;background:rgba(0,0,0,0.08);"></div>
      <input id="map-search" type="text" placeholder="Search vessel, Zendesk, HBL..." style="border:1px solid rgba(153,0,51,0.3);border-radius:8px;padding:6px 12px;font-size:12px;width:560px;outline:none;font-family:inherit;color:#1C1C1E;background:#fff;transition:border-color .15s;" onfocus="this.style.borderColor='#990033'" onblur="this.style.borderColor='rgba(153,0,51,0.3)'"/>
    </div>
    <div style="display:flex;align-items:center;gap:16px;">
      <div id="map-counter" style="font-size:11px;color:#6E6E73;"></div>
      <button id="map-detail-btn" type="button"
              style="font-size:11px;font-weight:500;color:#990033;background:#fff;border:1px solid rgba(153,0,51,0.3);border-radius:8px;padding:6px 12px;cursor:pointer;font-family:inherit;display:flex;align-items:center;gap:6px;transition:background .15s;"
              onmouseover="this.style.background='rgba(153,0,51,0.05)'"
              onmouseout="this.style.background='#fff'">
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>
        Detail view
      </button>
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
    ctx.fillStyle='#C2C2C2';
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

    // ── appliedByPO: use window.state.records + fetch older weeks ──
    // state.records only covers current week — fetch all 16 weeks for complete picture
    const appliedByPO=new Map();
    const stateRecords=Array.isArray(window.state?.records)?window.state.records:[];
    for(const r of stateRecords){
      const po=String(r.po||r.po_number||'').trim().toUpperCase();
      if(po) appliedByPO.set(po,(appliedByPO.get(po)||0)+Number(r.units||0));
    }
    // Also fetch summary for older weeks to catch POs applied in previous weeks
    try {
      const weDate=new Date(weekStart+'T00:00:00'); weDate.setDate(weDate.getDate()+6);
      const oldFrom=new Date(weekStart+'T00:00:00'); oldFrom.setDate(oldFrom.getDate()-5*7);
      const summaryOld=await api(`/records/summary?from=${oldFrom.toISOString().slice(0,10)}&to=${weDate.toISOString().slice(0,10)}&status=complete`);
      for(const r of (summaryOld?.by_po||[])){
        const po=String(r.po||r.po_number||'').trim().toUpperCase();
        if(po && !appliedByPO.has(po)) appliedByPO.set(po,Number(r.units||0));
      }
    } catch(e){ console.warn('[Map] extended summary fetch failed',e); }
    console.log('[Map] appliedByPO — entries:',appliedByPO.size,'sample:',Array.from(appliedByPO.entries()).slice(0,3));

    // ── receiving: use window.state.receiving ──
    const receiving=Array.isArray(window.state?.receiving)?window.state.receiving:[];
    console.log('[Map] receiving from state.receiving — rows:',receiving.length,'sample:',receiving.slice(0,2));

    // ── Fetch 5 weeks of data (current + 4 prior = ~35 days, covers full cycle) ──
    const weeks=[];
    for(let i=0;i<5;i++){
      const d=new Date(weekStart+'T00:00:00'); d.setDate(d.getDate()-i*7);
      weeks.push(d.toISOString().slice(0,10));
    }

    const [flowResults, planResults] = await Promise.all([
      Promise.allSettled(weeks.map(ws=>api(`/flow/week/${encodeURIComponent(ws)}/all`))),
      Promise.allSettled(weeks.map(ws=>api(`/plan/weeks/${encodeURIComponent(ws)}`)))
    ]);

    // Merge plan rows from all weeks — tag each with its week_start
    const allPlanRows=[];
    const seenPlanKeys=new Set();
    // Start with current state.plan (week 0 = current week)
    for(const p of (Array.isArray(window.state?.plan)?window.state.plan:[])){
      const key=String(p.po_number||'').trim()+'|'+String(p.sku_code||'').trim()+'|'+String(p.zendesk_ticket||'').trim();
      if(!seenPlanKeys.has(key)){ seenPlanKeys.add(key); allPlanRows.push({...p, _week_start: weeks[0]}); }
    }
    // Add from older weeks — keep latest week per plan key
    planResults.forEach((res, i) => {
      if(res.status!=='fulfilled') return;
      const rows=Array.isArray(res.value)?res.value:[];
      for(const p of rows){
        const key=String(p.po_number||'').trim()+'|'+String(p.sku_code||'').trim()+'|'+String(p.zendesk_ticket||'').trim();
        if(!seenPlanKeys.has(key)){ seenPlanKeys.add(key); allPlanRows.push({...p, _week_start: weeks[i]}); }
      }
    });
    console.log('[Map] allPlanRows:',allPlanRows.length,'from',weeks.length,'weeks');
    const planRows=allPlanRows;
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

    // Deduplicate lanes by zendesk — same zendesk may appear in multiple weeks
    // Keep the version with the most milestone dates (most advanced state)
    const laneByZD = new Map();
    for(const lane of allLanes){
      const zd = lane.zendesk;
      if(!zd) continue;
      const m = lane.manual||{};
      const existing = laneByZD.get(zd);
      if(!existing){
        laneByZD.set(zd, lane);
      } else {
        // Score by number of milestone dates present — more dates = more advanced
        const score = (l) => [getDeparted(l.manual||{}),getArrived(l.manual||{}),getDestClr(l.manual||{}),getPackingReady(l.manual||{})].filter(Boolean).length;
        if(score(lane) > score(existing)) laneByZD.set(zd, lane);
      }
    }
    const dedupedLanes = Array.from(laneByZD.values());
    console.log('[Map] dedupedLanes:',dedupedLanes.length,'(from',allLanes.length,'raw)');
    console.log('[Map] plan sample zendesk fields:',planRows.slice(0,3).map(p=>p.zendesk_ticket));
    console.log('[Map] lane sample zendesks:',dedupedLanes.slice(0,3).map(l=>l.zendesk));

    return {lanes:dedupedLanes,containers:allContainers,plan:planRows,receiving,appliedByPO};
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
      // Stash for the Detail modal — it needs plan rows (for PO/SKU) and the
      // built location/vessel groups (to derive stage + days-at-stage).
      _modalCache.mapData = mapData;
      _modalCache.planRows = raw.plan || [];
    }catch(e){ console.error('[Map] data failed',e); }

    if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,'');

    if(searchInput){
      searchInput.addEventListener('input',function(){
        if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,this.value);
      });
    }

    // "Detail view" button — opens modal unscoped
    const detailBtn = document.getElementById('map-detail-btn');
    if(detailBtn){
      detailBtn.addEventListener('click', ()=> openDetailModal({scope:'all'}));
    }

    let resizeTimer;
    window.addEventListener('resize',()=>{
      clearTimeout(resizeTimer);
      resizeTimer=setTimeout(async()=>{
        try{ const dims=await drawWorldMap(canvas); _proj=dims.project; if(mapData) renderMap(svgEl,mapData.vesselGroups,mapData.locationGroups,searchInput?.value||''); }catch(e){}
      },300);
    });
  }

  // ─────────────────────────────────────────────────────────────────────
  // Live Map Detail Modal
  //
  // A full-screen modal that surfaces per-PO detail (Zendesk, Week, PO, Supplier,
  // SKUs, Freight, Stage, Location, Days at stage, Applied/Planned, HBL).
  //
  // Opens from:
  //   1. "Detail view" button on the map top bar (scope: all)
  //   2. "View full detail" link in the location side panel (scope: that pin)
  //   3. "View full detail" link in the vessel side panel (scope: that vessel's ZDs)
  //
  // Data sources (read from _modalCache, populated during initMap):
  //   - mapData.locationGroups, mapData.vesselGroups → per-Zendesk stage info
  //   - planRows → PO + SKU detail (one plan row = one PO×SKU×Zendesk)
  // ─────────────────────────────────────────────────────────────────────
  const _modalCache = { mapData: null, planRows: [] };

  // Map stage key → field that holds the "entered this stage" date.
  // 'at_supplier' has no real signal (it's the default), so we return null
  // and show "—" in the table. Cheap call as agreed.
  function _stageEntryDate(stage, manual){
    if(!manual) return null;
    switch(stage){
      case 'last_mile':    return getDestClr(manual);
      case 'customs_hold': return getArrived(manual); // hold has no own ts; arrived is best proxy
      case 'clearing':     return getArrived(manual);
      case 'transit':      return getDeparted(manual);
      case 'origin_port':  return getPackingReady(manual);
      case 'vas':          return null; // computed from earliest received_at per PO outside
      case 'at_supplier':  return null; // no signal — show "—"
      default:             return null;
    }
  }

  function _daysSince(isoLike){
    if(!isoLike) return null;
    try{
      const d = new Date(String(isoLike).slice(0,10) + 'T00:00:00Z');
      if(Number.isNaN(d.getTime())) return null;
      const diff = Date.now() - d.getTime();
      const days = Math.floor(diff / (1000*60*60*24));
      return days >= 0 ? days : null;
    }catch{ return null; }
  }

  function _daysColor(days){
    if(days==null) return '#AEAEB2';
    if(days <= 3) return '#22C55E';
    if(days <= 7) return '#F59E0B';
    return '#DC2626';
  }

  function _stageLabel(stage){ return STAGE_LABEL[stage] || stage || '—'; }

  // Build the flat per-PO row list from cache, filtered by scope.
  // scope = {scope:'all'}                            → every row
  //       = {scope:'location', locKey:'vas'}         → POs at that pin
  //       = {scope:'vessel', zendesks:[...]}         → POs on that vessel
  function _buildDetailRows(scope){
    const md = _modalCache.mapData;
    const planRows = _modalCache.planRows || [];
    if(!md) return [];

    // Map zendesk → {stage, manual, applied, planned, hbl, freight, supplier, weekLabel, isAir}
    // Built from both locationGroups (static pins) and vesselGroups (in-transit).
    const zdInfo = new Map();
    for(const stage of Object.keys(md.locationGroups || {})){
      for(const z of md.locationGroups[stage] || []){
        zdInfo.set(String(z.zendesk).trim(), {
          stage,
          manual: z.manual || {},
          applied: z.applied || 0,
          planned: z.planned || 0,
          hbl: z.hbl || '',
          freight: z.isAir ? 'Air' : (z.freight || 'Sea'),
          supplier: z.supplier || '',
          weekLabel: z.weekLabel || '',
        });
      }
    }
    for(const vg of (md.vesselGroups || [])){
      for(const z of vg.zdentrys || []){
        zdInfo.set(String(z.zendesk).trim(), {
          stage: 'transit',
          manual: z.manual || {},
          applied: z.applied || 0,
          planned: z.planned || 0,
          hbl: z.hbl || '',
          freight: z.isAir ? 'Air' : (z.freight || 'Sea'),
          supplier: z.supplier || '',
          weekLabel: z.weekLabel || '',
          vessel: vg.vessel || '',
        });
      }
    }

    // Scope filter on zendesks
    let allowedZDs = null;
    if(scope.scope === 'location'){
      const stage = scope.locKey; // already a stage key like 'vas','customs_hold' etc.
      allowedZDs = new Set((md.locationGroups[stage] || []).map(z => String(z.zendesk).trim()));
    } else if(scope.scope === 'vessel'){
      allowedZDs = new Set((scope.zendesks || []).map(z => String(z).trim()));
    }
    // scope.scope === 'all' → allowedZDs stays null (no filter)

    // Group plan rows by (zendesk_ticket, po_number) — one PO can have many SKUs.
    // Each group becomes one table row, with SKUs rolled up.
    const poGroups = new Map(); // key: zd|po → {zd, po, supplier, freight, weekStart, plannedSum, skus[]}
    for(const p of planRows){
      const zd = String(p.zendesk_ticket || '').trim();
      const po = String(p.po_number || '').trim();
      if(!zd || !po) continue;
      if(allowedZDs && !allowedZDs.has(zd)) continue;
      // We only include zendesks that the map actually knows about — otherwise
      // we have no stage info to display. This also strips delivered ZDs since
      // they were filtered out by getLaneStage.
      if(!zdInfo.has(zd)) continue;
      const k = zd + '|' + po;
      let g = poGroups.get(k);
      if(!g){
        g = {
          zd, po,
          supplier: String(p.supplier_name || '').trim() || zdInfo.get(zd).supplier,
          freight: String(p.freight_type || '').trim() || zdInfo.get(zd).freight,
          weekStart: String(p._week_start || '').trim() || zdInfo.get(zd).weekLabel || '',
          plannedSum: 0,
          skus: [],
        };
        poGroups.set(k, g);
      }
      g.plannedSum += Number(p.target_qty || 0) || 0;
      const sku = String(p.sku_code || '').trim();
      if(sku && !g.skus.includes(sku)) g.skus.push(sku);
    }

    // Build the final flat rows
    const rows = [];
    for(const g of poGroups.values()){
      const info = zdInfo.get(g.zd);
      const entryDate = _stageEntryDate(info.stage, info.manual);
      const days = entryDate ? _daysSince(entryDate) : null;
      rows.push({
        zendesk: g.zd,
        weekStart: g.weekStart,
        po: g.po,
        supplier: g.supplier,
        skus: g.skus,
        skuCount: g.skus.length,
        freight: g.freight,
        stage: info.stage,
        stageLabel: _stageLabel(info.stage),
        days,                      // null if at_supplier or stage has no date
        applied: info.applied,
        planned: info.planned || g.plannedSum,
        hbl: info.hbl,
        vessel: info.vessel || '',
      });
    }
    return rows;
  }

  // ─── Modal state (per-instance) ───
  const _modalState = {
    rows: [],
    sort: { col: 'days', dir: 'desc' },
    filters: { stage:'', freight:'', supplier:'', week:'', q:'' },
    scope: null,
  };

  function _applyFiltersAndSort(){
    const f = _modalState.filters;
    let rows = _modalState.rows.filter(r => {
      if(f.stage && r.stage !== f.stage) return false;
      if(f.freight && r.freight !== f.freight) return false;
      if(f.supplier && r.supplier !== f.supplier) return false;
      if(f.week && r.weekStart !== f.week) return false;
      if(f.q){
        const q = f.q.toLowerCase();
        const hay = (r.zendesk + ' ' + r.po + ' ' + r.hbl + ' ' + r.skus.join(' ')).toLowerCase();
        if(!hay.includes(q)) return false;
      }
      return true;
    });
    const {col, dir} = _modalState.sort;
    const mul = dir === 'asc' ? 1 : -1;
    rows.sort((a,b)=>{
      let av = a[col], bv = b[col];
      // Nulls last regardless of direction
      if(av==null && bv==null) return 0;
      if(av==null) return 1;
      if(bv==null) return -1;
      if(typeof av === 'number' && typeof bv === 'number') return (av-bv)*mul;
      return String(av).localeCompare(String(bv)) * mul;
    });
    return rows;
  }

  function _renderModalTable(){
    const tbody = document.getElementById('mdm-tbody');
    const countEl = document.getElementById('mdm-count');
    if(!tbody) return;
    const rows = _applyFiltersAndSort();
    if(countEl){
      const zdCount = new Set(rows.map(r=>r.zendesk)).size;
      countEl.textContent = `Showing ${rows.length} PO${rows.length!==1?'s':''} across ${zdCount} Zendesk${zdCount!==1?'s':''}`;
    }
    if(!rows.length){
      tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:32px;color:#AEAEB2;font-size:12px;">No shipments match these filters.</td></tr>`;
      return;
    }
    const esc = s => String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    tbody.innerHTML = rows.map(r => {
      const skuCell = r.skus.length === 0
        ? '<span style="color:#AEAEB2;">—</span>'
        : r.skus.length === 1
          ? esc(r.skus[0])
          : `<span title="${esc(r.skus.join(', '))}" style="cursor:help;border-bottom:1px dotted #AEAEB2;">${r.skus.length} SKUs</span>`;
      const daysCell = r.days == null
        ? '<span style="color:#AEAEB2;">—</span>'
        : `<span style="color:${_daysColor(r.days)};font-weight:500;">${r.days}d</span>`;
      const appliedPlanned = r.planned > 0
        ? `${r.applied.toLocaleString()} / ${r.planned.toLocaleString()}`
        : `${r.applied.toLocaleString()}`;
      const stageColor = STAGE_COLOR[r.stage] || '#6E6E73';
      return `<tr style="border-top:0.5px solid rgba(0,0,0,0.06);">
        <td style="padding:8px 10px;font-size:11px;font-weight:500;color:#1C1C1E;">#${esc(r.zendesk)}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${esc(r.weekStart || '—')}</td>
        <td style="padding:8px 10px;font-size:11px;color:#1C1C1E;font-family:ui-monospace,Menlo,monospace;">${esc(r.po)}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${esc(r.supplier)}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${skuCell}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${esc(r.freight)}</td>
        <td style="padding:8px 10px;font-size:11px;">
          <span style="display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:5px;background:${stageColor}18;color:${stageColor};font-weight:500;">
            <span style="width:5px;height:5px;border-radius:50%;background:${stageColor};"></span>${esc(r.stageLabel)}
          </span>
        </td>
        <td style="padding:8px 10px;font-size:11px;text-align:right;">${daysCell}</td>
        <td style="padding:8px 10px;font-size:11px;color:#1C1C1E;text-align:right;tabular-nums:auto;">${esc(appliedPlanned)}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${esc(r.hbl || '—')}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6E6E73;">${esc(r.vessel || '—')}</td>
      </tr>`;
    }).join('');
  }

  // Build the list of options for a select dropdown from current rows (unfiltered)
  function _uniqueValues(rows, key){
    const s = new Set();
    for(const r of rows){
      const v = r[key];
      if(v == null || v === '') continue;
      s.add(v);
    }
    return Array.from(s).sort();
  }

  function _exportXlsx(){
    if(typeof window.XLSX === 'undefined'){
      alert('Excel export library not loaded. Please refresh the page.');
      return;
    }
    // Export honours current filters but goes to SKU grain (more useful in Excel).
    const rows = _applyFiltersAndSort();
    const sheetData = [];
    for(const r of rows){
      const skus = r.skus.length ? r.skus : [''];
      for(const sku of skus){
        sheetData.push({
          Zendesk: r.zendesk,
          'Week Start': r.weekStart || '',
          PO: r.po,
          SKU: sku,
          Supplier: r.supplier || '',
          Freight: r.freight || '',
          Stage: r.stageLabel,
          'Days at stage': r.days == null ? '' : r.days,
          Applied: r.applied || 0,
          Planned: r.planned || 0,
          HBL: r.hbl || '',
          Vessel: r.vessel || '',
        });
      }
    }
    if(!sheetData.length){
      alert('Nothing to export with current filters.');
      return;
    }
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(sheetData);
    // Sensible default column widths
    ws['!cols'] = [
      {wch:10}, {wch:12}, {wch:14}, {wch:14}, {wch:30}, {wch:8},
      {wch:18}, {wch:14}, {wch:10}, {wch:10}, {wch:16}, {wch:18},
    ];
    XLSX.utils.book_append_sheet(wb, ws, 'Live Shipments');
    const now = new Date();
    const stamp = now.toISOString().slice(0,10);
    const scopeName = _modalState.scope?.scope === 'location'
      ? '_' + (_modalState.scope.locKey || 'loc')
      : _modalState.scope?.scope === 'vessel'
        ? '_vessel'
        : '';
    XLSX.writeFile(wb, `live_shipments${scopeName}_${stamp}.xlsx`);
  }

  function openDetailModal(scope){
    // scope: {scope:'all'} | {scope:'location', locKey} | {scope:'vessel', zendesks, vessel}
    if(!_modalCache.mapData){
      // Probably still loading — be quiet, not loud.
      console.warn('[Map] detail modal: data not ready yet');
      return;
    }
    // Drop any existing modal first
    const existing = document.getElementById('map-detail-modal');
    if(existing) existing.remove();

    _modalState.rows = _buildDetailRows(scope);
    _modalState.scope = scope;
    _modalState.filters = { stage:'', freight:'', supplier:'', week:'', q:'' };
    _modalState.sort = { col: 'days', dir: 'desc' };

    const stages   = _uniqueValues(_modalState.rows, 'stage');
    const freights = _uniqueValues(_modalState.rows, 'freight');
    const suppliers = _uniqueValues(_modalState.rows, 'supplier');
    const weeks    = _uniqueValues(_modalState.rows, 'weekStart');

    const title = scope.scope === 'location'
      ? `Live Shipments — ${STAGE_LABEL[scope.locKey] || 'Location'}`
      : scope.scope === 'vessel'
        ? `Live Shipments — ${scope.vessel || 'Vessel'}`
        : 'Live Shipments — All';

    const overlay = document.createElement('div');
    overlay.id = 'map-detail-modal';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center;font-family:inherit;';

    const stageOpts = `<option value="">All stages</option>` +
      stages.map(s => `<option value="${s}">${STAGE_LABEL[s]||s}</option>`).join('');
    const freightOpts = `<option value="">All freight</option>` +
      freights.map(s => `<option value="${s}">${s}</option>`).join('');
    const supplierOpts = `<option value="">All suppliers</option>` +
      suppliers.map(s => `<option value="${s.replace(/"/g,'&quot;')}">${s}</option>`).join('');
    const weekOpts = `<option value="">All weeks</option>` +
      weeks.map(s => `<option value="${s}">${s}</option>`).join('');

    const headerCell = (col, label, align) => {
      const isActive = _modalState.sort.col === col;
      const arrow = isActive ? (_modalState.sort.dir === 'asc' ? ' ↑' : ' ↓') : '';
      const algn = align === 'right' ? 'text-align:right;' : '';
      return `<th data-sort-col="${col}" style="padding:10px;font-size:10px;font-weight:600;color:#6E6E73;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;user-select:none;background:#FAFAFA;border-bottom:0.5px solid rgba(0,0,0,0.08);${algn}">${label}${arrow}</th>`;
    };

    overlay.innerHTML = `
      <div style="background:#fff;border-radius:16px;width:min(95vw,1280px);max-height:90vh;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,0.2);overflow:hidden;">
        <!-- Header -->
        <div style="padding:20px 24px;border-bottom:0.5px solid rgba(0,0,0,0.08);display:flex;align-items:center;justify-content:space-between;gap:16px;flex-shrink:0;">
          <div>
            <div style="font-size:15px;font-weight:600;color:#1C1C1E;">${title}</div>
            <div id="mdm-count" style="font-size:11px;color:#6E6E73;margin-top:2px;">Loading…</div>
          </div>
          <div style="display:flex;align-items:center;gap:8px;">
            <button id="mdm-export" type="button" style="display:flex;align-items:center;gap:6px;background:#990033;color:#fff;border:none;border-radius:9px;padding:9px 16px;font-size:12px;font-weight:500;cursor:pointer;font-family:inherit;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
              Download XLSX
            </button>
            <button id="mdm-close" type="button" style="width:32px;height:32px;border-radius:8px;background:#F5F5F7;border:0.5px solid rgba(0,0,0,0.08);cursor:pointer;font-size:14px;color:#6E6E73;display:flex;align-items:center;justify-content:center;font-family:inherit;">✕</button>
          </div>
        </div>

        <!-- Filters -->
        <div style="padding:14px 24px;border-bottom:0.5px solid rgba(0,0,0,0.06);display:flex;align-items:center;gap:10px;flex-wrap:wrap;flex-shrink:0;background:#FCFCFD;">
          <select id="mdm-f-stage" style="font-size:12px;padding:7px 10px;border:0.5px solid rgba(0,0,0,0.15);border-radius:7px;font-family:inherit;outline:none;background:#fff;">${stageOpts}</select>
          <select id="mdm-f-freight" style="font-size:12px;padding:7px 10px;border:0.5px solid rgba(0,0,0,0.15);border-radius:7px;font-family:inherit;outline:none;background:#fff;">${freightOpts}</select>
          <select id="mdm-f-supplier" style="font-size:12px;padding:7px 10px;border:0.5px solid rgba(0,0,0,0.15);border-radius:7px;font-family:inherit;outline:none;background:#fff;max-width:240px;">${supplierOpts}</select>
          <select id="mdm-f-week" style="font-size:12px;padding:7px 10px;border:0.5px solid rgba(0,0,0,0.15);border-radius:7px;font-family:inherit;outline:none;background:#fff;">${weekOpts}</select>
          <input id="mdm-f-q" type="text" placeholder="Search Zendesk / PO / SKU / HBL…" style="flex:1;min-width:200px;font-size:12px;padding:7px 10px;border:0.5px solid rgba(0,0,0,0.15);border-radius:7px;font-family:inherit;outline:none;background:#fff;"/>
          <button id="mdm-clear" type="button" style="font-size:11px;color:#6E6E73;background:transparent;border:none;cursor:pointer;font-family:inherit;padding:7px 8px;">Clear</button>
        </div>

        <!-- Table -->
        <div style="flex:1;overflow:auto;">
          <table style="width:100%;border-collapse:collapse;">
            <thead style="position:sticky;top:0;z-index:1;">
              <tr>
                ${headerCell('zendesk','Zendesk')}
                ${headerCell('weekStart','Week')}
                ${headerCell('po','PO')}
                ${headerCell('supplier','Supplier')}
                ${headerCell('skuCount','SKUs')}
                ${headerCell('freight','Freight')}
                ${headerCell('stageLabel','Stage')}
                ${headerCell('days','Days', 'right')}
                ${headerCell('applied','Applied / Planned', 'right')}
                ${headerCell('hbl','HBL')}
                ${headerCell('vessel','Vessel')}
              </tr>
            </thead>
            <tbody id="mdm-tbody"></tbody>
          </table>
        </div>

        <!-- Footer -->
        <div style="padding:10px 24px;border-top:0.5px solid rgba(0,0,0,0.06);font-size:10px;color:#AEAEB2;flex-shrink:0;background:#FAFAFA;">
          Days at stage colour: <span style="color:#22C55E;">green ≤3d</span> · <span style="color:#F59E0B;">amber 4–7d</span> · <span style="color:#DC2626;">red &gt;7d</span> · At Supplier shows "—" (no event date)
        </div>
      </div>
    `;

    document.body.appendChild(overlay);

    // Wire close (✕, backdrop click, Escape)
    const closeFn = ()=>{
      overlay.remove();
      document.removeEventListener('keydown', escFn);
    };
    const escFn = e => { if(e.key === 'Escape') closeFn(); };
    document.addEventListener('keydown', escFn);
    overlay.querySelector('#mdm-close').addEventListener('click', closeFn);
    overlay.addEventListener('click', e => { if(e.target === overlay) closeFn(); });

    // Wire filters
    const f = overlay.querySelector('#mdm-f-stage');
    f.addEventListener('change', ()=>{ _modalState.filters.stage = f.value; _renderModalTable(); });
    const fF = overlay.querySelector('#mdm-f-freight');
    fF.addEventListener('change', ()=>{ _modalState.filters.freight = fF.value; _renderModalTable(); });
    const fS = overlay.querySelector('#mdm-f-supplier');
    fS.addEventListener('change', ()=>{ _modalState.filters.supplier = fS.value; _renderModalTable(); });
    const fW = overlay.querySelector('#mdm-f-week');
    fW.addEventListener('change', ()=>{ _modalState.filters.week = fW.value; _renderModalTable(); });
    const fQ = overlay.querySelector('#mdm-f-q');
    let qTimer;
    fQ.addEventListener('input', ()=>{
      clearTimeout(qTimer);
      qTimer = setTimeout(()=>{ _modalState.filters.q = fQ.value.trim(); _renderModalTable(); }, 120);
    });
    overlay.querySelector('#mdm-clear').addEventListener('click', ()=>{
      _modalState.filters = { stage:'', freight:'', supplier:'', week:'', q:'' };
      f.value = ''; fF.value = ''; fS.value = ''; fW.value = ''; fQ.value = '';
      _renderModalTable();
    });

    // Wire sort headers (click to toggle direction; click new column to switch)
    overlay.querySelectorAll('[data-sort-col]').forEach(th => {
      th.addEventListener('click', ()=>{
        const col = th.getAttribute('data-sort-col');
        if(_modalState.sort.col === col){
          _modalState.sort.dir = _modalState.sort.dir === 'asc' ? 'desc' : 'asc';
        } else {
          _modalState.sort.col = col;
          _modalState.sort.dir = (col === 'days' || col === 'applied') ? 'desc' : 'asc';
        }
        // Re-render arrows by rebuilding the header row — easiest path
        overlay.querySelectorAll('[data-sort-col]').forEach(t => {
          const c = t.getAttribute('data-sort-col');
          const arrow = c === _modalState.sort.col
            ? (_modalState.sort.dir === 'asc' ? ' ↑' : ' ↓') : '';
          // Strip any existing arrow then re-add
          t.innerHTML = t.innerHTML.replace(/\s*[↑↓]\s*$/,'') + arrow;
        });
        _renderModalTable();
      });
    });

    // Wire XLSX export
    overlay.querySelector('#mdm-export').addEventListener('click', _exportXlsx);

    _renderModalTable();
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
