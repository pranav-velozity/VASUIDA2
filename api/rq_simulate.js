/* ── VelOzity Pinpoint — Door-to-door simulation engine (Phase 5c) ──
   Turns a request's released options into points on price × days, each with the chance of
   being in the Sydney FC by the client's date. Deterministic: percentiles from the signals
   tables, no sampling, so the same inputs always give the same board.

   A leg's timeline, stage by stage (the same stages Transit Movements plans with):
     packing list   = period Monday + rule offset, + the supplier's measured packing-list slip
     origin cleared = packing + rule gap
     departed       = the option's sailing/flight; a PO whose cleared date passes the cut-off
                      rolls to the next departure (+7 d sea, +1 d air)
     arrived        = departed + the carrier's quoted transit + the measured slip on that lane
                      (carrier grain when it has ≥5 movements, else mode grain)
     dest cleared   = arrived + rule gap + measured stage slip
     FC receipt     = cleared + rule gap + measured stage slip
   P50 and P90 are carried through the chain separately. The chance of making a date treats
   the FC day as normal(P50, (P90−P50)/1.2816) — explainable, and honest about thin history.

   Volume: units per supplier × recent fill rate when that supplier's adjustment is on.
   Containers from CBM (client-given, else an assumed CBM per unit — flagged); chargeable kg
   from kg / CBM (else assumed kg per unit — flagged). Prices are the released client prices
   only; cost and margin never enter here.

   Splits: for each sea × air pair, POs whose sea leg misses the need-by date at P50 go air,
   the rest sea; the client can flip any PO. When no PO misses, or every PO does, the split
   collapses into the single option and is not listed twice. */
module.exports = function ({ db, rulesFor, vasDaysFor, log }) {
  const L = log || console;
  const J = (s, d) => { try { return s ? JSON.parse(s) : d; } catch (_) { return d; } };
  const r1 = (n) => Math.round(Number(n) * 10) / 10, r2 = (n) => Math.round(Number(n) * 100) / 100;
  const ymd = (d) => d.toISOString().slice(0, 10);
  const addDays = (s, n) => { const d = new Date(s + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + Math.round(n)); return ymd(d); };
  const daysBetween = (a, b) => Math.round((new Date(b + 'T00:00:00Z') - new Date(a + 'T00:00:00Z')) / 86400000);
  const mondayOf = (s) => { const d = new Date(s + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7)); return ymd(d); };
  const today = () => ymd(new Date());
  const supplierKey = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const dmy = (s) => { if (!s) return '—'; const d = new Date(s + 'T00:00:00Z'); return d.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' }); };
  // Standard normal CDF via erf (Abramowitz–Stegun 7.1.26, |error| < 1.5e-7).
  const erf = (x) => { const t = 1 / (1 + 0.3275911 * Math.abs(x)); const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x); return x >= 0 ? y : -y; };
  const Phi = (z) => 0.5 * (1 + erf(z / Math.SQRT2));

  const CAP_CBM = { '20': 28, '40': 58, '40HC': 68 };          // usable CBM per container, loaded as ICONIC cartons load
  const ASSUME = { cbm_per_unit: 0.0024, kg_per_unit: 0.26 };  // only when the request carries neither CBM nor kg — flagged in the output

  function signals(client) {
    const sup = new Map(); try { db.prepare('SELECT * FROM signal_supplier WHERE client_id = ?').all(client).forEach(s => sup.set(supplierKey(s.supplier), s)); } catch (_) {}
    const lanes = { mode: new Map(), carrier: new Map() };
    try { db.prepare("SELECT * FROM signal_lane WHERE client_id = ? AND grain IN ('mode','carrier')").all(client).forEach(l => { l.stage = J(l.stage_json, {}); if (l.grain === 'mode') lanes.mode.set(l.mode, l); else lanes.carrier.set(l.mode + '|' + String(l.carrier || '').toLowerCase(), l); }); } catch (_) {}
    return { sup, lanes };
  }
  const laneFor = (sig, mode, carrier) => { const c = carrier && sig.lanes.carrier.get(mode + '|' + String(carrier).toLowerCase()); return (c && c.n >= 5) ? c : (sig.lanes.mode.get(mode) || null); };
  const slip = (lane, stage, q) => { const s = lane && lane.stage && lane.stage[stage]; const v = s ? (q === 90 ? s.slip_p90 : s.slip_p50) : null; return v == null ? 0 : Math.max(0, Number(v)); };

  function rulesOf(client, facility, mode) {
    const r = rulesFor ? rulesFor(client, facility || '', mode) : null;
    if (mode === 'Air') { const bd = (r && r.before_departure) || { cleared: 1, packing: 2 }; return { mode, packingBefore: Number(bd.packing) || 2, clearedBefore: Number(bd.cleared) || 1, arrDest: r ? Number(r.arrived_to_dest_cleared_days) : 1, destFc: r ? Number(r.dest_cleared_to_fc_days) : 1, transit: r && r.default_transit_days != null ? Number(r.default_transit_days) : 2 }; }
    return { mode, packing: r ? Number(r.packing_list_offset_days) : 4, cleared: r ? Number(r.origin_cleared_offset_days) : 7, departed: r ? Number(r.departed_offset_days) : 11, arrDest: r ? Number(r.arrived_to_dest_cleared_days) : 2, destFc: r ? Number(r.dest_cleared_to_fc_days) : 4, transit: r && r.default_transit_days != null ? Number(r.default_transit_days) : 20 };
  }

  // One PO on one option: dates at P50 and P90, whether it rolled to a later departure.
  function legForPo(po, opt, rule, lane, sigSup, adjustOn, q) {
    const ws = po.week_start || opt.__week; const sup = sigSup.get(supplierKey(po.supplier));
    const readySlip = adjustOn && sup ? Math.max(0, Number(q === 90 ? (sup.ready_late_p90 != null ? sup.ready_late_p90 : sup.ready_late_p50) : sup.ready_late_p50) || 0) : 0;
    let departs = opt.departs || (rule.mode === 'Sea' ? addDays(ws, rule.departed) : addDays(ws, 9));
    let packing, cleared, rolled = 0;
    if (rule.mode === 'Sea') { packing = addDays(ws, rule.packing + readySlip); cleared = addDays(ws, rule.cleared + readySlip); const gap = opt.cutoff ? Math.max(0, daysBetween(opt.cutoff, departs)) : 2;   // cut-off this many days before each sailing
      while (cleared > addDays(departs, -gap) && rolled < 8) { departs = addDays(departs, 7); rolled++; } }
    else { packing = addDays(departs, -rule.packingBefore + readySlip); cleared = addDays(departs, -rule.clearedBefore + readySlip); while (cleared > departs && rolled < 14) { departs = addDays(departs, 1); rolled++; } }
    const quoted = opt.transit_days != null ? Number(opt.transit_days) : (lane && lane.transit_p50 != null ? Number(lane.transit_p50) : rule.transit);
    // Transit slip against quoted: measured achieved vs quoted on this lane; without a quoted history, achieved P90−P50 at the 90th.
    let tr = quoted;
    if (lane && lane.transit_p50 != null) { if (lane.quoted_p50 != null) tr = quoted + Math.max(0, (q === 90 ? lane.transit_p90 : lane.transit_p50) - lane.quoted_p50); else if (opt.transit_days == null) tr = q === 90 ? lane.transit_p90 : lane.transit_p50; else if (q === 90 && lane.transit_p90 != null && lane.transit_p50 != null) tr = quoted + Math.max(0, lane.transit_p90 - lane.transit_p50); }
    const arrived = addDays(departs, tr);
    const dcleared = addDays(arrived, rule.arrDest + slip(lane, 'dest_cleared', q));
    const fc = addDays(dcleared, rule.destFc + slip(lane, 'fc_receipt', q));
    return { packing, cleared, departs, arrived, dcleared, fc, rolled, transit: r1(tr), quoted };
  }

  function simulate(client, rq, options, params) {
    const p = params || {}; const vol = J(rq.volume_json, {}); const origins = J(rq.origins_json, []);
    const needBy = p.need_by || rq.need_by || null; const sig = signals(client);
    const vasDays = Math.max(0, Number(vasDaysFor ? vasDaysFor(client) : 4) || 0);   // days of VAS before the packing list — client setting
    const facility = p.facility || vol.facility || null;
    const pos = (vol.pos && vol.pos.length ? vol.pos : [{ po: 'ALL', supplier: (vol.suppliers && vol.suppliers[0] && vol.suppliers[0].supplier) || 'Plan', units: Number(vol.units) || 0, mode: 'Sea', due: null, week_start: mondayOf(rq.period_start) }])
      .map(x => ({ ...x, units: Number(x.units) || 0, week_start: x.week_start || mondayOf(rq.period_start) }));
    const totalUnits = pos.reduce((a, x) => a + x.units, 0) || Number(vol.units) || 0;
    const adjust = Object.assign({}, p.adjust || {});                       // supplierKey → false to switch off
    const adjOn = (s) => adjust[supplierKey(s)] !== false;
    const assumptions = [];
    // Unit profile from the client's own history beats a typed figure that is wildly off it (a CBM typed where cartons were meant),
    // and fills in whatever the request did not carry. Typed figures within 2× of history stand.
    const prof = p.profile || { sea: {}, air: {} }; const profSea = prof.sea || {}, profAir = prof.air || {};
    let cbmTotal = Number(vol.cbm) || 0, kgTotal = Number(vol.kg) || 0;
    const histKg = profAir.kg_per_unit || null, histCbm = profAir.cbm_per_unit || null;
    const off = (typed, hist) => typed && hist && (typed / hist > 2 || hist / typed > 2);
    if (totalUnits && off(cbmTotal / totalUnits, histCbm)) { assumptions.push(`The request says ${r1(cbmTotal)} CBM for ${totalUnits} units (${(cbmTotal / totalUnits).toFixed(4)} CBM/unit); your history runs ${histCbm} CBM/unit (${profAir.basis}) — history used.`); cbmTotal = 0; }
    if (totalUnits && off(kgTotal / totalUnits, histKg)) { assumptions.push(`The request says ${r1(kgTotal)} kg for ${totalUnits} units (${(kgTotal / totalUnits).toFixed(2)} kg/unit); your history runs ${histKg} kg/unit (${profAir.basis}) — history used.`); kgTotal = 0; }
    const cbmPer = cbmTotal ? cbmTotal / (totalUnits || 1) : (histCbm || ASSUME.cbm_per_unit), kgPer = kgTotal ? kgTotal / (totalUnits || 1) : (histKg || ASSUME.kg_per_unit);
    if (!cbmTotal) assumptions.push(histCbm ? `CBM per unit ${histCbm} from your approved air quotes (${profAir.basis}).` : `No CBM on the request and no history — containers assume ${ASSUME.cbm_per_unit} CBM per unit.`);
    if (!kgTotal) assumptions.push(histKg ? `Chargeable weight ${histKg} kg per unit from your approved air quotes (${profAir.quotes_8w || profAir.quotes_26w} quotes, ${profAir.basis}).` : `No weight on the request and no history — air assumes ${ASSUME.kg_per_unit} kg per unit.`);
    const unitsPerBox = (size) => { const e = profSea[size] || profSea['40']; return e && e.units_per_container ? e : null; };
    if (unitsPerBox('40')) { const e = unitsPerBox('40'); assumptions.push(`Containers sized on your history: ${e.units_per_container} units per 40' (${e.basis}, ${e.containers_8w || e.containers_26w} containers${e.units_26w && e.units_8w && e.units_26w !== e.units_8w ? ` · 26-wk ${e.units_26w}, 8-wk ${e.units_8w} — the mix changes with the season` : ''}).`); }
    const cbmOf = (units) => units * cbmPer;
    const kgOf = (units) => { const kg = units * kgPer; const vk = cbmOf(units) * 166.67; return Math.max(kg, vk); };
    const expectedUnits = (po) => { const s = sig.sup.get(supplierKey(po.supplier)); const f = s && adjOn(po.supplier) ? Number(s.fill_rate_recent != null ? s.fill_rate_recent : s.fill_rate) : null; return f != null ? Math.round(po.units * Math.min(1, f / 100)) : po.units; };
    const live = (options || []).filter(o => ['approximate', 'picked', 'confirmed', 'firm', 'approved'].includes(o.state) && o.price != null);
    if (!totalUnits) return { request_id: rq.id, ref: rq.ref, period_label: rq.period_label, period_start: rq.period_start, need_by: needBy, total_units: 0, expected_units: 0, suppliers: [], assumptions, computed_at: new Date().toISOString(), points: [], recommended: null, cheapest: null, fastest: null, recommendation: 'This request has no units — there is nothing to price or time. Raise it again with the units for the period.' };
    const suppliers = [...new Map(pos.map(x => [supplierKey(x.supplier), x.supplier])).entries()].map(([k, name]) => { const s = sig.sup.get(k); return { supplier: name, key: k, units: pos.filter(x => supplierKey(x.supplier) === k).reduce((a, x) => a + x.units, 0), fill_rate: s ? (s.fill_rate_recent != null ? s.fill_rate_recent : s.fill_rate) : null, ready_late_p50: s ? s.ready_late_p50 : null, ready_late_p90: s ? s.ready_late_p90 : null, late_days_p50: s ? s.late_days_p50 : null, confidence: s ? s.confidence : 'none', adjust: adjOn(name) }; });

    // A leg = one option carrying a set of POs.
    function leg(opt, legPos) {
      const rule = rulesOf(client, facility, opt.mode); const lane = laneFor(sig, opt.mode, opt.carrier); opt.__week = mondayOf(rq.period_start);
      const units = legPos.reduce((a, x) => a + expectedUnits(x), 0); const plannedUnits = legPos.reduce((a, x) => a + x.units, 0);
      const perPo = legPos.map(po => { const a = legForPo(po, opt, rule, lane, sig.sup, adjOn(po.supplier), 50), b = legForPo(po, opt, rule, lane, sig.sup, adjOn(po.supplier), 90);
        const target = needBy || po.due || null; let prob = null;
        if (target) { const mu = daysBetween(a.packing, a.fc), sg = Math.max(0.75, (daysBetween(b.packing, b.fc) - mu) / 1.2816); prob = Phi((daysBetween(a.packing, target) - mu) / sg); }
        return { po: po.po, supplier: po.supplier, units: expectedUnits(po), due: po.due || null, p50: a, p90: b, prob: prob == null ? null : Math.round(prob * 100) / 100 }; });
      const fc50 = perPo.map(x => x.p50.fc).sort().slice(-1)[0] || null, fc90 = perPo.map(x => x.p90.fc).sort().slice(-1)[0] || null;
      const departs = perPo.map(x => x.p50.departs).sort()[0] || null; const rolled = perPo.filter(x => x.p50.rolled > 0).length;
      const wUnits = perPo.reduce((a, x) => a + x.units, 0) || 1;
      const prob = perPo.some(x => x.prob != null) ? Math.round(perPo.reduce((a, x) => a + (x.prob == null ? 1 : x.prob) * x.units, 0) / wUnits * 100) : null;
      let qty, price, basis;
      if (opt.mode === 'Sea') { const size = opt.container_size || '40'; const hist = unitsPerBox(size); const cap = CAP_CBM[size] || CAP_CBM['40']; const cbm = cbmOf(units);
        const containers = Math.max(1, hist ? Math.ceil(units / hist.units_per_container) : Math.ceil(cbm / cap));
        qty = { containers, cbm: r1(cbm), size, units_per_container: hist ? hist.units_per_container : null, units_this_fill: Math.round(units / containers), utilisation: hist ? Math.round(units / (containers * hist.units_per_container) * 100) : Math.round(cbm / (containers * cap) * 100), sized_by: hist ? `history · ${hist.units_per_container} units per ${size}' (${hist.basis})` : `${cap} CBM per ${size}'` };
        price = opt.price_basis === 'per_container' ? r2(containers * opt.price) : r2(opt.price * units / (totalUnits || units || 1)); basis = opt.price_basis === 'per_container' ? `${containers} × ${size}' @ ${opt.price}` : 'total'; qty.price_per_container = opt.price_basis === 'per_container' ? opt.price : r2(price / containers); }
      else { const kg = kgOf(units); qty = { chargeable_kg: Math.round(kg), kg_per_unit: Math.round(kg / (units || 1) * 100) / 100 }; price = opt.price_basis === 'per_kg' ? r2(kg * opt.price) : r2(opt.price * units / (totalUnits || units || 1)); basis = opt.price_basis === 'per_kg' ? `${Math.round(kg)} kg × ${opt.price}/kg` : 'total'; qty.price_per_kg = opt.price_basis === 'per_kg' ? opt.price : r2(price / (kg || 1)); }
      // Working back from the need-by date: the latest departure that still makes it (P50 and the safer P90),
      // the packing list that departure needs, and the ex-factory date before VAS. Also what THIS departure needs.
      const tail = (q) => { const l = legPos[0] ? legForPo(legPos[0], opt, rule, lane, sig.sup, false, q) : null; return l ? daysBetween(l.departs, l.fc) : null; };   // departed → FC on this lane at q
      const prep = opt.mode === 'Sea' ? Math.max(0, rule.departed - rule.packing) : rule.packingBefore;                                   // packing list → departure
      const back = (q) => { const t = tail(q); if (!needBy || t == null) return null; const ship = addDays(needBy, -t); const ready = addDays(ship, -prep); return { ship_by: ship, cargo_ready_by: ready, ex_factory_by: addDays(ready, -vasDays), makes_it: departs ? departs <= ship : null }; };
      const dates = { p50: back(50), p90: back(90), this_departure: departs ? { departs, cargo_ready: addDays(departs, -prep), ex_factory: addDays(departs, -prep - vasDays) } : null, vas_days: vasDays, packing_to_departure_days: prep };
      const sample = lane ? { lane: lane.grain === 'carrier' ? `${opt.carrier} · ${lane.n} movements` : `all ${opt.mode.toLowerCase()} · ${lane.n} movements`, achieved_p50: lane.transit_p50, achieved_p90: lane.transit_p90, quoted_history: lane.quoted_p50 } : null;
      const first = perPo[0];
      const explain = [];
      if (first) {
        const late = perPo.filter(x => x.p50.rolled > 0);
        explain.push(`${opt.mode} · ${[opt.carrier, opt.service].filter(Boolean).join(' ') || (opt.kind === 'history' ? 'from history' : 'option')}: departs ${dmy(first.p50.departs)}${late.length ? ` — ${late.length} PO${late.length === 1 ? '' : 's'} (${[...new Set(late.map(x => x.supplier))].join(', ')}) miss the cut-off on their packing-list history and roll to the next departure` : ''}.`);
        explain.push(`Port to port ${first.p50.quoted} d quoted${sample ? `, this lane runs ${sample.achieved_p50}–${sample.achieved_p90} d (${sample.lane})` : ', no measured history on this lane yet'}; arrival → cleared → FC from the lane rules plus measured slip.`);
        explain.push(`In the Sydney FC ${dmy(fc50)} (P50) – ${dmy(fc90)} (P90)${needBy ? ` against your ${dmy(needBy)}: ${prob}% of units on time` : ''}.`);
        if (dates.p50) explain.push(`To make ${dmy(needBy)}: ship by ${dmy(dates.p50.ship_by)} (${dmy(dates.p90.ship_by)} to be safe) → cargo ready by ${dmy(dates.p50.cargo_ready_by)} → ex-factory by ${dmy(dates.p50.ex_factory_by)} with ${vasDays} d of VAS. This departure (${dmy(departs)}) needs cargo ready ${dmy(dates.this_departure.cargo_ready)}, ex-factory ${dmy(dates.this_departure.ex_factory)}.`);
      }
      return { option_id: opt.id, mode: opt.mode, carrier: opt.carrier, service: opt.service, kind: opt.kind, container_size: opt.container_size, pos: perPo, po_count: perPo.length, units, planned_units: plannedUnits, qty, price, currency: opt.currency, basis, departs, dates, fc_p50: fc50, fc_p90: fc90, price_per_unit: r2(price / (units || 1)), days_p50: fc50 ? daysBetween(mondayOf(rq.period_start), fc50) : null, days_p90: fc90 ? daysBetween(mondayOf(rq.period_start), fc90) : null, prob, rolled, transit_quoted: first ? first.p50.quoted : null, transit_p50: first ? first.p50.transit : null, transit_p90: first ? first.p90.transit : null, sample, explain };
    }
    // Totals in one currency: AUD when the legs mix, converted at the finance FX rate (USD→AUD).
    const fx = (() => { try { const r = db.prepare("SELECT rate FROM fin_fx_rates WHERE from_curr = 'USD' AND to_curr = 'AUD'").get(); return r && Number(r.rate) > 0 ? Number(r.rate) : null; } catch (_) { return null; } })();
    let fxNoted = false;
    const point = (id, label, kind, legs) => { const curs = [...new Set(legs.map(l => l.currency || 'USD'))]; let cur = curs[0] || 'USD', price;
      if (curs.length > 1) { cur = 'AUD'; price = r2(legs.reduce((a, l) => a + ((l.currency || 'USD') === 'AUD' ? l.price : l.price * (fx || 1)), 0)); if (!fxNoted) { fxNoted = true; assumptions.push(fx ? `Mixes total in AUD at the finance FX rate (1 USD = ${fx} AUD).` : 'Mixes total in AUD — no USD→AUD rate is set in Finance, so USD legs are added at 1:1 until one is.'); } }
      else price = r2(legs.reduce((a, l) => a + l.price, 0));
      const units = legs.reduce((a, l) => a + l.units, 0) || 1;
      const cont = legs.filter(l => l.mode === 'Sea').reduce((a, l) => a + ((l.qty || {}).containers || 0), 0), kg = legs.filter(l => l.mode === 'Air').reduce((a, l) => a + ((l.qty || {}).chargeable_kg || 0), 0);
      return { id, label, kind, legs, price, currency: cur, price_aud: r2(cur === 'AUD' ? price : price * (fx || 1)), price_per_unit: r2(price / units), containers: cont || 0, air_kg: kg || 0, air_units: legs.filter(l => l.mode === 'Air').reduce((a, l) => a + l.units, 0), fc_p50: legs.map(l => l.fc_p50).filter(Boolean).sort().slice(-1)[0] || null, fc_p90: legs.map(l => l.fc_p90).filter(Boolean).sort().slice(-1)[0] || null,
        days_p50: Math.max(...legs.map(l => l.days_p50 || 0)), days_p90: Math.max(...legs.map(l => l.days_p90 || 0)), prob: legs.some(l => l.prob != null) ? Math.round(legs.reduce((a, l) => a + (l.prob == null ? 100 : l.prob) * l.units, 0) / units) : null, units, option_ids: legs.map(l => l.option_id) }; };

    const points = [];
    for (const o of live) { const lg = leg(o, pos); points.push(point(o.id, `${o.mode} · ${[o.carrier, o.service].filter(Boolean).join(' ') || (o.kind === 'history' ? 'from history' : 'option')}`, 'single', [lg])); }
    // Splits: every sea × air pair; air takes the POs that miss their date by sea (P50), overrides win.
    const overrides = p.overrides || {};                                     // po → option_id
    const seas = live.filter(o => o.mode === 'Sea'), airs = live.filter(o => o.mode === 'Air');
    for (const s of seas) for (const a of airs) {
      const seaAll = leg(s, pos); const byPo = new Map(seaAll.pos.map(x => [x.po, x]));
      const misses = (po) => { const x = byPo.get(po.po); const target = needBy || po.due; return !!(x && target && x.p50.fc > target); };
      const allMiss = pos.every(misses); const composing = Object.keys(overrides).length > 0;
      // Default allocation: a PO goes air when sea misses the date for it but not for everyone (a late supplier, a rolled sailing);
      // when every PO misses, the honest answer is the air option itself, unless the client is composing with overrides —
      // then untouched POs stay on sea.
      const airPos = pos.filter(po => overrides[po.po] ? overrides[po.po] === a.id : (composing ? false : ((misses(po) && !allMiss) || (byPo.get(po.po) || {}).p50?.rolled > 0)));
      const seaPos = pos.filter(po => !airPos.includes(po));
      if (!airPos.length || !seaPos.length) continue;
      const legs = [leg(a, airPos), leg(s, seaPos)];
      const pt = point(`${s.id}+${a.id}`, `Split · ${airPos.length} PO${airPos.length === 1 ? '' : 's'} by air, ${seaPos.length} by sea`, 'split', legs); pt.split = { sea: s.id, air: a.id, air_pos: airPos.map(x => x.po), sea_pos: seaPos.map(x => x.po) }; points.push(pt);
    }
    // Mix by share: a quarter, half, three quarters of the units by air (or the share asked for), spread across every PO.
    // Always offered so the client sees what a partial airlift buys, PO list or not.
    const shares = [...new Set([25, 50, 75].concat(p.air_share != null ? [Math.min(99, Math.max(1, Math.round(Number(p.air_share))))] : []))].filter(x => x > 0 && x < 100).sort((a, b) => a - b);
    for (const s of seas) for (const a of airs) for (const sh of shares) {
      const airPos = pos.map(po => ({ ...po, units: Math.round(po.units * sh / 100) })).filter(x => x.units > 0);
      const seaPos = pos.map(po => ({ ...po, units: po.units - Math.round(po.units * sh / 100) })).filter(x => x.units > 0);
      if (!airPos.length || !seaPos.length) continue;
      const legs = [leg(a, airPos), leg(s, seaPos)];
      const pt = point(`${s.id}+${a.id}@${sh}`, `Mix · ${sh}% of units by air, ${100 - sh}% by sea`, 'mix', legs); pt.split = { sea: s.id, air: a.id, air_share: sh, air_pos: airPos.map(x => x.po), sea_pos: seaPos.map(x => x.po) }; points.push(pt);
    }
    // Pareto flag: no other point is both cheaper and sooner (P50) with at least the same chance.
    for (const x of points) x.frontier = !points.some(y => y !== x && y.price_aud <= x.price_aud && y.days_p50 <= x.days_p50 && (y.prob == null || x.prob == null || y.prob >= x.prob) && (y.price_aud < x.price_aud || y.days_p50 < x.days_p50 || (y.prob || 0) > (x.prob || 0)));
    points.sort((a, b) => a.price_aud - b.price_aud);
    const best = needBy ? points.filter(x => (x.prob || 0) >= 80).sort((a, b) => a.price_aud - b.price_aud)[0] || null : null;
    const cheapest = points[0] || null, fastest = points.slice().sort((a, b) => a.days_p50 - b.days_p50)[0] || null;
    return { request_id: rq.id, ref: rq.ref, period_label: rq.period_label, period_start: rq.period_start, need_by: needBy, total_units: totalUnits, expected_units: pos.reduce((a, x) => a + expectedUnits(x), 0), suppliers, assumptions, computed_at: new Date().toISOString(),
      points, recommended: best ? best.id : null, cheapest: cheapest ? cheapest.id : null, fastest: fastest ? fastest.id : null,
      recommendation: best ? `${best.label} — ${best.currency} ${best.price.toLocaleString()} · in FC ${dmy(best.fc_p50)} (P50) · ${best.prob}% on time. Cheapest that clears 80%.` : (needBy ? (points.length ? 'Nothing clears 80% on time by your date — the fastest option is shown; consider moving the date or splitting.' : 'No priced options to simulate yet.') : 'Set the date you need stock in the FC to get a recommendation.') };
  }
  return { simulate, _internal: { legForPo, rulesOf, Phi } };
};
