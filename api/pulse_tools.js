'use strict';
/* ── VelOzity Pinpoint — Pulse tools v1 ──
   What Pulse can look up, and later do, on demand. Before this, Pulse answered from one
   context block the browser handed it; now the model asks for what it needs through a
   catalogue of named tools with typed arguments, and the server runs them.

   HOW A TOOL RUNS — through Pinpoint's own HTTP routes, on the same process, as the caller:
   the user's bearer token and client header are forwarded to 127.0.0.1. That is deliberate.
   It means a tool can never do more than the person asking could do in the UI, because the
   route's own guards (authenticateRequest, requireRole, requireInternalOrg, tenancy) run on
   every call. A client session therefore cannot reach finance through Pulse even if the
   tool list were wrong — the route refuses it. The list is also filtered before the model
   sees it, so a client session never has a finance tool to call in the first place.

   TIERS — read: runs on sight. auto: a write that is cheap to undo and visible on a screen;
   runs on sight with a receipt (Phase 3). confirm: Pulse proposes it and waits for a yes
   (Phase 3). Phase 1 ships read tools only; the tier field is here so the registry does not
   change shape when writes arrive.

   RESULT SIZE — the model's context is finite and a board payload is not. Every tool trims
   what it returns to what a question could use, and caps lists. Trimming here is cheaper
   than paying for tokens the answer never needed.                                      */

module.exports = function makePulseTools(deps) {
  const { port, log } = deps;
  const logger = log || console;
  const PORT = port || process.env.PORT || 4000;

  // ── Plumbing ──
  const ymdOk = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));
  const mondayOf = (ymd) => {
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(d)) return null;
    const day = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - day);
    return d.toISOString().slice(0, 10);
  };
  const addDays = (ymd, n) => { const d = new Date(ymd + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const isoWeek = (ymd) => {
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z'); if (isNaN(d)) return '';
    const day = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - day + 3);
    const ft = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
    return 'W' + (1 + Math.round(((d - ft) / 86400000 - 3 + ((ft.getUTCDay() + 6) % 7)) / 7));
  };
  // "W46", "week 46", "wk46" → the Monday of that ISO week in the given year (default: this year).
  function weekFromText(text, today) {
    const m = String(text || '').match(/\b(?:w|wk|week)\s*(\d{1,2})\b/i);
    if (!m) return null;
    const wk = Number(m[1]); if (!(wk >= 1 && wk <= 53)) return null;
    const y = Number(String(today).slice(0, 4));
    const jan4 = new Date(Date.UTC(y, 0, 4));
    const mon1 = new Date(jan4); mon1.setUTCDate(jan4.getUTCDate() - ((jan4.getUTCDay() + 6) % 7));
    mon1.setUTCDate(mon1.getUTCDate() + (wk - 1) * 7);
    return mon1.toISOString().slice(0, 10);
  }
  const num = (v) => (v === null || v === undefined || v === '' ? null : (Number.isFinite(Number(v)) ? Number(v) : v));
  const take = (arr, n) => (Array.isArray(arr) ? arr.slice(0, n) : []);

  // Call one of Pinpoint's own routes as the caller. Bearer token and client header travel
  // with the request so identity, role and tenancy are exactly the user's.
  async function callRoute(ctx, method, path, body) {
    const headers = { 'content-type': 'application/json', 'x-pulse-enabled': '1', 'x-pulse-tool': '1' };
    if (ctx.authorization) headers['authorization'] = ctx.authorization;
    if (ctx.internalToken) headers['x-internal-token'] = ctx.internalToken;   // the mailbox answering as Pulse
    if (ctx.client) headers['x-pinpoint-client'] = ctx.client;
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), ctx.timeoutMs || 12000);
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined, signal: ac.signal });
      const text = await r.text();
      let json; try { json = JSON.parse(text); } catch (_) { json = { raw: text.slice(0, 400) }; }
      if (!r.ok) return { ok: false, status: r.status, error: (json && (json.error || json.message)) || `HTTP ${r.status}` };
      return { ok: true, status: r.status, data: json };
    } catch (e) {
      return { ok: false, status: 0, error: e.name === 'AbortError' ? 'timed out' : String(e.message || e) };
    } finally { clearTimeout(t); }
  }

  // ── Shapers: what each tool hands back to the model ──
  const shapeMovement = (c) => ({
    uid: c.consignment_uid, reference: c.reference || 'not advised', mode: c.mode, size_ft: c.size_ft || null,
    vessel: c.vessel || null, carrier: c.carrier || null, week: c.week_start, status: c.status,
    health: c.health ? { key: c.health.key, label: c.health.label, why: c.health.why, held: !!c.health.held, late_days: c.health.late ?? null, lfd_in_days: c.health.lfd_in ?? null } : null,
    eta_fc: c.eta_fc || null, transit_confirmed: !!c.transit_confirmed,
    lanes: take(c.lanes || [], 8),
    milestones: (c.milestones || []).map(m => ({ stage: m.stage, planned: String(m.planned_at || '').slice(0, 10) || null,
      actual: m.state === 'assumed' ? null : String(m.actual_at || '').slice(0, 10) || null, state: m.state })),
    terminal: c.terminal ? { lfd: c.terminal.lfd || null, holds: c.terminal.holds || [] } : null,
  });

  // ── The registry ──
  // Each tool: name, description (what the model reads), input_schema, tier, roles,
  // internalOnly, run(args, ctx). `roles` uses Pinpoint's role labels; the filter below
  // matches on the caller's org type and role.
  const TOOLS = [
    {
      name: 'get_week_status',
      description: 'Transit & Clearing rollup for one week: every movement (container or AWB) with health, confirmed milestones, terminal holds and last free day, plus the plan lanes not yet on any movement. Use for "what is at risk this week", "which lanes have no container", "how many on time".',
      input_schema: { type: 'object', properties: { week: { type: 'string', description: 'Monday YYYY-MM-DD, or "W46"' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx);
        if (!ws) return { error: 'week not understood; give a Monday date or "W46"' };
        const r = await callRoute(ctx, 'GET', `/consignments/week-status?week=${ws}`);
        if (!r.ok) return { error: r.error };
        const st = r.data;
        return {
          week: ws, iso: isoWeek(ws), source: st.source, level: st.level, sea: st.sea_count, air: st.air_count, holds: st.holds,
          last_confirmed_milestone: st.last_milestone_at || null,
          lanes_without_movement: (st.lanes || []).filter(l => !(l.consignments || []).length && !l.no_lane).map(l => ({ supplier: l.supplier, ticket: l.ticket, mode: l.freight, planned_units: l.plannedUnits })).slice(0, 25),
          movements: take((st.consignments || []).map(shapeMovement), 40),
        };
      },
    },
    {
      name: 'find_movement',
      description: 'Find a container, AWB, Zendesk ticket, vendor or PO across all weeks. Returns matching movements with their week and health. Use when a question names a reference.',
      input_schema: { type: 'object', properties: { query: { type: 'string', description: 'container number, AWB, Zendesk ticket, PO number or vendor name' } }, required: ['query'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const q = String(a.query || '').trim(); if (!q) return { error: 'query required' };
        const r = await callRoute(ctx, 'GET', `/consignments/find?q=${encodeURIComponent(q)}`);
        if (!r.ok) return { error: r.error };
        const hits = Array.isArray(r.data) ? r.data : (r.data.results || r.data.hits || []);
        return { query: q, matches: take(hits.map(h => ({ uid: h.consignment_uid || h.uid, reference: h.reference, mode: h.mode, week: h.week_start, iso: isoWeek(h.week_start), matched_on: h.match || h.matched_on || null, supplier: h.supplier || null, ticket: h.zendesk || h.ticket || null, health: h.health ? (h.health.label || h.health.key) : (h.status || null) })), 12) };
      },
    },
    {
      name: 'get_movement',
      description: 'Full detail of one movement by its uid (from find_movement or get_week_status): milestones planned vs confirmed, terminal status, lanes, history of changes.',
      input_schema: { type: 'object', properties: { uid: { type: 'string' } }, required: ['uid'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const uid = String(a.uid || '').trim(); if (!uid) return { error: 'uid required' };
        const r = await callRoute(ctx, 'GET', `/consignments/${encodeURIComponent(uid)}`);
        if (!r.ok) return { error: r.error };
        const c = r.data.consignment || r.data;
        const out = shapeMovement(c);
        if (r.data.tracking) out.tracking = r.data.tracking;
        out.events = take((c.events || []).map(e => ({ at: String(e.at || '').slice(0, 16), kind: e.kind, text: e.text })), 12);
        out.notes = take((c.notes || []).map(n => ({ at: String(n.created_at || n.at || '').slice(0, 16), by: n.author || n.by || null, text: String(n.text || n.body || '').slice(0, 300) })), 8);
        return out;
      },
    },
    {
      name: 'get_quote_intel',
      description: 'Pricing intelligence for one air quote (internal only): partner cost against the vendor\'s history, the client\'s usual sell per kg and per unit, the win curve by price position, the ceiling (lowest price the client has declined), and priced options — rate card, hold the average, best expected margin — each with sell, markup, gross margin, /kg and win likelihood. Use it for "what should we charge", "is this a margin opportunity", "what if we go 20%". Never do the arithmetic yourself; quote these figures. Sell and margin figures are never for the client or the partner.',
      input_schema: { type: 'object', properties: { ref: { type: 'string', description: 'quote reference like AQ-2611-003, or the quote id' } }, required: ['ref'] },
      tier: 'read', roles: ['admin'], internalOnly: true,
      async run(a, ctx) {
        const ref = String(a.ref || '').trim(); if (!ref) return { error: 'ref required' };
        const r = await callRoute(ctx, 'GET', `/air-quotes/intel?ref=${encodeURIComponent(ref)}`);
        if (!r.ok) return { error: r.error };
        const i = r.data.intel || r.data;
        // Trim what the model does not need to reason with.
        return { quote: i.quote, headline: i.headline, kind: i.kind, anchor: i.anchor, benchmarks: i.benchmarks, partner: { cost_kg: i.partner.cost_kg, median_kg: i.partner.median_kg, min_kg: i.partner.min_kg, max_kg: i.partner.max_kg, vs_median_pct: i.partner.vs_median_pct, vs_size_median_pct: i.partner.vs_size_median_pct, rank_low: i.partner.rank_low, n: i.partner.n, volatility_pct: i.partner.volatility_pct, by_size: i.partner.by_size, by_mode: i.partner.by_mode }, ceiling: i.ceiling, win_curve: i.win_curve, options: i.options, recommended: i.recommended };
      },
    },
    {
      name: 'get_transit_board',
      description: 'The live Transit Movements board across the last 12 weeks: counts by health, movements needing attention (late, held, unconfirmed), and recent progress. Use for cross-week questions like "what is late right now" or "what moved in the last 7 days".',
      input_schema: { type: 'object', properties: { week: { type: 'string', description: 'optional Monday or "W46" to narrow to one week' } } },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = a.week ? resolveWeek(a.week, ctx) : null;
        const r = await callRoute(ctx, 'GET', `/consignments/board${ws ? `?week=${ws}` : ''}`);
        if (!r.ok) return { error: r.error };
        const all = (r.data.consignments || []).map(shapeMovement);
        const byHealth = {};
        for (const m of all) { const k = (m.health && m.health.key) || 'unknown'; byHealth[k] = (byHealth[k] || 0) + 1; }
        const attention = all.filter(m => m.health && (m.health.held || m.health.key === 'delayed' || m.health.key === 'behind' || m.health.key === 'not_booked'));
        return { weeks: [...new Set(all.map(m => m.week))].sort(), total: all.length, by_health: byHealth,
                 needs_attention: take(attention, 25), movements: ws ? take(all, 40) : undefined };
      },
    },
    {
      name: 'get_receiving',
      description: 'Receiving for one week: each PO with supplier, planned vs received cartons and units, received date, lateness, cartons replaced, discrepancies. Use for "which POs have not been received", "who is late", "how many cartons replaced".',
      input_schema: { type: 'object', properties: { week: { type: 'string' }, supplier: { type: 'string', description: 'optional supplier name filter (contains)' }, only_outstanding: { type: 'boolean' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        // A PO that has not arrived has no receiving row, so the plan is the spine and
        // receiving is joined onto it; otherwise "not received yet" would always be empty.
        const [pr, rr] = await Promise.all([callRoute(ctx, 'GET', `/plan/weeks/${ws}`), callRoute(ctx, 'GET', `/receiving?weekStart=${ws}`)]);
        if (!rr.ok) return { error: rr.error };
        const planRows = pr.ok ? (Array.isArray(pr.data) ? pr.data : (pr.data.rows || pr.data.plan || [])) : [];
        const recRows = Array.isArray(rr.data) ? rr.data : (rr.data.rows || rr.data.receiving || []);
        const byPo = new Map();
        for (const x of planRows) {
          const po = String(x.po_number || '').trim(); if (!po) continue;
          const e = byPo.get(po) || { po, supplier: x.supplier_name || x.supplier || null, freight: x.freight_type || x.freight || null, ticket: x.zendesk_ticket || x.zendesk || null,
                                      planned_units: 0, due: String(x.due_date || x.ex_factory || '').slice(0, 10) || null, received_units: 0, cartons_in: 0, cartons_out: 0, cartons_replaced: 0, received_at: null, discrepancy: null };
          e.planned_units += Number(x.target_qty ?? x.planned_units ?? 0) || 0;
          byPo.set(po, e);
        }
        for (const x of recRows) {
          const po = String(x.po_number || '').trim(); if (!po) continue;
          const e = byPo.get(po) || { po, supplier: x.supplier_name || x.supplier || null, freight: x.freight_type || null, ticket: x.zendesk_ticket || null, planned_units: num(x.planned_units) || 0, due: null, received_units: 0, cartons_in: 0, cartons_out: 0, cartons_replaced: 0, received_at: null, discrepancy: null };
          e.received_units += Number(x.received_units ?? x.units_received ?? 0) || 0;
          e.cartons_in += Number(x.cartons_received ?? x.cartons_in ?? 0) || 0;
          e.cartons_out += Number(x.cartons_out ?? 0) || 0;
          e.cartons_replaced += Number(x.cartons_replaced ?? 0) || 0;
          const d = String(x.received_date || x.received_at || x.date_received || '').slice(0, 10);
          if (d && (!e.received_at || d < e.received_at)) e.received_at = d;
          if (x.discrepancy_note || x.discrepancy) e.discrepancy = x.discrepancy_note || x.discrepancy;
          if (!e.supplier && (x.supplier_name || x.supplier)) e.supplier = x.supplier_name || x.supplier;
          byPo.set(po, e);
        }
        let shaped = [...byPo.values()].map(e => ({ ...e, late: !!(e.received_at && e.due && e.received_at > e.due), status: e.received_at ? 'received' : 'not received' }));
        const sup = String(a.supplier || '').trim().toLowerCase();
        if (sup) shaped = shaped.filter(x => String(x.supplier || '').toLowerCase().includes(sup));
        const list = a.only_outstanding ? shaped.filter(s => !s.received_at) : shaped;
        const tot = (k) => shaped.reduce((s, x) => s + (Number(x[k]) || 0), 0);
        return { week: ws, iso: isoWeek(ws), pos: shaped.length, received: shaped.filter(s => s.received_at).length, outstanding: shaped.filter(s => !s.received_at).length,
                 planned_units: tot('planned_units'), received_units: tot('received_units'), cartons_replaced: tot('cartons_replaced'), rows: take(list, 60), truncated: list.length > 60 };
      },
    },
    {
      name: 'get_vas_progress',
      description: 'VAS (labelling) progress for one week: units planned vs applied by PO and supplier, completion %, and the daily run-rate. Use for "are we on track", "how much is left", "which suppliers are behind on VAS".',
      input_schema: { type: 'object', properties: { week: { type: 'string' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        const r = await callRoute(ctx, 'GET', `/summary/po_sku?week=${ws}`);
        if (!r.ok) return { error: r.error };
        const rows = Array.isArray(r.data) ? r.data : (r.data.rows || []);
        const byPo = new Map();
        for (const x of rows) {
          const k = String(x.po_number || x.po || ''); if (!k) continue;
          const e = byPo.get(k) || { po: k, supplier: x.supplier_name || x.supplier || null, planned: 0, applied: 0 };
          e.planned += Number(x.planned ?? x.target_qty ?? 0) || 0; e.applied += Number(x.applied ?? x.applied_units ?? 0) || 0;
          byPo.set(k, e);
        }
        const pos = [...byPo.values()].map(p => ({ ...p, pct: p.planned ? Math.round(p.applied / p.planned * 100) : null })).sort((a2, b) => (b.planned - b.applied) - (a2.planned - a2.applied));
        const planned = pos.reduce((s, p) => s + p.planned, 0), applied = pos.reduce((s, p) => s + p.applied, 0);
        const bySup = {};
        for (const p of pos) { const s = p.supplier || 'Unknown'; bySup[s] = bySup[s] || { planned: 0, applied: 0 }; bySup[s].planned += p.planned; bySup[s].applied += p.applied; }
        return { week: ws, iso: isoWeek(ws), planned, applied, remaining: Math.max(0, planned - applied), pct: planned ? Math.round(applied / planned * 100) : null,
                 by_supplier: Object.entries(bySup).map(([s, v]) => ({ supplier: s, ...v, remaining: Math.max(0, v.planned - v.applied) })).sort((x, y) => y.remaining - x.remaining).slice(0, 15),
                 top_remaining_pos: take(pos, 20) };
      },
    },
    {
      name: 'get_last_mile',
      description: 'Last-mile delivery for one week: each container with dock booking status (not scheduled / scheduled / delivered), slot time, delivered time, and whether it is cleared and waiting for a slot. Use for "what needs a dock booking", "what is delivering tomorrow".',
      input_schema: { type: 'object', properties: { week: { type: 'string' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        const [st, fw] = await Promise.all([callRoute(ctx, 'GET', `/consignments/week-status?week=${ws}`), callRoute(ctx, 'GET', `/flow/week/${ws}/all`)]);
        if (!st.ok) return { error: st.error };
        const receipts = {};
        try {
          const d = fw.ok ? fw.data : null;
          const blobs = d && d.facilities ? Object.values(d.facilities).map(f => f && f.data).filter(Boolean)
                      : (Array.isArray(d) ? d.map(b => (typeof b.data === 'string' ? JSON.parse(b.data) : (b.data || b))) : (d ? [d] : []));
          for (const b of blobs) Object.assign(receipts, (b && b.lastmile_receipts) || {});
        } catch (_) {}
        const out = (st.data.consignments || []).map(c => {
          const rc = receipts[c.consignment_uid] || Object.values(receipts).find(x => x && String(x.container_id || '').trim() === String(c.reference || '').trim()) || null;
          const cleared = (c.milestones || []).some(m => m.stage === 'dest_cleared' && m.state !== 'assumed' && m.actual_at);
          const arrived = (c.milestones || []).some(m => m.stage === 'arrived' && m.state !== 'assumed' && m.actual_at);
          const status = rc && rc.delivery_local ? 'delivered' : (rc && rc.scheduled_for ? 'scheduled' : 'not scheduled');
          return { uid: c.consignment_uid, reference: c.reference || 'not advised', mode: c.mode, status, slot: rc && rc.scheduled_for ? String(rc.scheduled_for).slice(0, 16) : null,
                   delivered: rc && rc.delivery_local ? String(rc.delivery_local).slice(0, 16) : null, cleared, arrived,
                   needs_slot_now: status === 'not scheduled' && (cleared || arrived), eta_fc: c.eta_fc || null, lfd: c.terminal && c.terminal.lfd || null };
        });
        return { week: ws, iso: isoWeek(ws), total: out.length, delivered: out.filter(x => x.status === 'delivered').length, scheduled: out.filter(x => x.status === 'scheduled').length,
                 needs_slot_now: out.filter(x => x.needs_slot_now), not_yet_cleared: out.filter(x => x.status === 'not scheduled' && !x.needs_slot_now).length, containers: take(out, 40) };
      },
    },
    {
      name: 'get_uid_labels',
      description: 'UID label issuance for one week: runs, labels issued per PO, format (footwear / standard), POs flagged new department code, print and email status. Use for "have labels been issued", "how many labels for PO X".',
      input_schema: { type: 'object', properties: { week: { type: 'string' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        const r = await callRoute(ctx, 'GET', `/uid-labels/week/${ws}`);
        if (!r.ok) return { error: r.error };
        const d = r.data;
        return { week: ws, iso: isoWeek(ws), runs: take((d.runs || []).map(x => ({ id: x.id, at: String(x.created_at || '').slice(0, 16), reason: x.reason, labels: x.label_count ?? x.labels, pos: x.po_count ?? x.pos, emailed: !!(x.emailed_at || x.email_sent) })), 10),
                 pos: take((d.pos || d.rows || []).map(x => ({ po: x.po_number || x.po, format: x.format, issued: x.issued ?? x.label_count, planned: x.planned ?? x.planned_units, new_code: !!(x.flag_new || x.new_code), moved_from: x.moved_from || null })), 60),
                 totals: d.totals || d.tiles || null };
      },
    },
    {
      name: 'get_alerts',
      description: 'Open alerts on movements for a week: delivery date moved, milestone unconfirmed past its date, transit running on a guess. Each carries the uid, kind and signature needed to acknowledge it.',
      input_schema: { type: 'object', properties: { week: { type: 'string', description: 'YYYY-MM-DD Monday, "W46", "this week" (default), "last week"' } } },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week || 'this week', ctx); if (!ws) return { error: 'could not read that week' };
        const r = await callRoute(ctx, 'GET', `/alerts?week=${ws}`);
        if (!r.ok) return { error: r.error };
        const items = Array.isArray(r.data) ? r.data : (r.data.alerts || r.data.items || []);
        const open = items.filter(x => !x.acknowledged_at);
        return { week: ws, iso: isoWeek(ws), count: open.length, acknowledged: items.length - open.length,
          alerts: take(open.map(x => ({ uid: x.consignment_uid, kind: x.kind, signature: x.signature, severity: x.severity, reference: x.reference,
            text: x.headline || x.text || x.title, detail: x.detail || null, action: x.action || x.suggested_action || null })), 30) };
      },
    },
    {
      name: 'get_air_quotes',
      description: 'Air freight quotes: state (requested, in review, quoted, approved, declined, expired), vendor, week, chargeable kg, cartons, units, sell price and per-kg/per-unit figures. VelOzity staff also see partner cost and margin. Use for "what quotes are waiting", "what did we quote for vendor X".',
      input_schema: { type: 'object', properties: { state: { type: 'string', description: 'optional: requested | in_review | quoted | approved | declined | expired' }, vendor: { type: 'string' } } },
      tier: 'read', roles: ['admin', 'ops', 'client'],
      async run(a, ctx) {
        const path = ctx.isInternal ? '/air-quotes/internal' : '/air-quotes';
        const r = await callRoute(ctx, 'GET', `${path}?client_id=${encodeURIComponent(ctx.client || '')}`);
        if (!r.ok) return { error: r.error };
        let q = r.data.quotes || (Array.isArray(r.data) ? r.data : []);
        if (a.state) q = q.filter(x => String(x.state || '').toLowerCase() === String(a.state).toLowerCase());
        if (a.vendor) q = q.filter(x => String(x.vendor_raw || x.vendor || '').toLowerCase().includes(String(a.vendor).toLowerCase()));
        q = q.filter(x => !x.superseded_by);
        return { count: q.length, quotes: take(q.map(x => Object.assign({ ref: x.ref, state: x.state, week: x.week_start, iso: isoWeek(x.week_start), vendor: x.vendor_raw || x.vendor, cartons: x.cartons, units: x.units, chargeable_kg: x.chargeable_kg,
            sell: x.sell_preview ?? x.sell, sell_per_kg: x.sell_per_kg, sell_per_unit: x.sell_per_unit, decided_at: String(x.decided_at || '').slice(0, 10) || null },
          ctx.isInternal ? { cost: x.cost, cost_per_kg: x.cost_per_kg, margin_pct: x.gross_margin_pct, partner: x.partner_name || null } : {})), 25) };
      },
    },
    {
      name: 'get_non_compliance',
      description: 'Supplier non-compliance incidents for one week: supplier, category, remedy (labelling or carton replacement), quantity, chargeable, cost. Use for "which suppliers had issues", "how many cartons replaced for supplier X".',
      input_schema: { type: 'object', properties: { week: { type: 'string' }, supplier: { type: 'string' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        const r = await callRoute(ctx, 'GET', `/nc/incidents?week_start=${ws}${a.supplier ? `&supplier=${encodeURIComponent(a.supplier)}` : ''}`);
        if (!r.ok) return { error: r.error };
        const rows = Array.isArray(r.data) ? r.data : (r.data.incidents || r.data.rows || []);
        const bySup = {};
        for (const x of rows) { const s = x.supplier || 'Unknown'; bySup[s] = bySup[s] || { incidents: 0, qty: 0, cost: 0 }; bySup[s].incidents++; bySup[s].qty += Number(x.qty) || 0; bySup[s].cost += Number(x.cost) || 0; }
        return { week: ws, iso: isoWeek(ws), incidents: rows.length, by_supplier: Object.entries(bySup).map(([s, v]) => ({ supplier: s, ...v })).sort((x, y) => y.incidents - x.incidents),
                 rows: take(rows.map(x => ({ supplier: x.supplier, po: x.po_number, category: x.category_name || x.category, remedy: x.remedy_type, qty: x.qty, chargeable: !!x.chargeable, cost: ctx.isInternal ? x.cost : undefined, note: String(x.note || '').slice(0, 160) })), 40) };
      },
    },
    {
      name: 'get_ops_summary',
      description: 'Week Hub style summary of the operation right now: POs planned / received / applied, units, cartons, lanes, and the exceptions the Control Tower is showing. Good first call for "how is this week going".',
      input_schema: { type: 'object', properties: { week: { type: 'string' } }, required: ['week'] },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'week not understood' };
        const [rec, vas, st] = await Promise.all([this._rec(a, ctx, ws), this._vas(ctx, ws), callRoute(ctx, 'GET', `/consignments/week-status?week=${ws}`)]);
        return { week: ws, iso: isoWeek(ws), receiving: rec, vas, transit: st.ok ? { level: st.data.level, sea: st.data.sea_count, air: st.data.air_count, holds: st.data.holds, lanes_without_movement: st.data.unassigned_lanes } : { error: st.error } };
      },
      async _rec(a, ctx, ws) { const t = TOOLS.find(x => x.name === 'get_receiving'); const r = await t.run({ week: ws }, ctx); if (r.error) return r; delete r.rows; return r; },
      async _vas(ctx, ws) { const t = TOOLS.find(x => x.name === 'get_vas_progress'); const r = await t.run({ week: ws }, ctx); if (r.error) return r; delete r.top_remaining_pos; return r; },
    },
    {
      name: 'list_weeks',
      description: 'The weeks Pinpoint has a plan for (most recent first) with ISO labels, so "last week" or "W44" can be resolved. Call when unsure which week a question means.',
      input_schema: { type: 'object', properties: {} },
      tier: 'read', roles: ['admin', 'ops', 'client', 'partner'],
      async run(a, ctx) {
        const r = await callRoute(ctx, 'GET', '/plan/weeks');
        if (!r.ok) return { error: r.error };
        const weeks = (Array.isArray(r.data) ? r.data : (r.data.weeks || [])).map(w => String(w.week_start || w).slice(0, 10)).filter(ymdOk).sort().reverse();
        return { today: ctx.today, current_week: mondayOf(ctx.today), weeks: take(weeks.map(w => ({ week: w, iso: isoWeek(w) })), 16) };
      },
    },
    // ── Internal-only (VelOzity staff) ──
    {
      name: 'get_invoices',
      description: 'VelOzity invoices for this client: type (VAS, SEA, AIR, MISC), week, reference, status (draft, sent, paid, overdue), subtotal, GST, total. Internal only.',
      input_schema: { type: 'object', properties: { week: { type: 'string' }, status: { type: 'string' }, type: { type: 'string' } } },
      tier: 'read', roles: ['admin'], internalOnly: true,
      async run(a, ctx) {
        const r = await callRoute(ctx, 'GET', '/finance/invoices');
        if (!r.ok) return { error: r.error };
        let inv = Array.isArray(r.data) ? r.data : (r.data.invoices || []);
        const ws = a.week ? resolveWeek(a.week, ctx) : null;
        if (ws) inv = inv.filter(i => i.week_start === ws);
        if (a.status) inv = inv.filter(i => String(i.status).toLowerCase() === String(a.status).toLowerCase());
        if (a.type) inv = inv.filter(i => String(i.type).toUpperCase() === String(a.type).toUpperCase());
        inv.sort((x, y) => String(y.week_start).localeCompare(String(x.week_start)));
        return { count: inv.length, invoices: take(inv.map(i => ({ ref: i.ref_number, type: i.type, week: i.week_start, iso: isoWeek(i.week_start), status: i.status, invoice_date: i.invoice_date, due: i.due_date, subtotal: i.subtotal, gst: i.gst, customs: i.customs, total: i.total })), 30) };
      },
    },
    {
      name: 'get_pl',
      description: 'Company P&L by month for a year: revenue by channel (VAS, sea, air, misc) net of GST, expenses by category, net result. Internal only. Note: revenue here is cash-basis tiles in some views; say which basis if asked.',
      input_schema: { type: 'object', properties: { year: { type: 'integer' } } },
      tier: 'read', roles: ['admin'], internalOnly: true,
      async run(a, ctx) {
        const y = a.year || Number(String(ctx.today).slice(0, 4));
        const r = await callRoute(ctx, 'GET', `/finance/pl?year=${y}`);
        if (!r.ok) return { error: r.error };
        const months = (r.data.months || []).map(m => ({ month: m.month || m.key || m.month_key, rev_vas: m.rev_vas, rev_sea: m.rev_sea, rev_air: m.rev_air, rev_misc: m.rev_misc, revenue: m.revenue ?? m.rev_total, expenses: m.expenses ?? m.exp_total, net: m.net ?? m.result }));
        return { year: y, months, totals: r.data.totals || r.data.aggregate || null };
      },
    },
    // ══════════════════════════════════════════════════════════════════════════════════════
    // WRITE TOOLS (Phase 3). Each is a thin wrapper over the route a person would call from
    // the screen, run as that person. `tier` decides what happens when the model asks for it:
    //   auto    — runs at once; the receipt and an undo link go back to the user
    //   confirm — nothing runs; the user sees "Pulse wants to: …" and must say yes
    // `summary(args, pre)` is the sentence read back; `undo(record, ctx)` reverses a run.
    // Admin only at launch (agreed 19 Nov). Adding a role is a one-word change per tool.
    // ══════════════════════════════════════════════════════════════════════════════════════
    {
      name: 'add_note',
      description: 'Add a short note to a movement (container or flight). The note shows on the Last Mile panel and in the weekly history. Use when someone says "note that…" or "make a note on…". Not for changing dates or statuses.',
      input_schema: { type: 'object', properties: { movement: { type: 'string', description: 'container number, AWB, Zendesk ticket or movement uid' }, note: { type: 'string' } }, required: ['movement', 'note'] },
      tier: 'auto', roles: ['admin'], write: true,
      summary: (a, pre) => `Add a note to ${pre && pre.label || a.movement}: “${String(a.note || '').trim()}”`,
      async run(a, ctx) {
        const note = String(a.note || '').trim(); if (!note) return { error: 'note is empty' };
        const mv = await resolveMovement(ctx, a.movement); if (mv.error) return mv;
        const rc = await readReceipts(ctx, mv); if (rc.error) return rc;
        const prev = rc.receipts[mv.uid] ? JSON.parse(JSON.stringify(rc.receipts[mv.uid])) : null;
        const stamp = `[Pulse ${fmtShort(ctx.today)}] ${note}`;
        const cur = String((prev && prev.last_mile_note) || '').trim();
        rc.receipts[mv.uid] = { ...(prev || {}), container_id: (prev && prev.container_id) || mv.reference || '', last_mile_note: cur ? `${cur} | ${stamp}` : stamp, _updatedAt: new Date().toISOString() };
        const w = await writeReceipts(ctx, mv, rc.receipts); if (w.error) return w;
        return { ok: true, receipt: `Noted on ${mv.label}: “${note}”`, undo: { kind: 'receipt', uid: mv.uid, week: mv.week, facility: mv.facility, prev } };
      },
      undo: (rec, ctx) => restoreReceipt(ctx, rec),
    },
    {
      name: 'confirm_milestone',
      description: 'Confirm a movement stage happened on its planned date — only when Pinpoint already has evidence: tracking shows the vessel departed/arrived/was released, the origin stages that run to schedule, or a last-mile delivery already recorded. If there is no evidence the tool refuses and tells you to use amend_milestone with the date the person gives. Stages: packing_list_ready, origin_cleared, departed, arrived, dest_cleared, fc_receipt.',
      input_schema: { type: 'object', properties: { movement: { type: 'string', description: 'container number, AWB, Zendesk ticket or uid' }, stage: { type: 'string', enum: ['packing_list_ready', 'origin_cleared', 'departed', 'arrived', 'dest_cleared', 'fc_receipt'] } }, required: ['movement', 'stage'] },
      tier: 'auto', roles: ['admin'], write: true,
      summary: (a, pre) => `Confirm ${STAGE_LABEL[a.stage] || a.stage} for ${pre && pre.label || a.movement} as planned`,
      async run(a, ctx) {
        const stage = String(a.stage || ''); if (!STAGE_LABEL[stage]) return { error: 'unknown stage' };
        const mv = await resolveMovement(ctx, a.movement); if (mv.error) return mv;
        const m = (mv.c.milestones || []).find(x => x.stage === stage) || {};
        if (m.state && m.state !== 'assumed') return { error: `${STAGE_LABEL[stage]} for ${mv.label} is already recorded as ${m.state}${m.actual_at ? ' on ' + String(m.actual_at).slice(0, 10) : ''}`, already: true };
        if (!mv.c.transit_confirmed) return { error: `${mv.label} has no carrier transit quote yet, so its planned dates are a guess; enter the details in Transit Movements first`, needs_confirmation: true };
        const ev = await corroboration(ctx, mv, stage);
        if (!ev) return { error: `No evidence in Pinpoint that ${STAGE_LABEL[stage]} happened for ${mv.label}. Ask the person for the date and use amend_milestone.`, needs_confirmation: true, hint: 'amend_milestone' };
        const r = await callRoute(ctx, 'POST', `/consignments/${encodeURIComponent(mv.uid)}/milestone`, { stage, note: `Pulse: ${ev}` });
        if (!r.ok) return { error: r.error };
        const row = ((r.data.consignment || {}).milestones || []).find(x => x.stage === stage) || {};
        return { ok: true, receipt: `Confirmed ${STAGE_LABEL[stage]} for ${mv.label} as ${fmtShort(row.actual_at || m.planned_at)} (${ev})`, undo: { kind: 'milestone', uid: mv.uid, stage, prev: { state: m.state || 'assumed', actual_at: m.actual_at || null, note: m.source_detail || null } } };
      },
      undo: (rec, ctx) => restoreMilestone(ctx, rec),
    },
    {
      name: 'amend_milestone',
      description: 'Record that a movement stage happened on a given date (confirming or correcting it) when a person states the date. The person must approve before it runs. Stages: packing_list_ready, origin_cleared, departed, arrived, dest_cleared, fc_receipt.',
      input_schema: { type: 'object', properties: { movement: { type: 'string' }, stage: { type: 'string', enum: ['packing_list_ready', 'origin_cleared', 'departed', 'arrived', 'dest_cleared', 'fc_receipt'] }, date: { type: 'string', description: 'YYYY-MM-DD' }, note: { type: 'string' } }, required: ['movement', 'stage', 'date'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Record ${STAGE_LABEL[a.stage] || a.stage} for ${pre && pre.label || a.movement} as ${fmtShort(a.date)}${a.note ? ` (${a.note})` : ''}`,
      async prepare(a, ctx) { const mv = await resolveMovement(ctx, a.movement); return mv.error ? mv : { label: mv.label, uid: mv.uid }; },
      async run(a, ctx) {
        const stage = String(a.stage || ''); if (!STAGE_LABEL[stage]) return { error: 'unknown stage' };
        if (!ymdOk(a.date)) return { error: 'date must be YYYY-MM-DD' };
        const mv = await resolveMovement(ctx, a.movement); if (mv.error) return mv;
        const m = (mv.c.milestones || []).find(x => x.stage === stage) || {};
        const r = await callRoute(ctx, 'POST', `/consignments/${encodeURIComponent(mv.uid)}/milestone`, { stage, actual_at: a.date, note: a.note ? `Pulse: ${a.note}` : 'Pulse: stated by user', clear: false });
        if (!r.ok) return { error: r.error };
        return { ok: true, receipt: `Recorded ${STAGE_LABEL[stage]} for ${mv.label} as ${fmtShort(a.date)}`, undo: { kind: 'milestone', uid: mv.uid, stage, prev: { state: m.state || 'assumed', actual_at: m.actual_at || null, note: m.source_detail || null } } };
      },
      undo: (rec, ctx) => restoreMilestone(ctx, rec),
    },
    {
      name: 'mark_last_mile',
      description: 'Record last-mile status for a movement: "scheduled" with the dock booking date and time, or "delivered" (received at the fulfilment centre). Use when someone says the dock is booked or the container has been delivered.',
      input_schema: { type: 'object', properties: { movement: { type: 'string' }, status: { type: 'string', enum: ['scheduled', 'delivered'] }, when: { type: 'string', description: 'scheduled: YYYY-MM-DDTHH:MM local; delivered: YYYY-MM-DD or YYYY-MM-DDTHH:MM (default now)' }, note: { type: 'string' } }, required: ['movement', 'status'] },
      tier: 'auto', roles: ['admin'], write: true,
      summary: (a, pre) => a.status === 'delivered' ? `Mark ${pre && pre.label || a.movement} delivered${a.when ? ' on ' + fmtShort(a.when) : ''}` : `Mark ${pre && pre.label || a.movement} delivery scheduled for ${fmtWhen(a.when)}`,
      async run(a, ctx) {
        const status = String(a.status || '');
        if (status === 'scheduled' && !/^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?$/.test(String(a.when || ''))) return { error: 'scheduled needs when as YYYY-MM-DDTHH:MM' };
        if (a.when && !/^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?$/.test(String(a.when))) return { error: 'when must be YYYY-MM-DD or YYYY-MM-DDTHH:MM' };
        const mv = await resolveMovement(ctx, a.movement); if (mv.error) return mv;
        const rc = await readReceipts(ctx, mv); if (rc.error) return rc;
        const prev = rc.receipts[mv.uid] ? JSON.parse(JSON.stringify(rc.receipts[mv.uid])) : null;
        const now = localNow();
        const base = { ...(prev || {}), container_id: (prev && prev.container_id) || mv.reference || '', _updatedAt: new Date().toISOString() };
        if (a.note) base.last_mile_note = [String((prev && prev.last_mile_note) || '').trim(), `[Pulse ${fmtShort(ctx.today)}] ${a.note}`].filter(Boolean).join(' | ');
        let receipt;
        if (status === 'scheduled') {
          const when = String(a.when).length === 10 ? a.when + 'T09:00' : a.when;
          Object.assign(base, { scheduled_for: when, scheduled_local: (prev && prev.scheduled_local) || now, rescheduled_local: prev && prev.scheduled_for ? now : undefined, status: 'Scheduled' });
          receipt = `${prev && prev.scheduled_for ? 'Rebooked' : 'Scheduled'} delivery of ${mv.label} for ${fmtWhen(when)}`;
        } else {
          const when = a.when ? (String(a.when).length === 10 ? a.when + 'T' + now.slice(11, 16) : a.when) : now;
          Object.assign(base, { delivery_local: when, status: 'Delivered', pod_received: true });
          receipt = `Marked ${mv.label} delivered ${fmtShort(when)}`;
        }
        rc.receipts[mv.uid] = base;
        const w = await writeReceipts(ctx, mv, rc.receipts); if (w.error) return w;
        return { ok: true, receipt, undo: { kind: 'receipt', uid: mv.uid, week: mv.week, facility: mv.facility, prev } };
      },
      undo: (rec, ctx) => restoreReceipt(ctx, rec),
    },
    {
      name: 'acknowledge_alert',
      description: 'Acknowledge an open alert (from get_alerts) so it stops showing as open: someone has dealt with it. Needs the alert uid, kind and signature exactly as get_alerts returned them.',
      input_schema: { type: 'object', properties: { uid: { type: 'string' }, kind: { type: 'string' }, signature: { type: 'string' } }, required: ['uid', 'kind', 'signature'] },
      tier: 'auto', roles: ['admin'], write: true,
      summary: (a, pre) => `Acknowledge the ${ALERT_LABEL[a.kind] || a.kind} alert on ${pre && pre.label || a.uid}`,
      async run(a, ctx) {
        const uid = String(a.uid || '').trim(), kind = String(a.kind || ''), sig = String(a.signature || '');
        if (!uid || !kind) return { error: 'uid and kind required' };
        const r = await callRoute(ctx, 'POST', '/alerts/ack', { consignment_uid: uid, kind, signature: sig });
        if (!r.ok) return { error: r.error };
        let label = uid; try { const mv = await resolveMovement(ctx, uid); if (!mv.error) label = mv.label; } catch (_) {}
        return { ok: true, receipt: `Acknowledged the ${ALERT_LABEL[kind] || kind} alert on ${label}`, undo: { kind: 'alert', uid, alert_kind: kind, signature: sig } };
      },
      async undo(rec, ctx) {
        const r = await callRoute(ctx, 'POST', '/alerts/ack', { consignment_uid: rec.uid, kind: rec.alert_kind, signature: rec.signature, clear: true });
        return r.ok ? { ok: true, receipt: 'Alert is open again' } : { error: r.error };
      },
    },
    // ── Phase 3b: Confirm-tier tools. Nothing below runs until the person says yes. ──
    {
      name: 'create_movement',
      description: 'Add a new container (sea) or flight (air) to a week and put lanes on it. Give the week, mode, reference (container number or flight), vessel or airline, carrier-quoted transit days, size (20/40/40HQ for sea) and the lanes by supplier name or Zendesk ticket. Lanes must exist in that week\'s plan. The person must approve before it runs.',
      input_schema: { type: 'object', properties: { week: { type: 'string', description: 'YYYY-MM-DD Monday, "W47", "this week", "next week"' }, mode: { type: 'string', enum: ['Sea', 'Air'] }, reference: { type: 'string', description: 'container number or flight/AWB' }, vessel: { type: 'string', description: 'vessel and voyage, or airline' }, transit_days: { type: 'number' }, size_ft: { type: 'string', description: '20 | 40 | 40HQ, sea only' }, lanes: { type: 'array', items: { type: 'string' }, description: 'supplier names or Zendesk tickets' } }, required: ['week', 'mode', 'reference', 'lanes'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Create ${a.mode === 'Air' ? 'flight' : 'container'} ${a.reference} in ${pre && pre.iso || a.week}${a.vessel ? ` on ${a.vessel}` : ''}${a.transit_days ? `, ${a.transit_days}-day transit` : ''}, carrying ${pre && pre.laneLabels ? pre.laneLabels.join(', ') : (a.lanes || []).join(', ')}`,
      async prepare(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'could not read that week' };
        const ref = String(a.reference || '').trim(); if (!ref) return { error: 'reference required' };
        const dup = await callRoute(ctx, 'GET', `/consignments/find?q=${encodeURIComponent(ref)}`);
        const hits = dup.ok ? (dup.data.hits || []) : [];
        if (hits.some(h => String(h.reference || '').toLowerCase() === ref.toLowerCase() && h.week_start === ws)) return { error: `${ref} already exists in ${isoWeek(ws)}; use amend_movement` };
        const ln = await resolveLanes(ctx, ws, a.mode, a.lanes); if (ln.error) return ln;
        return { ws, iso: isoWeek(ws), laneKeys: ln.keys, laneLabels: ln.labels, facility: ln.facility };
      },
      async run(a, ctx) {
        const pre = await this.prepare(a, ctx); if (pre.error) return pre;
        const body = { week_start: pre.ws, facility: pre.facility, mode: a.mode === 'Air' ? 'Air' : 'Sea', reference: String(a.reference).trim(), vessel: a.vessel || null,
          transit_days: Number.isFinite(Number(a.transit_days)) ? Number(a.transit_days) : undefined, size_ft: a.mode === 'Air' ? null : (a.size_ft || null), lanes: pre.laneKeys };
        const r = await callRoute(ctx, 'POST', '/consignments', body); if (!r.ok) return { error: r.error };
        const c = r.data.consignment || {};
        return { ok: true, receipt: `Created ${body.mode === 'Air' ? 'flight' : 'container'} ${body.reference} in ${pre.iso} with ${pre.laneLabels.length} lane${pre.laneLabels.length === 1 ? '' : 's'}${body.transit_days ? '' : ' (no carrier transit yet — dates are the rule default)'}`, uid: c.consignment_uid, undo: { kind: 'create', uid: c.consignment_uid } };
      },
      async undo(rec, ctx) {
        const r = await callRoute(ctx, 'DELETE', `/consignments/${encodeURIComponent(rec.uid)}`);
        return r.ok ? { ok: true, receipt: 'Removed the movement again' } : { error: r.error };
      },
    },
    {
      name: 'amend_movement',
      description: 'Change an existing movement: its reference (container number / flight), vessel or airline, carrier-quoted transit days, size, or add/remove lanes (by supplier or ticket). Only the fields given change. The person must approve before it runs.',
      input_schema: { type: 'object', properties: { movement: { type: 'string', description: 'current container number, AWB, ticket or uid' }, reference: { type: 'string' }, vessel: { type: 'string' }, transit_days: { type: 'number' }, size_ft: { type: 'string' }, add_lanes: { type: 'array', items: { type: 'string' } }, remove_lanes: { type: 'array', items: { type: 'string' } } }, required: ['movement'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Change ${pre && pre.label || a.movement}: ${(pre && pre.changes || ['(no change given)']).join('; ')}`,
      async prepare(a, ctx) {
        const mv = await resolveMovement(ctx, a.movement); if (mv.error) return mv;
        const c = mv.c, changes = [];
        if (a.reference && String(a.reference).trim() !== (c.reference || '')) changes.push(`reference → ${String(a.reference).trim()}`);
        if (a.vessel && a.vessel !== (c.vessel || '')) changes.push(`${c.mode === 'Air' ? 'airline' : 'vessel'} → ${a.vessel}`);
        if (Number.isFinite(Number(a.transit_days)) && Number(a.transit_days) !== Number(c.transit_days)) changes.push(`transit ${c.transit_confirmed ? c.transit_days + ' → ' : ''}${a.transit_days} days`);
        if (a.size_ft && a.size_ft !== (c.size_ft || '')) changes.push(`size → ${a.size_ft}`);
        let addKeys = [], removeKeys = [];
        if (Array.isArray(a.add_lanes) && a.add_lanes.length) { const ln = await resolveLanes(ctx, c.week_start, c.mode, a.add_lanes); if (ln.error) return ln; addKeys = ln.keys.filter(k => !(c.lanes || []).includes(k)); if (addKeys.length) changes.push(`add ${ln.labels.join(', ')}`); }
        if (Array.isArray(a.remove_lanes) && a.remove_lanes.length) {
          const want = a.remove_lanes.map(x => String(x).trim().toLowerCase());
          removeKeys = (c.lanes || []).filter(k => { const [sup, tk] = k.split('||'); return want.includes(sup.toLowerCase()) || want.includes(tk.toLowerCase()); });
          if (removeKeys.length !== want.length) return { error: `could not match every lane to remove on ${mv.label}; it carries: ${(c.lanes || []).map(k => k.split('||')[0] + ' (' + k.split('||')[1] + ')').join(', ')}` };
          changes.push(`remove ${removeKeys.map(k => k.split('||')[0]).join(', ')}`);
        }
        if (!changes.length) return { error: `nothing to change on ${mv.label}` };
        return { label: mv.label, uid: mv.uid, c, changes, addKeys, removeKeys };
      },
      async run(a, ctx) {
        const pre = await this.prepare(a, ctx); if (pre.error) return pre;
        const c = pre.c;
        const lanes = (c.lanes || []).filter(k => !pre.removeKeys.includes(k)).concat(pre.addKeys);
        const body = fullBody(c, { reference: a.reference ? String(a.reference).trim() : c.reference, vessel: a.vessel || c.vessel, size_ft: a.size_ft || c.size_ft,
          transit_days: Number.isFinite(Number(a.transit_days)) ? Number(a.transit_days) : c.transit_days, lanes });
        const r = await callRoute(ctx, 'POST', '/consignments', body); if (!r.ok) return { error: r.error };
        return { ok: true, receipt: `Changed ${pre.label}: ${pre.changes.join('; ')}`, undo: { kind: 'movement', body: fullBody(c, { lanes: c.lanes || [] }), label: pre.label } };
      },
      async undo(rec, ctx) {
        const r = await callRoute(ctx, 'POST', '/consignments', rec.body);
        return r.ok ? { ok: true, receipt: `${rec.label} is back as it was` } : { error: r.error };
      },
    },
    {
      name: 'issue_extra_labels',
      description: 'Print extra UID labels for one PO and SKU in a week (lost or torn labels, or a quantity change). The run is emailed to the warehouse as usual. Labels cannot be unprinted, so make sure the PO, SKU and quantity are exactly what the person said. The person must approve before it runs.',
      input_schema: { type: 'object', properties: { week: { type: 'string' }, po: { type: 'string' }, sku: { type: 'string' }, qty: { type: 'integer' }, reason: { type: 'string' } }, required: ['week', 'po', 'sku', 'qty'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Print ${a.qty} extra UID label${Number(a.qty) === 1 ? '' : 's'} for PO ${a.po}, SKU ${a.sku} in ${pre && pre.iso || a.week}${a.reason ? ` (${a.reason})` : ''} — this cannot be undone`,
      async prepare(a, ctx) {
        const ws = resolveWeek(a.week, ctx); if (!ws) return { error: 'could not read that week' };
        const qty = Math.floor(Number(a.qty)); if (!(qty >= 1 && qty <= 5000)) return { error: 'qty must be 1–5000' };
        const plan = await callRoute(ctx, 'GET', `/plan/weeks/${ws}`);
        const rows = plan.ok && Array.isArray(plan.data) ? plan.data : [];
        const po = String(a.po || '').trim().toUpperCase(), sku = String(a.sku || '').trim();
        const hit = rows.find(r => String(r.po_number || '').trim().toUpperCase() === po && String(r.sku_code || r.sku || '').trim() === sku);
        if (!hit) return { error: `PO ${po} with SKU ${sku} is not in the ${isoWeek(ws)} plan${rows.some(r => String(r.po_number || '').trim().toUpperCase() === po) ? ' (the PO is there with other SKUs)' : ''}` };
        return { ws, iso: isoWeek(ws), po, sku, qty };
      },
      async run(a, ctx) {
        const pre = await this.prepare(a, ctx); if (pre.error) return pre;
        const r = await callRoute(ctx, 'POST', '/uid-labels/extra', { week: pre.ws, po: pre.po, sku: pre.sku, qty: pre.qty, reason: a.reason ? `Pulse: ${a.reason}` : 'Pulse: requested by voice/chat' });
        if (!r.ok) return { error: r.error };
        return { ok: true, receipt: `Printed ${pre.qty} extra label${pre.qty === 1 ? '' : 's'} for PO ${pre.po} / ${pre.sku} in ${pre.iso} (run #${r.data.run_id}); the warehouse has been emailed` };
      },
    },
    {
      name: 'set_department_format',
      description: 'Set which UID label format a department code uses: footwear or standard. Codes are 4 characters (e.g. WFDA). Use when someone says a department is footwear, or a new code was flagged. The person must approve before it runs.',
      input_schema: { type: 'object', properties: { code: { type: 'string' }, format: { type: 'string', enum: ['footwear', 'standard'] } }, required: ['code', 'format'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Set department ${String(a.code || '').toUpperCase()} to ${a.format} labels${pre && pre.prev ? ` (currently ${pre.prev})` : (pre && pre.prev === null ? ' (new code)' : '')}`,
      async prepare(a, ctx) {
        const code = String(a.code || '').trim().toUpperCase(); if (!/^[A-Z0-9]{4}$/.test(code)) return { error: 'a department code is 4 letters/digits' };
        const r = await callRoute(ctx, 'GET', '/uid-labels/formats'); if (!r.ok) return { error: r.error };
        const rows = Array.isArray(r.data) ? r.data : (r.data.codes || r.data.formats || r.data.rows || []);
        const cur = rows.find(x => String(x.code).toUpperCase() === code);
        if (cur && cur.format === a.format) return { error: `${code} is already ${a.format}` };
        return { code, prev: cur ? cur.format : null };
      },
      async run(a, ctx) {
        const pre = await this.prepare(a, ctx); if (pre.error) return pre;
        const r = await callRoute(ctx, 'POST', '/uid-labels/formats', { code: pre.code, format: a.format }); if (!r.ok) return { error: r.error };
        return { ok: true, receipt: `Department ${pre.code} now prints ${a.format} labels`, undo: { kind: 'format', code: pre.code, prev: pre.prev } };
      },
      async undo(rec, ctx) {
        const r = rec.prev ? await callRoute(ctx, 'POST', '/uid-labels/formats', { code: rec.code, format: rec.prev }) : await callRoute(ctx, 'DELETE', `/uid-labels/formats/${rec.code}`);
        return r.ok ? { ok: true, receipt: rec.prev ? `${rec.code} is back to ${rec.prev}` : `${rec.code} mapping removed again` } : { error: r.error };
      },
    },
    {
      name: 'move_po',
      description: 'Move a purchase order from one week\'s plan to another (all its SKU lines). Refuses if any of it has already been received in the original week. UID labels already printed travel with the PO — nothing is reprinted. The person must approve before it runs.',
      input_schema: { type: 'object', properties: { po: { type: 'string' }, to_week: { type: 'string', description: 'YYYY-MM-DD Monday, "W48", "next week"' }, from_week: { type: 'string', description: 'optional; found from the plans if omitted' } }, required: ['po', 'to_week'] },
      tier: 'confirm', roles: ['admin'], write: true,
      summary: (a, pre) => `Move PO ${String(a.po || '').toUpperCase()}${pre ? ` (${pre.rows.length} line${pre.rows.length === 1 ? '' : 's'}, ${pre.units} units, ${pre.supplier})` : ''} from ${pre && pre.fromIso || a.from_week || '?'} to ${pre && pre.toIso || a.to_week}`,
      async prepare(a, ctx) {
        const po = String(a.po || '').trim().toUpperCase(); if (!po) return { error: 'PO required' };
        const to = resolveWeek(a.to_week, ctx); if (!to) return { error: 'could not read the destination week' };
        let from = a.from_week ? resolveWeek(a.from_week, ctx) : null;
        if (!from) {
          // Look back 6 weeks and ahead 6 for the PO.
          const base = mondayOf(ctx.today); const found = [];
          for (let i = -6; i <= 6; i++) { const ws = addDays(base, i * 7); if (ws === to) continue; const r = await callRoute(ctx, 'GET', `/plan/weeks/${ws}`); if (r.ok && Array.isArray(r.data) && r.data.some(x => String(x.po_number || '').trim().toUpperCase() === po)) found.push(ws); }
          if (!found.length) return { error: `PO ${po} is not in any plan within six weeks of today (other than ${isoWeek(to)})` };
          if (found.length > 1) return { error: `PO ${po} is in ${found.map(isoWeek).join(' and ')}; say which week to move it from` };
          from = found[0];
        }
        if (from === to) return { error: 'the PO is already in that week' };
        const [pa, pb, rr] = await Promise.all([callRoute(ctx, 'GET', `/plan/weeks/${from}`), callRoute(ctx, 'GET', `/plan/weeks/${to}`), callRoute(ctx, 'GET', `/receiving?weekStart=${from}`)]);
        if (!pa.ok || !pb.ok) return { error: (pa.error || pb.error) };
        const planA = Array.isArray(pa.data) ? pa.data : [], planB = Array.isArray(pb.data) ? pb.data : [];
        const rows = planA.filter(x => String(x.po_number || '').trim().toUpperCase() === po);
        if (!rows.length) return { error: `PO ${po} is not in the ${isoWeek(from)} plan` };
        if (planB.some(x => String(x.po_number || '').trim().toUpperCase() === po)) return { error: `PO ${po} is already in the ${isoWeek(to)} plan` };
        const rec = (rr.ok ? (Array.isArray(rr.data) ? rr.data : (rr.data.rows || [])) : []).filter(x => String(x.po_number || '').trim().toUpperCase() === po);
        if (rec.length) return { error: `PO ${po} has receiving recorded in ${isoWeek(from)} (${rec.reduce((s, x) => s + (Number(x.cartons_received) || 0), 0)} cartons); it cannot be moved` };
        const units = rows.reduce((s, x) => s + (Number(x.target_qty) || 0), 0);
        return { po, from, to, fromIso: isoWeek(from), toIso: isoWeek(to), rows, planA, planB, units, supplier: String(rows[0].supplier_name || rows[0].supplier || '').trim() || 'unknown supplier' };
      },
      async run(a, ctx) {
        const pre = await this.prepare(a, ctx); if (pre.error) return pre;
        const facility = (q) => { const f = String(((pre.rows[0] || {}).facility_name || (pre.rows[0] || {}).facility || '')).trim(); return f ? `?facility=${encodeURIComponent(f)}` : ''; };
        // Destination first: if that save fails nothing has left the original week.
        const rb = await callRoute(ctx, 'PUT', `/plan/weeks/${pre.to}${facility()}`, pre.planB.concat(pre.rows)); if (!rb.ok) return { error: `could not add to ${pre.toIso}: ${rb.error}` };
        const ra = await callRoute(ctx, 'PUT', `/plan/weeks/${pre.from}${facility()}`, pre.planA.filter(x => String(x.po_number || '').trim().toUpperCase() !== pre.po));
        if (!ra.ok) { await callRoute(ctx, 'PUT', `/plan/weeks/${pre.to}${facility()}`, pre.planB); return { error: `could not remove from ${pre.fromIso}: ${ra.error} (destination restored)` }; }
        // A lane left with no POs stays on whatever container it was assigned to; say so.
        const laneKey = `${pre.supplier}||${String(pre.rows[0].zendesk_ticket || '').trim()}`;
        const laneLeft = pre.planA.some(x => String(x.po_number || '').trim().toUpperCase() !== pre.po && `${String(x.supplier_name || x.supplier || '').trim()}||${String(x.zendesk_ticket || '').trim()}` === laneKey);
        return { ok: true, receipt: `Moved PO ${pre.po} (${pre.units} units, ${pre.supplier}) from ${pre.fromIso} to ${pre.toIso}${laneLeft ? '' : `; the ${pre.supplier} lane in ${pre.fromIso} now has no POs — check its container in Transit Movements`}`,
          undo: { kind: 'move_po', po: pre.po, from: pre.from, to: pre.to, planA: pre.planA, planB: pre.planB, facility: facility() } };
      },
      async undo(rec, ctx) {
        const ra = await callRoute(ctx, 'PUT', `/plan/weeks/${rec.from}${rec.facility}`, rec.planA); if (!ra.ok) return { error: ra.error };
        const rb = await callRoute(ctx, 'PUT', `/plan/weeks/${rec.to}${rec.facility}`, rec.planB); if (!rb.ok) return { error: rb.error };
        return { ok: true, receipt: `PO ${rec.po} is back in ${isoWeek(rec.from)}` };
      },
    },
    // ── Phase 3c: Pulse as an actor — tasks and its own mailbox ──
    {
      name: 'list_tasks',
      description: 'Pulse\'s task list for this client: things handed to Pulse or to people in chat and meetings, with owner, due date, status (open, waiting for a reply, replied, done) and any email replies received. Use for "what is outstanding", "did Kerry reply", "what did you send".',
      input_schema: { type: 'object', properties: { status: { type: 'string', description: 'open (default) | done | cancelled | all' } } },
      tier: 'read', roles: ['admin'], internalOnly: true,
      async run(a, ctx) {
        const r = await callRoute(ctx, 'GET', `/pulse/tasks?status=${encodeURIComponent(a.status || 'open')}`); if (!r.ok) return { error: r.error };
        return { count: (r.data.tasks || []).length, open: r.data.open, today: ctx.today,
          tasks: take((r.data.tasks || []).map(t => ({ id: t.id, title: t.title, owner: t.owner, status: t.status, due: t.due_at, about: t.about, created: String(t.created_at).slice(0, 10), outcome: t.outcome || null,
            overdue: !!(t.due_at && String(t.due_at).slice(0, 10) < ctx.today && ['open', 'waiting', 'replied'].includes(t.status)),
            emails: (t.emails || []).map(e => ({ to: e.to, subject: e.subject, status: e.status, sent: e.sent_at ? String(e.sent_at).slice(0, 16) : null })),
            replies: (t.replies || []).map(x => ({ from: x.from_addr, at: String(x.received_at).slice(0, 16), text: String(x.text || '').slice(0, 400) })) })), 40) };
      },
    },
    {
      name: 'create_task',
      description: 'Record a task: something for Pulse or a person to do, with an optional due date and what it is about. Use for "remind me to…", "add a task…", "Yash to chase…", or an action agreed in a meeting that cannot be done right now. If the task is an email Pulse should send, use send_email instead.',
      input_schema: { type: 'object', properties: { title: { type: 'string' }, detail: { type: 'string' }, owner: { type: 'string', description: '"pulse", "me", or a person\'s name (default pulse)' }, due: { type: 'string', description: 'YYYY-MM-DD or YYYY-MM-DDTHH:MM' }, about: { type: 'string', description: 'movement reference, PO or week it concerns' } }, required: ['title'] },
      tier: 'auto', roles: ['admin'], internalOnly: true, write: true,
      summary: (a) => `Add task “${a.title}”${a.owner && a.owner !== 'pulse' ? ` for ${a.owner}` : ''}${a.due ? ` due ${fmtWhen(a.due)}` : ''}`,
      async run(a, ctx) {
        const title = String(a.title || '').trim(); if (!title) return { error: 'title required' };
        const owner = !a.owner || /^pulse$/i.test(a.owner) ? 'pulse' : (/^me$/i.test(a.owner) ? (ctx.userName || 'me') : String(a.owner).trim());
        const r = await callRoute(ctx, 'POST', '/pulse/tasks', { title, detail: a.detail || null, owner, due_at: a.due || null, about: a.about || null, source: ctx.style === 'spoken' ? 'voice' : 'chat', created_by_name: ctx.userName || null });
        if (!r.ok) return { error: r.error };
        const t = r.data.task;
        return { ok: true, receipt: `Task #${t.id}: ${t.title}${owner !== 'pulse' ? ` — ${owner}` : ''}${t.due_at ? `, due ${fmtWhen(t.due_at)}` : ''}`, task_id: t.id, undo: { kind: 'task_cancel', id: t.id } };
      },
      async undo(rec, ctx) { const r = await callRoute(ctx, 'PATCH', `/pulse/tasks/${rec.id}`, { status: 'cancelled', outcome: 'Undone' }); return r.ok ? { ok: true, receipt: `Task #${rec.id} removed` } : { error: r.error }; },
    },
    {
      name: 'update_task',
      description: 'Close, reopen, reassign or re-date a task from list_tasks: status done | cancelled | open, with an outcome line; or a new owner or due date.',
      input_schema: { type: 'object', properties: { id: { type: 'integer' }, status: { type: 'string', enum: ['done', 'cancelled', 'open'] }, outcome: { type: 'string' }, owner: { type: 'string' }, due: { type: 'string' } }, required: ['id'] },
      tier: 'auto', roles: ['admin'], internalOnly: true, write: true,
      summary: (a) => `${a.status === 'done' ? 'Close' : a.status === 'cancelled' ? 'Cancel' : 'Update'} task #${a.id}${a.outcome ? `: ${a.outcome}` : ''}`,
      async run(a, ctx) {
        const body = {}; if (a.status) body.status = a.status; if (a.outcome !== undefined) body.outcome = a.outcome; if (a.owner) body.owner = a.owner; if (a.due) body.due_at = a.due;
        const r = await callRoute(ctx, 'PATCH', `/pulse/tasks/${Number(a.id)}`, body); if (!r.ok) return { error: r.error };
        const t = r.data.task;
        return { ok: true, receipt: `Task #${t.id} ${t.status === 'done' ? 'closed' : t.status === 'cancelled' ? 'cancelled' : 'updated'}: ${t.title}`, undo: { kind: 'task_restore', id: t.id, previous: r.data.previous } };
      },
      async undo(rec, ctx) { const p = rec.previous || {}; const r = await callRoute(ctx, 'PATCH', `/pulse/tasks/${rec.id}`, { status: p.status || 'open', outcome: p.outcome || '', due_at: p.due_at || null, owner: p.owner || 'pulse' }); return r.ok ? { ok: true, receipt: `Task #${rec.id} is back to ${p.status || 'open'}` } : { error: r.error }; },
    },
    {
      name: 'send_email',
      description: 'Send an email from Pulse\'s own mailbox (pulseAI@velozity.au) on behalf of the person asking — to a client, partner or colleague at a known address. Write a short, plain, professional body; do not add a greeting signature, Pulse signs it. Pulse holds the message for 60 seconds before it goes, so the person can say "Pulse, stop". Only addresses at known client, partner or VelOzity domains are accepted. Always state recipient and gist in your reply.',
      input_schema: { type: 'object', properties: { to: { type: 'array', items: { type: 'string' } }, cc: { type: 'array', items: { type: 'string' } }, subject: { type: 'string' }, body: { type: 'string' }, about: { type: 'string', description: 'movement reference, PO or week this concerns' }, task_id: { type: 'integer', description: 'existing task this email belongs to, if any' } }, required: ['to', 'subject', 'body'] },
      tier: 'auto', roles: ['admin'], internalOnly: true, write: true, hold: true,
      summary: (a) => `Email ${(a.to || []).join(', ')}: “${a.subject}”`,
      async run(a, ctx) {
        const r = await callRoute(ctx, 'POST', '/pulse/email', { to: a.to, cc: a.cc || [], subject: a.subject, body: a.body, about: a.about || null, task_id: a.task_id || null, source: ctx.style === 'spoken' ? 'voice' : 'chat', created_by_name: ctx.userName || null });
        if (!r.ok) return { error: r.error };
        const d = r.data;
        return { ok: true, receipt: `Emailing ${d.to.join(', ')}${d.cc && d.cc.length ? ` (cc ${d.cc.join(', ')})` : ''}: “${d.subject}” — sends in ${d.hold_seconds} seconds unless you say stop. Task #${d.task_id}.`,
          hold_until: d.hold_until, task_id: d.task_id, undo: { kind: 'email_recall', id: d.email_id, task_id: d.task_id } };
      },
      async undo(rec, ctx) { const r = await callRoute(ctx, 'POST', `/pulse/email/${rec.id}/recall`, {}); return r.ok ? { ok: true, receipt: 'Stopped — the email was not sent' } : { error: r.error }; },
    },
  ];

  // ── Write-tool helpers ──
  const STAGE_LABEL = { packing_list_ready: 'packing list ready', origin_cleared: 'origin cleared', departed: 'departure', arrived: 'arrival', dest_cleared: 'destination clearance', fc_receipt: 'FC receipt' };
  const ALERT_LABEL = { fc_moved: 'delivery date moved', unconfirmed: 'milestone unconfirmed', no_quote: 'no carrier quote' };
  const fmtShort = (ymd) => { const s = String(ymd || '').slice(0, 10); const d = new Date(s + 'T00:00:00Z'); return isNaN(d) ? s : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); };
  const fmtWhen = (dt) => { const s = String(dt || ''); return s.length > 10 ? `${fmtShort(s)} ${s.slice(11, 16)}` : fmtShort(s); };
  const localNow = () => { const p = new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(new Date()); const g = (t) => (p.find(x => x.type === t) || {}).value; return `${g('year')}-${g('month')}-${g('day')}T${g('hour') === '24' ? '00' : g('hour')}:${g('minute')}`; };

  // Lanes named by supplier or ticket → lane keys from the week's plan (via week status).
  async function resolveLanes(ctx, ws, mode, names) {
    const want = (Array.isArray(names) ? names : [names]).map(x => String(x || '').trim()).filter(Boolean);
    if (!want.length) return { error: 'at least one lane (supplier or ticket) is required' };
    const st = await callRoute(ctx, 'GET', `/consignments/week-status?week=${ws}`); if (!st.ok) return { error: st.error };
    const lanes = (st.data.lanes || []).filter(l => !mode || String(l.freight || '').toLowerCase() === String(mode).toLowerCase() || !l.freight);
    const keys = [], labels = [], missing = [];
    for (const w of want) {
      const wl = w.toLowerCase();
      const hit = lanes.filter(l => String(l.ticket || '').toLowerCase() === wl || String(l.supplier || '').toLowerCase() === wl || String(l.supplier || '').toLowerCase().startsWith(wl));
      if (hit.length === 1) { keys.push(hit[0].key); labels.push(`${hit[0].supplier} (${hit[0].ticket})`); }
      else if (hit.length > 1) return { error: `“${w}” matches ${hit.length} lanes in ${isoWeek(ws)}: ${hit.map(l => l.supplier + ' ' + l.ticket).join(', ')} — give the ticket` };
      else missing.push(w);
    }
    if (missing.length) return { error: `no ${mode || ''} lane in the ${isoWeek(ws)} plan for: ${missing.join(', ')}`, lanes_in_week: take(lanes.map(l => `${l.supplier} (${l.ticket}, ${l.freight})`), 12) };
    let facility = '';
    try { const plan = await callRoute(ctx, 'GET', `/plan/weeks/${ws}`); facility = (Array.isArray(plan.data) ? plan.data : []).map(r => String(r.facility_name || r.facility || '').trim()).find(Boolean) || ''; } catch (_) {}
    if (!facility) { const cs = st.data.consignments || []; facility = (cs.find(c => c.facility) || {}).facility || 'VOZ_KY'; }
    return { keys, labels, facility };
  }
  // The consignment POST overwrites every field it knows, so an amendment must resend them all.
  function fullBody(c, patch) {
    return Object.assign({ consignment_uid: c.consignment_uid, week_start: c.week_start, facility: c.facility, mode: c.mode, reference: c.reference, size_ft: c.size_ft, carrier: c.carrier,
      vessel: c.vessel, transit_days: c.transit_days, departure_planned: c.departure_planned, shipment_ref: c.shipment_ref, hbl: c.hbl, mbl: c.mbl, status: c.status, lanes: c.lanes || [] }, patch);
  }
  // A movement named any way a person would: uid, container, AWB, ticket. One match or an
  // error that lists the candidates — a write must never guess between two containers.
  async function resolveMovement(ctx, text) {
    const q = String(text || '').trim(); if (!q) return { error: 'which movement? give a container number, AWB, ticket or uid' };
    const get = async (uid) => { const r = await callRoute(ctx, 'GET', `/consignments/${encodeURIComponent(uid)}`); return r.ok ? r.data : null; };
    let d = /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(q) || /^cg[_-]/i.test(q) ? await get(q) : null;
    if (!d) {
      const f = await callRoute(ctx, 'GET', `/consignments/find?q=${encodeURIComponent(q)}`);
      if (!f.ok) return { error: f.error };
      let hits = Array.isArray(f.data) ? f.data : (f.data.hits || f.data.results || []);
      const exact = hits.filter(h => String(h.reference || '').trim().toLowerCase() === q.toLowerCase());
      if (exact.length) hits = exact;
      if (!hits.length) return { error: `no movement matches “${q}” in the last 10 weeks` };
      if (hits.length > 1) return { error: `“${q}” matches ${hits.length} movements; say which`, candidates: take(hits.map(h => ({ uid: h.uid || h.consignment_uid, reference: h.reference, mode: h.mode, week: h.week_start, iso: isoWeek(h.week_start) })), 6) };
      d = await get(hits[0].uid || hits[0].consignment_uid);
      if (!d) return { error: 'could not load that movement' };
    }
    const c = d.consignment || d;
    return { uid: c.consignment_uid, c, tracking: d.tracking || null, reference: c.reference || '', week: c.week_start, facility: c.facility || '', mode: c.mode,
      label: c.reference ? `${c.mode === 'Air' ? 'flight' : 'container'} ${c.reference}` : `the unadvised ${String(c.mode || '').toLowerCase()} movement in ${isoWeek(c.week_start)}` };
  }
  // Evidence that a stage happened, or null. The rule agreed 21 Nov: Auto-tier confirms only
  // where tracking (or a recorded delivery) shows it; the two origin stages run to schedule.
  async function corroboration(ctx, mv, stage) {
    if (stage === 'packing_list_ready' || stage === 'origin_cleared') return 'origin schedule';
    const evs = ((mv.tracking && mv.tracking.events) || []).map(e => String(e.event || ''));
    const has = (re) => evs.some(e => re.test(e));
    if (stage === 'departed' && has(/vessel_departed$/)) return 'tracking shows the vessel departed';
    if (stage === 'arrived' && has(/vessel_arrived$|vessel_discharged$/)) return 'tracking shows the vessel arrived';
    if (stage === 'dest_cleared' && has(/\.available$|full_out$/)) return 'tracking shows the container released';
    if (stage === 'fc_receipt') { const rc = await readReceipts(ctx, mv); const r = !rc.error && rc.receipts[mv.uid]; if (r && r.delivery_local) return `delivered ${fmtShort(r.delivery_local)} per last mile`; }
    return null;
  }
  async function readReceipts(ctx, mv) {
    if (!mv.facility) return { error: `${mv.label} has no facility recorded, so its last-mile record cannot be found` };
    const r = await callRoute(ctx, 'GET', `/flow/week/${mv.week}?facility=${encodeURIComponent(mv.facility)}`);
    if (!r.ok) return { error: r.error };
    const data = (r.data && r.data.data) || {};
    const receipts = (data.lastmile_receipts && typeof data.lastmile_receipts === 'object') ? data.lastmile_receipts : {};
    return { receipts, data };
  }
  async function writeReceipts(ctx, mv, receipts) {
    const r = await callRoute(ctx, 'POST', `/flow/week/${mv.week}?facility=${encodeURIComponent(mv.facility)}`, { lastmile_receipts: receipts });
    return r.ok ? { ok: true } : { error: r.error };
  }
  async function restoreReceipt(ctx, rec) {
    const mv = { uid: rec.uid, week: rec.week, facility: rec.facility, label: rec.uid };
    const rc = await readReceipts(ctx, mv); if (rc.error) return rc;
    if (rec.prev) rc.receipts[rec.uid] = rec.prev; else delete rc.receipts[rec.uid];
    const w = await writeReceipts(ctx, mv, rc.receipts); if (w.error) return w;
    return { ok: true, receipt: 'Put the last-mile record back as it was' };
  }
  async function restoreMilestone(ctx, rec) {
    const body = rec.prev && rec.prev.state !== 'assumed' && rec.prev.actual_at
      ? { stage: rec.stage, actual_at: rec.prev.actual_at, note: rec.prev.note || undefined }
      : { stage: rec.stage, clear: true };
    const r = await callRoute(ctx, 'POST', `/consignments/${encodeURIComponent(rec.uid)}/milestone`, body);
    return r.ok ? { ok: true, receipt: body.clear ? `${STAGE_LABEL[rec.stage]} is back to planned (unconfirmed)` : `${STAGE_LABEL[rec.stage]} is back to ${fmtShort(rec.prev.actual_at)}` } : { error: r.error };
  }

  // Week text → Monday. Accepts a date, "W46", "this week", "last week".
  function resolveWeek(text, ctx) {
    const s = String(text || '').trim().toLowerCase();
    if (!s || s === 'this week' || s === 'current') return mondayOf(ctx.today);
    if (s === 'last week' || s === 'previous week') return addDays(mondayOf(ctx.today), -7);
    if (s === 'next week') return addDays(mondayOf(ctx.today), 7);
    if (ymdOk(s)) return mondayOf(s);
    return weekFromText(s, ctx.today);
  }

  // ── Role filter: the only tools the model is ever shown ──
  // ctx.role is Pinpoint's label (Admin, Ops, Client, Facility/partner); ctx.isInternal is
  // the org type. A tool is visible only if both pass; the route guard still runs on call.
  function roleKey(ctx) {
    const r = String(ctx.role || '').toLowerCase();
    if (!ctx.isInternal) return ctx.isPartner ? 'partner' : 'client';
    if (r.startsWith('admin')) return 'admin';
    return 'ops';
  }
  // PULSE_WRITES=off on Render hides every write tool at once — the kill switch for Phase 3.
  const writesOff = () => /^(off|0|false|no)$/i.test(String(process.env.PULSE_WRITES || ''));
  function toolsFor(ctx) {
    const rk = roleKey(ctx);
    // noInternalOnly: a meeting room may have a client in it, so finance never reaches the model there.
    return TOOLS.filter(t => (!t.internalOnly || (ctx.isInternal && !ctx.noInternalOnly)) && t.roles.includes(rk) && !(t.write && writesOff()));
  }
  function anthropicTools(ctx) {
    return toolsFor(ctx).map(t => ({ name: t.name, description: t.description, input_schema: t.input_schema }));
  }

  // ── Run one tool ──
  async function runTool(name, args, ctx) {
    const t = toolsFor(ctx).find(x => x.name === name);
    if (!t) return { error: `tool ${name} is not available to this user` };
    const started = Date.now();
    try {
      const out = await t.run(args || {}, ctx);
      const text = JSON.stringify(out);
      // Hard cap on what goes back to the model. Trimmed shapes above keep this rare.
      const MAX = 14000;
      const result = text.length > MAX ? { truncated: true, note: 'result cut at ' + MAX + ' characters; ask a narrower question', preview: text.slice(0, MAX) } : out;
      return { ok: !out || !out.error, result, ms: Date.now() - started, tier: t.tier };
    } catch (e) {
      logger.warn('[pulse-tools] ' + name + ' failed:', e.message);
      return { ok: false, result: { error: String(e.message || e) }, ms: Date.now() - started, tier: t.tier };
    }
  }

  // ── Spoken mode ──
  // In a meeting the answer is heard, not read. Short, no tables, references spelled so a
  // listener can write them down, numbers rounded unless the exact figure is the point.
  const SPOKEN_GUIDE = [
    '## Spoken mode',
    'Your answer will be read aloud by a voice. Write for the ear:',
    '- Two or three short sentences. Lead with the answer, then one reason or one next step. Never more than 60 words unless asked for detail.',
    '- No markdown, no bullet points, no tables, no headings, no emoji.',
    '- Spell container numbers and AWBs letter by letter with spaces, digits in groups of three or four: "M S K U, nine nine nine, zero zero zero one". Zendesk tickets as digits: "eight three one seven nine". Weeks as "week forty-six".',
    '- Round units to the nearest hundred unless the exact count is what was asked. Say dates as "the sixteenth of October".',
    '- If something is planned and not yet confirmed, say "planned" or "awaiting confirmation", never "done".',
    '- If you do not have it, say so in one sentence and name where it would be.',
  ].join('\n');

  // Reference spelling for text-to-speech, applied to the final spoken string so the model
  // does not have to get it right every time.
  function speakable(text) {
    const digits = (d) => d.split('').join(' ');
    return String(text || '')
      // Container numbers: four letters, seven digits → "M S K U, 9 9 9, 0 0 0 1"
      .replace(/\b([A-Z]{4})(\d{3})(\d{4})\b/g, (m, a, d1, d2) => `${a.split('').join(' ')}, ${digits(d1)}, ${digits(d2)}`)
      // Zendesk-style 5–6 digit tickets → spaced digits
      .replace(/\b(\d{5,6})\b/g, (m, d) => digits(d))
      .replace(/\bW(\d{1,2})\b/g, (m, n) => 'week ' + n)
      .replace(/[*_#`>]/g, '');
  }

  // How Pulse writes when it is answering an EMAIL rather than a chat (Phase 3d).
  const EMAIL_GUIDE = (persona) => [
    'EMAIL MODE. You are replying to an email sent to Pulse\'s mailbox. ' + (persona && persona.as === 'client'
      ? `The sender is a client user at ${persona.client || 'the client'}; answer only from that client\'s data. `
      : 'The sender is a VelOzity colleague or partner replying on a thread Pulse started. ') +
    'Write 2–6 short plain sentences, no markdown, no bullet symbols, no tables, no greeting and no sign-off (Pulse adds those). State facts with their dates. Spell references normally (MSKU1234567), not letter by letter.',
    'You can only READ. If the email asks for a change (rebooking, confirming, amending, labels, invoices) or for something you cannot answer from the tools, say in one sentence that you have passed it to the VelOzity team, and end your reply with the exact token [[HANDOVER]] on its own line. Never promise an action, never invent a figure, never mention tools or Pinpoint internals.',
    'If the email is just an acknowledgement or thanks with no question, reply with exactly [[NOREPLY]].',
  ].join('\n');
  // In a meeting (Phase 4): spoken, brief, and nothing is changed live.
  const MEETING_GUIDE = (p) => [
    `MEETING MODE. You are a participant in a ${p.title ? `meeting "${p.title}"` : 'meeting'} for ${p.client || 'the client'}${Array.isArray(p.attendees) && p.attendees.length ? ` with ${p.attendees.slice(0, 8).join(', ')}` : ''}. ${p.speaker ? `${p.speaker} just addressed you.` : ''}`,
    'Answer in ONE or TWO short sentences for the ear; offer "I can put the detail in the notes" rather than listing. Figures rounded, dates as day and month, references spoken plainly.',
    'Nothing changes during the meeting. If asked to do something, the tool will return {proposed:true}: reply with one short sentence that it is noted and will be confirmed in Pinpoint after the meeting. Never say it is done.',
    'Finance, margins and costs are never discussed in a meeting.',
    p.context ? `What was said in the last ${20} minutes (oldest first):\n${p.context}` : 'Nothing has been said yet.',
  ].join('\n');
  const findTool = (name, ctx) => toolsFor(ctx).find(x => x.name === name) || null;
  // Undo a previous run, through the tool that made it.
  async function undoTool(name, record, ctx) {
    const t = findTool(name, ctx);
    if (!t || typeof t.undo !== 'function') return { ok: false, result: { error: 'this action cannot be undone' } };
    const started = Date.now();
    try { const out = await t.undo(record, ctx); return { ok: !out || !out.error, result: out, ms: Date.now() - started }; }
    catch (e) { return { ok: false, result: { error: String(e.message || e) }, ms: Date.now() - started }; }
  }
  return { TOOLS, toolsFor, anthropicTools, runTool, undoTool, findTool, resolveWeek, isoWeek, mondayOf, SPOKEN_GUIDE, EMAIL_GUIDE, MEETING_GUIDE, speakable, callRoute };
};
