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
  ];

  // ── Write-tool helpers ──
  const STAGE_LABEL = { packing_list_ready: 'packing list ready', origin_cleared: 'origin cleared', departed: 'departure', arrived: 'arrival', dest_cleared: 'destination clearance', fc_receipt: 'FC receipt' };
  const ALERT_LABEL = { fc_moved: 'delivery date moved', unconfirmed: 'milestone unconfirmed', no_quote: 'no carrier quote' };
  const fmtShort = (ymd) => { const s = String(ymd || '').slice(0, 10); const d = new Date(s + 'T00:00:00Z'); return isNaN(d) ? s : d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }); };
  const fmtWhen = (dt) => { const s = String(dt || ''); return s.length > 10 ? `${fmtShort(s)} ${s.slice(11, 16)}` : fmtShort(s); };
  const localNow = () => { const p = new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(new Date()); const g = (t) => (p.find(x => x.type === t) || {}).value; return `${g('year')}-${g('month')}-${g('day')}T${g('hour') === '24' ? '00' : g('hour')}:${g('minute')}`; };

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
  function toolsFor(ctx) {
    const rk = roleKey(ctx);
    return TOOLS.filter(t => (!t.internalOnly || ctx.isInternal) && t.roles.includes(rk));
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

  const findTool = (name, ctx) => toolsFor(ctx).find(x => x.name === name) || null;
  // Undo a previous run, through the tool that made it.
  async function undoTool(name, record, ctx) {
    const t = findTool(name, ctx);
    if (!t || typeof t.undo !== 'function') return { ok: false, result: { error: 'this action cannot be undone' } };
    const started = Date.now();
    try { const out = await t.undo(record, ctx); return { ok: !out || !out.error, result: out, ms: Date.now() - started }; }
    catch (e) { return { ok: false, result: { error: String(e.message || e) }, ms: Date.now() - started }; }
  }
  return { TOOLS, toolsFor, anthropicTools, runTool, undoTool, findTool, resolveWeek, isoWeek, mondayOf, SPOKEN_GUIDE, speakable, callRoute };
};
