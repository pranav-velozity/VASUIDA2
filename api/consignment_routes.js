/* ── VelOzity Pinpoint — consignments v1 ──
   Dates move from the lane to the movement that actually carries it.

   Why this exists. Today every lane holds its own six dates, so twelve lanes sitting in three
   containers means the same departure is typed twelve times. Nobody does that honestly — they
   accept the pre-filled value and save. The result is 157 recorded departures with zero
   variance: every "actual" equal to the plan, and no way to see the ETD slips that are the
   whole operational problem.

   One consignment is one movement: a 20ft, a 40ft, a Tuesday flight, a Thursday flight. Four
   in a week, not twelve. Lanes attach to one and inherit its dates.

   Three things this model insists on:

     · a stage is `assumed` until a person or a carrier says otherwise. Assumed is visible,
       never silently promoted to actual. That single distinction is what makes planned-versus-
       actual reporting mean anything.

     · transit time belongs to the sailing, not the facility. Two carriers on the same lane in
       the same week differ; so does the same carrier week to week. Arrival is computed from
       THIS consignment's departure plus ITS OWN transit days.

     · the origin rhythm is a rule, not an estimate. Packing Friday, cleared Monday, departs
       Wednesday is a schedule you run to. Stored as configurable offsets per mode rather than
       hardcoded, because air does not keep sea's rhythm.

   Lanes are not split across consignments, by convention rather than by constraint: the rare
   air part-shipment is allowed and flagged, because blocking it is what drives people to
   invent a second lane and falsify the record.
*/
'use strict';

const { randomUUID } = require('crypto');

module.exports = function mountConsignments(deps) {
  const { express, db, authenticateRequest, requireRole, auditLog, curClient } = deps;
  // Optional, passed by server.js. Absent, the notify route fails closed rather than open.
  const { requireInternalOrg, sendViaResend } = deps;
  const router = express.Router();

  // ── Schema ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS consignment (
      consignment_uid TEXT PRIMARY KEY,          -- surrogate: container numbers are reused,
                                                 -- reallocated, and sometimes not yet advised
      client_id       TEXT NOT NULL,
      week_start      TEXT NOT NULL,
      facility        TEXT,
      mode            TEXT NOT NULL CHECK(mode IN ('Sea','Air')),
      reference       TEXT,                      -- container number or AWB, as people read it
      size_ft         TEXT,                      -- 20 / 40 / 40HQ, blank for air
      carrier         TEXT,
      vessel          TEXT,                      -- vessel and voyage, or flight
      transit_days    REAL,                      -- carrier-quoted, THIS sailing, port to port
      departure_planned TEXT,                    -- air: the flight date, when not in the reference
      transit_confirmed INTEGER NOT NULL DEFAULT 0,  -- 0 = still the rule default, not a carrier quote
      baseline_fc_at  TEXT,                      -- the FC date the plan promised at the outset
      baseline_transit_days REAL,                -- the first quote entered; later ones are deviations
      -- What the carrier currently expects. Kept on the consignment rather than written into
      -- a milestone's planned date, because the plan is recomputed whenever anything changes
      -- and a value written into it is erased on the next pass.
      carrier_etd     TEXT,
      carrier_eta     TEXT,
      carrier_est_at  TEXT,
      requote_count   INTEGER NOT NULL DEFAULT 0,
      requote_log     TEXT,
      shipment_ref    TEXT,
      hbl             TEXT,
      mbl             TEXT,                      -- what an aggregator subscribes with
      status          TEXT NOT NULL DEFAULT 'planned',  -- planned | booked | closed
      created_at      TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_consignment_week ON consignment(client_id, week_start);
    CREATE INDEX IF NOT EXISTS idx_consignment_ref  ON consignment(reference);

    -- A lane rides exactly one consignment. Not enforced as a hard constraint: the rare air
    -- part-shipment is recorded and flagged rather than refused.
    CREATE TABLE IF NOT EXISTS consignment_lane (
      consignment_uid TEXT NOT NULL,
      lane_key        TEXT NOT NULL,
      week_start      TEXT NOT NULL,
      client_id       TEXT NOT NULL,
      PRIMARY KEY (consignment_uid, lane_key, week_start)
    );
    CREATE INDEX IF NOT EXISTS idx_cl_lane ON consignment_lane(lane_key, week_start);

    -- One row per stage per consignment. The state is the point: an assumed date and a
    -- confirmed one look the same on screen and mean entirely different things in a report.
    CREATE TABLE IF NOT EXISTS consignment_milestone (
      consignment_uid TEXT NOT NULL,
      stage           TEXT NOT NULL CHECK(stage IN (
                        'packing_list_ready','origin_cleared','departed',
                        'arrived','dest_cleared','fc_receipt')),
      planned_at      TEXT,
      actual_at       TEXT,
      state           TEXT NOT NULL DEFAULT 'assumed' CHECK(state IN (
                        'assumed',       -- the plan says so; nobody has looked
                        'confirmed',     -- a person agreed it happened on the planned day
                        'amended',       -- a person gave a different date
                        'carrier')),     -- an aggregator event said so
      source_user     TEXT,
      source_detail   TEXT,              -- carrier event id, or a note
      recorded_at     TEXT,
      PRIMARY KEY (consignment_uid, stage)
    );

    -- The origin rhythm, per facility and mode. Offsets in days from the Monday of the
    -- execution week: Friday is 4, the Monday after is 7, Wednesday after is 9.
    CREATE TABLE IF NOT EXISTS consignment_rules (
      client_id                 TEXT NOT NULL,
      facility                  TEXT NOT NULL,
      mode                      TEXT NOT NULL CHECK(mode IN ('Sea','Air')),
      packing_list_offset_days  REAL NOT NULL,
      origin_cleared_offset_days REAL NOT NULL,
      departed_offset_days      REAL NOT NULL,
      arrived_to_dest_cleared_days REAL NOT NULL,
      dest_cleared_to_fc_days   REAL NOT NULL,
      default_transit_days      REAL,            -- a starting point only; never an actual
      before_departure          TEXT,            -- air: JSON {cleared, packing} days before the flight
      updated_at                TEXT,
      PRIMARY KEY (client_id, facility, mode)
    );
  `);

  // ── Columns added after the table first existed ──
  // CREATE TABLE IF NOT EXISTS does nothing to a table that is already there, so every column
  // added since the first deploy has to be applied on its own. Without this, a database
  // created last week has none of them and saving fails with "no such column".
  function addColumn(table, column, decl) {
    try {
      const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
      if (!cols.includes(column)) {
        db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${decl}`);
        console.log(`[consignments] added ${table}.${column}`);
      }
    } catch (e) {
      console.warn(`[consignments] could not add ${table}.${column}:`, e.message);
    }
  }

  addColumn('consignment', 'transit_confirmed', 'INTEGER NOT NULL DEFAULT 0');
  addColumn('consignment', 'departure_planned', 'TEXT');
  addColumn('consignment', 'baseline_fc_at', 'TEXT');
  addColumn('consignment', 'baseline_transit_days', 'REAL');
  addColumn('consignment', 'requote_count', 'INTEGER NOT NULL DEFAULT 0');
  addColumn('consignment', 'requote_log', 'TEXT');
  addColumn('consignment', 'carrier_etd', 'TEXT');
  addColumn('consignment', 'carrier_eta', 'TEXT');
  addColumn('consignment', 'carrier_est_at', 'TEXT');
  addColumn('consignment_rules', 'before_departure', 'TEXT');

  // ── Where the movement runs ──
  // Sea ports come from tracking (port of loading / port of discharge) the first time the
  // shipment is reported, and transshipment ports from the transshipment events. Nobody types
  // them. Air has no tracking, so it runs on a fixed default unless these are set.
  addColumn('consignment', 'origin_port', 'TEXT');        // UN/LOCODE, e.g. CNYTN
  addColumn('consignment', 'origin_port_name', 'TEXT');
  addColumn('consignment', 'dest_port', 'TEXT');
  addColumn('consignment', 'dest_port_name', 'TEXT');
  addColumn('consignment', 'via_ports', 'TEXT');          // JSON [{code, name}]
  addColumn('consignment', 'ports_at', 'TEXT');

  db.exec(`
    -- Every revision of the carrier's expectation, with the value it replaced. The consignment
    -- holds only the latest, so without this "was Mon 5 Oct" and the overnight replay on the
    -- Transit Movements screen have nothing to read.
    CREATE TABLE IF NOT EXISTS consignment_estimate_log (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      consignment_uid TEXT NOT NULL,
      field           TEXT NOT NULL,      -- carrier_etd | carrier_eta
      old_value       TEXT,
      new_value       TEXT,
      source          TEXT,
      recorded_at     TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cel_uid ON consignment_estimate_log(consignment_uid, id);

    -- What was actually sent to a client about a movement — the text as sent, not the draft,
    -- so the record matches what they received.
    CREATE TABLE IF NOT EXISTS consignment_notification (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id       TEXT NOT NULL,
      consignment_uid TEXT NOT NULL,
      sent_at         TEXT NOT NULL,
      sent_by         TEXT,
      to_json         TEXT NOT NULL,
      subject         TEXT,
      body            TEXT,
      attached        INTEGER NOT NULL DEFAULT 0,
      resend_id       TEXT,
      status          TEXT NOT NULL,      -- sent | failed
      error           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_cn_uid ON consignment_notification(consignment_uid, id);

    -- Addresses offered next time, per client.
    CREATE TABLE IF NOT EXISTS notify_recipient (
      client_id     TEXT NOT NULL,
      email         TEXT NOT NULL,
      uses          INTEGER NOT NULL DEFAULT 0,
      last_used_at  TEXT,
      PRIMARY KEY (client_id, email)
    );
  `);

  // Anything migrated before the quote was tracked carries a rule default, not a carrier
  // figure, and must stay locked until somebody enters the real one. The default for the
  // column does that; this only makes the intent explicit for rows that predate it.
  try {
    db.prepare(`UPDATE consignment SET transit_confirmed = 0
                 WHERE transit_confirmed IS NULL`).run();
  } catch (_) { /* nothing to do on a fresh table */ }

  const STAGES = ['packing_list_ready', 'origin_cleared', 'departed', 'arrived', 'dest_cleared', 'fc_receipt'];

  // Sea reflects the rhythm as described: packing Friday (+4), cleared the Monday after (+7),
  // departs Wednesday (+9). Air is seeded from the same shape and is meant to be corrected —
  // a flight cut-off is not Wednesday. Seeded rather than guessed silently.
  const DEFAULT_RULES = {
    // Sea: offsets forward from the Monday of the execution week. Packing Friday (+4),
    // cleared the Monday after (+7), departs Wednesday (+9). A rhythm, not an estimate.
    Sea: { packing: 4, cleared: 7, departed: 9, arrDest: 2, destFc: 1, transit: 28 },
    // Air: offsets BACKWARD from the flight. Cleared the day before, packing list the day
    // before that. Departure is entered rather than derived, because it moves.
    Air: { beforeDeparture: { cleared: 1, packing: 2 }, arrDest: 1, destFc: 1, transit: 2 },
  };

  // CA1306/25SEP, SQ7823/23SEP — the flight date is in the reference. Read rather than asked
  // for again, since re-typing something already on screen is how it ends up wrong.
  function departureFromReference(ref, weekStart) {
    const m = String(ref || '').match(/\/(\d{1,2})\s*([A-Za-z]{3})/);
    if (!m) return null;
    const MONTHS = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
                     jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };
    const mon = MONTHS[m[2].toLowerCase()];
    if (mon == null) return null;
    const day = Number(m[1]);
    // The year is not in the reference. Take the week's year, and roll forward if that puts
    // the flight before the week — a late-December week flying in January.
    const base = new Date(String(weekStart) + 'T00:00:00Z');
    let year = base.getUTCFullYear();
    let d = new Date(Date.UTC(year, mon, day));
    if (d < base) d = new Date(Date.UTC(year + 1, mon, day));
    return d.toISOString().slice(0, 10);
  }

  const addDays = (ymd, n) => {
    if (!ymd || n == null) return null;
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(d)) return null;
    d.setUTCDate(d.getUTCDate() + Math.round(n));
    return d.toISOString().slice(0, 10);
  };

  function rulesFor(client, facility, mode) {
    const row = db.prepare(`SELECT * FROM consignment_rules
                             WHERE client_id = ? AND facility = ? AND mode = ?`)
      .get(client, facility || '', mode);
    if (row) {
      if (row.before_departure) {
        try { row.before_departure = JSON.parse(row.before_departure); } catch (_) { row.before_departure = null; }
      }
      return row;
    }
    const d = DEFAULT_RULES[mode] || DEFAULT_RULES.Sea;
    if (mode === 'Air') {
      return {
        before_departure: d.beforeDeparture,
        arrived_to_dest_cleared_days: d.arrDest,
        dest_cleared_to_fc_days: d.destFc,
        default_transit_days: d.transit,
        _default: true,
      };
    }
    return {
      packing_list_offset_days: d.packing,
      origin_cleared_offset_days: d.cleared,
      departed_offset_days: d.departed,
      arrived_to_dest_cleared_days: d.arrDest,
      dest_cleared_to_fc_days: d.destFc,
      default_transit_days: d.transit,
      _default: true,
    };
  }

  /**
   * Planned dates for a consignment. The origin three come from the rhythm; arrival comes
   * from THIS sailing's departure plus ITS transit; the landside two follow arrival.
   *
   * Departure is taken from the actual where one has been recorded — a ship that left three
   * days late arrives three days late unless someone says otherwise, and a plan that ignores
   * that is worse than no plan.
   */
  function computePlanned(c, milestones) {
    const r = rulesFor(c.client_id, c.facility, c.mode);
    const ws = c.week_start;
    const m = milestones || {};

    const planned = {};
    const air = c.mode === 'Air';

    if (air) {
      // Anchored on the flight: whatever was entered, or whatever the reference says.
      const flight = (m.departed && m.departed.actual_at)
        || c.departure_planned || departureFromReference(c.reference, ws);
      planned.departed = flight || null;
      const back = (r.before_departure && typeof r.before_departure === 'object')
        ? r.before_departure : DEFAULT_RULES.Air.beforeDeparture;
      planned.origin_cleared = flight ? addDays(flight, -(back.cleared ?? 1)) : null;
      planned.packing_list_ready = flight ? addDays(flight, -(back.packing ?? 2)) : null;
    } else {
      planned.packing_list_ready = addDays(ws, r.packing_list_offset_days);
      planned.origin_cleared = addDays(ws, r.origin_cleared_offset_days);
      // The rhythm says Wednesday; the carrier says when the ship actually sails. Where they
      // disagree the carrier wins, because the rhythm is a planning convenience and the
      // carrier is looking at the vessel.
      planned.departed = c.carrier_etd || addDays(ws, r.departed_offset_days);
    }

    const departedReal = (m.departed && m.departed.actual_at) || null;
    const departBasis = departedReal || planned.departed;

    // Arrival follows the quoted transit from whenever departure actually is — so a three-day
    // ETD slip moves the arrival by three days without anyone touching the quote. A carrier
    // ETA, where there is one, is more specific and takes precedence.
    const transit = Number(c.transit_days);
    planned.arrived = c.carrier_eta
      || (Number.isFinite(transit) ? addDays(departBasis, transit) : null);

    const arrivedReal = (m.arrived && m.arrived.actual_at) || null;
    const arriveBasis = arrivedReal || planned.arrived;
    planned.dest_cleared = arriveBasis ? addDays(arriveBasis, r.arrived_to_dest_cleared_days) : null;
    planned.fc_receipt = planned.dest_cleared
      ? addDays(planned.dest_cleared, r.dest_cleared_to_fc_days) : null;

    return { planned, rules: r, departure_basis: departedReal ? 'actual' : 'plan' };
  }

  function milestonesFor(uid) {
    const rows = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ?').all(uid);
    const out = {};
    for (const r of rows) out[r.stage] = r;
    return out;
  }

  const upsertMilestone = db.prepare(`
    INSERT INTO consignment_milestone
      (consignment_uid, stage, planned_at, actual_at, state, source_user, source_detail, recorded_at)
    VALUES (@uid, @stage, @planned, @actual, @state, @user, @detail, @at)
    ON CONFLICT(consignment_uid, stage) DO UPDATE SET
      planned_at = @planned, actual_at = @actual, state = @state,
      source_user = @user, source_detail = @detail, recorded_at = @at`);

  // Planned dates are refreshed whenever anything they depend on changes. Never touches a
  // stage that a person or a carrier has spoken for.
  function refreshPlanned(uid) {
    const c = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ?').get(uid);
    if (!c) return;
    const m = milestonesFor(uid);
    const { planned } = computePlanned(c, m);

    // Set once, and only once the transit time is a carrier quote rather than a rule default.
    // Freezing it against the default made entering the real quote look like a two-day gain
    // before anything had happened — the baseline has to be the first plan anyone promised,
    // not the first one the system guessed.
    if (!c.baseline_fc_at && c.transit_confirmed && planned.fc_receipt) {
      db.prepare(`UPDATE consignment SET baseline_fc_at = ?, baseline_transit_days = ?
                   WHERE consignment_uid = ?`)
        .run(planned.fc_receipt, c.transit_days, uid);
      c.baseline_fc_at = planned.fc_receipt;
      c.baseline_transit_days = c.transit_days;
    }
    for (const stage of STAGES) {
      const existing = m[stage];
      if (existing && existing.state !== 'assumed') {
        // Keep the recorded fact; only the plan beside it moves.
        upsertMilestone.run({ uid, stage, planned: planned[stage] || null,
          actual: existing.actual_at, state: existing.state,
          user: existing.source_user, detail: existing.source_detail, at: existing.recorded_at });
      } else {
        upsertMilestone.run({ uid, stage, planned: planned[stage] || null,
          actual: null, state: 'assumed', user: null, detail: null, at: null });
      }
    }
  }

  function shape(c) {
    const m = milestonesFor(c.consignment_uid);
    const { planned, departure_basis } = computePlanned(c, m);
    const lanes = db.prepare('SELECT lane_key FROM consignment_lane WHERE consignment_uid = ?')
      .all(c.consignment_uid).map(x => x.lane_key);

    // A lane on more than one consignment in the same week: allowed, surfaced, not blocked.
    const shared = lanes.filter(k => {
      const n = db.prepare(`SELECT COUNT(*) n FROM consignment_lane
                             WHERE lane_key = ? AND week_start = ?`).get(k, c.week_start);
      return n && n.n > 1;
    });

    return {
      ...c,
      lanes,
      split_lanes: shared,
      departure_basis,
      milestones: STAGES.map(stage => {
        const row = m[stage] || {};
        return {
          stage,
          planned_at: planned[stage] || row.planned_at || null,
          actual_at: row.actual_at || null,
          state: row.state || 'assumed',
          source_user: row.source_user || null,
          recorded_at: row.recorded_at || null,
        };
      }),
      // Quoted is what the carrier's schedule said at booking; achieved is what the confirmed
      // dates show. Kept apart on purpose — an aggregator's revised ETA must never overwrite
      // the quote, or the carrier's own revision erases the evidence of their slip.
      transit: (() => {
        const quoted = Number.isFinite(Number(c.transit_days)) ? Number(c.transit_days) : null;
        const baseline = Number.isFinite(Number(c.baseline_transit_days)) ? Number(c.baseline_transit_days) : null;
        const dep = m.departed && m.departed.actual_at;
        const arr = m.arrived && m.arrived.actual_at;
        const achieved = (dep && arr)
          ? Math.round((new Date(arr + 'T00:00:00Z') - new Date(dep + 'T00:00:00Z')) / 86400000)
          : null;
        let log = [];
        try { log = JSON.parse(c.requote_log || '[]') || []; } catch (_) {}
        return {
          quoted, achieved, baseline,
          // Against the first promise, which is what the client experienced.
          variance: (baseline != null && achieved != null) ? achieved - baseline : null,
          // And against the quote in force now, which is what the carrier last said.
          variance_to_current: (quoted != null && achieved != null) ? achieved - quoted : null,
          requotes: Number(c.requote_count) || 0,
          requote_days: (baseline != null && quoted != null) ? quoted - baseline : null,
          requote_log: log,
        };
      })(),

      // How much the promised FC date has moved, and whether the inputs behind it are real.
      // A plan built on a rule default rather than a carrier quote is low confidence however
      // little it has drifted — the number it rests on was never a promise.
      eta_fc: planned.fc_receipt || null,
      carrier: (c.carrier_etd || c.carrier_eta)
        ? { etd: c.carrier_etd || null, eta: c.carrier_eta || null, as_at: c.carrier_est_at || null }
        : null,
      baseline_fc_at: c.baseline_fc_at || null,
      confidence: (() => {
        if (!c.transit_confirmed) return { level: 'unverified', drift: null,
          why: 'transit time is still the default, not a carrier quote' };
        const base = c.baseline_fc_at, now = planned.fc_receipt;
        if (!base || !now) return { level: 'unknown', drift: null, why: 'no FC date yet' };
        const drift = Math.round((new Date(now + 'T00:00:00Z') - new Date(base + 'T00:00:00Z')) / 86400000);
        const level = Math.abs(drift) <= 1 ? 'on plan'
          : (drift <= 4 && drift > 0 ? 'slipping' : (drift > 4 ? 'at risk' : 'ahead'));
        return { level, drift,
          why: drift === 0 ? 'tracking the original plan'
             : `${Math.abs(drift)} day${Math.abs(drift) === 1 ? '' : 's'} ${drift > 0 ? 'later' : 'earlier'} than first planned` };
      })(),

      // What is missing before this can be tracked or invoiced.
      transit_confirmed: !!c.transit_confirmed,
      needs: [
        !c.reference ? 'reference' : null,
        (!c.transit_confirmed || !Number.isFinite(Number(c.transit_days))) ? 'transit_days' : null,
        c.status === 'booked' && !c.mbl ? 'mbl' : null,
        c.status === 'booked' && !c.hbl ? 'hbl' : null,
        c.status === 'booked' && !c.shipment_ref ? 'shipment_ref' : null,
      ].filter(Boolean),
    };
  }

  // ── Read ──
  router.get('/', authenticateRequest, auditLog('view_consignments'), (req, res) => {
    try {
      const ws = String(req.query.week || '').trim();
      const client = curClient();
      const rows = ws
        ? db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND week_start = ?
                       ORDER BY mode, reference`).all(client, ws)
        : db.prepare(`SELECT * FROM consignment WHERE client_id = ?
                       ORDER BY week_start DESC, mode, reference LIMIT 200`).all(client);
      res.json({ ok: true, week_start: ws || null, consignments: rows.map(shape) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Create or update ──
  router.post('/', authenticateRequest, requireRole(['admin', 'supplier']),
    auditLog('save_consignment'), (req, res) => {
    try {
      const b = req.body || {};
      const client = curClient();
      const uid = String(b.consignment_uid || '').trim() || randomUUID();
      const mode = b.mode === 'Air' ? 'Air' : 'Sea';
      const ws = String(b.week_start || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) {
        return res.status(400).json({ ok: false, error: 'week_start must be YYYY-MM-DD.' });
      }

      // Transit time is asked for at setup, because it belongs to the sailing and nobody
      // knows it later. The rule default is offered, never applied silently.
      const r = rulesFor(client, b.facility, mode);
      const transit = Number.isFinite(Number(b.transit_days)) ? Number(b.transit_days) : null;

      const exists = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ?').get(uid);

      // A changed quote after the baseline is set is a deviation from the plan, logged with
      // who changed it and when. The baseline itself never moves.
      if (exists && exists.baseline_transit_days != null && transit != null
          && Number(transit) !== Number(exists.transit_days)) {
        let log = [];
        try { log = JSON.parse(exists.requote_log || '[]') || []; } catch (_) { log = []; }
        log.push({ from: exists.transit_days, to: transit, at: new Date().toISOString(),
                   by: (req.auth && req.auth.userId) || null });
        db.prepare('UPDATE consignment SET requote_count = ?, requote_log = ? WHERE consignment_uid = ?')
          .run(log.length, JSON.stringify(log.slice(-20)), uid);
      }

      if (exists) {
        db.prepare(`UPDATE consignment SET facility=@facility, mode=@mode, reference=@reference,
                      size_ft=@size, carrier=@carrier, vessel=@vessel, transit_days=@transit,
                      transit_confirmed=@tconf, departure_planned=@depPlanned,
                      shipment_ref=@shipment, hbl=@hbl, mbl=@mbl, status=@status, updated_at=@now
                     WHERE consignment_uid=@uid`)
          .run({ uid, facility: b.facility || null, mode, reference: b.reference || null,
                 size: b.size_ft || null, carrier: b.carrier || null, vessel: b.vessel || null,
                 transit,
                 // Entering a transit time IS confirming it: the field only gets a value when
                 // somebody puts the carrier's figure in.
                 tconf: transit != null ? 1 : 0,
                 depPlanned: b.departure_planned || null,
                 shipment: b.shipment_ref || null, hbl: b.hbl || null, mbl: b.mbl || null,
                 status: b.status || 'planned', now: new Date().toISOString() });
      } else {
        db.prepare(`INSERT INTO consignment (consignment_uid, client_id, week_start, facility, mode,
                      reference, size_ft, carrier, vessel, transit_days, transit_confirmed,
                      departure_planned, shipment_ref, hbl, mbl, status, updated_at)
                    VALUES (@uid,@client,@ws,@facility,@mode,@reference,@size,@carrier,@vessel,
                            @transit,@tconf,@depPlanned,@shipment,@hbl,@mbl,@status,@now)`)
          .run({ uid, client, ws, facility: b.facility || null, mode, reference: b.reference || null,
                 size: b.size_ft || null, carrier: b.carrier || null, vessel: b.vessel || null,
                 transit: transit == null ? (r.default_transit_days || null) : transit,
                 // A transit time supplied on create is a quote; one filled in from the rule
                 // default is not, and must not unlock recording.
                 tconf: transit != null ? 1 : 0,
                 depPlanned: b.departure_planned || null,
                 shipment: b.shipment_ref || null, hbl: b.hbl || null, mbl: b.mbl || null,
                 status: b.status || 'planned', now: new Date().toISOString() });
      }

      if (Array.isArray(b.lanes)) {
        db.prepare('DELETE FROM consignment_lane WHERE consignment_uid = ?').run(uid);
        const ins = db.prepare(`INSERT OR IGNORE INTO consignment_lane
                                  (consignment_uid, lane_key, week_start, client_id)
                                VALUES (?,?,?,?)`);
        for (const k of b.lanes) if (String(k || '').trim()) ins.run(uid, String(k).trim(), ws, client);
      }

      refreshPlanned(uid);
      const row = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ?').get(uid);
      res.json({ ok: true, consignment: shape(row) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Confirm or amend a milestone ──
  // The only way a date becomes actual. Confirming takes the planned date as fact; amending
  // supplies a different one. Both are deliberate; neither can happen by saving a form.
  router.post('/:uid/milestone', authenticateRequest, requireRole(['admin', 'supplier']),
    auditLog('confirm_milestone'), (req, res) => {
    try {
      const uid = String(req.params.uid || '').trim();
      const b = req.body || {};
      const stage = String(b.stage || '').trim();
      if (!STAGES.includes(stage)) return res.status(400).json({ ok: false, error: 'Unknown stage.' });

      const c = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ? AND client_id = ?')
        .get(uid, curClient());
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });

      // Nothing can be confirmed until the movement is actually described. Confirming a
      // stage against a date computed from a default transit records a fact that was never
      // planned — which is how the old screen filled up with dates nobody believed.
      if (!c.transit_confirmed && !b.clear) {
        return res.status(409).json({ ok: false, error: 'details_required',
          message: 'Enter this consignment\u2019s details first — the carrier\u2019s quoted transit time, '
                 + 'and the shipment references. Until then the planned dates are a guess.',
          missing: [
            !c.transit_confirmed ? 'transit_days' : null,
            !c.reference ? 'reference' : null,
          ].filter(Boolean) });
      }

      const m = milestonesFor(uid);
      const { planned } = computePlanned(c, m);
      const user = (req.auth && req.auth.userId) || null;
      const now = new Date().toISOString();

      if (b.clear) {
        // Back to assumed. A mistaken confirmation must be undoable, or people stop confirming.
        upsertMilestone.run({ uid, stage, planned: planned[stage] || null, actual: null,
          state: 'assumed', user: null, detail: null, at: null });
      } else {
        const amended = String(b.actual_at || '').trim();
        const actual = amended || planned[stage];
        if (!actual) return res.status(400).json({ ok: false, error: 'No date to record for this stage.' });
        upsertMilestone.run({
          uid, stage, planned: planned[stage] || null, actual: actual.slice(0, 10),
          state: amended && amended.slice(0, 10) !== (planned[stage] || '') ? 'amended' : 'confirmed',
          user, detail: b.note || null, at: now,
        });
      }

      // A real departure or arrival moves everything downstream that nobody has spoken for.
      refreshPlanned(uid);
      const row = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ?').get(uid);
      res.json({ ok: true, consignment: shape(row) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Rules ──
  router.get('/rules', authenticateRequest, (req, res) => {
    const client = curClient();
    const rows = db.prepare('SELECT * FROM consignment_rules WHERE client_id = ?').all(client);
    res.json({ ok: true, rules: rows, defaults: DEFAULT_RULES,
      note: 'Air offsets are seeded from the sea rhythm and are meant to be corrected — a flight cut-off is not Wednesday.' });
  });

  router.put('/rules', authenticateRequest, requireRole(['admin']), auditLog('save_consignment_rules'), (req, res) => {
    try {
      const b = req.body || {};
      const mode = b.mode === 'Air' ? 'Air' : 'Sea';
      db.prepare(`INSERT INTO consignment_rules (client_id, facility, mode,
                    packing_list_offset_days, origin_cleared_offset_days, departed_offset_days,
                    arrived_to_dest_cleared_days, dest_cleared_to_fc_days, default_transit_days,
                    before_departure, updated_at)
                  VALUES (@client,@facility,@mode,@p,@o,@d,@ad,@df,@t,@bd,@now)
                  ON CONFLICT(client_id, facility, mode) DO UPDATE SET
                    packing_list_offset_days=@p, origin_cleared_offset_days=@o,
                    departed_offset_days=@d, arrived_to_dest_cleared_days=@ad,
                    dest_cleared_to_fc_days=@df, default_transit_days=@t,
                    before_departure=@bd, updated_at=@now`)
        .run({ client: curClient(), facility: b.facility || '', mode,
               p: Number(b.packing_list_offset_days) || 0, o: Number(b.origin_cleared_offset_days) || 0,
               d: Number(b.departed_offset_days) || 0, ad: Number(b.arrived_to_dest_cleared_days),
               df: Number(b.dest_cleared_to_fc_days),
               t: b.default_transit_days == null ? null : Number(b.default_transit_days),
               bd: b.before_departure ? JSON.stringify(b.before_departure) : null,
               now: new Date().toISOString() });
      res.json({ ok: true });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── The worklist ──
  // What needs a person, derived rather than flagged. On a quiet day it is empty, and an empty
  // list is the signal that nothing needs you.
  router.get('/worklist', authenticateRequest, auditLog('view_consignment_worklist'), (req, res) => {
    try {
      const today = new Date().toISOString().slice(0, 10);
      const rows = db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND status != 'closed'
                                ORDER BY week_start DESC LIMIT 200`).all(curClient());
      const items = [];
      for (const c of rows) {
        const s = shape(c);
        for (const ms of s.milestones) {
          if (ms.state !== 'assumed') continue;
          if (!ms.planned_at) continue;
          if (ms.planned_at > today) continue;          // not due yet; nothing to do
          const daysOver = Math.round(
            (new Date(today + 'T00:00:00Z') - new Date(ms.planned_at + 'T00:00:00Z')) / 86400000);
          items.push({
            consignment_uid: c.consignment_uid,
            reference: c.reference || 'not advised',
            mode: c.mode,
            week_start: c.week_start,
            stage: ms.stage,
            planned_at: ms.planned_at,
            days_overdue: daysOver,
            // A stage days past its plan with nobody confirming is either late or unrecorded.
            severity: daysOver >= 3 ? 'overdue' : (daysOver >= 1 ? 'due' : 'today'),
          });
        }
        for (const need of s.needs) {
          items.push({ consignment_uid: c.consignment_uid, reference: c.reference || 'not advised',
            mode: c.mode, week_start: c.week_start, stage: null, missing: need, severity: 'incomplete' });
        }
      }
      items.sort((a, b) => (b.days_overdue || 0) - (a.days_overdue || 0));
      res.json({ ok: true, as_at: today, count: items.length, items });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Migration from the week blob ──
  // Containers already live in flow_week under intl_weekcontainers, each carrying the lane
  // keys it holds. That relationship is what makes this migration possible without anyone
  // re-keying a week.
  //
  // The existing dates are NOT imported as actuals. Every one of them equals the plan — they
  // were written by the mirror when a pre-filled form was saved, not observed. Importing them
  // as facts would carry the original problem into the new model and make the first twelve
  // weeks of reporting as meaningless as the last twelve. They come in as `assumed`, which is
  // what they always were.
  function migrateWeek(ws, opts) {
    const client = curClient();
    const dry = !!(opts && opts.dryRun);
    const rows = db.prepare('SELECT facility, data FROM flow_week WHERE week_start = ?').all(ws);

    const made = [], skipped = [], lanesSeen = new Map();
    for (const r of rows) {
      let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
      const wc = blob && blob.intl_weekcontainers;
      const list = Array.isArray(wc) ? wc : (Array.isArray(wc && wc.containers) ? wc.containers : []);

      for (const c of list) {
        const ref = String(c.container_id || c.container || '').trim();
        const laneKeys = Array.isArray(c.lane_keys) ? c.lane_keys.filter(Boolean) : [];
        if (!ref && !laneKeys.length) { skipped.push({ reason: 'no reference and no lanes' }); continue; }

        // Air is identified the way the rest of the codebase does it: the reference, or a
        // size field carrying 'AIR' rather than a box size.
        const air = /air|awb|^[A-Z]{2}\d{3,4}\//i.test(ref)
          || String(c.size_ft || '').toUpperCase().includes('AIR');
        const mode = air ? 'Air' : 'Sea';

        const existing = db.prepare(`SELECT consignment_uid FROM consignment
                                      WHERE client_id = ? AND week_start = ? AND reference IS ?`)
          .get(client, ws, ref || null);
        if (existing) { skipped.push({ reference: ref, reason: 'already migrated' }); continue; }

        const uid = randomUUID();
        const rule = rulesFor(client, r.facility, mode);

        if (!dry) {
          db.prepare(`INSERT INTO consignment (consignment_uid, client_id, week_start, facility,
                        mode, reference, size_ft, carrier, vessel, transit_days, status, updated_at)
                      VALUES (@uid,@client,@ws,@facility,@mode,@ref,@size,@carrier,@vessel,@transit,'planned',@now)`)
            .run({ uid, client, ws, facility: r.facility || null, mode, ref: ref || null,
                   size: air ? null : (c.size_ft || null), carrier: c.carrier || null,
                   vessel: c.vessel || null,
                   // The rule default, not a measured transit. Flagged in `needs` until
                   // somebody enters the real one for this sailing.
                   transit: rule.default_transit_days || null, now: new Date().toISOString() });

          const ins = db.prepare(`INSERT OR IGNORE INTO consignment_lane
                                    (consignment_uid, lane_key, week_start, client_id) VALUES (?,?,?,?)`);
          for (const k of laneKeys) ins.run(uid, String(k).trim(), ws, client);
          refreshPlanned(uid);
        }

        for (const k of laneKeys) lanesSeen.set(k, (lanesSeen.get(k) || 0) + 1);
        made.push({ consignment_uid: dry ? null : uid, reference: ref || 'not advised',
                    mode, lanes: laneKeys.length, transit_days_defaulted: rule.default_transit_days || null });
      }
    }

    // A lane on two containers in the same week is the rare air split. Reported, not refused.
    const split = [...lanesSeen.entries()].filter(([, n]) => n > 1).map(([k]) => k);

    // Lanes in the week that no container claims: they would silently have no dates at all,
    // which is worse than the current state. Named so they can be assigned.
    const orphans = [];
    for (const r of rows) {
      let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
      const lanes = (blob && blob.intl_lanes && typeof blob.intl_lanes === 'object') ? Object.keys(blob.intl_lanes) : [];
      for (const k of lanes) if (!lanesSeen.has(k)) orphans.push(k);
    }

    return {
      week_start: ws, dry_run: dry,
      consignments_created: made.length, consignments: made,
      skipped, split_lanes: split, unassigned_lanes: orphans,
      note: 'Existing lane dates were not imported as actuals: they are copies of the plan, '
          + 'not observations. Every stage starts assumed.',
    };
  }

  router.post('/migrate', authenticateRequest, requireRole(['admin']),
    auditLog('migrate_consignments'), (req, res) => {
    try {
      const ws = String((req.body && req.body.week_start) || req.query.week || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) {
        return res.status(400).json({ ok: false, error: 'week_start must be YYYY-MM-DD.' });
      }
      const dry = String((req.body && req.body.dry_run) ?? req.query.dry_run ?? 'true') !== 'false';
      res.json({ ok: true, ...migrateWeek(ws, { dryRun: dry }) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Clearing the legacy dates ──
  // The 297 rows in lane_actual_dates are copies of the plan, written by the mirror when a
  // pre-filled form was saved. Left in place they would sit alongside real confirmations and
  // be indistinguishable in a report, so they come out — along with the same dates in the
  // week's plan blob, which is where the pre-fill came from.
  //
  // Per week, dry run by default. Doing a week, checking the transit screen, then proceeding
  // is recoverable; clearing everything at once is not.
  function clearLegacy(ws, opts) {
    const dry = !(opts && opts.confirm === true);
    const client = curClient();

    const actuals = db.prepare(`SELECT lane_key, stage, actual_at, source
                                  FROM lane_actual_dates WHERE week_start = ?`).all(ws);
    const bySource = {};
    for (const a of actuals) bySource[a.source] = (bySource[a.source] || 0) + 1;

    // Lane date fields inside the week blob, which is what pre-filled the form.
    const FIELDS = ['packing_list_ready_at', 'origin_customs_cleared_at', 'departed_at',
                    'arrived_at', 'dest_customs_cleared_at', 'eta_fc'];
    const rows = db.prepare('SELECT facility, data FROM flow_week WHERE week_start = ?').all(ws);
    let blobDates = 0;
    const blobPlan = [];
    for (const r of rows) {
      let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
      const lanes = (blob && blob.intl_lanes && typeof blob.intl_lanes === 'object') ? blob.intl_lanes : {};
      for (const [k, lane] of Object.entries(lanes)) {
        const hit = FIELDS.filter(f => lane && lane[f]);
        if (hit.length) { blobDates += hit.length; blobPlan.push({ facility: r.facility, lane_key: k, fields: hit }); }
      }
    }

    if (!dry) {
      const tx = db.transaction(() => {
        db.prepare('DELETE FROM lane_actual_dates WHERE week_start = ?').run(ws);
        for (const r of rows) {
          let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
          const lanes = (blob && blob.intl_lanes && typeof blob.intl_lanes === 'object') ? blob.intl_lanes : null;
          if (!lanes) continue;
          let touched = false;
          for (const lane of Object.values(lanes)) {
            for (const f of FIELDS) if (lane && lane[f]) { delete lane[f]; touched = true; }
          }
          // Only the six date fields are removed. Shipment refs, notes, customs holds and
          // everything invoicing reads are left exactly as they are.
          if (touched) {
            db.prepare('UPDATE flow_week SET data = ?, updated_at = ? WHERE facility = ? AND week_start = ?')
              .run(JSON.stringify(blob), new Date().toISOString(), r.facility, ws);
          }
        }
      });
      tx();
    }

    return {
      week_start: ws, dry_run: dry,
      lane_actual_rows: actuals.length, by_source: bySource,
      plan_blob_dates: blobDates, plan_blob_lanes: blobPlan,
      preserved: 'container records, shipment refs, notes, customs holds, and everything invoicing reads',
      note: dry ? 'Nothing removed. Send confirm: true to apply.' : 'Removed.',
    };
  }

  router.post('/legacy/clear', authenticateRequest, requireRole(['admin']),
    auditLog('clear_legacy_lane_dates'), (req, res) => {
    try {
      const b = req.body || {};
      const ws = String(b.week_start || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) {
        return res.status(400).json({ ok: false, error: 'week_start must be YYYY-MM-DD.' });
      }
      res.json({ ok: true, ...clearLegacy(ws, { confirm: b.confirm === true }) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ════════════════════════════════════════════════════════════════════════════════════════
  // Transit Movements — the screen that replaced the Live Map.
  //
  // Everything here is read-only except the notification send. Nothing in shape() changed:
  // the screen's extra facts are assembled by enrich(), used only by /board, so the Transit &
  // Clearing worklist and every other reader of this router get byte-identical responses.
  // ════════════════════════════════════════════════════════════════════════════════════════

  const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const MO = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const ymdOk = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));
  const fmtDay = (ymd) => {
    if (!ymd) return null;
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(d)) return String(ymd);
    return `${WD[d.getUTCDay()]} ${d.getUTCDate()} ${MO[d.getUTCMonth()]}`;
  };
  const dayDiff = (a, b) => {
    if (!a || !b) return null;
    const x = new Date(String(a).slice(0, 10) + 'T00:00:00Z');
    const y = new Date(String(b).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(x) || isNaN(y)) return null;
    return Math.round((y - x) / 86400000);
  };
  // Planning happens in Sydney. The worklist uses UTC; for a last-free-day countdown the
  // difference is a whole day for half of every working day, so this one is local.
  const BOARD_TZ = process.env.BOARD_TZ || 'Australia/Sydney';
  const todayLocal = () => {
    try { return new Intl.DateTimeFormat('en-CA', { timeZone: BOARD_TZ }).format(new Date()); }
    catch (_) { return new Date().toISOString().slice(0, 10); }
  };
  const mondayOf = (ymd) => {
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    const dow = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - dow);
    return d.toISOString().slice(0, 10);
  };

  // The weekly VAS model's tables (flow_week, records) carry no client_id: they are ICONIC's.
  // Anything read from them is gated exactly the way the tenancy middleware gates their own
  // endpoints, so a door-to-door client never sees another client's week.
  function ownsWeekModel(client) {
    try {
      return !!db.prepare(`SELECT 1 x FROM client_capability
                            WHERE client_id = ? AND capability = 'week_hub' AND enabled = 1`).get(client);
    } catch (_) { return client === 'ICONIC'; }
  }

  // ── Ports ──
  const AIR_ORIGIN = { code: process.env.AIR_ORIGIN_CODE || 'SZX', name: process.env.AIR_ORIGIN_NAME || 'Shenzhen Bao\u2019an' };
  const AIR_DEST = { code: process.env.AIR_DEST_CODE || 'SYD', name: process.env.AIR_DEST_NAME || 'Sydney Airport' };

  function routeOf(c) {
    let via = [];
    try { via = JSON.parse(c.via_ports || '[]') || []; } catch (_) { via = []; }
    const o = (c.origin_port || c.origin_port_name)
      ? { code: c.origin_port || null, name: c.origin_port_name || c.origin_port } : null;
    const d = (c.dest_port || c.dest_port_name)
      ? { code: c.dest_port || null, name: c.dest_port_name || c.dest_port } : null;
    if (c.mode === 'Air') return { origin: o || AIR_ORIGIN, dest: d || AIR_DEST, via, source: (o || d) ? 'recorded' : 'default' };
    return { origin: o, dest: d, via, source: (o || d) ? 'tracking' : 'unknown' };
  }

  // Written by the tracking integration. Never blanks a value it already holds: a later
  // event that omits the port is not evidence the port changed.
  function recordPorts(uid, p) {
    try {
      const v = (x) => (x == null || String(x).trim() === '') ? null : String(x).trim();
      const o = v(p && p.origin), on = v(p && p.origin_name), d = v(p && p.dest), dn = v(p && p.dest_name);
      if (!o && !on && !d && !dn) return false;
      const r = db.prepare(`UPDATE consignment SET origin_port = COALESCE(?, origin_port),
                              origin_port_name = COALESCE(?, origin_port_name),
                              dest_port = COALESCE(?, dest_port),
                              dest_port_name = COALESCE(?, dest_port_name), ports_at = ?
                            WHERE consignment_uid = ?`)
        .run(o, on, d, dn, new Date().toISOString(), uid);
      return r.changes > 0;
    } catch (e) { console.warn('[consignments] recordPorts:', e.message); return false; }
  }

  function addViaPort(uid, port) {
    try {
      const code = port && port.code ? String(port.code).trim() : null;
      const name = port && port.name ? String(port.name).trim() : null;
      if (!code && !name) return false;
      const row = db.prepare('SELECT via_ports, origin_port, dest_port FROM consignment WHERE consignment_uid = ?').get(uid);
      if (!row) return false;
      // The ports at either end are not stops along the way.
      if (code && (code === row.origin_port || code === row.dest_port)) return false;
      let via = []; try { via = JSON.parse(row.via_ports || '[]') || []; } catch (_) { via = []; }
      if (via.some(x => (code && x.code === code) || (!code && name && x.name === name))) return false;
      via.push({ code, name: name || code });
      db.prepare('UPDATE consignment SET via_ports = ? WHERE consignment_uid = ?').run(JSON.stringify(via.slice(0, 6)), uid);
      return true;
    } catch (e) { console.warn('[consignments] addViaPort:', e.message); return false; }
  }

  // ── Estimate history ──
  // Called BEFORE the consignment is updated, so the value being replaced is still there.
  function logEstimate(uid, field, newValue, source) {
    try {
      if (field !== 'carrier_etd' && field !== 'carrier_eta') return false;
      if (!newValue) return false;
      const row = db.prepare(`SELECT ${field} v FROM consignment WHERE consignment_uid = ?`).get(uid);
      if (!row) return false;
      const old = row.v ? String(row.v).slice(0, 10) : null;
      const nv = String(newValue).slice(0, 10);
      if (old === nv) return false;
      db.prepare(`INSERT INTO consignment_estimate_log (consignment_uid, field, old_value, new_value, source, recorded_at)
                  VALUES (?,?,?,?,?,?)`).run(uid, field, old, nv, source || null, new Date().toISOString());
      return true;
    } catch (e) { console.warn('[consignments] logEstimate:', e.message); return false; }
  }

  // ── Terminal facts ──
  // Kept by the tracking integration in its own table. Read defensively: the table belongs to
  // another module and may not exist on a deployment without tracking.
  function terminalFor(uid) {
    try {
      const t = db.prepare(`SELECT state, pickup_lfd, holds, available_at, pod_terminal, last_context, updated_at
                              FROM t49_link WHERE consignment_uid = ?`).get(uid);
      if (!t) return null;
      let holds = [];
      try {
        const raw = JSON.parse(t.holds || '[]');
        holds = (Array.isArray(raw) ? raw : [])
          .filter(h => {
            if (!h) return false;
            if (typeof h !== 'object') return true;
            const st = String(h.status || '').toLowerCase();
            return !/(released|cleared|resolved|none)/.test(st);
          })
          .map(h => {
            const n = typeof h === 'object' ? String(h.name || h.description || h.type || 'hold') : String(h);
            return n.replace(/_/g, ' ').replace(/^./, ch => ch.toUpperCase());
          });
      } catch (_) { holds = []; }
      return {
        tracking: t.state === 'tracking', state: t.state || null,
        lfd: t.pickup_lfd || null, holds, available_at: t.available_at || null,
        terminal: t.pod_terminal || null, last_context: t.last_context || null, updated_at: t.updated_at || null,
      };
    } catch (_) { return null; }
  }

  // ── What happened, in words ──
  const STAGE_TEXT = {
    packing_list_ready: 'Packing list ready', origin_cleared: 'Origin cleared', departed: 'Departure',
    arrived: 'Arrival', dest_cleared: 'Destination clearance', fc_receipt: 'FC receipt',
  };
  const STAGE_SHORT = {
    packing_list_ready: 'packing list', origin_cleared: 'origin cleared', departed: 'departed',
    arrived: 'arrived', dest_cleared: 'available', fc_receipt: 'FC receipt',
  };
  const ACT_TEXT = { departed: 'Departed the port of loading', arrived: 'Arrived at the port of discharge', dest_cleared: 'Available for pickup' };

  function humanEvent(r) {
    const ev = String(r.event || ''), note = String(r.note || '');
    if (!note || /attributes updated|nothing to apply/.test(note) || ev.startsWith('tracking_request')) return null;
    if (ev === 'backfill') {
      const parts = note.split(' · ').map(p => p.split('=')).filter(p => p.length === 2 && ymdOk(p[1]));
      if (!parts.length) return null;
      return { kind: 'backfill', text: 'Tracking recorded ' + parts.map(([s, d]) => `${STAGE_SHORT[s] || s} ${fmtDay(d)}`).join(', ') };
    }
    const dates = note.match(/\d{4}-\d{2}-\d{2}/g) || [];
    if (/estimated/.test(ev)) {
      if (!dates.length) return null;
      return { estimate: true, kind: 'estimate', field: /departed/.test(ev) ? 'carrier_etd' : 'carrier_eta', new_value: dates[0],
               text: `${/departed/.test(ev) ? 'Departure' : 'Arrival'} estimate now ${fmtDay(dates[0])}` };
    }
    const m = note.match(/^(departed|arrived|dest_cleared) = (\d{4}-\d{2}-\d{2})/);
    if (m) return { kind: 'actual', stage: m[1], date: m[2], text: `${ACT_TEXT[m[1]]} ${fmtDay(m[2])}` };
    if (/^LFD |hold\(s\)/.test(note)) {
      const bits = [];
      const lfd = note.match(/LFD (\d{4}-\d{2}-\d{2})/);
      if (lfd) bits.push(`Last free day ${fmtDay(lfd[1])}`);
      const h = note.match(/(\d+) hold\(s\)/);
      if (h) bits.push(`${h[1]} hold${h[1] === '1' ? '' : 's'} reported at the terminal`);
      return bits.length ? { kind: h ? 'hold' : 'lfd', lfd: lfd ? lfd[1] : null, text: bits.join(' · ') } : null;
    }
    let t = note;
    for (const d of dates) t = t.replace(d, fmtDay(d));
    return { kind: /transshipment/.test(ev) ? 'transshipment' : 'context', text: t.charAt(0).toUpperCase() + t.slice(1) };
  }

  function eventsFor(uid, milestones) {
    const out = [];
    let haveEstimateLog = false;
    try {
      const est = db.prepare(`SELECT field, old_value, new_value, recorded_at FROM consignment_estimate_log
                               WHERE consignment_uid = ? ORDER BY id DESC LIMIT 10`).all(uid);
      haveEstimateLog = est.length > 0;
      for (const e of est) {
        const what = e.field === 'carrier_etd' ? 'Departure' : 'Arrival';
        out.push({ at: e.recorded_at, kind: 'estimate', field: e.field, old_value: e.old_value, new_value: e.new_value, text: e.old_value
          ? `${what} estimate revised ${fmtDay(e.old_value)} \u2192 ${fmtDay(e.new_value)}`
          : `${what} estimate set: ${fmtDay(e.new_value)}` });
      }
    } catch (_) { /* table predates this deploy only in theory */ }
    try {
      const rows = db.prepare(`SELECT received_at, event, note FROM t49_event
                                WHERE applied_to = ? AND outcome = 'applied' ORDER BY id DESC LIMIT 40`).all(uid);
      for (const r of rows) {
        const h = humanEvent(r);
        if (!h) continue;
        if (h.estimate && haveEstimateLog) continue;     // the log says it better: old and new
        const { estimate, ...rest } = h;
        out.push(Object.assign({ at: String(r.received_at || '').replace(' ', 'T') + (String(r.received_at || '').includes('Z') ? '' : 'Z') }, rest));
      }
    } catch (_) { /* no tracking on this deployment */ }
    for (const m of (milestones || [])) {
      if ((m.state === 'confirmed' || m.state === 'amended') && m.recorded_at && m.actual_at) {
        out.push({ at: m.recorded_at, kind: 'confirmed', stage: m.stage, date: m.actual_at,
                   text: `${STAGE_TEXT[m.stage]} ${m.state === 'amended' ? 'recorded' : 'confirmed'} for ${fmtDay(m.actual_at)}` });
      }
    }
    const seen = new Set();
    return out
      .filter(e => { const k = e.text; if (seen.has(k)) return false; seen.add(k); return true; })
      .sort((a, b) => String(b.at).localeCompare(String(a.at)))
      .slice(0, 8);
  }

  // ── The plan as first promised, leg by leg ──
  // Not stored, and does not need to be: the original plan is fully determined by the rules
  // and the first carrier quote, so it is recomputed with the carrier's later revisions and
  // every recorded actual taken out. The frozen FC date wins where the two disagree (a rule
  // edited since), so the legs always add up to the promise that was actually made.
  function baselinePlan(c) {
    if (c.baseline_transit_days == null) return null;
    try {
      const cb = { ...c, transit_days: c.baseline_transit_days, carrier_eta: null, carrier_etd: null };
      const { planned } = computePlanned(cb, {});
      if (c.baseline_fc_at) planned.fc_receipt = c.baseline_fc_at;
      return planned;
    } catch (_) { return null; }
  }

  // ── On time, behind, delayed ──
  // One reading, against the first promise: within a day is on time, two to four days late is
  // behind, more than four is delayed — and a container held at the terminal with its last
  // free day two days out or less is delayed whatever the dates say, because that is when it
  // starts costing money.
  function statusOf(c, sh, term, today) {
    const fc = (sh.milestones || []).find(m => m.stage === 'fc_receipt') || {};
    const delivered = !!fc.actual_at;
    const drift = sh.confidence && typeof sh.confidence.drift === 'number' ? sh.confidence.drift : null;
    const late = delivered ? (c.baseline_fc_at ? dayDiff(c.baseline_fc_at, fc.actual_at) : null) : drift;
    const held = !!(term && term.holds && term.holds.length && !term.available_at && !delivered);
    const lfdIn = term && term.lfd ? dayDiff(today, term.lfd) : null;
    const base = { delivered, late, held, lfd_in: lfdIn };
    if (!delivered && !c.reference) return { ...base, key: 'not_booked', label: 'Not booked', why: 'No container number or AWB yet, so it cannot be tracked.' };
    if (!c.transit_confirmed && !(delivered && late != null)) {
      return { ...base, key: 'no_quote', label: 'No quote', why: 'Transit is the rule default, not a carrier quote.' };
    }
    if (held && lfdIn != null && lfdIn <= 2) {
      return { ...base, key: 'delayed', label: 'Delayed',
        why: lfdIn < 0 ? `Held at the terminal; the last free day passed ${-lfdIn} day${lfdIn === -1 ? '' : 's'} ago.`
                       : `Held at the terminal; the last free day is ${lfdIn === 0 ? 'today' : `in ${lfdIn} day${lfdIn === 1 ? '' : 's'}`}.` };
    }
    if (late != null && late > 4) return { ...base, key: 'delayed', label: 'Delayed', why: `${late} days later than first promised.` };
    if (late != null && late >= 2) return { ...base, key: 'behind', label: 'Behind', why: `${late} days later than first promised.` };
    return { ...base, key: 'on_time', label: 'On time',
      why: held ? 'Held at the terminal, but within the free days.' : (late != null && late < -1 ? `${-late} days earlier than first promised.` : 'Within a day of the first promise.') };
  }

  // ── Contents: lanes → POs → SKUs ──
  function planRowsFor(ws, client) {
    try {
      const r = db.prepare('SELECT data FROM plans WHERE week_start = ? AND client_id = ?').get(ws, client);
      if (!r) return [];
      const arr = JSON.parse(r.data);
      return Array.isArray(arr) ? arr : [];
    } catch (_) { return []; }
  }

  function rowsForLane(laneKey, rows) {
    const [sup = '', zd = '', fr = ''] = String(laneKey).split('||');
    const z = zd.trim(), f = fr.trim().toLowerCase();
    let m = z ? rows.filter(r => {
      const t = String(r.zendesk_ticket == null ? '' : r.zendesk_ticket).trim();
      return t === z || (t !== '' && !isNaN(Number(t)) && !isNaN(Number(z)) && Number(t) === Number(z));
    }) : [];
    if (!m.length) {
      const s = sup.trim().toLowerCase();
      m = rows.filter(r => String(r.supplier_name || '').trim().toLowerCase() === s);
    }
    // A lane is supplier + ticket + freight; the same ticket can ship part by air.
    if (f && m.some(r => r.freight_type)) m = m.filter(r => !r.freight_type || String(r.freight_type).trim().toLowerCase() === f);
    return m;
  }

  function laneLatestArrival(ws) {
    const map = {};
    try {
      const rows = db.prepare('SELECT data FROM flow_week WHERE week_start = ?').all(ws);
      for (const r of rows) {
        let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
        const lanes = (blob && blob.intl_lanes && typeof blob.intl_lanes === 'object') ? blob.intl_lanes : {};
        for (const [k, v] of Object.entries(lanes)) if (v && v.latest_arrival_date) map[k] = String(v.latest_arrival_date).slice(0, 10);
      }
    } catch (_) { /* no week data */ }
    return map;
  }

  let _recordsHasClient = null;
  function processedFor(pos, client) {
    const out = new Map();
    if (!pos.length) return out;
    try {
      if (_recordsHasClient == null) {
        _recordsHasClient = db.prepare('PRAGMA table_info(records)').all().some(c => c.name === 'client_id');
      }
      for (let i = 0; i < pos.length; i += 400) {
        const chunk = pos.slice(i, i + 400);
        const ph = chunk.map(() => '?').join(',');
        const sql = `SELECT po_number po, sku_code sku, COUNT(*) n FROM records
                      WHERE status = 'complete' AND po_number IN (${ph})
                      ${_recordsHasClient ? 'AND (client_id = ? OR client_id IS NULL)' : ''}
                      GROUP BY po_number, sku_code`;
        const rows = db.prepare(sql).all(...chunk, ...(_recordsHasClient ? [client] : []));
        for (const r of rows) {
          const k = String(r.po || '').trim().toUpperCase() + '|' + String(r.sku || '').trim().toUpperCase();
          out.set(k, (out.get(k) || 0) + Number(r.n || 0));
        }
      }
    } catch (e) { console.warn('[consignments] processedFor:', e.message); }
    return out;
  }

  function contentsFor(c, laneKeys, opts) {
    const client = c.client_id;
    const withProcessed = !!(opts && opts.processed);
    const owns = ownsWeekModel(client);
    const plan = (opts && opts.plan) || planRowsFor(c.week_start, client);
    const latest = owns ? ((opts && opts.latest) || laneLatestArrival(c.week_start)) : {};
    const lanes = laneKeys.map(k => {
      const [sup = '', zd = '', fr = ''] = String(k).split('||');
      const rows = rowsForLane(k, plan);
      const byPo = new Map();
      for (const r of rows) {
        const po = String(r.po_number || '').trim();
        if (!po) continue;
        if (!byPo.has(po)) byPo.set(po, new Map());
        const sku = String(r.sku_code || '').trim();
        const skus = byPo.get(po);
        const cur = skus.get(sku) || {
          sku,
          description: [r.item_description, r.item_color, r.item_size].filter(x => x && String(x).trim()).join(' · ') || null,
          planned: 0, processed: null,
        };
        cur.planned += Number(r.target_qty || 0) || 0;
        skus.set(sku, cur);
      }
      const pos = [...byPo.entries()].map(([po, skus]) => ({ po, skus: [...skus.values()] }));
      return { lane_key: k, supplier: sup, zendesk: zd, freight: fr, latest_arrival: latest[k] || null, pos };
    });
    let processedAvailable = false;
    if (withProcessed && owns) {
      const allPos = [...new Set(lanes.flatMap(l => l.pos.map(p => p.po)))];
      const done = processedFor(allPos, client);
      processedAvailable = true;
      for (const l of lanes) for (const p of l.pos) for (const s of p.skus) {
        s.processed = done.get(p.po.toUpperCase() + '|' + s.sku.toUpperCase()) || 0;
      }
    }
    const totals = { lanes: lanes.length, pos: 0, skus: 0, planned: 0, processed: processedAvailable ? 0 : null };
    for (const l of lanes) {
      l.planned = 0; l.processed = processedAvailable ? 0 : null; l.skus = 0;
      for (const p of l.pos) {
        p.planned = p.skus.reduce((a, s) => a + s.planned, 0);
        p.processed = processedAvailable ? p.skus.reduce((a, s) => a + (s.processed || 0), 0) : null;
        l.planned += p.planned; l.skus += p.skus.length;
        if (processedAvailable) l.processed += p.processed;
      }
      totals.pos += l.pos.length; totals.skus += l.skus; totals.planned += l.planned;
      if (processedAvailable) totals.processed += l.processed;
    }
    return { lanes, totals, processed_available: processedAvailable, week_model: owns };
  }

  function enrich(c, sh, ctx) {
    const term = terminalFor(c.consignment_uid);
    const health = statusOf(c, sh, term, ctx.today);
    let est = [];
    try {
      est = db.prepare(`SELECT field, old_value, new_value, recorded_at FROM consignment_estimate_log
                         WHERE consignment_uid = ? ORDER BY id DESC LIMIT 20`).all(c.consignment_uid);
    } catch (_) { est = []; }
    const prevOf = (f) => { const r = est.find(e => e.field === f && e.old_value); return r ? r.old_value : null; };
    const since = Date.now() - 24 * 3600 * 1000;
    // For the overnight replay: the earliest value each estimate held inside the last day.
    const changed = {};
    for (const e of est.slice().reverse()) {
      // A first estimate (nothing before it) is not a change anyone can replay.
      if (Date.parse(e.recorded_at) >= since && e.old_value && !(e.field in changed)) changed[e.field] = e.old_value;
    }
    let last = null;
    try {
      last = db.prepare(`SELECT sent_at, to_json, subject FROM consignment_notification
                          WHERE consignment_uid = ? AND status = 'sent' ORDER BY id DESC LIMIT 1`).get(c.consignment_uid) || null;
      if (last) { let to = []; try { to = JSON.parse(last.to_json || '[]'); } catch (_) {} last = { sent_at: last.sent_at, to_count: to.length, subject: last.subject }; }
    } catch (_) { last = null; }
    const contents = ctx.planByWeek ? (() => {
      if (!ctx.planByWeek.has(c.week_start)) ctx.planByWeek.set(c.week_start, planRowsFor(c.week_start, c.client_id));
      const cs = contentsFor(c, sh.lanes || [], { plan: ctx.planByWeek.get(c.week_start), latest: {} });
      return cs.totals;
    })() : null;
    return {
      ...sh,
      // shape() replaces `carrier` with the carrier's estimates; the name is kept here.
      carrier_name: c.carrier || null,
      route: routeOf(c),
      terminal: term,
      // `status` is already the consignment's own planned | booked | closed; this is the reading.
      health,
      baseline_plan: baselinePlan(c),
      estimate_prev: { carrier_eta: prevOf('carrier_eta'), carrier_etd: prevOf('carrier_etd') },
      changed_24h: changed,
      events: eventsFor(c.consignment_uid, sh.milestones),
      contents_summary: contents,
      last_notification: last,
    };
  }

  // ── Ex-factory and VAS: the two week-level milestones before anything moves ──
  // Built from the same sources the Week Hub reads, so the two screens agree on a week:
  //   · Received  — POs received ÷ POs planned (receiving vs plan). Closed when every PO is
  //                 in, or its lane is ticked complete: the tick means "done on whatever
  //                 arrived", so a PO that never came stops counting against the week.
  //   · VAS       — lanes complete ÷ lanes in the week. A lane is complete when units applied
  //                 reach units planned (the Week Hub's auto-complete), or someone ticked it.
  //                 Units applied are counted the Week Hub's way: work done Mon–Sun of the week
  //                 against that week's POs.
  // Status against a target: Received by the latest PO due date, VAS by the Friday of the
  // execution week. At risk is the Week Hub's own line: under 80% within a day of target.
  const _colCache = {};
  const hasCol = (t, c) => {
    const k = t + '.' + c;
    if (!(k in _colCache)) { try { _colCache[k] = db.prepare(`PRAGMA table_info(${t})`).all().some(x => x.name === c); } catch (_) { _colCache[k] = false; } }
    return _colCache[k];
  };
  const dayOnly = (v) => v ? String(v).replace('T', ' ').slice(0, 10) : null;
  function originStatus(done, pct, target, today) {
    if (done) return 'complete';
    if (target && today > target) return 'past_due';
    if (target && dayDiff(today, target) <= 1 && pct < 80) return 'at_risk';
    return pct > 0 ? 'in_progress' : 'not_started';
  }
  function originFor(ws, client, today, planRows) {
    try {
      const plan = planRows || planRowsFor(ws, client);
      if (!plan.length) return null;
      const we = addDays(ws, 6);
      const tickets = new Map();          // zendesk → { planned, pos:Set, supplier }
      const poTicket = new Map(), poDue = new Map(), poSupplier = new Map();
      for (const r of plan) {
        const po = String(r.po_number || '').trim(); if (!po) continue;
        const t = String(r.zendesk_ticket == null ? '' : r.zendesk_ticket).trim() || 'Unspecified';
        if (!tickets.has(t)) tickets.set(t, { planned: 0, pos: new Set() });
        const tk = tickets.get(t);
        tk.planned += Number(r.target_qty || 0) || 0; tk.pos.add(po);
        poTicket.set(po, t);
        if (r.supplier_name) poSupplier.set(po, String(r.supplier_name).trim());
        if (r.due_date && ymdOk(String(r.due_date).slice(0, 10))) {
          const d = String(r.due_date).slice(0, 10);
          if (!poDue.has(po) || d > poDue.get(po)) poDue.set(po, d);
        }
      }
      const allPos = [...poTicket.keys()];

      // Receiving
      const recvSql = `SELECT po_number po, received_at_local at FROM receiving WHERE week_start = ?`
        + (hasCol('receiving', 'client_id') ? ' AND (client_id = ? OR client_id IS NULL)' : '');
      const recv = db.prepare(recvSql).all(...(hasCol('receiving', 'client_id') ? [ws, client] : [ws]));
      const received = new Map();
      for (const r of recv) {
        const po = String(r.po || '').trim();
        if (!poTicket.has(po)) continue;
        const d = dayOnly(r.at) || ws;
        if (!received.has(po) || d > received.get(po)) received.set(po, d);
      }

      // Applied, the Week Hub's way
      const lastCol = hasCol('records', 'completed_at') ? 'COALESCE(completed_at, date_local)' : 'date_local';
      const recSql = `SELECT po_number po, COUNT(*) n, MAX(${lastCol}) last FROM records
                       WHERE status = 'complete' AND date_local >= ? AND date_local <= ?`
        + (hasCol('records', 'client_id') ? ' AND (client_id = ? OR client_id IS NULL)' : '') + ' GROUP BY po_number';
      const applied = new Map();
      for (const r of db.prepare(recSql).all(...(hasCol('records', 'client_id') ? [ws, we, client] : [ws, we]))) {
        const po = String(r.po || '').trim();
        if (poTicket.has(po)) applied.set(po, { n: Number(r.n || 0), last: dayOnly(r.last) });
      }

      // Ticks
      const ticks = new Map();
      try {
        for (const r of db.prepare(`SELECT zendesk_ticket t, completed_at at FROM zendesk_completions WHERE week_start = ? AND completed = 1`).all(ws)) {
          ticks.set(String(r.t).trim(), dayOnly(r.at));
        }
      } catch (_) { /* table absent on a fresh database */ }

      let lanesDone = 0, unitsPlanned = 0, unitsApplied = 0, vasDone = null;
      const ticketDone = new Map();
      for (const [t, tk] of tickets) {
        const ap = [...tk.pos].reduce((a, po) => a + ((applied.get(po) || {}).n || 0), 0);
        unitsPlanned += tk.planned; unitsApplied += ap;
        const auto = tk.planned > 0 && ap >= tk.planned;
        const tick = ticks.get(t) || null;
        if (auto || tick) {
          lanesDone++;
          const lastWork = [...tk.pos].map(po => (applied.get(po) || {}).last).filter(Boolean).sort().pop() || null;
          const at = tick || lastWork;
          ticketDone.set(t, at);
          if (at && (!vasDone || at > vasDone)) vasDone = at;
        }
      }
      const vasComplete = tickets.size > 0 && lanesDone === tickets.size;
      const vasTarget = addDays(ws, 4);
      const vasPct = tickets.size ? Math.round(100 * lanesDone / tickets.size) : 0;
      const unitsPct = unitsPlanned ? Math.round(100 * Math.min(unitsApplied, unitsPlanned) / unitsPlanned) : 0;

      let posClosed = 0, recvDone = null, latePos = 0;
      for (const po of allPos) {
        const got = received.get(po);
        if (got) {
          posClosed++;
          if (!recvDone || got > recvDone) recvDone = got;
          if (poDue.get(po) && got > poDue.get(po)) latePos++;
        } else if (ticketDone.has(poTicket.get(po))) {
          posClosed++;
          const at = ticketDone.get(poTicket.get(po));
          if (at && (!recvDone || at > recvDone)) recvDone = at;
        }
      }
      const recvComplete = allPos.length > 0 && posClosed === allPos.length;
      const recvPct = allPos.length ? Math.round(100 * received.size / allPos.length) : 0;
      const dues = [...poDue.values()].sort();
      const recvTarget = dues.length ? dues[dues.length - 1] : addDays(ws, 2);

      // Who is furthest behind on VAS, while the week is still open.
      const bySup = new Map();
      for (const r of plan) {
        const po = String(r.po_number || '').trim(); if (!po) continue;
        const s = poSupplier.get(po) || 'Unknown';
        if (!bySup.has(s)) bySup.set(s, { name: s, planned: 0, applied: 0, pos: new Set() });
        const x = bySup.get(s); x.planned += Number(r.target_qty || 0) || 0; x.pos.add(po);
      }
      for (const x of bySup.values()) x.applied = [...x.pos].reduce((a, po) => a + ((applied.get(po) || {}).n || 0), 0);
      const behind = vasComplete ? [] : [...bySup.values()]
        .filter(x => x.planned > 0 && x.applied < x.planned && !([...x.pos].every(po => ticketDone.has(poTicket.get(po)))))
        .map(x => ({ name: x.name, planned: x.planned, applied: x.applied, pct: Math.round(100 * x.applied / x.planned) }))
        .sort((a, b) => a.pct - b.pct).slice(0, 3);

      return {
        received: {
          pos: allPos.length, pos_received: received.size, pos_closed: posClosed, pct: recvPct, late_pos: latePos,
          closed_by_tick: posClosed - received.size, target: recvTarget, done_at: recvComplete ? recvDone : null,
          status: originStatus(recvComplete, recvPct, recvTarget, today),
        },
        vas: {
          lanes: tickets.size, lanes_complete: lanesDone, pct: vasPct,
          units_planned: unitsPlanned, units_applied: unitsApplied, units_pct: unitsPct,
          target: vasTarget, done_at: vasComplete ? vasDone : null,
          status: originStatus(vasComplete, vasPct, vasTarget, today),
        },
        suppliers_behind: behind,
      };
    } catch (e) {
      console.warn('[consignments] originFor', ws, e.message);
      return null;
    }
  }

  // Lanes in a week's week model that no consignment carries. Read from the week model only
  // (gated by the caller), for the weeks given. Shared by the board and by Pulse.
  function unassignedLanes(client, fromWs, toWs, ctx) {
    const FIELDS = ['packing_list_ready_at', 'origin_customs_cleared_at', 'departed_at', 'arrived_at', 'dest_customs_cleared_at', 'eta_fc'];
    const out = [];
    const weeks = db.prepare(`SELECT DISTINCT week_start FROM flow_week WHERE week_start >= ? AND week_start <= ?`)
      .all(fromWs, toWs).map(r => r.week_start);
    for (const ws of weeks) {
      const assigned = new Set(db.prepare('SELECT lane_key FROM consignment_lane WHERE week_start = ? AND client_id = ?')
        .all(ws, client).map(r => r.lane_key));
      let legacyRows = [];
      try { legacyRows = db.prepare('SELECT DISTINCT lane_key FROM lane_actual_dates WHERE week_start = ?').all(ws).map(r => r.lane_key); } catch (_) {}
      const legacySet = new Set(legacyRows);
      const plan = ctx.planByWeek.has(ws) ? ctx.planByWeek.get(ws) : planRowsFor(ws, client);
      ctx.planByWeek.set(ws, plan);
      const seen = new Set();
      for (const r of db.prepare('SELECT data FROM flow_week WHERE week_start = ?').all(ws)) {
        let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
        const lanes = (blob && blob.intl_lanes && typeof blob.intl_lanes === 'object') ? blob.intl_lanes : {};
        for (const [k, v] of Object.entries(lanes)) {
          if (assigned.has(k) || seen.has(k)) continue;
          seen.add(k);
          const [sup = '', zd = '', fr = ''] = k.split('||');
          const legacy = FIELDS.filter(f => v && v[f]);
          const dep = v && v.departed_at ? String(v.departed_at).slice(0, 10) : null;
          const pos = new Set(rowsForLane(k, plan).map(x => String(x.po_number || '').trim()).filter(Boolean));
          out.push({ week_start: ws, lane_key: k, supplier: sup, zendesk: zd, freight: fr,
            legacy_dates: legacy.length > 0 || legacySet.has(k), legacy_departed: dep, pos: pos.size });
        }
      }
    }
    return out;
  }

  // ════════════════════════════════════════════════════════════════════════════════════════
  // Pulse AI — the transit picture, in words.
  //
  // Pulse used to read transit from the dates typed on each lane and the old week-container
  // list: the data that drew the phantom ship, and nothing of tracking, holds, revisions or
  // status. This builds its transit section from the same enrich() the Transit Movements
  // screen uses, so the screen and the assistant cannot disagree.
  //
  // Window: the last 30 days. A movement is in it when its execution week began in the last
  // 30 days, when anything about it happened in the last 30 days (a milestone recorded, an
  // estimate revised, a tracking event), or when it is still on its way — a W35 container
  // still at sea is very much part of "what transit looks like".
  // ════════════════════════════════════════════════════════════════════════════════════════
  const PULSE_DAYS = 30;
  const STATE_WORDS = {
    carrier: 'tracked from the carrier', confirmed: 'confirmed by the VelOzity team',
    amended: 'recorded by the VelOzity team', assumed: 'planned, not yet confirmed',
  };
  const isoWeekOf = (ymd) => {
    const d = new Date(String(ymd).slice(0, 10) + 'T00:00:00Z');
    const day = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - day + 3);
    const firstThu = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
    return 1 + Math.round(((d - firstThu) / 86400000 - 3 + ((firstThu.getUTCDay() + 6) % 7)) / 7);
  };
  const fmtFull = (ymd) => { const f = fmtDay(ymd); if (!f) return null; return `${f} ${String(ymd).slice(0, 4)}`; };
  const fmtN = (n) => Number(n || 0).toLocaleString('en-AU');
  const pl = (n, w, p) => `${n} ${n === 1 ? w : (p || w + 's')}`;

  function d2dLegs(sh) {
    const ms = {}; for (const m of (sh.milestones || [])) ms[m.stage] = m;
    const d = (st) => ms[st] ? String(ms[st].actual_at || ms[st].planned_at || '').slice(0, 10) || null : null;
    const P = sh.baseline_plan;
    const first = d('packing_list_ready'), dep = d('departed'), arr = d('arrived'), fc = d('fc_receipt');
    if (!first || !dep || !arr || !fc) return null;
    const act = dayDiff(first, fc);
    const plan = (P && P.packing_list_ready && P.fc_receipt) ? dayDiff(P.packing_list_ready, P.fc_receipt) : null;
    const settled = !!(ms.fc_receipt && ms.fc_receipt.actual_at);
    return { plan, act, settled };
  }

  function movementLines(sh, today) {
    const c = sh;
    const out = [];
    const ms = {}; for (const m of (sh.milestones || [])) ms[m.stage] = m;
    const air = c.mode === 'Air';
    const ref = c.reference || (air ? 'AWB not yet advised' : 'container number not yet advised');
    const r = sh.route || {};
    const port = (p) => p ? (p.name || p.code) : null;
    const routeTxt = (r.origin || r.dest)
      ? `${port(r.origin) || 'origin not yet known'} → ${port(r.dest) || 'destination not yet known'}${(r.via || []).length ? ' via ' + r.via.map(port).filter(Boolean).join(', ') : ''}`
      : 'ports not yet known';
    const size = c.size_ft ? String(c.size_ft).replace(/ft$/i, '').trim() : '';
    const bits = [air ? 'Air' : 'Sea', ref, size ? (/^\d+$/.test(size) ? `${size}ft` : size) : null,
      c.carrier_name ? `carrier ${c.carrier_name}` : null, c.vessel ? `vessel ${c.vessel}` : null, routeTxt].filter(Boolean);
    out.push(`- ${bits.join(' · ')}`);
    const h = sh.health || {};
    out.push(`  Status: ${h.label || 'Unknown'}${h.why ? ' — ' + h.why : ''}`);
    const fc = ms.fc_receipt || {};
    const fcNow = String(fc.actual_at || fc.planned_at || '').slice(0, 10) || null;
    const fcBits = [];
    if (fc.actual_at) fcBits.push(`received at FC ${fmtFull(fc.actual_at)}`);
    else if (fcNow) fcBits.push(`FC receipt expected ${fmtFull(fcNow)}`);
    if (c.baseline_fc_at) fcBits.push(`first promised ${fmtFull(c.baseline_fc_at)}`);
    else fcBits.push('no frozen first promise yet (transit not quoted)');
    if (!air && c.carrier_eta) {
      const prev = c.estimate_prev && c.estimate_prev.carrier_eta;
      fcBits.push(`carrier ETA ${fmtFull(c.carrier_eta)}${prev ? ` (previously ${fmtFull(prev)})` : ''}`);
    }
    out.push(`  Delivery: ${fcBits.join('; ')}`);
    const msTxt = STAGES.filter(st => ms[st]).map(st => {
      const m = ms[st];
      const when = String(m.actual_at || m.planned_at || '').slice(0, 10);
      const overdue = !m.actual_at && when && when < today;
      return `${STAGE_TEXT[st]} ${fmtDay(when) || '—'} (${m.actual_at ? (STATE_WORDS[m.state] || 'recorded') : (overdue ? 'planned, not yet confirmed — overdue' : 'planned, not yet confirmed')})`;
    });
    if (msTxt.length) out.push(`  Milestones: ${msTxt.join(' · ')}`);
    const t = sh.terminal;
    if (!air && t && (t.terminal || t.lfd || (t.holds || []).length)) {
      out.push(`  Terminal: ${[t.terminal, (t.holds || []).length ? `${t.holds.join(', ').toLowerCase()} hold` : 'no holds reported',
        t.lfd ? `last free day ${fmtFull(t.lfd)}` : null, t.available_at ? `available for pickup ${fmtFull(t.available_at)}` : null].filter(Boolean).join('; ')}`);
    }
    if (air) out.push('  Tracking: air is not tracked automatically; dates are confirmed by the VelOzity team.');
    else if (t && t.tracking) out.push('  Tracking: live carrier tracking through Pinpoint.');
    else if (c.reference) out.push('  Tracking: not yet subscribed to carrier tracking.');
    const tr = c.transit || {};
    if (c.transit_days != null) {
      const base = c.transit_confirmed && c.baseline_transit_days != null ? Number(c.baseline_transit_days) : null;
      out.push(`  Transit: ${c.transit_confirmed ? `carrier quote ${pl(Number(c.transit_days), 'day')}` : `${c.transit_days}-day rule default, not a carrier quote`}${base != null && base !== Number(c.transit_days) ? `; first quote ${pl(base, 'day')}` : ''}${tr.achieved != null ? `; achieved ${pl(Number(tr.achieved), 'day')}` : ''}`);
    }
    const dd = d2dLegs(sh);
    if (dd) out.push(`  Door to door (packing list → FC): ${dd.plan != null ? `plan ${dd.plan} days, ` : ''}${dd.settled ? 'actual' : 'projected'} ${dd.act} days`);
    const cs = sh.contents_summary;
    const laneNames = (sh.lanes || []).map(k => { const p = String(k).split('||'); return `${p[0]} (ZD ${p[1] || '—'})`; });
    if (cs) out.push(`  Contents: ${pl(cs.lanes, 'lane')} · ${pl(cs.pos, 'PO')} · ${pl(cs.skus, 'SKU')} · ${fmtN(cs.planned)} planned units`);
    if (laneNames.length) out.push(`  Lanes: ${laneNames.slice(0, 12).join('; ')}${laneNames.length > 12 ? `; and ${laneNames.length - 12} more` : ''}`);
    const ev = (sh.events || []).slice(0, 5).map(e => `${e.text}${e.at ? ` (${fmtDay(String(e.at).slice(0, 10))})` : ''}`);
    if (ev.length) out.push(`  Recent: ${ev.join(' · ')}`);
    if (sh.last_notification) out.push(`  Client last notified: ${fmtFull(String(sh.last_notification.sent_at).slice(0, 10))} (${pl(sh.last_notification.to_count, 'recipient')})`);
    return out;
  }

  function pulseTransit(client) {
    const today = todayLocal();
    const thisMonday = mondayOf(today);
    const cutoff = addDays(today, -PULSE_DAYS);
    const cutoffWeek = mondayOf(cutoff);
    const lookback = addDays(thisMonday, -16 * 7);   // only to find what is still on its way
    const rows = db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND week_start >= ? AND week_start <= ?
                              ORDER BY week_start, mode, reference`).all(client, lookback, addDays(thisMonday, 7));
    const ctx = { today, planByWeek: new Map() };
    const items = [];
    for (const c of rows) {
      const sh = shape(c);
      const fc = (sh.milestones || []).find(m => m.stage === 'fc_receipt') || {};
      const stillMoving = !fc.actual_at && c.status !== 'closed';
      const touched = (sh.milestones || []).some(m => (m.recorded_at && String(m.recorded_at).slice(0, 10) >= cutoff) || (m.actual_at && String(m.actual_at).slice(0, 10) >= cutoff));
      if (!(c.week_start >= cutoffWeek || stillMoving || touched)) continue;
      const en = enrich(c, sh, ctx);
      const recentEvent = (en.events || []).some(e => String(e.at || '').slice(0, 10) >= cutoff);
      if (!(c.week_start >= cutoffWeek || stillMoving || touched || recentEvent)) continue;
      items.push(en);
    }
    const owns = ownsWeekModel(client);
    const weeks = [...new Set(items.map(x => x.week_start).concat(owns ? [thisMonday] : []))].sort();
    const unassigned = owns ? unassignedLanes(client, cutoffWeek, thisMonday, ctx) : [];
    const lines = [];
    lines.push(`## Transit movements (Pinpoint) — last ${PULSE_DAYS} days, as at ${fmtFull(today)}`);
    lines.push(`Source of truth for every transit, shipping, container, flight, ETA and delivery question. Week numbers are ISO weeks: W${isoWeekOf(thisMonday)} is the week of ${fmtFull(thisMonday)}.`);
    lines.push(`Included: movements whose execution week began on or after ${fmtFull(cutoffWeek)}, anything that changed in the last ${PULSE_DAYS} days, and anything still on its way. Older, completed movements are not included.`);
    if (!weeks.length) { lines.push('No movements in this window.'); return lines.join('\n'); }
    const totals = { mv: items.length, delayed: 0, behind: 0, held: 0 };
    for (const x of items) { if ((x.health || {}).key === 'delayed') totals.delayed++; if ((x.health || {}).key === 'behind') totals.behind++; if ((x.health || {}).held) totals.held++; }
    lines.push(`Summary: ${pl(totals.mv, 'movement')} — ${totals.delayed} delayed, ${totals.behind} behind, ${totals.held} held at a terminal.`);
    lines.push('');
    for (const ws of weeks) {
      const list = items.filter(x => x.week_start === ws);
      const pos = list.reduce((a, x) => a + ((x.contents_summary || {}).pos || 0), 0);
      const lanes = list.reduce((a, x) => a + ((x.lanes || []).length), 0);
      lines.push(`### W${isoWeekOf(ws)} · execution week of ${fmtFull(ws)} · ${pl(list.length, 'movement')} · ${pl(lanes, 'lane')} · ${pl(pos, 'PO')}`);
      if (owns) {
        if (!ctx.planByWeek.has(ws)) ctx.planByWeek.set(ws, planRowsFor(ws, client));
        const o = originFor(ws, client, today, ctx.planByWeek.get(ws));
        if (o) {
          const word = (s) => ({ complete: 'complete', in_progress: 'in progress', not_started: 'not started', at_risk: 'at risk', past_due: 'past due' })[s] || s;
          const rr = o.received, v = o.vas;
          lines.push(`Ex-factory (received): ${rr.pct}% — ${rr.pos_received} of ${pl(rr.pos, 'PO')} received${rr.closed_by_tick ? `, ${rr.closed_by_tick} closed by the lane completion tick` : ''}${rr.late_pos ? `, ${rr.late_pos} after their due date` : ''}; ${word(rr.status)}${rr.done_at ? ` ${fmtDay(rr.done_at)}` : ''} (target ${fmtDay(rr.target)}).`);
          lines.push(`VAS: ${v.lanes_complete} of ${pl(v.lanes, 'lane')} complete; ${fmtN(v.units_applied)} of ${fmtN(v.units_planned)} units applied within the week (${v.units_pct}%); ${word(v.status)}${v.done_at ? ` ${fmtDay(v.done_at)}` : ''} (target ${fmtDay(v.target)}).${(o.suppliers_behind || []).length ? ` Furthest behind: ${o.suppliers_behind.map(sb => `${sb.name} ${sb.pct}%`).join(', ')}.` : ''}`);
        }
      }
      if (!list.length) lines.push('No containers or flights set up for this week yet.');
      for (const x of list) lines.push(...movementLines(x, today));
      const un = unassigned.filter(u => u.week_start === ws);
      if (un.length) {
        const legacy = un.filter(u => u.legacy_dates).length;
        lines.push(`Lanes on no movement: ${pl(un.length, 'lane')} (${pl(un.reduce((a, u) => a + (u.pos || 0), 0), 'PO')}) not assigned to any container or flight${legacy ? `; ${legacy} still ${legacy === 1 ? 'carries' : 'carry'} dates typed on the lane, which are not used` : ''}.`);
      }
      lines.push('');
    }
    return lines.join('\n');
  }

  // ── The board ──
  router.get('/board', authenticateRequest, auditLog('view_transit_board'), (req, res) => {
    try {
      const client = curClient();
      const today = todayLocal();
      const thisMonday = mondayOf(today);
      const addW = (ymd, n) => addDays(ymd, n * 7);
      let from = ymdOk(req.query.from) ? mondayOf(req.query.from) : addW(thisMonday, -10);
      let to = ymdOk(req.query.to) ? mondayOf(req.query.to) : addW(thisMonday, 1);
      if (to < from) [from, to] = [to, from];
      if (dayDiff(from, to) > 26 * 7) from = addW(to, -26);

      const rows = db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND week_start >= ? AND week_start <= ?
                                ORDER BY week_start, mode, reference`).all(client, from, to);
      const ctx = { today, planByWeek: new Map() };
      const consignments = rows.map(c => enrich(c, shape(c), ctx));

      const first = db.prepare('SELECT MIN(week_start) w FROM consignment WHERE client_id = ?').get(client);

      // Lanes in a week that no consignment carries — named, never placed.
      const owns = ownsWeekModel(client);
      const liveFrom = addW(thisMonday, -5);
      const unassigned = owns ? unassignedLanes(client, liveFrom > from ? liveFrom : from, thisMonday, ctx) : [];

      let lastEvent = null;
      try {
        const r = db.prepare(`SELECT MAX(e.received_at) at FROM t49_event e
                                JOIN consignment c ON c.consignment_uid = e.applied_to
                               WHERE c.client_id = ? AND e.outcome = 'applied'`).get(client);
        lastEvent = r && r.at ? String(r.at).replace(' ', 'T') + (String(r.at).includes('Z') ? '' : 'Z') : null;
      } catch (_) { lastEvent = null; }

      // Ex-factory and VAS per week, for weeks with something in play (and this week).
      const weeks_origin = {};
      if (owns) {
        const wks = new Set(rows.map(c => c.week_start).filter(w => w >= addW(thisMonday, -6)));
        wks.add(thisMonday);
        for (const ws of wks) {
          if (!ctx.planByWeek.has(ws)) ctx.planByWeek.set(ws, planRowsFor(ws, client));
          const o = originFor(ws, client, today, ctx.planByWeek.get(ws));
          if (o) weeks_origin[ws] = o;
        }
      }

      res.json({
        ok: true, client_id: client, today, this_week: thisMonday, from, to,
        weeks_origin,
        tracking_started: (first && first.w) || null,
        tracking_last_event_at: lastEvent,
        week_model: owns,
        consignments, unassigned,
      });
    } catch (e) {
      console.error('[consignments] board failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  function loadOwn(uid) {
    return db.prepare('SELECT * FROM consignment WHERE consignment_uid = ? AND client_id = ?').get(uid, curClient());
  }

  router.get('/:uid/contents', authenticateRequest, auditLog('view_consignment_contents'), (req, res) => {
    try {
      const c = loadOwn(String(req.params.uid || '').trim());
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });
      const sh = shape(c);
      res.json({ ok: true, consignment_uid: c.consignment_uid, reference: c.reference || null,
        week_start: c.week_start, ...contentsFor(c, sh.lanes || [], { processed: true }) });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── One spreadsheet, whoever asks for it ──
  // The download button and the email attachment call the same builder, so the file a client
  // receives and the one a person downloads cannot differ.
  async function contentsWorkbook(list) {
    const ExcelJS = require('exceljs');
    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();
    const ws = wb.addWorksheet('POs and SKUs', { views: [{ state: 'frozen', ySplit: 1 }] });
    ws.columns = [
      { header: 'Week', key: 'week', width: 11 },
      { header: 'Movement', key: 'ref', width: 20 },
      { header: 'Mode', key: 'mode', width: 6 },
      { header: 'Status', key: 'status', width: 11 },
      { header: 'FC receipt (now)', key: 'fc', width: 15 },
      { header: 'First promised', key: 'base', width: 15 },
      { header: 'Supplier', key: 'sup', width: 30 },
      { header: 'Zendesk', key: 'zd', width: 10 },
      { header: 'PO', key: 'po', width: 14 },
      { header: 'SKU', key: 'sku', width: 22 },
      { header: 'Description', key: 'desc', width: 34 },
      { header: 'Planned units', key: 'planned', width: 13 },
      { header: 'Processed units', key: 'processed', width: 15 },
      { header: 'Lane latest arrival', key: 'latest', width: 18 },
    ];
    ws.getRow(1).font = { bold: true };
    for (const item of list) {
      const { c, sh, contents } = item;
      const fc = (sh.milestones || []).find(m => m.stage === 'fc_receipt') || {};
      const term = terminalFor(c.consignment_uid);
      const st = statusOf(c, sh, term, todayLocal());
      for (const l of contents.lanes) {
        const lanePos = l.pos.length ? l.pos : [{ po: '', skus: [{ sku: '', description: '', planned: null, processed: null }] }];
        for (const p of lanePos) for (const s of (p.skus.length ? p.skus : [{ sku: '', planned: null, processed: null }])) {
          ws.addRow({ week: c.week_start, ref: c.reference || 'not advised', mode: c.mode, status: st.label,
            fc: fc.actual_at || fc.planned_at || '', base: c.baseline_fc_at || '',
            sup: l.supplier, zd: l.zendesk, po: p.po, sku: s.sku, desc: s.description || '',
            planned: s.planned, processed: s.processed, latest: l.latest_arrival || '' });
        }
      }
    }
    return Buffer.from(await wb.xlsx.writeBuffer());
  }

  const safeName = (s) => String(s || 'movement').replace(/[^A-Za-z0-9._-]+/g, '_').slice(0, 60);

  router.get('/:uid/contents.xlsx', authenticateRequest, auditLog('download_consignment_contents'), async (req, res) => {
    try {
      const c = loadOwn(String(req.params.uid || '').trim());
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });
      const sh = shape(c);
      const buf = await contentsWorkbook([{ c, sh, contents: contentsFor(c, sh.lanes || [], { processed: true }) }]);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="${safeName(c.reference)}_${c.week_start}_POs.xlsx"`);
      res.send(buf);
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // Every live movement in one sheet — what the old map's Detail view export was for.
  router.get('/contents.xlsx', authenticateRequest, auditLog('download_transit_contents'), async (req, res) => {
    try {
      const uids = String(req.query.uids || '').split(',').map(s => s.trim()).filter(Boolean).slice(0, 60);
      if (!uids.length) return res.status(400).json({ ok: false, error: 'uids is required.' });
      const list = [];
      for (const uid of uids) {
        const c = loadOwn(uid);
        if (!c) continue;
        const sh = shape(c);
        list.push({ c, sh, contents: contentsFor(c, sh.lanes || [], { processed: true }) });
      }
      const buf = await contentsWorkbook(list);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="transit_movements_${todayLocal()}.xlsx"`);
      res.send(buf);
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Client notifications ──
  // VelOzity staff only. requireRole cannot express that — a client's own org admin holds the
  // Clerk admin role — so this uses the internal-organisation check, and fails closed when it
  // was not provided.
  const internalOnly = typeof requireInternalOrg === 'function'
    ? requireInternalOrg
    : (req, res) => res.status(403).json({ ok: false, error: 'internal_only' });

  const EMAIL_RE = /^[^\s@<>,;]+@[^\s@<>,;]+\.[^\s@<>,;]+$/;
  const NOTIFY_FROM = process.env.NOTIFY_FROM || 'VelOzity Operations <operations@velozity.au>';
  const NOTIFY_REPLY_TO = process.env.NOTIFY_REPLY_TO || 'operations@velozity.au';
  const escHtml = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  router.get('/notify/recipients', authenticateRequest, internalOnly, (req, res) => {
    try {
      const client = curClient();
      const rows = db.prepare(`SELECT email FROM notify_recipient WHERE client_id = ?
                                ORDER BY last_used_at DESC, uses DESC LIMIT 40`).all(client);
      const lastRow = db.prepare(`SELECT to_json FROM consignment_notification WHERE client_id = ? AND status = 'sent'
                                   ORDER BY id DESC LIMIT 1`).get(client);
      let last = []; try { last = JSON.parse((lastRow && lastRow.to_json) || '[]'); } catch (_) {}
      res.json({ ok: true, recipients: rows.map(r => r.email), last, from: NOTIFY_FROM, reply_to: NOTIFY_REPLY_TO });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router.post('/:uid/notify', authenticateRequest, internalOnly, auditLog('notify_consignment'), async (req, res) => {
    const client = curClient();
    const uid = String(req.params.uid || '').trim();
    const b = req.body || {};
    const user = (req.auth && req.auth.userId) || null;
    try {
      const c = loadOwn(uid);
      if (!c) return res.status(404).json({ ok: false, error: 'No such consignment.' });
      if (typeof sendViaResend !== 'function') return res.status(503).json({ ok: false, error: 'Email sending is not configured on this server.' });

      const to = [...new Set((Array.isArray(b.to) ? b.to : []).map(x => String(x || '').trim().toLowerCase()).filter(Boolean))];
      const bad = to.filter(x => !EMAIL_RE.test(x));
      if (!to.length) return res.status(400).json({ ok: false, error: 'Add at least one recipient.' });
      if (bad.length) return res.status(400).json({ ok: false, error: `Not a valid email address: ${bad.join(', ')}` });
      if (to.length > 20) return res.status(400).json({ ok: false, error: 'At most 20 recipients.' });
      const subject = String(b.subject || '').trim().slice(0, 200);
      const body = String(b.body || '').replace(/\r\n/g, '\n').trim().slice(0, 10000);
      if (!subject) return res.status(400).json({ ok: false, error: 'A subject is required.' });
      if (!body) return res.status(400).json({ ok: false, error: 'The message is empty.' });

      const attachments = [];
      if (b.attach) {
        const sh = shape(c);
        const buf = await contentsWorkbook([{ c, sh, contents: contentsFor(c, sh.lanes || [], { processed: true }) }]);
        attachments.push({ filename: `${safeName(c.reference)}_${c.week_start}_POs.xlsx`, content: buf.toString('base64') });
      }
      const html = `<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;color:#121212;">`
        + body.split(/\n{2,}/).map(p => `<p style="margin:0 0 12px;">${escHtml(p).replace(/\n/g, '<br>')}</p>`).join('')
        + `</div>`;

      const sentAt = new Date().toISOString();
      let result;
      try {
        result = await sendViaResend({ from: NOTIFY_FROM, replyTo: NOTIFY_REPLY_TO, to, subject, html, text: body, attachments });
      } catch (err) {
        db.prepare(`INSERT INTO consignment_notification (client_id, consignment_uid, sent_at, sent_by, to_json, subject, body, attached, status, error)
                    VALUES (?,?,?,?,?,?,?,?,?,?)`)
          .run(client, uid, sentAt, user, JSON.stringify(to), subject, body, attachments.length ? 1 : 0, 'failed', String(err.message || err).slice(0, 500));
        return res.status(502).json({ ok: false, error: 'The email could not be sent: ' + String(err.message || err).slice(0, 200) });
      }
      const tx = db.transaction(() => {
        db.prepare(`INSERT INTO consignment_notification (client_id, consignment_uid, sent_at, sent_by, to_json, subject, body, attached, resend_id, status)
                    VALUES (?,?,?,?,?,?,?,?,?,'sent')`)
          .run(client, uid, sentAt, user, JSON.stringify(to), subject, body, attachments.length ? 1 : 0, (result && result.id) || null);
        const up = db.prepare(`INSERT INTO notify_recipient (client_id, email, uses, last_used_at) VALUES (?,?,1,?)
                               ON CONFLICT(client_id, email) DO UPDATE SET uses = uses + 1, last_used_at = excluded.last_used_at`);
        for (const e of to) up.run(client, e, sentAt);
      });
      tx();
      res.json({ ok: true, sent_at: sentAt, to, resend_id: (result && result.id) || null });
    } catch (e) {
      console.error('[consignments] notify failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  console.log('[consignments] model v1 mounted');
  router._internals = { computePlanned, refreshPlanned, shape, STAGES, DEFAULT_RULES, rulesFor, migrateWeek, clearLegacy,
    logEstimate, recordPorts, addViaPort, routeOf, statusOf, contentsFor, originFor, pulseTransit };
  return router;
};
