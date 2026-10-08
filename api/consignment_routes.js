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
    //
    // Transit 18, arrival to cleared 1, cleared to FC 4 — set from what the lanes actually
    // run, not from the 28 the first build guessed. Monday to FC is now 32 days, against the
    // 34 the Advanced PO publishes to the client; it was 40, which meant our own tracker
    // disagreed with our own PO by nearly a week.
    Sea: { packing: 4, cleared: 7, departed: 9, arrDest: 1, destFc: 4, transit: 18 },
    // Air: offsets BACKWARD from the flight. Cleared the day before, packing list the day
    // before that. Departure is entered rather than derived, because it moves.
    Air: { beforeDeparture: { cleared: 1, packing: 2 }, arrDest: 1, destFc: 1, transit: 2 },
  };

  // The old sea figures, kept only so the migration below can recognise a stored rule nobody
  // has deliberately changed. Never used for planning.
  const SUPERSEDED_SEA = { arrDest: 2, destFc: 1, transit: 28 };

  // ── Sea rule migration ──
  // Rules live in a table, so changing the constant above does nothing to a client who has
  // already saved a row. Rewrite only the rows still carrying the superseded figures exactly;
  // a row somebody edited is their decision and is left alone.
  try {
    const stale = db.prepare(`SELECT client_id, facility FROM consignment_rules
                               WHERE mode = 'Sea'
                                 AND arrived_to_dest_cleared_days = ?
                                 AND dest_cleared_to_fc_days = ?
                                 AND default_transit_days = ?`)
      .all(SUPERSEDED_SEA.arrDest, SUPERSEDED_SEA.destFc, SUPERSEDED_SEA.transit);
    if (stale.length) {
      db.prepare(`UPDATE consignment_rules
                     SET arrived_to_dest_cleared_days = ?,
                         dest_cleared_to_fc_days      = ?,
                         default_transit_days         = ?,
                         updated_at                   = ?
                   WHERE mode = 'Sea'
                     AND arrived_to_dest_cleared_days = ?
                     AND dest_cleared_to_fc_days = ?
                     AND default_transit_days = ?`)
        .run(DEFAULT_RULES.Sea.arrDest, DEFAULT_RULES.Sea.destFc, DEFAULT_RULES.Sea.transit,
             new Date().toISOString(),
             SUPERSEDED_SEA.arrDest, SUPERSEDED_SEA.destFc, SUPERSEDED_SEA.transit);
      console.log(`[consignments] sea rules moved to 18/1/4 on ${stale.length} stored row(s)`);
    }
  } catch (e) {
    console.warn('[consignments] sea rule migration skipped:', e.message);
  }

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

  // ── The sea origin rhythm, recorded without asking ──
  // Packing on the Friday and clearing the Monday after are a schedule this operation runs
  // to, not events anybody observes or chases. Asking the team to tick them every week is
  // precisely the unproductive entry the consignment model exists to end, and a tick nobody
  // thought about is worth nothing in a report regardless.
  //
  // So for sea, once the planned day has passed, the stage records itself at that date.
  // Three things keep that honest:
  //
  //   · never in advance. Recording a future date as fact is the original sin of the old
  //     model — 157 departures with zero variance — and doing it here would repeat it.
  //
  //   · tagged `system:rhythm`, so a report can tell it apart from a person's confirmation
  //     and never claims observed variance it does not have. Departure, arrival, clearance
  //     and FC receipt are untouched: those are the legs that actually move.
  //
  //   · still editable. Anyone who knows packing ran late amends it and the amendment wins.
  //     The sweep only ever acts on a stage still `assumed`, so it cannot overwrite a fact.
  const RHYTHM_STAGES = ['packing_list_ready', 'origin_cleared'];
  const RHYTHM_USER = 'system:rhythm';
  const RHYTHM_NOTE = 'Recorded from the sea origin rhythm, not observed. Amend if it ran differently.';

  // Packing and clearing happen at the origin, so the day they pass is the origin's day, not
  // UTC's. Left in UTC, a Friday packing records on the Saturday — the kind of one-day skew
  // that makes an on-time report quietly wrong.
  const RHYTHM_TZ = process.env.CONSIGNMENT_RHYTHM_TZ || 'Asia/Shanghai';
  function rhythmToday() {
    try {
      return new Intl.DateTimeFormat('en-CA', { timeZone: RHYTHM_TZ,
        year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
    } catch (_) {
      return new Date().toISOString().slice(0, 10);
    }
  }

  function autoConfirmRhythm(uid) {
    const c = db.prepare('SELECT * FROM consignment WHERE consignment_uid = ?').get(uid);
    if (!c || c.mode !== 'Sea') return 0;

    const today = rhythmToday();
    const m = milestonesFor(uid);
    const { planned } = computePlanned(c, m);
    const now = new Date().toISOString();
    let n = 0;

    for (const stage of RHYTHM_STAGES) {
      const existing = m[stage];
      if (existing && existing.state !== 'assumed') continue;   // a fact is never overwritten
      const day = planned[stage];
      if (!day || day > today) continue;                        // not yet; never in advance
      upsertMilestone.run({ uid, stage, planned: day, actual: day, state: 'confirmed',
        user: RHYTHM_USER, detail: RHYTHM_NOTE, at: now });
      n++;
    }
    return n;
  }

  // Run across the client's open sea consignments. Cheap, idempotent, and called on read
  // because a consignment nobody touches still passes its Friday.
  function sweepRhythm(client) {
    try {
      const rows = db.prepare(`SELECT consignment_uid FROM consignment
                                WHERE client_id = ? AND mode = 'Sea' AND status != 'closed'`)
        .all(client);
      let n = 0;
      for (const r of rows) n += autoConfirmRhythm(r.consignment_uid);
      return n;
    } catch (e) {
      console.warn('[consignments] rhythm sweep skipped:', e.message);
      return 0;
    }
  }

  // ── Transit migration ──
  // A consignment still carrying the superseded 28-day guess moves with the rule. A carrier
  // quote never moves: transit_confirmed is the whole line between a guess and a promise,
  // and baseline_transit_days is only ever set on the confirmed side of it.
  try {
    const pending = db.prepare(`SELECT consignment_uid FROM consignment
                                 WHERE mode = 'Sea' AND transit_confirmed = 0
                                   AND transit_days = ?`).all(SUPERSEDED_SEA.transit);
    if (pending.length) {
      db.prepare(`UPDATE consignment SET transit_days = ?
                   WHERE mode = 'Sea' AND transit_confirmed = 0 AND transit_days = ?`)
        .run(DEFAULT_RULES.Sea.transit, SUPERSEDED_SEA.transit);
      for (const p of pending) refreshPlanned(p.consignment_uid);
      console.log(`[consignments] ${pending.length} unquoted sea consignment(s) moved 28 → 18 days`);
    }
  } catch (e) {
    console.warn('[consignments] transit migration skipped:', e.message);
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
          // Recorded by the rhythm rather than by a person. Flagged explicitly so no screen
          // or report has to string-match the user, and so "confirmed" on a tile can be
          // shown for what it is: nobody looked, the schedule simply held.
          auto: row.source_user === RHYTHM_USER,
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
      sweepRhythm(client);          // a consignment nobody opened still passed its Friday
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
      // Before deriving what needs a person, let the rhythm settle what does not.
      sweepRhythm(curClient());
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

  console.log('[consignments] model v1 mounted');
  router._internals = { computePlanned, refreshPlanned, shape, STAGES, DEFAULT_RULES, rulesFor, migrateWeek, clearLegacy };
  return router;
};
