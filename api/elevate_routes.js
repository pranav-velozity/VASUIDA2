/* ── VelOzity Pinpoint — Project Elevate: the daily client tracker ──
   THE ICONIC's "VOZ FF Tracker", one row per Zendesk ticket per transport, rebuilt from
   Pinpoint every day and sent at 23:00 Sydney: the full sheet by email as a workbook, the
   same sheet to their SFTP as a CSV, and a second sheet of what changed since the last send.

   What this file insists on:

     · nothing is stored that can be derived. Status, lead times, the week number — all read
       off the dates at build time, so the tracker can never contradict the consignments it
       is built from. The only things persisted are what was SENT (so tomorrow's diff is
       against what the client actually saw) and the notes people write about a change.

     · a row enters at departure. Their sheet carries Handover on exactly the rows that have
       an ETD and never before; the states before that (Ordered, Approved, Pending quote) are
       theirs, not ours. We start at Shipped.

     · the diff is against the last snapshot that was sent, not against yesterday's data. A
       day that failed to send does not lose its changes — they roll into the next send.

     · a note binds to a change, not a day. "Vessel slipped — port congestion at Yantian" is
       about ETA moving from the 8th to the 11th; if it moves again to the 14th that is a new
       fact and the old note does not travel with it.

     · delivered rows go quiet. Seven days after delivery a row leaves change detection for
       good. Without that, one bad upstream day resurrects 292 rows into a single diff.

     · the schedule is an hourly poke and a local-time gate, like the monthly stock report,
       because a fixed UTC cron drifts by an hour twice a year. The gate also catches up: if
       23:00 was missed, the next hour sends it, labelled with the day it is for.

   Mount:
     app.use('/elevate', require('./elevate_routes')({ express, db, authenticateRequest,
       requireRole, auditLog, curClient, sendViaResend, iconicPublisher, ExcelJS, logger }));
*/
'use strict';

module.exports = function mountElevate(deps) {
  const { express, db, authenticateRequest, requireRole, auditLog, curClient,
          sendViaResend, iconicPublisher, ExcelJS, logger } = deps;
  const router = express.Router();
  const log = logger || console;

  const TZ = process.env.ELEVATE_TZ || 'Australia/Sydney';
  const SEND_HOUR = Number(process.env.ELEVATE_HOUR || 23);
  const GRACE_DAYS = Number(process.env.ELEVATE_DELIVERED_GRACE_DAYS || 7);
  const FROM_WEEK = process.env.ELEVATE_FROM_WEEK || '2026-01-05';   // first Monday included
  const SFTP_ENABLED = String(process.env.ELEVATE_SFTP_ENABLED || '').toLowerCase() === 'true';

  // ── Schema ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS elevate_snapshot (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id   TEXT NOT NULL,
      taken_at    TEXT NOT NULL,
      for_date    TEXT NOT NULL,          -- the local calendar day this send is for
      trigger     TEXT NOT NULL,          -- cron | manual | dry_run
      sent        INTEGER NOT NULL DEFAULT 0,
      row_count   INTEGER NOT NULL DEFAULT 0,
      change_count INTEGER NOT NULL DEFAULT 0,
      email_id    TEXT,
      sftp_path   TEXT,
      error       TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_elevate_snap ON elevate_snapshot(client_id, sent, id);

    CREATE TABLE IF NOT EXISTS elevate_snapshot_row (
      snapshot_id INTEGER NOT NULL,
      row_key     TEXT NOT NULL,          -- zendesk|transport
      data        TEXT NOT NULL,          -- the row as sent, JSON
      PRIMARY KEY (snapshot_id, row_key)
    );

    -- A note is about one change: the row, the field, and the old → new values. If the
    -- value moves again the signature differs and the note stays with the fact it was about.
    CREATE TABLE IF NOT EXISTS elevate_note (
      client_id   TEXT NOT NULL,
      row_key     TEXT NOT NULL,
      signature   TEXT NOT NULL,
      note        TEXT NOT NULL,
      user_id     TEXT,
      created_at  TEXT NOT NULL,
      PRIMARY KEY (client_id, row_key, signature)
    );
  `);

  // ── Column layout: THE ICONIC's sheet, A → AB, in their order and their words ──
  // Three header rows, as theirs: an ownership band, a blank, then the field names. Row 4 is
  // the first data row. Kept exactly so the file is a drop-in for the one they maintain.
  const COLUMNS = [
    { key: 'transport',  head: 'TRANSPORT',                    band: '' },
    { key: 'vessel',     head: 'Vessel ',                      band: 'VELOZITY' },
    { key: 'ccl',        head: 'CCL',                          band: 'VELOZITY' },
    { key: 'sca',        head: 'SCA',                          band: 'VELOZITY' },
    { key: 'dor',        head: 'DOR',                          band: 'VELOZITY' },
    { key: 'shipment',   head: 'Shipment #',                   band: 'VELOZITY' },
    { key: 'hbl',        head: 'HBL',                          band: 'VELOZITY TO FILL IN' },
    { key: 'mbl',        head: 'MBL / CNTR',                   band: '' },
    { key: 'pos',        head: 'PO #',                         band: '' },
    { key: 'zendesk',    head: 'ZENDESK JOB # \n(Link to docs)', band: 'Jess' },
    { key: 'vendor',     head: 'VENDOR',                       band: 'ICONIC TO FILL IN' },
    { key: 'status',     head: 'Status',                       band: '' },
    { key: 'ex_factory', head: 'Ex Factory',                   band: '' },
    { key: 'handover',   head: 'Handover Date',                band: 'VELOZITY  TO FILL IN ' },
    { key: 'etd',        head: 'ETD',                          band: '' },
    { key: 'eta',        head: 'ETA',                          band: '' },
    { key: 'delivery',   head: 'Delivery Date to FC',          band: '' },
    { key: 'dw_week',    head: 'Expected DW into the FC',      band: 'ICONIC' },
    { key: 'lt_d2d',     head: 'Lead time (Door to door)',     band: 'ICONIC' },
    { key: 'lt_transit', head: 'Lead time (transit days)',     band: 'ICONIC' },
    { key: 'lt_port_fc', head: 'Lead Time (Port to FC)',       band: 'ICONIC' },
    { key: 'lt_xf_fc',   head: 'Lead Time XF > FC',            band: '' },
    { key: 'weight_kg',  head: 'Weight of shipment  (KG)',     band: '' },
    { key: 'cartons',    head: 'CTNS',                         band: 'ICONIC' },
    { key: 'units',      head: 'Units',                        band: 'ICONIC' },
    { key: 'origin_country', head: 'Origin Country ',          band: '' },
    { key: 'origin_city',    head: 'Origin City ',             band: 'ICONIC' },
    { key: 'rolling_dw', head: 'Rolling DW in BC',             band: 'ICONIC' },
  ];
  const DATE_KEYS = new Set(['ex_factory', 'handover', 'etd', 'eta', 'delivery']);
  const NUM_KEYS = new Set(['lt_d2d', 'lt_transit', 'lt_port_fc', 'lt_xf_fc', 'weight_kg', 'cartons', 'units']);

  // The fields a change is reported on. Lead times and the week are derived from these, so
  // reporting them too would say the same thing twice.
  const TRACKED = [
    ['status',   'Status'],
    ['etd',      'ETD'],
    ['eta',      'ETA'],
    ['delivery', 'Delivery Date to FC'],
    ['handover', 'Handover Date'],
    ['vessel',   'Vessel'],
    ['shipment', 'Shipment #'],
    ['mbl',      'MBL / CNTR'],
    ['hbl',      'HBL'],
  ];
  const TRACKED_LABEL = Object.fromEntries(TRACKED);

  // Their four words for our four states, spelled as their sheet spells them — including
  // the trailing space on Delivery Booked, which their filters depend on.
  const STATUS = { shipped: 'Shipped', landed: 'Landed On Route', booked: 'Delivery Booked ', delivered: 'Delivered' };
  const STATUS_RANK = { 'Shipped': 1, 'Landed On Route': 2, 'Delivery Booked ': 3, 'Delivered': 4 };

  // ── Small helpers ──
  const ymd = (v) => {
    if (!v) return null;
    const s = String(v).trim();
    const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
    return m ? m[1] : null;
  };
  const addDays = (d, n) => { const x = new Date(d + 'T00:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
  const dayDiff = (a, b) => (a && b) ? Math.round((new Date(b + 'T00:00:00Z') - new Date(a + 'T00:00:00Z')) / 86400000) : null;
  const isoWeek = (d) => {
    if (!d) return null;
    const x = new Date(d + 'T00:00:00Z');
    const day = x.getUTCDay() || 7;
    x.setUTCDate(x.getUTCDate() + 4 - day);
    const y0 = new Date(Date.UTC(x.getUTCFullYear(), 0, 1));
    return Math.ceil((((x - y0) / 86400000) + 1) / 7);
  };
  const fmtDay = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }) : '';
  const fmtLong = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }) : '';

  function localParts(at) {
    const f = new Intl.DateTimeFormat('en-AU', { timeZone: TZ,
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hour12: false });
    const p = Object.fromEntries(f.formatToParts(at || new Date()).map(x => [x.type, x.value]));
    return { date: `${p.year}-${p.month}-${p.day}`, h: Number(p.hour) % 24 };
  }
  const localToday = () => localParts().date;

  // Origin city from the facility code. VOZ_* runs out of Shenzhen, VOS_* out of Shanghai;
  // anything else can be named in ELEVATE_ORIGIN_CITIES as "PREFIX=City,PREFIX=City".
  const ORIGIN_CITY = (() => {
    const m = { VOZ: 'Shenzhen', VOS: 'Shanghai' };
    String(process.env.ELEVATE_ORIGIN_CITIES || '').split(',').map(s => s.trim()).filter(Boolean)
      .forEach(pair => { const [k, v] = pair.split('='); if (k && v) m[k.trim().toUpperCase()] = v.trim(); });
    return m;
  })();
  const originCity = (facility) => {
    const p = String(facility || '').split('_')[0].toUpperCase();
    return ORIGIN_CITY[p] || ORIGIN_CITY.VOZ;
  };

  // ── Data access ──
  function planRows(ws, client) {
    try {
      const r = db.prepare('SELECT data FROM plans WHERE week_start = ? AND client_id = ?').get(ws, client);
      if (!r) return [];
      const arr = JSON.parse(r.data);
      return Array.isArray(arr) ? arr : [];
    } catch (_) { return []; }
  }
  const _planCache = new Map();
  const planFor = (ws, client) => {
    const k = client + '|' + ws;
    if (!_planCache.has(k)) _planCache.set(k, planRows(ws, client));
    return _planCache.get(k);
  };

  // Same lane resolution the consignment model uses: supplier||zendesk||freight.
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
    if (f && m.some(r => r.freight_type)) m = m.filter(r => !r.freight_type || String(r.freight_type).trim().toLowerCase() === f);
    return m;
  }

  // Handover: the latest receipt among the POs that HAVE one. A PO not yet received does
  // not blank the row; only a row with no receipt at all stays empty.
  const receiptStmt = db.prepare(`SELECT received_at_local, received_at_utc FROM receiving
                                   WHERE week_start = ? AND po_number = ?`);
  function handoverFor(ws, pos) {
    let best = null;
    for (const po of pos) {
      const r = receiptStmt.get(ws, po);
      const d = r ? (ymd(r.received_at_local) || ymd(r.received_at_utc)) : null;
      if (d && (!best || d > best)) best = d;
    }
    return best;
  }

  // Last mile lives in flow_week.data.lastmile_receipts, keyed by container_uid, which is the
  // container number — the consignment's reference.
  const _flowCache = new Map();
  // The consignment's reference is Manage Containers' container_id; Last Mile keys its
  // bookings by container_uid. Usually the same string, not always — server.js looks up by
  // both, and so does this. The uid is found through the week's container list.
  function lastMileFor(facility, ws, reference) {
    if (!reference) return {};
    const k = facility + '|' + ws;
    if (!_flowCache.has(k)) {
      let map = {}, uidOf = {};
      try {
        const r = db.prepare('SELECT data FROM flow_week WHERE facility = ? AND week_start = ?').get(facility, ws);
        if (r) {
          const d = JSON.parse(r.data) || {};
          if (d.lastmile_receipts && typeof d.lastmile_receipts === 'object') map = d.lastmile_receipts;
          const wc = d.intl_weekcontainers;
          const list = Array.isArray(wc) ? wc : (Array.isArray(wc && wc.containers) ? wc.containers : []);
          for (const c of list) {
            const id = String(c.container_id || c.container || '').trim();
            const uid = String(c.container_uid || c.uid || '').trim();
            if (id && uid) uidOf[id] = uid;
          }
        }
      } catch (_) {}
      _flowCache.set(k, { map, uidOf });
    }
    const { map, uidOf } = _flowCache.get(k);
    const ref = String(reference).trim();
    return map[ref] || (uidOf[ref] && map[uidOf[ref]]) || {};
  }

  // Cartons, units and weight: what went into the box, from the mobile bins. One bin is one
  // carton out; its units and weight are on the bin; the PO comes from the scans in it, as
  // the Pulse context already computes. Planned target_qty is the fallback for units only,
  // and only where nothing was binned — a row enters at departure, so that should be rare.
  const _binCache = new Map();
  function binsByPo(ws) {
    if (_binCache.has(ws)) return _binCache.get(ws);
    const out = new Map();
    try {
      const we = addDays(ws, 6);
      const rows = db.prepare(`
        SELECT b.mobile_bin, b.total_units, b.weight_kg, r.po_number
        FROM bins b
        LEFT JOIN (
          SELECT TRIM(mobile_bin) AS mobile_bin, po_number, COUNT(*) AS scan_count
          FROM records
          WHERE date_local >= ? AND date_local <= ?
            AND TRIM(COALESCE(mobile_bin,'')) <> '' AND TRIM(COALESCE(po_number,'')) <> ''
          GROUP BY TRIM(mobile_bin), po_number
        ) r ON TRIM(b.mobile_bin) = r.mobile_bin
        WHERE b.week_start = ?
        ORDER BY r.scan_count DESC`).all(ws, we, ws);
      const seen = new Set();
      for (const b of rows) {
        const bin = String(b.mobile_bin || '').trim();
        if (!bin || seen.has(bin)) continue;        // first row per bin is the PO with most scans
        seen.add(bin);
        const po = String(b.po_number || '').trim();
        if (!po) continue;
        if (!out.has(po)) out.set(po, { cartons: 0, units: 0, weight_kg: 0 });
        const e = out.get(po);
        e.cartons++;
        e.units += Number(b.total_units || 0) || 0;
        e.weight_kg += Number(b.weight_kg || 0) || 0;
      }
    } catch (e) { log.warn('[elevate] bins lookup failed:', e.message); }
    _binCache.set(ws, out);
    return out;
  }

  // ── Assemble: every consignment that has departed, one row per Zendesk per transport ──
  function assemble(client) {
    _planCache.clear(); _flowCache.clear(); _binCache.clear();
    const cons = db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND week_start >= ?
                              ORDER BY week_start, mode, reference`).all(client, FROM_WEEK);
    const msStmt = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ?');
    const laneStmt = db.prepare('SELECT lane_key FROM consignment_lane WHERE consignment_uid = ?');

    const byKey = new Map();   // row_key → accumulating row
    for (const c of cons) {
      const ms = {};
      for (const r of msStmt.all(c.consignment_uid)) ms[r.stage] = r;
      const departed = ms.departed && ms.departed.actual_at ? ymd(ms.departed.actual_at) : null;
      if (!departed) continue;                       // not ours yet: the row enters at Shipped

      const arrived = ms.arrived && ms.arrived.actual_at ? ymd(ms.arrived.actual_at) : null;
      const fcActual = ms.fc_receipt && ms.fc_receipt.actual_at ? ymd(ms.fc_receipt.actual_at) : null;
      const etaForecast = ymd(c.carrier_eta) || (ms.arrived && ymd(ms.arrived.planned_at)) || null;
      const fcForecast = ms.fc_receipt && ymd(ms.fc_receipt.planned_at) || null;
      const lm = lastMileFor(c.facility, c.week_start, c.reference);
      const delivered = ymd(lm.delivery_local || lm.delivered_at || lm.delivered_local) || fcActual || null;
      const booked = ymd(lm.scheduled_for) || null;

      const status = delivered ? STATUS.delivered
                   : booked    ? STATUS.booked
                   : arrived   ? STATUS.landed
                   :             STATUS.shipped;
      const transport = c.mode === 'Air' ? 'AIR' : 'FCL';
      const plan = planFor(c.week_start, client);

      for (const { lane_key } of laneStmt.all(c.consignment_uid)) {
        const zendesk = String(lane_key).split('||')[1] || '';
        if (!zendesk.trim()) continue;
        const rows = rowsForLane(lane_key, plan);
        const pos = [...new Set(rows.map(r => String(r.po_number || r.po || '').trim()).filter(Boolean))];
        const key = `${zendesk.trim()}|${transport}`;
        const bins = binsByPo(c.week_start);
        let cartons = 0, units = 0, weight = 0, binned = false;
        for (const po of pos) { const b = bins.get(po); if (b) { binned = true; cartons += b.cartons; units += b.units; weight += b.weight_kg; } }
        if (!binned) units = rows.reduce((a, r) => a + (Number(r.target_qty) || 0), 0);   // planned, as a fallback

        const piece = {
          row_key: key,
          transport,
          vessel: c.mode === 'Air' ? (c.reference || '') : (c.vessel || ''),
          ccl: '', sca: '', dor: '',
          shipment: c.shipment_ref || c.mbl || '',
          hbl: c.hbl || '',
          mbl: c.mode === 'Air' ? (c.mbl || c.reference || '') : (c.reference || ''),
          pos,
          zendesk: zendesk.trim(),
          vendor: (rows[0] && rows[0].supplier_name) || String(lane_key).split('||')[0] || '',
          status,
          ex_factory: c.week_start,
          handover: handoverFor(c.week_start, pos),
          etd: departed,
          eta: arrived || etaForecast,
          // Delivered beats booked beats forecast: each is a firmer statement than the last.
          delivery: delivered || booked || fcForecast,
          weight_kg: weight ? Math.round(weight * 10) / 10 : null,
          cartons: cartons || null,
          units: units || null,
          _binned: binned,
          origin_country: 'China',
          origin_city: originCity(c.facility),
          rolling_dw: '',
          // provenance, for the change context and the screen — never written to the sheet
          _eta_actual: !!arrived, _delivery_actual: !!delivered, _delivered_on: delivered,
          _eta_source: arrived ? 'confirmed' : (c.carrier_eta ? 'carrier' : 'plan'),
          _delivery_source: delivered ? 'confirmed' : (booked ? 'booked' : 'plan'),
          _consignments: [c.reference || c.consignment_uid],
          _week: isoWeek(c.week_start),
          // Later than first promised, in days, against the frozen baseline FC date. Null
          // where there is no baseline (no carrier quote was ever entered).
          _late: c.baseline_fc_at ? dayDiff(ymd(c.baseline_fc_at), delivered || booked || fcForecast) : null,
          _eta_basis: arrived ? 'confirmed' : (c.carrier_eta ? 'carrier estimate' : `plan: departed + ${c.transit_days} days`),
        };

        // A Zendesk on two consignments of the same mode (the rare air split): earliest ETD,
        // latest ETA, latest delivery, the furthest-along status — the rule agreed for POs.
        const have = byKey.get(key);
        if (!have) { byKey.set(key, piece); continue; }
        have.etd = [have.etd, piece.etd].filter(Boolean).sort()[0] || null;
        have.eta = [have.eta, piece.eta].filter(Boolean).sort().pop() || null;
        have.delivery = [have.delivery, piece.delivery].filter(Boolean).sort().pop() || null;
        have.handover = [have.handover, piece.handover].filter(Boolean).sort().pop() || null;
        if (STATUS_RANK[piece.status] < STATUS_RANK[have.status]) have.status = piece.status;   // the slower one
        have.pos = [...new Set([...have.pos, ...piece.pos])];
        for (const k of ['vessel', 'shipment', 'hbl', 'mbl']) if (piece[k] && !String(have[k]).includes(piece[k])) have[k] = [have[k], piece[k]].filter(Boolean).join(', ');
        have.cartons = (have.cartons || 0) + (piece.cartons || 0) || null;
        have.units = (have.units || 0) + (piece.units || 0) || null;
        have.weight_kg = (have.weight_kg || 0) + (piece.weight_kg || 0) || null;
        have._late = [have._late, piece._late].filter(x => x != null).sort((x, y) => y - x)[0] ?? null;
        have._binned = have._binned || piece._binned;
        have._eta_actual = have._eta_actual && piece._eta_actual;
        have._delivery_actual = have._delivery_actual && piece._delivery_actual;
        if (!have._eta_actual && piece._eta_source === 'plan') have._eta_source = 'plan';
        if (!have._delivery_actual && piece._delivery_source === 'plan') have._delivery_source = 'plan';
        have._consignments.push(...piece._consignments);
      }
    }

    // Derived columns, from the dates and nothing else.
    const out = [];
    for (const r of byKey.values()) {
      // A dock cannot be booked, let alone delivered, before the ship arrives. A forecast
      // arrival after either is not a forecast, it is an arrival nobody has confirmed. The
      // screen shows it so somebody does; the file does not send a date that cannot be true.
      r._eta_impossible = !r._eta_actual && r.eta && r.delivery
        && r._delivery_source !== 'plan' && r.eta > r.delivery;
      r.dw_week = r.delivery ? `Week ${isoWeek(r.delivery)}` : '';
      r.lt_d2d = dayDiff(r.handover, r.delivery);
      r.lt_transit = dayDiff(r.etd, r.eta);
      r.lt_port_fc = dayDiff(r.eta, r.delivery);
      r.lt_xf_fc = dayDiff(r.ex_factory, r.delivery);
      out.push(r);
    }
    out.sort((a, b) => (a.ex_factory || '').localeCompare(b.ex_factory || '') || a.row_key.localeCompare(b.row_key));
    return out;
  }

  // The row as it goes to the sheet and the snapshot: public fields only, POs as their
  // newline list, nothing that starts with an underscore.
  function publicRow(r) {
    const o = {};
    for (const col of COLUMNS) {
      let v = r[col.key];
      if (col.key === 'pos') v = (r.pos || []).join('\n');
      if (col.key === 'eta' && r._eta_impossible) v = '';
      o[col.key] = v == null ? '' : v;
    }
    return o;
  }

  // What the page gets: the sheet's columns, plus the week and where each date came from.
  // Neither of the extras is in the file — the week would shift THE ICONIC's column letters,
  // and provenance is what the colour on screen says, not a thing the client needs in a cell.
  function screenRow(r) {
    return Object.assign(publicRow(r), {
      ex_week: r._week ? `W${r._week}` : '',
      eta: r.eta || '',                        // the page sees it even when the file will not
      late_days: r._late,
      eta_impossible: !!r._eta_impossible,
      eta_basis: r._eta_basis,
      qty_source: r._binned ? 'bins' : (r.units ? 'plan' : ''),
      prov: {
        handover: r.handover ? 'actual' : '',
        etd: 'actual',
        eta: r._eta_actual ? 'actual' : (r._eta_source === 'carrier' ? 'carrier' : 'plan'),
        delivery: r._delivery_actual ? 'actual' : (r._delivery_source === 'booked' ? 'booked' : 'plan'),
      },
    });
  }

  // ── Snapshots ──
  function lastSent(client) {
    const s = db.prepare(`SELECT * FROM elevate_snapshot WHERE client_id = ? AND sent = 1
                           ORDER BY id DESC LIMIT 1`).get(client);
    if (!s) return null;
    const rows = db.prepare('SELECT row_key, data FROM elevate_snapshot_row WHERE snapshot_id = ?').all(s.id);
    const map = new Map();
    for (const r of rows) { try { map.set(r.row_key, JSON.parse(r.data)); } catch (_) {} }
    return { snapshot: s, rows: map };
  }

  const insSnap = db.prepare(`INSERT INTO elevate_snapshot (client_id, taken_at, for_date, trigger, sent, row_count, change_count, email_id, sftp_path, error)
                              VALUES (@client, @at, @for_date, @trigger, @sent, @rows, @changes, @email_id, @sftp_path, @error)`);
  const insRow = db.prepare('INSERT INTO elevate_snapshot_row (snapshot_id, row_key, data) VALUES (?,?,?)');
  const writeSnapshot = db.transaction((meta, rows) => {
    const info = insSnap.run(meta);
    const id = info.lastInsertRowid;
    for (const r of rows) insRow.run(id, r.row_key, JSON.stringify(Object.assign(publicRow(r), { _delivered_on: r._delivered_on || null })));
    return id;
  });

  // ── Diff: what the client has not seen yet ──
  // Context is written here, from provenance the sheet never shows: whether a new ETA is a
  // confirmed arrival, a carrier revision, or a re-plan. The person on the working page can
  // add to it; they should not have to write it.
  function describe(field, oldV, newV, r) {
    const moved = (DATE_KEYS.has(field) && oldV && newV) ? dayDiff(oldV, newV) : null;
    const dir = moved == null ? '' : moved > 0 ? `${moved} day${moved === 1 ? '' : 's'} later` : moved < 0 ? `${-moved} day${moved === -1 ? '' : 's'} earlier` : 'same day';
    if (field === 'status') {
      if (STATUS_RANK[newV] < STATUS_RANK[oldV]) return `Went back from ${String(oldV).trim()} — the booking no longer stands.`;
      if (newV === STATUS.delivered) return `Delivered to the FC on ${fmtDay(r.delivery)}.`;
      if (newV === STATUS.booked) return `Dock booked for ${fmtDay(r.delivery)}.`;
      if (newV === STATUS.landed) return `Arrived ${fmtDay(r.eta)}; awaiting release and delivery.`;
      return `Departed ${fmtDay(r.etd)}.`;
    }
    if (field === 'eta') {
      if (r._eta_actual) return `Arrival confirmed ${fmtDay(newV)}${oldV ? ` — ${dir} than the ${fmtDay(oldV)} forecast` : ''}.`;
      if (r._eta_source === 'carrier') return `Carrier revised the ETA to ${fmtDay(newV)}${oldV ? ` (${dir})` : ''}.`;
      return `ETA re-planned to ${fmtDay(newV)}${oldV ? ` (${dir})` : ''}.`;
    }
    if (field === 'delivery') {
      if (r._delivery_actual) return `Delivered ${fmtDay(newV)}${oldV ? ` — ${dir} than forecast` : ''}.`;
      if (r._delivery_source === 'booked') return `Dock booked for ${fmtDay(newV)}${oldV ? ` (${dir})` : ''}.`;
      return `Forecast delivery moved to ${fmtDay(newV)}${oldV ? ` (${dir})` : ''}.`;
    }
    if (field === 'etd') return `Departure recorded ${fmtDay(newV)}${oldV ? ` (${dir})` : ''}.`;
    if (field === 'handover') return `Handover ${fmtDay(newV)}${oldV ? ` (was ${fmtDay(oldV)})` : ''}.`;
    return oldV ? `${TRACKED_LABEL[field]} changed from ${oldV} to ${newV}.` : `${TRACKED_LABEL[field]} added: ${newV}.`;
  }

  function diff(client, current, last) {
    const prev = last ? last.rows : new Map();
    const asOf = localToday();
    const changes = [];
    const notes = new Map(db.prepare('SELECT row_key, signature, note FROM elevate_note WHERE client_id = ?').all(client)
      .map(n => [n.row_key + '::' + n.signature, n.note]));

    for (const r of current) {
      const was = prev.get(r.row_key);
      const wasDelivered = was && was.status === STATUS.delivered;
      // Seven days delivered: out of change detection for good.
      if (wasDelivered && was._delivered_on && dayDiff(was._delivered_on, asOf) > GRACE_DAYS) continue;

      if (!was) {
        if (last) {   // a brand new row is a change; on the very first send nothing is "new"
          const sig = `new`;
          changes.push({ row_key: r.row_key, zendesk: r.zendesk, transport: r.transport, field: 'new', label: 'New',
            old: '', new: r.status, signature: sig, context: `Departed ${fmtDay(r.etd)}; first appearance on the tracker.`,
            note: notes.get(r.row_key + '::' + sig) || '', anomaly: false, vendor: r.vendor, pos: r.pos.join(', ') });
        }
        continue;
      }
      for (const [field] of TRACKED) {
        const a = was[field] == null ? '' : String(was[field]);
        const b = r[field] == null ? '' : String(r[field]);
        if (a === b) continue;
        const sig = `${field}|${a}>${b}`;
        changes.push({ row_key: r.row_key, zendesk: r.zendesk, transport: r.transport, field, label: TRACKED_LABEL[field],
          old: a, new: b, signature: sig, context: describe(field, a, b, r),
          note: notes.get(r.row_key + '::' + sig) || '',
          // A delivered row that changes is a question, not an update.
          anomaly: !!wasDelivered, vendor: r.vendor, pos: r.pos.join(', ') });
      }
    }
    // Rows that vanished since the last send: not silently. Reported once.
    const nowKeys = new Set(current.map(r => r.row_key));
    for (const [k, was] of prev) {
      if (nowKeys.has(k)) continue;
      if (was.status === STATUS.delivered && was._delivered_on && dayDiff(was._delivered_on, asOf) > GRACE_DAYS) continue;
      const sig = 'removed';
      changes.push({ row_key: k, zendesk: was.zendesk, transport: was.transport, field: 'removed', label: 'Removed',
        old: was.status, new: '', signature: sig, context: 'No longer on a departed consignment. Check the week plan.',
        note: notes.get(k + '::' + sig) || '', anomaly: true, vendor: was.vendor, pos: was.pos });
    }
    const order = { new: 0, status: 1, delivery: 2, eta: 3, etd: 4, handover: 5, vessel: 6, shipment: 7, mbl: 8, hbl: 9, removed: 10 };
    changes.sort((x, y) => (x.anomaly === y.anomaly ? 0 : x.anomaly ? 1 : -1) || (order[x.field] - order[y.field]) || x.row_key.localeCompare(y.row_key));
    return changes;
  }

  function stats(rows, changes) {
    const delivered = rows.filter(r => r.status === STATUS.delivered).length;
    const booked = rows.filter(r => r.status === STATUS.booked).length;
    const landed = rows.filter(r => r.status === STATUS.landed).length;
    const shipped = rows.filter(r => r.status === STATUS.shipped).length;
    return { rows: rows.length, delivered, open: rows.length - delivered, shipped, landed, booked,
             changes: changes.length, anomalies: changes.filter(c => c.anomaly).length,
             attention: rows.filter(r => r._eta_impossible).length,
             late: rows.filter(r => r._late != null && r._late >= 2 && r.status !== STATUS.delivered).length };
  }

  // ── The workbook and the CSV ──
  async function workbook(rows, changes, forDate) {
    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();

    // Notes per row for column AC: every change on the row since the last send, context
    // and note together. AC sits after their last column, so their letters do not move.
    const noteFor = new Map();
    for (const r of rows) if (r._eta_impossible) noteFor.set(r.row_key, 'ETA: arrival date to be confirmed.');
    for (const c of changes) {
      const bits = [c.context, c.note].filter(Boolean).join(' ');
      if (!bits) continue;
      noteFor.set(c.row_key, (noteFor.get(c.row_key) ? noteFor.get(c.row_key) + '\n' : '') + `${c.label}: ${bits}`);
    }

    const ws = wb.addWorksheet('VOZ FF Tracker', { views: [{ state: 'frozen', ySplit: 3 }] });
    ws.addRow([...COLUMNS.map(c => c.band), 'VELOZITY']);
    ws.addRow([]);
    const head = ws.addRow([...COLUMNS.map(c => c.head), 'VelOzity notes']);
    head.font = { bold: true };
    head.alignment = { wrapText: true, vertical: 'middle' };
    ws.getRow(1).font = { bold: true, color: { argb: 'FF6E6E73' }, size: 9 };
    for (const r of rows) {
      const p = publicRow(r);
      const line = ws.addRow([...COLUMNS.map(c => {
        const v = p[c.key];
        if (DATE_KEYS.has(c.key)) return v ? new Date(v + 'T00:00:00Z') : null;
        if (NUM_KEYS.has(c.key)) return v === '' ? null : Number(v);
        return v;
      }), noteFor.get(r.row_key) || '']);
      line.getCell(9).alignment = { wrapText: true, vertical: 'top' };   // PO #
      line.getCell(COLUMNS.length + 1).alignment = { wrapText: true, vertical: 'top' };
    }
    ws.getColumn(COLUMNS.length + 1).width = 48;
    COLUMNS.forEach((c, i) => {
      const col = ws.getColumn(i + 1);
      if (DATE_KEYS.has(c.key)) { col.numFmt = 'dd/mm/yyyy'; col.width = 12; }
      else if (NUM_KEYS.has(c.key)) col.width = 9;
      else if (c.key === 'pos') col.width = 14;
      else if (c.key === 'vendor' || c.key === 'vessel') col.width = 30;
      else col.width = Math.max(10, Math.min(22, String(c.head).length + 2));
    });

    const cs = wb.addWorksheet('Changes', { views: [{ state: 'frozen', ySplit: 1 }] });
    cs.addRow(['Zendesk', 'Transport', 'Vendor', 'PO #', 'Field', 'Was', 'Now', 'Context', 'Note from VelOzity', 'Flag']).font = { bold: true };
    if (!changes.length) {
      cs.addRow([`No changes since the last update. Position as at ${fmtLong(forDate)}.`]);
    }
    for (const c of changes) {
      cs.addRow([c.zendesk, c.transport, c.vendor, c.pos, c.label,
        DATE_KEYS.has(c.field) && c.old ? new Date(c.old + 'T00:00:00Z') : String(c.old).trim(),
        DATE_KEYS.has(c.field) && c.new ? new Date(c.new + 'T00:00:00Z') : String(c.new).trim(),
        c.context, c.note || '', c.anomaly ? 'Changed after delivery — please check' : '']);
    }
    [10, 10, 28, 16, 18, 12, 12, 56, 44, 30].forEach((w, i) => { cs.getColumn(i + 1).width = w; });
    cs.getColumn(6).numFmt = 'dd/mm/yyyy'; cs.getColumn(7).numFmt = 'dd/mm/yyyy';
    cs.getColumn(8).alignment = { wrapText: true, vertical: 'top' };
    cs.getColumn(9).alignment = { wrapText: true, vertical: 'top' };

    return Buffer.from(await wb.xlsx.writeBuffer());
  }

  function csv(rows) {
    const q = (v) => {
      const s = v == null ? '' : String(v);
      return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    const lines = [COLUMNS.map(c => q(String(c.head).replace(/\s*\n\s*/g, ' ').trim())).join(',')];
    for (const r of rows) {
      const p = publicRow(r);
      lines.push(COLUMNS.map(c => q(c.key === 'pos' ? String(p.pos).replace(/\n/g, '; ') : p[c.key])).join(','));
    }
    return lines.join('\r\n') + '\r\n';
  }

  // ── Send ──
  function recipients() {
    const split = (v) => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
    return { to: split(process.env.ELEVATE_TO), cc: split(process.env.ELEVATE_CC),
             from: process.env.ELEVATE_FROM || process.env.WEEKLY_REPORT_FROM
                || process.env.MONTHLY_CLIENT_REPORT_FROM || process.env.EXCEPTION_EMAIL_FROM || null };
  }

  function emailBody(st, changes, forDate, filename) {
    const line = (c) => `${c.zendesk} ${c.transport} · ${c.label}: ${c.old ? (DATE_KEYS.has(c.field) ? fmtDay(c.old) : String(c.old).trim()) + ' → ' : ''}${DATE_KEYS.has(c.field) ? fmtDay(c.new) : String(c.new).trim()}`;
    const top = changes.filter(c => !c.anomaly).slice(0, 12);
    const text = [
      `Daily FF tracker — ${fmtLong(forDate)}`, '',
      `${st.rows} lines: ${st.delivered} delivered, ${st.open} open (${st.shipped} shipped, ${st.landed} landed, ${st.booked} dock booked).`,
      changes.length ? `${changes.length} change${changes.length === 1 ? '' : 's'} since the last update:` : 'No changes since the last update.',
      ...top.map(c => `  · ${line(c)}${c.context ? ' — ' + c.context : ''}${c.note ? ' (' + c.note + ')' : ''}`),
      changes.length > top.length ? `  … and ${changes.length - top.length} more on the Changes sheet.` : '',
      st.anomalies ? `${st.anomalies} change${st.anomalies === 1 ? '' : 's'} on delivered lines — flagged on the Changes sheet for your review.` : '',
      '', `Attached: ${filename} (sheet 1 the full tracker, sheet 2 the changes).`,
    ].filter(l => l !== '').join('\n');

    const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const html = `
      <div style="font-family:-apple-system,Segoe UI,sans-serif;color:#1C1C1E;max-width:680px;">
        <div style="font-size:16px;font-weight:700;">Daily FF tracker</div>
        <div style="font-size:12px;color:#6E6E73;margin-top:2px;">${esc(fmtLong(forDate))}</div>
        <p style="font-size:14px;line-height:1.6;margin:16px 0 8px;">
          <b>${st.rows}</b> lines — ${st.delivered} delivered, <b>${st.open} open</b>
          (${st.shipped} shipped, ${st.landed} landed, ${st.booked} dock booked).</p>
        ${changes.length ? `
        <div style="font-size:13px;font-weight:600;margin:14px 0 6px;">${changes.length} change${changes.length === 1 ? '' : 's'} since the last update</div>
        <table style="border-collapse:collapse;font-size:13px;width:100%;">
          ${top.map(c => `<tr>
            <td style="padding:5px 8px 5px 0;border-top:1px solid #EEE;white-space:nowrap;vertical-align:top;"><b>${esc(c.zendesk)}</b> <span style="color:#6E6E73;">${esc(c.transport)}</span></td>
            <td style="padding:5px 8px;border-top:1px solid #EEE;white-space:nowrap;vertical-align:top;">${esc(c.label)}</td>
            <td style="padding:5px 8px;border-top:1px solid #EEE;vertical-align:top;">${c.old ? esc(DATE_KEYS.has(c.field) ? fmtDay(c.old) : String(c.old).trim()) + ' → ' : ''}<b>${esc(DATE_KEYS.has(c.field) ? fmtDay(c.new) : String(c.new).trim())}</b></td>
            <td style="padding:5px 0 5px 8px;border-top:1px solid #EEE;color:#48484A;vertical-align:top;">${esc(c.context)}${c.note ? `<div style="color:#1C1C1E;margin-top:2px;">${esc(c.note)}</div>` : ''}</td>
          </tr>`).join('')}
        </table>
        ${changes.length > top.length ? `<div style="font-size:12px;color:#6E6E73;margin-top:6px;">… and ${changes.length - top.length} more on the Changes sheet.</div>` : ''}
        ${st.anomalies ? `<div style="font-size:13px;margin-top:12px;padding:8px 10px;background:#FFF4E5;border-radius:6px;">${st.anomalies} change${st.anomalies === 1 ? '' : 's'} on delivered lines — flagged on the Changes sheet for your review.</div>` : ''}
        ` : `<p style="font-size:14px;color:#6E6E73;">No changes since the last update.</p>`}
        <div style="font-size:12px;color:#6E6E73;margin-top:18px;">${esc(filename)} — sheet 1 the full tracker, sheet 2 the changes.</div>
      </div>`;
    return { text, html };
  }

  async function send(client, opts) {
    const o = opts || {};
    const forDate = o.forDate || localToday();
    const trigger = o.dryRun ? 'dry_run' : (o.trigger || 'manual');
    const rows = assemble(client);
    const last = lastSent(client);
    const changes = diff(client, rows, last);
    const st = stats(rows, changes);
    const stamp = forDate.replace(/-/g, '');
    const xlsxName = `VOZ_FF_Tracker_${stamp}.xlsx`;
    const csvName = `VOZ_FF_Tracker_${stamp}.csv`;

    if (!rows.length) {
      return { sent: false, reason: 'no_rows', message: 'Nothing has departed since the start of the tracker window. Nothing was sent.' };
    }
    const { to, cc, from } = recipients();
    const xlsx = await workbook(rows, changes, forDate);
    const body = emailBody(st, changes, forDate, xlsxName);
    const subject = `ICONIC — Daily FF tracker — ${fmtLong(forDate)}${changes.length ? ` — ${changes.length} change${changes.length === 1 ? '' : 's'}` : ' — no changes'}`;

    if (o.dryRun) {
      return { sent: false, dryRun: true, for_date: forDate, to, cc, from, subject,
               stats: st, changes, xlsx_bytes: xlsx.length, filename: xlsxName,
               sftp: SFTP_ENABLED ? csvName : 'disabled', text: body.text };
    }
    if (!to.length || !from) {
      writeSnapshot({ client, at: new Date().toISOString(), for_date: forDate, trigger, sent: 0,
        rows: rows.length, changes: changes.length, email_id: null, sftp_path: null, error: 'no_recipients' }, rows);
      return { sent: false, reason: 'no_recipients', message: 'ELEVATE_TO and a from address are needed. Nothing was sent.' };
    }

    let emailId = null, sftpPath = null, error = null;
    try {
      const r = await sendViaResend({ from, to, cc, subject, html: body.html, text: body.text,
        attachments: [{ filename: xlsxName, content: xlsx.toString('base64') }] });
      emailId = r && r.id || null;
    } catch (e) {
      error = 'email: ' + String(e.message || e);
      log.error('[elevate] email failed:', e);
    }
    if (!error && SFTP_ENABLED && iconicPublisher && typeof iconicPublisher.publish === 'function') {
      try {
        const r = await iconicPublisher.publish({ filename: csvName, csv: csv(rows), weekStart: forDate, userId: o.userId || 'elevate' });
        if (r && r.ok) sftpPath = r.remotePath || csvName;
        else error = 'sftp: ' + ((r && r.error) || 'publish failed');
      } catch (e) {
        error = 'sftp: ' + String(e.message || e);
        log.error('[elevate] sftp failed:', e);
      }
    }
    // The snapshot is "sent" if the email went: that is what the client saw, and what
    // tomorrow's diff must be against. An SFTP failure is recorded, not hidden, and does not
    // make tomorrow re-report today's changes.
    const sent = !!emailId;
    writeSnapshot({ client, at: new Date().toISOString(), for_date: forDate, trigger, sent: sent ? 1 : 0,
      rows: rows.length, changes: changes.length, email_id: emailId, sftp_path: sftpPath, error }, rows);
    if (sent) log.log(`[elevate] ${forDate} sent to ${to.join(', ')} — ${rows.length} rows, ${changes.length} changes${sftpPath ? ', published ' + sftpPath : ''}`);
    return { sent, for_date: forDate, to, cc, subject, stats: st, email_id: emailId, sftp_path: sftpPath, error };
  }

  const alreadySentFor = (client, forDate) => !!db.prepare(
    `SELECT 1 x FROM elevate_snapshot WHERE client_id = ? AND for_date = ? AND sent = 1`).get(client, forDate);

  // ── Routes ──
  const canSend = requireRole ? requireRole(['admin', 'supplier']) : (req, res, next) => next();

  // What the working page shows: the rows as they stand, the changes against the last send,
  // with any notes already written merged in.
  router.get('/preview', authenticateRequest, auditLog('view_elevate'), (req, res) => {
    try {
      const client = curClient();
      const rows = assemble(client);
      const last = lastSent(client);
      const changes = diff(client, rows, last);
      const p = localParts();
      const forDate = p.h >= SEND_HOUR ? p.date : addDays(p.date, -1);
      res.json({ ok: true, rows: rows.map(screenRow), changes, stats: stats(rows, changes),
        last_sent: last ? { at: last.snapshot.taken_at, for_date: last.snapshot.for_date, rows: last.snapshot.row_count,
                            changes: last.snapshot.change_count, sftp: last.snapshot.sftp_path, trigger: last.snapshot.trigger } : null,
        schedule: { zone: TZ, hour: SEND_HOUR, local_now: `${p.date} ${String(p.h).padStart(2, '0')}:00`,
                    due_for: forDate, due_sent: alreadySentFor(client, forDate) },
        sftp_enabled: SFTP_ENABLED, recipients: recipients() });
    } catch (e) {
      log.error('[elevate] preview failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router.post('/note', authenticateRequest, canSend, auditLog('elevate_note'), (req, res) => {
    try {
      const b = req.body || {};
      const rowKey = String(b.row_key || '').trim(), sig = String(b.signature || '').trim();
      const note = String(b.note || '').trim().slice(0, 500);
      if (!rowKey || !sig) return res.status(400).json({ ok: false, error: 'row_key and signature are required' });
      const client = curClient();
      if (!note) db.prepare('DELETE FROM elevate_note WHERE client_id = ? AND row_key = ? AND signature = ?').run(client, rowKey, sig);
      else db.prepare(`INSERT INTO elevate_note (client_id, row_key, signature, note, user_id, created_at) VALUES (?,?,?,?,?,?)
                       ON CONFLICT(client_id, row_key, signature) DO UPDATE SET note = excluded.note, user_id = excluded.user_id, created_at = excluded.created_at`)
             .run(client, rowKey, sig, note, (req.auth && (req.auth.userId || req.auth.sub)) || null, new Date().toISOString());
      res.json({ ok: true });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // One button: email the workbook, publish the CSV, record what was sent. ?dryRun=1 builds
  // everything and sends nothing.
  router.post('/send', authenticateRequest, canSend, auditLog('elevate_send'), async (req, res) => {
    try {
      const dryRun = String(req.query.dryRun || '') === '1';
      const out = await send(curClient(), { dryRun, trigger: 'manual', userId: req.auth && (req.auth.userId || req.auth.sub) });
      res.json({ ok: true, ...out });
    } catch (e) {
      log.error('[elevate] send failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // The scheduled poke. Hourly from a cron with the lane secret; this decides whether to act.
  // Due for day D once the local hour reaches SEND_HOUR; if D was missed, any hour of D+1
  // before SEND_HOUR still sends D's position, labelled D. One send per day, never two.
  router.post('/run', (req, res, next) => {
    const secret = process.env.LANE_CRON_SECRET;
    if (secret && req.headers['x-lane-cron-secret'] === secret) return next();
    return authenticateRequest(req, res, next);
  }, auditLog('run_elevate'), async (req, res) => {
    const cron = !!req.headers['x-lane-cron-secret'];
    const force = String(req.query.force || '') === '1';
    const client = String(req.query.client || '').trim() || (cron ? (process.env.ELEVATE_CLIENT || 'ICONIC') : curClient());
    try {
      if (String(process.env.ELEVATE_AUTO_SEND || '').toLowerCase() !== 'true' && !force) {
        return res.json({ ok: true, skipped: true, reason: 'auto_send_off', message: 'Set ELEVATE_AUTO_SEND=true to send on the schedule.' });
      }
      const p = localParts();
      const forDate = p.h >= SEND_HOUR ? p.date : addDays(p.date, -1);
      if (!force && alreadySentFor(client, forDate)) {
        return res.json({ ok: true, skipped: true, reason: 'already_sent', for_date: forDate });
      }
      const late = p.h < SEND_HOUR;
      const out = await send(client, { trigger: late ? 'cron_late' : 'cron', forDate });
      if (late && out.sent) log.warn(`[elevate] ${forDate} sent late, at ${p.date} ${p.h}:00 local`);
      res.json({ ok: true, late, ...out });
    } catch (e) {
      log.error('[elevate] run failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router.get('/status', authenticateRequest, (req, res) => {
    try {
      const client = curClient();
      const runs = db.prepare(`SELECT id, taken_at, for_date, trigger, sent, row_count, change_count, email_id, sftp_path, error
                                 FROM elevate_snapshot WHERE client_id = ? ORDER BY id DESC LIMIT 30`).all(client);
      const p = localParts();
      res.json({ ok: true, schedule: { zone: TZ, hour: SEND_HOUR, auto: String(process.env.ELEVATE_AUTO_SEND || '').toLowerCase() === 'true',
                 sftp: SFTP_ENABLED, local_now: `${p.date} ${String(p.h).padStart(2, '0')}:00` },
                 recipients: recipients(), grace_days: GRACE_DAYS, from_week: FROM_WEEK, recent: runs });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // The workbook as it stands right now, for anyone who wants it before the send.
  router.get('/tracker.xlsx', authenticateRequest, auditLog('download_elevate'), async (req, res) => {
    try {
      const client = curClient();
      const rows = assemble(client);
      const changes = diff(client, rows, lastSent(client));
      const buf = await workbook(rows, changes, localToday());
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="VOZ_FF_Tracker_${localToday().replace(/-/g, '')}.xlsx"`);
      res.send(buf);
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router._internals = { assemble, diff, stats, workbook, csv, send, COLUMNS, STATUS, localParts };
  log.log(`[elevate] daily client tracker mounted — ${TZ} ${SEND_HOUR}:00, auto ${String(process.env.ELEVATE_AUTO_SEND || 'false')}, sftp ${SFTP_ENABLED}`);
  return router;
};
