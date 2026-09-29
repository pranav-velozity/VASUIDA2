/* ── VelOzity Pinpoint — weekly job wiring v1 ──
   Hands weekly_report_job.js its real dependencies. The job decides what happens and in what
   order; this decides where each piece comes from.

   Four attachments are built in-process. Two — stock status and supplier discrepancy — are
   fetched from this server's own endpoints over loopback, because their computation lives
   inside the route handlers. Calling them rather than moving ~425 lines of reporting logic
   was the smaller risk, and it means the emailed files are byte-identical to the ones the
   screens produce.
*/
'use strict';

module.exports = function createWiring(deps) {
  const {
    db, ExcelJS, curClient,
    REPORT_BUILDER, APO_BUILDER,
    receivingSummary, discrepancyReport, internal,
    iconicPublisher, logEmail, pulseNarrative, logger,
  } = deps;

  const log = logger || console;

  const addDays = (ws, n) => {
    const d = new Date(ws + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };

  // ── the three inputs the workbook and the APO share ──
  function planFor(ws) {
    const row = db.prepare('SELECT data FROM plans WHERE week_start = ? AND client_id = ?')
      .get(ws, curClient());
    if (!row || !row.data) return [];
    try { return JSON.parse(row.data) || []; }
    catch (e) { throw new Error(`The plan for ${ws} could not be read: ${e.message}`); }
  }

  // records carries a client_id, so the job filters on it directly. The request-based helpers
  // are not usable here: tenantReadIds reads a header off the request object, and the job has
  // no request to read one from.
  function recordsFor(ws) {
    return db.prepare(`
      SELECT date_local, mobile_bin, sscc_label, po_number, sku_code, uid, status
        FROM records
       WHERE client_id = ? AND date_local >= ? AND date_local <= ? AND status = 'complete'
       ORDER BY date_local, mobile_bin, po_number, sku_code
    `).all(curClient(), ws, addDays(ws, 6));
  }

  // bins has no client_id column at all — it is keyed by week and mobile bin, and the server's
  // own _getBinsForWeek reads it by week alone. Filtering on a column that does not exist
  // threw, which is how four sheets ended up empty.
  const binsFor = (ws) => db.prepare(`
    SELECT week_start, mobile_bin, total_units, weight_kg, date_local,
           carton_length_cm, carton_width_cm, carton_height_cm
      FROM bins WHERE week_start = ?`).all(ws);

  async function buildWorkbook(ws) {
    const plan = planFor(ws);
    const records = recordsFor(ws);
    const bins = binsFor(ws);

    // PO x SKU rollup, the same shape /summary/po_sku returns.
    const byPS = new Map();
    for (const r of records) {
      const po = String(r.po_number || '').trim(), sku = String(r.sku_code || '').trim();
      if (!po || !sku) continue;
      const k = po + '|||' + sku;
      byPS.set(k, (byPS.get(k) || 0) + 1);
    }
    const poSkuRows = [...byPS].map(([k, units]) => {
      const [po, sku] = k.split('|||');
      return { po, sku, units };
    });

    const built = REPORT_BUILDER.build({
      weekStart: ws, appliedUids: records, plan, poSkuRows, records, bins,
      // shipmentSummary / shipmentDetail left out on purpose: the builder computes them from
      // plan, records and bins, verified against two published weeks.
    });

    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();
    for (const name of built.sheetNames) {
      const sheet = wb.addWorksheet(name);
      sheet.columns = built.columns[name].map(h => ({ header: h, key: h, width: Math.max(12, h.length + 4) }));
      sheet.getRow(1).font = { bold: true };
      sheet.views = [{ state: 'frozen', ySplit: 1 }];
      for (const r of built.sheets[name]) sheet.addRow(r);
    }
    const buffer = await wb.xlsx.writeBuffer();
    return { buffer: Buffer.from(buffer), filename: built.filename,
             counts: built.counts, missing: built.missing };
  }

  async function buildApo(ws) {
    const plan = planFor(ws);
    const records = recordsFor(ws);

    let settings = {};
    try {
      const row = db.prepare('SELECT data FROM apo_settings WHERE client_id = ?').get(curClient());
      if (row && row.data) settings = JSON.parse(row.data) || {};
    } catch (e) { log.warn('[weekly] apo settings unreadable, using defaults:', e.message); }

    return APO_BUILDER.build({ weekStart: ws, planRows: plan, records, settings });
  }

  // ── the two fetched over loopback ──
  async function buildStockStatus(ws) {
    const buf = await internal.buffer(
      `/report/stock-status?from=${encodeURIComponent(ws)}&to=${encodeURIComponent(ws)}&format=xlsx`);
    return { buffer: buf, filename: `Stock_Status_${ws}.xlsx` };
  }

  const discrepancyPassword = () => process.env.COST_REPORT_PASSWORD || 'velozity2026';

  async function buildDiscrepancyXlsx(ws, opts) {
    const cost = !!(opts && opts.cost);
    const q = `/report/supplier-discrepancy.xlsx?week=${encodeURIComponent(ws)}`
      + (cost ? `&cost=1&password=${encodeURIComponent(discrepancyPassword())}` : '');
    const buf = await internal.buffer(q);
    return { buffer: buf, filename: `Supplier_Discrepancy_${ws}${cost ? '_costed' : ''}.xlsx` };
  }

  // No PDF builder is offered. The report has never had one — the printed copy was always
  // somebody printing an HTML page — and returning the workbook under a PDF label attached
  // the same bytes to the email twice.
  const buildDiscrepancyPdf = null;

  const buildSupplierSummary = (ws) => receivingSummary.buildWorkbook(ws);

  // The consignments for the week, with the dates actually recorded against each lane.
  // This is the section that was typed by hand every Monday — container, what is aboard,
  // when it left and when it is due.
  function freightFor(ws) {
    let rows = [];
    try {
      rows = db.prepare('SELECT facility, data FROM flow_week WHERE week_start = ?').all(ws);
    } catch (_) { return []; }

    // Dates come from the lane actuals, keyed by the lane the container sits on.
    let actuals = new Map();
    try {
      for (const a of db.prepare(`SELECT lane_key, stage, actual_at FROM lane_actual_dates
                                   WHERE week_start = ?`).all(ws)) {
        if (!actuals.has(a.lane_key)) actuals.set(a.lane_key, {});
        actuals.get(a.lane_key)[a.stage] = a.actual_at;
      }
    } catch (_) { /* a week with nothing recorded simply has no dates */ }

    const out = [];
    for (const r of rows) {
      let data;
      try { data = JSON.parse(r.data); } catch (_) { continue; }
      const wc = data && data.intl_weekcontainers;
      const list = Array.isArray(wc) ? wc : (Array.isArray(wc && wc.containers) ? wc.containers : []);
      for (const c of list) {
        const ref = String(c.container_id || c.container || '').trim();
        if (!ref) continue;
        const laneKeys = Array.isArray(c.lane_keys) ? c.lane_keys : [];
        // A container can sit on more than one lane; the earliest departure and the latest
        // arrival describe the consignment as a whole.
        // lane_actual_dates stores a timestamp; the email wants a day. Trimmed here so every
        // consumer gets the same shape rather than each one guessing.
        const dayOf = (v) => v ? String(v).trim().slice(0, 10) : null;
        let etd = null, eta = null;
        for (const k of laneKeys) {
          const a = actuals.get(k) || {};
          const dep = dayOf(a.departed), arr = dayOf(a.arrived);
          if (dep && (!etd || dep < etd)) etd = dep;
          if (arr && (!eta || arr > eta)) eta = arr;
        }
        const pos = String(c.pos || '').split(',').map(x => x.trim()).filter(Boolean);
        const air = /air|awb/i.test(ref) || String(c.size_ft || '').toLowerCase() === 'air';
        // An air consignment has no box size, so it is described by its flight rather than
        // labelled "AIRft", which is what happens when a size field is used for both.
        const detail = air
          ? (c.vessel ? String(c.vessel) : 'Air freight')
          : String(c.size_ft || '40') + 'ft' + (c.vessel ? ' · ' + c.vessel : '');
        out.push({ reference: ref, mode: air ? 'Air' : 'Sea', detail, poCount: pos.length, etd, eta });
      }
    }
    out.sort((a, b) => String(a.reference).localeCompare(String(b.reference)));
    return out;
  }

  // ── the numbers the summary is written from ──
  async function weekFigures(ws) {
    const plan = planFor(ws);
    const records = recordsFor(ws);
    const bins = binsFor(ws);

    const plannedUnits = plan.reduce((n, p) => n + (Number(p.target_qty) || 0), 0);
    const recv = db.prepare(`
      SELECT po_number, received_at_utc, cartons_received
        FROM receiving WHERE week_start = ? AND client_id = ?`).all(ws, curClient());

    const cutoff = new Date(`${ws}T04:00:00.000Z`);      // Monday 12:00 Asia/Shanghai
    let posLate = 0, posReceived = 0;
    for (const r of recv) {
      if (!r.received_at_utc) continue;
      posReceived++;
      if (new Date(r.received_at_utc).getTime() > cutoff.getTime()) posLate++;
    }

    let discrepancySuppliers = 0;
    try {
      const row = db.prepare(`SELECT COUNT(DISTINCT supplier) n FROM nc_incident
                               WHERE week_start = ? AND client_id = ?`).get(ws, curClient());
      discrepancySuppliers = (row && row.n) || 0;
    } catch (_) { /* the table may not exist for every client */ }

    const freight = freightFor(ws);

    return {
      plannedUnits,
      appliedUnits: records.length,
      posReceived,
      posLate,
      bins: bins.length,
      discrepancySuppliers,
      containersInTransit: freight.length,
      freight,
    };
  }

  // ── mail ──
  async function sendMail(m) {
    const key = process.env.RESEND_API_KEY;
    if (!key) throw new Error('RESEND_API_KEY is not set — the weekly email cannot be sent.');
    const from = process.env.EMAIL_FROM || 'Pinpoint <reports@velozity.au>';
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from, to: m.to, cc: m.cc && m.cc.length ? m.cc : undefined,
        subject: m.subject, html: m.html, text: m.text,
        attachments: m.attachments,
      }),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Resend refused the message (${res.status}): ${body.slice(0, 300)}`);
    }
    return res.json();
  }

  // The row-count history the validation gate compares against.
  async function recentApoRowCounts(ws, n) {
    const out = [];
    for (let i = 1; i <= (n || 4); i++) {
      const w = addDays(ws, -7 * i);
      try {
        const built = await buildApo(w);
        out.push(built.rows.length);
      } catch (_) { /* a week with no plan simply contributes nothing */ }
    }
    return out;
  }

  // ── The Pulse summary ──
  // Deliberately NOT routed through /pulse/context. That path reads `records` without a
  // client filter, so a narrative built from it could describe another client's units in
  // ICONIC's email. This gets only the figures already computed for this week — the ones
  // printed in the table beneath it — so the sentences cannot contradict the numbers or
  // reach data they should not.
  async function pulseNarrativeFor(figures) {
    const key = process.env.ANTHROPIC_API_KEY;
    if (!key) { log.warn('[weekly] ANTHROPIC_API_KEY not set — using the plain summary'); return null; }

    const facts = {
      planned_units: figures.plannedUnits,
      applied_units: figures.appliedUnits,
      pos_received: figures.posReceived,
      pos_late: figures.posLate,
      mobile_bins: figures.bins,
      suppliers_with_discrepancies: figures.discrepancySuppliers,
      consignments_in_transit: figures.containersInTransit,
      freight: (figures.freight || []).map(f => ({
        reference: f.reference, mode: f.mode, pos: f.poCount, etd: f.etd, eta: f.eta })),
    };

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: process.env.WEEKLY_REPORT_MODEL || 'claude-sonnet-4-6',
        max_tokens: 300,
        system: [
          'You write the opening summary of a weekly logistics report sent to a retail client.',
          'Use ONLY the figures provided. Never invent a number, a supplier, a container or a cause.',
          'Two or three sentences. Plain, factual, no greeting, no sign-off, no bullet points.',
          'Lead with what happened to the week\u2019s volume, then whatever most needs attention.',
          'Do not speculate about why something happened \u2014 the data does not say why.',
          'Do not recommend actions. Do not use the words "I" or "we".',
        ].join(' '),
        messages: [{ role: 'user', content: 'Week figures:\n' + JSON.stringify(facts, null, 2) }],
      }),
    });

    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Claude refused the request (${res.status}): ${body.slice(0, 200)}`);
    }
    const data = await res.json();
    const text = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join(' ').trim();
    return text || null;
  }

  // The last week already sent, so a restart cannot send it twice.
  function lastSentWeek() {
    try {
      const row = db.prepare(`SELECT week_start FROM email_send_log
                               WHERE kind = 'weekly_general' AND status = 'success'
                               ORDER BY sent_at DESC LIMIT 1`).get();
      return (row && row.week_start) || null;
    } catch (_) { return null; }
  }

  return {
    pulseNarrative: pulseNarrativeFor,
    lastSentWeek,
    buildWorkbook, buildApo, buildStockStatus, buildSupplierSummary,
    buildDiscrepancyXlsx,
    weekFigures, sendMail, recentApoRowCounts,
    publishToIconic: (x) => iconicPublisher.publish(x),
    logEmail,
    now: () => new Date(),
    logger: log,
  };
};
