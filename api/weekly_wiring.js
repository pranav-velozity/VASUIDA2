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

  // The discrepancy report has no PDF generator — the printed version has always been the
  // browser printing an HTML page. Rather than add a headless renderer, the same workbook
  // goes out once; the job's attachment list names it plainly.
  const buildDiscrepancyPdf = async (ws, opts) => buildDiscrepancyXlsx(ws, opts);

  const buildSupplierSummary = (ws) => receivingSummary.buildWorkbook(ws);

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

    // Consignments in flight, and their dates, for the freight block.
    let freight = [];
    try {
      freight = db.prepare(`
        SELECT reference, mode, service, carrier, plan_departed, plan_arrived
          FROM d2d_shipment WHERE client_id = ? AND status NOT IN ('delivered','cancelled')
          ORDER BY plan_arrived LIMIT 20`).all(curClient())
        .map(s => ({ reference: s.reference || 'not advised', mode: s.mode === 'air' ? 'Air' : 'Sea',
                     supplier: s.service || s.carrier || '', zendesk: '',
                     etd: s.plan_departed, eta: s.plan_arrived }));
    } catch (_) { /* clients without door-to-door have no such table */ }

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

  return {
    buildWorkbook, buildApo, buildStockStatus, buildSupplierSummary,
    buildDiscrepancyPdf, buildDiscrepancyXlsx,
    weekFigures, sendMail, recentApoRowCounts,
    publishToIconic: (x) => iconicPublisher.publish(x),
    logEmail, pulseNarrative,
    now: () => new Date(),
    logger: log,
  };
};
