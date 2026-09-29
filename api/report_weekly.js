/* ── VelOzity Pinpoint — weekly report workbook v1 ──
   The six-sheet Pinpoint_Report_<week>.xlsx that goes out with the Monday email. Until now
   somebody produced it by hand each week, which is why the email could not be automated.

   It computes nothing new. Every sheet is the same data the Reports screens already show —
   the summary endpoints, the records table, the bins table — assembled into one workbook.
   Anything that disagrees with a screen is a bug here, not a second opinion.

   Mount:
     app.use('/report', require('./report_weekly')({ express, db, authenticateRequest,
       auditLog, curClient, scopeSql, tenantReadIds, ExcelJS,
       getPlanRows, getBins, getPoSku, getSku, getShipmentSummary, getShipmentDetail }));

   The getters are passed in rather than re-queried here, so the workbook and the screens
   cannot drift apart.
*/
'use strict';

module.exports = function mountWeeklyReport(deps) {
  const {
    express, db, authenticateRequest, auditLog,
    curClient, scopeSql, tenantReadIds, ExcelJS,
    getPlanRows, getBins, getPoSku, getSku, getShipmentSummary, getShipmentDetail,
  } = deps;

  const router = express.Router();

  const mondayOf = (ymd) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7));
    return d.toISOString().slice(0, 10);
  };
  const addDays = (ymd, n) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };
  const str = (v) => (v === null || v === undefined) ? '' : String(v);
  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : null; };

  // Every UID applied in the week. Scoped the way /records scopes: the table carries no
  // client_id of its own, so filtering by date alone would pull another client's work in.
  // The hand-made workbook carried a trailing carriage return on every UID — it shows up in
  // the file as "_x000d_" on both the header and the values, and anyone parsing those UIDs
  // downstream gets a token with \r on the end. It comes in with the data, so it is stripped
  // here rather than reproduced.
  const clean = (v) => (v === null || v === undefined) ? '' : String(v).replace(/[\r\n]+/g, '').trim();

  function appliedUids(req, ws) {
    const params = [];
    let sql = `SELECT date_local, mobile_bin, sscc_label, po_number, sku_code, uid
                 FROM records WHERE 1=1`;
    const sc = scopeSql(tenantReadIds(req));
    sql += sc.clause;
    params.push(...sc.params);
    sql += ` AND date_local >= ? AND date_local <= ? AND status = 'complete'
             ORDER BY date_local, mobile_bin, po_number, sku_code`;
    params.push(ws, addDays(ws, 6));
    return db.prepare(sql).all(...params).map(r => ({
      date_local: clean(r.date_local),
      mobile_bin: clean(r.mobile_bin),
      sscc_label: clean(r.sscc_label),
      po_number: clean(r.po_number),
      sku_code: clean(r.sku_code),
      uid: clean(r.uid),
    }));
  }

  // Bins carry weight; the PO they belong to comes from the UIDs inside them.
  function mobileBins(req, ws) {
    const bins = getBins(ws) || [];
    const uids = appliedUids(req, ws);
    const poByBin = new Map(), unitsByBin = new Map();
    for (const r of uids) {
      const b = str(r.mobile_bin).trim(); if (!b) continue;
      if (!poByBin.has(b)) poByBin.set(b, new Set());
      poByBin.get(b).add(str(r.po_number).trim());
      unitsByBin.set(b, (unitsByBin.get(b) || 0) + 1);
    }
    const supplierByPo = new Map();
    for (const p of (getPlanRows(ws) || [])) {
      const po = str(p.po_number).trim();
      if (po && !supplierByPo.has(po)) supplierByPo.set(po, str(p.supplier_name));
    }

    const out = [];
    for (const b of bins) {
      const bin = str(b.mobile_bin).trim();
      const pos = [...(poByBin.get(bin) || [])].filter(Boolean).sort();
      // A bin holding two POs is one physical carton, so it is listed once per PO rather
      // than split — the weight belongs to the carton, not to either PO.
      out.push({
        supplier: pos.length ? (supplierByPo.get(pos[0]) || '') : '',
        po: pos.join(', '),
        mobile_bin: bin,
        weight_kg: num(b.weight_kg),
        units: unitsByBin.get(bin) != null ? unitsByBin.get(bin) : num(b.total_units),
      });
    }
    out.sort((a, b) => (a.supplier || '').localeCompare(b.supplier || '')
      || (a.po || '').localeCompare(b.po || '')
      || (a.mobile_bin || '').localeCompare(b.mobile_bin || ''));
    return out;
  }

  const SHEETS = [
    {
      name: 'Applied UIDs',
      columns: [
        { header: 'Date Applied', key: 'date_local', width: 14 },
        { header: 'Mobile Bin', key: 'mobile_bin', width: 18 },
        { header: 'SSCC Label', key: 'sscc_label', width: 22 },
        { header: 'PO Number', key: 'po_number', width: 16 },
        { header: 'SKU Code', key: 'sku_code', width: 14 },
        { header: 'UID', key: 'uid', width: 42 },
      ],
    },
    {
      name: 'PO x SKU Summary',
      columns: [
        { header: 'PO', key: 'po', width: 16 },
        { header: 'SKU', key: 'sku', width: 14 },
        { header: 'Supplier Name', key: 'supplier_name', width: 34 },
        { header: 'Facility Name', key: 'facility_name', width: 14 },
        { header: 'Freight Type', key: 'freight_type', width: 12 },
        { header: 'Zendesk Ticket #', key: 'zendesk', width: 16 },
        { header: 'Mobile Bin Count', key: 'bin_count', width: 16 },
        { header: 'Mobile Bins', key: 'bins', width: 30 },
        { header: 'Planned', key: 'planned', width: 10 },
        { header: 'Applied', key: 'applied', width: 10 },
        { header: 'Discrepancy (Applied - Planned)', key: 'discrepancy', width: 28 },
      ],
    },
    {
      name: 'SKU Summary',
      columns: [
        { header: 'PO', key: 'po', width: 16 },
        { header: 'Supplier Name', key: 'supplier_name', width: 34 },
        { header: 'Facility Name', key: 'facility_name', width: 14 },
        { header: 'Freight Type', key: 'freight_type', width: 12 },
        { header: 'Zendesk Ticket #', key: 'zendesk', width: 16 },
        { header: 'Planned', key: 'planned', width: 10 },
        { header: 'Applied', key: 'applied', width: 10 },
        { header: 'Discrepancy (Applied - Planned)', key: 'discrepancy', width: 28 },
      ],
    },
    {
      name: 'Mobile Bins',
      columns: [
        { header: 'Supplier', key: 'supplier', width: 34 },
        { header: 'PO', key: 'po', width: 16 },
        { header: 'Mobile_Bin', key: 'mobile_bin', width: 18 },
        { header: 'Weight_kg', key: 'weight_kg', width: 12 },
        { header: 'Units', key: 'units', width: 10 },
      ],
    },
    {
      name: 'Shipment Summary',
      columns: [
        { header: 'Supplier Name', key: 'supplier_name', width: 34 },
        { header: 'Zendesk Ticket #', key: 'zendesk', width: 16 },
        { header: 'Freight Type', key: 'freight_type', width: 12 },
        { header: 'Facility Name', key: 'facility_name', width: 14 },
        { header: 'Unique PO Count', key: 'po_count', width: 16 },
        { header: 'Total Units Applied', key: 'units', width: 18 },
        { header: 'Total Mobile Bins', key: 'bins', width: 17 },
        { header: 'Gross Weight', key: 'weight_kg', width: 14 },
        { header: 'CBM', key: 'cbm', width: 10 },
      ],
    },
    {
      name: 'Shipment Details',
      columns: [
        { header: 'Supplier Name', key: 'supplier_name', width: 34 },
        { header: 'Zendesk Ticket #', key: 'zendesk', width: 16 },
        { header: 'Freight Type', key: 'freight_type', width: 12 },
        { header: 'Facility Name', key: 'facility_name', width: 14 },
        { header: 'PO', key: 'po', width: 16 },
        { header: 'Total Units Applied', key: 'units', width: 18 },
        { header: 'Total Mobile Bins', key: 'bins', width: 17 },
        { header: 'Gross Weight', key: 'weight_kg', width: 14 },
        { header: 'CBM', key: 'cbm', width: 10 },
      ],
    },
  ];

  // Pull each sheet's rows, mapping whatever the summary returns onto the column keys above.
  function collect(req, ws) {
    const pick = (o, names) => {
      for (const n of names) if (o && o[n] !== undefined && o[n] !== null) return o[n];
      return '';
    };

    const poSku = (getPoSku(ws) || []).map(r => ({
      po: pick(r, ['po', 'po_number']),
      sku: pick(r, ['sku', 'sku_code']),
      supplier_name: pick(r, ['supplier_name', 'supplier']),
      facility_name: pick(r, ['facility_name', 'facility']),
      freight_type: pick(r, ['freight_type', 'freight']),
      zendesk: pick(r, ['zendesk_ticket', 'zendesk', 'zendesk_ticket_number']),
      bin_count: num(pick(r, ['mobile_bin_count', 'bin_count'])),
      bins: pick(r, ['mobile_bins', 'bins']),
      planned: num(pick(r, ['planned', 'planned_units'])),
      applied: num(pick(r, ['applied', 'applied_units'])),
      discrepancy: num(pick(r, ['discrepancy'])) != null
        ? num(pick(r, ['discrepancy']))
        : (num(pick(r, ['applied', 'applied_units'])) || 0) - (num(pick(r, ['planned', 'planned_units'])) || 0),
    }));

    const sku = (getSku(ws) || []).map(r => ({
      po: pick(r, ['po', 'po_number']),
      supplier_name: pick(r, ['supplier_name', 'supplier']),
      facility_name: pick(r, ['facility_name', 'facility']),
      freight_type: pick(r, ['freight_type', 'freight']),
      zendesk: pick(r, ['zendesk_ticket', 'zendesk', 'zendesk_ticket_number']),
      planned: num(pick(r, ['planned', 'planned_units'])),
      applied: num(pick(r, ['applied', 'applied_units'])),
      discrepancy: num(pick(r, ['discrepancy'])) != null
        ? num(pick(r, ['discrepancy']))
        : (num(pick(r, ['applied', 'applied_units'])) || 0) - (num(pick(r, ['planned', 'planned_units'])) || 0),
    }));

    const shipSum = (getShipmentSummary(ws) || []).map(r => ({
      supplier_name: pick(r, ['supplier_name', 'supplier']),
      zendesk: pick(r, ['zendesk_ticket', 'zendesk']),
      freight_type: pick(r, ['freight_type', 'freight']),
      facility_name: pick(r, ['facility_name', 'facility']),
      po_count: num(pick(r, ['unique_po_count', 'po_count'])),
      units: num(pick(r, ['total_units_applied', 'units'])),
      bins: num(pick(r, ['total_mobile_bins', 'bins'])),
      weight_kg: num(pick(r, ['gross_weight', 'weight_kg'])),
      cbm: num(pick(r, ['cbm'])),
    }));

    const shipDet = (getShipmentDetail(ws) || []).map(r => ({
      supplier_name: pick(r, ['supplier_name', 'supplier']),
      zendesk: pick(r, ['zendesk_ticket', 'zendesk']),
      freight_type: pick(r, ['freight_type', 'freight']),
      facility_name: pick(r, ['facility_name', 'facility']),
      po: pick(r, ['po', 'po_number']),
      units: num(pick(r, ['total_units_applied', 'units'])),
      bins: num(pick(r, ['total_mobile_bins', 'bins'])),
      weight_kg: num(pick(r, ['gross_weight', 'weight_kg'])),
      cbm: num(pick(r, ['cbm'])),
    }));

    return {
      'Applied UIDs': appliedUids(req, ws),
      'PO x SKU Summary': poSku,
      'SKU Summary': sku,
      'Mobile Bins': mobileBins(req, ws),
      'Shipment Summary': shipSum,
      'Shipment Details': shipDet,
    };
  }

  async function buildWorkbook(req, ws) {
    const data = collect(req, ws);
    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();

    const counts = {};
    for (const def of SHEETS) {
      const sheet = wb.addWorksheet(def.name);
      sheet.columns = def.columns;
      sheet.getRow(1).font = { bold: true };
      sheet.views = [{ state: 'frozen', ySplit: 1 }];
      const rows = data[def.name] || [];
      for (const r of rows) sheet.addRow(r);
      counts[def.name] = rows.length;
    }

    const buffer = await wb.xlsx.writeBuffer();
    return { buffer: Buffer.from(buffer), counts, filename: 'Pinpoint_Report_' + ws + '.xlsx' };
  }

  router.get('/weekly-workbook', authenticateRequest, auditLog('report_weekly_workbook'), async (req, res) => {
    try {
      const ws = mondayOf(String(req.query.week || req.query.week_start || '').trim());
      if (!ws) return res.status(400).json({ error: 'invalid', message: 'week must be YYYY-MM-DD.' });

      const built = await buildWorkbook(req, ws);

      // An empty workbook is worse than an error: it looks like a quiet week rather than a
      // missing plan, and it would go out attached to an email saying nothing is wrong.
      const total = Object.values(built.counts).reduce((a, b) => a + b, 0);
      if (!total) {
        return res.status(409).json({ error: 'no_data',
          message: 'Nothing recorded for week ' + ws + '. The workbook would be empty.',
          counts: built.counts });
      }

      if (String(req.query.format || '').toLowerCase() === 'json') {
        return res.json({ ok: true, week_start: ws, filename: built.filename,
                          bytes: built.buffer.length, counts: built.counts });
      }

      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename="' + built.filename + '"');
      res.send(built.buffer);
    } catch (e) {
      console.error('[report] weekly workbook failed:', e);
      res.status(500).json({ error: 'build_failed', message: String(e.message || e) });
    }
  });

  console.log('[report] weekly workbook v1 mounted');
  router.buildWorkbook = buildWorkbook;      // the Monday job attaches it without an HTTP hop
  return router;
};
