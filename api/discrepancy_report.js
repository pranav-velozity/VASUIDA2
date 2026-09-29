/* ── VelOzity Pinpoint — supplier discrepancy workbook v1 ──
   The non-compliance report as an xlsx, so it can be attached to the Monday email.

   /report/supplier-discrepancy builds an HTML page meant to be read on screen or printed —
   a server cannot attach it without a headless browser, and a printed page is not much use
   to whoever has to chase the supplier. This queries the same tables and writes a workbook
   the client can filter and total.

   Costing is the same as the HTML page:
     · only chargeable incidents carry a cost
     · rate comes from the client rate card via remedy_type, falling back to the built-in
       rate when the card has no entry
     · cost = rate x qty, rounded to the cent
   The rate lookup is passed in rather than reimplemented, so the two cannot diverge.

   Mount:
     app.use('/report', require('./discrepancy_report')({ express, db, authenticateRequest,
       auditLog, curClient, ExcelJS, ncIncidentCost, ncRemedyRate, NC_REMEDY }));
*/
'use strict';

module.exports = function mountDiscrepancyReport(deps) {
  const {
    express, db, authenticateRequest, auditLog, curClient, ExcelJS,
    ncIncidentCost, ncRemedyRate, NC_REMEDY,
  } = deps;

  const router = express.Router();

  const mondayOf = (ymd) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7));
    return d.toISOString().slice(0, 10);
  };
  const str = (v) => (v === null || v === undefined) ? '' : String(v);
  const num = (v) => Number(v) || 0;
  const money = (v) => v == null ? null : Math.round(Number(v) * 100) / 100;

  function collect(ws, includeCost) {
    const client = curClient();

    const cats = new Map(
      db.prepare('SELECT id, label, grp, grain FROM nc_category').all().map(c => [c.id, c])
    );

    const incidents = db.prepare(`
      SELECT * FROM nc_incident
       WHERE week_start = ? AND client_id = ?
       ORDER BY supplier, po_number, created_at
    `).all(ws, client);

    const remedyLabel = (r) => (NC_REMEDY[r] || {}).label || str(r) || '—';
    const unitOf = (r) => (NC_REMEDY[r] || {}).unit || '';

    const detail = incidents.map(i => {
      const cat = cats.get(i.category_id);
      const cost = includeCost ? ncIncidentCost(client, i) : null;
      const rate = includeCost && i.chargeable ? ncRemedyRate(client, i.remedy_type) : null;
      const row = {
        'Supplier': str(i.supplier),
        'Facility': str(i.facility),
        'PO': str(i.po_number),
        'SKU': str(i.sku),
        'Category': cat ? str(cat.label) : str(i.category_id),
        'Group': cat ? str(cat.grp) : '',
        'Grain': str(i.grain),
        'Qty': num(i.qty),
        'Remedy': remedyLabel(i.remedy_type),
        'Chargeable': i.chargeable ? 'Yes' : 'No',
        'Status': str(i.status),
        'Corrective Action': str(i.corrective_action),
        'Raised': str(i.created_at).slice(0, 10),
      };
      if (includeCost) {
        row['Rate (USD)'] = rate == null ? '' : money(rate);
        row['Unit'] = unitOf(i.remedy_type);
        // A chargeable incident whose remedy has no rate shows blank rather than zero:
        // zero would read as "no charge" when it means "not priced yet".
        row['Cost (USD)'] = cost == null ? '' : money(cost);
      }
      return row;
    });

    // One row per supplier, which is what anyone reading this actually acts on.
    const bySupplier = new Map();
    for (const i of incidents) {
      const k = str(i.supplier) || '(not stated)';
      if (!bySupplier.has(k)) {
        bySupplier.set(k, { supplier: k, incidents: 0, pos: new Set(), qty: 0,
                            open: 0, resolved: 0, chargeable: 0, cost: 0, unpriced: 0 });
      }
      const g = bySupplier.get(k);
      g.incidents++;
      g.pos.add(str(i.po_number));
      g.qty += num(i.qty);
      if (str(i.status) === 'resolved') g.resolved++; else g.open++;
      if (i.chargeable) {
        g.chargeable++;
        const c = includeCost ? ncIncidentCost(client, i) : null;
        if (c == null) g.unpriced++; else g.cost += c;
      }
    }

    const summary = [...bySupplier.values()]
      .sort((a, b) => (b.cost - a.cost) || a.supplier.localeCompare(b.supplier))
      .map(g => {
        const row = {
          'Supplier': g.supplier,
          'Incidents': g.incidents,
          'POs Affected': g.pos.size,
          'Total Qty': g.qty,
          'Open': g.open,
          'Resolved': g.resolved,
          'Chargeable': g.chargeable,
        };
        if (includeCost) {
          row['Cost (USD)'] = money(g.cost);
          // Named rather than folded into the total, so a low figure is not mistaken for a
          // good week when it is really an unpriced remedy.
          row['Unpriced Chargeable'] = g.unpriced;
        }
        return row;
      });

    let totals = null;
    if (includeCost && summary.length) {
      const cost = summary.reduce((n, r) => n + num(r['Cost (USD)']), 0);
      totals = {
        'Supplier': 'TOTAL',
        'Incidents': summary.reduce((n, r) => n + r['Incidents'], 0),
        'POs Affected': '',
        'Total Qty': summary.reduce((n, r) => n + r['Total Qty'], 0),
        'Open': summary.reduce((n, r) => n + r['Open'], 0),
        'Resolved': summary.reduce((n, r) => n + r['Resolved'], 0),
        'Chargeable': summary.reduce((n, r) => n + r['Chargeable'], 0),
        'Cost (USD)': money(cost),
        'Unpriced Chargeable': summary.reduce((n, r) => n + r['Unpriced Chargeable'], 0),
      };
    }

    return { summary, detail, totals, incidents: incidents.length };
  }

  async function buildWorkbook(ws, options) {
    const includeCost = !!(options && options.cost);
    const data = collect(ws, includeCost);

    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();

    const sheets = [
      { name: 'Supplier Summary', rows: data.summary.concat(data.totals ? [data.totals] : []) },
      { name: 'Incident Detail', rows: data.detail },
    ];

    const counts = {};
    for (const def of sheets) {
      const sheet = wb.addWorksheet(def.name);
      const headers = def.rows.length ? Object.keys(def.rows[0]) : [];
      if (headers.length) {
        sheet.columns = headers.map(h => ({
          header: h, key: h,
          width: h === 'Supplier' ? 34 : (h === 'Corrective Action' ? 40 : (h.length + 6)),
        }));
        sheet.getRow(1).font = { bold: true };
        sheet.views = [{ state: 'frozen', ySplit: 1 }];
        for (const r of def.rows) sheet.addRow(r);
        if (def.name === 'Supplier Summary' && data.totals) {
          sheet.getRow(def.rows.length + 1).font = { bold: true };
        }
      } else {
        sheet.addRow(['No discrepancies recorded for this week.']);
      }
      counts[def.name] = def.rows.length;
    }

    const buffer = await wb.xlsx.writeBuffer();
    return {
      buffer: Buffer.from(buffer),
      filename: `Supplier_Discrepancy_${ws}${includeCost ? '_costed' : ''}.xlsx`,
      counts, incidents: data.incidents, cost: includeCost,
    };
  }

  router.get('/supplier-discrepancy.xlsx', authenticateRequest,
    auditLog('report_supplier_discrepancy_xlsx'), async (req, res) => {
    try {
      const ws = mondayOf(String(req.query.week || '').trim());
      if (!ws) return res.status(400).json({ error: 'invalid', message: 'week must be YYYY-MM-DD.' });

      // Cost is opt-in on the query and gated by the same password as the HTML report, so the
      // two cannot disagree about who may see charges.
      let cost = false;
      if (String(req.query.cost || '') === '1') {
        const expected = process.env.COST_REPORT_PASSWORD || 'velozity2026';
        if (String(req.query.password || '') !== expected) {
          return res.status(403).json({ error: 'forbidden', message: 'Cost requires the report password.' });
        }
        cost = true;
      }

      const built = await buildWorkbook(ws, { cost });
      if (!built.incidents) {
        return res.status(409).json({ error: 'no_data',
          message: 'No discrepancies recorded for week ' + ws + '.' });
      }

      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename="' + built.filename + '"');
      res.send(built.buffer);
    } catch (e) {
      console.error('[report] supplier discrepancy xlsx failed:', e);
      res.status(500).json({ error: 'build_failed', message: String(e.message || e) });
    }
  });

  console.log('[report] supplier discrepancy workbook v1 mounted');
  router.buildWorkbook = buildWorkbook;
  return router;
};
