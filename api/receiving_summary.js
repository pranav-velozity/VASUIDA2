/* ── VelOzity Pinpoint — receiving supplier summary v1 ──
   The attachment that has always been a browser print-to-PDF, produced here as a workbook.

   The existing "Supplier PDF" button builds HTML, opens a window and calls window.print(),
   so a server cannot reproduce it without a headless browser. It turns out not to need one:
   everything on those pages already lives in the `receiving` table, and the charges page is
   arithmetic. This queries the data directly rather than rendering a document.

   The rules are copied from receiving_live_additive.js so the numbers agree with the screen:
     · cutoff  — Monday 12:00 Asia/Shanghai, which is 04:00 UTC the same date
     · status  — received at or before the cutoff is On-time, after it is Delayed
     · charges — replaced cartons x $1.10 USD, GST 10%, zero-replaced suppliers omitted

   Mount:
     app.use('/report', require('./receiving_summary')({ express, db, authenticateRequest,
       auditLog, curClient, ExcelJS }));
*/
'use strict';

module.exports = function mountReceivingSummary(deps) {
  const { express, db, authenticateRequest, auditLog, curClient, ExcelJS } = deps;
  const router = express.Router();

  const RATE_USD = 1.10;          // per replaced carton
  const GST_RATE = 0.10;
  const BUSINESS_UTC_OFFSET_HOURS = 8;   // Asia/Shanghai

  const mondayOf = (ymd) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7));
    return d.toISOString().slice(0, 10);
  };

  // Monday 12:00 in the business zone. The screen computes the same instant; keeping the
  // derivation rather than a hardcoded '04:00' means a zone change moves both together.
  const cutoffUtcISO = (ws) => {
    const hourUtc = 12 - BUSINESS_UTC_OFFSET_HOURS;
    return `${ws}T${String(hourUtc).padStart(2, '0')}:00:00.000Z`;
  };

  const num = (v) => Number(v) || 0;
  const str = (v) => (v === null || v === undefined) ? '' : String(v);

  function statusOf(receivedAtUtc, cutoff, now) {
    if (!receivedAtUtc) {
      // Nothing received yet: whether that is a problem depends on the cutoff having passed.
      return now.getTime() > cutoff.getTime() ? 'Not received (Delayed)' : 'Not received';
    }
    const d = new Date(receivedAtUtc);
    if (isNaN(d.getTime())) return '';
    return d.getTime() <= cutoff.getTime() ? 'On-time' : 'Delayed';
  }

  function collect(ws) {
    const client = curClient();
    // receiving is keyed by week and carries client_id, so it scopes cleanly.
    const rows = db.prepare(`
      SELECT po_number, supplier_name, facility_name, received_at_utc, received_at_local,
             received_tz, cartons_received, cartons_damaged, cartons_noncompliant,
             cartons_replaced
        FROM receiving
       WHERE week_start = ? AND client_id = ?
       ORDER BY supplier_name, po_number
    `).all(ws, client);

    const cutoff = new Date(cutoffUtcISO(ws));
    const now = new Date();

    const detail = rows.map(r => ({
      'Supplier': str(r.supplier_name),
      'Facility': str(r.facility_name),
      'PO': str(r.po_number),
      'Cartons In': num(r.cartons_received),
      'Damaged': num(r.cartons_damaged),
      'Non-compliant': num(r.cartons_noncompliant),
      'Replaced': num(r.cartons_replaced),
      'Received At (local)': str(r.received_at_local),
      'Received TZ': str(r.received_tz),
      'Status': statusOf(r.received_at_utc, cutoff, now),
    }));

    // One row per supplier, the same rollup the per-supplier pages show.
    const bySupplier = new Map();
    for (const d of detail) {
      const k = d['Supplier'] || '(not stated)';
      if (!bySupplier.has(k)) {
        bySupplier.set(k, { supplier: k, pos: 0, cartons: 0, damaged: 0, nc: 0,
                            replaced: 0, onTime: 0, delayed: 0, notReceived: 0 });
      }
      const g = bySupplier.get(k);
      g.pos++;
      g.cartons += d['Cartons In'];
      g.damaged += d['Damaged'];
      g.nc += d['Non-compliant'];
      g.replaced += d['Replaced'];
      if (d['Status'] === 'On-time') g.onTime++;
      else if (d['Status'] === 'Delayed') g.delayed++;
      else g.notReceived++;
    }

    const summary = [...bySupplier.values()]
      .sort((a, b) => a.supplier.localeCompare(b.supplier))
      .map(g => ({
        'Supplier': g.supplier,
        'POs': g.pos,
        'Cartons In': g.cartons,
        'Damaged': g.damaged,
        'Non-compliant': g.nc,
        'Replaced': g.replaced,
        'On-time': g.onTime,
        'Delayed': g.delayed,
        'Not received': g.notReceived,
        // Of the POs that actually arrived, not of every PO — otherwise a week with
        // deliveries still outstanding reads as poor performance rather than incomplete.
        'On-time %': (g.onTime + g.delayed) ? Math.round(g.onTime / (g.onTime + g.delayed) * 100) : null,
      }));

    // Charges. Suppliers with nothing replaced are omitted, as on the printed page.
    const charges = [];
    let tReplaced = 0, tSub = 0, tGst = 0, tTot = 0;
    for (const g of [...bySupplier.values()].sort((a, b) => a.supplier.localeCompare(b.supplier))) {
      if (g.replaced <= 0) continue;
      const subtotal = g.replaced * RATE_USD;
      const gst = subtotal * GST_RATE;
      const total = subtotal + gst;
      tReplaced += g.replaced; tSub += subtotal; tGst += gst; tTot += total;
      charges.push({
        'Supplier': g.supplier,
        'Replaced Cartons': g.replaced,
        'Rate (USD)': RATE_USD,
        'Subtotal (USD)': Math.round(subtotal * 100) / 100,
        'GST (USD)': Math.round(gst * 100) / 100,
        'Total (USD)': Math.round(total * 100) / 100,
      });
    }
    if (charges.length) {
      charges.push({
        'Supplier': 'TOTAL',
        'Replaced Cartons': tReplaced,
        'Rate (USD)': '',
        'Subtotal (USD)': Math.round(tSub * 100) / 100,
        'GST (USD)': Math.round(tGst * 100) / 100,
        'Total (USD)': Math.round(tTot * 100) / 100,
      });
    }

    return { summary, detail, charges, cutoff: cutoffUtcISO(ws), rows: rows.length };
  }

  const SHEETS = [
    { name: 'Supplier Summary', key: 'summary', widths: [34, 8, 12, 10, 15, 10, 10, 10, 13, 11] },
    { name: 'PO Detail', key: 'detail', widths: [34, 12, 16, 11, 10, 15, 10, 22, 12, 20] },
    { name: 'Carton Replacement Charges', key: 'charges', widths: [34, 18, 12, 16, 13, 14] },
  ];

  async function buildWorkbook(ws) {
    const data = collect(ws);
    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();

    const counts = {};
    for (const def of SHEETS) {
      const rows = data[def.key] || [];
      const sheet = wb.addWorksheet(def.name);
      const headers = rows.length ? Object.keys(rows[0]) : [];
      if (headers.length) {
        sheet.columns = headers.map((h, i) => ({ header: h, key: h, width: def.widths[i] || 16 }));
        sheet.getRow(1).font = { bold: true };
        sheet.views = [{ state: 'frozen', ySplit: 1 }];
        for (const r of rows) sheet.addRow(r);
        // The total line reads as a total rather than as another supplier.
        if (def.key === 'charges' && rows.length) sheet.getRow(rows.length + 1).font = { bold: true };
      } else {
        sheet.addRow([def.key === 'charges'
          ? 'No cartons were replaced this week.'
          : 'Nothing recorded for this week.']);
      }
      counts[def.name] = rows.length;
    }

    const buffer = await wb.xlsx.writeBuffer();
    return {
      buffer: Buffer.from(buffer),
      filename: `Receiving_Supplier_Summary_${ws}.xlsx`,
      counts, cutoff: data.cutoff, poRows: data.rows,
    };
  }

  router.get('/receiving-supplier-summary', authenticateRequest,
    auditLog('report_receiving_supplier_summary'), async (req, res) => {
    try {
      const ws = mondayOf(String(req.query.week || req.query.week_start || '').trim());
      if (!ws) return res.status(400).json({ error: 'invalid', message: 'week must be YYYY-MM-DD.' });

      const built = await buildWorkbook(ws);
      if (!built.poRows) {
        return res.status(409).json({ error: 'no_data',
          message: 'No receiving recorded for week ' + ws + '.' });
      }

      if (String(req.query.format || '').toLowerCase() === 'json') {
        return res.json({ ok: true, week_start: ws, filename: built.filename,
                          bytes: built.buffer.length, counts: built.counts, cutoff: built.cutoff });
      }

      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename="' + built.filename + '"');
      res.send(built.buffer);
    } catch (e) {
      console.error('[report] receiving supplier summary failed:', e);
      res.status(500).json({ error: 'build_failed', message: String(e.message || e) });
    }
  });

  console.log('[report] receiving supplier summary v1 mounted');
  router.buildWorkbook = buildWorkbook;     // the weekly job attaches it without an HTTP hop
  return router;
};
