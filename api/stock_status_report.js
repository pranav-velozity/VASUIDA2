/* ── VelOzity Pinpoint — Stock Status workbook v1 ──
   Turns the rows /report/stock-status already returns into the xlsx that goes out with the
   Monday email.

   This one needed no extraction. The endpoint computes every field; the browser was only
   renaming keys and writing a sheet. That mapping lives here now, so the file the server
   produces and the file someone downloads have the same columns in the same order.

   Used two ways:
     · the route adds ?format=xlsx and calls writeWorkbook(rows) with what it just computed
     · the weekly job calls the route's builder directly, with no HTTP hop

   Exports as CommonJS (server) and window.STOCK_STATUS (browser).
*/
;(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.STOCK_STATUS = api;
})(typeof window !== 'undefined' ? window : null, function () {
  'use strict';

  // Column order is the contract: people read this file weekly and filter on position.
  const COLUMNS = [
    'Supplier', 'Zendesk', 'PO', 'SKU', 'Freight', 'Facility', 'Due Date',
    'Planned Units', 'Actual Received', 'Applied Units',
    'Received Date', 'Departed Date', 'ETA FC', 'Delivered Date', 'Status',
  ];

  const WIDTHS = [18, 12, 16, 14, 8, 14, 12, 14, 16, 14, 14, 14, 12, 14, 12];

  // Timestamps are shown as dates: the time of day is noise in a stock report and it made
  // the columns three times wider than they needed to be.
  const day = (v) => (v ? String(v).slice(0, 10) : '');
  const str = (v) => (v === null || v === undefined) ? '' : String(v);
  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

  function toRows(rows) {
    return (rows || []).map(r => ({
      'Supplier': str(r.supplier),
      'Zendesk': str(r.zendesk),
      'PO': str(r.po),
      'SKU': str(r.sku),
      'Freight': str(r.freight),
      'Facility': str(r.facility),
      'Due Date': str(r.due_date),
      'Planned Units': num(r.planned),
      'Actual Received': num(r.actual_received),
      'Applied Units': num(r.applied),
      'Received Date': day(r.received_date),
      'Departed Date': day(r.departed_date),
      'ETA FC': day(r.eta_fc),
      'Delivered Date': day(r.delivered_date),
      'Status': str(r.status),
    }));
  }

  const filenameFor = (from, to) =>
    'Stock_Status_' + from + (to && to !== from ? '_to_' + to : '') + '.xlsx';

  /**
   * Server-side writer. ExcelJS is passed in rather than required, so this file stays
   * loadable in the browser where it is only used for toRows and the column list.
   */
  async function writeWorkbook(ExcelJS, rows, from, to) {
    const wb = new ExcelJS.Workbook();
    wb.creator = 'VelOzity Pinpoint';
    wb.created = new Date();

    const sheet = wb.addWorksheet('Stock Status');
    sheet.columns = COLUMNS.map((h, i) => ({ header: h, key: h, width: WIDTHS[i] || 14 }));
    sheet.getRow(1).font = { bold: true };
    sheet.views = [{ state: 'frozen', ySplit: 1 }];

    const mapped = toRows(rows);
    for (const r of mapped) sheet.addRow(r);

    const buffer = await wb.xlsx.writeBuffer();
    return {
      buffer: Buffer.from(buffer),
      filename: filenameFor(from, to || from),
      rows: mapped.length,
    };
  }

  return { COLUMNS, WIDTHS, toRows, writeWorkbook, filenameFor };
});
