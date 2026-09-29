/* ── VelOzity Pinpoint — weekly report workbook builder (shared) v1 ──
   The six sheets of Pinpoint_Report_<week>.xlsx, in one place.

   The logic lived in index.html, which is why the server could not produce the workbook and
   the Monday email had to be assembled by hand. This is the same move we made for the
   Advanced PO: one implementation, two callers — the Reports screen and the weekly job — so
   the file someone downloads and the file that goes out are the same rows by construction.

   It takes data in and returns rows out. No DOM, no fetch, no database: the caller supplies
   the six inputs, whichever way it obtains them.

   One deliberate change from the browser version. There, every sheet sat in a try/catch that
   logged a warning and carried on, so a failed fetch produced a workbook silently missing a
   sheet. A person clicking Download would probably notice. An unattended Monday email would
   not, and the recipients would read a short file as a quiet week. Here, a sheet that cannot
   be built is reported in `missing`, and the caller decides — the job refuses to send.

   Exports as CommonJS (server) and window.REPORT_BUILDER (browser). */
;(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.REPORT_BUILDER = api;
})(typeof window !== 'undefined' ? window : null, function () {
  'use strict';

  const str = (v) => (v === null || v === undefined) ? '' : String(v);
  const trim = (v) => str(v).trim();
  const toNum = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

  // Applied UIDs arrive with a trailing carriage return in the source data. The hand-made
  // workbook carried it through — every UID ends "_x000d_" in the file — so anything parsing
  // those tokens downstream gets a stray \r. Stripped here rather than reproduced.
  const clean = (v) => str(v).replace(/[\r\n]+/g, '').trim();

  // Sheet order and column order are the contract: people read this file every week and
  // scripts have been written against it.
  const SHEET_NAMES = [
    'Applied UIDs', 'PO x SKU Summary', 'SKU Summary',
    'Mobile Bins', 'Shipment Summary', 'Shipment Details',
  ];

  const COLUMNS = {
    'Applied UIDs': ['Date Applied', 'Mobile Bin', 'SSCC Label', 'PO Number', 'SKU Code', 'UID'],
    'PO x SKU Summary': ['PO', 'SKU', 'Supplier Name', 'Facility Name', 'Freight Type',
      'Zendesk Ticket #', 'Mobile Bin Count', 'Mobile Bins', 'Planned', 'Applied',
      'Discrepancy (Applied - Planned)'],
    'SKU Summary': ['PO', 'Supplier Name', 'Facility Name', 'Freight Type', 'Zendesk Ticket #',
      'Planned', 'Applied', 'Discrepancy (Applied - Planned)'],
    'Mobile Bins': ['Supplier', 'PO', 'Mobile_Bin', 'Weight_kg', 'Units'],
    'Shipment Summary': ['Supplier Name', 'Zendesk Ticket #', 'Freight Type', 'Facility Name',
      'Unique PO Count', 'Total Units Applied', 'Total Mobile Bins', 'Gross Weight', 'CBM'],
    'Shipment Details': ['Supplier Name', 'Zendesk Ticket #', 'Freight Type', 'Facility Name',
      'PO', 'Total Units Applied', 'Total Mobile Bins', 'Gross Weight', 'CBM'],
  };

  // ── Sheet 1 ──
  function appliedUidRows(records) {
    return (records || []).map(r => ({
      'Date Applied': clean(r.date_local ?? r['Date Applied']),
      'Mobile Bin': clean(r.mobile_bin ?? r['Mobile Bin']),
      'SSCC Label': clean(r.sscc_label ?? r['SSCC Label']),
      'PO Number': clean(r.po_number ?? r['PO Number']),
      'SKU Code': clean(r.sku_code ?? r['SKU Code']),
      'UID': clean(r.uid ?? r['UID']),
    }));
  }

  // ── Sheet 2 ──
  // Accepts either the summary shape { po, sku, units } or raw records, because the screen
  // has historically passed both.
  function poSkuRows(plan, recRows) {
    const byPS = new Map(), binsByPS = new Map();

    for (const r of recRows || []) {
      if (!r) continue;
      if (('po' in r) && ('sku' in r) && ('units' in r) && !('po_number' in r)) {
        const po = trim(r.po), sku = trim(r.sku);
        if (!po || !sku) continue;
        const k = po + '|||' + sku;
        byPS.set(k, (byPS.get(k) || 0) + toNum(r.units));
        continue;
      }
      if (r.status !== 'complete') continue;
      const po = trim(r.po_number), sku = trim(r.sku_code);
      if (!po || !sku) continue;
      const k = po + '|||' + sku;
      byPS.set(k, (byPS.get(k) || 0) + 1);
      const mb = trim(r.mobile_bin || r.bin);
      if (mb) {
        if (!binsByPS.has(k)) binsByPS.set(k, new Set());
        binsByPS.get(k).add(mb);
      }
    }

    const seen = new Set(), out = [];
    for (const p of plan || []) {
      const po = trim(p.po_number), sku = trim(p.sku_code);
      if (!po || !sku) continue;
      const k = po + '|||' + sku;
      if (seen.has(k)) continue;
      seen.add(k);

      const planned = toNum(p.target_qty);
      const applied = byPS.get(k) || 0;
      const binList = Array.from(binsByPS.get(k) || new Set()).sort();

      out.push({
        'PO': po,
        'SKU': sku,
        'Supplier Name': str(p.supplier_name),
        'Facility Name': str(p.facility_name),
        'Freight Type': str(p.freight_type),
        'Zendesk Ticket #': str(p.zendesk_ticket),
        'Mobile Bin Count': binList.length,
        'Mobile Bins': binList.join('; '),
        'Planned': planned,
        'Applied': applied,
        'Discrepancy (Applied - Planned)': applied - planned,
      });
    }
    return out;
  }

  // Units by PO, rolled up from the same rows sheet 2 uses.
  function unitsByPo(recRows) {
    const byPO = new Map();
    for (const r of recRows || []) {
      if (!r) continue;
      if (('po' in r) && ('units' in r) && !('po_number' in r)) {
        const po = trim(r.po); if (!po) continue;
        byPO.set(po, (byPO.get(po) || 0) + toNum(r.units));
        continue;
      }
      if (r.status !== 'complete') continue;
      const po = trim(r.po_number); if (!po) continue;
      byPO.set(po, (byPO.get(po) || 0) + 1);
    }
    return byPO;
  }

  // PO-level planned vs applied, ordered by due date, as the screen does it.
  function joinPoProgress(plan, byPO) {
    const map = new Map();
    for (const p of plan || []) {
      const po = trim(p.po_number); if (!po) continue;
      const rec = map.get(po) || { due: p.due_date, planned: 0, applied: 0 };
      rec.planned += toNum(p.target_qty);
      if (rec.due !== p.due_date) rec.due = rec.due < p.due_date ? rec.due : p.due_date;
      map.set(po, rec);
    }
    for (const [po, ap] of byPO.entries()) {
      const rec = map.get(po) || { due: '', planned: 0, applied: 0 };
      rec.applied += ap;
      map.set(po, rec);
    }
    const list = [];
    for (const [po, rec] of map.entries()) {
      list.push({ po, due: rec.due, planned: rec.planned, applied: rec.applied });
    }
    list.sort((a, b) => String(a.due).localeCompare(String(b.due)));
    return list;
  }

  // ── Sheet 3 ──
  function skuSummaryRows(joined, plan) {
    const metaByPO = new Map();
    for (const p of plan || []) {
      const po = trim(p.po_number);
      if (!po || metaByPO.has(po)) continue;
      metaByPO.set(po, {
        supplier_name: str(p.supplier_name),
        facility_name: str(p.facility_name),
        freight_type: str(p.freight_type),
        zendesk_ticket: str(p.zendesk_ticket),
      });
    }
    return (joined || []).map(r => {
      const m = metaByPO.get(r.po) || { supplier_name: '', facility_name: '', freight_type: '', zendesk_ticket: '' };
      const planned = toNum(r.planned), applied = toNum(r.applied);
      return {
        'PO': r.po,
        'Supplier Name': m.supplier_name,
        'Facility Name': m.facility_name,
        'Freight Type': m.freight_type,
        'Zendesk Ticket #': m.zendesk_ticket,
        'Planned': planned,
        'Applied': applied,
        'Discrepancy (Applied - Planned)': applied - planned,
      };
    });
  }

  // ── Sheet 4 ──
  // One row per physical bin. Weight comes from the bin manifest; the PO from whatever was
  // scanned into it first, which is how the screen has always attributed it.
  function mobileBinRows(plan, records, bins) {
    const weightByBin = new Map(), unitsByBin = new Map();
    for (const b of bins || []) {
      const mb = trim(b.mobile_bin);
      if (!mb) continue;
      weightByBin.set(mb, toNum(b.weight_kg));
      unitsByBin.set(mb, toNum(b.total_units));
    }
    const poToSup = new Map();
    for (const p of plan || []) {
      const po = trim(p.po_number);
      if (po && !poToSup.has(po)) poToSup.set(po, trim(p.supplier_name));
    }

    const seen = new Map();
    for (const r of records || []) {
      const mb = trim(r.mobile_bin || r.bin);
      const po = trim(r.po_number || r.po);
      if (!mb || seen.has(mb)) continue;
      seen.set(mb, { po, supplier: poToSup.get(po) || '' });
    }

    const out = Array.from(seen.entries()).map(([mb, meta]) => ({
      'Supplier': meta.supplier,
      'PO': meta.po,
      'Mobile_Bin': mb,
      'Weight_kg': weightByBin.get(mb) || 0,
      'Units': unitsByBin.get(mb) || 0,
    }));
    out.sort((a, b) => (a.Supplier || '').localeCompare(b.Supplier || '')
      || (a.PO || '').localeCompare(b.PO || '')
      || (a.Mobile_Bin || '').localeCompare(b.Mobile_Bin || ''));
    return out;
  }

  // ── Sheets 5 and 6 ──
  // Already computed by /summary/shipment_summary and /summary/shipment_detail; mapped onto
  // the column names rather than recomputed, so the workbook agrees with the screen.
  const pick = (o, names) => {
    for (const n of names) if (o && o[n] !== undefined && o[n] !== null && o[n] !== '') return o[n];
    return '';
  };
  function shipmentSummaryRows(rows) {
    return (rows || []).map(r => ({
      'Supplier Name': pick(r, ['Supplier Name', 'supplier_name', 'supplier']),
      'Zendesk Ticket #': pick(r, ['Zendesk Ticket #', 'zendesk_ticket', 'zendesk']),
      'Freight Type': pick(r, ['Freight Type', 'freight_type', 'freight']),
      'Facility Name': pick(r, ['Facility Name', 'facility_name', 'facility']),
      'Unique PO Count': toNum(pick(r, ['Unique PO Count', 'unique_po_count', 'po_count'])),
      'Total Units Applied': toNum(pick(r, ['Total Units Applied', 'total_units_applied', 'units'])),
      'Total Mobile Bins': toNum(pick(r, ['Total Mobile Bins', 'total_mobile_bins', 'bins'])),
      'Gross Weight': toNum(pick(r, ['Gross Weight', 'gross_weight', 'weight_kg'])),
      'CBM': toNum(pick(r, ['CBM', 'cbm'])),
    }));
  }
  function shipmentDetailRows(rows) {
    return (rows || []).map(r => ({
      'Supplier Name': pick(r, ['Supplier Name', 'supplier_name', 'supplier']),
      'Zendesk Ticket #': pick(r, ['Zendesk Ticket #', 'zendesk_ticket', 'zendesk']),
      'Freight Type': pick(r, ['Freight Type', 'freight_type', 'freight']),
      'Facility Name': pick(r, ['Facility Name', 'facility_name', 'facility']),
      'PO': pick(r, ['PO', 'po', 'po_number']),
      'Total Units Applied': toNum(pick(r, ['Total Units Applied', 'total_units_applied', 'units'])),
      'Total Mobile Bins': toNum(pick(r, ['Total Mobile Bins', 'total_mobile_bins', 'bins'])),
      'Gross Weight': toNum(pick(r, ['Gross Weight', 'gross_weight', 'weight_kg'])),
      'CBM': toNum(pick(r, ['CBM', 'cbm'])),
    }));
  }

  /**
   * @param {object} input
   *   weekStart        'YYYY-MM-DD'
   *   appliedUids      records for the week (or already-shaped rows)
   *   plan             plan rows for the week
   *   poSkuRows        /summary/po_sku rows, or raw records
   *   records          raw records, for the bin attribution
   *   bins             bin manifest, for weights
   *   shipmentSummary  /summary/shipment_summary rows
   *   shipmentDetail   /summary/shipment_detail rows
   * @returns {{sheets:object, counts:object, missing:string[], filename:string}}
   */
  function build(input) {
    const i = input || {};
    const ws = trim(i.weekStart);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) throw new Error('weekStart must be YYYY-MM-DD');

    const plan = i.plan || [];
    const byPO = unitsByPo(i.poSkuRows);

    const sheets = {
      'Applied UIDs': appliedUidRows(i.appliedUids),
      'PO x SKU Summary': poSkuRows(plan, i.poSkuRows),
      'SKU Summary': skuSummaryRows(joinPoProgress(plan, byPO), plan),
      'Mobile Bins': mobileBinRows(plan, i.records, i.bins),
      'Shipment Summary': shipmentSummaryRows(i.shipmentSummary),
      'Shipment Details': shipmentDetailRows(i.shipmentDetail),
    };

    const counts = {};
    const missing = [];
    for (const name of SHEET_NAMES) {
      counts[name] = sheets[name].length;
      // An empty sheet is reported rather than swallowed. Sometimes it is legitimate — a week
      // with no air freight — so the caller decides what an empty one means, not this module.
      if (!sheets[name].length) missing.push(name);
    }

    return { sheets, counts, missing, filename: 'Pinpoint_Report_' + ws + '.xlsx', sheetNames: SHEET_NAMES, columns: COLUMNS };
  }

  return {
    build, SHEET_NAMES, COLUMNS,
    appliedUidRows, poSkuRows, skuSummaryRows, mobileBinRows,
    shipmentSummaryRows, shipmentDetailRows, joinPoProgress, unitsByPo,
  };
});
