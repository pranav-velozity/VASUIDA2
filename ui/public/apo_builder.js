/* ── VelOzity Pinpoint — Advanced PO builder (shared) v1 ──
   ONE implementation, used by two callers:
     · the Reports screen, where a person generates, reviews and publishes
     · the Sunday job, which generates and publishes unattended

   It was previously ~240 lines inside index.html, which meant the server had no way to
   produce the file at all. Copying it would have been worse than moving it: the first time
   a lead time changed in one copy and not the other, the file a human reviewed and the file
   ICONIC received would quietly differ.

   No DOM, no fetch, no globals. Inputs in, CSV out, so both callers get the same bytes by
   construction and the whole thing can be tested against a real published file.

   Exports as CommonJS (server) and window.APO_BUILDER (browser). */
;(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.APO_BUILDER = api;
})(typeof window !== 'undefined' ? window : null, function () {
  'use strict';

  // Exact column order required by THE ICONIC OWMS (Advance_PO_-_Required_Headers).
  // Order is part of the contract: their importer reads positionally.
  const APO_OUTPUT_COLUMNS = [
    'supplier', 'shipping_type', 'agreement', 'po_number', 'grand_total', 'tax_amount',
    'shipping_cost', 'note', 'reverse', 'logistic_type', 'logistic_method', 'logistic_date',
    'logistic_eta', 'inbound_tracking', 'supplier_contact_name', 'supplier_contact_phone',
    'supplier_contact_email', 'po_type', 'contract_type', 'payment_term', 'payment_type',
    'vendor_order_number', 'sku', 'cost', 'estimated_landed_cost', 'item_tax_amount',
    'quantity', 'production', 'po_currency', 'initiator', 'measurement', 'po_master',
    'qty_tolerance', 'id_excess', 'import_type', 'country_quantity', 'volumes_to_collect',
    'pickup_info', 'source_supplier_cost', 'source_supplier_currency', 'uid',
    'invoice_nr', 'master_carton_number',
  ];

  // The settings the form carries. They are defaults, not weekly decisions — but they are
  // passed in rather than baked, so the screen stays authoritative for a person who needs
  // to change one.
  const APO_DEFAULTS = {
    shipping_type: 'Warehouse',
    logistic_type: 'Delivered to Warehouse',
    import_type: 'China import',
    po_currency: 'AUD',
    payment_term: 30,
    note: 'China_3PL',
    po_type: 'Supply',
    contract_type: 'Outright',
    payment_type: 'Relabel',
    qty_tolerance: 5,
    tax_rate: 10,              // percent
    shipping_cost: 1.1,
    logistic_eta: '11:00',     // a time of day, constant across the file
    air_lead_days: 12,
    sea_lead_days: 30,
  };

  const norm = (v) => String(v == null ? '' : v).trim();

  const skuCore = (val) => {
    const s = norm(val);
    if (!s) return '';
    const parts = s.split('-');
    return parts[parts.length - 1].trim();
  };

  // UID cell format: "STYLE-SKUNUM;UIDTOKEN"
  const parseUid = (cell) => {
    const s = norm(cell);
    if (!s) return { skuFull: '', uidToken: '' };
    const parts = s.split(';');
    if (parts.length >= 2) return { skuFull: parts[0].trim(), uidToken: parts[1].trim() };
    return { skuFull: '', uidToken: s };
  };

  function toCsv(rows, columns) {
    const esc = (x) => {
      const s = (x === null || x === undefined) ? '' : String(x);
      if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    };
    const header = columns.map(esc).join(',');
    const body = rows.map(r => columns.map(c => esc(r[c])).join(',')).join('\n');
    return header + '\n' + body + '\n';
  }

  const fmtYmd = (d) => '' + d.getUTCFullYear()
    + String(d.getUTCMonth() + 1).padStart(2, '0')
    + String(d.getUTCDate()).padStart(2, '0');

  const methodFor = (ft) => {
    const f = String(ft || '').trim().toLowerCase();
    if (f === 'air') return 'AIR';
    if (f === 'sea') return 'SHIP';
    return 'TBD';
  };

  // Spreadsheet error text reaches the plan often enough to be worth stripping: a literal
  // "#N/A" in inbound_tracking would be imported as a tracking number.
  const cleanTicket = (t) => {
    const s = String(t || '').trim();
    if (!s) return '';
    if (/^#?n\/?a$/i.test(s) || /^#(ref|value|name|null|num|div)/i.test(s)) return '';
    return s;
  };

  /**
   * @param {object} input
   *   weekStart  'YYYY-MM-DD' Monday of the processing week
   *   planRows   plan rows for that week
   *   records    completed UID records for that week
   *   settings   overrides for APO_DEFAULTS
   * @returns {{csv:string, rows:Array, metrics:object, filename:string}}
   */
  function build(input) {
    const weekStart = norm(input && input.weekStart);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(weekStart)) throw new Error('weekStart must be YYYY-MM-DD');
    const planRows = (input && input.planRows) || [];
    const records = (input && input.records) || [];
    if (!planRows.length) throw new Error('No plan data found for week ' + weekStart + '. Upload the plan first.');
    if (!records.length) throw new Error('No applied UID records found for week ' + weekStart + '.');

    const s = Object.assign({}, APO_DEFAULTS, input && input.settings);
    const dflt = {
      shipping_type: norm(s.shipping_type) || APO_DEFAULTS.shipping_type,
      logistic_type: norm(s.logistic_type) || APO_DEFAULTS.logistic_type,
      import_type: norm(s.import_type) || APO_DEFAULTS.import_type,
      po_currency: norm(s.po_currency) || APO_DEFAULTS.po_currency,
      payment_term: Number(s.payment_term) || APO_DEFAULTS.payment_term,
      note: norm(s.note) || APO_DEFAULTS.note,
      po_type: norm(s.po_type) || APO_DEFAULTS.po_type,
      contract_type: norm(s.contract_type) || APO_DEFAULTS.contract_type,
      payment_type: norm(s.payment_type) || APO_DEFAULTS.payment_type,
      qty_tolerance: Number(s.qty_tolerance) || APO_DEFAULTS.qty_tolerance,
      tax_rate: Number(s.tax_rate == null ? APO_DEFAULTS.tax_rate : s.tax_rate) / 100,
      shipping_cost: Number(s.shipping_cost == null ? APO_DEFAULTS.shipping_cost : s.shipping_cost),
      logistic_eta: String(s.logistic_eta == null ? APO_DEFAULTS.logistic_eta : s.logistic_eta).trim(),
      air_lead_days: Number(s.air_lead_days == null ? APO_DEFAULTS.air_lead_days : s.air_lead_days),
      sea_lead_days: Number(s.sea_lead_days == null ? APO_DEFAULTS.sea_lead_days : s.sea_lead_days),
    };

    // ── Plan map: key = PO || numeric sku ──
    const planMap = new Map();
    let planSkipped = 0;
    for (const p of planRows) {
      const po = norm(p.po_number || p.po || '');
      const core = norm(p.sku_code || '');
      const style = norm(p.style || '');
      if (!po || !core) { planSkipped++; continue; }
      const key = po + '||' + core;
      if (!planMap.has(key)) {
        planMap.set(key, {
          supplier: norm(p.supplier_name || ''),
          supplier_contact_name: norm(p.supplier_contact || p.supplier_name || ''),
          supplier_contact_email: norm(p.supplier_contact_email || ''),
          supplier_contact_phone: norm(p.supplier_contact_phone || ''),
          vendor_order_number: norm(p.vendor_code || ''),
          initiator: norm(p.brand_name || ''),
          cost: p.cost,
          freight_type: norm(p.freight_type || ''),
          zendesk_ticket: norm(p.zendesk_ticket || ''),
          sku_full: style ? (style + '-' + core) : core,
        });
      }
    }

    // ── Normalise UID records ──
    let droppedMissingUID = 0, droppedMissingBin = 0;
    const normalized = [];
    for (const r of records) {
      const po = norm(r.po_number || r.po || '');
      const bin = norm(r.mobile_bin || r.mobileBin || r.bin || '');
      const parsed = parseUid(r.uid || '');
      const uidToken = norm(parsed.uidToken);
      const core = norm(r.sku_code || '') || skuCore(parsed.skuFull);
      if (!uidToken) { droppedMissingUID++; continue; }
      if (!bin) { droppedMissingBin++; continue; }
      normalized.push({
        po_key: po, sku_core: core, sku_full: parsed.skuFull,
        uid_token: uidToken, master_carton_number: bin,
      });
    }

    // ── Group by PO + SKU + bin, dedup UIDs ──
    const groups = new Map();
    let unmatched = 0, dupRemoved = 0;
    for (const x of normalized) {
      const pl = planMap.get(x.po_key + '||' + x.sku_core);
      if (!pl) unmatched++;
      const groupKey = x.po_key + '||' + x.sku_core + '||' + x.master_carton_number;
      if (!groups.has(groupKey)) {
        groups.set(groupKey, {
          po_number: x.po_key, sku_core: x.sku_core, master_carton_number: x.master_carton_number,
          uidSet: new Set(), uidList: [],
          supplier: (pl && pl.supplier) || '',
          supplier_contact_name: (pl && pl.supplier_contact_name) || '',
          supplier_contact_email: (pl && pl.supplier_contact_email) || '',
          supplier_contact_phone: (pl && pl.supplier_contact_phone) || '',
          vendor_order_number: (pl && pl.vendor_order_number) || '',
          initiator: (pl && pl.initiator) || '',
          cost: pl && pl.cost != null ? pl.cost : '',
          freight_type: (pl && pl.freight_type) || '',
          zendesk_ticket: (pl && pl.zendesk_ticket) || '',
          matched: !!pl,
          sku_full_plan: (pl && pl.sku_full) || '',
          sku_full_ship: x.sku_full || '',
        });
      }
      const g = groups.get(groupKey);
      if (g.uidSet.has(x.uid_token)) dupRemoved++;
      else { g.uidSet.add(x.uid_token); g.uidList.push(x.uid_token); }
    }

    // Friday of the processing week (Monday + 4), in UTC. Lead times run from there.
    const friday = new Date(weekStart + 'T00:00:00Z');
    friday.setUTCDate(friday.getUTCDate() + 4);
    const logisticDateFor = (method) => {
      let off = null;
      if (method === 'AIR') off = dflt.air_lead_days;
      else if (method === 'SHIP') off = dflt.sea_lead_days;
      if (off == null || !Number.isFinite(off)) return '';   // TBD → blank
      const d = new Date(friday.getTime());
      d.setUTCDate(d.getUTCDate() + off);
      return fmtYmd(d);
    };

    const rowsOut = [];
    let mappedUidTotal = 0, excludedUnmatched = 0, rowsMissingCost = 0, rowsTbd = 0;

    for (const g of groups.values()) {
      if (!g.matched) { excludedUnmatched++; continue; }      // a UID with no plan row is not shipped

      const costNum = Number.isFinite(Number(g.cost)) && String(g.cost).trim() !== '' ? Number(g.cost) : NaN;
      const hasCost = Number.isFinite(costNum);
      if (!hasCost) rowsMissingCost++;
      const qty = g.uidList.length;
      mappedUidTotal += qty;
      const skuOut = g.sku_full_plan || g.sku_full_ship || g.sku_core;
      const method = methodFor(g.freight_type);
      if (method === 'TBD') rowsTbd++;

      const row = {};
      for (const c of APO_OUTPUT_COLUMNS) row[c] = '';

      row.supplier = g.supplier;
      row.shipping_type = dflt.shipping_type;
      row.po_number = g.po_number;
      row.shipping_cost = dflt.shipping_cost;
      row.note = dflt.note;
      row.logistic_type = dflt.logistic_type;
      row.logistic_method = method;
      row.logistic_date = logisticDateFor(method);
      row.logistic_eta = dflt.logistic_eta;
      row.inbound_tracking = cleanTicket(g.zendesk_ticket);
      row.supplier_contact_name = g.supplier_contact_name;
      row.supplier_contact_phone = g.supplier_contact_phone;
      row.supplier_contact_email = g.supplier_contact_email;
      row.po_type = dflt.po_type;
      row.contract_type = dflt.contract_type;
      row.payment_term = dflt.payment_term;
      row.payment_type = dflt.payment_type;
      row.vendor_order_number = g.vendor_order_number;
      row.sku = skuOut;
      row.cost = hasCost ? costNum : '';
      row.estimated_landed_cost = hasCost ? (costNum + dflt.shipping_cost) : '';
      row.item_tax_amount = hasCost ? (costNum * dflt.tax_rate) : '';
      row.quantity = qty;
      row.po_currency = dflt.po_currency;
      row.initiator = '';                    // blank per spec
      row.po_master = g.po_number;
      row.qty_tolerance = dflt.qty_tolerance;
      row.import_type = dflt.import_type;
      row.country_quantity = '';             // blank per spec
      row.source_supplier_cost = hasCost ? costNum : '';
      row.source_supplier_currency = dflt.po_currency;
      row.uid = g.uidList.join(';');
      row.master_carton_number = g.master_carton_number;

      rowsOut.push(row);
    }

    rowsOut.sort((a, b) =>
      (a.po_number + '||' + a.sku + '||' + a.master_carton_number)
        .localeCompare(b.po_number + '||' + b.sku + '||' + b.master_carton_number));

    const outputUidSet = new Set();
    for (const g of groups.values()) for (const u of g.uidList) outputUidSet.add(u);

    const metrics = {
      'Plan rows read': planRows.length,
      'Plan rows skipped (no PO or SKU)': planSkipped,
      'UID records read': records.length,
      'Dropped — no UID token': droppedMissingUID,
      'Dropped — no mobile bin': droppedMissingBin,
      'Unique UIDs in input': new Set(normalized.map(x => x.uid_token)).size,
      'Unique mobile bins in input': new Set(normalized.map(x => x.master_carton_number)).size,
      'Duplicate UID occurrences removed': dupRemoved,
      'Unmatched UID rows (no Plan match)': unmatched,
      'Groups excluded — unmatched': excludedUnmatched,
      'Grouped output rows': rowsOut.length,
      'Total mapped UIDs in output': mappedUidTotal,
      'Unique UIDs in output': outputUidSet.size,
      'Rows with no cost': rowsMissingCost,
      'Rows with freight TBD': rowsTbd,
    };

    return {
      csv: toCsv(rowsOut, APO_OUTPUT_COLUMNS),
      rows: rowsOut,
      metrics,
      filename: 'TIC_VOZ_' + weekStart + '_AdvancedPO.csv',
    };
  }

  return { build, APO_OUTPUT_COLUMNS, APO_DEFAULTS, toCsv, parseUid, skuCore, methodFor, cleanTicket };
});
