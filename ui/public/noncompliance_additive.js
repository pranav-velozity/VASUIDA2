/* ── VelOzity Pinpoint — Non-Compliance Capture v1 ──
   Direct-entry capture on the VAS Ops (#intake) page. Self-contained modal;
   Supplier→PO→SKU cascades from window.state.plan; carton cap + flag-for-review
   surfaced inline. Desktop drag-drop photos (auth'd route). Phone-QR upload and
   the PDF report are separate increments. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const BG = '#F5F5F7', AMBER = '#C8860A', GREEN = '#34C759', RED = '#D7263D';

  let _apiBase = '';
  const _nc = { week: '', categories: null, incidents: [], recon: null };
  let _staged = [];        // File objects staged in the form before the incident exists
  let _formOpen = false;

  // ── API ──
  function apiBase() { return _apiBase || window.apiBase || ''; }
  async function getToken() { if (window.Clerk && window.Clerk.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts = {}) {
    const token = await getToken();
    const headers = { ...(opts.headers || {}) };
    if (!(opts.body instanceof FormData)) headers['Content-Type'] = 'application/json';
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const r = await fetch(apiBase() + path, { ...opts, headers });
    const text = await r.text();
    let data = null; try { data = text ? JSON.parse(text) : null; } catch (e) { data = text; }
    if (!r.ok) { const err = new Error('HTTP ' + r.status); err.status = r.status; err.body = data; throw err; }
    return data;
  }

  // ── helpers ──
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const el = id => document.getElementById(id);
  function currentWeek() { return (window.state && window.state.weekStart) || _nc.week || ''; }
  function onVasPage() { return (location.hash || '').indexOf('intake') !== -1; }

  // Supplier → PO → SKU cascade from the loaded plan (defensive field reads).
  function planTree() {
    const plan = (window.state && Array.isArray(window.state.plan)) ? window.state.plan : [];
    const tree = new Map(); // supplier -> Map(po -> Set(sku))
    for (const r of plan) {
      const supplier = String(r.supplier || r.supplier_name || r.brand_name || '').trim();
      const po = String(r.po_number || r.po || '').trim();
      if (!supplier || !po) continue;
      const sku = String(r.item_sku || r.sku || r.sku_code || '').trim();
      if (!tree.has(supplier)) tree.set(supplier, new Map());
      const pos = tree.get(supplier);
      if (!pos.has(po)) pos.set(po, new Set());
      if (sku) pos.get(po).add(sku);
    }
    return tree;
  }

  async function ensureCategories() {
    if (_nc.categories) return _nc.categories;
    const d = await req('/nc/categories');
    _nc.categories = (d && d.categories) || [];
    return _nc.categories;
  }
  async function loadIncidents() {
    const week = currentWeek(); if (!week) return;
    try {
      const [inc, rec] = await Promise.all([
        req('/nc/incidents?week_start=' + encodeURIComponent(week)),
        req('/nc/reconciliation?week_start=' + encodeURIComponent(week)).catch(() => null),
      ]);
      _nc.incidents = (inc && inc.incidents) || [];
      _nc.recon = rec || null;
    } catch (e) { _nc.incidents = []; _nc.recon = null; }
  }

  // ── styles ──
  function injectStyles() {
    if (el('nc-styles')) return;
    const s = document.createElement('style'); s.id = 'nc-styles';
    s.textContent = `
      #nc-fab{position:fixed;right:22px;bottom:22px;z-index:9000;display:none;align-items:center;gap:8px;
        background:${BRAND};color:#fff;border:none;border-radius:999px;padding:12px 18px;font:600 13px/1 inherit;
        cursor:pointer;box-shadow:0 8px 24px rgba(153,0,51,0.35);}
      #nc-fab .nc-badge{background:#fff;color:${BRAND};border-radius:999px;font-size:11px;font-weight:700;padding:2px 7px;min-width:18px;text-align:center;}
      .nc-overlay{position:fixed;inset:0;z-index:9500;background:rgba(0,0,0,0.4);display:flex;align-items:center;justify-content:center;padding:24px;}
      .nc-modal{background:#fff;border-radius:16px;width:min(920px,96vw);max-height:92vh;overflow:auto;box-shadow:0 24px 64px rgba(0,0,0,0.3);}
      .nc-head{position:sticky;top:0;background:#fff;border-bottom:0.5px solid rgba(0,0,0,0.08);padding:16px 20px;display:flex;align-items:center;justify-content:space-between;z-index:2;}
      .nc-title{font-size:16px;font-weight:700;color:${DARK};letter-spacing:-0.02em;}
      .nc-sub{font-size:11px;color:${LIGHT};margin-top:2px;}
      .nc-x{background:none;border:none;font-size:20px;color:${MID};cursor:pointer;line-height:1;}
      .nc-body{padding:16px 20px;}
      .nc-btn{background:${BRAND};color:#fff;border:none;border-radius:8px;padding:8px 14px;font:600 12px/1 inherit;cursor:pointer;}
      .nc-btn.ghost{background:#fff;color:${DARK};border:0.5px solid rgba(0,0,0,0.14);}
      .nc-lbl{font-size:10px;color:${LIGHT};text-transform:uppercase;letter-spacing:.05em;margin:0 0 4px;}
      .nc-in,.nc-sel,.nc-ta{width:100%;border:0.5px solid rgba(0,0,0,0.14);border-radius:8px;padding:8px 10px;font:400 12px/1.3 inherit;color:${DARK};box-sizing:border-box;background:#fff;}
      .nc-ta{min-height:52px;resize:vertical;}
      .nc-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
      .nc-tile{border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:10px 12px;cursor:pointer;background:${BG};}
      .nc-tile.sel{border-color:${BRAND};box-shadow:0 0 0 1px ${BRAND} inset;background:#fff;}
      .nc-tile .s{font-size:12px;font-weight:600;color:${DARK};}
      .nc-tile .m{font-size:10px;color:${MID};margin-top:2px;}
      .nc-card{border:0.5px solid rgba(0,0,0,0.1);border-radius:10px;padding:10px 12px;margin-bottom:8px;}
      .nc-chip{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;border-radius:999px;}
      .nc-err{background:rgba(215,38,61,0.08);border:0.5px solid rgba(215,38,61,0.3);color:${RED};font-size:11px;border-radius:8px;padding:8px 10px;margin:8px 0;}
      .nc-drop{border:1.5px dashed rgba(0,0,0,0.18);border-radius:10px;padding:14px;text-align:center;font-size:11px;color:${MID};cursor:pointer;}
      .nc-drop.over{border-color:${BRAND};background:rgba(153,0,51,0.04);}
      .nc-thumb{width:44px;height:44px;object-fit:cover;border-radius:6px;border:0.5px solid rgba(0,0,0,0.1);}
      .nc-review{background:rgba(200,134,10,0.12);color:${AMBER};}
    `;
    document.head.appendChild(s);
  }

  // ── FAB ──
  let _fabWasVisible = false, _assertScheduled = false;
  function ensureFab() {
    let fab = el('nc-fab');
    if (!fab) {
      injectStyles();
      fab = document.createElement('button');
      fab.id = 'nc-fab';
      fab.innerHTML = '<span>⚑ Log non-compliance</span><span class="nc-badge" id="nc-badge">0</span>';
      fab.addEventListener('click', openNC);
      document.body.appendChild(fab);
    }
    const visible = onVasPage();
    fab.style.display = visible ? 'flex' : 'none';
    // Refresh the count only on transition INTO view — keeps the poll/observer network-free.
    if (visible && !_fabWasVisible) refreshBadge();
    _fabWasVisible = visible;
  }
  // Coalesce re-render bursts to one assert per animation frame.
  function scheduleEnsureFab() {
    if (_assertScheduled) return;
    _assertScheduled = true;
    requestAnimationFrame(() => { _assertScheduled = false; try { ensureFab(); } catch (e) {} });
  }
  async function refreshBadge() {
    try { await loadIncidents(); const b = el('nc-badge'); if (b) b.textContent = String(_nc.incidents.length); } catch (e) {}
  }

  // ── modal ──
  function closeModal() { const o = document.querySelector('.nc-overlay'); if (o) o.remove(); _formOpen = false; _staged = []; }
  async function openNC() {
    injectStyles();
    await ensureCategories().catch(() => {});
    await loadIncidents().catch(() => {});
    let o = document.querySelector('.nc-overlay');
    if (!o) {
      o = document.createElement('div'); o.className = 'nc-overlay';
      o.addEventListener('click', e => { if (e.target === o) closeModal(); });
      o.innerHTML = `<div class="nc-modal">
        <div class="nc-head">
          <div><div class="nc-title">Non-Compliance — ${esc(currentWeek() || 'this week')}</div>
          <div class="nc-sub">Log issues found at VAS · counts by carton or PO-SKU unit</div></div>
          <button class="nc-x" id="nc-close">×</button></div>
        <div class="nc-body" id="nc-content"></div></div>`;
      document.body.appendChild(o);
      el('nc-close').addEventListener('click', closeModal);
    }
    renderList();
  }

  function renderList() {
    _formOpen = false; _staged = [];
    const c = el('nc-content'); if (!c) return;
    const recon = _nc.recon;
    const uncat = recon ? recon.totals.uncategorized : null;
    const bySup = new Map();
    for (const i of _nc.incidents) { if (!bySup.has(i.supplier)) bySup.set(i.supplier, []); bySup.get(i.supplier).push(i); }
    let html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
      <div style="font-size:12px;color:${MID};">${_nc.incidents.length} incident(s) logged this week${uncat != null ? ` · <span style="color:${AMBER};font-weight:600;">${uncat} replaced carton(s) still without a reason</span>` : ''}</div>
      <button class="nc-btn" id="nc-new">＋ Log incident</button></div>`;
    if (!_nc.incidents.length) {
      html += `<div style="text-align:center;color:${LIGHT};font-size:12px;padding:24px;">No incidents yet. Click “Log incident” to add the first.</div>`;
    } else {
      for (const [sup, list] of bySup) {
        html += `<div style="font-size:12px;font-weight:700;color:${DARK};margin:14px 0 6px;">${esc(sup)}</div>`;
        for (const i of list) {
          const unit = i.grain === 'carton' ? 'cartons' : 'units';
          const catLabel = (_nc.categories || []).find(x => x.id === i.category_id);
          html += `<div class="nc-card">
            <div style="display:flex;justify-content:space-between;gap:8px;">
              <div>
                <div style="font-size:12px;font-weight:600;color:${DARK};">${esc(catLabel ? catLabel.label : i.category_id)} · ${i.qty} ${unit}</div>
                <div style="font-size:10px;color:${MID};margin-top:2px;">${esc(i.po_number)}${i.sku ? ' · ' + esc(i.sku) : ''}${i.corrective_action ? ' · ' + esc(i.corrective_action) : ''}</div>
              </div>
              <div style="display:flex;gap:6px;align-items:flex-start;">
                ${i.needs_review ? `<span class="nc-chip nc-review">⚑ review</span>` : ''}
                ${i.chargeable === 0 || i.chargeable === false ? `<span class="nc-chip" style="background:rgba(0,0,0,0.05);color:${MID};">no charge</span>` : ''}
                <span class="nc-chip" style="background:${i.status === 'resolved' ? 'rgba(52,199,89,0.14)' : 'rgba(0,0,0,0.06)'};color:${i.status === 'resolved' ? GREEN : MID};">${esc(i.status)}</span>
              </div>
            </div>
            <div style="display:flex;gap:6px;margin-top:8px;align-items:center;flex-wrap:wrap;">
              ${(i.images || []).slice(0, 6).map(im => `<img class="nc-thumb" src="${esc(im.url)}" alt="">`).join('')}
              <button class="nc-btn ghost" data-add-photo="${esc(i.id)}" style="padding:6px 10px;">＋ photo</button>
              <button class="nc-btn ghost" data-toggle="${esc(i.id)}" data-st="${esc(i.status)}" style="padding:6px 10px;">${i.status === 'resolved' ? 'Reopen' : 'Resolve'}</button>
            </div></div>`;
        }
      }
    }
    c.innerHTML = html;
    el('nc-new').addEventListener('click', renderForm);
    c.querySelectorAll('[data-toggle]').forEach(b => b.addEventListener('click', async () => {
      const id = b.getAttribute('data-toggle'), cur = b.getAttribute('data-st');
      try { await req('/nc/incident/' + id, { method: 'PATCH', body: JSON.stringify({ status: cur === 'resolved' ? 'open' : 'resolved' }) }); await loadIncidents(); renderList(); refreshBadge(); } catch (e) { alert('Could not update status: ' + (e.message || e)); }
    }));
    c.querySelectorAll('[data-add-photo]').forEach(b => b.addEventListener('click', () => addPhotoToExisting(b.getAttribute('data-add-photo'))));
  }

  function addPhotoToExisting(incidentId) {
    const inp = document.createElement('input'); inp.type = 'file'; inp.accept = 'image/*'; inp.multiple = true;
    inp.addEventListener('change', async () => {
      for (const f of inp.files) { try { await uploadPhoto(incidentId, f, 'desktop'); } catch (e) { alert('Upload failed: ' + (e.message || e)); } }
      await loadIncidents(); renderList();
    });
    inp.click();
  }

  async function uploadPhoto(incidentId, file, via) {
    const fd = new FormData(); fd.append('file', file);
    return req('/nc/incident/' + incidentId + '/image' + (via === 'phone' ? '?via=phone' : ''), { method: 'POST', body: fd });
  }

  // ── new-incident form ──
  function renderForm() {
    _formOpen = true; _staged = [];
    const c = el('nc-content'); if (!c) return;
    const tree = planTree();
    const suppliers = Array.from(tree.keys()).sort();
    const cats = _nc.categories || [];
    const rework = cats.filter(x => x.grp === 'rework');
    const delivery = cats.filter(x => x.grp === 'delivery_failure');
    c.innerHTML = `
      <button class="nc-btn ghost" id="nc-back" style="margin-bottom:12px;">← Back to list</button>
      <div id="nc-form-err"></div>
      <div style="margin-bottom:12px;"><div class="nc-lbl">Supplier</div>
        <select class="nc-sel" id="nc-supplier"><option value="">Select supplier…</option>
          ${suppliers.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('')}</select></div>
      <div class="nc-grid" style="margin-bottom:12px;">
        <div><div class="nc-lbl">PO</div><select class="nc-sel" id="nc-po" disabled><option value="">Select supplier first</option></select></div>
        <div><div class="nc-lbl">Category</div><select class="nc-sel" id="nc-cat">
          <option value="">Select category…</option>
          <optgroup label="Delivery failure — per carton">${delivery.map(x => `<option value="${esc(x.id)}" data-grain="carton">${esc(x.label)}</option>`).join('')}</optgroup>
          <optgroup label="Rework — per unit">${rework.map(x => `<option value="${esc(x.id)}" data-grain="unit">${esc(x.label)}</option>`).join('')}</optgroup>
        </select></div>
      </div>
      <div class="nc-grid" style="margin-bottom:12px;">
        <div id="nc-sku-wrap" style="display:none;"><div class="nc-lbl">SKU</div><select class="nc-sel" id="nc-sku" disabled><option value="">Select PO first</option></select></div>
        <div><div class="nc-lbl" id="nc-qty-lbl">Quantity</div><input class="nc-in" id="nc-qty" type="number" min="1" step="1" value="1"></div>
      </div>
      <div style="margin-bottom:12px;"><div class="nc-lbl">Corrective action / VAS done</div><textarea class="nc-ta" id="nc-action" placeholder="e.g. relabelled, transferred to mobile bin…"></textarea></div>
      <div style="margin-bottom:12px;"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:12px;color:${DARK};"><input type="checkbox" id="nc-chargeable" checked style="accent-color:${BRAND};"> Chargeable <span style="color:${LIGHT};font-size:10px;">— impacts invoice. Leave on unless this issue isn't billable.</span></label></div>
      <div style="margin-bottom:12px;"><div class="nc-lbl">Photos</div>
        <div class="nc-drop" id="nc-drop">Drag &amp; drop photos here, or click to choose. <span id="nc-staged" style="color:${BRAND};font-weight:600;"></span></div></div>
      <div style="display:flex;gap:8px;justify-content:flex-end;">
        <button class="nc-btn ghost" id="nc-cancel">Cancel</button>
        <button class="nc-btn" id="nc-save">Save incident</button>
      </div>`;

    el('nc-back').addEventListener('click', renderList);
    el('nc-cancel').addEventListener('click', renderList);

    const supSel = el('nc-supplier'), poSel = el('nc-po'), catSel = el('nc-cat'), skuWrap = el('nc-sku-wrap'), skuSel = el('nc-sku'), qtyLbl = el('nc-qty-lbl');
    supSel.addEventListener('change', () => {
      const pos = tree.get(supSel.value);
      if (pos) { poSel.disabled = false; poSel.innerHTML = '<option value="">Select PO…</option>' + Array.from(pos.keys()).sort().map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join(''); }
      else { poSel.disabled = true; poSel.innerHTML = '<option value="">—</option>'; }
      skuSel.disabled = true; skuSel.innerHTML = '<option value="">Select PO first</option>';
    });
    poSel.addEventListener('change', () => refreshSku());
    catSel.addEventListener('change', () => {
      const opt = catSel.selectedOptions[0];
      const grain = opt ? opt.getAttribute('data-grain') : '';
      skuWrap.style.display = grain === 'unit' ? '' : 'none';
      qtyLbl.textContent = grain === 'carton' ? 'Cartons' : (grain === 'unit' ? 'Units' : 'Quantity');
      if (grain === 'unit') refreshSku();
    });
    function refreshSku() {
      const opt = catSel.selectedOptions[0];
      if (!opt || opt.getAttribute('data-grain') !== 'unit') return;
      const pos = tree.get(supSel.value);
      const skus = (pos && pos.get(poSel.value)) ? Array.from(pos.get(poSel.value)).sort() : [];
      skuSel.disabled = !skus.length;
      skuSel.innerHTML = (skus.length ? '<option value="">Select SKU…</option>' : '<option value="">No SKUs on plan — enter manually</option>')
        + '<option value="__all">ALL SKUs on this PO</option>'
        + skus.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('')
        + '<option value="__other">Other / not on plan…</option>';
    }

    // photo drop zone (stage files; upload after the incident is created)
    const drop = el('nc-drop');
    const pick = document.createElement('input'); pick.type = 'file'; pick.accept = 'image/*'; pick.multiple = true; pick.style.display = 'none';
    drop.appendChild(pick);
    drop.addEventListener('click', () => pick.click());
    pick.addEventListener('change', () => { for (const f of pick.files) _staged.push(f); updateStaged(); });
    ['dragover', 'dragenter'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); drop.classList.add('over'); }));
    ['dragleave', 'drop'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); drop.classList.remove('over'); }));
    drop.addEventListener('drop', e => { const fs = e.dataTransfer && e.dataTransfer.files; if (fs) { for (const f of fs) if (f.type.startsWith('image/')) _staged.push(f); updateStaged(); } });
    function updateStaged() { const s = el('nc-staged'); if (s) s.textContent = _staged.length ? `${_staged.length} photo(s) staged` : ''; }

    el('nc-save').addEventListener('click', submitIncident);
  }

  function showFormErr(msg) { const e = el('nc-form-err'); if (e) e.innerHTML = msg ? `<div class="nc-err">${esc(msg)}</div>` : ''; }

  async function submitIncident() {
    showFormErr('');
    const supplier = el('nc-supplier').value.trim();
    const po = el('nc-po').value.trim();
    const catId = el('nc-cat').value;
    const opt = el('nc-cat').selectedOptions[0];
    const grain = opt ? opt.getAttribute('data-grain') : '';
    const qty = parseInt(el('nc-qty').value, 10) || 0;
    let sku = '';
    if (grain === 'unit') {
      sku = el('nc-sku').value;
      if (sku === '__other') { sku = (prompt('Enter SKU (not on plan):') || '').trim(); }
      else if (sku === '__all') { sku = 'ALL'; }
    }
    if (!supplier || !po) return showFormErr('Select a supplier and PO.');
    if (!catId) return showFormErr('Select a category.');
    if (qty <= 0) return showFormErr('Quantity must be at least 1.');
    if (grain === 'unit' && !sku) return showFormErr('Select or enter a SKU for a unit-level issue.');

    const payload = {
      week_start: currentWeek(), supplier, po_number: po, category_id: catId, qty,
      sku: grain === 'unit' ? sku : null,
      corrective_action: el('nc-action').value.trim() || null,
      chargeable: el('nc-chargeable') ? el('nc-chargeable').checked : true,
      created_by: (window.state && window.state.userName) || null,
    };
    const saveBtn = el('nc-save'); if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = 'Saving…'; }
    let incident;
    try {
      incident = await req('/nc/incident', { method: 'POST', body: JSON.stringify(payload) });
    } catch (e) {
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = 'Save incident'; }
      if (e.status === 409 && e.body && e.body.error === 'carton_cap_exceeded') {
        return showFormErr(e.body.message || `You can only categorise within the ${e.body.legacy} replaced cartons recorded for ${po} (${e.body.already} already assigned).`);
      }
      return showFormErr('Could not save: ' + (e.message || e));
    }
    // upload staged photos
    for (const f of _staged) { try { await uploadPhoto(incident.id, f, 'desktop'); } catch (e) {} }
    _staged = [];
    await loadIncidents(); renderList(); refreshBadge();
  }

  // ── init ──
  function init() {
    _apiBase = window.apiBase || '';
    injectStyles();
    ensureFab();
    // Immediate triggers (best-effort — the app sometimes sets the hash silently and
    // re-fires state:ready on week switches, so these can't be the only signal).
    window.addEventListener('hashchange', ensureFab);
    window.addEventListener('state:ready', () => { _apiBase = window.apiBase || _apiBase; ensureFab(); });
    // Robust re-assert: the VAS Ops page rebuilds its DOM on week switches without a
    // hashchange, so watch for any re-render and re-assert the button's presence/visibility.
    try { new MutationObserver(scheduleEnsureFab).observe(document.body, { childList: true, subtree: true }); } catch (e) {}
    // Backstop poll in case a re-render replaces body wholesale and the observer is lost.
    setInterval(ensureFab, 700);
    // expose for a native button hook if the VAS Ops page wants one
    window.openNC = openNC;
    console.log('[noncompliance] module v3 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
