/*
 * lane_snapshot_additive.js — v1
 *
 * Thin client for the lane-engine APIs (snapshots, overrides, baselines).
 * Exposes a small global `LaneSnapshots` namespace that other modules consume.
 *
 * This is a READ-MOSTLY module. The heavy UI rendering stays in
 * flow_live_additive.js; this module's job is:
 *   - Fetch snapshots + actuals for a week (cached per week, auto-invalidates
 *     on flow-week updates).
 *   - Provide fast getters by laneKey for planned dates and actual sources.
 *   - Open a small per-lane override dialog (Layer 3).
 *   - Render the /settings/lane-baselines admin page (Layer 4).
 *
 * Nothing in this module writes to localStorage. All persistence goes through
 * the API endpoints (POST /lanes/snapshot/override, PUT /lanes/baselines/:f/:m).
 */

(function () {
  'use strict';

  // ------------------------- API base helpers -------------------------
  function getApiBase() {
    const m = document.querySelector('meta[name="api-base"]');
    return (m?.content || '').replace(/\/$/, '');
  }
  async function api(path, opts) {
    const base = getApiBase();
    const url = base ? `${base}${path}` : path;
    const headers = { 'Accept': 'application/json', ...(opts?.headers || {}) };
    if (opts?.method && opts.method !== 'GET' && !headers['Content-Type']) {
      headers['Content-Type'] = 'application/json';
    }
    // Bearer token supplied by Clerk via existing auth helper if available.
    try {
      if (window.__authHeader && typeof window.__authHeader === 'function') {
        const h = await window.__authHeader();
        if (h) Object.assign(headers, h);
      } else if (window.Clerk && window.Clerk.session && typeof window.Clerk.session.getToken === 'function') {
        const tok = await window.Clerk.session.getToken();
        if (tok) headers['Authorization'] = `Bearer ${tok}`;
      }
    } catch (_) { /* best-effort */ }
    const res = await fetch(url, { ...(opts || {}), headers });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(`${res.status} ${res.statusText} ${txt.slice(0, 200)}`);
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  }

  // ------------------------- Snapshot cache -------------------------
  //
  // Cached per weekStart. A cache entry looks like:
  //   { loaded_at, snapshots: [...], actuals_by_lane: { laneKey: { stage: {actual_at, source, ...} } } }
  //
  // Auto-invalidated when any /flow/week POST completes (monkey-wrapped fetch).

  const cache = new Map();
  const inflight = new Map();

  function buildSnapIndex(entry) {
    const idx = {};
    for (const s of (entry.snapshots || [])) {
      if (s && s.lane_key) idx[s.lane_key] = s;
    }
    entry._snapByLane = idx;
    return entry;
  }

  async function fetchSnapshotsForWeek(ws, opts) {
    if (!ws) return null;
    const force = !!(opts && opts.force);
    if (!force && cache.has(ws)) return cache.get(ws);
    if (inflight.has(ws)) return inflight.get(ws);
    const p = (async () => {
      try {
        const r = await api(`/lanes/snapshots/${encodeURIComponent(ws)}`);
        const entry = {
          loaded_at: Date.now(),
          week_start: ws,
          snapshots: Array.isArray(r?.snapshots) ? r.snapshots : [],
          actuals_by_lane: (r && r.actuals_by_lane && typeof r.actuals_by_lane === 'object') ? r.actuals_by_lane : {},
        };
        buildSnapIndex(entry);
        cache.set(ws, entry);
        return entry;
      } catch (e) {
        console.warn('[lane-snapshot] fetch failed for', ws, e.message || e);
        const empty = { loaded_at: Date.now(), week_start: ws, snapshots: [], actuals_by_lane: {}, _snapByLane: {}, _error: true };
        cache.set(ws, empty);
        return empty;
      } finally {
        inflight.delete(ws);
      }
    })();
    inflight.set(ws, p);
    return p;
  }

  function invalidate(ws) {
    if (ws) cache.delete(ws);
    else cache.clear();
  }

  // ------------------------- Getters -------------------------
  function toDate(iso) {
    if (!iso) return null;
    try { const d = new Date(iso); return isNaN(d) ? null : d; } catch { return null; }
  }

  // Returns planned dates + override set for a lane, or all-nulls if no snapshot.
  function getPlanned(ws, laneKey) {
    const entry = cache.get(ws);
    const snap = entry && entry._snapByLane ? entry._snapByLane[laneKey] : null;
    if (!snap) {
      return { pack: null, originClr: null, departed: null, arrived: null, destClr: null, fcReceipt: null, overridden: new Set(), hasSnapshot: false };
    }
    let overridden = [];
    try { overridden = JSON.parse(snap.overridden_fields_json || '[]'); } catch { overridden = []; }
    return {
      pack:      toDate(snap.planned_packing_list_ready_at),
      originClr: toDate(snap.planned_origin_cleared_at),
      departed:  toDate(snap.planned_departed_at),
      arrived:   toDate(snap.planned_arrived_at),
      destClr:   toDate(snap.planned_dest_cleared_at),
      fcReceipt: toDate(snap.planned_fc_receipt_at),
      overridden: new Set(overridden),
      hasSnapshot: true,
      _snap: snap,
    };
  }

  function getActualSource(ws, laneKey, stage) {
    const entry = cache.get(ws);
    if (!entry || !entry.actuals_by_lane) return null;
    const forLane = entry.actuals_by_lane[laneKey];
    if (!forLane) return null;
    const row = forLane[stage];
    return (row && row.source) ? row.source : null;
  }

  // Map intl_lanes field name → engine stage key. Must match server.
  const INTL_FIELD_TO_STAGE = {
    packing_list_ready_at:     'packing_list_ready',
    origin_customs_cleared_at: 'origin_cleared',
    departed_at:               'departed',
    arrived_at:                'arrived',
    dest_customs_cleared_at:   'dest_cleared',
    eta_fc:                    'fc_receipt',
  };

  // For a list of candidate field names, return source ('manual'/'auto_filled'/'imported') for the first match.
  function getActualSourceByIntlKeys(ws, laneKey, keys) {
    if (!Array.isArray(keys)) return null;
    for (const k of keys) {
      const stage = INTL_FIELD_TO_STAGE[k];
      if (!stage) continue;
      const src = getActualSource(ws, laneKey, stage);
      if (src) return src;
    }
    return null;
  }

  // ------------------------- Write actions -------------------------
  async function setOverride(laneKey, ws, field, valueIsoOrNull) {
    const body = JSON.stringify({ field, value: valueIsoOrNull });
    const r = await api(`/lanes/snapshot/override/${encodeURIComponent(laneKey)}/${encodeURIComponent(ws)}`, { method: 'POST', body });
    invalidate(ws);
    return r;
  }
  async function clearOverride(laneKey, ws, field) {
    return setOverride(laneKey, ws, field, null);
  }
  async function loadBaselines() {
    return api('/lanes/baselines');
  }
  async function saveBaseline(facility, mode, body) {
    return api(`/lanes/baselines/${encodeURIComponent(facility)}/${encodeURIComponent(mode)}`, { method: 'PUT', body: JSON.stringify(body) });
  }

  // ------------------------- DOM helpers -------------------------
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }
  function isoDateLocalInput(date) {
    if (!date || !(date instanceof Date) || isNaN(date)) return '';
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }

  // ------------------------- Layer 3: Override dialog -------------------------
  const STAGES_FOR_DIALOG = [
    { field: 'planned_packing_list_ready_at', label: 'Packing list ready' },
    { field: 'planned_origin_cleared_at',     label: 'Origin customs cleared' },
    { field: 'planned_departed_at',           label: 'Departed origin' },
    { field: 'planned_arrived_at',            label: 'Arrived destination' },
    { field: 'planned_dest_cleared_at',       label: 'Destination customs cleared' },
    { field: 'planned_fc_receipt_at',         label: 'ETA FC' },
  ];

  function ensureOverrideModal() {
    let root = document.getElementById('lane-override-modal');
    if (root) return root;
    root = document.createElement('div');
    root.id = 'lane-override-modal';
    root.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.45);z-index:1000;display:none;align-items:center;justify-content:center;padding:20px;';
    root.innerHTML = `
      <div style="background:white;border-radius:14px;max-width:640px;width:100%;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,0.3);">
        <div style="padding:18px 22px;border-bottom:1px solid #e5e7eb;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;">
          <div>
            <div id="lane-override-title" style="font-size:16px;font-weight:600;color:#111827;"></div>
            <div id="lane-override-sub" style="font-size:12px;color:#6b7280;margin-top:2px;"></div>
          </div>
          <button data-lane-override-close style="background:transparent;border:0;font-size:22px;color:#6b7280;cursor:pointer;line-height:1;">×</button>
        </div>
        <div id="lane-override-body" style="padding:16px 22px;overflow:auto;flex:1;"></div>
        <div style="padding:12px 22px;border-top:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center;gap:10px;">
          <div id="lane-override-status" style="font-size:12px;color:#6b7280;"></div>
          <div style="display:flex;gap:8px;">
            <button data-lane-override-close style="padding:8px 16px;border-radius:8px;border:1px solid #d1d5db;background:white;font-size:13px;font-weight:500;color:#374151;cursor:pointer;">Close</button>
            <button data-lane-override-save style="padding:8px 16px;border-radius:8px;border:0;background:#990033;font-size:13px;font-weight:600;color:white;cursor:pointer;">Save changes</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(root);
    root.addEventListener('click', (ev) => {
      if (ev.target === root || ev.target.closest('[data-lane-override-close]')) root.style.display = 'none';
    });
    return root;
  }

  async function openOverrideDialog(opts) {
    const { laneKey, weekStart, laneLabel } = opts || {};
    if (!laneKey || !weekStart) return;

    await fetchSnapshotsForWeek(weekStart, { force: true });
    const planned = getPlanned(weekStart, laneKey);

    const root = ensureOverrideModal();
    root.querySelector('#lane-override-title').textContent = 'Edit planned dates';
    root.querySelector('#lane-override-sub').textContent = (laneLabel || laneKey) + ' • Week ' + weekStart;
    const statusEl = root.querySelector('#lane-override-status');
    statusEl.textContent = '';

    const body = root.querySelector('#lane-override-body');
    if (!planned.hasSnapshot) {
      body.innerHTML = `
        <div style="padding:24px;text-align:center;color:#6b7280;">
          No planned-date snapshot exists for this lane yet. It will be created automatically
          next time the plan is uploaded, or an admin can run backfill.
        </div>
      `;
      root.style.display = 'flex';
      return;
    }

    const rowsHtml = STAGES_FOR_DIALOG.map((s) => {
      const currentVal = planned._snap[s.field] || '';
      const inputVal = currentVal ? isoDateLocalInput(new Date(currentVal)) : '';
      const isOverridden = planned.overridden.has(s.field);
      return `
        <div class="lor-row" data-field="${esc(s.field)}" style="display:grid;grid-template-columns:1fr 160px 100px;gap:10px;align-items:center;padding:10px 0;border-bottom:1px solid #f3f4f6;">
          <div>
            <div style="font-size:13px;font-weight:500;color:#111827;">${esc(s.label)}</div>
            <div style="font-size:11px;color:${isOverridden ? '#990033' : '#9ca3af'};margin-top:2px;">
              ${isOverridden ? 'Manually overridden' : 'Auto-computed from baseline'}
            </div>
          </div>
          <input type="date" value="${esc(inputVal)}" data-date-input style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;" />
          <button data-clear-override ${isOverridden ? '' : 'disabled'}
            style="padding:6px 10px;border:1px solid ${isOverridden ? '#fecdd3' : '#e5e7eb'};border-radius:6px;background:${isOverridden ? '#fef2f2' : '#f9fafb'};font-size:12px;color:${isOverridden ? '#990033' : '#9ca3af'};cursor:${isOverridden ? 'pointer' : 'not-allowed'};">
            Clear
          </button>
        </div>
      `;
    }).join('');

    body.innerHTML = `
      <div style="font-size:12px;color:#6b7280;margin-bottom:10px;">
        Changing a planned date marks it as <b>overridden</b>. Overridden dates survive baseline
        edits and downstream recomputes. Use <b>Clear</b> to revert a field to auto-computed.
      </div>
      ${rowsHtml}
    `;

    body.querySelectorAll('[data-clear-override]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        if (btn.hasAttribute('disabled')) return;
        const row = btn.closest('.lor-row');
        const field = row?.getAttribute('data-field');
        if (!field) return;
        btn.disabled = true; btn.textContent = '…';
        try {
          await clearOverride(laneKey, weekStart, field);
          statusEl.textContent = 'Cleared override.';
          setTimeout(() => openOverrideDialog(opts), 150);
        } catch (e) {
          statusEl.textContent = 'Clear failed: ' + (e.message || e);
          btn.disabled = false; btn.textContent = 'Clear';
        }
      });
    });

    // Replace save button to clear prior listeners
    const saveBtn = root.querySelector('[data-lane-override-save]');
    const newSaveBtn = saveBtn.cloneNode(true);
    saveBtn.replaceWith(newSaveBtn);
    newSaveBtn.addEventListener('click', async () => {
      const rowEls = body.querySelectorAll('.lor-row');
      const overrides = {};
      let any = false;
      for (const row of rowEls) {
        const field = row.getAttribute('data-field');
        const input = row.querySelector('[data-date-input]');
        const val = input && input.value ? input.value.trim() : '';
        const currentIso = planned._snap[field] || '';
        const currentDate = currentIso ? isoDateLocalInput(new Date(currentIso)) : '';
        if (val !== currentDate) {
          overrides[field] = val || null;
          any = true;
        }
      }
      if (!any) { statusEl.textContent = 'No changes to save.'; return; }
      newSaveBtn.disabled = true; newSaveBtn.textContent = 'Saving…';
      try {
        await api(`/lanes/snapshot/override/${encodeURIComponent(laneKey)}/${encodeURIComponent(weekStart)}`, {
          method: 'POST',
          body: JSON.stringify({ overrides }),
        });
        invalidate(weekStart);
        statusEl.textContent = 'Saved.';
        setTimeout(() => {
          root.style.display = 'none';
          window.dispatchEvent(new CustomEvent('lane-snapshot-changed', { detail: { weekStart, laneKey } }));
        }, 300);
      } catch (e) {
        statusEl.textContent = 'Save failed: ' + (e.message || e);
        newSaveBtn.disabled = false; newSaveBtn.textContent = 'Save changes';
      }
    });

    root.style.display = 'flex';
  }

  // ------------------------- Layer 4: Baselines admin page -------------------------
  const BASELINE_FIELDS = [
    { key: 'vas_to_packing_days',             label: 'VAS → Pack',        short: 'v→p' },
    { key: 'packing_to_origin_cleared_days',  label: 'Pack → Origin clr', short: 'p→oc' },
    { key: 'origin_cleared_to_departed_days', label: 'Origin clr → Dep',  short: 'oc→dep' },
    { key: 'departed_to_arrived_days',        label: 'Dep → Arrive',      short: 'dep→arr' },
    { key: 'arrived_to_dest_cleared_days',    label: 'Arrive → Dest clr', short: 'arr→dc' },
    { key: 'dest_cleared_to_fc_days',         label: 'Dest clr → FC',     short: 'dc→fc' },
    { key: 'grace_days',                      label: 'Grace',             short: 'grace' },
  ];

  function ensureBaselinesModal() {
    let root = document.getElementById('lane-baselines-modal');
    if (root) return root;
    root = document.createElement('div');
    root.id = 'lane-baselines-modal';
    root.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.45);z-index:1000;display:none;align-items:center;justify-content:center;padding:20px;';
    root.innerHTML = `
      <div style="background:white;border-radius:14px;max-width:1000px;width:100%;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,0.3);">
        <div style="padding:18px 22px;border-bottom:1px solid #e5e7eb;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;">
          <div>
            <div style="font-size:16px;font-weight:600;color:#111827;">Lane Baselines</div>
            <div style="font-size:12px;color:#6b7280;margin-top:2px;">
              Per-facility transit durations (median days). Edits apply to <b>new</b> lane snapshots only —
              in-flight lanes keep their frozen baseline values.
            </div>
          </div>
          <button data-baselines-close style="background:transparent;border:0;font-size:22px;color:#6b7280;cursor:pointer;line-height:1;">×</button>
        </div>
        <div id="lane-baselines-body" style="padding:14px 22px;overflow:auto;flex:1;"></div>
        <div style="padding:12px 22px;border-top:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center;gap:10px;">
          <div id="lane-baselines-status" style="font-size:12px;color:#6b7280;"></div>
          <button data-baselines-close style="padding:8px 16px;border-radius:8px;border:1px solid #d1d5db;background:white;font-size:13px;font-weight:500;color:#374151;cursor:pointer;">Close</button>
        </div>
      </div>
    `;
    document.body.appendChild(root);
    root.addEventListener('click', (ev) => {
      if (ev.target === root || ev.target.closest('[data-baselines-close]')) root.style.display = 'none';
    });
    return root;
  }

  async function openBaselinesAdmin() {
    const root = ensureBaselinesModal();
    const body = root.querySelector('#lane-baselines-body');
    const status = root.querySelector('#lane-baselines-status');
    body.innerHTML = `<div style="padding:40px;text-align:center;color:#6b7280;">Loading baselines…</div>`;
    status.textContent = '';
    root.style.display = 'flex';

    let data;
    try { data = await loadBaselines(); }
    catch (e) {
      body.innerHTML = `<div style="padding:40px;text-align:center;color:#b91c1c;">Load failed: ${esc(e.message || e)}</div>`;
      return;
    }
    const rows = Array.isArray(data?.baselines) ? data.baselines : [];

    const thCss = 'padding:8px 10px;font-size:11px;text-transform:uppercase;letter-spacing:0.04em;color:#6b7280;text-align:left;border-bottom:1px solid #e5e7eb;white-space:nowrap;';
    const tdCss = 'padding:8px 10px;font-size:13px;border-bottom:1px solid #f3f4f6;';
    const inputCss = 'width:60px;padding:4px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;text-align:right;';

    body.innerHTML = !rows.length
      ? `<div style="padding:40px;text-align:center;color:#6b7280;">No baselines configured.</div>`
      : `
        <table style="width:100%;border-collapse:collapse;">
          <thead>
            <tr>
              <th style="${thCss}">Facility</th>
              <th style="${thCss}">Mode</th>
              ${BASELINE_FIELDS.map((f) => `<th style="${thCss}" title="${esc(f.label)}">${esc(f.short)}</th>`).join('')}
              <th style="${thCss}">Last edited</th>
              <th style="${thCss}"></th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((r) => `
              <tr data-baseline-row data-facility="${esc(r.facility)}" data-mode="${esc(r.freight_mode)}">
                <td style="${tdCss}font-weight:600;color:#111827;">${esc(r.facility)}</td>
                <td style="${tdCss}color:${r.freight_mode === 'Air' ? '#0369a1' : '#065f46'};font-weight:500;">${esc(r.freight_mode)}</td>
                ${BASELINE_FIELDS.map((f) => `
                  <td style="${tdCss}">
                    <input type="number" step="0.5" min="0" max="365" data-field="${esc(f.key)}"
                      value="${esc(r[f.key] != null ? r[f.key] : '')}" style="${inputCss}" />
                  </td>
                `).join('')}
                <td style="${tdCss}color:#6b7280;font-size:11px;">${esc(r.updated_at ? String(r.updated_at).slice(0, 10) : '')}</td>
                <td style="${tdCss}">
                  <button data-save-baseline style="padding:5px 10px;border-radius:6px;border:0;background:#990033;color:white;font-size:12px;font-weight:500;cursor:pointer;">Save</button>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;

    body.querySelectorAll('[data-save-baseline]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const row = btn.closest('[data-baseline-row]');
        const facility = row.getAttribute('data-facility');
        const mode = row.getAttribute('data-mode');
        const payload = {};
        for (const f of BASELINE_FIELDS) {
          const input = row.querySelector(`input[data-field="${f.key}"]`);
          payload[f.key] = Number(input.value);
        }
        btn.disabled = true; btn.textContent = '…';
        try {
          await saveBaseline(facility, mode, payload);
          status.textContent = `Saved ${facility} ${mode}.`;
          btn.textContent = 'Saved';
          setTimeout(() => { btn.textContent = 'Save'; btn.disabled = false; }, 1200);
        } catch (e) {
          status.textContent = 'Save failed: ' + (e.message || e);
          btn.textContent = 'Save'; btn.disabled = false;
        }
      });
    });
  }

  // ------------------------- Cache invalidation hooks -------------------------
  window.addEventListener('lane-snapshot-changed', (ev) => {
    const ws = ev?.detail?.weekStart;
    if (ws) invalidate(ws);
  });

  // Invalidate on any /flow/week POST — that endpoint mirrors actuals to
  // lane_actual_dates and triggers recompute, so cached data is stale.
  (function monkeyWrapFlowWeekFetch() {
    if (window.__LANE_SNAPSHOT_FETCH_WRAPPED__) return;
    window.__LANE_SNAPSHOT_FETCH_WRAPPED__ = true;
    const origFetch = window.fetch;
    window.fetch = async function (...args) {
      const resp = await origFetch.apply(this, args);
      try {
        const req = args[0];
        const url = typeof req === 'string' ? req : (req && req.url) || '';
        const method = (args[1] && args[1].method) || (req && req.method) || 'GET';
        if (resp.ok && String(method).toUpperCase() === 'POST' && /\/flow\/week\//.test(url)) {
          const m = url.match(/\/flow\/week\/([^/?]+)/);
          if (m) invalidate(decodeURIComponent(m[1]));
        }
      } catch (_) { /* ignore */ }
      return resp;
    };
  })();

  // ------------------------- Public API -------------------------
  window.LaneSnapshots = {
    fetchSnapshotsForWeek,
    invalidate,
    getPlanned,
    getActualSource,
    getActualSourceByIntlKeys,
    openOverrideDialog,
    openBaselinesAdmin,
    _cache: cache,
  };

  // Lazy-warm: when weekStart changes in window.state, kick off a background fetch.
  (function warmLoop() {
    let lastWarmed = '';
    setInterval(() => {
      try {
        const ws = (window.state && window.state.weekStart) || (window.UI && window.UI.currentWs) || '';
        if (ws && ws !== lastWarmed) {
          lastWarmed = ws;
          fetchSnapshotsForWeek(ws).catch(() => {});
        }
      } catch (_) { /* ignore */ }
    }, 3000);
  })();

  console.log('[lane-snapshot] module v1 loaded');
})();
