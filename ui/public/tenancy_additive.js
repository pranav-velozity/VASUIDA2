/* ── VelOzity Pinpoint — Tenancy switchers v1 ──
   Client switcher (internal users) + org switcher (multi-org users, e.g. KLN).
   Adds x-pinpoint-client to API calls so writes carry an explicit client.
   Read-only with respect to data — enforcement lands in Step 3C. */
;(function () {
  'use strict';

  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const LS_KEY = 'pinpoint.activeClient';
  let _who = null;

  function apiBase() {
    return (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');
  }
  async function token() {
    if (window.Clerk && window.Clerk.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} }
    return null;
  }

  // ── active client (internal users only; client/partner orgs derive it server-side) ──
  function getActiveClient() { try { return localStorage.getItem(LS_KEY) || null; } catch (e) { return null; } }
  function setActiveClient(c) {
    try { c ? localStorage.setItem(LS_KEY, c) : localStorage.removeItem(LS_KEY); } catch (e) {}
    window.pinpointClient = c || null;
  }
  window.pinpointClient = getActiveClient();

  // ── fetch interceptor: attach the active client to API calls ──
  (function patchFetch() {
    if (window.__pinpointClientFetchPatched) return;
    window.__pinpointClientFetchPatched = true;
    const orig = window.fetch;
    window.fetch = async function (input, init) {
      try {
        const base = apiBase();
        const url = typeof input === 'string' ? input : (input && input.url) || '';
        const active = window.pinpointClient;
        if (base && active && url.startsWith(base)) {
          init = init || {};
          const h = new Headers(init.headers || (typeof input !== 'string' && input.headers) || {});
          if (!h.has('x-pinpoint-client')) h.set('x-pinpoint-client', active);
          init.headers = h;
        }
      } catch (e) { /* never block a request */ }
      return orig.call(this, input, init);
    };
  })();

  // ── Capability-gated nav ──
  // Subtractive only: hide what the active client doesn't have. Never force-show, so the
  // existing role-based gating (e.g. Finance) keeps the final say.
  const NAV_CAPS = {
    'nav-weekhub':        'week_hub',
    'nav-receiving':      'receiving_ops',
    'nav-vas-labelling':  'vas_labelling',
    'nav-exec':           'executive',
    'nav-map':            'live_map',
    'nav-reports':        'reports',
    'nav-finance':        'finance',
  };
  function applyCapabilityNav() {
    if (!_who) return;
    const r = _who.resolved || {};
    const active = window.pinpointClient || (r.client_ids || [])[0];
    const caps = (r.capabilities && r.capabilities[active]) || null;
    if (!caps) return;                       // unknown -> change nothing
    for (const [id, cap] of Object.entries(NAV_CAPS)) {
      const n = document.getElementById(id);
      if (n && !caps.includes(cap)) n.style.display = 'none';
    }
    // If Labelling is hidden, only show the VAS Ops parent when Fulfilment is available.
    const lab = document.getElementById('nav-vas-labelling');
    const ful = document.getElementById('nav-vas-fulfilment');
    const dd  = document.getElementById('nav-vas-dd');
    if (dd) {
      const labOn = lab && lab.style.display !== 'none';
      const fulOn = ful && ful.style.display !== 'none';
      if (!labOn && !fulOn) dd.style.display = 'none';
    }
  }

  // ── UI ──
  function styles() {
    if (document.getElementById('tenancy-styles')) return;
    const s = document.createElement('style'); s.id = 'tenancy-styles';
    s.textContent = `
      /* Deliberately quiet: context, not a control to be drawn to. No boxes, no brand
         colour — it only gains definition on hover. */
      .tn-wrap{display:flex;align-items:center;gap:14px;margin-right:12px;}
      .tn-box{display:flex;align-items:baseline;gap:5px;border:none;background:transparent;padding:0;
              border-bottom:1px solid transparent;transition:border-color .15s;}
      .tn-box:hover{border-bottom-color:rgba(0,0,0,0.18);}
      .tn-lbl{font-size:8.5px;text-transform:uppercase;letter-spacing:.08em;color:${LIGHT};font-weight:500;}
      .tn-sel{border:none;outline:none;background:transparent;font:500 11.5px/1 inherit;color:${MID};
              cursor:pointer;padding:1px 0;transition:color .15s;}
      .tn-sel:hover{color:#1C1C1E;}
      .tn-sel:disabled{color:${MID};cursor:default;}
    `;
    document.head.appendChild(s);
  }

  function render() {
    styles();
    const host = document.querySelector('.pn-user');
    if (!host || !_who) return;
    let wrap = document.getElementById('tn-wrap');
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.id = 'tn-wrap'; wrap.className = 'tn-wrap';
      host.insertBefore(wrap, host.firstChild);   // sits left of date/user/sign-out
    }

    const r = _who.resolved || {};
    const memberships = (window.Clerk && window.Clerk.user && window.Clerk.user.organizationMemberships) || [];
    let html = '';

    // Org switcher — only when the user belongs to more than one org (e.g. Kason: KLN CN + KLN US)
    if (memberships.length > 1) {
      const cur = window.Clerk?.organization?.id || '';
      html += `<div class="tn-box"><span class="tn-lbl">Org</span><select class="tn-sel" id="tn-org">
        ${memberships.map(m => {
          const o = m.organization || {};
          return `<option value="${o.id}" ${o.id === cur ? 'selected' : ''}>${(o.name || o.id)}</option>`;
        }).join('')}
      </select></div>`;
    }

    // Client switcher — internal users only; others have exactly one client, shown read-only
    const pickable = (r.client_ids || []).filter(c => c !== 'VOZ');   // VOZ = internal placeholder, not an operating tenant
    if (r.org_type === 'internal' && pickable.length > 1) {
      const active = window.pinpointClient || '';
      html += `<div class="tn-box"><span class="tn-lbl">Client</span><select class="tn-sel" id="tn-client">
        ${!active ? '<option value="">Select…</option>' : ''}
        ${pickable.map(c => `<option value="${c}" ${c === active ? 'selected' : ''}>${c}</option>`).join('')}
      </select></div>`;
    }
    // Client/partner users have exactly one client, derived server-side — show them nothing.

    wrap.innerHTML = html;

    const orgSel = document.getElementById('tn-org');
    if (orgSel) orgSel.addEventListener('change', async () => {
      try {
        await window.Clerk.setActive({ organization: orgSel.value });
        setActiveClient(null);          // scope changed — drop any stale client pick
        location.reload();
      } catch (e) { alert('Could not switch organisation: ' + (e.message || e)); }
    });

    const cliSel = document.getElementById('tn-client');
    if (cliSel) cliSel.addEventListener('change', () => {
      setActiveClient(cliSel.value || null);
      location.reload();                // re-fetch everything under the new client
    });
  }

  async function load() {
    const base = apiBase(); if (!base) return;
    const t = await token(); if (!t) return;
    try {
      const res = await fetch(base + '/tenancy/whoami', { headers: { Authorization: 'Bearer ' + t } });
      if (!res.ok) return;
      _who = await res.json();
      const r = _who.resolved || {};

      if (r.org_type === 'internal') {
        // Default to ICONIC on first load — the only client with data today, so behaviour is unchanged.
        if (!window.pinpointClient && (r.client_ids || []).includes('ICONIC')) setActiveClient('ICONIC');
      } else {
        // client/partner orgs: the server derives the client; don't send a header that could conflict.
        setActiveClient(null);
      }
      render();
      applyCapabilityNav();
      console.log('[tenancy v5] org:', r.org_type, '| clients:', (r.client_ids || []).join(','), '| active:', window.pinpointClient || '(server-derived)');
    } catch (e) { /* diagnostics only — never block the app */ }
  }

  function init() {
    // Retry with backoff: a page loaded during an API restart must not stay dead all session.
    let tries = 0;
    (function attempt() {
      load().then(() => {
        if (!_who && tries < 6) { tries++; setTimeout(attempt, 1000 * tries); }
      }).catch(() => { if (tries < 6) { tries++; setTimeout(attempt, 1000 * tries); } });
    })();
    window.addEventListener('state:ready', load);
    // header can re-render on navigation — re-assert the switcher
    setInterval(() => { if (_who && !document.getElementById('tn-wrap')) render(); applyCapabilityNav(); }, 1000);
    window.pinpointTenancy = () => _who;
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
