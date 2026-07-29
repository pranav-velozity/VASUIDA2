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

  // ── UI ──
  function styles() {
    if (document.getElementById('tenancy-styles')) return;
    const s = document.createElement('style'); s.id = 'tenancy-styles';
    s.textContent = `
      .tn-wrap{display:flex;align-items:center;gap:8px;margin-right:10px;}
      .tn-box{display:flex;align-items:center;gap:5px;border:0.5px solid rgba(0,0,0,0.14);border-radius:8px;padding:3px 7px;background:#fff;}
      .tn-lbl{font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};}
      .tn-sel{border:none;outline:none;background:transparent;font:600 11px/1 inherit;color:${BRAND};cursor:pointer;padding:2px 0;}
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
      console.log('[tenancy v3] org:', r.org_type, '| clients:', (r.client_ids || []).join(','), '| active:', window.pinpointClient || '(server-derived)');
    } catch (e) { /* diagnostics only — never block the app */ }
  }

  function init() {
    load();
    window.addEventListener('state:ready', load);
    // header can re-render on navigation — re-assert the switcher
    setInterval(() => { if (_who && !document.getElementById('tn-wrap')) render(); }, 1000);
    window.pinpointTenancy = () => _who;
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
