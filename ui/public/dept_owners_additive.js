/* ── VelOzity Pinpoint — Department owners (Receiving Ops) v1 ──
   Maps the four-character PO prefix to the category admin who can actually chase it. The
   exception email groups outstanding POs by that person, so an unmapped code shows up in a
   client-facing email as UNKNOWN — this is where that gets fixed.

   WHY A STANDALONE MODULE: the Receiving page is rendered by a script this module does not
   own. Rather than edit a working page, this injects a single button into #page-receiving
   and keeps everything else in its own modal. If it fails to load, Receiving is unchanged.

   INTERNAL ONLY. These are people's names attached to accountability figures in an email
   that goes to the client, so it follows the same rule as Quote Review: entitlement comes
   from the org type, not a client capability. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#1B7F3B', AMBER = '#8A6D00', RED = '#B33F40';

  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || window.apiBase || '').replace(/\/+$/, '');
  const entitled = () => window.pinpointIsInternal === true;

  async function tok() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (e) {} } return null; }
  async function req(path, opts) {
    const t = await tok(), o = opts || {};
    const headers = { 'Content-Type': 'application/json', ...(o.headers || {}) };
    if (t) headers.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) headers['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { ...o, headers });
    const txt = await r.text(); let d = null;
    try { d = txt ? JSON.parse(txt) : null; } catch (e) { d = txt; }
    if (!r.ok) { const e = new Error((d && (d.error || d.message)) || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return d;
  }

  function styles() {
    if (el('do-styles')) return;
    const s = document.createElement('style'); s.id = 'do-styles';
    s.textContent = `
      .do-bar{display:flex;justify-content:flex-end;margin:0 0 12px;}
      .do-btn{border:0;border-radius:9px;background:${BRAND};color:#fff;font:600 12px inherit;
              padding:9px 15px;cursor:pointer;white-space:nowrap;}
      .do-btn.g{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);}
      .do-btn.sm{padding:5px 10px;font-size:11px;}
      .do-btn:disabled{opacity:.55;cursor:default;}
      .do-ov{position:fixed;inset:0;background:rgba(0,0,0,.34);z-index:9850;display:flex;
             align-items:flex-start;justify-content:center;padding:24px 18px;overflow:auto;}
      .do-panel{background:#fff;border-radius:14px;width:min(880px,100%);padding:20px 22px;
                box-shadow:0 18px 60px rgba(0,0,0,.22);}
      .do-in{padding:7px 9px;border:.5px solid rgba(0,0,0,.14);border-radius:7px;
             font:inherit;font-size:12px;color:${DARK};background:#fff;width:100%;}
      table.do{width:100%;border-collapse:collapse;font-size:12px;}
      table.do th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;
                  color:${LIGHT};font-weight:600;padding:6px 5px;border-bottom:.5px solid rgba(0,0,0,.08);}
      table.do td{padding:5px;border-bottom:.5px solid rgba(0,0,0,.05);vertical-align:middle;}
      .do-sec{font-size:10px;font-weight:700;color:${LIGHT};text-transform:uppercase;
              letter-spacing:.07em;margin:16px 0 7px;}
      .do-msg{margin-top:10px;padding:9px 11px;border-radius:8px;font-size:11px;}
      .do-warn{background:rgba(138,109,0,.10);color:${AMBER};padding:10px 12px;border-radius:9px;
               font-size:11px;line-height:1.5;margin-bottom:12px;}
    `;
    document.head.appendChild(s);
  }

  function injectButton() {
    if (!entitled()) { const b = el('do-open'); if (b) b.closest('.do-bar')?.remove(); return; }
    const page = el('page-receiving');
    if (!page || el('do-open')) return;
    // Only mount once the page has actually rendered, or the button lands in an empty
    // container that the receiving script then overwrites.
    if (!page.children.length) return;
    const bar = document.createElement('div');
    bar.className = 'do-bar';
    bar.innerHTML = `<button class="do-btn g" id="do-open">Department owners</button>`;
    page.insertBefore(bar, page.firstChild);
    el('do-open').addEventListener('click', open);
  }

  let _data = null;

  async function open() {
    styles();
    const ov = document.createElement('div');
    ov.className = 'do-ov';
    ov.addEventListener('click', e => { if (e.target === ov) ov.remove(); });
    ov.innerHTML = `<div class="do-panel"><div style="font-size:11px;color:${LIGHT};">Loading…</div></div>`;
    document.body.appendChild(ov);
    try { _data = await req('/department-owners'); } catch (e) {
      ov.querySelector('.do-panel').innerHTML =
        `<div style="font-size:12px;color:${RED};">Could not load: ${esc(e.message || e)}</div>`;
      return;
    }
    paint(ov);
  }

  function paint(ov) {
    const owners = (_data.owners || []).filter(o => o.active);
    const unmapped = _data.unmapped || [];
    const admins = _data.admins || [];

    ov.querySelector('.do-panel').innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <div><div style="font-size:15px;font-weight:700;">Department owners</div>
          <div style="font-size:11px;color:${LIGHT};">The first four characters of a PO are its department code. The exception report groups outstanding POs by the owner named here.</div></div>
        <button id="do-x" style="background:none;border:0;font-size:22px;color:${LIGHT};cursor:pointer;">&times;</button>
      </div>

      ${unmapped.length ? `<div class="do-warn" style="margin-top:14px;">
        <b>${unmapped.length} code${unmapped.length === 1 ? '' : 's'} seen on real POs with no owner mapped.</b>
        Until mapped, their POs appear in the client email under &ldquo;Owner To Be Assigned&rdquo;.<br>
        ${unmapped.map(u => `<button class="do-btn sm g" data-fill="${esc(u.code)}"
            style="margin:5px 5px 0 0;">${esc(u.code)} &middot; ${u.n} PO${u.n === 1 ? '' : 's'}</button>`).join('')}
      </div>` : ''}

      <div class="do-sec" style="margin-top:14px;">Add or update</div>
      <table class="do"><tr>
        <td style="width:15%"><input class="do-in" id="do-code" placeholder="CODE" maxlength="4"
             style="text-transform:uppercase;font-weight:600;"></td>
        <td style="width:14%"><input class="do-in" id="do-seg" placeholder="Segment" maxlength="8"
             style="text-transform:uppercase;"></td>
        <td style="width:33%"><input class="do-in" id="do-name" placeholder="Department name"></td>
        <td style="width:28%"><input class="do-in" id="do-admin" placeholder="Category admin" list="do-admins">
          <datalist id="do-admins">${admins.map(a => `<option value="${esc(a)}">`).join('')}</datalist></td>
        <td style="width:10%"><button class="do-btn sm" id="do-save" style="width:100%">Save</button></td>
      </tr></table>
      <div id="do-msg"></div>

      <div class="do-sec">Mapped &middot; ${owners.length} code${owners.length === 1 ? '' : 's'} across ${new Set(owners.map(o => o.admin)).size} owner${new Set(owners.map(o => o.admin)).size === 1 ? '' : 's'}</div>
      ${owners.length ? `<table class="do"><thead><tr>
          <th>Code</th><th>Segment</th><th>Department</th><th>Category admin</th><th></th>
        </tr></thead><tbody>${owners.map(o => `<tr data-row="${esc(o.code)}">
          <td><b>${esc(o.code)}</b></td>
          <td style="color:${MID}">${esc(o.segment || '—')}</td>
          <td style="color:${MID}">${esc(o.name || '—')}</td>
          <td>${esc(o.admin)}</td>
          <td style="text-align:right;white-space:nowrap;">
            <button class="do-btn sm g" data-edit="${esc(o.code)}">Edit</button>
            <button class="do-btn sm g" data-del="${esc(o.code)}" style="color:${RED};">Remove</button>
          </td>
        </tr>`).join('')}</tbody></table>`
        : `<div style="font-size:11px;color:${LIGHT};padding:12px 2px;">Nothing mapped yet.</div>`}`;

    const msg = (t, c) => { const m = ov.querySelector('#do-msg');
      m.innerHTML = t ? `<div class="do-msg" style="background:${c === GREEN ? 'rgba(27,127,59,.10)' : 'rgba(179,63,64,.10)'};color:${c}">${esc(t)}</div>` : ''; };

    ov.querySelector('#do-x').addEventListener('click', () => ov.remove());

    // A code from the unmapped list pre-fills the form — that is the whole point of
    // surfacing it, so it should take one click rather than retyping.
    ov.querySelectorAll('[data-fill]').forEach(b => b.addEventListener('click', () => {
      ov.querySelector('#do-code').value = b.getAttribute('data-fill');
      ov.querySelector('#do-admin').focus();
    }));

    ov.querySelectorAll('[data-edit]').forEach(b => b.addEventListener('click', () => {
      const o = owners.find(x => x.code === b.getAttribute('data-edit'));
      if (!o) return;
      ov.querySelector('#do-code').value = o.code;
      ov.querySelector('#do-seg').value = o.segment || '';
      ov.querySelector('#do-name').value = o.name || '';
      ov.querySelector('#do-admin').value = o.admin || '';
      ov.querySelector('#do-code').scrollIntoView({ block: 'center' });
      ov.querySelector('#do-admin').focus();
    }));

    ov.querySelector('#do-save').addEventListener('click', async function () {
      const code = (ov.querySelector('#do-code').value || '').trim().toUpperCase();
      const admin = (ov.querySelector('#do-admin').value || '').trim();
      if (!/^[A-Z0-9]{4}$/.test(code)) return msg('The code must be exactly four characters — it is the PO prefix.', RED);
      if (!admin) return msg('Enter the category admin.', RED);
      this.disabled = true;
      try {
        await req('/department-owners', { method: 'POST', body: JSON.stringify({
          code, admin,
          segment: (ov.querySelector('#do-seg').value || '').trim(),
          name: (ov.querySelector('#do-name').value || '').trim(),
        }) });
        _data = await req('/department-owners');
        paint(ov);
      } catch (e) { msg(e.message || String(e), RED); this.disabled = false; }
    });

    ov.querySelectorAll('[data-del]').forEach(b => b.addEventListener('click', async () => {
      const code = b.getAttribute('data-del');
      if (!confirm(`Remove ${code}?\n\nIts POs will show as unmapped in the exception report until it is mapped again.`)) return;
      b.disabled = true;
      try {
        await req('/department-owners/' + encodeURIComponent(code), { method: 'DELETE' });
        _data = await req('/department-owners');
        paint(ov);
      } catch (e) { msg(e.message || String(e), RED); b.disabled = false; }
    }));
  }

  function init() {
    styles();
    // The receiving page is rendered by a script this module does not own and can redraw
    // at any time, so the button is re-checked rather than mounted once.
    setInterval(injectButton, 1200);
    window.addEventListener('tenancy:ready', injectButton);
    setTimeout(injectButton, 900);
    console.log('[department-owners] module v1 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
