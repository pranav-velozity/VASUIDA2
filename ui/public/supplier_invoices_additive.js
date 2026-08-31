/* ── VelOzity Pinpoint — Supplier Invoices (Finance tab) v1 ──
   Monthly invoices from our service providers, reconciled line by line against what
   Pinpoint recorded.

   WHY THIS IS A SEPARATE MODULE RATHER THAN AN EDIT TO finance_additive.js:
   that file renders invoices, P&L, expenses and rates — all of it working and all of it
   load-bearing. Adding a fifth tab by editing its skeleton and its tab switcher would put
   four working features at risk for one new one. This injects a nav button and a content
   pane, and wraps window._finTab so the original is never modified. If this module fails
   to load, Finance behaves exactly as it does today.

   INTERNAL ONLY. Supplier cost across every client is visible here, so it follows the same
   rule as Quote Review: entitlement comes from the org type, not the client capability. */
;(function () {
  'use strict';

  const BRAND = '#990033', DARK = '#1C1C1E', MID = '#6E6E73', LIGHT = '#AEAEB2';
  const GREEN = '#1B7F3B', AMBER = '#B7791F', RED = '#B33F40';

  let _data = null, _sel = null, _busy = false, _month = '';

  const el = id => document.getElementById(id);
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const nf = n => Number(n || 0).toLocaleString();
  const money = n => n == null ? '—' : 'USD ' + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
    if (!r.ok) { const e = new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return d;
  }

  function styles() {
    if (el('si-styles')) return;
    const s = document.createElement('style'); s.id = 'si-styles';
    s.textContent = `
      .si-tiles{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px;}
      @media (max-width:900px){ .si-tiles{grid-template-columns:repeat(2,minmax(0,1fr));} }
      .si-tile{background:#fff;border:.5px solid rgba(0,0,0,.09);border-radius:11px;padding:12px 14px;
               box-shadow:0 1px 2px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.06);}
      .si-tl{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:${LIGHT};}
      .si-tv{font-size:19px;font-weight:700;color:${DARK};margin-top:3px;font-variant-numeric:tabular-nums;letter-spacing:-.02em;}
      .si-ts{font-size:10px;color:${LIGHT};margin-top:2px;}
      .si-card{background:#fff;border:.5px solid rgba(0,0,0,.09);border-radius:12px;padding:15px 17px;
               box-shadow:0 1px 2px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.06);margin-bottom:14px;}
      .si-sec{font-size:10px;font-weight:700;color:${LIGHT};text-transform:uppercase;letter-spacing:.07em;margin:0 0 9px;}
      table.si{width:100%;border-collapse:collapse;font-size:11px;}
      table.si th{text-align:left;font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:${LIGHT};
                  font-weight:600;padding:6px;border-bottom:.5px solid rgba(0,0,0,.08);}
      table.si th.n, table.si td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;}
      table.si td{padding:8px 6px;border-bottom:.5px solid rgba(0,0,0,.05);color:${DARK};}
      table.si tbody tr{cursor:pointer;}
      table.si tbody tr:hover td{background:rgba(0,0,0,.02);}
      table.si tr.sel td{background:rgba(153,0,51,.05);}
      .si-pill{display:inline-block;font-size:9px;font-weight:600;padding:2px 7px;border-radius:20px;}
      .si-none{font-size:11px;color:${LIGHT};padding:16px 2px;}
      .si-btn{border:0;border-radius:9px;background:${BRAND};color:#fff;font:600 12px inherit;
              padding:9px 15px;cursor:pointer;white-space:nowrap;}
      .si-btn.g{background:#fff;color:${DARK};border:.5px solid rgba(0,0,0,.16);}
      .si-btn.ok{background:${GREEN};}
      .si-btn:disabled{opacity:.55;cursor:default;}
      .si-in{width:100%;padding:9px 11px;border:.5px solid rgba(0,0,0,.14);border-radius:9px;
             font:inherit;font-size:13px;color:${DARK};background:#fff;}
      .si-lbl{display:block;font-size:11px;font-weight:600;color:${MID};margin:12px 0 4px;}
      .si-grid{display:grid;grid-template-columns:minmax(0,1fr) 420px;gap:18px;align-items:start;}
      @media (max-width:1200px){ .si-grid{grid-template-columns:1fr;} }
      .si-iss{font-size:11px;padding:7px 10px;border-radius:8px;margin-bottom:5px;line-height:1.4;}
      .si-msg{margin-top:10px;padding:9px 11px;border-radius:8px;font-size:11px;}
      .si-wk{border:.5px solid rgba(0,0,0,.09);border-radius:10px;padding:10px 12px;margin-bottom:8px;}
      .si-wkh{font-size:11px;font-weight:700;color:${DARK};display:flex;justify-content:space-between;}
    `;
    document.head.appendChild(s);
  }

  const ST = {
    requested: { t: 'Requested', c: MID,   b: 'rgba(0,0,0,.05)' },
    received:  { t: 'Received',  c: BRAND, b: 'rgba(153,0,51,.10)' },
    queried:   { t: 'Queried',   c: AMBER, b: 'rgba(183,121,31,.12)' },
    accepted:  { t: 'Accepted',  c: GREEN, b: 'rgba(27,127,59,.12)' },
    paid:      { t: 'Paid',      c: MID,   b: 'rgba(0,0,0,.05)' },
  };
  const pill = st => { const s = ST[st] || ST.requested;
    return `<span class="si-pill" style="color:${s.c};background:${s.b}">${s.t}</span>`; };

  // ── mount: a nav button and a pane, without touching finance_additive ──
  function inject() {
    if (!entitled()) { const b = el('fin-nav-supplier'); if (b) b.remove(); return false; }
    const rates = el('fin-nav-rates');
    if (!rates || !rates.parentNode) return false;
    if (!el('fin-nav-supplier')) {
      const b = document.createElement('button');
      b.className = rates.className; b.id = 'fin-nav-supplier';
      b.innerHTML = '<span class="nav-icon">$</span>Supplier Invoices';
      b.addEventListener('click', () => window._finTab('supplier'));
      rates.parentNode.insertBefore(b, rates.nextSibling);
    }
    if (!el('fin-tab-supplier')) {
      const host = el('fin-tab-rates');
      if (!host || !host.parentNode) return false;
      const d = document.createElement('div');
      d.id = 'fin-tab-supplier'; d.style.display = 'none';
      host.parentNode.insertBefore(d, host.nextSibling);
    }
    return true;
  }

  // Wrap rather than modify: the original switcher does not know about this tab, so it
  // cannot hide it — that is handled here, and the original is called untouched.
  let _wrapped = false;
  function wrapTabs() {
    if (_wrapped || typeof window._finTab !== 'function') return;
    const orig = window._finTab;
    window._finTab = function (tab) {
      const mine = el('fin-tab-supplier');
      if (tab === 'supplier') {
        ['invoices', 'pl', 'expenses', 'rates'].forEach(t => {
          const c = el('fin-tab-' + t); if (c) c.style.display = 'none';
          const b = el('fin-nav-' + t); if (b) b.classList.remove('active');
        });
        const b = el('fin-nav-supplier'); if (b) b.classList.add('active');
        const wk = el('fin-sidebar-week'); if (wk) wk.style.display = 'none';
        const k = el('fin-kpis'); if (k) k.style.display = 'none';
        if (mine) mine.style.display = '';
        load();
        return;
      }
      if (mine) mine.style.display = 'none';
      const b = el('fin-nav-supplier'); if (b) b.classList.remove('active');
      return orig.apply(this, arguments);
    };
    _wrapped = true;
  }

  // ── render ──
  async function load() {
    const host = el('fin-tab-supplier'); if (!host) return;
    if (!_data) host.innerHTML = '<div class="si-none">Loading…</div>';
    try {
      _data = await req('/finance/supplier-invoices' + (_month ? '?month=' + encodeURIComponent(_month) : ''));
      render();
    } catch (e) {
      host.innerHTML = `<div class="si-card"><div class="si-sec">Could not load</div>
        <div style="font-size:12px;color:${RED};">${esc(e.message || e)}</div></div>`;
    }
  }

  function render() {
    const host = el('fin-tab-supplier'); if (!host || !_data) return;
    const s = _data.summary || {}, list = _data.invoices || [];
    const sel = list.find(x => x.id === _sel) || null;
    const ag = s.ageing || {};

    host.innerHTML = `
      <div class="si-tiles">
        <div class="si-tile"><div class="si-tl">Payable to suppliers</div>
          <div class="si-tv">${money(s.payable_total)}</div>
          <div class="si-ts">accepted, not yet paid</div></div>
        <div class="si-tile"><div class="si-tl">Overdue</div>
          <div class="si-tv" style="color:${(s.overdue_total > 0) ? RED : DARK}">${money(s.overdue_total)}</div>
          <div class="si-ts">past 30-day terms</div></div>
        <div class="si-tile"><div class="si-tl">Awaiting review</div>
          <div class="si-tv">${nf(s.awaiting_review)}</div>
          <div class="si-ts">${s.with_issues ? `<b style="color:${AMBER}">${s.with_issues} with discrepancies</b>` : 'no discrepancies flagged'}</div></div>
        <div class="si-tile"><div class="si-tl">Ageing</div>
          <div class="si-tv" style="font-size:12px;line-height:1.5;">
            ${['current','1-30','31-60','60+'].map(k => `${k}: <b>${money(ag[k]).replace('USD ','')}</b>`).join('<br>')}
          </div></div>
      </div>

      <div class="si-grid">
        <div class="si-card">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:8px;">
            <div class="si-sec" style="margin:0;">Invoices</div>
            <div style="display:flex;gap:8px;align-items:center;">
              <select class="si-in" id="si-month" style="width:auto;padding:6px 9px;font-size:11px;">
                <option value="">All months</option>
                ${(_data.months || []).map(m => `<option value="${esc(m)}" ${m === _month ? 'selected' : ''}>${esc(m)}</option>`).join('')}
              </select>
              <button class="si-btn g" id="si-gen">Request a month</button>
            </div>
          </div>
          ${list.length ? `<table class="si"><thead><tr>
              <th>Month</th><th>Supplier</th><th>Service</th><th class="n">Weeks</th>
              <th class="n">Amount</th><th>Due</th><th>Status</th><th class="n">Issues</th>
            </tr></thead><tbody>${list.map(x => `<tr data-i="${esc(x.id)}" class="${x.id === _sel ? 'sel' : ''}">
              <td>${esc(x.month_key)}</td>
              <td>${esc(x.supplier)}</td>
              <td style="color:${MID}">${esc(x.type_label)}</td>
              <td class="n">${x.week_count}</td>
              <td class="n">${x.total_amount == null ? '—' : money(x.total_amount)}</td>
              <td style="color:${x.overdue ? RED : MID};white-space:nowrap;">${esc(x.due_date || '—')}${x.overdue ? ' <b>overdue</b>' : ''}</td>
              <td>${pill(x.status)}</td>
              <td class="n">${x.issue_count ? `<b style="color:${AMBER}">${x.issue_count}</b>` : '—'}</td>
            </tr>`).join('')}</tbody></table>`
            : `<div class="si-none">No supplier invoices yet. Use <b>Request a month</b> to create the month's submissions.</div>`}
        </div>
        <div id="si-detail">${sel ? '<div class="si-none">Loading…</div>' : '<div class="si-card"><div class="si-none">Select an invoice.</div></div>'}</div>
      </div>`;

    el('si-month').addEventListener('change', e => { _month = e.target.value; _data = null; load(); });
    el('si-gen').addEventListener('click', generate);
    host.querySelectorAll('[data-i]').forEach(r => r.addEventListener('click', () => {
      _sel = r.getAttribute('data-i'); render(); loadDetail();
    }));
    if (sel) loadDetail();
  }

  async function generate() {
    const m = prompt('Which month? (YYYY-MM)\n\nA week belongs to the month containing its Monday, so a month runs 4 or 5 weeks.',
      new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth() - 1, 1)).toISOString().slice(0, 7));
    if (!m) return;
    const send = confirm('Email the request links to the suppliers now?\n\nCancel creates the submissions without sending, so you can review them first.');
    try {
      const r = await req('/finance/supplier-invoices/generate',
        { method: 'POST', body: JSON.stringify({ month: m, send: send ? '1' : '' }) });
      alert(`${m}: ${r.weeks} weeks, closes ${r.closes}.\n`
        + `${r.invoices.filter(x => x.created).length} created, ${r.invoices.filter(x => !x.created).length} already existed.`
        + (r.warning ? `\n\n${r.warning}` : '')
        + (send ? `\n\nRequests emailed.` : `\n\nNot sent — use Resend link on each row when ready.`));
      _data = null; _month = m; load();
    } catch (e) { alert('Could not generate: ' + (e.message || e)); }
  }

  async function loadDetail() {
    const box = el('si-detail'); if (!box || !_sel) return;
    try {
      const d = await req('/finance/supplier-invoices/' + encodeURIComponent(_sel));
      box.innerHTML = detailHtml(d);
      wireDetail(d);
    } catch (e) {
      box.innerHTML = `<div class="si-card"><div style="font-size:12px;color:${RED};">${esc(e.message || e)}</div></div>`;
    }
  }

  function issueHtml(kind, i) {
    const map = {
      not_in_pinpoint: { c: RED,   t: 'Billed, not recorded' },
      not_billed:      { c: AMBER, t: 'Recorded, not billed' },
      size_mismatch:   { c: AMBER, t: 'Size mismatch' },
    };
    const m = map[kind] || { c: MID, t: kind };
    return `<div class="si-iss" style="background:${m.c === RED ? 'rgba(179,63,64,.08)' : 'rgba(183,121,31,.10)'};color:${m.c};">
      <b>${m.t}</b> — ${esc(i.ref || '')}${i.billed ? ` (billed ${esc(i.billed)}, recorded ${esc(i.recorded)})` : ''}
      ${i.note ? `<div style="color:${MID};font-size:10px;margin-top:2px;">${esc(i.note)}</div>` : ''}</div>`;
  }

  function detailHtml(d) {
    const canReview = ['received', 'queried'].includes(d.status);
    const vasRow = (w) => {
      const v = w.variance || {}, r = w.recorded || {}, b = w.billed || {};
      const cell = (billed, recorded, pct) => `${nf(billed)} <span style="color:${LIGHT}">/ ${nf(recorded)}</span>`
        + (pct == null || pct === 0 ? '' : ` <b style="color:${Math.abs(pct) > 2 ? AMBER : MID}">${pct > 0 ? '+' : ''}${pct}%</b>`);
      return `<div class="si-wk">
        <div class="si-wkh"><span>${esc(w.week_start)} → ${esc(w.week_end)}</span><span>${money(w.amount)}</span></div>
        <div style="font-size:11px;color:${DARK};margin-top:5px;line-height:1.6;">
          Units ${cell(b.units, r.units, v.units)} &middot; Cartons ${cell(b.cartons, r.cartons, v.cartons)} &middot; Pallets ${cell(b.pallets, r.pallets, v.pallets)}
          ${w.cost_per_unit != null ? `<br><span style="color:${MID}">Cost per unit <b>${w.cost_per_unit.toFixed(4)}</b> against our recorded units</span>` : ''}
        </div></div>`;
    };
    const lineRow = (w) => `<div class="si-wk">
      <div class="si-wkh"><span>${esc(w.week_start)} → ${esc(w.week_end)}</span><span>${money(w.amount)}</span></div>
      <div style="font-size:11px;color:${MID};margin-top:4px;">
        ${w.billed_count} billed &middot; ${w.recorded_count} recorded &middot; <b style="color:${w.matched === w.recorded_count && !w.issues.length ? GREEN : DARK}">${w.matched} matched</b></div>
      ${(w.issues || []).map(i => issueHtml(i.kind, i)).join('')}</div>`;

    return `<div class="si-card">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px;">
        <div><div style="font-size:14px;font-weight:700;color:${DARK};">${esc(d.supplier)}</div>
          <div style="font-size:11px;color:${LIGHT};">${esc(d.type_label)} &middot; ${esc(d.month_key)} &middot; ${d.week_count} weeks</div></div>
        ${pill(d.status)}
      </div>

      <dl style="display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin:14px 0 0;">
        ${d.invoice_number ? `<dt style="color:${MID}">Invoice</dt><dd style="margin:0;text-align:right;">${esc(d.invoice_number)}</dd>` : ''}
        ${d.invoice_date ? `<dt style="color:${MID}">Dated</dt><dd style="margin:0;text-align:right;">${esc(d.invoice_date)}</dd>` : ''}
        ${d.due_date ? `<dt style="color:${MID}">Due</dt><dd style="margin:0;text-align:right;color:${d.overdue ? RED : DARK}"><b>${esc(d.due_date)}</b>${d.overdue ? ' overdue' : ''}</dd>` : ''}
        ${d.total_amount != null ? `<dt style="color:${MID}">Total</dt><dd style="margin:0;text-align:right;"><b>${money(d.total_amount)}</b></dd>` : ''}
        ${d.submitted_by ? `<dt style="color:${MID}">Submitted by</dt><dd style="margin:0;text-align:right;">${esc(d.submitted_by)}</dd>` : ''}
        ${d.paid_at ? `<dt style="color:${MID}">Paid</dt><dd style="margin:0;text-align:right;">${esc(d.paid_at)}${d.payment_ref ? ' · ' + esc(d.payment_ref) : ''}</dd>` : ''}
      </dl>

      ${d.status === 'requested'
        ? `<div class="si-none">Not yet submitted.</div>
           <button class="si-btn g" id="si-resend" style="width:100%;margin-top:8px;">Resend the request link</button>`
        : `
        <div class="si-sec" style="margin:16px 0 8px;">Week by week &middot; billed against recorded</div>
        ${(d.reconciliation || []).map(w => d.shape === 'vas' ? vasRow(w) : lineRow(w)).join('')}
        ${d.issue_count ? `<div style="font-size:11px;color:${AMBER};margin-top:4px;"><b>${d.issue_count} discrepancy(ies)</b> — acceptance is not blocked, but the variance is recorded against the invoice.</div>` : ''}
      `}

      ${(d.messages || []).length ? `<div class="si-sec" style="margin:16px 0 6px;">Correspondence</div>
        ${d.messages.map(m => `<div style="padding:8px 10px;border-radius:8px;margin-bottom:6px;
          background:${m.author_role === 'internal' ? 'rgba(153,0,51,.05)' : '#F5F5F7'};">
          <div style="font-size:9px;color:${LIGHT};margin-bottom:2px;">${m.author_role === 'internal' ? 'VelOzity' : esc(m.author || 'Supplier')} &middot; ${esc(String(m.created_at).slice(0, 16))}</div>
          <div style="font-size:11px;line-height:1.45;">${esc(m.body)}</div></div>`).join('')}` : ''}

      ${canReview ? `
        <label class="si-lbl" for="si-q">Send back to the supplier</label>
        <textarea class="si-in" id="si-q" rows="2" placeholder="What needs correcting" style="resize:vertical;font-family:inherit;"></textarea>
        <div style="display:flex;gap:10px;margin-top:12px;">
          <button class="si-btn g" id="si-query" style="flex:1;color:${AMBER};border-color:rgba(183,121,31,.35);">Query</button>
          <button class="si-btn ok" id="si-accept" style="flex:2;">Accept</button>
        </div>` : ''}

      ${d.status === 'accepted' ? `
        <label class="si-lbl" for="si-ref">Payment reference</label>
        <input class="si-in" id="si-ref" placeholder="Transfer reference">
        <label class="si-lbl" for="si-pd">Payment date</label>
        <input class="si-in" id="si-pd" type="date" value="${new Date().toISOString().slice(0, 10)}">
        <button class="si-btn ok" id="si-pay" style="width:100%;margin-top:12px;">Mark as paid</button>
        <div style="font-size:10px;color:${LIGHT};margin-top:5px;">Marking as paid posts this to Expenses for ${esc(d.month_key)}.</div>` : ''}

      ${d.status === 'paid' ? `
        <button class="si-btn g" id="si-unpay" style="width:100%;margin-top:12px;color:${RED};border-color:rgba(179,63,64,.3);">Reverse payment</button>
        <div style="font-size:10px;color:${LIGHT};margin-top:5px;">Removes the expense this created and returns it to accepted.</div>` : ''}

      <div id="si-out"></div>
    </div>`;
  }

  function wireDetail(d) {
    const out = t => { const m = el('si-out'); if (m) m.innerHTML = t; };
    const err = t => out(`<div class="si-msg" style="background:rgba(179,63,64,.10);color:${RED};">${esc(t)}</div>`);
    const ok = t => out(`<div class="si-msg" style="background:rgba(27,127,59,.10);color:${GREEN};">${esc(t)}</div>`);
    const act = async (btn, path, body, done) => {
      if (_busy) return; _busy = true; if (btn) btn.disabled = true;
      try { const r = await req(path, { method: 'POST', body: JSON.stringify(body || {}) });
            done(r); _data = null; await load(); }
      catch (e) { err(e.message || String(e)); if (btn) btn.disabled = false; }
      _busy = false;
    };

    el('si-resend')?.addEventListener('click', function () {
      act(this, '/finance/supplier-invoices/' + _sel + '/send', {},
        r => ok(r.skipped ? `Not sent: ${r.reason}` : `Request emailed to ${(r.emailed_to || []).join(', ')}.`));
    });

    el('si-query')?.addEventListener('click', function () {
      const c = (el('si-q') || {}).value?.trim();
      if (!c) return err('Say what needs correcting — a bare query wastes a round.');
      act(this, '/finance/supplier-invoices/' + _sel + '/query', { comment: c },
        r => ok('Sent back to the supplier with your comment.'));
    });

    el('si-accept')?.addEventListener('click', function () {
      if (!confirm(`Accept ${d.invoice_number || 'this invoice'} for ${money(d.total_amount)}?\n\n`
        + (d.issue_count ? `${d.issue_count} discrepancy(ies) will be recorded against it.\n\n` : '')
        + `It becomes payable, due ${d.due_date}. It does not hit expenses until you mark it paid.`)) return;
      act(this, '/finance/supplier-invoices/' + _sel + '/accept', {}, r => ok(`Accepted — due ${r.due_date}.`));
    });

    el('si-pay')?.addEventListener('click', function () {
      const ref = (el('si-ref') || {}).value?.trim();
      const pd = (el('si-pd') || {}).value;
      if (!confirm(`Mark ${d.invoice_number} as paid?\n\n${money(d.total_amount)} will be posted to Expenses for ${d.month_key}.`)) return;
      act(this, '/finance/supplier-invoices/' + _sel + '/pay', { payment_ref: ref, paid_at: pd },
        r => ok(`Paid and posted to expenses.`));
    });

    el('si-unpay')?.addEventListener('click', function () {
      if (!confirm('Reverse this payment?\n\nThe expense it created will be removed and the invoice returns to accepted.')) return;
      act(this, '/finance/supplier-invoices/' + _sel + '/unpay', {}, r => ok('Payment reversed, expense removed.'));
    });
  }

  function init() {
    styles();
    const tick = setInterval(() => {
      if (!entitled()) { const b = el('fin-nav-supplier'); if (b) b.remove(); return; }
      wrapTabs();
      inject();
    }, 1200);
    window.addEventListener('tenancy:ready', () => { wrapTabs(); inject(); });
    setTimeout(() => { wrapTabs(); inject(); }, 900);
    console.log('[supplier-invoices] module v1 loaded');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
