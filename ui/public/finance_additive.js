/* ── VelOzity Pinpoint — Finance Module ── */
/* Admin-only: Invoices, P&L, Expenses       */
;(function(){
'use strict';

// ── Constants ──
const BRAND = '#990033';
const DARK  = '#1C1C1E';
const MID   = '#6E6E73';
const LIGHT = '#AEAEB2';
const BG    = '#F5F5F7';
const WHITE = '#FFFFFF';
const GREEN = '#34C759';
const AMBER = '#C8860A';
const BLUE  = '#3B82F6';

const EXPENSE_CATS = ['Freight Cost','Labour','Software','Office','Duties & Customs','Storage','Marketing','Other'];

const VAS_RATES = {
  'VAS Base Processing':          { unit:'Per Unit',        rate:0.21 },
  'Outbound Activities':          { unit:'Per Unit',        rate:0.05 },
  'Additional Labelling':         { unit:'Per Unit',        rate:0.01 },
  'Polybagging':                  { unit:'Per Unit',        rate:0.05 },
  'Storage post-processing':      { unit:'Per Unit Per Day',rate:0.01 },
  'Carton Replacement - labour only': { unit:'Per Carton', rate:1.10 },
};

let _apiBase = '';
let _token   = null;
let _finState = {
  tab: 'invoices',
  week: '',
  invoices: [],
  expenses: [],
  pl: null,
  fxRates: {},
  currency: 'USD',
  editInvoice: null,
};

// ── API helper ──
async function api(path, opts={}) {
  if (!_token && window.Clerk?.session) {
    try { _token = await window.Clerk.session.getToken(); } catch {}
  }
  const headers = { 'Content-Type':'application/json', ...(opts.headers||{}) };
  if (_token) headers['Authorization'] = 'Bearer ' + _token;
  const r = await fetch(_apiBase + path, { ...opts, headers });
  if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + await r.text());
  return r.json();
}

// ── Helpers ──
function fmtUSD(v, curr) {
  const c = curr || _finState.currency || 'USD';
  const rate = _finState.fxRates[c] || 1;
  const f = (parseFloat(v)||0) * (c === 'USD' ? 1 : rate);
  return new Intl.NumberFormat('en-US', { style:'currency', currency: c, minimumFractionDigits:2 }).format(f);
}
function fmtDate(s) {
  if (!s) return '—';
  try { return new Date(s+'T00:00:00Z').toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'}); } catch { return s; }
}
function isoToday() { return new Date().toISOString().slice(0,10); }
function addDays(iso, n) {
  const d = new Date(iso+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()+n); return d.toISOString().slice(0,10);
}
function weekLabel(ws) {
  try {
    const d = new Date(ws+'T00:00:00Z');
    return 'W/C '+d.toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'});
  } catch { return ws; }
}
function statusBadge(status) {
  const cfg = {
    draft:   { bg:'rgba(174,174,178,0.15)', color:MID,   label:'Draft'   },
    sent:    { bg:'rgba(50,130,246,0.12)',  color:BLUE,  label:'Sent'    },
    paid:    { bg:'rgba(52,199,89,0.12)',   color:GREEN, label:'Paid'    },
    overdue: { bg:`rgba(153,0,51,0.12)`,   color:BRAND, label:'Overdue' },
  }[status] || { bg:BG, color:MID, label:status };
  return `<span style="display:inline-block;padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;background:${cfg.bg};color:${cfg.color};">${cfg.label}</span>`;
}
function typeIcon(type) {
  return type==='VAS' ? '⚙️' : type==='SEA' ? '🚢' : '✈️';
}
function el(id) { return document.getElementById(id); }
function esc(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// ── Fetch FX rates (live with manual fallback) ──
async function fetchFX() {
  try {
    const r = await fetch('https://open.er-api.com/v6/latest/USD');
    if (r.ok) {
      const data = await r.json();
      if (data.rates) {
        _finState.fxRates = data.rates;
        _finState.fxRates.USD = 1;
        _finState.fxLastUpdated = data.time_last_update_utc || 'live';
        // Save to server
        for (const [to, rate] of Object.entries(data.rates)) {
          api('/finance/fx', { method:'POST', body: JSON.stringify({from_curr:'USD', to_curr:to, rate, source:'live'}) }).catch(()=>{});
        }
        return;
      }
    }
  } catch {}
  // Fallback: load from server
  try {
    const rows = await api('/finance/fx');
    for (const r of rows) { if (r.from_curr==='USD') _finState.fxRates[r.to_curr] = r.rate; }
  } catch {}
  _finState.fxRates.USD = 1;
}

// ═══════════════════════════════════════════════════════
// ── PAGE SKELETON ──
// ═══════════════════════════════════════════════════════
function injectSkeleton(host) {
  host.innerHTML = `
  <div id="fin-wrap" style="max-width:1100px;margin:0 auto;padding:28px 20px 60px;">

    <!-- Header row -->
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
      <div>
        <div style="font-size:20px;font-weight:700;color:${DARK};letter-spacing:-0.02em;">Finance</div>
        <div style="font-size:11px;color:${LIGHT};margin-top:2px;">Invoices · P&amp;L · Expenses · Admin only</div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;">
        <div id="fin-fx-label" style="font-size:10px;color:${LIGHT};"></div>
        <select id="fin-currency" style="border:1px solid rgba(0,0,0,0.12);border-radius:8px;padding:5px 10px;font-size:12px;font-family:inherit;background:#fff;color:${DARK};outline:none;cursor:pointer;">
          <option value="USD">USD</option>
          <option value="AUD">AUD</option>
          <option value="EUR">EUR</option>
          <option value="GBP">GBP</option>
        </select>
      </div>
    </div>

    <!-- KPI tiles -->
    <div id="fin-kpis" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;"></div>

    <!-- Sub-tabs -->
    <div style="display:flex;gap:4px;background:${BG};border-radius:10px;padding:4px;margin-bottom:20px;width:fit-content;">
      <button class="fin-tab active" data-tab="invoices" onclick="window._finTab('invoices')">Invoices</button>
      <button class="fin-tab" data-tab="pl" onclick="window._finTab('pl')">P&amp;L</button>
      <button class="fin-tab" data-tab="expenses" onclick="window._finTab('expenses')">Expenses</button>
    </div>

    <!-- Tab content -->
    <div id="fin-tab-invoices"></div>
    <div id="fin-tab-pl" style="display:none;"></div>
    <div id="fin-tab-expenses" style="display:none;"></div>

  </div>

  <!-- Invoice editor slide-in panel -->
  <div id="fin-panel" style="position:fixed;top:0;right:0;width:560px;height:100vh;background:#fff;border-left:0.5px solid rgba(0,0,0,0.1);box-shadow:-8px 0 40px rgba(0,0,0,0.08);transform:translateX(100%);transition:transform .3s cubic-bezier(0.4,0,0.2,1);z-index:200;overflow-y:auto;padding:24px 22px 40px;"></div>
  <div id="fin-overlay" onclick="window._finClosePanel()" style="position:fixed;inset:0;background:rgba(0,0,0,0.2);z-index:199;display:none;"></div>
  `;

  // Tab style
  const s = document.createElement('style');
  s.textContent = `
    .fin-tab{font-size:12px;font-weight:500;color:${MID};padding:6px 16px;border-radius:7px;border:none;background:transparent;cursor:pointer;transition:all .15s;font-family:inherit;}
    .fin-tab.active{background:#fff;color:${DARK};box-shadow:0 1px 4px rgba(0,0,0,0.08);}
    .fin-tab:hover:not(.active){background:rgba(0,0,0,0.04);color:${DARK};}
    .fin-card{background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;}
    .fin-input{border:1px solid rgba(0,0,0,0.12);border-radius:8px;padding:7px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;width:100%;box-sizing:border-box;transition:border-color .15s;}
    .fin-input:focus{border-color:${BRAND};}
    .fin-btn{border:none;border-radius:8px;padding:8px 16px;font-size:12px;font-weight:600;font-family:inherit;cursor:pointer;transition:all .15s;}
    .fin-btn-primary{background:${BRAND};color:#fff;}
    .fin-btn-primary:hover{background:#7a0029;}
    .fin-btn-ghost{background:${BG};color:${DARK};}
    .fin-btn-ghost:hover{background:rgba(0,0,0,0.08);}
    .fin-tbl{width:100%;border-collapse:collapse;font-size:12px;}
    .fin-tbl th{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.04em;padding:8px 12px;text-align:left;border-bottom:0.5px solid rgba(0,0,0,0.07);}
    .fin-tbl td{padding:10px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);color:${DARK};vertical-align:middle;}
    .fin-tbl tr:hover td{background:${BG};}
    .fin-label{font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:5px;}
  `;
  document.head.appendChild(s);
}

// ── Tab switch ──
window._finTab = function(tab) {
  _finState.tab = tab;
  document.querySelectorAll('.fin-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  ['invoices','pl','expenses'].forEach(t => {
    const d = el(`fin-tab-${t}`);
    if (d) d.style.display = t === tab ? '' : 'none';
  });
  if (tab === 'invoices') renderInvoicesTab();
  if (tab === 'pl')       renderPLTab();
  if (tab === 'expenses') renderExpensesTab();
};

window._finClosePanel = function() {
  const p = el('fin-panel'); const o = el('fin-overlay');
  if (p) p.style.transform = 'translateX(100%)';
  if (o) o.style.display = 'none';
};
function openPanel(html) {
  const p = el('fin-panel'); const o = el('fin-overlay');
  if (p) { p.innerHTML = html; p.style.transform = 'translateX(0)'; }
  if (o) o.style.display = 'block';
}

// ═══════════════════════════════════════════════════════
// ── KPI TILES ──
// ═══════════════════════════════════════════════════════
async function renderKPIs() {
  try {
    const s = await api('/finance/summary');
    const outstanding = s.outstanding || {};
    const paid = s.paid_ytd || {};
    const expenses = s.expenses_ytd || {};
    const revenue = parseFloat(paid.total||0);
    const exp = parseFloat(expenses.total||0);
    const margin = revenue > 0 ? Math.round((revenue-exp)/revenue*100) : 0;

    const tiles = [
      { label:'Revenue YTD', value: fmtUSD(revenue), sub:'Paid invoices', color:GREEN },
      { label:'Expenses YTD', value: fmtUSD(exp), sub:'All categories', color:AMBER },
      { label:'Net Margin', value: margin+'%', sub:'Revenue - Expenses', color: margin>20?GREEN:margin>0?AMBER:BRAND },
      { label:'Outstanding', value: fmtUSD(outstanding.total||0), sub:`${outstanding.n||0} invoice${outstanding.n!==1?'s':''}`, color:BRAND },
    ];

    const cont = el('fin-kpis');
    if (!cont) return;
    cont.innerHTML = tiles.map(t => `
      <div class="fin-card" style="cursor:default;">
        <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;">${t.label}</div>
        <div style="font-size:22px;font-weight:700;color:${DARK};letter-spacing:-0.02em;margin-bottom:4px;">${t.value}</div>
        <div style="font-size:10px;color:${t.color};">${t.sub}</div>
      </div>
    `).join('');
  } catch(e) { console.warn('[Finance] KPI load failed', e); }
}

// ═══════════════════════════════════════════════════════
// ── INVOICES TAB ──
// ═══════════════════════════════════════════════════════
async function renderInvoicesTab() {
  const cont = el('fin-tab-invoices');
  if (!cont) return;
  cont.innerHTML = `<div style="color:${LIGHT};font-size:12px;padding:20px;">Loading invoices…</div>`;

  try {
    // Week selector — last 12 weeks
    const weeks = [];
    const now = new Date();
    // Round down to Monday
    const day = now.getDay();
    const monday = new Date(now); monday.setDate(now.getDate() - ((day+6)%7));
    for (let i = 0; i < 12; i++) {
      const d = new Date(monday); d.setDate(monday.getDate() - i*7);
      weeks.push(d.toISOString().slice(0,10));
    }
    if (!_finState.week) _finState.week = weeks[0];

    const invoices = await api('/finance/invoices');
    _finState.invoices = invoices;

    cont.innerHTML = `
      <!-- Week selector -->
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="font-size:11px;color:${MID};">Week</span>
          <select id="fin-week-sel" onchange="window._finSelectWeek(this.value)" style="border:1px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 10px;font-size:12px;font-family:inherit;background:#fff;outline:none;cursor:pointer;color:${DARK};">
            ${weeks.map(w=>`<option value="${w}" ${w===_finState.week?'selected':''}>${weekLabel(w)}</option>`).join('')}
          </select>
        </div>
        <div style="font-size:10px;color:${LIGHT};">Click a card to create or edit an invoice</div>
      </div>
      <!-- Invoice cards grid -->
      <div id="fin-inv-grid" style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:32px;"></div>
      <!-- Recent invoices table -->
      <div class="fin-card">
        <div style="font-size:13px;font-weight:600;color:${DARK};margin-bottom:14px;">All Invoices</div>
        <div id="fin-inv-table"></div>
      </div>
    `;

    window._finSelectWeek = async function(w) {
      _finState.week = w;
      renderInvoiceGrid();
    };

    renderInvoiceGrid();
    renderInvoiceTable();
  } catch(e) {
    cont.innerHTML = `<div style="color:${BRAND};font-size:12px;padding:20px;">Failed to load: ${e.message}</div>`;
  }
}

async function renderInvoiceGrid() {
  const cont = el('fin-inv-grid');
  if (!cont) return;
  const ws = _finState.week;
  const weekInvs = _finState.invoices.filter(i => i.week_start === ws);

  const types = ['VAS','SEA','AIR'];
  cont.innerHTML = types.map(type => {
    const inv = weekInvs.find(i => i.type === type);
    if (inv) {
      return `
        <div class="fin-card" style="cursor:pointer;transition:box-shadow .15s;" onmouseenter="this.style.boxShadow='0 4px 20px rgba(0,0,0,0.08)'" onmouseleave="this.style.boxShadow='none'" onclick="window._finEditInvoice('${inv.id}')">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
            <div style="display:flex;align-items:center;gap:8px;">
              <span style="font-size:18px;">${typeIcon(type)}</span>
              <div>
                <div style="font-size:12px;font-weight:600;color:${DARK};">${type} Invoice</div>
                <div style="font-size:10px;color:${LIGHT};">${inv.ref_number}</div>
              </div>
            </div>
            ${statusBadge(inv.status)}
          </div>
          <div style="font-size:22px;font-weight:700;color:${DARK};margin-bottom:4px;">${fmtUSD(inv.total)}</div>
          <div style="font-size:10px;color:${MID};">Due: ${fmtDate(inv.due_date)}</div>
          <div style="margin-top:12px;display:flex;gap:6px;">
            <button class="fin-btn fin-btn-ghost" style="flex:1;font-size:11px;" onclick="event.stopPropagation();window._finEditInvoice('${inv.id}')">Edit</button>
            <button class="fin-btn fin-btn-ghost" style="flex:1;font-size:11px;" onclick="event.stopPropagation();window._finDownloadPDF('${inv.id}','${inv.ref_number}')">PDF</button>
          </div>
        </div>`;
    } else {
      return `
        <div class="fin-card" style="cursor:pointer;border:1.5px dashed rgba(0,0,0,0.1);transition:all .15s;" onmouseenter="this.style.borderColor='${BRAND}'" onmouseleave="this.style.borderColor='rgba(0,0,0,0.1)'" onclick="window._finCreateInvoice('${type}','${ws}')">
          <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px 0;gap:8px;">
            <span style="font-size:28px;">${typeIcon(type)}</span>
            <div style="font-size:12px;font-weight:600;color:${MID};">${type} Invoice</div>
            <div style="font-size:10px;color:${LIGHT};">Click to create</div>
            <div style="margin-top:8px;background:${BRAND};color:#fff;border-radius:20px;padding:4px 14px;font-size:11px;font-weight:600;">+ New</div>
          </div>
        </div>`;
    }
  }).join('');
}

function renderInvoiceTable() {
  const cont = el('fin-inv-table');
  if (!cont) return;
  const invs = [..._finState.invoices].sort((a,b) => b.week_start.localeCompare(a.week_start));
  if (!invs.length) { cont.innerHTML = `<div style="font-size:11px;color:${LIGHT};padding:16px 0;">No invoices yet.</div>`; return; }
  cont.innerHTML = `
    <table class="fin-tbl">
      <thead><tr>
        <th>Ref</th><th>Type</th><th>Week</th><th>Date</th><th>Due</th><th>Amount</th><th>Status</th><th></th>
      </tr></thead>
      <tbody>
        ${invs.map(i=>`<tr>
          <td style="font-weight:500;font-size:11px;">${esc(i.ref_number)}</td>
          <td>${typeIcon(i.type)} ${i.type}</td>
          <td style="color:${MID};font-size:11px;">${weekLabel(i.week_start)}</td>
          <td style="color:${MID};font-size:11px;">${fmtDate(i.invoice_date)}</td>
          <td style="color:${MID};font-size:11px;">${fmtDate(i.due_date)}</td>
          <td style="font-weight:600;">${fmtUSD(i.total)}</td>
          <td>${statusBadge(i.status)}</td>
          <td>
            <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:4px 10px;" onclick="window._finEditInvoice('${i.id}')">Edit</button>
            <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:4px 10px;" onclick="window._finDownloadPDF('${i.id}','${i.ref_number}')">PDF</button>
          </td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

// ── Create new invoice — prefill from Pinpoint data ──
window._finCreateInvoice = async function(type, weekStart) {
  openPanel(`<div style="color:${LIGHT};font-size:12px;padding:20px;">Loading ${type} data for ${weekLabel(weekStart)}…</div>`);
  try {
    const prefill = await api(`/finance/prefill/${type}/${weekStart}`);
    const today = isoToday();
    const dueDate = addDays(today, type==='VAS' ? 30 : 7);
    renderInvoiceEditor({
      id: null, type, week_start: weekStart,
      invoice_date: today, due_date: dueDate,
      status: 'draft', notes: '',
      subtotal: prefill.subtotal||0,
      gst: prefill.gst||0,
      customs: prefill.customs||0,
      misc_total: 0,
      total: prefill.total||0,
      lines: prefill.lines||[],
      _prefill: prefill,
    });
  } catch(e) {
    openPanel(`<div style="color:${BRAND};padding:20px;">Error: ${e.message}</div>`);
  }
};

// ── Edit existing invoice ──
window._finEditInvoice = async function(id) {
  openPanel(`<div style="color:${LIGHT};font-size:12px;padding:20px;">Loading…</div>`);
  try {
    const inv = await api(`/finance/invoices/${id}`);
    renderInvoiceEditor(inv);
  } catch(e) {
    openPanel(`<div style="color:${BRAND};padding:20px;">Error: ${e.message}</div>`);
  }
};

// ── Invoice editor panel ──
function renderInvoiceEditor(inv) {
  const isNew = !inv.id;
  const type  = inv.type;
  const lines = inv.lines || [];
  const mainLines = lines.filter(l => !l.gst_free && !l.is_misc);
  const customsLines = lines.filter(l => l.gst_free && !l.is_misc);
  const miscLines = lines.filter(l => l.is_misc);

  // Ensure 2 misc lines
  while (miscLines.length < 2) miscLines.push({ description:'', unit_label:'', rate:0, quantity:0, total:0, gst_free:0, is_misc:1 });

  function lineRow(l, idx, editable=true) {
    const isVAS = type === 'VAS';
    if (isVAS) {
      return `<tr>
        <td style="font-size:11px;color:${DARK};padding:7px 8px;">${esc(l.description)}</td>
        <td style="font-size:11px;color:${MID};padding:7px 8px;">${esc(l.unit_label)}</td>
        <td style="padding:4px 6px;"><input class="fin-input" style="width:65px;text-align:right;" data-field="rate" data-idx="${idx}" value="${l.rate||0}" oninput="window._finLineChange(${idx})"/></td>
        <td style="padding:4px 6px;"><input class="fin-input" style="width:75px;text-align:right;" data-field="quantity" data-idx="${idx}" value="${l.quantity||0}" oninput="window._finLineChange(${idx})"/></td>
        <td style="font-size:11px;font-weight:600;color:${DARK};padding:7px 8px;text-align:right;" id="fin-line-total-${idx}">${fmtUSD(l.total)}</td>
      </tr>`;
    } else {
      // Sea/Air — desc editable, rate editable
      return `<tr>
        <td style="padding:4px 6px;"><input class="fin-input" style="font-size:11px;" data-field="description" data-idx="${idx}" value="${esc(l.description)}" oninput="window._finLineChange(${idx})"/></td>
        <td style="padding:4px 6px;"><input class="fin-input" style="width:70px;text-align:right;" data-field="rate" data-idx="${idx}" value="${l.rate||0}" oninput="window._finLineChange(${idx})"/></td>
        <td style="padding:4px 6px;"><input class="fin-input" style="width:65px;text-align:right;" data-field="quantity" data-idx="${idx}" value="${l.quantity||0}" oninput="window._finLineChange(${idx})"/></td>
        <td style="font-size:11px;font-weight:600;color:${DARK};padding:7px 8px;text-align:right;" id="fin-line-total-${idx}">${fmtUSD(l.total)}</td>
      </tr>`;
    }
  }

  function miscRow(l, idx) {
    return `<tr>
      <td colspan="${type==='VAS'?2:2}" style="padding:4px 6px;">
        <input class="fin-input" placeholder="Description (optional)" data-field="description" data-idx="${idx}" value="${esc(l.description)}" oninput="window._finLineChange(${idx})"/>
      </td>
      <td style="padding:4px 6px;"><input class="fin-input" style="width:90px;text-align:right;" data-field="rate" data-idx="${idx}" value="${l.rate||0}" oninput="window._finLineChange(${idx})"/></td>
      <td style="padding:4px 6px;"><input class="fin-input" style="width:65px;text-align:right;" data-field="quantity" data-idx="${idx}" value="${l.quantity||0}" oninput="window._finLineChange(${idx})"/></td>
      <td style="font-size:11px;font-weight:600;color:${DARK};padding:7px 8px;text-align:right;" id="fin-line-total-${idx}">${fmtUSD(l.total)}</td>
    </tr>`;
  }

  const allLines = [...mainLines, ...miscLines, ...customsLines];

  const html = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
      <div>
        <div style="font-size:15px;font-weight:700;color:${DARK};">${typeIcon(type)} ${type} Invoice</div>
        <div style="font-size:11px;color:${LIGHT};">${weekLabel(inv.week_start)}</div>
      </div>
      <button onclick="window._finClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;">✕</button>
    </div>

    <!-- Ref + Status -->
    <div style="display:flex;gap:10px;margin-bottom:16px;">
      <div style="flex:1;">
        <div class="fin-label">Reference</div>
        <div style="font-size:12px;font-weight:600;color:${DARK};padding:8px 10px;background:${BG};border-radius:8px;">${esc(inv.ref_number||'Will be generated')}</div>
      </div>
      <div>
        <div class="fin-label">Status</div>
        <select id="fin-inv-status" class="fin-input" style="width:110px;">
          ${['draft','sent','paid','overdue'].map(s=>`<option value="${s}" ${s===inv.status?'selected':''}>${s.charAt(0).toUpperCase()+s.slice(1)}</option>`).join('')}
        </select>
      </div>
    </div>

    <!-- Dates -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px;">
      <div>
        <div class="fin-label">Invoice Date</div>
        <input id="fin-inv-date" type="date" class="fin-input" value="${inv.invoice_date||isoToday()}"/>
      </div>
      <div>
        <div class="fin-label">Due Date</div>
        <input id="fin-inv-due" type="date" class="fin-input" value="${inv.due_date||addDays(isoToday(),30)}"/>
      </div>
    </div>

    <!-- Line items -->
    <div class="fin-label" style="margin-bottom:6px;">Line Items</div>
    <div style="border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;overflow:hidden;margin-bottom:12px;">
      <table style="width:100%;border-collapse:collapse;" id="fin-lines-tbl">
        <thead style="background:${BG};">
          <tr>
            ${type==='VAS'
              ? `<th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;text-align:left;">Service</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;">Unit</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;">Rate</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;">Qty</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;text-align:right;">Total</th>`
              : `<th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;text-align:left;">Description</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;">Rate (USD)</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;">Qty / KG</th>
                 <th style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:7px 8px;text-align:right;">Total</th>`
            }
          </tr>
        </thead>
        <tbody>
          ${mainLines.map((l,i) => lineRow(l,i)).join('')}
          ${miscLines.length ? `<tr><td colspan="5" style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:5px 8px;background:${BG};border-top:0.5px solid rgba(0,0,0,0.05);">Miscellaneous</td></tr>` : ''}
          ${miscLines.map((l,i) => miscRow(l, mainLines.length+i)).join('')}
          ${customsLines.length ? `<tr><td colspan="5" style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;padding:5px 8px;background:${BG};border-top:0.5px solid rgba(0,0,0,0.05);">Customs / GST-free</td></tr>` : ''}
          ${customsLines.map((l,i) => lineRow(l, mainLines.length+miscLines.length+i)).join('')}
        </tbody>
      </table>
    </div>

    <!-- Totals -->
    <div style="background:${BG};border-radius:10px;padding:12px 14px;margin-bottom:16px;">
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:5px;">
        <span>Subtotal (excl. GST)</span><span id="fin-tot-subtotal" style="font-weight:500;color:${DARK};">${fmtUSD(inv.subtotal)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:5px;">
        <span>GST (10%)</span><span id="fin-tot-gst" style="font-weight:500;color:${DARK};">${fmtUSD(inv.gst)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:8px;">
        <span>Customs / GST-free</span><span id="fin-tot-customs" style="font-weight:500;color:${DARK};">${fmtUSD(inv.customs)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:700;color:${DARK};border-top:0.5px solid rgba(0,0,0,0.08);padding-top:8px;">
        <span>Total Payable</span><span id="fin-tot-total">${fmtUSD(inv.total)}</span>
      </div>
    </div>

    <!-- Notes -->
    <div class="fin-label" style="margin-bottom:5px;">Notes (optional)</div>
    <textarea id="fin-inv-notes" class="fin-input" rows="2" style="resize:vertical;">${esc(inv.notes||'')}</textarea>

    <!-- Actions -->
    <div style="display:flex;gap:8px;margin-top:16px;">
      <button class="fin-btn fin-btn-primary" style="flex:1;" onclick="window._finSaveInvoice(${isNew?'null':`'${inv.id}'`},'${type}','${inv.week_start}')">
        ${isNew ? 'Create Invoice' : 'Save Changes'}
      </button>
      ${!isNew ? `<button class="fin-btn fin-btn-ghost" onclick="window._finDownloadPDF('${inv.id}','${inv.ref_number||'invoice'}')">⬇ PDF</button>` : ''}
      <button class="fin-btn fin-btn-ghost" onclick="window._finClosePanel()">Cancel</button>
    </div>
    ${!isNew ? `<div style="margin-top:12px;text-align:center;"><button onclick="window._finDeleteInvoice('${inv.id}')" style="background:none;border:none;color:${LIGHT};font-size:11px;cursor:pointer;">Delete invoice</button></div>` : ''}
  `;

  openPanel(html);

  // Store lines in memory for live recalc
  window._finCurrentLines = allLines.map(l=>({...l}));
  window._finCurrentType = type;

  // Wire up line change handler
  window._finLineChange = function(idx) {
    const tbl = el('fin-lines-tbl');
    if (!tbl) return;
    const row = tbl.querySelectorAll('tbody tr')[idx];
    if (!row) return;
    const descInp = row.querySelector('[data-field="description"]');
    const rateInp = row.querySelector('[data-field="rate"]');
    const qtyInp  = row.querySelector('[data-field="quantity"]');
    const rate = parseFloat(rateInp?.value||0);
    const qty  = parseFloat(qtyInp?.value||0);
    const total = Math.round(rate*qty*100)/100;
    if (descInp) window._finCurrentLines[idx].description = descInp.value;
    if (rateInp) window._finCurrentLines[idx].rate = rate;
    if (qtyInp)  window._finCurrentLines[idx].quantity = qty;
    window._finCurrentLines[idx].total = total;
    const totEl = el(`fin-line-total-${idx}`);
    if (totEl) totEl.textContent = fmtUSD(total);
    recalcTotals();
  };

  function recalcTotals() {
    const lines = window._finCurrentLines || [];
    const mainTotal = lines.filter(l=>!l.gst_free&&!l.is_misc).reduce((s,l)=>s+(parseFloat(l.total)||0),0);
    const miscTotal = lines.filter(l=>l.is_misc).reduce((s,l=>s+(parseFloat(l.total)||0)),0);
    const customsTotal = lines.filter(l=>l.gst_free&&!l.is_misc).reduce((s,l)=>s+(parseFloat(l.total)||0),0);
    const subtotal = Math.round((mainTotal+miscTotal)*100)/100;
    const gst = Math.round(subtotal*0.10*100)/100;
    const total = Math.round((subtotal+gst+customsTotal)*100)/100;
    const s = el('fin-tot-subtotal'); if(s) s.textContent = fmtUSD(subtotal);
    const g = el('fin-tot-gst');      if(g) g.textContent = fmtUSD(gst);
    const c = el('fin-tot-customs');  if(c) c.textContent = fmtUSD(customsTotal);
    const t = el('fin-tot-total');    if(t) t.textContent = fmtUSD(total);
  }
}

window._finSaveInvoice = async function(id, type, weekStart) {
  const lines = window._finCurrentLines || [];
  const customsLines = lines.filter(l=>l.gst_free&&!l.is_misc);
  const mainLines = lines.filter(l=>!l.gst_free&&!l.is_misc);
  const miscLines = lines.filter(l=>l.is_misc&&l.description);
  const allSave = [...mainLines, ...miscLines, ...customsLines];
  const customs = customsLines.reduce((s,l)=>s+(parseFloat(l.total)||0),0);
  const misc_total = miscLines.reduce((s,l)=>s+(parseFloat(l.total)||0),0);
  const payload = {
    type, week_start: weekStart,
    invoice_date: el('fin-inv-date')?.value || isoToday(),
    due_date: el('fin-inv-due')?.value || '',
    status: el('fin-inv-status')?.value || 'draft',
    notes: el('fin-inv-notes')?.value || '',
    customs, misc_total,
    lines: allSave,
  };
  try {
    const saved = id
      ? await api(`/finance/invoices/${id}`, { method:'PATCH', body: JSON.stringify(payload) })
      : await api('/finance/invoices', { method:'POST', body: JSON.stringify(payload) });
    // Refresh
    const invs = await api('/finance/invoices');
    _finState.invoices = invs;
    window._finClosePanel();
    renderInvoiceGrid();
    renderInvoiceTable();
    renderKPIs();
  } catch(e) { alert('Save failed: ' + e.message); }
};

window._finDownloadPDF = function(id, ref) {
  const url = _apiBase + `/finance/invoice/${id}/pdf`;
  const a = document.createElement('a');
  a.href = url;
  a.download = (ref||'invoice') + '.pdf';
  document.body.appendChild(a);
  // Need auth token in URL for PDF download
  if (_token) a.href = url + '?_token=' + encodeURIComponent(_token);
  a.click();
  a.remove();
};

window._finDeleteInvoice = async function(id) {
  if (!confirm('Delete this invoice? This cannot be undone.')) return;
  try {
    await api(`/finance/invoices/${id}`, { method:'DELETE' });
    _finState.invoices = _finState.invoices.filter(i=>i.id!==id);
    window._finClosePanel();
    renderInvoiceGrid();
    renderInvoiceTable();
    renderKPIs();
  } catch(e) { alert('Delete failed: '+e.message); }
};

// ═══════════════════════════════════════════════════════
// ── P&L TAB ──
// ═══════════════════════════════════════════════════════
async function renderPLTab() {
  const cont = el('fin-tab-pl');
  if (!cont) return;
  cont.innerHTML = `<div style="color:${LIGHT};font-size:12px;padding:20px;">Loading P&L…</div>`;

  try {
    const year = new Date().getUTCFullYear();
    const pl = await api(`/finance/pl?year=${year}`);
    _finState.pl = pl;

    const ytd = pl.ytd || {};
    const months = pl.months || [];

    cont.innerHTML = `
      <!-- Year selector -->
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:20px;">
        <span style="font-size:11px;color:${MID};">Year</span>
        <select id="fin-pl-year" onchange="window._finPLYear(this.value)" style="border:1px solid rgba(0,0,0,0.12);border-radius:8px;padding:6px 10px;font-size:12px;font-family:inherit;background:#fff;outline:none;cursor:pointer;">
          ${[year-1,year,year+1].map(y=>`<option value="${y}" ${y===year?'selected':''}>${y}</option>`).join('')}
        </select>
        <button class="fin-btn fin-btn-ghost" style="margin-left:auto;" onclick="window._finAddExpense()">+ Add Expense</button>
      </div>

      <!-- YTD summary -->
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:24px;">
        <div class="fin-card">
          <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">YTD Revenue</div>
          <div style="font-size:24px;font-weight:700;color:${DARK};">${fmtUSD(ytd.revenue)}</div>
        </div>
        <div class="fin-card">
          <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">YTD Expenses</div>
          <div style="font-size:24px;font-weight:700;color:${DARK};">${fmtUSD(ytd.expenses)}</div>
        </div>
        <div class="fin-card">
          <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">YTD Net Margin</div>
          <div style="font-size:24px;font-weight:700;color:${ytd.margin_pct>20?GREEN:ytd.margin_pct>0?AMBER:BRAND};">${ytd.margin_pct}%</div>
          <div style="font-size:10px;color:${MID};">${fmtUSD(ytd.net)}</div>
        </div>
      </div>

      <!-- Charts row -->
      <div style="display:grid;grid-template-columns:3fr 2fr;gap:16px;margin-bottom:24px;">
        <div class="fin-card">
          <div style="font-size:12px;font-weight:600;color:${DARK};margin-bottom:14px;">Revenue vs Expenses</div>
          <canvas id="fin-chart-rev" height="160"></canvas>
        </div>
        <div class="fin-card">
          <div style="font-size:12px;font-weight:600;color:${DARK};margin-bottom:14px;">Expense Breakdown</div>
          <canvas id="fin-chart-exp" height="160"></canvas>
        </div>
      </div>

      <!-- Monthly table -->
      <div class="fin-card">
        <div style="font-size:13px;font-weight:600;color:${DARK};margin-bottom:14px;">Monthly Breakdown</div>
        <table class="fin-tbl" id="fin-pl-tbl">
          <thead><tr>
            <th>Month</th><th>Revenue</th><th>Expenses</th><th>Net</th><th>Margin</th><th></th>
          </tr></thead>
          <tbody>
            ${months.map(m => {
              const hasData = m.revenue > 0 || m.expenses > 0;
              const monthName = new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'long',year:'numeric'});
              return `<tr id="fin-pl-row-${m.month_key}">
                <td style="font-weight:500;">${monthName}</td>
                <td style="color:${GREEN};font-weight:${m.revenue>0?600:400};">${m.revenue>0?fmtUSD(m.revenue):'—'}</td>
                <td style="color:${m.expenses>0?AMBER:LIGHT};font-weight:${m.expenses>0?600:400};">${m.expenses>0?fmtUSD(m.expenses):'—'}</td>
                <td style="font-weight:600;color:${m.net>0?GREEN:m.net<0?BRAND:LIGHT};">${hasData?fmtUSD(m.net):'—'}</td>
                <td>${hasData?`<span style="color:${m.margin_pct>20?GREEN:m.margin_pct>0?AMBER:BRAND};font-weight:600;">${m.margin_pct}%</span>`:'—'}</td>
                <td>
                  <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 8px;" onclick="window._finExpandMonth('${m.month_key}')">Detail</button>
                  <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 8px;" onclick="window._finAddExpense('${m.month_key}')">+ Expense</button>
                </td>
              </tr>
              <tr id="fin-pl-detail-${m.month_key}" style="display:none;background:${BG};">
                <td colspan="6" style="padding:0;">
                  <div style="padding:12px 16px;" id="fin-pl-detail-content-${m.month_key}"></div>
                </td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
      </div>
    `;

    // Render charts
    renderPLCharts(months);

    window._finPLYear = async function(y) {
      const newPL = await api(`/finance/pl?year=${y}`);
      _finState.pl = newPL;
      renderPLTab();
    };

    window._finExpandMonth = async function(mk) {
      const row = el(`fin-pl-detail-${mk}`);
      const cont2 = el(`fin-pl-detail-content-${mk}`);
      if (!row || !cont2) return;
      if (row.style.display === 'none') {
        row.style.display = '';
        // Load expenses for this month
        const exps = await api(`/finance/expenses?month_key=${mk}`);
        const m = (_finState.pl?.months||[]).find(m=>m.month_key===mk);
        const invList = (m?.invoices||[]);
        cont2.innerHTML = `
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
            <div>
              <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;margin-bottom:8px;">Invoices</div>
              ${invList.length ? invList.map(i=>`<div style="display:flex;justify-content:space-between;font-size:11px;padding:4px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <span style="color:${MID};">${i.type} — ${i.ref||''}</span>
                <span style="font-weight:600;color:${GREEN};">${fmtUSD(i.amount)}</span>
              </div>`).join('') : `<div style="font-size:11px;color:${LIGHT};">No invoices</div>`}
            </div>
            <div>
              <div style="font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;margin-bottom:8px;">Expenses</div>
              ${exps.length ? exps.map(e=>`<div style="display:flex;justify-content:space-between;font-size:11px;padding:4px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <div>
                  <span style="color:${DARK};">${esc(e.description)}</span>
                  <span style="color:${LIGHT};margin-left:6px;">${e.category}</span>
                  ${e.is_recurring?`<span style="color:${BLUE};margin-left:4px;font-size:9px;">↻ recurring</span>`:''}
                </div>
                <div style="display:flex;align-items:center;gap:8px;">
                  <span style="font-weight:600;color:${AMBER};">${fmtUSD(e.amount)}</span>
                  <button onclick="window._finEditExpense('${e.id}')" style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:11px;">✎</button>
                </div>
              </div>`).join('') : `<div style="font-size:11px;color:${LIGHT};">No expenses</div>`}
            </div>
          </div>`;
      } else {
        row.style.display = 'none';
      }
    };

  } catch(e) {
    cont.innerHTML = `<div style="color:${BRAND};padding:20px;">Failed to load P&L: ${e.message}</div>`;
  }
}

function renderPLCharts(months) {
  const revEl = el('fin-chart-rev');
  const expEl = el('fin-chart-exp');
  if (!revEl || !expEl || !window.Chart) {
    // Load Chart.js if needed
    if (!window.Chart) {
      const s = document.createElement('script');
      s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';
      s.onload = () => renderPLCharts(months);
      document.head.appendChild(s);
      return;
    }
    return;
  }

  const labels = months.map(m => new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short'}));
  const revenues = months.map(m=>m.revenue);
  const expenses = months.map(m=>m.expenses);

  // Destroy existing
  if (window._finChartRev) { window._finChartRev.destroy(); }
  if (window._finChartExp) { window._finChartExp.destroy(); }

  window._finChartRev = new Chart(revEl, {
    type:'bar',
    data:{
      labels,
      datasets:[
        { label:'Revenue', data:revenues, backgroundColor:'rgba(52,199,89,0.7)', borderRadius:4 },
        { label:'Expenses', data:expenses, backgroundColor:'rgba(200,134,10,0.6)', borderRadius:4 },
      ]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{ position:'top', align:'end', labels:{ font:{size:10}, boxWidth:8 } } },
      scales:{
        x:{ ticks:{font:{size:10}}, grid:{display:false} },
        y:{ ticks:{font:{size:10}, callback:v=>'$'+v.toLocaleString()}, grid:{color:'rgba(0,0,0,0.04)'}, beginAtZero:true }
      }
    }
  });

  // Expense breakdown donut
  const expByMonth = months.reduce((acc, m) => { acc += m.expenses; return acc; }, 0);
  const nonZero = months.filter(m=>m.expenses>0);
  if (nonZero.length > 0) {
    window._finChartExp = new Chart(expEl, {
      type:'doughnut',
      data:{
        labels: nonZero.map(m=>new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short'})),
        datasets:[{
          data: nonZero.map(m=>m.expenses),
          backgroundColor:['#990033','#C8860A','#4A9B8E','#3B82F6','#8B5CF6','#F97316','#059669','#6B7280','#EC4899','#EAB308','#14B8A6','#F43F5E'],
          borderWidth:2, borderColor:'#fff',
        }]
      },
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ position:'bottom', labels:{ font:{size:9}, boxWidth:8 } } }
      }
    });
  }
}

// ═══════════════════════════════════════════════════════
// ── EXPENSES TAB ──
// ═══════════════════════════════════════════════════════
async function renderExpensesTab() {
  const cont = el('fin-tab-expenses');
  if (!cont) return;
  cont.innerHTML = `<div style="color:${LIGHT};font-size:12px;padding:20px;">Loading expenses…</div>`;

  try {
    const expenses = await api('/finance/expenses');
    _finState.expenses = expenses;

    // Category totals
    const catTotals = {};
    for (const e of expenses) {
      catTotals[e.category] = (catTotals[e.category]||0) + parseFloat(e.amount||0);
    }

    cont.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
        <div style="font-size:13px;font-weight:600;color:${DARK};">All Expenses</div>
        <button class="fin-btn fin-btn-primary" onclick="window._finAddExpense()">+ Add Expense</button>
      </div>

      <!-- Category tiles -->
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px;">
        ${EXPENSE_CATS.map(cat => {
          const total = catTotals[cat]||0;
          return `<div class="fin-card" style="padding:14px;cursor:pointer;" onclick="window._finFilterCat('${cat}')">
            <div style="font-size:10px;font-weight:600;color:${LIGHT};margin-bottom:6px;">${cat}</div>
            <div style="font-size:16px;font-weight:700;color:${total>0?DARK:LIGHT};">${total>0?fmtUSD(total):'—'}</div>
          </div>`;
        }).join('')}
      </div>

      <!-- Expense list -->
      <div class="fin-card">
        <div id="fin-exp-list"></div>
      </div>
    `;

    renderExpenseList(expenses);

    window._finFilterCat = async function(cat) {
      const filtered = await api(`/finance/expenses?category=${encodeURIComponent(cat)}`);
      renderExpenseList(filtered);
    };

  } catch(e) {
    cont.innerHTML = `<div style="color:${BRAND};padding:20px;">Failed: ${e.message}</div>`;
  }
}

function renderExpenseList(expenses) {
  const cont = el('fin-exp-list');
  if (!cont) return;
  if (!expenses.length) { cont.innerHTML = `<div style="font-size:11px;color:${LIGHT};padding:16px 0;">No expenses yet.</div>`; return; }
  cont.innerHTML = `
    <table class="fin-tbl">
      <thead><tr>
        <th>Description</th><th>Category</th><th>Date</th><th>Amount</th><th>Recurring</th><th></th>
      </tr></thead>
      <tbody>
        ${expenses.map(e=>`<tr>
          <td style="font-weight:500;">${esc(e.description)}</td>
          <td style="color:${MID};">${esc(e.category)}</td>
          <td style="color:${MID};font-size:11px;">${fmtDate(e.expense_date)}</td>
          <td style="font-weight:600;color:${AMBER};">${fmtUSD(e.amount, e.currency)}</td>
          <td>${e.is_recurring?`<span style="color:${BLUE};font-size:10px;">↻ ${e.recur_freq||'monthly'}</span>`:'—'}</td>
          <td>
            <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 8px;" onclick="window._finEditExpense('${e.id}')">Edit</button>
            <button style="background:none;border:none;color:${LIGHT};font-size:11px;cursor:pointer;" onclick="window._finDeleteExpense('${e.id}')">✕</button>
          </td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

// ── Add / Edit expense panel ──
window._finAddExpense = function(defaultMonth) {
  const today = isoToday();
  const defDate = defaultMonth ? defaultMonth + '-01' : today;
  renderExpenseEditor({ id:null, category:'Other', description:'', amount:0, currency:'USD', expense_date:defDate, is_recurring:0, recur_freq:'monthly', recur_end:'' });
};

window._finEditExpense = async function(id) {
  const exp = _finState.expenses.find(e=>e.id===id) || await api('/finance/expenses').then(r=>r.find(e=>e.id===id));
  if (exp) renderExpenseEditor(exp);
};

function renderExpenseEditor(exp) {
  const isNew = !exp.id;
  openPanel(`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
      <div style="font-size:15px;font-weight:700;color:${DARK};">${isNew?'New Expense':'Edit Expense'}</div>
      <button onclick="window._finClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;">✕</button>
    </div>

    <div style="margin-bottom:12px;">
      <div class="fin-label">Category</div>
      <select id="exp-cat" class="fin-input">
        ${EXPENSE_CATS.map(c=>`<option value="${c}" ${c===exp.category?'selected':''}>${c}</option>`).join('')}
      </select>
    </div>
    <div style="margin-bottom:12px;">
      <div class="fin-label">Description</div>
      <input id="exp-desc" class="fin-input" value="${esc(exp.description)}" placeholder="e.g. Sea freight - COSCO"/>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">
      <div>
        <div class="fin-label">Amount</div>
        <input id="exp-amount" type="number" step="0.01" class="fin-input" value="${exp.amount||0}"/>
      </div>
      <div>
        <div class="fin-label">Currency</div>
        <select id="exp-currency" class="fin-input">
          ${['USD','AUD','EUR','GBP'].map(c=>`<option value="${c}" ${c===exp.currency?'selected':''}>${c}</option>`).join('')}
        </select>
      </div>
    </div>
    <div style="margin-bottom:12px;">
      <div class="fin-label">Date</div>
      <input id="exp-date" type="date" class="fin-input" value="${exp.expense_date||isoToday()}"/>
    </div>
    <div style="margin-bottom:12px;">
      <label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
        <input type="checkbox" id="exp-recur" ${exp.is_recurring?'checked':''} style="width:14px;height:14px;accent-color:${BRAND};" onchange="document.getElementById('exp-recur-opts').style.display=this.checked?'':'none'"/>
        <span style="font-size:12px;color:${DARK};">Recurring expense</span>
      </label>
    </div>
    <div id="exp-recur-opts" style="display:${exp.is_recurring?'':'none'};margin-bottom:12px;padding:12px;background:${BG};border-radius:8px;">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div>
          <div class="fin-label">Frequency</div>
          <select id="exp-recur-freq" class="fin-input">
            <option value="monthly" ${exp.recur_freq==='monthly'?'selected':''}>Monthly</option>
            <option value="quarterly" ${exp.recur_freq==='quarterly'?'selected':''}>Quarterly</option>
            <option value="annually" ${exp.recur_freq==='annually'?'selected':''}>Annually</option>
          </select>
        </div>
        <div>
          <div class="fin-label">End Date (optional)</div>
          <input id="exp-recur-end" type="date" class="fin-input" value="${exp.recur_end||''}"/>
        </div>
      </div>
    </div>

    <div style="display:flex;gap:8px;margin-top:20px;">
      <button class="fin-btn fin-btn-primary" style="flex:1;" onclick="window._finSaveExpense('${exp.id||''}')">
        ${isNew?'Add Expense':'Save Changes'}
      </button>
      <button class="fin-btn fin-btn-ghost" onclick="window._finClosePanel()">Cancel</button>
    </div>
    ${!isNew?`<div style="margin-top:12px;text-align:center;"><button onclick="window._finDeleteExpense('${exp.id}')" style="background:none;border:none;color:${LIGHT};font-size:11px;cursor:pointer;">Delete expense</button></div>`:''}
  `);
}

window._finSaveExpense = async function(id) {
  const payload = {
    category: el('exp-cat')?.value,
    description: el('exp-desc')?.value,
    amount: parseFloat(el('exp-amount')?.value||0),
    currency: el('exp-currency')?.value||'USD',
    expense_date: el('exp-date')?.value,
    is_recurring: el('exp-recur')?.checked ? 1 : 0,
    recur_freq: el('exp-recur-freq')?.value||null,
    recur_end: el('exp-recur-end')?.value||null,
  };
  try {
    if (id) {
      await api(`/finance/expenses/${id}`, { method:'PATCH', body: JSON.stringify(payload) });
    } else {
      await api('/finance/expenses', { method:'POST', body: JSON.stringify(payload) });
    }
    window._finClosePanel();
    renderExpensesTab();
    if (_finState.tab === 'pl') renderPLTab();
    renderKPIs();
  } catch(e) { alert('Save failed: '+e.message); }
};

window._finDeleteExpense = async function(id) {
  if (!confirm('Delete this expense and any recurring copies?')) return;
  try {
    await api(`/finance/expenses/${id}`, { method:'DELETE' });
    window._finClosePanel();
    renderExpensesTab();
    renderKPIs();
  } catch(e) { alert('Delete failed: '+e.message); }
};

// ═══════════════════════════════════════════════════════
// ── PAGE INIT ──
// ═══════════════════════════════════════════════════════
window.showFinancePage = async function() {
  const main = document.querySelector('main.vo-wrap') || document.querySelector('main') || document.body;

  // Get or create page-finance
  let page = document.getElementById('page-finance');
  if (!page) {
    page = document.createElement('section');
    page.id = 'page-finance';
    main.appendChild(page);
  }

  // Hide all other pages
  ['page-dashboard','page-intake','page-exec','page-receiving','page-reports','page-map'].forEach(id => {
    const el2 = document.getElementById(id);
    if (el2) { el2.classList.add('hidden'); el2.style.display='none'; }
  });
  if (typeof window.hideReceivingPage === 'function') window.hideReceivingPage();

  page.classList.remove('hidden');
  page.style.display = 'block';

  // Get API base + token
  _apiBase = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
  if (window.Clerk?.session) {
    try { _token = await window.Clerk.session.getToken(); } catch {}
  }

  // Init page if not already done
  if (!page.dataset.init) {
    page.dataset.init = '1';
    injectSkeleton(page);
    await fetchFX();
    // FX label
    const fxEl = el('fin-fx-label');
    if (fxEl) fxEl.textContent = _finState.fxLastUpdated ? 'Rates: live' : 'Rates: manual';
    // Currency switcher
    const sel = el('fin-currency');
    if (sel) sel.addEventListener('change', e => {
      _finState.currency = e.target.value;
      if (_finState.tab === 'invoices') renderInvoiceTable();
      if (_finState.tab === 'pl') renderPLTab();
      if (_finState.tab === 'expenses') renderExpensesTab();
      renderKPIs();
    });
  }

  await renderKPIs();
  window._finTab(_finState.tab || 'invoices');
};

})();
