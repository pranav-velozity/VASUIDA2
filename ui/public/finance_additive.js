/* ── VelOzity Pinpoint — Finance Module v2 ── */
;(function(){
'use strict';

const BRAND='#990033',DARK='#1C1C1E',MID='#6E6E73',LIGHT='#AEAEB2';
const BG='#F5F5F7',GREEN='#34C759',AMBER='#C8860A',BLUE='#3B82F6';
const EXPENSE_CATS=['VAS Cost','Sea Freight Cost','Air Freight Cost','Internal Overhead – Salaries','Internal Overhead – Software','Internal Overhead – Office','Internal Overhead – Other','Direct Labour','Duties & Customs','Storage','Marketing','Other'];
const EXPENSE_CAT_GROUPS={'Operations':['VAS Cost','Sea Freight Cost','Air Freight Cost','Direct Labour'],'Internal Overhead':['Internal Overhead – Salaries','Internal Overhead – Software','Internal Overhead – Office','Internal Overhead – Other'],'Other':['Duties & Customs','Storage','Marketing','Other']};

let _apiBase='',_finState={tab:'invoices',week:'',invoices:[],expenses:[],pl:null,fxRates:{USD:1},fxLabel:'',currency:'USD'};

async function getToken(){
  if(window.Clerk?.session){try{return await window.Clerk.session.getToken();}catch{}}
  return null;
}
async function api(path,opts={}){
  const token=await getToken();
  const headers={'Content-Type':'application/json',...(opts.headers||{})};
  if(token)headers['Authorization']='Bearer '+token;
  const r=await fetch(_apiBase+path,{...opts,headers});
  if(!r.ok){const t=await r.text();throw new Error('HTTP '+r.status+' '+t);}
  return r.json();
}

function fmtUSD(v){
  const c=_finState.currency||'USD';
  const rate=_finState.fxRates[c]||1;
  const f=(parseFloat(v)||0)*(c==='USD'?1:rate);
  return new Intl.NumberFormat('en-US',{style:'currency',currency:c,minimumFractionDigits:2}).format(f);
}
function fmtDate(s){
  if(!s)return'—';
  try{return new Date(s.slice(0,10)+'T00:00:00Z').toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'});}catch{return s;}
}
function isoToday(){return new Date().toISOString().slice(0,10);}
function addDays(iso,n){const d=new Date(iso.slice(0,10)+'T00:00:00Z');d.setUTCDate(d.getUTCDate()+n);return d.toISOString().slice(0,10);}
function safeDate(s){return s?String(s).slice(0,10):'';}
function getISOWeekNum(ws){
  try{
    const d=new Date(ws.slice(0,10)+'T00:00:00Z');
    const tmp=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate()));
    const day=tmp.getUTCDay()||7;
    tmp.setUTCDate(tmp.getUTCDate()+4-day);
    const yearStart=new Date(Date.UTC(tmp.getUTCFullYear(),0,1));
    return Math.ceil((((tmp-yearStart)/86400000)+1)/7);
  }catch{return 0;}
}
function weekLabel(ws){
  try{
    const d=new Date(ws.slice(0,10)+'T00:00:00Z');
    const wkNum=getISOWeekNum(ws);
    const dateStr=d.toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'});
    return`W${wkNum} · ${dateStr}`;
  }catch{return ws;}
}
function statusBadge(status){
  const cfg={draft:{bg:'rgba(174,174,178,0.15)',color:MID,label:'Draft'},sent:{bg:'rgba(50,130,246,0.12)',color:BLUE,label:'Sent'},paid:{bg:'rgba(52,199,89,0.12)',color:GREEN,label:'Paid'},overdue:{bg:'rgba(153,0,51,0.12)',color:BRAND,label:'Overdue'}}[status]||{bg:BG,color:MID,label:status};
  return `<span style="display:inline-block;padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;background:${cfg.bg};color:${cfg.color};">${cfg.label}</span>`;
}
function typeIcon(t){return t==='VAS'?'⚙️':t==='SEA'?'🚢':'✈️';}
function el(id){return document.getElementById(id);}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

async function fetchFX(){
  try{
    const r=await fetch('https://open.er-api.com/v6/latest/USD');
    if(r.ok){const d=await r.json();if(d.rates){_finState.fxRates={...d.rates,USD:1};_finState.fxLabel='Live rates';return;}}
  }catch{}
  try{const rows=await api('/finance/fx');for(const r of rows){if(r.from_curr==='USD')_finState.fxRates[r.to_curr]=r.rate;}}catch{}
  _finState.fxRates.USD=1;_finState.fxLabel='Manual rates';
}

function getWeeks(){
  const weeks=[],now=new Date();
  // Use UTC day to avoid timezone shifting to Sunday
  const dow=now.getUTCDay(); // 0=Sun,1=Mon...
  const monday=new Date(now);
  monday.setUTCDate(now.getUTCDate()-((dow+6)%7));
  monday.setUTCHours(0,0,0,0);
  for(let i=0;i<12;i++){
    const d=new Date(monday);
    d.setUTCDate(monday.getUTCDate()-i*7);
    weeks.push(d.toISOString().slice(0,10));
  }
  return weeks;
}

function injectStyles(){
  if(document.getElementById('fin-styles'))return;
  const s=document.createElement('style');s.id='fin-styles';
  s.textContent=`
  /* ── Finance page — modern design system ── */
  #page-finance{
    background: linear-gradient(135deg,#fafafa 0%,#f4f4f6 100%);
    min-height:100vh;
  }

  /* Sidebar */
  .fin-sidebar{
    width:196px;flex-shrink:0;
    background:linear-gradient(180deg,#1a0010 0%,#2d0018 100%);
    min-height:calc(100vh - 56px);
    padding:24px 0 20px;
  }
  .fin-sidebar-title{
    padding:0 20px 20px;
    border-bottom:1px solid rgba(255,255,255,0.08);
    margin-bottom:8px;
  }
  .fin-nav-item{
    display:flex;align-items:center;gap:10px;
    padding:10px 20px;font-size:12px;font-weight:500;
    color:rgba(255,255,255,0.55);cursor:pointer;
    transition:all .18s;border:none;
    width:100%;box-sizing:border-box;background:none;
    text-align:left;font-family:inherit;border-radius:0;
    letter-spacing:0.01em;
  }
  .fin-nav-item .nav-icon{
    width:28px;height:28px;border-radius:7px;
    display:flex;align-items:center;justify-content:center;
    background:rgba(255,255,255,0.08);flex-shrink:0;
    transition:all .18s;
  }
  .fin-nav-item:hover{color:rgba(255,255,255,0.85);}
  .fin-nav-item:hover .nav-icon{background:rgba(255,255,255,0.14);}
  .fin-nav-item.active{color:#fff;}
  .fin-nav-item.active .nav-icon{background:#990033;box-shadow:0 4px 12px rgba(153,0,51,0.4);}

  /* Content area */
  .fin-content{flex:1;padding:28px 32px;overflow-y:auto;min-width:0;}

  /* Cards */
  .fin-card{
    background:#fff;
    border:1px solid rgba(0,0,0,0.06);
    border-radius:16px;
    padding:20px 22px;
    box-shadow:0 1px 4px rgba(0,0,0,0.04),0 4px 16px rgba(0,0,0,0.03);
    transition:box-shadow .2s;
  }
  .fin-card:hover{box-shadow:0 2px 8px rgba(0,0,0,0.07),0 8px 24px rgba(0,0,0,0.05);}

  /* KPI cards — first row gets accent top borders */
  .fin-kpi-card{
    background:#fff;border-radius:16px;padding:20px 22px;
    border:1px solid rgba(0,0,0,0.06);
    box-shadow:0 1px 4px rgba(0,0,0,0.04);
    position:relative;overflow:hidden;
  }
  .fin-kpi-card::before{
    content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:var(--kpi-accent,#e5e7eb);
  }

  /* Inputs */
  .fin-input{
    border:1px solid rgba(0,0,0,0.10);border-radius:9px;
    padding:8px 11px;font-size:12px;font-family:inherit;
    color:#1C1C1E;outline:none;width:100%;box-sizing:border-box;
    transition:border-color .15s,box-shadow .15s;background:#fff;
  }
  .fin-input:focus{border-color:#990033;box-shadow:0 0 0 3px rgba(153,0,51,0.08);}

  /* Buttons */
  .fin-btn{
    border:none;border-radius:9px;padding:8px 16px;
    font-size:12px;font-weight:600;font-family:inherit;
    cursor:pointer;transition:all .15s;letter-spacing:0.01em;
  }
  .fin-btn-primary{background:#990033;color:#fff;box-shadow:0 2px 8px rgba(153,0,51,0.25);}
  .fin-btn-primary:hover{background:#7a0029;box-shadow:0 4px 14px rgba(153,0,51,0.35);transform:translateY(-1px);}
  .fin-btn-ghost{background:#f5f5f7;color:#1C1C1E;border:1px solid rgba(0,0,0,0.08);}
  .fin-btn-ghost:hover{background:rgba(0,0,0,0.06);}

  /* Table */
  .fin-tbl{width:100%;border-collapse:collapse;font-size:12px;}
  .fin-tbl th{
    font-size:10px;font-weight:600;color:#8e8e93;
    text-transform:uppercase;letter-spacing:0.05em;
    padding:9px 14px;text-align:left;
    background:#fafafa;
  }
  .fin-tbl th:first-child{border-radius:8px 0 0 8px;}
  .fin-tbl th:last-child{border-radius:0 8px 8px 0;}
  .fin-tbl td{padding:11px 14px;border-bottom:1px solid rgba(0,0,0,0.04);color:#1C1C1E;vertical-align:middle;}
  .fin-tbl tr:last-child td{border-bottom:none;}
  .fin-tbl tr:hover td{background:rgba(0,0,0,0.015);}

  /* Labels & titles */
  .fin-label{font-size:10px;font-weight:600;color:#8e8e93;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;display:block;}
  .fin-section-title{font-size:13px;font-weight:700;color:#1C1C1E;margin-bottom:14px;letter-spacing:-0.01em;}

  .fin-kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px;}

  /* Slide-in panel */
  .fin-panel{
    position:fixed;top:0;right:0;width:520px;height:100vh;
    background:#fff;
    border-left:1px solid rgba(0,0,0,0.08);
    box-shadow:-20px 0 60px rgba(0,0,0,0.12);
    transform:translateX(100%);
    transition:transform .3s cubic-bezier(0.4,0,0.2,1);
    z-index:300;overflow-y:auto;padding:28px 26px 60px;
  }
  .fin-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.25);backdrop-filter:blur(2px);z-index:299;display:none;}

  /* Sidebar week/currency section */
  .fin-sidebar-controls{padding:12px 14px;}
  .fin-sidebar-label{font-size:9px;font-weight:600;color:rgba(255,255,255,0.35);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:5px;display:block;padding:0 4px;}
  .fin-sidebar-select{
    width:100%;border:1px solid rgba(255,255,255,0.12);border-radius:8px;
    padding:7px 10px;font-size:11px;font-family:inherit;
    color:rgba(255,255,255,0.8);outline:none;
    background:rgba(255,255,255,0.07);
    transition:border-color .15s;
  }
  .fin-sidebar-select:focus{border-color:rgba(255,255,255,0.3);}
  .fin-sidebar-select option{background:#2d0018;color:#fff;}
  `;
  document.head.appendChild(s);
}

function injectSkeleton(host){
  injectStyles();
  const weeks=getWeeks();
  // SVG icons for nav
  const icons={
    invoices:`<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10,9 9,9 8,9"/></svg>`,
    pl:`<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>`,
    expenses:`<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="4" width="22" height="16" rx="2" ry="2"/><line x1="1" y1="10" x2="23" y2="10"/></svg>`,
  };
  host.innerHTML=`
  <div style="display:flex;align-items:stretch;min-height:calc(100vh - 56px);">

    <!-- Dark sidebar -->
    <div class="fin-sidebar">
      <div class="fin-sidebar-title">
        <div style="font-size:13px;font-weight:700;color:#fff;letter-spacing:-0.01em;">Finance</div>
        <div style="font-size:10px;color:rgba(255,255,255,0.35);margin-top:2px;">Admin only</div>
      </div>

      <div style="padding:8px 10px;margin-bottom:4px;">
        <button class="fin-nav-item active" id="fin-nav-invoices" onclick="window._finTab('invoices')">
          <span class="nav-icon">${icons.invoices}</span>Invoices
        </button>
        <button class="fin-nav-item" id="fin-nav-pl" onclick="window._finTab('pl')">
          <span class="nav-icon">${icons.pl}</span>P&amp;L
        </button>
        <button class="fin-nav-item" id="fin-nav-expenses" onclick="window._finTab('expenses')">
          <span class="nav-icon">${icons.expenses}</span>Expenses
        </button>
      </div>

      <div style="margin:8px 16px;height:1px;background:rgba(255,255,255,0.07);"></div>

      <div class="fin-sidebar-controls" style="margin-top:4px;">
        <span class="fin-sidebar-label">Week</span>
        <select id="fin-week-sel" class="fin-sidebar-select" onchange="window._finSelectWeek(this.value)">
          ${weeks.map(w=>`<option value="${w}">${weekLabel(w)}</option>`).join('')}
        </select>
      </div>
      <div class="fin-sidebar-controls" style="margin-top:8px;">
        <span class="fin-sidebar-label">Currency</span>
        <select id="fin-currency" class="fin-sidebar-select" onchange="window._finCurrencyChange(this.value)">
          <option value="USD">USD</option><option value="AUD">AUD</option><option value="EUR">EUR</option><option value="GBP">GBP</option>
        </select>
        <div id="fin-fx-label" style="font-size:9px;color:rgba(255,255,255,0.30);margin-top:5px;padding:0 4px;"></div>
      </div>
    </div>

    <!-- Main content -->
    <div class="fin-content">
      <div class="fin-kpi-grid" id="fin-kpis"></div>
      <div id="fin-tab-invoices"></div>
      <div id="fin-tab-pl" style="display:none;"></div>
      <div id="fin-tab-expenses" style="display:none;"></div>
    </div>
  </div>
  <div class="fin-panel" id="fin-panel"></div>
  <div class="fin-overlay" id="fin-overlay" onclick="window._finClosePanel()"></div>
  `;
}

window._finTab=function(tab){
  _finState.tab=tab;
  ['invoices','pl','expenses'].forEach(t=>{
    const btn=el('fin-nav-'+t),content=el('fin-tab-'+t);
    if(btn)btn.classList.toggle('active',t===tab);
    if(content)content.style.display=t===tab?'':'none';
  });
  const wk=el('fin-sidebar-week');if(wk)wk.style.display=tab==='invoices'?'':'none';
  // P&L renders its own KPI row — hide the shared top tiles to avoid duplication
  const kpiGrid=el('fin-kpis');if(kpiGrid)kpiGrid.style.display=tab==='pl'?'none':'';
  if(tab==='invoices')renderInvoicesTab();
  if(tab==='pl')renderPLTab();
  if(tab==='expenses')renderExpensesTab();
};
window._finSelectWeek=function(v){_finState.week=safeDate(v);renderInvoiceGrid();};
window._finCurrencyChange=function(v){
  _finState.currency=v;renderKPIs();
  if(_finState.tab==='invoices')renderInvoiceTable();
  if(_finState.tab==='pl')renderPLTab();
  if(_finState.tab==='expenses')renderExpensesTab();
};
window._finClosePanel=function(){
  const p=el('fin-panel'),o=el('fin-overlay');
  if(p)p.style.transform='translateX(100%)';if(o)o.style.display='none';
};
function openPanel(html){
  const p=el('fin-panel'),o=el('fin-overlay');
  if(p){p.innerHTML=html;p.style.transform='translateX(0)';}if(o)o.style.display='block';
}

async function renderKPIs(){
  try{
    const s=await api('/finance/summary');
    const rev=parseFloat(s.paid_ytd?.total||0),exp=parseFloat(s.expenses_ytd?.total||0);
    const net=rev-exp,margin=rev>0?Math.round(net/rev*100):0;
    const out=s.outstanding||{};
    const tiles=[
      {label:'Revenue YTD',value:fmtUSD(rev),sub:'Paid invoices',color:GREEN},
      {label:'Expenses YTD',value:fmtUSD(exp),sub:'All categories',color:AMBER},
      {label:'Net Margin',value:margin+'%',sub:fmtUSD(net)+' net',color:margin>20?GREEN:margin>0?AMBER:BRAND},
      {label:'Outstanding',value:fmtUSD(out.total||0),sub:(out.n||0)+' invoice'+(out.n!==1?'s':''),color:BRAND},
    ];
    const cont=el('fin-kpis');if(!cont)return;
    const kpiAccents=['#166534','#606a9f','#990033','#b8960c'];
    cont.innerHTML=tiles.map((t,i)=>`<div class="fin-kpi-card" style="--kpi-accent:${kpiAccents[i]};">
      <div style="font-size:10px;font-weight:600;color:#8e8e93;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">${t.label}</div>
      <div style="font-size:24px;font-weight:700;color:#1C1C1E;letter-spacing:-0.03em;margin-bottom:5px;">${t.value}</div>
      <div style="font-size:11px;color:${t.color};font-weight:500;">${t.sub}</div>
    </div>`).join('');
  }catch(e){console.warn('[Finance] KPI',e);}
}

async function renderInvoicesTab(){
  const cont=el('fin-tab-invoices');if(!cont)return;
  cont.innerHTML=`<div style="color:${LIGHT};font-size:12px;">Loading…</div>`;
  try{
    if(!_finState.week)_finState.week=getWeeks()[0];
    const sel=el('fin-week-sel');if(sel&&_finState.week)sel.value=_finState.week;
    const invoices=await api('/finance/invoices');
    _finState.invoices=invoices;
    cont.innerHTML=`
      <div id="fin-inv-grid" style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:28px;"></div>
      <div class="fin-card"><div class="fin-section-title">All Invoices</div><div id="fin-inv-table"></div></div>`;
    renderInvoiceGrid();renderInvoiceTable();
  }catch(e){cont.innerHTML=`<div style="color:${BRAND};padding:12px;">Error: ${esc(e.message)}</div>`;}
}

function renderInvoiceGrid(){
  const cont=el('fin-inv-grid');if(!cont)return;
  const ws=_finState.week;
  const weekInvs=_finState.invoices.filter(i=>i.week_start===ws);
  cont.innerHTML=['VAS','SEA','AIR'].map(type=>{
    const inv=weekInvs.find(i=>i.type===type);
    if(inv){
      return`<div class="fin-card" style="cursor:pointer;transition:box-shadow .15s;" onmouseenter="this.style.boxShadow='0 4px 24px rgba(0,0,0,0.1)'" onmouseleave="this.style.boxShadow='none'" onclick="window._finEditInvoice('${inv.id}')">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <span style="font-size:20px;">${typeIcon(type)}</span>
            <div><div style="font-size:12px;font-weight:600;color:${DARK};">${type}</div><div style="font-size:10px;color:${LIGHT};">${esc(inv.ref_number)}</div></div>
          </div>${statusBadge(inv.status)}
        </div>
        <div style="font-size:24px;font-weight:700;color:${DARK};margin-bottom:6px;">${fmtUSD(inv.total)}</div>
        <div style="font-size:10px;color:${MID};margin-bottom:14px;">Due ${fmtDate(inv.due_date)}</div>
        <div style="display:flex;gap:6px;">
          <button class="fin-btn fin-btn-ghost" style="flex:1;font-size:11px;" onclick="event.stopPropagation();window._finEditInvoice('${inv.id}')">Edit</button>
          <button class="fin-btn fin-btn-ghost" style="flex:1;font-size:11px;" onclick="event.stopPropagation();window._finDownloadPDF('${inv.id}','${esc(inv.ref_number)}')">⬇ PDF</button>
        </div>
      </div>`;
    }
    return`<div class="fin-card" style="cursor:pointer;border:1.5px dashed rgba(0,0,0,0.1);text-align:center;transition:all .15s;" onmouseenter="this.style.borderColor='${BRAND}'" onmouseleave="this.style.borderColor='rgba(0,0,0,0.1)'" onclick="window._finCreateInvoice('${type}','${ws}')">
      <div style="padding:28px 0;">
        <div style="font-size:28px;margin-bottom:8px;">${typeIcon(type)}</div>
        <div style="font-size:12px;font-weight:600;color:${MID};margin-bottom:4px;">${type} Invoice</div>
        <div style="font-size:10px;color:${LIGHT};margin-bottom:12px;">Not yet created</div>
        <span style="background:${BRAND};color:#fff;border-radius:20px;padding:5px 16px;font-size:11px;font-weight:600;">+ Create</span>
      </div>
    </div>`;
  }).join('');
}

function renderInvoiceTable(){
  const cont=el('fin-inv-table');if(!cont)return;
  const invs=[..._finState.invoices].sort((a,b)=>b.week_start.localeCompare(a.week_start));
  if(!invs.length){cont.innerHTML=`<div style="font-size:11px;color:${LIGHT};padding:12px 0;">No invoices yet.</div>`;return;}
  cont.innerHTML=`<table class="fin-tbl">
    <thead><tr><th>Reference</th><th>Type</th><th>Week</th><th>Date</th><th>Due</th><th>Amount</th><th>Status</th><th></th></tr></thead>
    <tbody>${invs.map(i=>`<tr style="cursor:pointer;" onclick="window._finEditInvoice('${i.id}')">
      <td style="font-weight:500;font-size:11px;">${esc(i.ref_number)}</td>
      <td>${typeIcon(i.type)} ${i.type}</td>
      <td style="color:${MID};font-size:11px;">${weekLabel(i.week_start)}</td>
      <td style="color:${MID};font-size:11px;">${fmtDate(i.invoice_date)}</td>
      <td style="color:${MID};font-size:11px;">${fmtDate(i.due_date)}</td>
      <td style="font-weight:600;">${fmtUSD(i.total)}</td>
      <td>${statusBadge(i.status)}</td>
      <td onclick="event.stopPropagation()"><button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:4px 10px;" onclick="window._finDownloadPDF('${i.id}','${esc(i.ref_number)}')">PDF</button></td>
    </tr>`).join('')}</tbody>
  </table>`;
}

window._finCreateInvoice=async function(type,weekStart){
  const ws=safeDate(weekStart)||_finState.week;
  openPanel(`<div style="padding:20px;color:${LIGHT};font-size:12px;">Loading ${type} data…</div>`);
  try{
    const pf=await api(`/finance/prefill/${type}/${ws}`);
    renderInvoiceEditor({id:null,type,week_start:ws,invoice_date:isoToday(),due_date:addDays(isoToday(),type==='VAS'?30:7),status:'draft',notes:'',subtotal:pf.subtotal||0,gst:pf.gst||0,customs:pf.customs||0,misc_total:0,total:pf.total||0,lines:pf.lines||[]});
  }catch(e){openPanel(`<div style="padding:20px;color:${BRAND};">Error: ${esc(e.message)}</div>`);}
};

window._finEditInvoice=async function(id){
  openPanel(`<div style="padding:20px;color:${LIGHT};font-size:12px;">Loading…</div>`);
  try{renderInvoiceEditor(await api(`/finance/invoices/${id}`));}
  catch(e){openPanel(`<div style="padding:20px;color:${BRAND};">Error: ${esc(e.message)}</div>`);}
};

function renderInvoiceEditor(inv){
  const isNew=!inv.id,type=inv.type;
  const lines=inv.lines||[];
  const mainL=lines.filter(l=>!l.gst_free&&!l.is_misc);
  const custL=lines.filter(l=>l.gst_free&&!l.is_misc);
  const miscL=lines.filter(l=>l.is_misc);
  while(miscL.length<2)miscL.push({description:'',unit_label:'',rate:0,quantity:0,total:0,gst_free:0,is_misc:1});
  window._finCurrentLines=[...mainL,...miscL,...custL].map(l=>({...l}));

  function lineHtml(l,idx){
    const r=`<input class="fin-input" style="width:65px;text-align:right;" data-field="rate" data-idx="${idx}" value="${l.rate||0}" oninput="window._finLCh(${idx})"/>`;
    const q=`<input class="fin-input" style="width:70px;text-align:right;" data-field="qty" data-idx="${idx}" value="${l.quantity||0}" oninput="window._finLCh(${idx})"/>`;
    const t=`<span id="flt-${idx}" style="font-size:11px;font-weight:600;">${fmtUSD(l.total)}</span>`;
    if(type==='VAS')return`<tr><td style="font-size:11px;padding:6px 8px;">${esc(l.description)}</td><td style="font-size:10px;color:${MID};padding:6px 8px;">${esc(l.unit_label)}</td><td style="padding:4px 6px;">${r}</td><td style="padding:4px 6px;">${q}</td><td style="text-align:right;padding:6px 8px;">${t}</td></tr>`;
    return`<tr><td style="padding:4px 6px;"><input class="fin-input" style="font-size:11px;" data-field="desc" data-idx="${idx}" value="${esc(l.description)}" oninput="window._finLCh(${idx})"/></td><td style="padding:4px 6px;">${r}</td><td style="padding:4px 6px;">${q}</td><td style="text-align:right;padding:6px 8px;">${t}</td></tr>`;
  }
  function miscHtml(l,idx){
    return`<tr><td colspan="${type==='VAS'?2:1}" style="padding:4px 6px;"><input class="fin-input" placeholder="Optional description" data-field="desc" data-idx="${idx}" value="${esc(l.description)}" oninput="window._finLCh(${idx})"/></td>
    <td style="padding:4px 6px;"><input class="fin-input" style="width:65px;text-align:right;" data-field="rate" data-idx="${idx}" value="${l.rate||0}" oninput="window._finLCh(${idx})"/></td>
    <td style="padding:4px 6px;"><input class="fin-input" style="width:65px;text-align:right;" data-field="qty" data-idx="${idx}" value="${l.quantity||0}" oninput="window._finLCh(${idx})"/></td>
    <td style="text-align:right;padding:6px 8px;"><span id="flt-${idx}">${fmtUSD(l.total)}</span></td></tr>`;
  }
  const nM=mainL.length,nMi=miscL.length;
  const allL=window._finCurrentLines;
  const col=type==='VAS'?`<th>Service</th><th>Unit</th><th>Rate</th><th>Qty</th><th style="text-align:right">Total</th>`:`<th>Description</th><th>Rate</th><th>Qty/KG</th><th style="text-align:right">Total</th>`;

  openPanel(`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
      <div><div style="font-size:15px;font-weight:700;color:${DARK};">${typeIcon(type)} ${type} Invoice</div><div style="font-size:11px;color:${LIGHT};">${weekLabel(inv.week_start)}</div></div>
      <button onclick="window._finClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;">✕</button>
    </div>
    <div style="display:grid;grid-template-columns:1fr auto;gap:10px;margin-bottom:14px;">
      <div><span class="fin-label">Reference</span><input id="fin-inv-ref" class="fin-input" style="font-weight:600;" value="${esc(inv.ref_number||'')}" placeholder="Auto-generated on save"/></div>
      <div><span class="fin-label">Status</span><select id="fin-inv-status" class="fin-input" style="width:110px;">${['draft','sent','paid','overdue'].map(s=>`<option value="${s}" ${s===inv.status?'selected':''}>${s[0].toUpperCase()+s.slice(1)}</option>`).join('')}</select></div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px;">
      <div><span class="fin-label">Invoice Date</span><input id="fin-inv-date" type="date" class="fin-input" value="${safeDate(inv.invoice_date)||isoToday()}"/></div>
      <div><span class="fin-label">Due Date</span><input id="fin-inv-due" type="date" class="fin-input" value="${safeDate(inv.due_date)||addDays(isoToday(),30)}"/></div>
    </div>
    <span class="fin-label">Line Items</span>
    <div style="border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;overflow:hidden;margin-bottom:12px;">
      <table style="width:100%;border-collapse:collapse;">
        <thead style="background:${BG};"><tr style="font-size:9px;font-weight:600;color:${LIGHT};text-transform:uppercase;">${col}</tr></thead>
        <tbody>
          ${allL.slice(0,nM).map((l,i)=>lineHtml(l,i)).join('')}
          ${nMi?`<tr><td colspan="5" style="font-size:9px;color:${LIGHT};text-transform:uppercase;padding:5px 8px;background:${BG};border-top:0.5px solid rgba(0,0,0,0.05);">Miscellaneous</td></tr>`:''}
          ${allL.slice(nM,nM+nMi).map((l,i)=>miscHtml(l,nM+i)).join('')}
          ${custL.length?`<tr><td colspan="5" style="font-size:9px;color:${LIGHT};text-transform:uppercase;padding:5px 8px;background:${BG};border-top:0.5px solid rgba(0,0,0,0.05);">Customs / GST-free</td></tr>`:''}
          ${allL.slice(nM+nMi).map((l,i)=>lineHtml(l,nM+nMi+i)).join('')}
        </tbody>
      </table>
    </div>
    <div style="background:${BG};border-radius:10px;padding:12px 14px;margin-bottom:14px;">
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:4px;"><span>Subtotal</span><span id="ft-sub">${fmtUSD(inv.subtotal)}</span></div>
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:4px;"><span>GST (10%)</span><span id="ft-gst">${fmtUSD(inv.gst)}</span></div>
      <div style="display:flex;justify-content:space-between;font-size:11px;color:${MID};margin-bottom:8px;"><span>Customs / GST-free</span><span id="ft-cus">${fmtUSD(inv.customs)}</span></div>
      <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:700;color:${DARK};border-top:0.5px solid rgba(0,0,0,0.08);padding-top:8px;"><span>Total Payable</span><span id="ft-tot">${fmtUSD(inv.total)}</span></div>
    </div>
    <span class="fin-label">Notes</span>
    <textarea id="fin-inv-notes" class="fin-input" rows="2" style="resize:vertical;margin-bottom:16px;">${esc(inv.notes||'')}</textarea>
    <div style="display:flex;gap:8px;">
      <button class="fin-btn fin-btn-primary" style="flex:1;" onclick="window._finSaveInv(${isNew?'null':`'${inv.id}'`},'${type}','${safeDate(inv.week_start)}')">${isNew?'Create Invoice':'Save Changes'}</button>
      ${!isNew?`<button class="fin-btn fin-btn-ghost" onclick="window._finDownloadPDF('${inv.id}','${esc(inv.ref_number||'')}')">⬇ PDF</button>`:''}
      <button class="fin-btn fin-btn-ghost" onclick="window._finClosePanel()">Cancel</button>
    </div>
    ${!isNew?`<div style="margin-top:12px;text-align:center;"><button onclick="window._finDelInv('${inv.id}')" style="background:none;border:none;color:${LIGHT};font-size:11px;cursor:pointer;">Delete invoice</button></div>`:''}
  `);

  window._finLCh=function(idx){
    const L=window._finCurrentLines;
    const rE=document.querySelector(`[data-idx="${idx}"][data-field="rate"]`);
    const qE=document.querySelector(`[data-idx="${idx}"][data-field="qty"]`);
    const dE=document.querySelector(`[data-idx="${idx}"][data-field="desc"]`);
    if(dE&&L[idx])L[idx].description=dE.value;
    const rate=parseFloat(rE?.value||0),qty=parseFloat(qE?.value||0),tot=Math.round(rate*qty*100)/100;
    if(L[idx]){L[idx].rate=rate;L[idx].quantity=qty;L[idx].total=tot;}
    const tEl=document.getElementById('flt-'+idx);if(tEl)tEl.textContent=fmtUSD(tot);
    const nM2=L.filter(l=>!l.gst_free&&!l.is_misc),nC2=L.filter(l=>l.gst_free&&!l.is_misc),nX2=L.filter(l=>l.is_misc);
    const sub=Math.round([...nM2,...nX2].reduce((s,l)=>s+(parseFloat(l.total)||0),0)*100)/100;
    const gst=Math.round(sub*0.10*100)/100,cus=nC2.reduce((s,l)=>s+(parseFloat(l.total)||0),0);
    const T=Math.round((sub+gst+cus)*100)/100;
    const S=el('ft-sub'),G=el('ft-gst'),C=el('ft-cus'),Tt=el('ft-tot');
    if(S)S.textContent=fmtUSD(sub);if(G)G.textContent=fmtUSD(gst);if(C)C.textContent=fmtUSD(cus);if(Tt)Tt.textContent=fmtUSD(T);
  };
}

window._finSaveInv=async function(id,type,weekStart){
  const L=window._finCurrentLines||[],ws=safeDate(weekStart)||_finState.week;
  const mL=L.filter(l=>!l.gst_free&&!l.is_misc),cL=L.filter(l=>l.gst_free&&!l.is_misc),xL=L.filter(l=>l.is_misc&&l.description);
  const customs=cL.reduce((s,l)=>s+(parseFloat(l.total)||0),0),misc_total=xL.reduce((s,l)=>s+(parseFloat(l.total)||0),0);
  const refOverride=el('fin-inv-ref')?.value?.trim()||'';
  const payload={type,week_start:ws,invoice_date:el('fin-inv-date')?.value||isoToday(),due_date:el('fin-inv-due')?.value||'',status:el('fin-inv-status')?.value||'draft',notes:el('fin-inv-notes')?.value||'',customs,misc_total,lines:[...mL,...xL,...cL],ref_override:refOverride};
  try{
    if(id)await api(`/finance/invoices/${id}`,{method:'PATCH',body:JSON.stringify(payload)});
    else await api('/finance/invoices',{method:'POST',body:JSON.stringify(payload)});
    _finState.invoices=await api('/finance/invoices');
    window._finClosePanel();renderInvoiceGrid();renderInvoiceTable();renderKPIs();
  }catch(e){alert('Save failed: '+e.message);}
};
window._finDownloadPDF=async function(id,ref){
  try{
    const token=await getToken();
    const url=_apiBase+`/finance/invoice/${id}/pdf`+(token?'?_token='+encodeURIComponent(token):'');
    // Use fetch+blob so browser never navigates away
    const r=await fetch(url);
    if(!r.ok){
      const err=await r.text();
      throw new Error('PDF failed: '+err);
    }
    const blob=await r.blob();
    const blobUrl=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=blobUrl;
    a.download=(ref||'invoice').replace(/[^a-zA-Z0-9\-_]/g,'_')+'.pdf';
    document.body.appendChild(a);a.click();a.remove();
    setTimeout(()=>URL.revokeObjectURL(blobUrl),5000);
  }catch(e){alert('PDF download failed: '+e.message);}
};
window._finDelInv=async function(id){
  if(!confirm('Delete this invoice?'))return;
  try{await api(`/finance/invoices/${id}`,{method:'DELETE'});_finState.invoices=_finState.invoices.filter(i=>i.id!==id);window._finClosePanel();renderInvoiceGrid();renderInvoiceTable();renderKPIs();}
  catch(e){alert('Delete failed: '+e.message);}
};

function finKpiHtml(ytd, months){
  function spark(vals, color, fill){
    const v=vals.filter(x=>x!=null&&x!==0);
    if(v.length<2)return'';
    const W=80,H=28,pad=2;
    const mn=Math.min(...v),mx=Math.max(...v);
    const range=mx-mn||1;
    const pts=v.map((val,i)=>{
      const x=pad+i*(W-pad*2)/(v.length-1);
      const y=H-pad-(val-mn)/range*(H-pad*2);
      return x.toFixed(1)+','+y.toFixed(1);
    });
    const area='M'+pts[0]+' '+pts.slice(1).map(p=>'L'+p).join(' ')+' L'+(W-pad).toFixed(1)+','+H+' L'+pad+','+H+' Z';
    const line='M'+pts[0]+' '+pts.slice(1).map(p=>'L'+p).join(' ');
    const last=pts[pts.length-1].split(',');
    return '<svg width="'+W+'" height="'+H+'" style="overflow:visible;" viewBox="0 0 '+W+' '+H+'">'
      +(fill?'<path d="'+area+'" fill="'+color+'" opacity="0.12"/>':"")
      +'<path d="'+line+'" fill="none" stroke="'+color+'" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>'
      +'<circle cx="'+last[0]+'" cy="'+last[1]+'" r="2.5" fill="'+color+'"/>'
      +'</svg>';
  }
  const revVals=months.map(m=>m.revenue||0);
  const expVals=months.map(m=>m.expenses||0);
  const netVals=months.map(m=>m.net||0);
  const unitVals=months.map(m=>m.units_vas||0);
  const revSpark=spark(revVals,'#166534',true);
  const expSpark=spark(expVals,'#606a9f',true);
  const netSpark=spark(netVals,ytd.net>=0?'#166534':'#990033',false);
  const unitSpark=spark(unitVals,'#b8960c',true);
  const revExpPct=ytd.revenue>0?Math.min(100,Math.round(ytd.expenses/ytd.revenue*100)):0;
  const netColor=ytd.net>0?'#166534':'#990033';
  const netPct=Math.min(100,Math.abs(ytd.margin_pct||0));
  const e=s=>String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return `
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">

    <div style="background:#fff;border-radius:14px;padding:16px 18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);position:relative;overflow:hidden;">
      <div style="position:absolute;bottom:10px;right:12px;opacity:0.85;">${revSpark}</div>
      <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;">Revenue</div>
      <div id="fin-kpi-rev" style="font-size:22px;font-weight:800;color:#1C1C1E;letter-spacing:-0.03em;">${fmtUSD(ytd.revenue)}</div>
      <div style="height:3px;background:rgba(0,0,0,0.06);border-radius:2px;margin:8px 0 6px;">
        <div style="height:100%;width:${Math.round((ytd.rev_vas||0)/Math.max(ytd.revenue,1)*100)}%;background:#990033;border-radius:2px;"></div>
      </div>
      <div id="fin-kpi-rev-sub" style="font-size:9px;color:#8e8e93;">
        <span style="color:#990033;font-weight:600;">VAS ${fmtUSD(ytd.rev_vas)}</span> &nbsp;·&nbsp;
        <span style="color:#a76e6e;">Sea ${fmtUSD(ytd.rev_sea)}</span> &nbsp;·&nbsp;
        <span style="color:#b8960c;">Air ${fmtUSD(ytd.rev_air)}</span>
      </div>
    </div>

    <div style="background:#fff;border-radius:14px;padding:16px 18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);position:relative;overflow:hidden;">
      <div style="position:absolute;bottom:10px;right:12px;opacity:0.85;">${expSpark}</div>
      <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;">Expenses</div>
      <div id="fin-kpi-exp" style="font-size:22px;font-weight:800;color:#1C1C1E;letter-spacing:-0.03em;">${fmtUSD(ytd.expenses)}</div>
      <div style="height:3px;background:rgba(0,0,0,0.06);border-radius:2px;margin:8px 0 6px;">
        <div style="height:100%;width:${revExpPct}%;background:#606a9f;border-radius:2px;"></div>
      </div>
      <div id="fin-kpi-exp-sub" style="font-size:9px;color:#8e8e93;">
        <span style="color:#990033;">Labour ${fmtUSD(ytd.exp_labour)}</span> &nbsp;·&nbsp;
        <span style="color:#606a9f;">Freight ${fmtUSD(ytd.exp_freight)}</span>
      </div>
    </div>

    <div style="background:#fff;border-radius:14px;padding:16px 18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);position:relative;overflow:hidden;">
      <div style="position:absolute;bottom:10px;right:12px;opacity:0.85;">${netSpark}</div>
      <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;">Net Profit</div>
      <div id="fin-kpi-net" style="font-size:22px;font-weight:800;color:${netColor};letter-spacing:-0.03em;">${fmtUSD(ytd.net)}</div>
      <div style="display:flex;align-items:center;gap:6px;margin:8px 0 6px;">
        <div style="flex:1;height:3px;background:rgba(0,0,0,0.06);border-radius:2px;">
          <div style="height:100%;width:${netPct}%;background:${netColor};border-radius:2px;"></div>
        </div>
        <span style="font-size:11px;font-weight:700;color:${netColor};">${ytd.margin_pct}%</span>
      </div>
      <div id="fin-kpi-net-sub" style="font-size:9px;color:#8e8e93;">margin on revenue</div>
    </div>

    <div style="background:#fff;border-radius:14px;padding:16px 18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);position:relative;overflow:hidden;">
      <div style="position:absolute;bottom:10px;right:12px;opacity:0.85;">${unitSpark}</div>
      <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;">Units Processed</div>
      <div id="fin-kpi-units" style="font-size:22px;font-weight:800;color:#1C1C1E;letter-spacing:-0.03em;">${(ytd.units_vas||0).toLocaleString()}</div>
      <div style="height:3px;background:rgba(0,0,0,0.06);border-radius:2px;margin:8px 0 6px;">
        <div style="height:100%;width:${Math.min(100,Math.round(((ytd.units_sea||0)+(ytd.units_air||0))/Math.max(ytd.units_vas,1)*100))}%;background:#b8960c;border-radius:2px;"></div>
      </div>
      <div id="fin-kpi-units-sub" style="font-size:9px;color:#8e8e93;">
        <span style="color:#a76e6e;">Sea ${(ytd.units_sea||0).toLocaleString()}</span> &nbsp;·&nbsp;
        <span style="color:#b8960c;">Air ${(ytd.units_air||0).toLocaleString()}</span>
      </div>
    </div>

  </div>`;
}


async function renderPLTab(){
  const cont=el('fin-tab-pl');if(!cont)return;
  cont.innerHTML=`<div style="color:${LIGHT};font-size:12px;">Loading P&L…</div>`;
  try{
    const year=new Date().getUTCFullYear();
    const pl=await api(`/finance/pl?year=${year}`);
    _finState.pl=pl;
    const ytd=pl.ytd||{},months=pl.months||[];

    // ── Main layout ──
    cont.innerHTML=`
      <div style="display:flex;gap:20px;align-items:flex-start;">

        <!-- LEFT MAIN CONTENT -->
        <div style="flex:1;min-width:0;">

          <!-- ── Page header: title + period + action ── -->
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
            <div style="display:flex;align-items:center;gap:14px;">
              <div>
                <div style="font-size:22px;font-weight:800;color:#1C1C1E;letter-spacing:-0.04em;">Profit &amp; Loss</div>
                <div style="font-size:11px;color:#8e8e93;margin-top:2px;letter-spacing:0.01em;">${year} &nbsp;·&nbsp; Accrual basis incl. draft</div>
              </div>
              <div style="display:flex;gap:2px;background:rgba(0,0,0,0.05);border-radius:10px;padding:3px;">
                <button id="fin-period-ytd" onclick="window._finSetPeriod('ytd')" style="font-size:10px;padding:5px 14px;border-radius:8px;border:none;background:#990033;color:#fff;cursor:pointer;font-family:inherit;font-weight:700;letter-spacing:0.03em;">YTD</button>
                <button id="fin-period-4w"  onclick="window._finSetPeriod('4w')"  style="font-size:10px;padding:5px 14px;border-radius:8px;border:none;background:transparent;color:#6E6E73;cursor:pointer;font-family:inherit;font-weight:500;">4W</button>
                <button id="fin-period-8w"  onclick="window._finSetPeriod('8w')"  style="font-size:10px;padding:5px 14px;border-radius:8px;border:none;background:transparent;color:#6E6E73;cursor:pointer;font-family:inherit;font-weight:500;">8W</button>
              </div>
            </div>
<div></div><!-- expense button moved to monthly rows -->
          </div>

          <!-- ── KPI row: 4 metric cards with sparklines ── -->
          ${finKpiHtml(ytd,months)}

          <!-- ── Unit Economics: 4-channel cards in one row ── -->
          <div style="background:#fff;border-radius:14px;padding:18px 20px;margin-bottom:18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
              <div style="font-size:13px;font-weight:700;color:#1C1C1E;letter-spacing:-0.02em;">Unit Economics</div>
              <div style="font-size:10px;color:#8e8e93;">Revenue · Cost · Margin per unit processed</div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;" id="fin-ue-strip">
              ${apoUnitCard('VAS', ytd.vas_rev_pu, ytd.vas_cost_pu, ytd.rev_vas, ytd.exp_labour, ytd.units_vas)}
              ${apoUnitCard('Sea', ytd.sea_rev_pu, ytd.sea_cost_pu, ytd.rev_sea, ytd.exp_freight_sea||0, ytd.units_sea)}
              ${apoUnitCard('Air', ytd.air_rev_pu, ytd.air_cost_pu, ytd.rev_air, ytd.exp_freight_air||0, ytd.units_air)}
              ${apoUnitCard('Blended', ytd.blended_rev_pu, ytd.blended_cost_pu, ytd.revenue, ytd.expenses, ytd.units_vas)}
            </div>
          </div>

          <!-- ── Heatmap ── -->
          <div style="background:#fff;border-radius:14px;padding:18px 20px;margin-bottom:18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
              <div style="font-size:13px;font-weight:700;color:#1C1C1E;letter-spacing:-0.02em;">Monthly Heatmap</div>
              <div style="display:flex;gap:3px;background:rgba(0,0,0,0.04);border-radius:8px;padding:2px;">
                <button id="plc-rev" onclick="window._finPlChart('rev')" style="font-size:10px;padding:4px 12px;border-radius:6px;border:none;background:#990033;color:#fff;cursor:pointer;font-family:inherit;font-weight:600;">Revenue</button>
                <button id="plc-pu"  onclick="window._finPlChart('pu')"  style="font-size:10px;padding:4px 12px;border-radius:6px;border:none;background:transparent;color:#6E6E73;cursor:pointer;font-family:inherit;font-weight:500;">Per Unit</button>
              </div>
            </div>
            <div id="fin-heatmap" style="overflow-x:auto;"></div>
          </div>

          <!-- ── Monthly Breakdown ── -->
          <div style="background:#fff;border-radius:14px;padding:18px 20px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
              <div style="font-size:13px;font-weight:700;color:#1C1C1E;letter-spacing:-0.02em;">Monthly Detail</div>
              <div style="display:flex;align-items:center;gap:6px;">
                <span style="font-size:10px;color:#8e8e93;">/u = per unit</span>
                <button onclick="window._finExpandAll(true)"  style="font-size:10px;padding:4px 12px;border-radius:7px;border:1px solid rgba(0,0,0,0.09);background:#f5f5f7;color:#1C1C1E;cursor:pointer;font-family:inherit;font-weight:500;">Expand All</button>
                <button onclick="window._finExpandAll(false)" style="font-size:10px;padding:4px 12px;border-radius:7px;border:1px solid rgba(0,0,0,0.09);background:#f5f5f7;color:#1C1C1E;cursor:pointer;font-family:inherit;font-weight:500;">Collapse All</button>
              </div>
            </div>
            <div style="overflow-x:auto;">
            <table class="fin-tbl" style="min-width:700px;">
              <thead><tr>
                <th style="width:130px;">Month</th>
                <th>Revenue</th>
                <th>Expenses</th>
                <th style="text-align:right;">Net</th>
                <th style="text-align:right;">Cash Flow</th>
                <th style="text-align:right;"></th>
              </tr></thead>
              <tbody id="fin-pl-tbody"></tbody>
            </table>
            </div>
          </div>

        </div>

        <!-- ── RIGHT: Insights panel ── -->
        <div style="width:280px;flex-shrink:0;">
          <div style="position:sticky;top:16px;background:#fff;border-radius:14px;padding:18px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 1px 4px rgba(0,0,0,0.04),0 4px 16px rgba(0,0,0,0.03);">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:16px;">
              <div style="width:30px;height:30px;border-radius:8px;background:linear-gradient(135deg,#990033,#7a0029);display:flex;align-items:center;justify-content:center;box-shadow:0 3px 8px rgba(153,0,51,0.3);flex-shrink:0;">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.2"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
              </div>
              <div style="flex:1;min-width:0;">
                <div style="font-size:13px;font-weight:700;color:#1C1C1E;letter-spacing:-0.01em;">AI Insights</div>
                <div style="font-size:9px;color:#8e8e93;">Auto-loads on open</div>
              </div>
              <span style="font-size:9px;background:rgba(153,0,51,0.08);color:#990033;padding:2px 7px;border-radius:6px;font-weight:700;letter-spacing:0.03em;">AI</span>
            </div>
            <button id="fin-insights-btn" onclick="window._finLoadInsights()" style="width:100%;font-size:11px;padding:8px 0;border-radius:8px;border:none;background:linear-gradient(135deg,#990033,#7a0029);color:#fff;cursor:pointer;font-family:inherit;font-weight:600;margin-bottom:16px;box-shadow:0 2px 8px rgba(153,0,51,0.2);letter-spacing:0.02em;">↻ Refresh</button>
            <div id="fin-insights-content" style="color:#8e8e93;font-size:11px;line-height:1.5;"></div>
          </div>
        </div>

      </div>
    `;
    // ── Period filter logic ──
    window._finPeriod = 'ytd';
    window._finSetPeriod = function(p){
      window._finPeriod = p;
      ['ytd','4w','8w'].forEach(id=>{
        const btn=el('fin-period-'+id);if(!btn)return;
        btn.style.background = id===p ? BRAND : 'transparent';
        btn.style.color = id===p ? '#fff' : MID;
      });
      const now = new Date();
      const cutoff = p==='4w' ? new Date(now-28*864e5).toISOString().slice(0,7)
                   : p==='8w' ? new Date(now-56*864e5).toISOString().slice(0,7)
                   : '0000-00';
      const filtered = p==='ytd' ? months : months.filter(m=>m.month_key>=cutoff);
      renderPLCharts(filtered, window._finPlChartMode||'rev');
      // Recompute KPI tiles for filtered period
      const agg = filtered.reduce((a,m)=>({
        revenue:a.revenue+m.revenue, rev_vas:a.rev_vas+m.rev_vas,
        rev_sea:a.rev_sea+m.rev_sea, rev_air:a.rev_air+m.rev_air,
        expenses:a.expenses+m.expenses, exp_labour:a.exp_labour+m.exp_labour,
        exp_freight:a.exp_freight+m.exp_freight,
        net:a.net+m.net, units_vas:a.units_vas+m.units_vas,
        units_sea:a.units_sea+m.units_sea, units_air:a.units_air+m.units_air,
      }),{revenue:0,rev_vas:0,rev_sea:0,rev_air:0,expenses:0,exp_labour:0,exp_freight:0,net:0,units_vas:0,units_sea:0,units_air:0});
      const mp = agg.revenue>0?Math.round(agg.net/agg.revenue*100):0;
      const setT=(id,v)=>{const e2=el(id);if(e2)e2.innerHTML=v;};
      setT('fin-kpi-rev', fmtUSD(agg.revenue));
      setT('fin-kpi-rev-sub',`VAS ${fmtUSD(agg.rev_vas)} · Sea ${fmtUSD(agg.rev_sea)} · Air ${fmtUSD(agg.rev_air)}`);
      setT('fin-kpi-exp', fmtUSD(agg.expenses));
      setT('fin-kpi-exp-sub',`Labour ${fmtUSD(agg.exp_labour)} · Freight ${fmtUSD(agg.exp_freight)}`);
      setT('fin-kpi-net', fmtUSD(agg.net));
      setT('fin-kpi-net-sub', mp+'% margin');
      setT('fin-kpi-units', (agg.units_vas||0).toLocaleString());
      setT('fin-kpi-units-sub', `Sea ${(agg.units_sea||0).toLocaleString()} · Air ${(agg.units_air||0).toLocaleString()}`);
      // Update unit economics strip
      const freightTotal=agg.rev_sea+agg.rev_air||1;
      const seaFrac=agg.rev_sea/freightTotal, airFrac=agg.rev_air/freightTotal;
      const exp_fsea=Math.round(agg.exp_freight*seaFrac*100)/100;
      const exp_fair=Math.round(agg.exp_freight*airFrac*100)/100;
      const ueStrip=el('fin-ue-strip');
      if(ueStrip) ueStrip.innerHTML=
        apoUnitCard('⚙️ VAS', agg.units_vas>0?Math.round(agg.rev_vas/agg.units_vas*100)/100:null, agg.units_vas>0?Math.round(agg.exp_labour/agg.units_vas*100)/100:null, agg.rev_vas, agg.exp_labour, agg.units_vas)+
        apoUnitCard('🚢 Sea', agg.units_sea>0?Math.round(agg.rev_sea/agg.units_sea*100)/100:null, agg.units_sea>0?Math.round(exp_fsea/agg.units_sea*100)/100:null, agg.rev_sea, exp_fsea, agg.units_sea)+
        apoUnitCard('✈️ Air', agg.units_air>0?Math.round(agg.rev_air/agg.units_air*100)/100:null, agg.units_air>0?Math.round(exp_fair/agg.units_air*100)/100:null, agg.rev_air, exp_fair, agg.units_air)+
        apoUnitCard('◈ Blended', agg.units_vas>0?Math.round(agg.revenue/agg.units_vas*100)/100:null, agg.units_vas>0?Math.round(agg.expenses/agg.units_vas*100)/100:null, agg.revenue, agg.expenses, agg.units_vas);
    };

    // ── Monthly rows ──
    // ── Modern accordion months ──
    const tbody=el('fin-pl-tbody');
    let runningCF=0;
    const maxRev=Math.max(...months.map(m=>m.revenue||0),1);
    const maxExp=Math.max(...months.map(m=>m.expenses||0),1);
    const monthsSorted=[...months].sort((a,b)=>a.month_key.localeCompare(b.month_key));
    if(tbody)tbody.innerHTML=monthsSorted.map(m=>{
      const hasData=m.revenue>0||m.expenses>0;
      const mn=new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short',year:'numeric'});
      if(hasData)runningCF+=m.net;
      const cfColor=runningCF>=0?'#166534':'#990033';
      const mColor=m.margin_pct>20?'#166534':m.margin_pct>0?'#606a9f':'#990033';
      const netColor=m.net>0?'#166534':m.net<0?'#990033':'#8e8e93';
      const revBar=Math.round(Math.min(100,m.revenue/maxRev*100));
      const expBar=Math.round(Math.min(100,m.expenses/maxExp*100));
      if(!hasData)return`<tr><td colspan="2" style="padding:10px 14px;color:#AEAEB2;font-size:11px;">${mn}</td>
        <td colspan="4" style="padding:10px 14px;"><span style="font-size:10px;color:#AEAEB2;">No activity</span></td>
        <td style="text-align:right;padding:10px 14px;">
          <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 10px;" onclick="window._finAddExpense('${m.month_key}')">+ Expense</button>
        </td></tr>`;
      return`
      <tr style="border-bottom:1px solid rgba(0,0,0,0.04);">
        <!-- Month + expand -->
        <td style="padding:14px 14px;white-space:nowrap;min-width:120px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <button onclick="window._finToggleMonth('${m.month_key}')" id="fin-exp-icon-${m.month_key}"
              style="width:20px;height:20px;border-radius:6px;border:1px solid rgba(0,0,0,0.10);background:#f5f5f7;cursor:pointer;font-size:9px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-family:inherit;transition:all .15s;">▶</button>
            <div>
              <div style="font-size:12px;font-weight:600;color:#1C1C1E;">${mn}</div>
              <div style="font-size:9px;color:#8e8e93;margin-top:1px;">${(m.units_vas||0).toLocaleString()} units</div>
            </div>
          </div>
        </td>
        <!-- Revenue bar -->
        <td style="padding:14px 10px;min-width:160px;">
          <div style="font-size:11px;font-weight:600;color:#166534;margin-bottom:4px;">${fmtUSD(m.revenue)}</div>
          <div style="height:5px;background:rgba(0,0,0,0.06);border-radius:3px;overflow:hidden;">
            <div style="height:100%;width:${revBar}%;background:linear-gradient(90deg,#990033,#a76e6e);border-radius:3px;"></div>
          </div>
          <div style="font-size:9px;color:#8e8e93;margin-top:3px;">
            ${m.rev_vas>0?`<span style="color:#990033;">VAS ${fmtUSD(m.rev_vas)}</span>`:''}
            ${m.rev_sea>0?` · <span style="color:#a76e6e;">Sea ${fmtUSD(m.rev_sea)}</span>`:''}
            ${m.rev_air>0?` · <span style="color:#b8960c;">Air ${fmtUSD(m.rev_air)}</span>`:''}
          </div>
        </td>
        <!-- Expenses bar -->
        <td style="padding:14px 10px;min-width:140px;">
          <div style="font-size:11px;font-weight:600;color:#606a9f;margin-bottom:4px;">${fmtUSD(m.expenses)}</div>
          <div style="height:5px;background:rgba(0,0,0,0.06);border-radius:3px;overflow:hidden;">
            <div style="height:100%;width:${expBar}%;background:linear-gradient(90deg,#606a9f,#a76e6e);border-radius:3px;"></div>
          </div>
        </td>
        <!-- Net -->
        <td style="padding:14px 10px;text-align:right;min-width:100px;">
          <div style="font-size:12px;font-weight:700;color:${netColor};">${fmtUSD(m.net)}</div>
          <div style="font-size:9px;font-weight:600;color:${mColor};margin-top:2px;">${m.margin_pct}% margin</div>
        </td>
        <!-- Cash flow -->
        <td style="padding:14px 10px;text-align:right;min-width:100px;">
          <div style="font-size:12px;font-weight:700;color:${cfColor};">${fmtUSD(runningCF)}</div>
          <div style="font-size:9px;color:#8e8e93;margin-top:2px;">cumulative</div>
        </td>
        <!-- Actions -->
        <td style="padding:14px 14px;text-align:right;">
          <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:4px 10px;" onclick="window._finAddExpense('${m.month_key}')">+ Expense</button>
        </td>
      </tr>
      <tr id="fin-pl-expand-${m.month_key}" style="display:none;">
        <td colspan="6" style="padding:0;background:rgba(0,0,0,0.015);">
          <div id="fin-pl-ec-${m.month_key}" style="padding:16px 20px;border-top:1px solid rgba(0,0,0,0.05);"></div>
        </td>
      </tr>`;
    }).join('');

    // ── Chart mode ──
    window._finPlChartMode='rev';
    window._finPlChart=function(mode){
      window._finPlChartMode=mode;
      const rBtn=el('plc-rev'),pBtn=el('plc-pu');
      if(rBtn){rBtn.style.background=mode==='rev'?BRAND:BG;rBtn.style.color=mode==='rev'?'#fff':MID;}
      if(pBtn){pBtn.style.background=mode==='pu'?BRAND:BG;pBtn.style.color=mode==='pu'?'#fff':MID;}
      const period=window._finPeriod||'ytd';
      const now=new Date();
      const cutoff=period==='4w'?new Date(now-28*864e5).toISOString().slice(0,7):period==='8w'?new Date(now-56*864e5).toISOString().slice(0,7):'0000-00';
      const filtered=period==='ytd'?months:months.filter(m=>m.month_key>=cutoff);
      renderPLCharts(filtered,mode);
    };

    renderPLCharts(months,'rev');

    // ── Expand month row ──
    window._finExpandAll=function(expand){
      const months2=document.querySelectorAll('[id^="fin-pl-expand-"]');
      months2.forEach(row=>{
        const mk=row.id.replace('fin-pl-expand-','');
        const icon=el('fin-exp-icon-'+mk);
        if(expand&&row.style.display==='none'){window._finToggleMonth(mk);}
        else if(!expand&&row.style.display!=='none'){row.style.display='none';if(icon){icon.textContent='▶';icon.style.color='';icon.style.background='';icon.style.borderColor='rgba(0,0,0,0.1)';}}
      });
    };
    window._finToggleMonth=async function(mk){
      const row=el('fin-pl-expand-'+mk);if(!row)return;
      const icon=el('fin-exp-icon-'+mk);
      if(row.style.display==='none'){
        if(icon){icon.textContent='▼';icon.style.color='#fff';icon.style.background=BRAND;icon.style.borderColor=BRAND;}
        row.style.display='';
        const c=el('fin-pl-ec-'+mk);if(!c)return;
        c.innerHTML=`<div style="color:${LIGHT};font-size:11px;">Loading…</div>`;
        try{
          const exps=await api(`/finance/expenses?month_key=${mk}`);
          const m=months.find(m=>m.month_key===mk);
          const invs=m?.invoices||[];
          c.innerHTML=`<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;">
            <div>
              <div class="fin-label">Revenue</div>
              ${invs.length?invs.map(i=>`<div style="display:flex;justify-content:space-between;font-size:11px;padding:5px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <span style="color:${MID};">${typeIcon(i.type||'')} ${esc(i.ref||i.type)} <span style="font-size:9px;color:${LIGHT};">${i.status}</span></span>
                <span style="font-weight:600;color:${GREEN};">${fmtUSD(i.amount)}</span>
              </div>`).join(''):`<div style="font-size:11px;color:${LIGHT};">No invoices</div>`}
            </div>
            <div>
              <div class="fin-label">Expenses <button onclick="window._finAddExpense('${mk}')" style="background:none;border:none;color:${BRAND};cursor:pointer;font-size:10px;font-weight:600;margin-left:6px;">+ Add</button></div>
              ${exps.length?exps.map(e=>`<div style="display:flex;justify-content:space-between;align-items:center;font-size:11px;padding:5px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <div><span style="color:${DARK};">${esc(e.description)}</span><span style="color:${LIGHT};margin-left:5px;font-size:9px;">${esc(e.category)}</span>${e.is_recurring?`<span style="color:${BLUE};font-size:9px;margin-left:3px;">↻</span>`:''}</div>
                <div style="display:flex;align-items:center;gap:8px;">
                  <span style="font-weight:600;color:${AMBER};">${fmtUSD(e.amount)}</span>
                  <button onclick="window._finEditExpense('${e.id}')" style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:11px;">✎</button>
                  <button onclick="window._finDeleteExpense('${e.id}')" style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:11px;">✕</button>
                </div>
              </div>`).join(''):`<div style="font-size:11px;color:${LIGHT};">No expenses yet</div>`}
            </div>
            <div>
              <div class="fin-label">Unit Economics</div>
              ${[
                ['⚙️ VAS', m?.vas_rev_pu, m?.vas_cost_pu, m?.units_vas, m?.rev_vas, m?.exp_labour],
                ['🚢 Sea', m?.sea_rev_pu, m?.sea_cost_pu, m?.units_sea, m?.rev_sea, m?.exp_freight_sea],
                ['✈️ Air', m?.air_rev_pu, m?.air_cost_pu, m?.units_air, m?.rev_air, m?.exp_freight_air],
              ].map(([label,rev,cost,units,totRev,totCost])=>`<div style="padding:6px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px;">
                  <span style="color:${MID};font-weight:500;">${label} <span style="font-size:9px;font-weight:400;">(${(units||0).toLocaleString()}u)</span></span>
                  <span style="color:${GREEN};font-size:10px;">${totRev?fmtUSD(totRev):'-'}</span>
                </div>
                <div style="display:flex;justify-content:space-between;font-size:10px;">
                  <span style="color:${LIGHT};">$${rev!==null&&rev!==undefined?Number(rev).toFixed(3):'-'}/u rev · $${cost!==null&&cost!==undefined?Number(cost).toFixed(3):'-'}/u cost</span>
                  <span style="color:${AMBER};font-size:10px;">${totCost?fmtUSD(totCost):'-'}</span>
                </div>
              </div>`).join('')}
            </div>
          </div>`;
        }catch(e){c.innerHTML=`<div style="color:${BRAND};font-size:11px;">${e.message}</div>`;}
      }else{row.style.display='none';if(icon){icon.textContent='▶';icon.style.color='';icon.style.background='';icon.style.borderColor='rgba(0,0,0,0.1)';}}
    };

    // ── Claude insights — routes through /pulse/chat backend (avoids CORS) ──
    window._finLoadInsights=async function(){
      const cont2=el('fin-insights-content');if(!cont2)return;
      const genBtn=el('fin-insights-btn');
      if(genBtn){genBtn.disabled=true;genBtn.textContent='Analysing…';}
      cont2.innerHTML=`<div style="color:${LIGHT};font-size:11px;text-align:center;padding:20px 0;">Analysing your P&L data…</div>`;
      try{
        const apiBase=(document.querySelector('meta[name="api-base"]')?.content||'').replace(/\/+$/,'');
        let token=null;
        if(window.Clerk?.session){try{token=await window.Clerk.session.getToken();}catch(_){}}
        const summary={year,ytd,months_with_data:months.filter(m=>m.revenue>0||m.expenses>0).map(m=>({
          month:m.month_key, revenue:m.revenue, rev_vas:m.rev_vas, rev_sea:m.rev_sea, rev_air:m.rev_air,
          expenses:m.expenses, exp_labour:m.exp_labour, exp_freight:m.exp_freight, exp_overhead:m.exp_overhead,
          net:m.net, margin_pct:m.margin_pct,
          units_vas:m.units_vas, units_sea:m.units_sea, units_air:m.units_air,
          vas_rev_pu:m.vas_rev_pu, vas_cost_pu:m.vas_cost_pu, vas_margin_pu:m.vas_margin_pu,
          sea_rev_pu:m.sea_rev_pu, sea_cost_pu:m.sea_cost_pu, air_rev_pu:m.air_rev_pu, air_cost_pu:m.air_cost_pu,
        }))};
        const prompt=`You are a financial analyst for VelOzity, a 3PL/VAS company. Revenue channels: VAS (value added services - labelling/processing units), Sea Freight, Air Freight. Labour expenses = direct VAS processing cost. Freight expenses split between Sea and Air. Analyse this P&L and give exactly 5 specific actionable insights to improve profitability. Be direct with numbers. Return ONLY a JSON array, no markdown, each object has: title (short 3-5 words), insight (1-2 sentences with specific numbers from data), action (one concrete next step), impact (High/Medium/Low), channel (VAS/Sea/Air/Overall). P&L data: ${JSON.stringify(summary)}`;
        const resp=await fetch(apiBase+'/finance/insights',{method:'POST',
          headers:Object.assign({'Content-Type':'application/json'},token?{'Authorization':'Bearer '+token}:{}),
          body:JSON.stringify({pl_data:summary})});
        if(!resp.ok)throw new Error('Server error '+resp.status);
        const d=await resp.json();
        let insights=d.insights||[];
        if(!Array.isArray(insights)||!insights.length)throw new Error('No insights returned');
        const impColors={'High':'#990033','Medium':'#606a9f','Low':'#a76e6e'};
        const chColors={'VAS':'#990033','Sea':'#a76e6e','Air':'#b8960c','Overall':'#606a9f'};
        cont2.innerHTML=insights.slice(0,5).map(ins=>{
          const impColor=impColors[ins.impact]||'#606a9f';
          const chColor=chColors[ins.channel]||'#8e8e93';
          return`<div style="background:#fff;border-radius:10px;padding:12px 14px;margin-bottom:8px;border:1px solid rgba(0,0,0,0.07);border-left:3px solid ${impColor};box-shadow:0 1px 3px rgba(0,0,0,0.04);">
            <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:6px;margin-bottom:5px;">
              <div style="font-size:11px;font-weight:700;color:#1C1C1E;line-height:1.3;">${esc(ins.title||'')}</div>
              <div style="display:flex;flex-direction:column;align-items:flex-end;gap:3px;flex-shrink:0;">
                <span style="font-size:8px;padding:1px 6px;border-radius:5px;background:rgba(0,0,0,0.05);color:${chColor};font-weight:700;white-space:nowrap;">${esc(ins.channel||'')}</span>
                <span style="font-size:8px;color:${impColor};font-weight:700;text-transform:uppercase;letter-spacing:0.04em;">${ins.impact||''}</span>
              </div>
            </div>
            <div style="font-size:10px;color:#6E6E73;margin-bottom:8px;line-height:1.55;">${esc(ins.insight||'')}</div>
            <div style="font-size:10px;color:#1C1C1E;background:#fafafa;padding:6px 8px;border-radius:6px;border:1px solid rgba(0,0,0,0.06);">→ ${esc(ins.action||'')}</div>
          </div>`;
        }).join('');
        if(genBtn){genBtn.disabled=false;genBtn.textContent='Refresh Insights';}
      }catch(e){
        cont2.innerHTML=`<div style="color:${BRAND};font-size:11px;padding:8px;">Unable to load insights: ${esc(e.message)}</div>`;
        if(genBtn){genBtn.disabled=false;genBtn.textContent='Retry';}
      }
    };
    // Auto-load insights when page renders
    setTimeout(()=>window._finLoadInsights&&window._finLoadInsights(), 800);

  }catch(e){cont.innerHTML=`<div style="color:${BRAND};padding:20px;">P&L failed: ${esc(e.message)}</div>`;}
}

function apoUnitCard(label, revPu, costPu, totalRev, totalCost, totalUnits){
  const marginPu = revPu!==null&&costPu!==null ? Math.round((revPu-costPu)*100)/100 : null;
  const mColor = marginPu===null ? LIGHT : marginPu>0 ? '#166534' : BRAND;
  const unitsStr = totalUnits ? Number(totalUnits).toLocaleString()+'u' : '—';
  const channelColor = label.includes('VAS')?'#990033':label.includes('Sea')?'#a76e6e':label.includes('Air')?'#b8960c':'#606a9f';
  const COST_COLOR = '#4a4a6a'; // dark slate — neutral, not alarming
  return`<div style="background:#fff;border-radius:10px;padding:12px 14px;border:0.5px solid rgba(0,0,0,0.07);border-top:3px solid ${channelColor};">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
      <div style="font-size:11px;font-weight:600;color:${DARK};">${label}</div>
      <div style="font-size:9px;background:rgba(0,0,0,0.04);padding:2px 7px;border-radius:8px;color:${MID};">${unitsStr}</div>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px;">
      <span style="color:${LIGHT};">Revenue</span>
      <span style="font-weight:600;color:#166534;">${totalRev!==null&&totalRev!==undefined?fmtUSD(totalRev):'—'}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px;">
      <span style="color:${LIGHT};">Cost</span>
      <span style="font-weight:600;color:${COST_COLOR};">${totalCost!==null&&totalCost!==undefined?fmtUSD(totalCost):'—'}</span>
    </div>
    <div style="height:0.5px;background:rgba(0,0,0,0.06);margin:7px 0;"></div>
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px;">
      <span style="color:${LIGHT};">Rev/unit</span>
      <span style="font-weight:600;color:#166534;">${revPu!==null?'$'+Number(revPu).toFixed(3):'—'}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px;">
      <span style="color:${LIGHT};">Cost/unit</span>
      <span style="font-weight:600;color:${COST_COLOR};">${costPu!==null?'$'+Number(costPu).toFixed(3):'—'}</span>
    </div>
    <div style="height:0.5px;background:rgba(0,0,0,0.06);margin:7px 0;"></div>
    <div style="display:flex;justify-content:space-between;font-size:10px;">
      <span style="color:${LIGHT};font-weight:500;">Margin/unit</span>
      <span style="font-weight:700;color:${mColor};">${marginPu!==null?'$'+Number(marginPu).toFixed(3):'—'}</span>
    </div>
  </div>`;
}


function loadChartJS(cb){if(window.Chart){cb();return;}const s=document.createElement('script');s.src='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';s.onload=cb;document.head.appendChild(s);}

// ── Heatmap renderer — replaces bar/line chart ──
function renderPLCharts(months, mode='rev'){
  const wrap=el('fin-heatmap');
  if(!wrap)return;
  if(window._fcR){try{window._fcR.destroy();}catch(_){}window._fcR=null;}
  if(window._fcE){try{window._fcE.destroy();}catch(_){}window._fcE=null;}

  const active=months.filter(m=>m.revenue>0||m.expenses>0||m.units_vas>0);
  if(!active.length){wrap.innerHTML='<div style="color:#AEAEB2;font-size:12px;padding:30px;text-align:center;">No data for this period</div>';return;}

  const labels=active.map(m=>new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short',year:'2-digit'}));
  const nullZ=arr=>arr.map(v=>(v&&v>0)?v:null);

  if(mode==='rev'){
    // ── Chart layout: Net & Margin full-width top, Revenue + Expense bottom row ──
    wrap.innerHTML=`
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
        <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
          ${[['VAS','#990033'],['Sea','#a76e6e'],['Air','#b8960c'],['Labour','rgba(153,0,51,0.75)'],['Freight','rgba(96,106,159,0.75)'],['Net +','#166534'],['Net −','#990033'],['Margin %','#606a9f']].map(([l,c])=>
            `<span style="display:inline-flex;align-items:center;gap:5px;font-size:9px;color:#6E6E73;">
              <span style="width:16px;height:3px;background:${c};border-radius:2px;display:inline-block;opacity:0.9;"></span>${l}
            </span>`).join('')}
        </div>
        <button onclick="window._finChartFullscreen()" style="display:flex;align-items:center;gap:4px;font-size:10px;padding:4px 10px;border-radius:7px;border:1px solid rgba(0,0,0,0.09);background:#f5f5f7;color:#1C1C1E;cursor:pointer;font-family:inherit;font-weight:500;flex-shrink:0;">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg>
          Full screen
        </button>
      </div>
      <div style="display:grid;grid-template-columns:1fr;gap:0;">
        <!-- TOP: Net & Margin full width -->
        <div style="padding-bottom:14px;border-bottom:1px solid rgba(0,0,0,0.05);">
          <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:8px;">Net Profit &amp; Margin %</div>
          <div style="height:120px;"><canvas id="fin-ch-net"></canvas></div>
        </div>
        <!-- BOTTOM: Channel Revenue + Expense Breakdown side by side -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;padding-top:14px;">
          <div>
            <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:8px;">Channel Revenue</div>
            <div style="height:110px;"><canvas id="fin-ch-rev"></canvas></div>
          </div>
          <div>
            <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:8px;">Expense Breakdown</div>
            <div style="height:110px;"><canvas id="fin-ch-exp"></canvas></div>
          </div>
        </div>
      </div>`;

    // Revenue area chart
    const ctxR=document.getElementById('fin-ch-rev');
    if(ctxR&&window.Chart){
      window._fcR=new window.Chart(ctxR,{type:'line',data:{labels,datasets:[
        {label:'VAS',  data:nullZ(active.map(m=>m.rev_vas)),  borderColor:'#990033',backgroundColor:'rgba(153,0,51,0.12)',borderWidth:2,fill:true,tension:0.4,pointRadius:3,pointBackgroundColor:'#fff',pointBorderColor:'#990033',pointBorderWidth:1.5,spanGaps:false},
        {label:'Sea',  data:nullZ(active.map(m=>m.rev_sea)),  borderColor:'#a76e6e',backgroundColor:'rgba(167,110,110,0.08)',borderWidth:2,fill:true,tension:0.4,pointRadius:3,pointBackgroundColor:'#fff',pointBorderColor:'#a76e6e',pointBorderWidth:1.5,spanGaps:false},
        {label:'Air',  data:nullZ(active.map(m=>m.rev_air)),  borderColor:'#b8960c',backgroundColor:'rgba(184,150,12,0.08)',borderWidth:2,fill:true,tension:0.4,pointRadius:3,pointBackgroundColor:'#fff',pointBorderColor:'#b8960c',pointBorderWidth:1.5,spanGaps:false},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,callbacks:{label:ctx=>`${ctx.dataset.label}: $${(ctx.parsed.y||0).toLocaleString()}`}}},
        scales:{x:{ticks:{font:{size:9},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:9},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}}}}});
    }

    // Net margin combo (bar=net $, line=margin %)
    const ctxN=document.getElementById('fin-ch-net');
    if(ctxN&&window.Chart){
      window._fcN=new window.Chart(ctxN,{type:'bar',data:{labels,datasets:[
        {label:'Net',data:active.map(m=>m.net||0),backgroundColor:active.map(m=>m.net>0?'rgba(22,101,52,0.7)':'rgba(153,0,51,0.55)'),borderRadius:3,order:2},
        {label:'Margin%',data:active.map(m=>m.margin_pct||0),type:'line',borderColor:'#606a9f',borderWidth:2,pointRadius:2,fill:false,tension:0.4,yAxisID:'y2',order:1},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false}},
        scales:{x:{ticks:{font:{size:8},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:8},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}},
                y2:{position:'right',ticks:{font:{size:8},color:'#606a9f',callback:v=>v+'%'},grid:{display:false},border:{display:false}}}}});
    }

    // Expense stacked bar
    const ctxE=document.getElementById('fin-ch-exp');
    if(ctxE&&window.Chart){
      window._fcE=new window.Chart(ctxE,{type:'bar',data:{labels,datasets:[
        {label:'Labour',   data:active.map(m=>m.exp_labour||0),   backgroundColor:'rgba(153,0,51,0.75)',borderRadius:2,stack:'s'},
        {label:'Freight',  data:active.map(m=>m.exp_freight||0),  backgroundColor:'rgba(96,106,159,0.75)',borderRadius:2,stack:'s'},
        {label:'Overhead', data:active.map(m=>m.exp_overhead||0), backgroundColor:'rgba(167,110,110,0.60)',borderRadius:2,stack:'s'},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false}},
        scales:{x:{ticks:{font:{size:8},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:8},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}}}}});
    }

  // ── Fullscreen modal ──
  window._finChartFullscreen = function(){
    // Remove any existing modal
    const existing = document.getElementById('fin-chart-modal');
    if(existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'fin-chart-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.55);backdrop-filter:blur(4px);z-index:9000;display:flex;align-items:center;justify-content:center;padding:24px;';
    modal.innerHTML = `
      <div style="background:#fff;border-radius:20px;width:100%;max-width:1100px;max-height:90vh;overflow-y:auto;box-shadow:0 24px 80px rgba(0,0,0,0.25);display:flex;flex-direction:column;">
        <div style="display:flex;align-items:center;justify-content:space-between;padding:20px 24px 0;">
          <div style="font-size:15px;font-weight:700;color:#1C1C1E;letter-spacing:-0.02em;">Monthly Performance Charts</div>
          <button onclick="document.getElementById('fin-chart-modal').remove()" style="width:30px;height:30px;border-radius:8px;border:1px solid rgba(0,0,0,0.1);background:#f5f5f7;cursor:pointer;font-size:14px;display:flex;align-items:center;justify-content:center;">✕</button>
        </div>
        <div style="padding:20px 24px 24px;display:flex;flex-direction:column;gap:20px;">
          <!-- Net & Margin full width -->
          <div>
            <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Net Profit &amp; Margin %</div>
            <div style="height:200px;"><canvas id="fin-modal-net"></canvas></div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
            <div>
              <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Channel Revenue</div>
              <div style="height:180px;"><canvas id="fin-modal-rev"></canvas></div>
            </div>
            <div>
              <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Expense Breakdown</div>
              <div style="height:180px;"><canvas id="fin-modal-exp"></canvas></div>
            </div>
          </div>
          <!-- Legend -->
          <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;padding-top:4px;border-top:1px solid rgba(0,0,0,0.05);">
            ${[['VAS','#990033'],['Sea','#a76e6e'],['Air','#b8960c'],['Labour','rgba(153,0,51,0.75)'],['Freight','rgba(96,106,159,0.75)'],['Net +','#166534'],['Net −','#990033'],['Margin %','#606a9f']].map(([l,c])=>
              '<span style="display:inline-flex;align-items:center;gap:6px;font-size:10px;color:#6E6E73;"><span style="width:18px;height:3px;background:'+c+';border-radius:2px;display:inline-block;opacity:0.9;"></span>'+l+'</span>'
            ).join('')}
          </div>
        </div>
      </div>`;

    modal.addEventListener('click', e => { if(e.target === modal) modal.remove(); });
    document.body.appendChild(modal);

    // Render charts inside modal using same data
    if(!window.Chart) return;
    setTimeout(() => {
      const mLabels = active.map(m=>new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short',year:'2-digit'}));
      const ctxMN = document.getElementById('fin-modal-net');
      if(ctxMN) new window.Chart(ctxMN,{type:'bar',data:{labels:mLabels,datasets:[
        {label:'Net',data:active.map(m=>m.net||0),backgroundColor:active.map(m=>m.net>0?'rgba(22,101,52,0.7)':'rgba(153,0,51,0.55)'),borderRadius:4,order:2},
        {label:'Margin%',data:active.map(m=>m.margin_pct||0),type:'line',borderColor:'#606a9f',borderWidth:2.5,pointRadius:3,fill:false,tension:0.4,yAxisID:'y2',order:1},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false}},
        scales:{x:{ticks:{font:{size:10},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:10},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}},
                y2:{position:'right',ticks:{font:{size:10},color:'#606a9f',callback:v=>v+'%'},grid:{display:false},border:{display:false}}}}});
      const ctxMR = document.getElementById('fin-modal-rev');
      if(ctxMR) new window.Chart(ctxMR,{type:'line',data:{labels:mLabels,datasets:[
        {label:'VAS',data:active.map(m=>m.rev_vas||null),borderColor:'#990033',backgroundColor:'rgba(153,0,51,0.10)',borderWidth:2.5,fill:true,tension:0.4,pointRadius:4,pointBackgroundColor:'#fff',pointBorderColor:'#990033',pointBorderWidth:2,spanGaps:false},
        {label:'Sea',data:active.map(m=>m.rev_sea||null),borderColor:'#a76e6e',backgroundColor:'rgba(167,110,110,0.06)',borderWidth:2.5,fill:true,tension:0.4,pointRadius:4,pointBackgroundColor:'#fff',pointBorderColor:'#a76e6e',pointBorderWidth:2,spanGaps:false},
        {label:'Air',data:active.map(m=>m.rev_air||null),borderColor:'#b8960c',backgroundColor:'rgba(184,150,12,0.06)',borderWidth:2.5,fill:true,tension:0.4,pointRadius:4,pointBackgroundColor:'#fff',pointBorderColor:'#b8960c',pointBorderWidth:2,spanGaps:false},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,callbacks:{label:ctx=>`${ctx.dataset.label}: $${(ctx.parsed.y||0).toLocaleString()}`}}},
        scales:{x:{ticks:{font:{size:10},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:10},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}}}}});
      const ctxME = document.getElementById('fin-modal-exp');
      if(ctxME) new window.Chart(ctxME,{type:'bar',data:{labels:mLabels,datasets:[
        {label:'Labour',  data:active.map(m=>m.exp_labour||0),  backgroundColor:'rgba(153,0,51,0.75)',borderRadius:3,stack:'s'},
        {label:'Freight', data:active.map(m=>m.exp_freight||0), backgroundColor:'rgba(96,106,159,0.75)',borderRadius:3,stack:'s'},
        {label:'Overhead',data:active.map(m=>m.exp_overhead||0),backgroundColor:'rgba(167,110,110,0.60)',borderRadius:3,stack:'s'},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false}},
        scales:{x:{ticks:{font:{size:10},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:10},color:'#8e8e93',callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}}}}});
    }, 50);
  };

  } else {
    // ── Per Unit view: grouped bar per channel ──
    wrap.innerHTML=`
      <div>
        <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:10px;">Revenue per Unit by Channel</div>
        <div style="height:160px;"><canvas id="fin-ch-pu"></canvas></div>
        <div style="display:flex;align-items:center;gap:14px;margin-top:10px;flex-wrap:wrap;">
          ${[['VAS Rev/u','#990033'],['VAS Cost/u','rgba(153,0,51,0.3)'],['Sea Rev/u','#a76e6e'],['Air Rev/u','#b8960c']].map(([l,c])=>
            `<span style="display:inline-flex;align-items:center;gap:5px;font-size:9px;color:#6E6E73;">
              <span style="width:16px;height:3px;background:${c};border-radius:2px;display:inline-block;"></span>${l}
            </span>`).join('')}
        </div>
      </div>`;
    const ctxP=document.getElementById('fin-ch-pu');
    if(ctxP&&window.Chart){
      window._fcR=new window.Chart(ctxP,{type:'bar',data:{labels,datasets:[
        {label:'VAS Rev/u', data:active.map(m=>m.vas_rev_pu),  backgroundColor:'#990033',borderRadius:4,barPercentage:0.6},
        {label:'VAS Cost/u',data:active.map(m=>m.vas_cost_pu), backgroundColor:'rgba(153,0,51,0.3)',borderRadius:4,barPercentage:0.6},
        {label:'Sea Rev/u', data:active.map(m=>m.sea_rev_pu),  backgroundColor:'#a76e6e',borderRadius:4,barPercentage:0.6},
        {label:'Air Rev/u', data:active.map(m=>m.air_rev_pu),  backgroundColor:'#b8960c',borderRadius:4,barPercentage:0.6},
      ]},options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,callbacks:{label:ctx=>`${ctx.dataset.label}: $${Number(ctx.parsed.y||0).toFixed(3)}/u`}}},
        scales:{x:{ticks:{font:{size:9},color:'#8e8e93'},grid:{display:false},border:{display:false}},
                y:{ticks:{font:{size:9},color:'#8e8e93',callback:v=>'$'+Number(v).toFixed(3)},grid:{color:'rgba(0,0,0,0.04)'},border:{display:false}}}}});
    }
  }
}



async function renderExpensesTab(){
  const root=el('fin-tab-expenses');if(!root)return;
  root.innerHTML=`<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
    <div>
      <div style="font-size:17px;font-weight:700;color:${DARK};">Expenses</div>
      <div style="font-size:12px;color:${MID};margin-top:2px;">Direct costs by period · Internal overhead</div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;">
      <select id="exp-filter-cat" class="fin-input" style="width:200px;" onchange="window._finExpFilter()">
        <option value="">All categories</option>
        ${Object.entries(EXPENSE_CAT_GROUPS).map(([g,cats])=>`<optgroup label="${g}">${cats.map(c=>`<option value="${c}">${c}</option>`).join('')}</optgroup>`).join('')}
      </select>
      <select id="exp-filter-period" class="fin-input" style="width:160px;" onchange="window._finExpFilter()">
        <option value="">All periods</option>
        ${(()=>{const opts=[];const now=new Date();for(let i=0;i<12;i++){const d=new Date(now.getUTCFullYear(),now.getUTCMonth()-i,1);const mk=d.toISOString().slice(0,7);const label=d.toLocaleDateString('en-AU',{month:'short',year:'numeric'});opts.push(`<option value="${mk}">${label}</option>`);}return opts.join('');})()}
      </select>
      <button class="fin-btn fin-btn-primary" onclick="window._finAddExpense()">+ Add Expense</button>
    </div>
  </div>
  <div id="exp-summary-row" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;"></div>
  <div id="exp-table-wrap"></div>`;

  await window._finExpFilter();
}

window._finExpFilter=async function(){
  const cat=el('exp-filter-cat')?.value||'';
  const mk=el('exp-filter-period')?.value||'';
  let url='/finance/expenses?';
  if(cat) url+=`category=${encodeURIComponent(cat)}&`;
  if(mk)  url+=`month_key=${encodeURIComponent(mk)}&`;
  try{
    const exps=await api(url);
    _finState.expenses=exps;
    renderExpSummary(exps);
    renderExpTable(exps);
  }catch(e){
    const w=el('exp-table-wrap');
    if(w)w.innerHTML=`<div style="color:#c00;font-size:12px;padding:16px;">Failed to load expenses: ${esc(e.message)}</div>`;
  }
};

function renderExpSummary(exps){
  const root=el('exp-summary-row');if(!root)return;
  const total=exps.reduce((s,e)=>s+parseFloat(e.amount||0),0);
  // Group by top-level category type
  const vas=exps.filter(e=>e.category==='VAS Cost').reduce((s,e)=>s+parseFloat(e.amount||0),0);
  const freight=exps.filter(e=>e.category==='Sea Freight Cost'||e.category==='Air Freight Cost').reduce((s,e)=>s+parseFloat(e.amount||0),0);
  const overhead=exps.filter(e=>String(e.category).startsWith('Internal Overhead')).reduce((s,e)=>s+parseFloat(e.amount||0),0);
  const other=total-vas-freight-overhead;
  const card=(label,val,color,sub)=>`
    <div style="background:#fff;border-radius:12px;border:1px solid #e5e7eb;padding:16px;">
      <div style="font-size:9px;font-weight:700;color:#8e8e93;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px;">${label}</div>
      <div style="font-size:20px;font-weight:800;color:${color};letter-spacing:-0.03em;">${fmtUSD(val)}</div>
      ${sub?`<div style="font-size:10px;color:${MID};margin-top:3px;">${sub}</div>`:''}
    </div>`;
  root.innerHTML=
    card('Total Expenses',total,DARK,`${exps.length} entries`)+
    card('VAS Cost',vas,'#7c3aed','Direct processing')+
    card('Freight Cost',freight,BLUE,'Sea + Air')+
    card('Internal Overhead',overhead,AMBER,'All sub-categories');
}

function renderExpTable(exps){
  const root=el('exp-table-wrap');if(!root)return;
  if(!exps.length){
    root.innerHTML=`<div style="background:#fff;border-radius:12px;border:1px solid #e5e7eb;padding:40px;text-align:center;color:${MID};font-size:13px;">No expenses found. <button class="fin-btn fin-btn-primary" style="margin-left:12px;" onclick="window._finAddExpense()">+ Add Expense</button></div>`;
    return;
  }
  // Group by month_key for display
  const byMonth=new Map();
  for(const e of exps){
    const mk=e.month_key||e.expense_date?.slice(0,7)||'Unknown';
    if(!byMonth.has(mk))byMonth.set(mk,[]);
    byMonth.get(mk).push(e);
  }
  const sorted=[...byMonth.entries()].sort((a,b)=>a[0].localeCompare(b[0]));
  root.innerHTML=sorted.map(([mk,rows])=>{
    const monthTotal=rows.reduce((s,e)=>s+parseFloat(e.amount||0),0);
    const d=new Date(mk+'-01T00:00:00Z');
    const label=d.toLocaleDateString('en-AU',{month:'long',year:'numeric'});
    return`<div style="margin-bottom:16px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
        <div style="font-size:12px;font-weight:700;color:${DARK};">${label}</div>
        <div style="display:flex;align-items:center;gap:10px;">
          <div style="font-size:12px;font-weight:700;color:${AMBER};">${fmtUSD(monthTotal)}</div>
          <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 10px;" onclick="window._finAddExpense('${mk}')">+ Add</button>
        </div>
      </div>
      <div style="background:#fff;border-radius:12px;border:1px solid #e5e7eb;overflow:hidden;">
        <table class="fin-tbl" style="margin:0;">
          <thead><tr>
            <th style="width:30%;">Description</th>
            <th>Category</th>
            <th>Date</th>
            <th style="text-align:right;">Amount</th>
            <th>Recurring</th>
            <th style="width:80px;"></th>
          </tr></thead>
          <tbody>${rows.map(e=>`<tr>
            <td style="font-weight:500;">${esc(e.description)}</td>
            <td><span style="display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;background:${catColor(e.category,0.12)};color:${catColor(e.category,1)};">${esc(e.category)}</span></td>
            <td style="color:${MID};font-size:11px;">${fmtDate(e.expense_date)}</td>
            <td style="text-align:right;font-weight:600;color:${AMBER};">${fmtUSD(e.amount)}</td>
            <td>${e.is_recurring?`<span style="color:${BLUE};font-size:10px;font-weight:600;">↻ ${e.recur_freq||'monthly'}</span>`:'—'}</td>
            <td>
              <button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:2px 8px;margin-right:4px;" onclick="window._finEditExpense('${e.id}')">Edit</button>
              <button style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:13px;" onclick="window._finDeleteExpense('${e.id}')">✕</button>
            </td>
          </tr>`).join('')}</tbody>
        </table>
      </div>
    </div>`;
  }).join('');
}

function catColor(cat,alpha){
  if(cat==='VAS Cost')return alpha<1?`rgba(124,58,237,${alpha})`:'#7c3aed';
  if(cat==='Sea Freight Cost')return alpha<1?`rgba(59,130,246,${alpha})`:'#3b82f6';
  if(cat==='Air Freight Cost')return alpha<1?`rgba(14,165,233,${alpha})`:'#0ea5e9';
  if(String(cat).startsWith('Internal Overhead'))return alpha<1?`rgba(200,134,10,${alpha})`:'#c8860a';
  if(cat==='Direct Labour')return alpha<1?`rgba(34,197,94,${alpha})`:'#22c55e';
  return alpha<1?`rgba(174,174,178,${alpha})`:'#6E6E73';
}

function renderExpList(exps){
  const c=el('fin-exp-list');if(!c)return;
  if(!exps.length){c.innerHTML=`<div style="font-size:11px;color:${LIGHT};padding:12px 0;">No expenses yet.</div>`;return;}
  c.innerHTML=`<table class="fin-tbl"><thead><tr><th>Description</th><th>Category</th><th>Date</th><th>Amount</th><th>Recurring</th><th></th></tr></thead>
  <tbody>${exps.map(e=>`<tr>
    <td style="font-weight:500;">${esc(e.description)}</td><td style="color:${MID};">${esc(e.category)}</td>
    <td style="color:${MID};font-size:11px;">${fmtDate(e.expense_date)}</td>
    <td style="font-weight:600;color:${AMBER};">${fmtUSD(e.amount)}</td>
    <td>${e.is_recurring?`<span style="color:${BLUE};font-size:10px;font-weight:600;">↻ ${e.recur_freq||'monthly'}</span>`:'—'}</td>
    <td><button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 8px;margin-right:4px;" onclick="window._finEditExpense('${e.id}')">Edit</button>
    <button style="background:none;border:none;color:${LIGHT};cursor:pointer;" onclick="window._finDeleteExpense('${e.id}')">✕</button></td>
  </tr>`).join('')}</tbody></table>`;
}

window._finAddExpense=function(defaultMonth){
  renderExpEditor({id:null,category:'VAS Cost',description:'',amount:0,currency:'USD',expense_date:defaultMonth?defaultMonth+'-01':isoToday(),is_recurring:0,recur_freq:'monthly',recur_end:''});
};
window._finEditExpense=async function(id){
  const exp=_finState.expenses.find(e=>e.id===id);
  if(exp){renderExpEditor(exp);}else{try{const a=await api('/finance/expenses');const f=a.find(e=>e.id===id);if(f)renderExpEditor(f);}catch{}}
};
function renderExpEditor(exp){
  const isNew=!exp.id;
  openPanel(`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
      <div style="font-size:15px;font-weight:700;color:${DARK};">${isNew?'New Expense':'Edit Expense'}</div>
      <button onclick="window._finClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;">✕</button>
    </div>
    <div style="margin-bottom:12px;"><span class="fin-label">Category</span><select id="exp-cat" class="fin-input">${Object.entries(EXPENSE_CAT_GROUPS).map(([g,cats])=>`<optgroup label="${g}">${cats.map(c=>`<option value="${c}" ${c===exp.category?'selected':''}>${c}</option>`).join('')}</optgroup>`).join('')}</select></div>
    <div style="margin-bottom:12px;"><span class="fin-label">Description</span><input id="exp-desc" class="fin-input" value="${esc(exp.description)}" placeholder="e.g. COSCO sea freight"/></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">
      <div><span class="fin-label">Amount</span><input id="exp-amount" type="number" step="0.01" class="fin-input" value="${exp.amount||0}"/></div>
      <div><span class="fin-label">Currency</span><select id="exp-currency" class="fin-input">${['USD','AUD','EUR','GBP'].map(c=>`<option value="${c}" ${c===exp.currency?'selected':''}>${c}</option>`).join('')}</select></div>
    </div>
    <div style="margin-bottom:12px;"><span class="fin-label">Date</span><input id="exp-date" type="date" class="fin-input" value="${safeDate(exp.expense_date)||isoToday()}"/></div>
    <label style="display:flex;align-items:center;gap:8px;cursor:pointer;margin-bottom:12px;">
      <input type="checkbox" id="exp-recur" ${exp.is_recurring?'checked':''} style="width:14px;height:14px;accent-color:${BRAND};" onchange="document.getElementById('exp-recur-opts').style.display=this.checked?'':'none'"/>
      <span style="font-size:12px;color:${DARK};">Recurring expense</span>
    </label>
    <div id="exp-recur-opts" style="display:${exp.is_recurring?'':'none'};margin-bottom:12px;padding:12px;background:${BG};border-radius:8px;">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div><span class="fin-label">Frequency</span><select id="exp-freq" class="fin-input"><option value="monthly" ${exp.recur_freq==='monthly'?'selected':''}>Monthly</option><option value="quarterly" ${exp.recur_freq==='quarterly'?'selected':''}>Quarterly</option><option value="annually" ${exp.recur_freq==='annually'?'selected':''}>Annually</option></select></div>
        <div><span class="fin-label">End Date</span><input id="exp-end" type="date" class="fin-input" value="${safeDate(exp.recur_end)||''}"/></div>
      </div>
    </div>
    <div style="display:flex;gap:8px;margin-top:20px;">
      <button class="fin-btn fin-btn-primary" style="flex:1;" onclick="window._finSaveExp('${exp.id||''}')">${isNew?'Add Expense':'Save'}</button>
      <button class="fin-btn fin-btn-ghost" onclick="window._finClosePanel()">Cancel</button>
    </div>
    ${!isNew?`<div style="margin-top:12px;text-align:center;"><button onclick="window._finDeleteExpense('${exp.id}')" style="background:none;border:none;color:${LIGHT};font-size:11px;cursor:pointer;">Delete &amp; recurring copies</button></div>`:''}
  `);
}

window._finSaveExp=async function(id){
  const payload={category:el('exp-cat')?.value,description:el('exp-desc')?.value,amount:parseFloat(el('exp-amount')?.value||0),currency:el('exp-currency')?.value||'USD',expense_date:el('exp-date')?.value,is_recurring:el('exp-recur')?.checked?1:0,recur_freq:el('exp-freq')?.value||null,recur_end:el('exp-end')?.value||null};
  try{
    if(id)await api(`/finance/expenses/${id}`,{method:'PATCH',body:JSON.stringify(payload)});
    else await api('/finance/expenses',{method:'POST',body:JSON.stringify(payload)});
    window._finClosePanel();
    if(_finState.tab==='expenses')renderExpensesTab();
    if(_finState.tab==='pl')renderPLTab();
    renderKPIs();
  }catch(e){alert('Save failed: '+e.message);}
};
window._finDeleteExpense=async function(id){
  if(!confirm('Delete this expense and any recurring copies?'))return;
  try{await api(`/finance/expenses/${id}`,{method:'DELETE'});window._finClosePanel();if(_finState.tab==='expenses')renderExpensesTab();if(_finState.tab==='pl')renderPLTab();renderKPIs();}
  catch(e){alert('Delete failed: '+e.message);}
};

window.showFinancePage=async function(){
  _apiBase=(document.querySelector('meta[name="api-base"]')?.content||'').replace(/\/+$/,'');
  const main=document.querySelector('main.vo-wrap')||document.querySelector('main')||document.body;
  ['page-dashboard','page-intake','page-exec','page-receiving','page-reports','page-map'].forEach(id=>{
    const e=document.getElementById(id);if(e){e.classList.add('hidden');e.style.display='none';}
  });
  if(typeof window.hideReceivingPage==='function')window.hideReceivingPage();
  let page=document.getElementById('page-finance');
  if(!page){page=document.createElement('section');page.id='page-finance';main.appendChild(page);}
  page.classList.remove('hidden');page.style.display='block';
  if(!page.dataset.init){
    page.dataset.init='1';injectSkeleton(page);
    fetchFX().then(()=>{const lbl=el('fin-fx-label');if(lbl)lbl.textContent=_finState.fxLabel;});
  }
  if(!_finState.week)_finState.week=getWeeks()[0];
  await renderKPIs();
  window._finTab(_finState.tab||'invoices');
};

})();
