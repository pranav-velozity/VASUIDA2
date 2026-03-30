/* ── VelOzity Pinpoint — Finance Module v2 ── */
;(function(){
'use strict';

const BRAND='#990033',DARK='#1C1C1E',MID='#6E6E73',LIGHT='#AEAEB2';
const BG='#F5F5F7',GREEN='#34C759',AMBER='#C8860A',BLUE='#3B82F6';
const EXPENSE_CATS=['Freight Cost','Labour','Software','Office','Duties & Customs','Storage','Marketing','Other'];

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
function weekLabel(ws){
  try{const d=new Date(ws.slice(0,10)+'T00:00:00Z');return'W/C '+d.toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'});}
  catch{return ws;}
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
  const weeks=[],now=new Date(),dow=now.getDay();
  const monday=new Date(now);monday.setDate(now.getDate()-((dow+6)%7));monday.setHours(0,0,0,0);
  for(let i=0;i<12;i++){const d=new Date(monday);d.setDate(monday.getDate()-i*7);weeks.push(d.toISOString().slice(0,10));}
  return weeks;
}

function injectStyles(){
  if(document.getElementById('fin-styles'))return;
  const s=document.createElement('style');s.id='fin-styles';
  s.textContent=`
  #page-finance{background:#F5F5F7;min-height:100vh;}
  .fin-sidebar{width:200px;flex-shrink:0;background:#fff;border-right:0.5px solid rgba(0,0,0,0.08);min-height:calc(100vh - 56px);padding:20px 0;}
  .fin-content{flex:1;padding:24px 28px;overflow-y:auto;}
  .fin-nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;font-size:13px;font-weight:500;color:${MID};cursor:pointer;transition:all .15s;border-left:3px solid transparent;width:100%;box-sizing:border-box;background:none;border-top:none;border-right:none;border-bottom:none;text-align:left;font-family:inherit;}
  .fin-nav-item:hover{background:${BG};color:${DARK};}
  .fin-nav-item.active{color:${BRAND};background:rgba(153,0,51,0.06);border-left-color:${BRAND};}
  .fin-card{background:#fff;border:0.5px solid rgba(0,0,0,0.08);border-radius:14px;padding:20px;}
  .fin-input{border:1px solid rgba(0,0,0,0.12);border-radius:8px;padding:7px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;width:100%;box-sizing:border-box;transition:border-color .15s;background:#fff;}
  .fin-input:focus{border-color:${BRAND};}
  .fin-btn{border:none;border-radius:8px;padding:8px 16px;font-size:12px;font-weight:600;font-family:inherit;cursor:pointer;transition:all .15s;}
  .fin-btn-primary{background:${BRAND};color:#fff;}
  .fin-btn-primary:hover{background:#7a0029;}
  .fin-btn-ghost{background:${BG};color:${DARK};border:0.5px solid rgba(0,0,0,0.08);}
  .fin-btn-ghost:hover{background:rgba(0,0,0,0.06);}
  .fin-tbl{width:100%;border-collapse:collapse;font-size:12px;}
  .fin-tbl th{font-size:10px;font-weight:600;color:${LIGHT};text-transform:uppercase;letter-spacing:0.04em;padding:8px 12px;text-align:left;border-bottom:0.5px solid rgba(0,0,0,0.07);}
  .fin-tbl td{padding:10px 12px;border-bottom:0.5px solid rgba(0,0,0,0.05);color:${DARK};vertical-align:middle;}
  .fin-tbl tr:last-child td{border-bottom:none;}
  .fin-tbl tr:hover td{background:${BG};}
  .fin-label{font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:5px;display:block;}
  .fin-section-title{font-size:13px;font-weight:600;color:${DARK};margin-bottom:14px;}
  .fin-kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;}
  .fin-panel{position:fixed;top:0;right:0;width:500px;height:100vh;background:#fff;border-left:0.5px solid rgba(0,0,0,0.1);box-shadow:-8px 0 40px rgba(0,0,0,0.1);transform:translateX(100%);transition:transform .3s cubic-bezier(0.4,0,0.2,1);z-index:300;overflow-y:auto;padding:24px 22px 60px;}
  .fin-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.2);z-index:299;display:none;}
  `;
  document.head.appendChild(s);
}

function injectSkeleton(host){
  injectStyles();
  const weeks=getWeeks();
  host.innerHTML=`
  <div style="display:flex;align-items:stretch;">
    <div class="fin-sidebar">
      <div style="padding:0 20px 16px;border-bottom:0.5px solid rgba(0,0,0,0.06);margin-bottom:12px;">
        <div style="font-size:14px;font-weight:700;color:${DARK};">Finance</div>
        <div style="font-size:10px;color:${LIGHT};margin-top:2px;">Admin only</div>
      </div>
      <button class="fin-nav-item active" id="fin-nav-invoices" onclick="window._finTab('invoices')"><span style="font-size:16px;">🧾</span>Invoices</button>
      <button class="fin-nav-item" id="fin-nav-pl" onclick="window._finTab('pl')"><span style="font-size:16px;">📊</span>P&amp;L</button>
      <button class="fin-nav-item" id="fin-nav-expenses" onclick="window._finTab('expenses')"><span style="font-size:16px;">💳</span>Expenses</button>
      <div style="margin:16px 20px;height:0.5px;background:rgba(0,0,0,0.06);"></div>
      <div id="fin-sidebar-week" style="padding:0 14px 12px;">
        <div class="fin-label" style="padding:0 4px;">Week</div>
        <select id="fin-week-sel" class="fin-input" onchange="window._finSelectWeek(this.value)">
          ${weeks.map(w=>`<option value="${w}">${weekLabel(w)}</option>`).join('')}
        </select>
      </div>
      <div style="padding:0 14px 12px;">
        <div class="fin-label" style="padding:0 4px;">Currency</div>
        <select id="fin-currency" class="fin-input" onchange="window._finCurrencyChange(this.value)">
          <option value="USD">USD</option><option value="AUD">AUD</option><option value="EUR">EUR</option><option value="GBP">GBP</option>
        </select>
        <div id="fin-fx-label" style="font-size:9px;color:${LIGHT};margin-top:4px;padding:0 4px;"></div>
      </div>
    </div>
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
    cont.innerHTML=tiles.map(t=>`<div class="fin-card">
      <div class="fin-label">${t.label}</div>
      <div style="font-size:22px;font-weight:700;color:${DARK};letter-spacing:-0.02em;margin-bottom:4px;">${t.value}</div>
      <div style="font-size:10px;color:${t.color};font-weight:500;">${t.sub}</div>
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
      <div><span class="fin-label">Reference</span><div style="font-size:12px;font-weight:600;color:${DARK};padding:8px 10px;background:${BG};border-radius:8px;">${esc(inv.ref_number||'Auto-generated')}</div></div>
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
  const payload={type,week_start:ws,invoice_date:el('fin-inv-date')?.value||isoToday(),due_date:el('fin-inv-due')?.value||'',status:el('fin-inv-status')?.value||'draft',notes:el('fin-inv-notes')?.value||'',customs,misc_total,lines:[...mL,...xL,...cL]};
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

async function renderPLTab(){
  const cont=el('fin-tab-pl');if(!cont)return;
  cont.innerHTML=`<div style="color:${LIGHT};font-size:12px;">Loading P&L…</div>`;
  try{
    const year=new Date().getUTCFullYear();
    const pl=await api(`/finance/pl?year=${year}`);
    _finState.pl=pl;const ytd=pl.ytd||{},months=pl.months||[];
    cont.innerHTML=`
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
        <div style="font-size:16px;font-weight:700;color:${DARK};">Profit &amp; Loss ${year}</div>
        <button class="fin-btn fin-btn-primary" onclick="window._finAddExpense()">+ Add Expense</button>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;">
        <div class="fin-card"><div class="fin-label">Revenue YTD</div><div style="font-size:22px;font-weight:700;color:${GREEN};">${fmtUSD(ytd.revenue)}</div><div style="font-size:10px;color:${MID};">paid invoices</div></div>
        <div class="fin-card"><div class="fin-label">Expenses YTD</div><div style="font-size:22px;font-weight:700;color:${AMBER};">${fmtUSD(ytd.expenses)}</div><div style="font-size:10px;color:${MID};">all categories</div></div>
        <div class="fin-card"><div class="fin-label">Net YTD</div><div style="font-size:22px;font-weight:700;color:${ytd.net>0?GREEN:BRAND};">${fmtUSD(ytd.net)}</div><div style="font-size:10px;color:${MID};">rev − exp</div></div>
        <div class="fin-card"><div class="fin-label">Margin</div><div style="font-size:22px;font-weight:700;color:${ytd.margin_pct>20?GREEN:ytd.margin_pct>0?AMBER:BRAND};">${ytd.margin_pct}%</div><div style="font-size:10px;color:${MID};">net ÷ revenue</div></div>
      </div>
      <div style="display:grid;grid-template-columns:2fr 1fr;gap:16px;margin-bottom:24px;">
        <div class="fin-card"><div class="fin-section-title">Revenue vs Expenses</div><div style="height:180px;"><canvas id="fin-chart-rev"></canvas></div></div>
        <div class="fin-card"><div class="fin-section-title">Expense Mix</div><div style="height:180px;"><canvas id="fin-chart-exp"></canvas></div></div>
      </div>
      <div class="fin-card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
          <div class="fin-section-title" style="margin-bottom:0;">Monthly Breakdown</div>
          <div style="font-size:10px;color:${LIGHT};">Click row to expand</div>
        </div>
        <table class="fin-tbl">
          <thead><tr><th>Month</th><th>Revenue</th><th>Expenses</th><th>Net</th><th>Margin</th><th></th></tr></thead>
          <tbody id="fin-pl-tbody"></tbody>
        </table>
      </div>`;
    const tbody=el('fin-pl-tbody');
    if(tbody)tbody.innerHTML=months.map(m=>{
      const hasData=m.revenue>0||m.expenses>0;
      const mn=new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'long',year:'numeric'});
      const mc=m.margin_pct>20?GREEN:m.margin_pct>0?AMBER:BRAND;
      return`<tr style="cursor:pointer;" onclick="window._finToggleMonth('${m.month_key}')">
        <td style="font-weight:500;">${mn}</td>
        <td style="color:${m.revenue>0?GREEN:LIGHT};font-weight:${m.revenue>0?600:400};">${m.revenue>0?fmtUSD(m.revenue):'—'}</td>
        <td style="color:${m.expenses>0?AMBER:LIGHT};font-weight:${m.expenses>0?600:400};">${m.expenses>0?fmtUSD(m.expenses):'—'}</td>
        <td style="font-weight:600;color:${m.net>0?GREEN:m.net<0?BRAND:LIGHT};">${hasData?fmtUSD(m.net):'—'}</td>
        <td>${hasData?`<span style="font-weight:600;color:${mc};">${m.margin_pct}%</span>`:'—'}</td>
        <td style="text-align:right;"><button class="fin-btn fin-btn-ghost" style="font-size:10px;padding:3px 10px;" onclick="event.stopPropagation();window._finAddExpense('${m.month_key}')">+ Expense</button></td>
      </tr>
      <tr id="fin-pl-expand-${m.month_key}" style="display:none;background:${BG};">
        <td colspan="6" style="padding:0;"><div id="fin-pl-ec-${m.month_key}" style="padding:14px 16px;"></div></td>
      </tr>`;
    }).join('');
    loadChartJS(()=>renderPLCharts(months));
    window._finToggleMonth=async function(mk){
      const row=el('fin-pl-expand-'+mk);if(!row)return;
      if(row.style.display==='none'){
        row.style.display='';
        const c=el('fin-pl-ec-'+mk);if(!c)return;
        c.innerHTML=`<div style="color:${LIGHT};font-size:11px;">Loading…</div>`;
        try{
          const exps=await api(`/finance/expenses?month_key=${mk}`);
          const m=months.find(m=>m.month_key===mk);
          const invs=m?.invoices||[];
          c.innerHTML=`<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
            <div><div class="fin-label">Revenue</div>${invs.length?invs.map(i=>`<div style="display:flex;justify-content:space-between;font-size:11px;padding:5px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
              <span style="color:${MID};">${typeIcon(i.type||'')} ${esc(i.ref||i.type)}</span><span style="font-weight:600;color:${GREEN};">${fmtUSD(i.amount)}</span></div>`).join(''):`<div style="font-size:11px;color:${LIGHT};">No invoices</div>`}
            </div>
            <div><div class="fin-label">Expenses <button onclick="window._finAddExpense('${mk}')" style="background:none;border:none;color:${BRAND};cursor:pointer;font-size:10px;font-weight:600;margin-left:6px;">+ Add</button></div>
              ${exps.length?exps.map(e=>`<div style="display:flex;justify-content:space-between;align-items:center;font-size:11px;padding:5px 0;border-bottom:0.5px solid rgba(0,0,0,0.05);">
                <div><span style="color:${DARK};">${esc(e.description)}</span><span style="color:${LIGHT};margin-left:5px;font-size:9px;">${esc(e.category)}</span>${e.is_recurring?`<span style="color:${BLUE};margin-left:3px;font-size:9px;">↻</span>`:''}</div>
                <div style="display:flex;align-items:center;gap:8px;"><span style="font-weight:600;color:${AMBER};">${fmtUSD(e.amount)}</span>
                  <button onclick="window._finEditExpense('${e.id}')" style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:11px;">✎</button>
                  <button onclick="window._finDeleteExpense('${e.id}')" style="background:none;border:none;color:${LIGHT};cursor:pointer;font-size:11px;">✕</button>
                </div></div>`).join(''):`<div style="font-size:11px;color:${LIGHT};">No expenses yet</div>`}
            </div></div>`;
        }catch(e){c.innerHTML=`<div style="color:${BRAND};font-size:11px;">${e.message}</div>`;}
      }else row.style.display='none';
    };
  }catch(e){cont.innerHTML=`<div style="color:${BRAND};padding:20px;">P&L failed: ${esc(e.message)}</div>`;}
}

function loadChartJS(cb){if(window.Chart){cb();return;}const s=document.createElement('script');s.src='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';s.onload=cb;document.head.appendChild(s);}
function renderPLCharts(months){
  const rE=el('fin-chart-rev'),eE=el('fin-chart-exp');if(!rE||!eE||!window.Chart)return;
  if(window._fcR)window._fcR.destroy();if(window._fcE)window._fcE.destroy();
  const labels=months.map(m=>new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short'}));
  window._fcR=new Chart(rE,{type:'bar',data:{labels,datasets:[
    {label:'Revenue',data:months.map(m=>m.revenue),backgroundColor:'rgba(52,199,89,0.65)',borderRadius:4},
    {label:'Expenses',data:months.map(m=>m.expenses),backgroundColor:'rgba(200,134,10,0.55)',borderRadius:4}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'top',align:'end',labels:{font:{size:10},boxWidth:8}}},
      scales:{x:{ticks:{font:{size:10}},grid:{display:false}},y:{ticks:{font:{size:10},callback:v=>'$'+v.toLocaleString()},grid:{color:'rgba(0,0,0,0.04)'},beginAtZero:true}}}});
  const em=months.filter(m=>m.expenses>0);
  if(em.length)window._fcE=new Chart(eE,{type:'doughnut',data:{labels:em.map(m=>new Date(m.month_key+'-01T00:00:00Z').toLocaleDateString('en-AU',{month:'short'})),
    datasets:[{data:em.map(m=>m.expenses),backgroundColor:['#990033','#C8860A','#4A9B8E','#3B82F6','#8B5CF6','#F97316','#059669','#6B7280','#EC4899','#EAB308','#14B8A6','#F43F5E'],borderWidth:2,borderColor:'#fff'}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{font:{size:9},boxWidth:8,padding:6}}}}});
}

async function renderExpensesTab(){
  const cont=el('fin-tab-expenses');if(!cont)return;
  cont.innerHTML=`<div style="color:${LIGHT};font-size:12px;">Loading…</div>`;
  try{
    const expenses=await api('/finance/expenses');_finState.expenses=expenses;
    const catTotals={};for(const e of expenses)catTotals[e.category]=(catTotals[e.category]||0)+parseFloat(e.amount||0);
    cont.innerHTML=`
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
        <div style="font-size:16px;font-weight:700;color:${DARK};">Expenses</div>
        <button class="fin-btn fin-btn-primary" onclick="window._finAddExpense()">+ Add Expense</button>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px;">
        ${EXPENSE_CATS.map(cat=>`<div class="fin-card" style="padding:14px;cursor:pointer;" onclick="window._finFilterExp('${cat}')">
          <div class="fin-label">${cat}</div>
          <div style="font-size:16px;font-weight:700;color:${(catTotals[cat]||0)>0?DARK:LIGHT};">${(catTotals[cat]||0)>0?fmtUSD(catTotals[cat]):'—'}</div>
        </div>`).join('')}
      </div>
      <div class="fin-card"><div id="fin-exp-list"></div></div>`;
    renderExpList(expenses);
    window._finFilterExp=async function(cat){renderExpList(await api(`/finance/expenses?category=${encodeURIComponent(cat)}`));};
  }catch(e){cont.innerHTML=`<div style="color:${BRAND};padding:20px;">${esc(e.message)}</div>`;}
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
  renderExpEditor({id:null,category:'Other',description:'',amount:0,currency:'USD',expense_date:defaultMonth?defaultMonth+'-01':isoToday(),is_recurring:0,recur_freq:'monthly',recur_end:''});
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
    <div style="margin-bottom:12px;"><span class="fin-label">Category</span><select id="exp-cat" class="fin-input">${EXPENSE_CATS.map(c=>`<option value="${c}" ${c===exp.category?'selected':''}>${c}</option>`).join('')}</select></div>
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
