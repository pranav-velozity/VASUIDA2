/*
* Pinpoint Exec — Additive Live Widgets (No baseline changes)
* - Mounts read-only KPIs, Radar (exceptions), Double-Donut (planned vs applied), and 3 exception cards
* - Uses existing global state & helpers: state, weekEndISO, ymdFromCompletedAtInTZ, todayInTZ, mondayOfInTZ, toNum, aggregate, joinPOProgress
* - No edits to existing routes/exports/logic; safe to include after current script.
*/
(function ExecLiveAdditive(){
const BRAND = (typeof window.BRAND !== 'undefined') ? window.BRAND : '#990033';
const BUSINESS_TZ = document.querySelector('meta[name="business-tz"]')?.content || 'Asia/Shanghai';


// ---------- Small utilities ----------
const $ = (s)=>document.querySelector(s);
const fmt = (n)=> Number(n||0).toLocaleString();
const clamp = (v,min,max)=> Math.max(min, Math.min(max, v));
const pct = (num,den)=> den>0 ? Math.round((num*100)/den) : 0;
const weekEndISO = window.weekEndISO || function(ws){ const d=new Date(ws); d.setDate(d.getDate()+6); return iso(d); };
function toISODate(v){
if (typeof window.toISODate === 'function') return window.toISODate(v);
const d = new Date(v); const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const day=String(d.getDate()).padStart(2,'0');
return `${y}-${m}-${day}`;
}
function bizYMDFromRecord(r){
// Prefer date_local (already YYYY-MM-DD in biz TZ), else bucket completed_at -> business day
if (r?.date_local) return String(r.date_local).trim();
if (r?.completed_at && typeof window.ymdFromCompletedAtInTZ === 'function') return window.ymdFromCompletedAtInTZ(r.completed_at, BUSINESS_TZ);
return '';
}


// ---------- Metric computations (scoped to selected week) ----------
function computeExecMetrics(){
const ws = window.state?.weekStart; if (!ws) return null;
const we = weekEndISO(ws);
const plan = Array.isArray(window.state?.plan) ? window.state.plan : [];
const recordsAll = Array.isArray(window.state?.records) ? window.state.records : [];
const bins = Array.isArray(window.state?.bins) ? window.state.bins : [];


// window.state.records already week-filtered in app, but keep a defensive guard
const wkRecords = recordsAll.filter(r=>{
if (r?.status !== 'complete') return false;
const ymd = bizYMDFromRecord(r);
return ymd && ymd >= ws && ymd <= we;
});


// Planned totals
const plannedTotal = plan.reduce((s,p)=> s + (window.toNum? toNum(p.target_qty) : Number(p.target_qty||0)), 0);
const appliedTotal = wkRecords.length;
const completionPct = pct(appliedTotal, plannedTotal);


// Aggregates for discrepancy
const agg = (typeof window.aggregate === 'function') ? window.aggregate(wkRecords) : {byPO:new Map(), bySKU:new Map()};


// --- Discrepancy % (SKU) ---
const planBySKU = new Map();
for (const p of plan){ const sku=String(p.sku_code||'').trim(); if(!sku) continue; planBySKU.set(sku, (planBySKU.get(sku)||0) + (toNum? toNum(p.target_qty):Number(p.target_qty||0))); }
let skuPctSum=0, skuCnt=0;
for (const [sku, planned] of planBySKU.entries()){
const applied = agg.bySKU.get(sku)||0;
if (planned>0){ skuPctSum += Math.abs(applied - planned)/planned; skuCnt++; }
}
const avgSkuDiscPct = Math.round((skuCnt? (skuPctSum/skuCnt) : 0)*100);


// --- Discrepancy % (PO) ---
const planByPO = new Map();
const poDue = new Map();
for (const p of plan){
const po = String(p.po_number||'').trim(); if(!po) continue;
planByPO.set(po,(planByPO.get(po)||0)+(toNum? toNum(p.target_qty):Number(p.target_qty||0)));
const d = String(p.due_date||'').trim();
if (!poDue.has(po)) poDue.set(po,d); else if (d && (!poDue.get(po) || d < poDue.get(po))) poDue.set(po,d);
}
let poPctSum=0, poCnt=0;
for (const [po, planned] of planByPO.entries()){
const applied = agg.byPO.get(po)||0;
if (planned>0){ poPctSum += Math.abs(applied - planned)/planned; poCnt++; }
}
const avgPoDiscPct = Math.round((poCnt? (poPctSum/poCnt) : 0)*100);


// --- Duplicate UIDs (same SKU+UID >1 within the week) ---
const pairCounts = new Map();
for (const r of wkRecords){
const sku = String(r.sku_code||'').trim();
const uid = String(r.uid||'').trim();
if (!sku || !uid) continue;
