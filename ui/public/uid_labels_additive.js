/* ── VelOzity Pinpoint — UID labels: the print queue (VAS Ops) v6 ──
   #page-uid-labels, route #uid-labels, reached from VAS Ops ▾ → UID labels. Internal only.

   What the page is for: the warehouse opens it, picks the week, and downloads one PDF per PO —
   one at a time or the whole run as a zip. Everything else is bookkeeping the ledger already
   did: what the plan asked for, what has been issued, what moved from another week, which
   department codes are new.

   Shape (from the agreed mockup):
     · one table. Runs are tabs on the card (newest first), then "All issued". Each row carries
       labels in this run, issued/planned and a status, so there is no second table to consult;
     · one place to download: search, selection and the zip button sit in a bar directly above
       the table; each row has one PDF button and a ⋯ menu for the rest;
     · the header has two buttons: Setup (department codes + sample sheets, in a drawer) and the
       primary Check plan now;
     · Pinpoint colours: black / white / #990033; the status palette only on status.

   Rules that show on screen (the API enforces them; see api/uid_label_routes.js):
     · a run is created by a plan upload (delta only) or by an Extras request (new UIDs);
     · a moved PO prints nothing — the warehouse chooses: download again (same UIDs) or issue new;
     · a department code the table has never seen prints as standard and is flagged New.

   No framework, no build step; same conventions as elevate_additive.js. */
;(function () {
  'use strict';
  if (window.__UID_LABELS_LOADED__) return;
  window.__UID_LABELS_LOADED__ = true;
  const VERSION = '7';

  const INK = '#121212', MUTED = '#6E6E73', LINE = '#E3E3E0', SOFT = '#EFEFEC', BG = '#F4F4F2';
  const BRAND = '#990033', LIME = '#C7EA46', YELLOW = '#F5BD25';
  const REFRESH_MS = 2 * 60 * 1000;

  // ── API ──
  const apiBase = () => ((document.querySelector('meta[name="api-base"]') || {}).content || '').replace(/\/+$/, '');
  async function token() { try { return (window.Clerk && window.Clerk.session) ? await window.Clerk.session.getToken() : null; } catch (_) { return null; } }
  async function req(method, path, body, opts) {
    const t = await token(); const h = {};
    if (body) h['Content-Type'] = 'application/json';
    if (t) h.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, { method, headers: h, body: body ? JSON.stringify(body) : undefined });
    if (opts && opts.raw) { if (!r.ok) { let d = null; try { d = await r.json(); } catch (_) {} throw new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); } return r; }
    let d = null; try { d = await r.json(); } catch (_) { d = null; }
    if (!r.ok) throw new Error((d && (d.message || d.error)) || ('HTTP ' + r.status));
    return d;
  }
  async function download(path, fallbackName) {
    const r = await req('GET', path, null, { raw: true });
    const blob = await r.blob();
    const m = (r.headers.get('content-disposition') || '').match(/filename="?([^";]+)"?/);
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = (m && m[1]) || fallbackName;
    document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }

  // ── Helpers ──
  const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const isDate = (v) => /^\d{4}-\d{2}-\d{2}$/.test(String(v || ''));
  const toD = (s) => new Date(String(s).slice(0, 10) + 'T00:00:00Z');
  const addD = (s, n) => { const d = toD(s); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const mondayOf = (s) => { const d = toD(s); return addD(s, -((d.getUTCDay() + 6) % 7)); };
  const todayLocal = () => { try { return new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' }).format(new Date()); } catch (_) { return new Date().toISOString().slice(0, 10); } };
  const isoWeek = (ymd) => { const d = toD(ymd); d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7)); const y0 = new Date(Date.UTC(d.getUTCFullYear(), 0, 1)); return 'W' + String(Math.ceil(((d - y0) / 86400000 + 1) / 7)).padStart(2, '0'); };
  // Shown in the viewer's own time zone — the warehouse sees Sydney, an admin abroad sees theirs.
  const asDate = (iso) => new Date(String(iso).replace(' ', 'T') + (String(iso).endsWith('Z') ? '' : 'Z'));
  const when = (iso) => { try { return asDate(iso).toLocaleString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }); } catch (_) { return String(iso || ''); } };
  const whenShort = (iso) => { try { return asDate(iso).toLocaleString('en-AU', { weekday: 'short', hour: 'numeric', minute: '2-digit' }); } catch (_) { return String(iso || ''); } };
  const fmtNum = (n) => Number(n || 0).toLocaleString('en-AU');
  const plural = (n, w, p) => `${fmtNum(n)} ${n === 1 ? w : (p || w + 's')}`;
  // A person's name or email; a raw auth id (runs stamped before names were recorded) is not worth showing.
  const person = (s) => { const v = String(s || '').trim(); return !v || v === 'user' || /^user_[A-Za-z0-9]+$/.test(v) ? '' : v.replace(/\buser_[A-Za-z0-9]+\b/g, '').replace(/\s*·\s*$/, '').trim(); };
  // trigger_by is "plan upload · who" or "Check plan now · who" (or just who, for extras).
  function trigger(r) {
    const raw = person(r.by);
    const m = /^(plan upload|Check plan now)(?:\s*·\s*(.*))?$/i.exec(raw);
    const how = m ? (/^plan/i.test(m[1]) ? 'From the plan upload' : 'Check plan now') : (r.kind === 'extra' ? 'Extra labels' : '');
    const who = m ? (m[2] || '').trim() : raw;
    return { how, who };
  }

  // ── State ──
  const S = { root: null, weeks: null, week: null, data: null, loading: false, error: null, loadedAt: 0, timer: null,
              busy: null, toast: '', toastTimer: null, tab: null /* run id | 'all' */, q: '', unsel: {},
              menu: null /* po */, extra: null, setupOpen: false, codes: null, codesErr: '', addFmt: 'footwear', otherWeek: false };

  // ── Styles ──
  function styles() {
    if (document.getElementById('ul-css')) return;
    const st = document.createElement('style'); st.id = 'ul-css';
    st.textContent = `
.ul{font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK};box-sizing:border-box;padding:28px 28px 56px;max-width:1360px;margin:0 auto;display:flex;flex-direction:column;gap:22px;-webkit-font-smoothing:antialiased}
.ul *{box-sizing:border-box}
:where(.ul) :where(button){font-family:inherit;cursor:pointer;color:inherit;background:none;border:0;padding:0}
.ul button:disabled{opacity:.45;cursor:default}
.ul input,.ul select{font-family:inherit;font-size:13px;color:${INK}}
.ul-mono{font-family:'Geist Mono',ui-monospace,Menlo,monospace}
.ul-k{font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:${MUTED};font-weight:500}
.ul-head{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap}
.ul-head h1{margin:0;font-size:34px;font-weight:600;letter-spacing:-.02em;line-height:1}
.ul-note{font-size:13.5px;color:${MUTED}}
.ul-btn{height:36px;padding:0 14px;border-radius:9px;border:1px solid #D6D6D2;background:#fff;font-size:13px;font-weight:500;display:inline-flex;align-items:center;gap:8px;white-space:nowrap}
.ul-btn:hover{border-color:#B8B8B4}
.ul-btn.p{background:${BRAND};border-color:${BRAND};color:#fff}
.ul-btn.p:hover{background:#7E0029;border-color:#7E0029}
.ul-btn.d{background:${INK};border-color:${INK};color:#fff}
.ul-btn.sm{height:30px;padding:0 11px;font-size:12.5px;border-radius:8px}
.ul-btn.ghost{border-color:transparent;background:transparent;color:${MUTED}}
.ul-btn.ghost:hover{color:${INK};border-color:transparent}
.ul-chip{height:30px;padding:0 13px;border-radius:15px;border:1px solid #D6D6D2;background:#fff;font-size:12.5px;font-weight:500;display:inline-flex;align-items:center;gap:7px}
.ul-chip[aria-pressed="true"]{background:${INK};color:#fff;border-color:${INK}}
.ul-chip.has::after{content:'';width:6px;height:6px;border-radius:50%;background:${LIME}}
.ul-chip.muted{color:${MUTED}}
.ul-wkin{height:30px;border:1px solid #D6D6D2;border-radius:15px;padding:0 10px;font-size:12px;background:#fff;color:${MUTED}}
.ul-stats{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}
@media (max-width:1100px){.ul-stats{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}}
.ul-stat{background:#fff;border:1px solid ${LINE};border-radius:14px;padding:16px 18px;min-width:0}
.ul-stat .v{font-size:28px;font-weight:600;letter-spacing:-.02em;margin-top:6px;line-height:1}
.ul-stat .v.t{font-size:19px;padding-top:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ul-stat .s{font-size:12.5px;color:${MUTED};margin-top:6px;line-height:1.35}
.ul-card{background:#fff;border:1px solid ${LINE};border-radius:14px;overflow:visible}
.ul-tabs{display:flex;align-items:center;gap:4px;border-bottom:1px solid ${LINE};padding:0 18px;overflow-x:auto}
.ul-tab{height:44px;padding:0 4px;margin-right:18px;font-size:13px;font-weight:500;color:${MUTED};display:inline-flex;align-items:center;gap:8px;border-bottom:2px solid transparent;white-space:nowrap;flex-shrink:0}
.ul-tab[aria-selected="true"]{color:${INK};border-bottom-color:${BRAND}}
.ul-tab .n{font-family:'Geist Mono',ui-monospace,Menlo,monospace;font-size:11.5px;color:${MUTED}}
.ul-tabs .right{margin-left:auto;display:flex;align-items:center;gap:8px;padding-left:12px;flex-shrink:0}
.ul-bar{display:flex;justify-content:space-between;align-items:center;gap:14px;padding:14px 18px;border-bottom:1px solid ${LINE};flex-wrap:wrap}
.ul-bar .l{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.ul-search{height:36px;width:320px;max-width:100%;border:1px solid #D6D6D2;border-radius:9px;padding:0 12px 0 34px;font-size:13px;outline:none;background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%236E6E73' stroke-width='2'%3E%3Ccircle cx='11' cy='11' r='7'/%3E%3Cpath d='m20 20-3.5-3.5'/%3E%3C/svg%3E") 11px center no-repeat}
.ul-search:focus{border-color:${INK}}
.ul-search::-webkit-search-cancel-button{-webkit-appearance:none;display:none}
.ul-table{border-collapse:separate;border-spacing:0;width:100%;font-size:13px}
.ul-table th{text-align:left;font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:${MUTED};font-weight:500;padding:10px 12px;border-bottom:1px solid ${LINE};white-space:nowrap}
.ul-table td{padding:10px 12px;border-bottom:1px solid ${SOFT};vertical-align:middle;white-space:nowrap}
.ul-table tbody tr:last-child td{border-bottom:0}
.ul-table tbody tr:hover td{background:#FAFAF8}
.ul-table th.num,.ul-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.ul-table td.act{text-align:right;position:relative;white-space:nowrap}
.ul-table input[type=checkbox]{width:15px;height:15px;accent-color:${INK};cursor:pointer;vertical-align:middle;margin:0}
.ul-tag{display:inline-flex;align-items:center;height:22px;padding:0 9px;border-radius:11px;font-size:11.5px;font-weight:600;white-space:nowrap}
.ul-tag.fw{background:${INK};color:#fff}
.ul-tag.st{background:#fff;color:${INK};border:1px solid #D6D6D2}
.ul-tag.ok{background:${LIME};color:#1F2A00}
.ul-tag.new{background:${YELLOW};color:#2B2000}
.ul-tag.bad{background:${BRAND};color:#fff}
.ul-tag.mv{background:#EDEDEA;color:${INK}}
.ul-tag.xs{height:18px;font-size:10.5px;padding:0 7px}
.ul-more{width:30px;height:30px;border-radius:8px;border:1px solid transparent;display:inline-flex;align-items:center;justify-content:center;color:${MUTED};font-size:16px;line-height:1;vertical-align:middle}
.ul-more:hover,.ul-more[aria-expanded="true"]{border-color:#D6D6D2;color:${INK}}
.ul-menu{position:absolute;right:12px;top:calc(100% - 6px);z-index:30;min-width:260px;background:#fff;border:1px solid ${LINE};border-radius:10px;box-shadow:0 12px 32px rgba(0,0,0,.14);padding:6px;text-align:left}
.ul-menu button{display:flex;justify-content:space-between;align-items:center;gap:14px;width:100%;padding:9px 10px;border-radius:7px;font-size:13px;white-space:nowrap}
.ul-menu button:hover{background:#F5F5F3}
.ul-menu button small{color:${MUTED};font-size:11.5px}
.ul-menu .info{padding:8px 10px 4px;font-size:11.5px;color:${MUTED};border-top:1px solid ${SOFT};margin-top:4px}
.ul-extra{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:10px 12px;background:#FAFAF8;border:1px solid ${LINE};border-radius:9px}
.ul-extra select,.ul-extra input{height:32px;border:1px solid #D6D6D2;border-radius:7px;padding:0 9px;background:#fff}
.ul-foot{padding:12px 18px;font-size:12.5px;color:${MUTED};border-top:1px solid ${SOFT};display:flex;gap:18px;flex-wrap:wrap}
.ul-foot b{color:${INK};font-weight:600}
.ul-err{font-size:12.5px;color:${BRAND}}
.ul-empty{padding:28px 18px;font-size:13.5px;color:${MUTED}}
.ul-dim{position:fixed;inset:0;background:rgba(18,18,18,.22);z-index:9000}
.ul-panel{position:fixed;top:0;right:0;bottom:0;width:min(640px,100vw);background:#fff;z-index:9001;box-shadow:-16px 0 48px rgba(0,0,0,.18);display:flex;flex-direction:column;font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK}}
.ul-panel *{box-sizing:border-box}
:where(.ul-panel) :where(button){font-family:inherit;cursor:pointer;color:inherit;background:none;border:0;padding:0}
.ul-panelh{display:flex;justify-content:space-between;align-items:flex-start;padding:22px 24px 18px;border-bottom:1px solid ${LINE}}
.ul-panelh h2{margin:0;font-size:20px;font-weight:600}
.ul-panelb{padding:20px 24px;overflow:auto;flex:1;display:flex;flex-direction:column;gap:26px}
.ul-sheet{display:flex;align-items:center;gap:14px;padding:12px 14px;border:1px solid ${LINE};border-radius:12px;background:#fff;flex:1 1 220px}
.ul-sheet .sw{background:#fff;border:1px solid ${INK};display:block;flex-shrink:0}
.ul-seg{display:inline-flex;background:#EDEDEA;border-radius:8px;padding:2px;gap:2px}
.ul-seg button{height:26px;padding:0 11px;border-radius:6px;font-size:12px;font-weight:500;color:${MUTED}}
.ul-seg button[aria-pressed="true"]{background:${INK};color:#fff}
.ul-add{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.ul-add input{height:36px;border:1px solid #D6D6D2;border-radius:9px;padding:0 12px;font-size:13px;flex:1 1 220px;font-family:'Geist Mono',ui-monospace,Menlo,monospace;text-transform:uppercase}
.ul-toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:${INK};color:#fff;padding:10px 16px;border-radius:10px;font-size:13px;z-index:9100;box-shadow:0 8px 24px rgba(0,0,0,.2)}
.ul-x{width:34px;height:34px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;color:${MUTED}}
.ul-x:hover{background:${SOFT};color:${INK}}
.ul-skel{background:linear-gradient(90deg,#F2F2EF 25%,#E8E8E4 50%,#F2F2EF 75%);background-size:200% 100%;animation:ulSk 1.2s infinite;border-radius:14px}
@keyframes ulSk{0%{background-position:200% 0}100%{background-position:-200% 0}}
`;
    document.head.appendChild(st);
  }
  const I = {
    dl: () => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg>`,
    x: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>`,
  };

  // ── Derived ──
  const fmtTag = (f) => f === 'footwear' ? `<span class="ul-tag fw">Footwear</span>` : `<span class="ul-tag st">Standard</span>`;
  const currentRun = () => (S.data && S.tab !== 'all') ? S.data.runs.find(r => r.id === S.tab) : null;
  function poInfo(po) { return (S.data && S.data.pos.find(p => p.po === po)) || null; }
  // Rows for the open tab: a run's files joined with the week's PO facts, or every PO of the week.
  function rows() {
    const d = S.data; if (!d) return [];
    const run = currentRun();
    let out;
    if (run) {
      out = (run.files || []).map(f => { const p = poInfo(f.po) || { po: f.po, dept: f.po.slice(0, 4), format: f.format, planned: 0, issued: f.labels, skus: 0, lines: [], moved_from: null }; return { ...p, inRun: f.labels, reprints: f.reprints, last_reprint: f.last_reprint, format: f.format || p.format }; });
      // POs that moved into this week sit alongside: the warehouse chooses what to do with them.
      for (const mf of (d.moved_files || [])) if (!out.some(r => r.po === mf.po)) { const p = poInfo(mf.po); if (p) out.push({ ...p, inRun: null, movedFile: mf }); }
    } else {
      out = d.pos.map(p => ({ ...p, inRun: null, movedFile: (d.moved_files || []).find(mf => mf.po === p.po) || null }));
    }
    const q = S.q.trim().toLowerCase();
    if (q) out = out.filter(r => r.po.toLowerCase().includes(q) || (r.lines || []).some(l => l.sku.toLowerCase().includes(q)));
    return out.sort((a, b) => a.po.localeCompare(b.po));
  }
  const selKey = () => S.tab === 'all' ? 'all' : String(S.tab);
  function selectable(r) { return S.tab !== 'all' && r.inRun != null; }

  // ── Render ──
  function renderHeader() {
    const wk = S.data;
    const weeks = (S.weeks && S.weeks.weeks) || [];
    const thisMon = mondayOf(todayLocal());
    const set = new Set(weeks.slice(0, 5).map(w => w.week_start)); set.add(thisMon); for (let i = 1; i <= 2; i++) set.add(addD(thisMon, -7 * i)); if (S.week) set.add(S.week);
    const chips = [...set].sort().reverse().slice(0, 6).map(w => {
      const has = weeks.some(x => x.week_start === w && x.labels > 0);
      return `<button class="ul-chip${has ? ' has' : ''}" data-act="week" data-v="${w}" aria-pressed="${S.week === w}" title="week of ${esc(w)}">${isoWeek(w)}</button>`;
    }).join('');
    const newCodes = S.weeks ? S.weeks.new_codes : 0;
    return `<div class="ul-head">
      <div style="display:flex;flex-direction:column;gap:6px">
        <div class="ul-k">Pinpoint · VAS Ops</div>
        <h1>UID labels</h1>
        <div class="ul-note">Issued from the plan the moment it is uploaded. One PDF per PO; the warehouse prints the latest run.</div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <button class="ul-btn" data-act="setup">Setup <span style="color:${MUTED};font-weight:400">· codes &amp; samples</span>${newCodes ? ` <span class="ul-tag new xs">${newCodes} new</span>` : ''}</button>
        <button class="ul-btn p" data-act="reconcile" ${S.busy === 'reconcile' || !wk || !wk.has_plan ? 'disabled' : ''} title="Issue anything the ledger still lacks for this week's stored plan">${S.busy === 'reconcile' ? 'Checking…' : 'Check plan now'}</button>
      </div></div>
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <span style="font-size:12.5px;color:${MUTED};margin-right:4px">Week</span>${chips}
      ${S.otherWeek ? `<input class="ul-wkin" type="date" data-act="week-pick" value="${esc(S.week || '')}" aria-label="Any week">` : `<button class="ul-chip muted" data-act="other-week">Other week…</button>`}
    </div>`;
  }

  function renderStats() {
    const d = S.data; if (!d) return '';
    const fw = d.pos.filter(p => p.format === 'footwear'), st = d.pos.filter(p => p.format !== 'footwear');
    const over = d.pos.filter(p => p.issued > p.planned).length, short = d.pos.filter(p => p.issued < p.planned).length;
    const stat = (k, v, s, cls) => `<div class="ul-stat"><div class="ul-k">${k}</div><div class="v${cls ? ' ' + cls : ''}">${v}</div>${s ? `<div class="s">${s}</div>` : ''}</div>`;
    const issuedSub = short ? `<span class="ul-tag bad xs">${plural(short, 'PO')} short</span>` : over ? `<span class="ul-tag mv xs">${plural(over, 'PO')} over plan</span>` : d.totals.issued ? `<span class="ul-tag ok xs">Matches the plan</span>` : 'nothing issued yet';
    const r = d.runs[0];
    let latest = stat('Latest run', '—', d.has_plan ? 'nothing issued yet' : 'no plan stored for this week', 't');
    if (r) {
      const t = trigger(r);
      const mail = r.emailed_at ? `<span style="color:#4A6A00;font-weight:600">Warehouse emailed</span>` : r.email_error ? `<span style="color:${BRAND};font-weight:600" title="${esc(r.email_error)}">Email not sent</span>` : `<span style="color:${MUTED}">Emailing…</span>`;
      latest = stat('Latest run', `#${r.id} · ${whenShort(r.at)}`, `${[t.how, t.who ? esc(t.who) : null].filter(Boolean).join(' · ')}<br>${mail}`, 't');
    }
    return `<div class="ul-stats">
      ${stat('Planned units', fmtNum(d.totals.planned), `${plural(d.pos.length, 'PO')} in the ${esc(d.label)} plan`)}
      ${stat('Issued', fmtNum(d.totals.issued), issuedSub)}
      ${stat('Footwear · 36×76', fmtNum(fw.reduce((a, p) => a + p.issued, 0)), plural(fw.length, 'PO'))}
      ${stat('Standard · 76×36', fmtNum(st.reduce((a, p) => a + p.issued, 0)), plural(st.length, 'PO'))}
      ${latest}
    </div>`;
  }

  function statusTag(r) {
    const diff = r.issued - r.planned;
    if (r.moved_from) return `<span class="ul-tag mv">Moved from ${isoWeek(r.moved_from)}</span>`;
    if (diff > 0) return `<span class="ul-tag mv">+${fmtNum(diff)} extras</span>`;
    if (diff < 0) return `<span class="ul-tag bad">${fmtNum(-diff)} short</span>`;
    if (r.issued) return `<span class="ul-tag ok">Issued</span>`;
    return `<span class="ul-tag st">Nothing yet</span>`;
  }

  function renderTable() {
    const d = S.data; if (!d) return '';
    const run = currentRun();
    const list = rows();
    const key = selKey();
    const un = S.unsel[key] || new Set();
    const selAll = list.filter(selectable);
    const selected = (run ? (run.files || []) : []).filter(f => !un.has(f.po));
    const shownSel = selAll.filter(r => !un.has(r.po)).length;
    const allShown = selAll.length > 0 && shownSel === selAll.length, noneShown = shownSel === 0;
    const zipBusy = S.busy === `zip:${key}`;
    const q = S.q.trim();
    const tabs = d.runs.map(r => `<button class="ul-tab" role="tab" aria-selected="${S.tab === r.id}" data-act="tab" data-v="${r.id}">${r.kind === 'extra' ? 'Extras' : 'Run'} #${r.id} <span class="n">${whenShort(r.at)} · ${r.labels ? fmtNum(r.labels) : 'moved in'}</span></button>`).join('')
      + `<button class="ul-tab" role="tab" aria-selected="${S.tab === 'all'}" data-act="tab" data-v="all">All issued <span class="n">${esc(d.label)}</span></button>`;
    const fwN = run ? (run.files || []).filter(f => f.format === 'footwear').reduce((a, f) => a + f.labels, 0) : d.pos.filter(p => p.format === 'footwear').reduce((a, p) => a + p.issued, 0);
    const stN = run ? (run.files || []).filter(f => f.format !== 'footwear').reduce((a, f) => a + f.labels, 0) : d.pos.filter(p => p.format !== 'footwear').reduce((a, p) => a + p.issued, 0);
    const s = run && run.summary ? run.summary : {};
    const notes = [];
    if (run && s.moved && s.moved.length) notes.push(`<b>${plural(s.moved.length, 'PO line')} moved</b> from ${[...new Set(s.moved.map(m => isoWeek(m.from)))].join(', ')} — nothing printed; the row offers <b>Download again</b> or <b>Issue new</b>.`);
    if (run && s.new_codes && s.new_codes.length) notes.push(`<b>New department code${s.new_codes.length === 1 ? '' : 's'}</b> ${s.new_codes.map(c => `<span class="ul-mono">${esc(c.code)}</span>`).join(', ')} printed as standard — set the format under Setup if that is wrong.`);
    if (run && s.over && s.over.length) notes.push(`<b>${plural(s.over.length, 'PO line')}</b> now plan fewer units than already issued — nothing printed.`);
    if (run && s.reason) notes.push(`Reason: ${esc(s.reason)}`);
    if (run && run.email_error) notes.push(`<span class="ul-err">Email not sent — ${esc(run.email_error)}</span> <button class="ul-btn sm ghost" data-act="email" data-v="${run.id}" style="height:24px">Send again</button>`);

    const row = (r) => {
      const canSel = selectable(r);
      const menuOpen = S.menu === r.po;
      const extraOpen = S.extra && S.extra.po === r.po;
      const mf = r.movedFile;
      const actions = mf && r.inRun == null
        ? `<button class="ul-btn sm" data-act="dl-again" data-run="${mf.run_id}" data-po="${esc(r.po)}" title="The same ${mf.labels} labels, same UIDs, printed again">Download again</button> <button class="ul-btn sm" data-act="extra-open" data-po="${esc(r.po)}">Issue new</button>`
        : `${run && r.inRun != null ? `<button class="ul-btn sm" data-act="dl" data-run="${run.id}" data-po="${esc(r.po)}" ${S.busy === `dl:${run.id}:${r.po}` ? 'disabled' : ''}>${S.busy === `dl:${run.id}:${r.po}` ? 'Building…' : 'PDF'}</button>`
            : r.issued ? `<button class="ul-btn sm" data-act="dl-all" data-po="${esc(r.po)}" ${S.busy === `dlall:${r.po}` ? 'disabled' : ''}>${S.busy === `dlall:${r.po}` ? 'Building…' : 'PDF'}</button>` : ''}
           <button class="ul-more" data-act="menu" data-po="${esc(r.po)}" aria-label="More for ${esc(r.po)}" aria-expanded="${menuOpen}">⋯</button>
           ${menuOpen ? renderMenu(r) : ''}`;
      return `<tr>
        <td style="width:36px">${canSel ? `<input type="checkbox" data-act="sel" data-po="${esc(r.po)}" ${un.has(r.po) ? '' : 'checked'} aria-label="Select ${esc(r.po)}">` : ''}</td>
        <td class="ul-mono" style="font-weight:600">${esc(r.po)}</td>
        <td><span class="ul-mono" style="color:${MUTED}">${esc(r.dept)}</span>${r.dept_new ? ` <span class="ul-tag new xs" style="margin-left:4px">New code</span>` : ''}</td>
        <td>${fmtTag(r.format)}</td>
        <td class="num">${run ? (r.inRun != null ? fmtNum(r.inRun) : `<span style="color:${MUTED}">—</span>`) : fmtNum(r.issued)}</td>
        <td class="num">${fmtNum(r.issued)} / ${fmtNum(r.planned)}</td>
        <td>${statusTag(r)}</td>
        <td class="act">${actions}</td></tr>
        ${extraOpen ? `<tr><td colspan="8" style="white-space:normal;padding:4px 12px 12px">${renderExtra(r)}</td></tr>` : ''}`;
    };

    return `<div class="ul-card">
      <div class="ul-tabs" role="tablist">${tabs}
        <div class="right">${fwN ? `<span class="ul-tag fw xs">${fmtNum(fwN)} footwear</span>` : ''}${stN ? `<span class="ul-tag st xs">${fmtNum(stN)} standard</span>` : ''}</div></div>
      <div class="ul-bar">
        <div class="l">
          <input class="ul-search" type="search" placeholder="Find a PO or SKU" value="${esc(S.q)}" data-act="q" aria-label="Find a PO or SKU">
          ${run ? `<span style="font-size:13px;color:${MUTED}">${fmtNum(selected.length)} of ${fmtNum((run.files || []).length)} selected${q ? ` · ${plural(selAll.length, 'match', 'match')} “${esc(q)}”` : ''}</span>
          <button class="ul-btn sm ghost" data-act="sel-all" ${allShown ? 'disabled' : ''}>Select ${q ? 'matching' : 'all'}</button>
          <button class="ul-btn sm ghost" data-act="sel-none" ${noneShown ? 'disabled' : ''}>Deselect ${q ? 'matching' : 'all'}</button>` : `<span style="font-size:13px;color:${MUTED}">${plural(list.length, 'PO')}${q ? ` match “${esc(q)}”` : ' · everything issued this week, all runs'}</span>`}
        </div>
        ${run ? `<button class="ul-btn d" data-act="zip" ${zipBusy || !selected.length ? 'disabled' : ''}>${I.dl()} ${zipBusy ? 'Building zip…' : `Download ${fmtNum(selected.length)} PDF${selected.length === 1 ? '' : 's'} · zip`}</button>` : ''}
      </div>
      ${list.length ? `<div style="overflow-x:auto"><table class="ul-table"><thead><tr>
          <th style="width:36px">${run ? `<input type="checkbox" data-act="sel-toggle-all" ${allShown ? 'checked' : ''} aria-label="Select all shown">` : ''}</th>
          <th>PO</th><th>Dept</th><th>Format</th><th class="num">${run ? 'Labels in run' : 'Labels issued'}</th><th class="num">Issued / planned</th><th>Status</th><th></th></tr></thead>
        <tbody>${list.map(row).join('')}</tbody></table></div>`
        : `<div class="ul-empty">${q ? `No PO matches “${esc(q)}”.` : run ? 'Nothing was printed in this run.' : d.has_plan ? 'No labels issued for this week yet. Uploading the plan issues them; Check plan now does the same by hand.' : 'No plan stored for this week yet. Labels are issued when the plan is uploaded.'}</div>`}
      ${notes.length ? `<div class="ul-foot" style="flex-direction:column;gap:6px">${notes.map(n => `<span>${n}</span>`).join('')}</div>` : ''}
    </div>`;
  }

  function renderMenu(r) {
    const last = r.last_reprint ? `Printed again ${plural(r.reprints, 'time')} · last ${when(r.last_reprint)}` : (r.inRun != null ? 'Never printed again' : '');
    return `<div class="ul-menu" role="menu">
      ${r.issued ? `<button role="menuitem" data-act="dl-all" data-po="${esc(r.po)}"><span>All labels issued for this PO</span><small>${fmtNum(r.issued)} · PDF</small></button>` : ''}
      <button role="menuitem" data-act="extra-open" data-po="${esc(r.po)}"><span>Extra labels…</span><small>new UIDs</small></button>
      ${r.inRun != null && currentRun() ? `<button role="menuitem" data-act="dl-again" data-run="${currentRun().id}" data-po="${esc(r.po)}"><span>Print this run's file again</span><small>same UIDs · recorded</small></button>` : ''}
      ${last ? `<div class="info">${esc(last)}</div>` : ''}
    </div>`;
  }

  function renderExtra(p) {
    const e = S.extra;
    const lines = p.lines && p.lines.length ? p.lines : [{ sku: e.sku, planned: p.planned, issued: p.issued }];
    const opts = lines.map(l => `<option value="${esc(l.sku)}" ${e.sku === l.sku ? 'selected' : ''}>${esc(l.sku)} — planned ${fmtNum(l.planned)}, issued ${fmtNum(l.issued)}</option>`).join('');
    return `<div class="ul-extra">
      <span style="font-size:12.5px;font-weight:600">${p.moved_from ? 'Issue new labels' : 'Extra labels'} for ${esc(p.po)}</span>
      <select data-act="extra-sku" aria-label="SKU">${opts}</select>
      <input type="number" min="1" max="5000" value="${e.qty}" data-act="extra-qty" style="width:90px" aria-label="How many">
      <input type="text" placeholder="Why (optional) — torn, lost…" value="${esc(e.reason)}" data-act="extra-reason" style="flex:1 1 180px" aria-label="Reason">
      <button class="ul-btn p sm" data-act="extra-go" ${S.busy === 'extra' ? 'disabled' : ''}>${S.busy === 'extra' ? 'Issuing…' : 'Issue new UIDs'}</button>
      <button class="ul-btn sm ghost" data-act="extra-cancel">Cancel</button>
      ${e.err ? `<span class="ul-err" style="flex-basis:100%">${esc(e.err)}</span>` : `<span style="flex-basis:100%;font-size:11.5px;color:${MUTED}">Always new UIDs, never a reprint — a torn label's UID cannot be told apart from the ones that went on, and two labels with one UID is the one thing the ledger cannot protect against.</span>`}
    </div>`;
  }

  function renderSetup() {
    if (!S.setupOpen) return '';
    const c = S.codes; const list = c ? c.codes : [];
    const seg = (r) => `<span class="ul-seg"><button data-act="code-set" data-code="${esc(r.code)}" data-v="standard" aria-pressed="${r.format !== 'footwear'}">Standard</button><button data-act="code-set" data-code="${esc(r.code)}" data-v="footwear" aria-pressed="${r.format === 'footwear'}">Footwear</button></span>`;
    const sheet = (k, w, h, label) => `<div class="ul-sheet"><span class="sw" style="width:${w}px;height:${h}px"></span><div style="flex:1;min-width:0"><div style="font-weight:600;font-size:13px">${label}</div><div style="font-size:12px;color:${MUTED}">Two sample labels</div></div><button class="ul-btn sm" data-act="sample" data-v="${k}">${I.dl()} PDF</button></div>`;
    return `<div class="ul-dim" data-act="setup-close"></div>
      <aside class="ul-panel" role="dialog" aria-label="Setup">
        <div class="ul-panelh"><div><h2>Setup</h2><div style="font-size:12.5px;color:${MUTED};margin-top:4px">What decides the format, and the sheets to check the printer with.</div></div><button class="ul-x" data-act="setup-close" aria-label="Close">${I.x()}</button></div>
        <div class="ul-panelb">
          <section style="display:flex;flex-direction:column;gap:12px">
            <div class="ul-k">Printer &amp; scanner check</div>
            <div style="display:flex;gap:12px;flex-wrap:wrap">${sheet('standard', 38, 18, 'Standard · 76×36')}${sheet('footwear', 18, 38, 'Footwear · 36×76')}</div>
          </section>
          <section style="display:flex;flex-direction:column;gap:12px">
            <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap"><div class="ul-k">Department codes</div><span style="font-size:12px;color:${MUTED}">First four characters of the PO. Footwear prints 36×76; everything else 76×36.</span></div>
            <div class="ul-add"><input id="ul-add-codes" placeholder="Add codes — WFDA, WFAT MAEF…" aria-label="Department codes to add">
              <span class="ul-seg"><button data-act="add-fmt" data-v="standard" aria-pressed="${S.addFmt !== 'footwear'}">Standard</button><button data-act="add-fmt" data-v="footwear" aria-pressed="${S.addFmt === 'footwear'}">Footwear</button></span>
              <button class="ul-btn p sm" data-act="code-add" ${S.busy === 'codes' ? 'disabled' : ''}>Add</button></div>
            ${S.codesErr ? `<div class="ul-err">${esc(S.codesErr)}</div>` : ''}
            ${!c ? `<div class="ul-skel" style="height:200px"></div>` : !list.length ? `<div class="ul-empty" style="padding:16px 0">No codes yet. Add the footwear codes above; anything else prints as standard.</div>` :
            `<table class="ul-table"><thead><tr><th>Code</th><th>Name</th><th>Format</th><th class="num">Issued</th><th></th></tr></thead><tbody>
              ${list.map(r => `<tr><td class="ul-mono" style="font-weight:600">${esc(r.code)}${r.is_new ? ` <span class="ul-tag new xs" style="margin-left:6px">New</span>` : ''}</td><td style="white-space:normal;color:${MUTED};font-size:12px">${esc(r.name || (r.is_new ? 'arrived on a plan upload' : ''))}</td><td>${seg(r)}</td><td class="num">${fmtNum(r.issued)}</td>
                <td class="act">${r.issued ? '' : `<button class="ul-x" style="width:28px;height:28px" data-act="code-del" data-code="${esc(r.code)}" title="Remove">${I.x()}</button>`}</td></tr>`).join('')}
            </tbody></table>`}
          </section>
        </div></aside>`;
  }

  function render() {
    if (!S.root) return;
    styles();
    if (S.error && !S.data) {
      S.root.innerHTML = `<div class="ul">${renderHeader()}<div class="ul-card"><div class="ul-empty" style="color:${BRAND}">Couldn’t load: ${esc(S.error)} <button class="ul-btn sm" data-act="retry" style="margin-left:10px">Try again</button></div></div></div>`;
      return;
    }
    if (!S.data) {
      S.root.innerHTML = `<div class="ul">${renderHeader()}<div class="ul-skel" style="height:96px"></div><div class="ul-skel" style="height:360px"></div></div>`;
      return;
    }
    const active = document.activeElement, keep = active && active.getAttribute && active.getAttribute('data-act');
    const sel = keep && active.selectionStart != null ? [active.selectionStart, active.selectionEnd] : null;
    S.root.innerHTML = `<div class="ul">${renderHeader()}${renderStats()}${renderTable()}${S.toast ? `<div class="ul-toast">${esc(S.toast)}</div>` : ''}</div>${renderSetup()}`;
    if (keep === 'q' || keep === 'extra-reason' || keep === 'extra-qty') {
      const el = S.root.querySelector(`[data-act="${keep}"]`);
      if (el) { el.focus(); try { if (sel) el.setSelectionRange(sel[0], sel[1]); } catch (_) {} }
    }
  }

  function toast(msg) { S.toast = msg; clearTimeout(S.toastTimer); render(); S.toastTimer = setTimeout(() => { S.toast = ''; render(); }, 3200); }

  // ── Data ──
  async function loadWeeks() {
    S.weeks = await req('GET', '/uid-labels/weeks');
    if (!S.week) S.week = (S.weeks.weeks[0] && S.weeks.weeks[0].week_start) || mondayOf(todayLocal());
  }
  async function loadWeek(quiet) {
    if (!S.week) return;
    try {
      const d = await req('GET', `/uid-labels/week/${S.week}`);
      if (d.week_start === S.week) {
        S.data = d; S.error = null; S.loadedAt = Date.now();
        // The tab follows the latest run unless the viewer picked one that still exists.
        if (S.tab !== 'all' && !d.runs.some(r => r.id === S.tab)) S.tab = d.runs[0] ? d.runs[0].id : 'all';
      }
    } catch (e) { S.error = e.message || String(e); if (!quiet) S.data = null; }
    render();
  }
  async function load() {
    try { await loadWeeks(); } catch (e) { S.error = e.message || String(e); render(); return; }
    await loadWeek();
  }
  async function loadCodes() { try { S.codes = await req('GET', '/uid-labels/formats'); S.codesErr = ''; } catch (e) { S.codesErr = e.message || String(e); } render(); }
  function switchWeek(w) { S.week = w; S.data = null; S.extra = null; S.menu = null; S.tab = null; S.unsel = {}; S.otherWeek = false; render(); loadWeek(); }

  // ── Actions ──
  async function act(a, el, ev) {
    const v = el.getAttribute('data-v');
    switch (a) {
      case 'retry': load(); break;
      case 'week': switchWeek(v); break;
      case 'other-week': S.otherWeek = true; render(); { const i = S.root.querySelector('[data-act="week-pick"]'); if (i) i.focus(); } break;
      case 'week-pick': if (isDate(el.value)) switchWeek(mondayOf(el.value)); break;
      case 'tab': S.tab = v === 'all' ? 'all' : Number(v); S.menu = null; S.extra = null; render(); break;
      case 'q': S.q = el.value; S.menu = null; render(); break;
      case 'sel': {
        const un = new Set(S.unsel[selKey()] || []); if (el.checked) un.delete(el.getAttribute('data-po')); else un.add(el.getAttribute('data-po'));
        S.unsel = Object.assign({}, S.unsel, { [selKey()]: un }); render(); break;
      }
      case 'sel-all': case 'sel-none': case 'sel-toggle-all': {
        const un = new Set(S.unsel[selKey()] || []);
        const check = a === 'sel-all' || (a === 'sel-toggle-all' && el.checked);
        for (const r of rows().filter(selectable)) { if (check) un.delete(r.po); else un.add(r.po); }
        S.unsel = Object.assign({}, S.unsel, { [selKey()]: un }); render(); break;
      }
      case 'zip': {
        const run = currentRun(); if (!run) break;
        const un = S.unsel[selKey()] || new Set();
        const pos = (run.files || []).filter(f => !un.has(f.po)).map(f => f.po);
        if (!pos.length) break;
        S.busy = `zip:${selKey()}`; render();
        try {
          const all = pos.length === (run.files || []).length;
          await download(`/uid-labels/zip/${run.id}${all ? '' : `?pos=${encodeURIComponent(pos.join(','))}`}`, `UID_labels_run${run.id}.zip`);
          toast(`Zip of ${plural(pos.length, 'PDF')} downloaded`);
        } catch (e) { toast('Download failed: ' + (e.message || e)); }
        S.busy = null; render(); break;
      }
      case 'dl': case 'dl-again': {
        const run = el.getAttribute('data-run'), po = el.getAttribute('data-po');
        S.menu = null; S.busy = `dl:${run}:${po}`; render();
        try { await download(`/uid-labels/file/${run}/${encodeURIComponent(po)}${a === 'dl-again' ? '?reprint=1' : ''}`, `${po}.pdf`); if (a === 'dl-again') { toast('Downloaded again — recorded as a reprint'); loadWeek(true); } }
        catch (e) { toast('Download failed: ' + (e.message || e)); }
        S.busy = null; render(); break;
      }
      case 'dl-all': {
        const po = el.getAttribute('data-po'); S.menu = null; S.busy = `dlall:${po}`; render();
        try { await download(`/uid-labels/po/${S.week}/${encodeURIComponent(po)}.pdf`, `${po}_all.pdf`); } catch (e) { toast('Download failed: ' + (e.message || e)); }
        S.busy = null; render(); break;
      }
      case 'menu': { const po = el.getAttribute('data-po'); S.menu = S.menu === po ? null : po; render(); break; }
      case 'sample': try { await download(`/uid-labels/sample/${v}.pdf`, `sample_${v}.pdf`); } catch (e) { toast('Download failed: ' + (e.message || e)); } break;
      case 'reconcile': {
        S.busy = 'reconcile'; render();
        try { const r = await req('POST', `/uid-labels/reconcile/${S.week}`); toast(r.labels ? `Issued ${plural(r.labels, 'label')} — run #${r.run_id}` : (r.moved && r.moved.length ? `${plural(r.moved.length, 'PO line')} moved in — nothing printed` : 'Nothing to issue — the ledger matches the plan')); if (r.run_id) S.tab = r.run_id; }
        catch (e) { toast(e.message || String(e)); }
        S.busy = null; await loadWeeks().catch(() => {}); loadWeek(true); break;
      }
      case 'email': try { const r = await req('POST', `/uid-labels/run/${v}/email`); toast(r.sent ? 'Email sent' : 'Not sent: ' + (r.reason || '')); loadWeek(true); } catch (e) { toast(e.message || String(e)); } break;
      case 'extra-open': {
        const po = el.getAttribute('data-po'); S.menu = null;
        if (S.extra && S.extra.po === po) { S.extra = null; render(); break; }
        const p = poInfo(po) || { lines: [] };
        S.extra = { po, sku: (p.lines[0] || {}).sku || '', qty: 1, reason: '', err: '' }; render(); break;
      }
      case 'extra-sku': S.extra.sku = el.value; break;
      case 'extra-qty': S.extra.qty = Math.max(1, Math.floor(Number(el.value) || 1)); break;
      case 'extra-reason': S.extra.reason = el.value; break;
      case 'extra-cancel': S.extra = null; render(); break;
      case 'extra-go': {
        const e = S.extra; if (!e) break;
        S.busy = 'extra'; e.err = ''; render();
        try {
          const r = await req('POST', '/uid-labels/extra', { week: S.week, po: e.po, sku: e.sku, qty: e.qty, reason: e.reason });
          S.busy = null; S.extra = null; S.tab = r.run_id; toast(`Issued ${plural(r.labels, 'new label')} — run #${r.run_id}`);
          await loadWeeks().catch(() => {}); await loadWeek(true);
        } catch (err) { S.busy = null; e.err = err.message || String(err); render(); }
        break;
      }
      case 'setup': S.setupOpen = true; S.menu = null; render(); loadCodes(); break;
      case 'setup-close': S.setupOpen = false; render(); loadWeeks().then(() => render()).catch(() => {}); loadWeek(true); break;
      case 'add-fmt': S.addFmt = v; render(); break;
      case 'code-add': {
        const inp = document.getElementById('ul-add-codes'); const codes = inp ? inp.value : '';
        if (!codes.trim()) break;
        S.busy = 'codes'; render();
        try { await req('POST', '/uid-labels/formats', { codes, format: S.addFmt || 'footwear' }); S.codesErr = ''; }
        catch (e) { S.codesErr = e.message || String(e); }
        S.busy = null; await loadCodes(); if (!S.codesErr) { const i2 = document.getElementById('ul-add-codes'); if (i2) i2.value = ''; }
        break;
      }
      case 'code-set': {
        const code = el.getAttribute('data-code');
        try { await req('POST', '/uid-labels/formats', { code, format: v }); S.codesErr = ''; } catch (e) { S.codesErr = e.message || String(e); }
        await loadCodes(); loadWeek(true); break;
      }
      case 'code-del': {
        const code = el.getAttribute('data-code');
        try { await req('DELETE', `/uid-labels/formats/${encodeURIComponent(code)}`); S.codesErr = ''; } catch (e) { S.codesErr = e.message || String(e); }
        await loadCodes(); break;
      }
      default: break;
    }
  }

  function bind(host) {
    if (host.__ulBound) return;
    host.__ulBound = true;
    host.addEventListener('click', (ev) => {
      const el = ev.target.closest('[data-act]');
      if (S.menu && !ev.target.closest('.ul-menu') && !(el && el.getAttribute('data-act') === 'menu')) { S.menu = null; render(); }
      if (!el || el.tagName === 'INPUT' || el.tagName === 'SELECT') return;
      if (el.disabled) return;
      act(el.getAttribute('data-act'), el, ev);
    });
    host.addEventListener('input', (ev) => {
      const el = ev.target.closest('[data-act]'); if (!el) return;
      const a = el.getAttribute('data-act');
      if (a === 'q' || a === 'extra-qty' || a === 'extra-reason') act(a, el, ev);
    });
    host.addEventListener('change', (ev) => {
      const el = ev.target.closest('[data-act]'); if (!el) return;
      const a = el.getAttribute('data-act');
      if (a === 'week-pick' || a === 'extra-sku' || a === 'sel' || a === 'sel-toggle-all') act(a, el, ev);
    });
    host.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' && ev.target.id === 'ul-add-codes') { ev.preventDefault(); act('code-add', ev.target, ev); }
    });
  }
  // Escape closes the open thing (menu, then drawer) wherever focus ended up.
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Escape' || !S.root || !S.root.isConnected) return;
    const pg = document.getElementById('page-uid-labels');
    if (!pg || pg.style.display === 'none') return;
    if (S.menu) { S.menu = null; render(); }
    else if (S.setupOpen) act('setup-close', S.root, ev);
  });

  function mount(pg) {
    styles();
    S.root = pg; bind(pg);
    render();
    load();
    clearInterval(S.timer);
    S.timer = setInterval(() => { if (S.root && S.root.isConnected && S.data && !S.busy && !S.extra && !S.menu) loadWeek(true); }, REFRESH_MS);
  }

  window.showUidLabelsPage = function () {
    let pg = document.getElementById('page-uid-labels');
    if (!pg) {
      pg = document.createElement('section'); pg.id = 'page-uid-labels'; pg.style.cssText = 'padding:0;display:block;';
      const main = document.querySelector('main.vo-wrap') || document.querySelector('main') || document.body;
      main.appendChild(pg);
      mount(pg);
    } else if (!S.root || !pg.contains(S.root)) {
      mount(pg);
    } else if (Date.now() - S.loadedAt > REFRESH_MS) { loadWeeks().then(() => loadWeek(true)).catch(() => {}); }
    pg.classList.remove('hidden'); pg.style.display = 'block';
  };
  window.hideUidLabelsPage = function () {
    const pg = document.getElementById('page-uid-labels');
    if (pg) { pg.classList.add('hidden'); pg.style.display = 'none'; }
    if (S.setupOpen || S.menu) { S.setupOpen = false; S.menu = null; render(); }
  };
  // The nav entry follows the page: VelOzity staff and partner warehouses (Kerry); never a client login.
  function gateNav() { const a = document.getElementById('nav-vas-uidlabels'); if (a) a.style.display = (window.pinpointIsInternal === true || window.pinpointIsPartner === true) ? '' : 'none'; }
  gateNav(); window.addEventListener('tenancy:ready', gateNav);

  window.__uidLabels = { reload: () => load(), state: () => S, version: VERSION };
  console.log('[uid-labels] v' + VERSION + ' loaded');
})();
