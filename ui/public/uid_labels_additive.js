/* ── VelOzity Pinpoint — UID labels: the print queue (VAS Ops) v1 ──
   #page-uid-labels, route #uid-labels, reached from VAS Ops ▾ → UID labels. Internal only.

   What the page is for: the warehouse opens it, picks the week, and downloads one PDF per PO.
   Everything else is bookkeeping the ledger already did — what the plan asked for, what has
   been issued, what moved from another week, which department codes are new.

   Rules that show on screen (the API enforces them; see api/uid_label_routes.js):
     · a run is created by a plan upload (delta only) or by an Extras request (new UIDs);
     · a moved PO prints nothing — the warehouse chooses: download again (same UIDs) or issue new;
     · a department code the table has never seen prints as standard and is flagged New.

   No framework, no build step; same conventions as elevate_additive.js. */
;(function () {
  'use strict';
  if (window.__UID_LABELS_LOADED__) return;
  window.__UID_LABELS_LOADED__ = true;
  const VERSION = '4';

  const INK = '#121212', MUTED = '#5F5F5F', LINE = '#E3E3E0', SOFT = '#EFEFEC';
  const RED = '#990033', GREEN = '#7CB518', PURPLE = '#7C5CBF', AMBER = '#8A6D00';
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
  const fmtShort = (v) => isDate(v) ? toD(v).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' }) : '';
  // Shown in the viewer's own time zone — the warehouse sees Sydney, an admin abroad sees theirs.
  const when = (iso) => { try { return new Date(String(iso).replace(' ', 'T') + (String(iso).endsWith('Z') ? '' : 'Z')).toLocaleString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }); } catch (_) { return String(iso || ''); } };
  // A person's name or email; a raw auth id (runs stamped before names were recorded) is not worth showing.
  const person = (s) => { const v = String(s || '').trim(); return !v || v === 'user' || /^user_[A-Za-z0-9]+$/.test(v) ? '' : v.replace(/\buser_[A-Za-z0-9]+\b/g, '').replace(/\s*·\s*$/, '').trim(); };
  const fmtNum = (n) => Number(n || 0).toLocaleString('en-AU');
  const plural = (n, w, p) => `${fmtNum(n)} ${n === 1 ? w : (p || w + 's')}`;

  // ── State ──
  const S = { root: null, weeks: null, week: null, data: null, loading: false, error: null, loadedAt: 0, timer: null,
              busy: null, toast: '', toastTimer: null, extra: null, codes: null, codesOpen: false, codesErr: '', runOpen: {}, posOpen: false, q: '', unsel: {} };

  // ── Styles ──
  function styles() {
    if (document.getElementById('ul-css')) return;
    const st = document.createElement('style'); st.id = 'ul-css';
    st.textContent = `
.ul{font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK};box-sizing:border-box;padding:24px 28px 48px;max-width:1400px;margin:0 auto;display:flex;flex-direction:column;gap:20px;-webkit-font-smoothing:antialiased}
.ul *{box-sizing:border-box}
.ul button{font-family:inherit;cursor:pointer;color:inherit;background:none;border:0;padding:0}
.ul button:disabled{opacity:.45;cursor:default}
.ul input,.ul select,.ul textarea{font-family:inherit;font-size:13px;color:${INK}}
.ul-mono{font-family:'Geist Mono',ui-monospace,Menlo,monospace}
.ul-head{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap}
.ul-head h1{margin:0;font-size:34px;font-weight:600;letter-spacing:-.02em;line-height:1}
.ul-kicker{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:${MUTED};font-weight:500}
.ul-note{font-size:13px;color:${MUTED}}
.ul-btn{height:38px;padding:0 14px;border-radius:8px;border:1px solid #D6D6D2;background:#fff;font-size:12.5px;font-weight:500;display:inline-flex;align-items:center;gap:8px;white-space:nowrap}
.ul-btn:hover{border-color:#B8B8B4}
.ul .ul-btn-p{height:38px;padding:0 16px;border-radius:8px;border:0;background:${INK};color:#fff;font-size:12.5px;font-weight:500;display:inline-flex;align-items:center;gap:8px;white-space:nowrap}
.ul-btn.sm,.ul .ul-btn-p.sm{height:30px;padding:0 11px;font-size:12px;border-radius:7px}
.ul-chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.ul-chip{height:30px;padding:0 12px;border-radius:15px;border:1px solid #D6D6D2;background:#fff;font-size:12px;font-weight:500;display:inline-flex;align-items:center;gap:6px}
.ul-chip[aria-pressed="true"]{background:${INK};color:#fff;border-color:${INK}}
.ul-chip.has::after{content:'';width:5px;height:5px;border-radius:50%;background:${GREEN}}
.ul-chip[aria-pressed="true"].has::after{background:#fff}
.ul-chip.dot::before{content:'';width:7px;height:7px;border-radius:50%;background:${RED}}
.ul-wkin{height:30px;border:1px solid #D6D6D2;border-radius:15px;padding:0 10px;font-size:12px;background:#fff;color:${MUTED}}
.ul-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}
.ul-stat{background:#fff;border:1px solid ${LINE};border-radius:12px;padding:14px 16px}
.ul-stat .k{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};font-weight:500}
.ul-stat .v{font-size:26px;font-weight:600;letter-spacing:-.02em;margin-top:4px;line-height:1}
.ul-stat .s{font-size:12px;color:${MUTED};margin-top:4px}
.ul-card{background:#fff;border:1px solid ${LINE};border-radius:12px;overflow:hidden}
.ul-cardh{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:14px 18px;border-bottom:1px solid ${LINE};flex-wrap:wrap}
.ul-h2{margin:0;font-size:15px;font-weight:600}
.ul-h2 small{font-weight:400;color:${MUTED};margin-left:8px;font-size:12.5px}
.ul-run{border-bottom:1px solid ${LINE}}
.ul-run:last-child{border-bottom:0}
.ul-runh{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:14px;align-items:center;padding:12px 18px;width:100%;text-align:left}
.ul-runh:hover{background:#FAFAF8}
.ul-runh .id{font-size:12px;color:${MUTED};min-width:40px}
.ul-runh .t{font-size:13.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ul-runh .m{font-size:12px;color:${MUTED};margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ul-tag{display:inline-flex;align-items:center;height:20px;padding:0 8px;border-radius:10px;font-size:11px;font-weight:600;white-space:nowrap}
.ul-tag.fw{background:#EFE9F8;color:${PURPLE}}
.ul-tag.st{background:${SOFT};color:${MUTED}}
.ul-tag.new{background:#FFF3C4;color:${AMBER}}
.ul-tag.mv{background:#E6F2F9;color:#2E6C8E}
.ul-tag.ok{background:#EEF7DC;color:#4A6A00}
.ul-tag.over{background:#FBE9EE;color:${RED}}
.ul-tag.err{background:#FBE9EE;color:${RED}}
.ul-files{padding:0 18px 14px 72px}
.ul-table{border-collapse:separate;border-spacing:0;font-size:12.5px;width:100%}
.ul-table th{text-align:left;font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:${MUTED};font-weight:500;padding:8px 10px;border-bottom:1px solid ${LINE};white-space:nowrap}
.ul-table td{padding:8px 10px;border-bottom:1px solid ${SOFT};vertical-align:middle;white-space:nowrap}
.ul-table tr:last-child td{border-bottom:0}
.ul-table th.num,.ul-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.ul-table td.act{text-align:right}
.ul-table td.act .ul-btn,.ul-table td.act .ul-btn-p{margin-left:6px}
.ul-banner{display:flex;gap:10px;align-items:flex-start;padding:10px 18px 10px 72px;font-size:12.5px;color:${MUTED};border-top:1px solid ${SOFT}}
.ul-banner b{color:${INK};font-weight:600}
.ul-selbar{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:10px 0 8px;flex-wrap:wrap}
.ul-table input[type=checkbox]{width:15px;height:15px;accent-color:${INK};cursor:pointer;vertical-align:middle}
.ul-search{height:34px;border:1px solid #D6D6D2;border-radius:8px;padding:0 12px;font-size:13px;outline:none;background:#fff;min-width:220px}
.ul-search:focus{border-color:#8A8A86}
.ul-extra{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:10px 12px;background:#FAFAF8;border:1px solid ${LINE};border-radius:8px;margin:6px 0}
.ul-extra select,.ul-extra input{height:32px;border:1px solid #D6D6D2;border-radius:7px;padding:0 9px;background:#fff}
.ul-err{font-size:12.5px;color:${RED}}
.ul-empty{padding:28px 18px;font-size:13.5px;color:${MUTED}}
.ul-dim{position:fixed;inset:0;background:rgba(0,0,0,.28);z-index:9000}
.ul-panel{position:fixed;top:0;right:0;bottom:0;width:min(640px,100vw);background:#fff;z-index:9001;box-shadow:-12px 0 40px rgba(0,0,0,.18);display:flex;flex-direction:column;font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK}}
.ul-panel *{box-sizing:border-box}
.ul-panel button{font-family:inherit;cursor:pointer;color:inherit;background:none;border:0;padding:0}
.ul-panelh{display:flex;justify-content:space-between;align-items:center;padding:18px 22px;border-bottom:1px solid ${LINE}}
.ul-panelh h2{margin:0;font-size:18px;font-weight:600}
.ul-panelb{padding:16px 22px;overflow:auto;flex:1}
.ul-seg{display:inline-flex;background:#E7E7E4;border-radius:7px;padding:2px;gap:2px}
.ul-seg button{height:26px;padding:0 10px;border-radius:5px;font-size:11.5px;font-weight:500;color:${MUTED}}
.ul-seg button[aria-pressed="true"]{background:#fff;color:${INK};box-shadow:0 1px 2px rgba(0,0,0,.08)}
.ul-seg button[aria-pressed="true"].fw{color:${PURPLE}}
.ul-add{display:flex;gap:8px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
.ul-add input{height:34px;border:1px solid #D6D6D2;border-radius:8px;padding:0 10px;font-size:13px;flex:1 1 200px;font-family:'Geist Mono',ui-monospace,Menlo,monospace;text-transform:uppercase}
.ul-toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:${INK};color:#fff;padding:10px 16px;border-radius:10px;font-size:13px;z-index:9100;box-shadow:0 8px 24px rgba(0,0,0,.2)}
.ul-x{width:36px;height:36px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;color:${MUTED}}
.ul-x:hover{background:${SOFT};color:${INK}}
.ul-skel{background:linear-gradient(90deg,#F2F2EF 25%,#E8E8E4 50%,#F2F2EF 75%);background-size:200% 100%;animation:ulSk 1.2s infinite;border-radius:12px}
@keyframes ulSk{0%{background-position:200% 0}100%{background-position:-200% 0}}
`;
    document.head.appendChild(st);
  }
  const I = {
    dl: () => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg>`,
    x: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>`,
    chev: (rot) => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="transition:transform .2s;transform:rotate(${rot}deg)" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>`,
    out: () => `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"/><path d="M8 7h9v9"/></svg>`,
  };

  // ── Render ──
  const fmtTag = (f) => f === 'footwear' ? `<span class="ul-tag fw">Footwear · 36×76</span>` : `<span class="ul-tag st">Standard · 76×36</span>`;

  function renderHeader() {
    const wk = S.data;
    const weeks = (S.weeks && S.weeks.weeks) || [];
    const thisMon = mondayOf(todayLocal());
    const set = new Set(weeks.map(w => w.week_start));
    if (S.week) set.add(S.week);
    set.add(thisMon);
    for (let i = 1; i <= 2; i++) set.add(addD(thisMon, -7 * i));
    const chips = [...set].sort().reverse().slice(0, 8).map(w => {
      const has = weeks.some(x => x.week_start === w && x.labels > 0);
      return `<button class="ul-chip${has ? ' has' : ''}" data-act="week" data-v="${w}" aria-pressed="${S.week === w}" title="${esc(w)}">${isoWeek(w)}</button>`;
    }).join('');
    const newCodes = S.weeks ? S.weeks.new_codes : 0;
    return `<div class="ul-head">
      <div style="display:flex;flex-direction:column;gap:6px">
        <div class="ul-kicker">Pinpoint · VAS Ops</div>
        <h1>UID labels</h1>
        <div class="ul-note">One PDF per PO. Each plan upload issues only the labels the week still lacks; the warehouse prints the latest run.</div>
        <div class="ul-chips" style="margin-top:8px"><span style="font-size:12px;color:${MUTED};margin-right:2px">Week</span>${chips}
          <input class="ul-wkin" type="date" data-act="week-pick" value="${esc(S.week || '')}" aria-label="Any week">
          <input class="ul-search" placeholder="Find a PO or SKU" value="${esc(S.q)}" data-act="q" aria-label="Find a PO or SKU" style="margin-left:10px"></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <button class="ul-btn" data-act="sample" data-v="standard">${I.dl()} Sample · standard</button>
        <button class="ul-btn" data-act="sample" data-v="footwear">${I.dl()} Sample · footwear</button>
        <button class="ul-btn${newCodes ? ' dot' : ''}" data-act="codes">Department codes${newCodes ? ` <span class="ul-tag new">${newCodes} new</span>` : ''}</button>
        <button class="ul-btn-p" data-act="reconcile" ${S.busy === 'reconcile' || !wk || !wk.has_plan ? 'disabled' : ''} title="Issue anything the ledger still lacks for this week's stored plan">${S.busy === 'reconcile' ? 'Checking…' : 'Check plan now'}</button>
      </div></div>`;
  }

  function renderStats() {
    const d = S.data; if (!d) return '';
    const fw = d.pos.filter(p => p.format === 'footwear'), st = d.pos.filter(p => p.format !== 'footwear');
    const over = d.pos.filter(p => p.issued > p.planned).length;
    const short = d.pos.filter(p => p.issued < p.planned).length;
    const moved = d.pos.filter(p => p.moved_from).length;
    const stat = (k, v, s) => `<div class="ul-stat"><div class="k">${k}</div><div class="v">${v}</div>${s ? `<div class="s">${s}</div>` : ''}</div>`;
    return `<div class="ul-stats">
      ${stat('Planned units', fmtNum(d.totals.planned), plural(d.pos.length, 'PO'))}
      ${stat('Issued', fmtNum(d.totals.issued), short ? `<span style="color:${RED}">${plural(short, 'PO')} short</span>` : over ? `${plural(over, 'PO')} over plan` : 'matches the plan')}
      ${stat('Footwear · 36×76', fmtNum(fw.reduce((a, p) => a + p.issued, 0)), plural(fw.length, 'PO'))}
      ${stat('Standard · 76×36', fmtNum(st.reduce((a, p) => a + p.issued, 0)), plural(st.length, 'PO'))}
      ${stat('Runs', fmtNum(d.runs.length), moved ? `${plural(moved, 'PO')} moved in` : d.runs[0] ? 'latest ' + when(d.runs[0].at) : 'nothing yet')}
    </div>`;
  }

  function runTitle(r) {
    const s = r.summary || {};
    if (r.kind === 'extra') return `Extra labels — ${esc((s.pos && s.pos[0] && s.pos[0].po) || '')}${s.sku ? ` / ${esc(s.sku)}` : ''}`;
    if (!r.labels) {
      const mv = (s.moved || []).length;
      return `Plan upload — nothing printed${mv ? ` · ${plural(mv, 'PO line')} moved from ${[...new Set(s.moved.map(m => isoWeek(m.from)))].join(', ')}` : ''}`;
    }
    return `Plan upload — ${plural(r.labels, 'label')} across ${plural((s.pos || []).length, 'PO')}`;
  }
  function runMeta(r) {
    // trigger_by is "plan upload · who" or "Check plan now · who" (or just who, for extras).
    const raw = person(r.by);
    const m = /^(plan upload|Check plan now)(?:\s*·\s*(.*))?$/i.exec(raw);
    const how = m ? (/^plan/i.test(m[1]) ? 'from the plan upload' : 'by Check plan now') : null;
    const whoName = m ? (m[2] || '').trim() : raw;
    const bits = [`Issued ${when(r.at)}`, how ? `${how}${whoName ? ` (${esc(whoName)})` : ''}` : (whoName ? `by ${esc(whoName)}` : null)];
    if (r.emailed_at) bits.push('warehouse emailed'); else if (r.email_error) bits.push(`<span style="color:${RED}" title="${esc(r.email_error)}">email not sent — ${esc(r.email_error)}</span>`);
    if (r.summary && r.summary.reason) bits.push(esc(r.summary.reason));
    return bits.filter(Boolean).join(' · ');
  }
  function renderRun(r, i) {
    const open = S.runOpen[r.id] != null ? S.runOpen[r.id] : i === 0;
    const s = r.summary || {};
    const files = r.files || [];
    const fw = files.filter(f => f.format === 'footwear').reduce((a, f) => a + f.labels, 0);
    const st = files.filter(f => f.format !== 'footwear').reduce((a, f) => a + f.labels, 0);
    const banners = [];
    if (s.moved && s.moved.length) banners.push(`<b>${plural(s.moved.length, 'PO line')} moved</b> from ${[...new Set(s.moved.map(m => isoWeek(m.from)))].join(', ')} — nothing printed; choose below under the PO whether to download again or issue new.`);
    if (s.new_codes && s.new_codes.length) banners.push(`<b>New department code${s.new_codes.length === 1 ? '' : 's'}</b> ${s.new_codes.map(c => `<span class="ul-mono">${esc(c.code)}</span>`).join(', ')} printed as standard — set the format under Department codes if that is wrong.`);
    if (s.over && s.over.length) banners.push(`<b>${plural(s.over.length, 'PO line')}</b> now plan fewer units than already issued — nothing printed.`);
    const q = S.q.trim().toLowerCase();
    const shown = q ? files.filter(f => f.po.toLowerCase().includes(q)) : files;
    const un = S.unsel[r.id] || new Set();
    const selected = files.filter(f => !un.has(f.po));
    const shownSel = shown.filter(f => !un.has(f.po)).length;
    const allShown = shown.length > 0 && shownSel === shown.length, noneShown = shownSel === 0;
    const zipBusy = S.busy === `zip:${r.id}`;
    return `<div class="ul-run">
      <button class="ul-runh" data-act="run-toggle" data-v="${r.id}" aria-expanded="${open}">
        <span class="id ul-mono">#${r.id}</span>
        <span style="min-width:0"><div class="t">${runTitle(r)}</div><div class="m">${runMeta(r)}</div></span>
        <span style="display:flex;align-items:center;gap:8px">${fw ? `<span class="ul-tag fw">${fmtNum(fw)} footwear</span>` : ''}${st ? `<span class="ul-tag st">${fmtNum(st)} standard</span>` : ''}${I.chev(open ? 180 : 0)}</span>
      </button>
      ${open ? `${files.length ? `<div class="ul-files">
        <div class="ul-selbar">
          <span style="font-size:12.5px;color:${MUTED}">${fmtNum(selected.length)} of ${fmtNum(files.length)} selected${q ? ` · ${fmtNum(shown.length)} match “${esc(S.q.trim())}”` : ''}</span>
          <span style="display:flex;gap:8px;align-items:center">
            <button class="ul-btn sm" data-act="sel-all" data-v="${r.id}" ${allShown ? 'disabled' : ''}>Select ${q ? 'matching' : 'all'}</button>
            <button class="ul-btn sm" data-act="sel-none" data-v="${r.id}" ${noneShown ? 'disabled' : ''}>Deselect ${q ? 'matching' : 'all'}</button>
            <button class="ul-btn-p sm" data-act="zip" data-v="${r.id}" ${zipBusy || !selected.length ? 'disabled' : ''}>${I.dl()} ${zipBusy ? 'Building zip…' : `Download ${selected.length === files.length ? 'all' : 'selected'} (${fmtNum(selected.length)})`}</button>
          </span></div>
        ${shown.length ? `<table class="ul-table"><thead><tr><th style="width:28px"><input type="checkbox" data-act="sel-toggle-all" data-v="${r.id}" ${allShown ? 'checked' : ''} aria-label="Select all shown"></th><th>PO</th><th>Format</th><th class="num">Labels</th><th>Printed again</th><th></th></tr></thead><tbody>
        ${shown.map(f => `<tr><td><input type="checkbox" data-act="sel" data-run="${r.id}" data-po="${esc(f.po)}" ${un.has(f.po) ? '' : 'checked'} aria-label="Select ${esc(f.po)}"></td><td class="ul-mono" style="font-weight:600">${esc(f.po)}</td><td>${fmtTag(f.format)}</td><td class="num">${fmtNum(f.labels)}</td>
          <td style="color:${MUTED};font-size:12px">${f.reprints ? `${plural(f.reprints, 'time')} · ${when(f.last_reprint)}` : '—'}</td>
          <td class="act"><button class="ul-btn-p sm" data-act="dl" data-run="${r.id}" data-po="${esc(f.po)}" ${S.busy === `dl:${r.id}:${f.po}` ? 'disabled' : ''}>${I.dl()} ${S.busy === `dl:${r.id}:${f.po}` ? 'Building…' : 'PDF'}</button></td></tr>`).join('')}
        </tbody></table>` : `<div class="ul-empty" style="padding:16px 10px">No PO in this run matches “${esc(S.q.trim())}”.</div>`}</div>` : banners.length ? '' : `<div class="ul-banner">Nothing was printed in this run.</div>`}
        ${banners.map(b => `<div class="ul-banner">${b}</div>`).join('')}
        ${r.email_error ? `<div class="ul-banner"><span class="ul-err">Email: ${esc(r.email_error)}</span><button class="ul-btn sm" data-act="email" data-v="${r.id}">Send again</button></div>` : ''}` : ''}
    </div>`;
  }

  function renderRuns() {
    const d = S.data; if (!d) return '';
    return `<div class="ul-card">
      <div class="ul-cardh"><h2 class="ul-h2">Runs · ${esc(d.label)}<small>newest first — print the latest</small></h2></div>
      ${d.runs.length ? d.runs.map(renderRun).join('') : `<div class="ul-empty">${d.has_plan ? 'No labels issued for this week yet. Uploading the plan issues them; "Check plan now" does the same by hand.' : 'No plan stored for this week yet. Labels are issued when the plan is uploaded.'}</div>`}
    </div>`;
  }

  function renderPos() {
    const d = S.data; if (!d) return '';
    const q = S.q.trim().toLowerCase();
    const rows = d.pos.filter(p => !q || p.po.toLowerCase().includes(q) || p.lines.some(l => l.sku.toLowerCase().includes(q)));
    const movedFiles = d.moved_files || [];
    const line = (p) => {
      const diff = p.issued - p.planned;
      const status = p.moved_from ? `<span class="ul-tag mv">Moved from ${isoWeek(p.moved_from)}</span>` : diff > 0 ? `<span class="ul-tag over">+${fmtNum(diff)} over plan</span>` : diff < 0 ? `<span class="ul-tag err">${fmtNum(-diff)} short</span>` : p.issued ? `<span class="ul-tag ok">Issued</span>` : `<span class="ul-tag st">Nothing yet</span>`;
      const mv = movedFiles.filter(f => f.po === p.po);
      const extraOpen = S.extra && S.extra.po === p.po;
      return `<tr>
        <td class="ul-mono" style="font-weight:600">${esc(p.po)}</td>
        <td><span class="ul-mono" style="font-size:12px">${esc(p.dept)}</span>${p.dept_new ? ` <span class="ul-tag new">New</span>` : ''}</td>
        <td>${fmtTag(p.format)}</td>
        <td class="num">${fmtNum(p.skus)}</td><td class="num">${fmtNum(p.planned)}</td><td class="num">${fmtNum(p.issued)}</td>
        <td>${status}</td>
        <td class="act">
          ${mv.map(f => `<button class="ul-btn sm" data-act="dl-again" data-run="${f.run_id}" data-po="${esc(f.po)}" title="The same ${f.labels} labels, same UIDs, printed again">${I.dl()} Download again</button>`).join('')}
          ${p.issued ? `<button class="ul-btn sm" data-act="dl-all" data-po="${esc(p.po)}" title="Every label issued for this PO this week, all runs, same UIDs">${I.dl()} All ${fmtNum(p.issued)}</button>` : ''}
          <button class="ul-btn sm" data-act="extra-open" data-po="${esc(p.po)}" aria-pressed="${extraOpen}">${p.moved_from ? 'Issue new' : 'Extra labels'}</button>
        </td></tr>
        ${extraOpen ? `<tr><td colspan="8" style="white-space:normal;padding:4px 10px 10px">${renderExtra(p)}</td></tr>` : ''}`;
    };
    return `<div class="ul-card">
      <div class="ul-cardh"><h2 class="ul-h2">Plan vs issued<small>${q ? `${plural(rows.length, 'PO')} match “${esc(S.q.trim())}”` : `${plural(d.pos.length, 'PO')} · extras are always new UIDs`}</small></h2></div>
      ${rows.length ? `<div style="overflow:auto"><table class="ul-table"><thead><tr><th>PO</th><th>Dept</th><th>Format</th><th class="num">SKUs</th><th class="num">Planned</th><th class="num">Issued</th><th>Status</th><th></th></tr></thead><tbody>${rows.map(line).join('')}</tbody></table></div>`
                    : `<div class="ul-empty">${d.pos.length ? 'No PO matches.' : 'Nothing in this week’s plan.'}</div>`}
    </div>`;
  }

  function renderExtra(p) {
    const e = S.extra;
    const opts = p.lines.map(l => `<option value="${esc(l.sku)}" ${e.sku === l.sku ? 'selected' : ''}>${esc(l.sku)} — planned ${fmtNum(l.planned)}, issued ${fmtNum(l.issued)}</option>`).join('');
    return `<div class="ul-extra">
      <span style="font-size:12.5px;font-weight:600">${p.moved_from ? 'Issue new labels' : 'Extra labels'} for ${esc(p.po)}</span>
      <select data-act="extra-sku" aria-label="SKU">${opts}</select>
      <input type="number" min="1" max="5000" value="${e.qty}" data-act="extra-qty" style="width:90px" aria-label="How many">
      <input type="text" placeholder="Why (optional) — torn, lost…" value="${esc(e.reason)}" data-act="extra-reason" style="flex:1 1 180px" aria-label="Reason">
      <button class="ul-btn-p sm" data-act="extra-go" ${S.busy === 'extra' ? 'disabled' : ''}>${S.busy === 'extra' ? 'Issuing…' : 'Issue new UIDs'}</button>
      <button class="ul-btn sm" data-act="extra-cancel">Cancel</button>
      ${e.err ? `<span class="ul-err" style="flex-basis:100%">${esc(e.err)}</span>` : `<span style="flex-basis:100%;font-size:11.5px;color:${MUTED}">New UIDs, never a reprint — a torn label's UID cannot be told apart from the ones that went on, and two labels with one UID is the one thing the ledger cannot protect against.</span>`}
    </div>`;
  }

  function renderCodes() {
    if (!S.codesOpen) return '';
    const c = S.codes;
    const rows = c ? c.codes : [];
    const seg = (r) => `<span class="ul-seg"><button data-act="code-set" data-code="${esc(r.code)}" data-v="standard" aria-pressed="${r.format !== 'footwear'}">Standard</button><button class="fw" data-act="code-set" data-code="${esc(r.code)}" data-v="footwear" aria-pressed="${r.format === 'footwear'}">Footwear</button></span>`;
    return `<div class="ul-dim" data-act="codes-close"></div>
      <aside class="ul-panel" role="dialog" aria-label="Department codes">
        <div class="ul-panelh"><div><h2>Department codes</h2><div style="font-size:12.5px;color:${MUTED};margin-top:3px">The first four characters of the PO. Footwear prints 36×76 portrait; everything else 76×36.</div></div><button class="ul-x" data-act="codes-close" aria-label="Close">${I.x()}</button></div>
        <div class="ul-panelb">
          <div class="ul-add"><input id="ul-add-codes" placeholder="Add codes — WFDA, WFAT MAEF…" aria-label="Department codes to add">
            <span class="ul-seg"><button data-act="add-fmt" data-v="standard" aria-pressed="${S.addFmt !== 'footwear'}">Standard</button><button class="fw" data-act="add-fmt" data-v="footwear" aria-pressed="${S.addFmt === 'footwear'}">Footwear</button></span>
            <button class="ul-btn-p sm" data-act="code-add" ${S.busy === 'codes' ? 'disabled' : ''}>Add</button></div>
          ${S.codesErr ? `<div class="ul-err" style="margin-bottom:10px">${esc(S.codesErr)}</div>` : ''}
          ${!c ? `<div class="ul-skel" style="height:200px"></div>` : !rows.length ? `<div class="ul-empty">No codes yet. Add the footwear codes above; anything else prints as standard.</div>` :
          `<table class="ul-table"><thead><tr><th>Code</th><th>Name</th><th>Format</th><th class="num">Issued</th><th></th></tr></thead><tbody>
            ${rows.map(r => `<tr><td class="ul-mono" style="font-weight:600">${esc(r.code)}${r.is_new ? ` <span class="ul-tag new">New</span>` : ''}</td><td style="white-space:normal;color:${MUTED};font-size:12px">${esc(r.name || '')}</td><td>${seg(r)}</td><td class="num">${fmtNum(r.issued)}</td>
              <td class="act">${r.issued ? '' : `<button class="ul-x" style="width:28px;height:28px" data-act="code-del" data-code="${esc(r.code)}" title="Remove">${I.x()}</button>`}</td></tr>`).join('')}
          </tbody></table>`}
        </div></aside>`;
  }

  function render() {
    if (!S.root) return;
    styles();
    if (S.error && !S.data) {
      S.root.innerHTML = `<div class="ul">${renderHeader()}<div class="ul-card"><div class="ul-empty" style="color:${RED}">Couldn’t load: ${esc(S.error)} <button class="ul-btn sm" data-act="retry" style="margin-left:10px">Try again</button></div></div></div>`;
      return;
    }
    if (!S.data) {
      S.root.innerHTML = `<div class="ul">${renderHeader()}<div class="ul-skel" style="height:90px"></div><div class="ul-skel" style="height:320px"></div></div>`;
      return;
    }
    const active = document.activeElement, keep = active && active.getAttribute && active.getAttribute('data-act');
    const sel = keep && active.selectionStart != null ? [active.selectionStart, active.selectionEnd] : null;
    S.root.innerHTML = `<div class="ul">${renderHeader()}${renderStats()}${renderRuns()}${renderPos()}${S.toast ? `<div class="ul-toast">${esc(S.toast)}</div>` : ''}</div>${renderCodes()}`;
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
    if (!quiet) { S.loading = true; }
    try {
      const d = await req('GET', `/uid-labels/week/${S.week}`);
      if (d.week_start === S.week) { S.data = d; S.error = null; S.loadedAt = Date.now(); }
    } catch (e) { S.error = e.message || String(e); if (!quiet) S.data = null; }
    S.loading = false; render();
  }
  async function load() {
    try { await loadWeeks(); } catch (e) { S.error = e.message || String(e); render(); return; }
    await loadWeek();
  }
  async function loadCodes() { try { S.codes = await req('GET', '/uid-labels/formats'); S.codesErr = ''; } catch (e) { S.codesErr = e.message || String(e); } render(); }

  // ── Actions ──
  async function act(a, el, ev) {
    const v = el.getAttribute('data-v');
    switch (a) {
      case 'retry': load(); break;
      case 'week': S.week = v; S.data = null; S.extra = null; S.runOpen = {}; render(); loadWeek(); break;
      case 'week-pick': if (isDate(el.value)) { S.week = mondayOf(el.value); S.data = null; S.extra = null; S.runOpen = {}; render(); loadWeek(); } break;
      case 'run-toggle': { const id = Number(v); const cur = S.runOpen[id] != null ? S.runOpen[id] : (S.data.runs[0] && S.data.runs[0].id === id); S.runOpen = Object.assign({}, S.runOpen, { [id]: !cur }); render(); break; }
      case 'dl': case 'dl-again': {
        const run = el.getAttribute('data-run'), po = el.getAttribute('data-po');
        S.busy = `dl:${run}:${po}`; render();
        try { await download(`/uid-labels/file/${run}/${encodeURIComponent(po)}${a === 'dl-again' ? '?reprint=1' : ''}`, `${po}.pdf`); if (a === 'dl-again') { toast('Downloaded again — recorded as a reprint'); loadWeek(true); } }
        catch (e) { toast('Download failed: ' + (e.message || e)); }
        S.busy = null; render(); break;
      }
      case 'dl-all': { const po = el.getAttribute('data-po'); try { await download(`/uid-labels/po/${S.week}/${encodeURIComponent(po)}.pdf`, `${po}_all.pdf`); } catch (e) { toast('Download failed: ' + (e.message || e)); } break; }
      case 'sample': try { await download(`/uid-labels/sample/${v}.pdf`, `sample_${v}.pdf`); } catch (e) { toast('Download failed: ' + (e.message || e)); } break;
      case 'reconcile': {
        S.busy = 'reconcile'; render();
        try { const r = await req('POST', `/uid-labels/reconcile/${S.week}`); toast(r.labels ? `Issued ${plural(r.labels, 'label')} — run #${r.run_id}` : (r.moved && r.moved.length ? `${plural(r.moved.length, 'PO line')} moved in — nothing printed` : 'Nothing to issue — the ledger matches the plan')); }
        catch (e) { toast(e.message || String(e)); }
        S.busy = null; await loadWeeks().catch(() => {}); loadWeek(true); break;
      }
      case 'email': try { const r = await req('POST', `/uid-labels/run/${v}/email`); toast(r.sent ? 'Email sent' : 'Not sent: ' + (r.reason || '')); loadWeek(true); } catch (e) { toast(e.message || String(e)); } break;
      case 'q': S.q = el.value; render(); break;
      case 'sel': {
        const run = Number(el.getAttribute('data-run')), po = el.getAttribute('data-po');
        const un = new Set(S.unsel[run] || []); if (el.checked) un.delete(po); else un.add(po);
        S.unsel = Object.assign({}, S.unsel, { [run]: un }); render(); break;
      }
      case 'sel-all': case 'sel-none': case 'sel-toggle-all': {
        const run = Number(v); const r = S.data.runs.find(x => x.id === run); if (!r) break;
        const q = S.q.trim().toLowerCase();
        const shown = (r.files || []).filter(f => !q || f.po.toLowerCase().includes(q));
        const un = new Set(S.unsel[run] || []);
        const check = a === 'sel-all' || (a === 'sel-toggle-all' && el.checked);
        for (const f of shown) { if (check) un.delete(f.po); else un.add(f.po); }
        S.unsel = Object.assign({}, S.unsel, { [run]: un }); render(); break;
      }
      case 'zip': {
        const run = Number(v); const r = S.data.runs.find(x => x.id === run); if (!r) break;
        const un = S.unsel[run] || new Set();
        const pos = (r.files || []).filter(f => !un.has(f.po)).map(f => f.po);
        if (!pos.length) break;
        S.busy = `zip:${run}`; render();
        try {
          const all = pos.length === (r.files || []).length;
          await download(`/uid-labels/zip/${run}${all ? '' : `?pos=${encodeURIComponent(pos.join(','))}`}`, `UID_labels_run${run}.zip`);
          toast(`Zip of ${plural(pos.length, 'PDF')} downloaded`);
        } catch (e) { toast('Download failed: ' + (e.message || e)); }
        S.busy = null; render(); break;
      }
      case 'extra-open': {
        const po = el.getAttribute('data-po');
        if (S.extra && S.extra.po === po) { S.extra = null; render(); break; }
        const p = S.data.pos.find(x => x.po === po);
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
          S.busy = null; S.extra = null; S.runOpen = {}; toast(`Issued ${plural(r.labels, 'new label')} — run #${r.run_id}`);
          await loadWeeks().catch(() => {}); await loadWeek(true);
        } catch (err) { S.busy = null; e.err = err.message || String(err); render(); }
        break;
      }
      case 'codes': S.codesOpen = true; S.addFmt = S.addFmt || 'footwear'; render(); loadCodes(); break;
      case 'codes-close': S.codesOpen = false; render(); loadWeeks().then(() => render()).catch(() => {}); loadWeek(true); break;
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
      if (!el || el.tagName === 'INPUT' || el.tagName === 'SELECT') return;
      if (el.disabled) return;
      act(el.getAttribute('data-act'), el, ev);
    });
    host.addEventListener('input', (ev) => {
      const el = ev.target.closest('[data-act]'); if (!el) return;
      const a = el.getAttribute('data-act');
      if (a === 'q' || a === 'extra-qty' || a === 'extra-reason' || a === 'extra-sku') act(a, el, ev);
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
  // Escape closes the department panel wherever focus ended up (a re-render can drop it to <body>).
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Escape' || !S.codesOpen || !S.root || !S.root.isConnected) return;
    const pg = document.getElementById('page-uid-labels');
    if (!pg || pg.style.display === 'none') return;
    act('codes-close', S.root, ev);
  });

  function mount(pg) {
    styles();
    S.root = pg; bind(pg);
    render();
    load();
    clearInterval(S.timer);
    S.timer = setInterval(() => { if (S.root && S.root.isConnected && S.data && !S.busy && !S.extra) loadWeek(true); }, REFRESH_MS);
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
    if (S.codesOpen) { S.codesOpen = false; render(); }
  };
  // The nav entry is internal-only, like the page: a client login never sees it.
  function gateNav() { const a = document.getElementById('nav-vas-uidlabels'); if (a) a.style.display = window.pinpointIsInternal === true ? '' : 'none'; }
  gateNav(); window.addEventListener('tenancy:ready', gateNav);

  window.__uidLabels = { reload: () => load(), state: () => S, version: VERSION };
  console.log('[uid-labels] v' + VERSION + ' loaded');
})();
