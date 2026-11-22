/* ── VelOzity Pinpoint — Pulse tasks v1 (Phase 3c) ──
   A quiet strip at the top of the Pulse panel: "Tasks · 3 open". Open it and you see what has
   been handed to Pulse or to people — title, who, due, status — with the emails Pulse sent for
   each and any reply that came back. Close a task or cancel it from here; everything else
   (adding, emailing) happens by asking Pulse.

   Only VelOzity admins see it: the API answers 403 for everyone else and the strip never mounts.
   It refreshes when the panel opens and after Pulse does something.                           */
;(function () {
  'use strict';
  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E', OK = '#1F7A4D', AMBER = '#B07000';
  const css = `
    .pt-strip{display:none;margin:0 0 8px;border:.5px solid rgba(0,0,0,.08);border-radius:10px;background:#fff;font-size:12px;color:${DARK};overflow:hidden;}
    .pt-strip.show{display:block;}
    .pt-head{display:flex;align-items:center;gap:8px;padding:7px 12px;cursor:pointer;user-select:none;}
    .pt-head b{font-weight:600;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:${MID};}
    .pt-head .pt-n{margin-left:auto;font-size:11px;color:${MID};} .pt-head .pt-n.hot{color:${BRAND};font-weight:600;}
    .pt-head .pt-car{width:0;height:0;border-left:4px solid transparent;border-right:4px solid transparent;border-top:5px solid ${LIGHT};transition:transform .15s;} .pt-strip.open .pt-car{transform:rotate(180deg);}
    .pt-list{display:none;border-top:.5px solid rgba(0,0,0,.06);max-height:260px;overflow:auto;} .pt-strip.open .pt-list{display:block;}
    .pt-row{display:grid;grid-template-columns:1fr auto;gap:2px 10px;padding:8px 12px;border-bottom:.5px solid rgba(0,0,0,.05);} .pt-row:last-child{border-bottom:0;}
    .pt-t{font-size:12.5px;} .pt-meta{grid-column:1/-1;font-size:11px;color:${MID};display:flex;flex-wrap:wrap;gap:4px 10px;align-items:center;}
    .pt-chip{display:inline-block;padding:1px 7px;border-radius:9px;font-size:10px;font-weight:500;border:.5px solid rgba(0,0,0,.08);background:#F5F5F7;color:${MID};}
    .pt-chip.replied{background:#FFF7F9;color:${BRAND};border-color:rgba(153,0,51,.3);} .pt-chip.waiting{color:${AMBER};background:#FFFBF2;} .pt-chip.overdue{color:#B42318;background:#FDF2F1;}
    .pt-reply{grid-column:1/-1;margin-top:3px;padding:6px 9px;border-left:2px solid ${BRAND};background:#FAFAFA;font-size:11.5px;color:${DARK};white-space:pre-wrap;}
    .pt-reply small{display:block;color:${LIGHT};font-size:10px;margin-bottom:2px;}
    .pt-acts{display:flex;gap:6px;align-self:start;} .pt-acts a{font-size:11px;color:${MID};cursor:pointer;text-decoration:underline;} .pt-acts a:hover{color:${BRAND};}
    .pt-empty{padding:10px 12px;color:${LIGHT};font-size:11.5px;}
  `;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
  async function call(path, init) {
    let token = null; if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
    const h = Object.assign({ 'Content-Type': 'application/json' }, token ? { Authorization: 'Bearer ' + token } : {}); if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, Object.assign({ headers: h }, init || {}));
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(j.message || j.error || ('HTTP ' + r.status)); e.status = r.status; throw e; }
    return j;
  }
  const fmtDue = (d) => { if (!d) return ''; const s = String(d); const dt = new Date(s.slice(0, 10) + 'T00:00:00'); const day = isNaN(dt) ? s : dt.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' }); return s.length > 10 ? `${day} ${s.slice(11, 16)}` : day; };
  const today = () => new Date().toISOString().slice(0, 10);

  const T = window._pulseTasks = { allowed: null, tasks: [], open: 0, el: null, expanded: false };
  function mount() {
    const panel = document.getElementById('pulse-panel'), chat = document.getElementById('pulse-chat');
    if (!panel || !chat || document.getElementById('pt-strip')) return false;
    const el = T.el = document.createElement('div'); el.id = 'pt-strip'; el.className = 'pt-strip';
    el.innerHTML = `<div class="pt-head"><b>Pulse tasks</b><span class="pt-car"></span><span class="pt-n"></span></div><div class="pt-list"></div>`;
    el.querySelector('.pt-head').onclick = () => { T.expanded = !T.expanded; el.classList.toggle('open', T.expanded); try { localStorage.setItem('pulse_tasks_open', T.expanded ? '1' : '0'); } catch (_) {} };
    try { T.expanded = localStorage.getItem('pulse_tasks_open') === '1'; } catch (_) {}
    el.classList.toggle('open', T.expanded);
    panel.insertBefore(el, chat);
    new MutationObserver(() => { if (panel.classList.contains('open')) refresh(); }).observe(panel, { attributes: true, attributeFilter: ['class'] });
    window.addEventListener('pulse:actions', () => setTimeout(refresh, 400));
    window.addEventListener('pulse:undone', () => setTimeout(refresh, 400));
    window.addEventListener('pulse:action-settled', () => setTimeout(refresh, 400));
    return true;
  }
  let busy = false;
  async function refresh() {
    if (busy || T.allowed === false) return; busy = true;
    try {
      const j = await call('/pulse/tasks?status=open');
      T.allowed = true; T.tasks = j.tasks || []; T.open = j.open || 0; paint();
    } catch (e) {
      if (e.status === 403 || e.status === 401) { T.allowed = false; if (T.el) T.el.classList.remove('show'); }
    } finally { busy = false; }
  }
  function paint() {
    const el = T.el; if (!el) return;
    el.classList.add('show');
    const n = el.querySelector('.pt-n'); const replied = T.tasks.filter(t => t.status === 'replied').length;
    n.textContent = T.open ? `${T.open} open${replied ? ` · ${replied} repl${replied === 1 ? 'y' : 'ies'}` : ''}` : 'none open'; n.classList.toggle('hot', replied > 0);
    const list = el.querySelector('.pt-list');
    if (!T.tasks.length) { list.innerHTML = `<div class="pt-empty">Nothing outstanding. Ask Pulse to add a task or send an email and it will appear here.</div>`; return; }
    list.innerHTML = T.tasks.map(t => {
      const overdue = t.due_at && String(t.due_at).slice(0, 10) < today();
      const chips = [`<span class="pt-chip ${t.status}">${esc(t.status === 'waiting' ? 'awaiting reply' : t.status)}</span>`, overdue ? `<span class="pt-chip overdue">overdue</span>` : ''].join('');
      const mails = (t.emails || []).map(e => `<span>✉ ${esc(e.to.join(', '))} · ${esc(e.status === 'held' ? 'sending shortly' : e.status)}</span>`).join('');
      const replies = (t.replies || []).map(r => `<div class="pt-reply"><small>${esc(r.from_addr)} · ${esc(String(r.received_at).slice(0, 16).replace('T', ' '))}</small>${esc(r.text)}</div>`).join('');
      return `<div class="pt-row" data-id="${t.id}"><div class="pt-t">#${t.id} ${esc(t.title)}</div>
        <div class="pt-acts">${t.status !== 'done' ? `<a data-done>done</a>` : ''}<a data-cancel>cancel</a></div>
        <div class="pt-meta">${chips}<span>${esc(t.owner === 'pulse' ? 'Pulse' : t.owner)}</span>${t.due_at ? `<span>due ${esc(fmtDue(t.due_at))}</span>` : ''}${t.about ? `<span>${esc(t.about)}</span>` : ''}${mails}</div>${replies}</div>`;
    }).join('');
    list.querySelectorAll('[data-done]').forEach(a => { a.onclick = () => act(a.closest('.pt-row').dataset.id, 'done'); });
    list.querySelectorAll('[data-cancel]').forEach(a => { a.onclick = () => act(a.closest('.pt-row').dataset.id, 'cancelled'); });
  }
  async function act(id, status) {
    try { await call(`/pulse/tasks/${id}`, { method: 'PATCH', body: JSON.stringify({ status, outcome: status === 'done' ? 'Closed from the panel' : 'Cancelled from the panel' }) }); } catch (e) { console.warn('[Pulse tasks]', e.message); }
    refresh();
  }
  Object.assign(T, { refresh, mount });
  let tries = 0; const iv = setInterval(() => { if (mount()) { clearInterval(iv); refresh(); } else if (++tries > 60) clearInterval(iv); }, 250);
})();
