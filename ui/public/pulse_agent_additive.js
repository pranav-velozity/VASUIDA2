/* ── VelOzity Pinpoint — Pulse agent UI v2 ──
   Pulse now looks things up through tools before it answers. This module shows that work
   in the panel, adds a Spoken toggle, and renders the proposed-action card the write tools
   will send in Phase 3. It wraps the existing pulseSend() in index.html rather than
   replacing the panel, so the inline chat keeps working if this file is missing.

   WHAT THE USER SEES
   - Under each answer, a quiet line: "Looked up · week status · receiving W46 · 1.2 s".
     People trust a figure more when they can see where it came from, and it tells them
     when Pulse answered from memory instead (no line at all).
   - A Spoken switch on the bar. On, Pulse answers in two or three sentences written for the
     ear and the panel shows that text; Phase 2 plays it through a voice. Off is the default.
   - Action cards (Phase 3). Two kinds come back in `actions`:
       done     — an Auto-tier write already happened: "Confirmed departure for MSKU… — undo"
                  (undo is live for 10 minutes, then the link goes)
       proposed — a Confirm-tier write is waiting: "Pulse wants to: record arrival… as 16 Oct"
                  with Confirm / Cancel. Confirm runs it; Cancel voids the token.
     The voice module drives the same cards from a spoken yes/no via window._pulseCards.     */
;(function () {
  'use strict';

  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E';

  const css = `
    .pa-trace{display:flex;flex-wrap:wrap;gap:4px 8px;align-items:center;margin-top:6px;font-size:10.5px;color:${LIGHT};}
    .pa-trace b{font-weight:500;color:${MID};}
    .pa-trace .pa-t{display:inline-flex;align-items:center;gap:4px;padding:1px 7px;border-radius:9px;border:.5px solid rgba(0,0,0,.08);background:#fff;color:${MID};}
    .pa-trace .pa-t.bad{color:#B42318;border-color:rgba(180,35,24,.3);}
    .pa-spk{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:${MID};cursor:pointer;user-select:none;white-space:nowrap;margin-right:6px;}
    .pa-spk .pa-sw{width:26px;height:15px;border-radius:8px;background:#D6D6D2;position:relative;transition:background .15s;}
    .pa-spk .pa-sw::after{content:'';position:absolute;top:2px;left:2px;width:11px;height:11px;border-radius:50%;background:#fff;transition:left .15s;}
    .pa-spk.on .pa-sw{background:${BRAND};} .pa-spk.on .pa-sw::after{left:13px;}
    .pa-card{margin-top:8px;border:1px solid rgba(153,0,51,.35);border-left:3px solid ${BRAND};border-radius:10px;padding:10px 12px;background:#FFF7F9;}
    .pa-card .pa-k{font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:${BRAND};font-weight:600;}
    .pa-card .pa-what{font-size:13px;color:${DARK};margin:4px 0 8px;}
    .pa-card.done{background:#F6FBF8;border-color:rgba(31,122,77,.35);border-left-color:#1F7A4D;padding:8px 12px;}
    .pa-card .pa-btns{display:flex;gap:8px;}
    .pa-card button{height:28px;padding:0 12px;border-radius:7px;border:.5px solid rgba(0,0,0,.15);background:#fff;font:500 12px inherit;font-family:inherit;cursor:pointer;color:${DARK};}
    .pa-card button.p{background:${BRAND};border-color:${BRAND};color:#fff;}
    .pa-receipt{font-size:11.5px;color:${DARK};display:flex;gap:6px;align-items:center;flex-wrap:wrap;} .pa-card.proposed .pa-receipt{margin-top:6px;color:${MID};}
    .pa-receipt a{color:${BRAND};cursor:pointer;text-decoration:underline;}
    .pulse-msg.ai .pa-spoken{font-size:13.5px;line-height:1.5;}
  `;
  const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style);

  const TOOL_LABEL = {
    get_week_status: 'week status', find_movement: 'movement search', get_movement: 'movement detail', get_transit_board: 'transit board',
    get_receiving: 'receiving', get_vas_progress: 'VAS progress', get_last_mile: 'last mile', get_uid_labels: 'UID labels', get_alerts: 'alerts',
    get_air_quotes: 'air quotes', get_non_compliance: 'non-compliance', get_ops_summary: 'ops summary', list_weeks: 'weeks', get_invoices: 'invoices', get_pl: 'P&L',
    add_note: 'note added', confirm_milestone: 'milestone confirmed', amend_milestone: 'milestone date', mark_last_mile: 'last mile', acknowledge_alert: 'alert acknowledged',
  };
  const isoOf = (ymd) => { const d = new Date(String(ymd || '').slice(0, 10) + 'T00:00:00Z'); if (isNaN(d)) return ''; const day = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - day + 3); const ft = new Date(Date.UTC(d.getUTCFullYear(), 0, 4)); return 'W' + (1 + Math.round(((d - ft) / 86400000 - 3 + ((ft.getUTCDay() + 6) % 7)) / 7)); };
  const argHint = (t) => {
    const a = t.args || {};
    if (a.week) return /^\d{4}-\d{2}-\d{2}$/.test(a.week) ? isoOf(a.week) : String(a.week);
    if (a.query) return String(a.query).slice(0, 18);
    if (a.state) return String(a.state);
    return '';
  };
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

  // ── Spoken toggle on the bar ──
  window._pulseStyle = window._pulseStyle || 'text';
  function mountToggle() {
    const bar = document.getElementById('pulse-bar');
    if (!bar || document.getElementById('pa-spoken')) return;
    const el = document.createElement('label');
    el.id = 'pa-spoken'; el.className = 'pa-spk'; el.title = 'Answer in two or three sentences written to be read aloud';
    el.innerHTML = '<span class="pa-sw"></span><span>Spoken</span>';
    el.onclick = () => { window._pulseStyle = window._pulseStyle === 'spoken' ? 'text' : 'spoken'; el.classList.toggle('on', window._pulseStyle === 'spoken'); };
    const quick = document.getElementById('pulse-quick');
    bar.insertBefore(el, quick || null);
  }

  // ── Trace line under an answer ──
  function traceHtml(tools) {
    if (!Array.isArray(tools) || !tools.length) return '';
    const ms = tools.reduce((s, t) => s + (Number(t.ms) || 0), 0);
    const reads = tools.filter(t => !t.write && !t.proposed), writes = tools.filter(t => t.write || t.proposed);
    const pill = (t) => `<span class="pa-t ${t.ok ? '' : 'bad'}" title="${esc(JSON.stringify(t.args || {}))}">${esc(TOOL_LABEL[t.tool] || t.tool)}${t.proposed ? ' · awaiting yes' : (argHint(t) ? ' · ' + esc(argHint(t)) : '')}</span>`;
    return `<div class="pa-trace">${reads.length ? `<b>Looked up</b>${reads.map(pill).join('')}` : ''}${writes.length ? `<b>${reads.length ? '· ' : ''}Did</b>${writes.map(pill).join('')}` : ''}<span>${(ms / 1000).toFixed(1)} s</span></div>`;
  }

  // ── Action cards ──
  const cards = new Map();                       // token → { confirm(), cancel() } for the voice module
  window._pulseCards = {
    confirm: (token) => { const c = cards.get(token); return c ? c.confirm() : Promise.reject(new Error('no such card')); },
    cancel: (token) => { const c = cards.get(token); return c ? c.cancel() : Promise.resolve(); },
    pending: () => [...cards.keys()],
  };
  function receiptHtml(text, undoToken, undoUntil) {
    return `<div class="pa-receipt"><span>✓</span><span>${esc(text || 'Done.')}</span>${undoToken ? ` <a data-undo>undo</a>` : ''}</div>`;
  }
  function wireUndo(wrap, undoToken, undoUntil) {
    const u = wrap.querySelector('[data-undo]'); if (!u) return;
    const ttl = undoUntil ? Math.max(0, undoUntil - Date.now()) : 10 * 60 * 1000;
    const timer = setTimeout(() => { u.remove(); }, ttl);
    u.onclick = async () => {
      clearTimeout(timer); u.textContent = 'undoing…'; u.onclick = null;
      try { const x = await window._pulseApi('/pulse/undo', { token: undoToken }); wrap.innerHTML = `<div class="pa-receipt"><span>↺</span><span>${esc(x.receipt || 'Undone.')}</span></div>`; }
      catch (e) { wrap.innerHTML = `<div class="pa-receipt" style="color:#B42318">${esc(e.message || 'Could not undo')}</div>`; }
      window.dispatchEvent(new CustomEvent('pulse:undone', { detail: { token: undoToken } }));
    };
  }
  function actionCard(action) {
    const wrap = document.createElement('div');
    wrap.className = 'pa-card ' + (action.kind === 'done' ? 'done' : 'proposed');
    wrap.dataset.token = action.token || action.undo_token || '';
    if (action.kind === 'done') {
      wrap.innerHTML = receiptHtml(action.summary, action.undo_token, action.undo_until);
      wireUndo(wrap, action.undo_token, action.undo_until);
      return wrap;
    }
    wrap.innerHTML = `<div class="pa-k">Pulse wants to</div><div class="pa-what">${esc(action.summary || action.tool)}</div>
      <div class="pa-btns"><button class="p" data-ok>Confirm</button><button data-no>Cancel</button></div>`;
    let settled = false;
    const finish = () => { settled = true; cards.delete(action.token); window.dispatchEvent(new CustomEvent('pulse:action-settled', { detail: { token: action.token } })); };
    const cancel = async () => {
      if (settled) return; finish();
      wrap.innerHTML = `<div class="pa-receipt">Cancelled.</div>`;
      try { await window._pulseApi('/pulse/cancel', { token: action.token }); } catch (_) {}
    };
    const confirm = async () => {
      if (settled) return null; finish();
      wrap.querySelector('.pa-btns').innerHTML = '<span class="pa-receipt">Doing it…</span>';
      try {
        const r = await window._pulseApi('/pulse/confirm', { token: action.token });
        wrap.innerHTML = receiptHtml(r.receipt, r.undo_token, r.undo_until);
        wireUndo(wrap, r.undo_token, r.undo_until);
        return r;
      } catch (e) { wrap.innerHTML = `<div class="pa-receipt" style="color:#B42318">${esc(e.message || 'Failed')}</div>`; throw e; }
    };
    wrap.querySelector('[data-no]').onclick = cancel;
    wrap.querySelector('[data-ok]').onclick = confirm;
    cards.set(action.token, { confirm, cancel });
    // A proposal is good for two minutes on the server; the card follows.
    setTimeout(() => { if (!settled) { finish(); wrap.innerHTML = `<div class="pa-receipt">Expired — ask Pulse again.</div>`; } }, Math.max(5000, ((action.expires_in || 120) * 1000) - 2000));
    return wrap;
  }

  // Small authenticated POST used by the card.
  window._pulseApi = async function (path, body) {
    const apiBase = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
    let token = null; if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
    const r = await fetch(apiBase + path, { method: 'POST', headers: Object.assign({ 'Content-Type': 'application/json' }, token ? { Authorization: 'Bearer ' + token } : {}), body: JSON.stringify(body || {}) });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.message || j.error || ('HTTP ' + r.status));
    return j;
  };

  // ── Wrap pulseSend: same request, plus style, trace and cards ──
  function install() {
    if (typeof window.pulseSend !== 'function' || window.pulseSend.__agent) return;
    const addMessage = window.pulseAddMessage, showLoading = window.pulseShowLoading;
    if (typeof addMessage !== 'function') return;

    window.pulseSend = async function pulseSendAgent() {
      const input = document.getElementById('pulse-input-display');
      if (!input) return;
      const text = input.value.trim();
      if (!text) return;
      input.value = '';
      if (typeof window.togglePulse === 'function') window.togglePulse(true);
      addMessage(text, 'user');
      const loading = typeof showLoading === 'function' ? showLoading() : null;

      window._pulseHistory = window._pulseHistory || [];
      window._pulseHistory.push({ role: 'user', content: text });
      if (window._pulseHistory.length > 20) window._pulseHistory = window._pulseHistory.slice(-20);
      if (typeof window.pulseLoadContext === 'function') { try { await window.pulseLoadContext(); } catch (_) {} }

      const currentWeek = {
        facility: ((window.state?.plan) || []).map(p => String(p.facility_name || p.facility || '').trim()).find(Boolean) || String(window.state?.facility || '').trim() || '',
        week_start: window.state?.weekStart || '',
      };
      try {
        const apiBase = (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
        let token = null; if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
        const resp = await fetch(apiBase + '/pulse/chat', {
          method: 'POST',
          headers: Object.assign({ 'Content-Type': 'application/json' }, token ? { Authorization: 'Bearer ' + token } : {}),
          body: JSON.stringify({ messages: window._pulseHistory, pulseContext: window._pulseSessionContext || null, currentWeek, style: window._pulseStyle || 'text' }),
        });
        if (loading) loading.remove();
        if (!resp.ok) {
          const err = await resp.text();
          addMessage('Sorry, I ran into an issue connecting. Please try again.', 'ai');
          console.error('[Pulse] API error:', err);
          return;
        }
        const data = await resp.json();
        const reply = data.reply || 'No response.';
        addMessage(reply, 'ai');
        window._pulseHistory.push({ role: 'assistant', content: reply });

        // Decorate the message just added: trace line, spoken class, any action cards.
        const chat = document.getElementById('pulse-chat');
        const last = chat && chat.querySelector('.pulse-msg.ai:last-of-type');
        if (last) {
          if (data.style === 'spoken') last.classList.add('pa-spoken');
          const tr = traceHtml(data.tools);
          if (tr) last.insertAdjacentHTML('beforeend', tr);
          const acts = Array.isArray(data.actions) ? data.actions : [];
          for (const a of acts) last.appendChild(actionCard(a));
          if (data.spoken) window.dispatchEvent(new CustomEvent('pulse:spoken', { detail: { text: data.spoken, actions: acts } }));
          if (acts.length) window.dispatchEvent(new CustomEvent('pulse:actions', { detail: { actions: acts } }));
          chat.scrollTop = chat.scrollHeight;
        }
      } catch (e) {
        if (loading) loading.remove();
        addMessage('Sorry, something went wrong. Please try again.', 'ai');
        console.error('[Pulse] error:', e);
      }
    };
    window.pulseSend.__agent = true;
    mountToggle();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install); else install();
  // The inline script defines pulseSend after this file may have run; retry briefly.
  let tries = 0; const t = setInterval(() => { install(); if (window.pulseSend && window.pulseSend.__agent || ++tries > 40) clearInterval(t); }, 250);
})();
