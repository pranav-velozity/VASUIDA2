/* ── VelOzity Pinpoint — Pulse meetings v1 (Phase 4) ──
   A second quiet strip in the Pulse panel, under Tasks: "Pulse meetings". From here you paste
   a Teams or Meet link and Pulse joins as a participant; meetings created by inviting
   pulseAI@velozity.au appear on their own. While a meeting is live you see the last few
   things said and Pulse's answers; afterwards the summary and, one by one, the actions Pulse
   was asked for in the room — each with Confirm / Dismiss, run as you, with the usual receipt.

   VelOzity admins only (the API answers 403 for everyone else and the strip never mounts).  */
;(function () {
  'use strict';
  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E', OK = '#1F7A4D', AMBER = '#B07000', BAD = '#B42318';
  const css = `
    .pm-strip{display:none;margin:0 0 8px;border:.5px solid rgba(0,0,0,.08);border-radius:10px;background:#fff;font-size:12px;color:${DARK};overflow:hidden;}
    .pm-strip.show{display:block;}
    .pm-head{display:flex;align-items:center;gap:8px;padding:7px 12px;cursor:pointer;user-select:none;}
    .pm-head b{font-weight:600;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:${MID};}
    .pm-head .pm-n{margin-left:auto;font-size:11px;color:${MID};} .pm-head .pm-n.live{color:${OK};font-weight:600;}
    .pm-head .pm-car{width:0;height:0;border-left:4px solid transparent;border-right:4px solid transparent;border-top:5px solid ${LIGHT};transition:transform .15s;} .pm-strip.open .pm-car{transform:rotate(180deg);}
    .pm-body{display:none;border-top:.5px solid rgba(0,0,0,.06);} .pm-strip.open .pm-body{display:block;}
    .pm-join{display:flex;gap:6px;padding:8px 12px;border-bottom:.5px solid rgba(0,0,0,.05);}
    .pm-join input{flex:1;height:28px;border:.5px solid rgba(0,0,0,.15);border-radius:7px;padding:0 9px;font:12px inherit;font-family:inherit;color:${DARK};min-width:0;}
    .pm-join button{height:28px;padding:0 12px;border-radius:7px;border:.5px solid ${BRAND};background:${BRAND};color:#fff;font:500 12px inherit;font-family:inherit;cursor:pointer;white-space:nowrap;}
    .pm-join button:disabled{opacity:.5;cursor:default;}
    .pm-list{max-height:300px;overflow:auto;}
    .pm-row{padding:8px 12px;border-bottom:.5px solid rgba(0,0,0,.05);} .pm-row:last-child{border-bottom:0;}
    .pm-t{display:flex;align-items:center;gap:8px;font-size:12.5px;} .pm-t .pm-title{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
    .pm-chip{display:inline-block;padding:1px 7px;border-radius:9px;font-size:10px;font-weight:500;border:.5px solid rgba(0,0,0,.08);background:#F5F5F7;color:${MID};flex:none;}
    .pm-chip.live{color:${OK};background:#F0FAF4;border-color:rgba(31,122,77,.3);} .pm-chip.joining,.pm-chip.waiting{color:${AMBER};background:#FFFBF2;} .pm-chip.failed{color:${BAD};background:#FDF2F1;}
    .pm-meta{font-size:11px;color:${MID};margin-top:2px;display:flex;flex-wrap:wrap;gap:4px 10px;}
    .pm-meta a{color:${MID};cursor:pointer;text-decoration:underline;} .pm-meta a:hover{color:${BRAND};}
    .pm-lines{margin-top:5px;padding:6px 9px;border-left:2px solid rgba(0,0,0,.08);background:#FAFAFA;font-size:11.5px;color:${DARK};}
    .pm-lines div{margin:1px 0;} .pm-lines .q{color:${BRAND};} .pm-lines .a{color:${OK};}
    .pm-sum{margin-top:5px;white-space:pre-wrap;font-size:11.5px;color:${DARK};max-height:160px;overflow:auto;padding:6px 9px;border-left:2px solid ${BRAND};background:#FAFAFA;}
    .pm-act{margin-top:6px;border:1px solid rgba(153,0,51,.3);border-left:3px solid ${BRAND};border-radius:8px;padding:7px 10px;background:#FFF7F9;display:flex;gap:8px;align-items:center;}
    .pm-act .pm-what{flex:1;font-size:12px;} .pm-act button{height:26px;padding:0 10px;border-radius:7px;border:.5px solid rgba(0,0,0,.15);background:#fff;font:500 11.5px inherit;font-family:inherit;cursor:pointer;color:${DARK};}
    .pm-act button.p{background:${BRAND};border-color:${BRAND};color:#fff;} .pm-act.done{border-color:rgba(31,122,77,.3);border-left-color:${OK};background:#F6FBF8;} .pm-act.dismissed{opacity:.55;}
    .pm-empty{padding:10px 12px;color:${LIGHT};font-size:11.5px;}
  `;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
  async function call(path, init) {
    let token = null; if (window.Clerk?.session) { try { token = await window.Clerk.session.getToken(); } catch (_) {} }
    const h = Object.assign({ 'Content-Type': 'application/json' }, token ? { Authorization: 'Bearer ' + token } : {}); if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    const r = await fetch(apiBase() + path, Object.assign({ headers: h }, init || {}));
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(j.message || j.error || ('HTTP ' + r.status)); e.status = r.status; e.body = j; throw e; }
    return j;
  }
  const me = () => { try { const u = window.Clerk && window.Clerk.user; return { name: u && u.fullName || null, email: u && u.primaryEmailAddress && u.primaryEmailAddress.emailAddress || null }; } catch (_) { return { name: null, email: null }; } };
  const when = (s) => { if (!s) return ''; const d = new Date(s); return isNaN(d) ? '' : d.toLocaleString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }); };
  const LABEL = { scheduled: 'scheduled', joining: 'joining', waiting: 'in lobby', live: 'live', ended: 'ended', failed: 'failed', cancelled: 'cancelled' };

  const M = window._pulseMeetings = { allowed: null, enabled: true, meetings: [], el: null, expanded: false, open: {} };
  function mount() {
    const panel = document.getElementById('pulse-panel'), chat = document.getElementById('pulse-chat');
    if (!panel || !chat || document.getElementById('pm-strip')) return false;
    const el = M.el = document.createElement('div'); el.id = 'pm-strip'; el.className = 'pm-strip';
    el.innerHTML = `<div class="pm-head"><b>Pulse meetings</b><span class="pm-car"></span><span class="pm-n"></span></div>
      <div class="pm-body"><div class="pm-join"><input type="url" placeholder="Paste a Teams or Meet link — Pulse joins now" aria-label="Meeting link"><button>Join</button></div><div class="pm-list"></div></div>`;
    el.querySelector('.pm-head').onclick = () => { M.expanded = !M.expanded; el.classList.toggle('open', M.expanded); try { localStorage.setItem('pulse_meetings_open', M.expanded ? '1' : '0'); } catch (_) {} };
    try { M.expanded = localStorage.getItem('pulse_meetings_open') === '1'; } catch (_) {}
    el.classList.toggle('open', M.expanded);
    const input = el.querySelector('input'), btn = el.querySelector('.pm-join button');
    btn.onclick = async () => {
      const url = input.value.trim(); if (!url) return;
      btn.disabled = true; btn.textContent = 'Joining…';
      try { await call('/pulse/meetings', { method: 'POST', body: JSON.stringify({ meeting_url: url, organiser: me().email, created_by_name: me().name }) }); input.value = ''; }
      catch (e) { alert(e.message || 'Could not join'); }
      btn.disabled = false; btn.textContent = 'Join'; refresh();
    };
    input.onkeydown = (e) => { if (e.key === 'Enter') btn.click(); };
    const after = document.getElementById('pt-strip');
    if (after && after.parentNode === panel) after.insertAdjacentElement('afterend', el); else panel.insertBefore(el, chat);
    new MutationObserver(() => { if (panel.classList.contains('open')) refresh(); }).observe(panel, { attributes: true, attributeFilter: ['class'] });
    return true;
  }
  let busy = false, timer = null;
  async function refresh() {
    if (busy || M.allowed === false) return; busy = true;
    try { const j = await call('/pulse/meetings'); M.allowed = true; M.enabled = j.enabled !== false; M.meetings = j.meetings || []; paint(); }
    catch (e) { if (e.status === 403 || e.status === 401) { M.allowed = false; if (M.el) M.el.classList.remove('show'); } }
    finally { busy = false; }
    // Poll while something is live.
    clearTimeout(timer);
    if (M.meetings.some(m => ['joining', 'waiting', 'live'].includes(m.status))) timer = setTimeout(refresh, 6000);
  }
  function paint() {
    const el = M.el; if (!el) return;
    el.classList.add('show');
    const live = M.meetings.filter(m => ['joining', 'waiting', 'live'].includes(m.status)).length, up = M.meetings.filter(m => m.status === 'scheduled').length, todo = M.meetings.reduce((s, m) => s + (m.proposed || 0), 0);
    const n = el.querySelector('.pm-n');
    n.textContent = live ? `${live} live` : up ? `${up} upcoming` : todo ? `${todo} to confirm` : (M.enabled ? '' : 'not set up');
    n.classList.toggle('live', live > 0);
    const list = el.querySelector('.pm-list');
    if (!M.enabled) { list.innerHTML = `<div class="pm-empty">Meetings are not configured on the server yet (Recall key).</div>`; return; }
    if (!M.meetings.length) { list.innerHTML = `<div class="pm-empty">No meetings yet. Paste a link above, or invite pulseAI@velozity.au to a meeting.</div>`; return; }
    list.innerHTML = M.meetings.map(m => {
      const open = !!M.open[m.id];
      const lines = (m.recent || []).map(l => `<div class="${l.is_question ? 'q' : ''}">${esc(l.speaker)}: ${esc(l.text)}</div>${l.answer ? `<div class="a">Pulse: ${esc(l.answer)}</div>` : ''}`).join('');
      const acts = (m.actions || []).map(a => `<div class="pm-act ${a.status}" data-m="${m.id}" data-a="${a.id}"><div class="pm-what">${esc(a.summary)}${a.status === 'done' ? ` <span style="color:${OK}">✓ ${esc(a.receipt || '')}</span>` : a.status === 'failed' ? ` <span style="color:${BAD}">${esc(a.receipt || 'failed')}</span>` : a.status === 'dismissed' ? ' <span>dismissed</span>' : ''}</div>${a.status === 'proposed' ? `<button class="p" data-ok>Confirm</button><button data-no>Dismiss</button>` : ''}</div>`).join('');
      return `<div class="pm-row" data-id="${m.id}">
        <div class="pm-t"><span class="pm-chip ${m.status}">${esc(LABEL[m.status] || m.status)}</span><span class="pm-title">${esc(m.title || m.meeting_url)}</span>${m.proposed ? `<span class="pm-chip" style="color:${BRAND}">${m.proposed} to confirm</span>` : ''}</div>
        <div class="pm-meta">${m.scheduled_at ? `<span>${esc(when(m.scheduled_at))}</span>` : ''}<span>${esc(m.platform || '')}</span>${m.lines ? `<span>${m.lines} exchanges</span>` : ''}${m.last_error ? `<span style="color:${BAD}">${esc(m.last_error)}</span>` : ''}
          ${(m.status === 'scheduled' && !m.bot_id) || m.status === 'failed' ? `<a data-join>${m.status === 'failed' ? 'retry' : 'join now'}</a>` : ''}${['scheduled', 'joining', 'waiting', 'live'].includes(m.status) ? `<a data-mode title="${m.open_mode ? 'Open mode: Pulse answers any operations question. Click for name only.' : 'Name only: say Pulse (follow-ups count for a while). Click for open mode.'}">${m.open_mode ? 'open mode' : 'name only'}</a>` : ''}${['joining', 'waiting', 'live'].includes(m.status) ? `<a data-answer title="Pulse answers what was just said, no wake word needed">answer that</a>` : ''}${['scheduled', 'joining', 'waiting', 'live'].includes(m.status) ? `<a data-leave>${m.status === 'scheduled' ? 'cancel' : 'leave'}</a>` : ''}${m.summary ? `<a data-toggle>${open ? 'hide notes' : 'notes'}</a>` : ''}</div>
        ${['live', 'joining', 'waiting'].includes(m.status) && lines ? `<div class="pm-lines">${lines}</div>` : ''}
        ${open && m.summary ? `<div class="pm-sum">${esc(m.summary)}</div>` : ''}
        ${acts}</div>`;
    }).join('');
    list.querySelectorAll('[data-mode]').forEach(a => { a.onclick = async () => { const id = a.closest('.pm-row').dataset.id, open = a.textContent !== 'open mode'; a.textContent = '…'; try { await call(`/pulse/meetings/${id}/mode`, { method: 'POST', body: JSON.stringify({ open }) }); } catch (e) { console.warn('[Pulse meetings]', e.message); } refresh(); }; });
    // "Answer that": when someone is not being picked up (accent, cross-talk), Pulse takes the last
    // 20 s as the question — or, if nothing was heard, answers the next thing anyone says.
    list.querySelectorAll('[data-answer]').forEach(a => { a.onclick = async () => {
      const id = a.closest('.pm-row').dataset.id; a.textContent = '…';
      try { const r = await call(`/pulse/meetings/${id}/answer`, { method: 'POST', body: '{}' }); a.textContent = r.mode === 'next' ? 'listening…' : 'answering…'; }
      catch (e) { a.textContent = 'answer that'; console.warn('[Pulse meetings]', e.message); return; }
      setTimeout(refresh, 4000);
    }; });
    list.querySelectorAll('[data-leave]').forEach(a => { a.onclick = async () => { const id = a.closest('.pm-row').dataset.id; try { await call(`/pulse/meetings/${id}/leave`, { method: 'POST', body: '{}' }); } catch (e) { console.warn('[Pulse meetings]', e.message); } refresh(); }; });
    list.querySelectorAll('[data-join]').forEach(a => { a.onclick = async () => { const id = a.closest('.pm-row').dataset.id; a.textContent = 'joining…'; try { await call(`/pulse/meetings/${id}/join`, { method: 'POST', body: '{}' }); } catch (e) { alert(e.message || 'Could not join'); } refresh(); }; });
    list.querySelectorAll('[data-toggle]').forEach(a => { a.onclick = () => { const id = a.closest('.pm-row').dataset.id; M.open[id] = !M.open[id]; paint(); }; });
    list.querySelectorAll('.pm-act [data-ok], .pm-act [data-no]').forEach(b => {
      b.onclick = async () => {
        const card = b.closest('.pm-act'); const verb = b.hasAttribute('data-ok') ? 'confirm' : 'dismiss';
        card.querySelectorAll('button').forEach(x => { x.disabled = true; }); if (verb === 'confirm') b.textContent = 'Doing it…';
        try { await call(`/pulse/meetings/${card.dataset.m}/actions/${card.dataset.a}/${verb}`, { method: 'POST', body: '{}' }); }
        catch (e) { card.querySelector('.pm-what').insertAdjacentHTML('beforeend', ` <span style="color:${BAD}">${esc(e.message || 'failed')}</span>`); }
        refresh();
      };
    });
  }
  Object.assign(M, { refresh, mount });
  let tries = 0; const iv = setInterval(() => { if (mount()) { clearInterval(iv); refresh(); } else if (++tries > 80) clearInterval(iv); }, 250);
})();
