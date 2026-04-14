/* ── VelOzity Pinpoint — Collaboration Module v1 ── */
;(function () {
'use strict';

// ── Constants ────────────────────────────────────────────────────
const BRAND   = '#990033';
const DARK    = '#1C1C1E';
const MID     = '#6E6E73';
const LIGHT   = '#AEAEB2';
const BG      = '#F5F5F7';
const PULSE_BG = 'linear-gradient(135deg,#1a0010 0%,#2d0018 100%)';

const ROLE_COLOR = {
  'Admin':    { bg:'rgba(153,0,51,0.1)',   color:'#990033' },
  'Facility': { bg:'rgba(59,130,246,0.1)', color:'#1d4ed8' },
  'Client':   { bg:'rgba(52,199,89,0.1)',  color:'#166534' },
  'Member':   { bg:'rgba(174,174,178,0.1)',color:'#6E6E73' },
  'AI':       { bg:'rgba(26,0,16,0.08)',   color:'#990033' },
};

// ── State ────────────────────────────────────────────────────────
let _colState = {
  threads: [],
  activeThread: null,
  messages: [],
  pollTimer: null,
  badgeCount: 0,
  panelOpen: false,
  view: 'inbox', // 'inbox' | 'thread'
  uploading: false,
  pendingAttachments: [], // [{name,url,type,size}]
};

// ── API helpers ──────────────────────────────────────────────────
function colApiBase() {
  return (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
}
async function colToken() {
  if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch(_){} }
  return null;
}
async function colApi(path, opts = {}) {
  const token = await colToken();
  const headers = { ...(opts.headers || {}) };
  if (token) headers['Authorization'] = 'Bearer ' + token;
  if (!(opts.body instanceof FormData) && opts.body && typeof opts.body === 'object') {
    headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(opts.body);
  }
  const r = await fetch(colApiBase() + path, { ...opts, headers });
  if (!r.ok) { const t = await r.text(); throw new Error('HTTP ' + r.status + ' ' + t); }
  return r.json();
}

// ── Formatters ───────────────────────────────────────────────────
function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso.endsWith('Z') ? iso : iso + 'Z');
    const now = new Date();
    const diff = (now - d) / 1000;
    if (diff < 60)   return 'just now';
    if (diff < 3600) return Math.floor(diff/60) + 'm ago';
    if (diff < 86400)return Math.floor(diff/3600) + 'h ago';
    return d.toLocaleDateString('en-AU', { day:'numeric', month:'short' });
  } catch { return ''; }
}
function esc(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function roleBadge(role) {
  const c = ROLE_COLOR[role] || ROLE_COLOR['Member'];
  return `<span style="display:inline-block;padding:1px 7px;border-radius:20px;font-size:9px;font-weight:700;background:${c.bg};color:${c.color};">${role}</span>`;
}
function initials(name) {
  const parts = String(name||'?').trim().split(' ');
  return (parts[0][0] + (parts[1]?.[0]||'')).toUpperCase();
}
function ctxIcon(type) {
  const icons = {
    po:       '📦', week:     '📅', lane:     '🚢',
    invoice:  '🧾', supplier: '🏭', general:  '💬',
    sku:      '🏷️', receiving:'📥',
  };
  return icons[type] || '💬';
}
function isImage(type) { return String(type||'').startsWith('image/'); }
function isExcel(name) { return /\.(xlsx|xls|csv)$/i.test(name||''); }
function fileIcon(name, type) {
  if (isImage(type)) return '🖼️';
  if (isExcel(name)) return '📊';
  if (/\.pdf$/i.test(name)) return '📄';
  return '📎';
}

// ── Badge ────────────────────────────────────────────────────────
function _colUpdateBadge(count) {
  _colState.badgeCount = count;
  const badge = document.getElementById('col-badge');
  if (badge) {
    badge.textContent = count > 0 ? (count > 99 ? '99+' : count) : '';
    badge.style.display = count > 0 ? 'flex' : 'none';
  }
}

async function _colRefreshBadge() {
  try {
    const data = await colApi('/threads/count');
    _colUpdateBadge(data.open || 0);
  } catch(_) {}
}

// ── Inject nav button ────────────────────────────────────────────
function _colInjectNav() {
  if (document.getElementById('col-nav-btn')) return;
  const userDiv = document.querySelector('.pn-user');
  if (!userDiv) return;

  const btn = document.createElement('button');
  btn.id = 'col-nav-btn';
  btn.title = 'Collaboration Threads';
  btn.style.cssText = `
    position:relative;width:34px;height:34px;border-radius:9px;border:0.5px solid rgba(0,0,0,0.1);
    background:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;
    margin-right:8px;flex-shrink:0;transition:background .15s;
  `;
  btn.innerHTML = `
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="${DARK}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
    </svg>
    <span id="col-badge" style="
      position:absolute;top:-4px;right:-4px;min-width:16px;height:16px;border-radius:8px;
      background:${BRAND};color:#fff;font-size:9px;font-weight:700;
      display:none;align-items:center;justify-content:center;padding:0 3px;
      font-family:inherit;
    "></span>
  `;
  btn.onmouseenter = () => { btn.style.background = BG; };
  btn.onmouseleave = () => { btn.style.background = '#fff'; };
  btn.onclick = () => window._colTogglePanel();
  userDiv.insertBefore(btn, userDiv.firstChild);
}

// ── Main panel ───────────────────────────────────────────────────
function _colEnsurePanel() {
  if (document.getElementById('col-panel')) return;

  // Overlay
  const overlay = document.createElement('div');
  overlay.id = 'col-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.25);z-index:900;display:none;';
  overlay.onclick = () => window._colClosePanel();
  document.body.appendChild(overlay);

  // Panel
  const panel = document.createElement('div');
  panel.id = 'col-panel';
  panel.style.cssText = `
    position:fixed;top:0;right:0;width:min(96vw,480px);height:100vh;
    background:#fff;z-index:901;
    box-shadow:-8px 0 32px rgba(0,0,0,0.12);
    transform:translateX(100%);transition:transform .28s cubic-bezier(.4,0,.2,1);
    display:flex;flex-direction:column;font-family:inherit;
  `;
  document.body.appendChild(panel);
}

window._colTogglePanel = function() {
  if (_colState.panelOpen) { window._colClosePanel(); }
  else { window._colOpenPanel(); }
};

window._colOpenPanel = async function() {
  _colEnsurePanel();
  const panel   = document.getElementById('col-panel');
  const overlay = document.getElementById('col-overlay');
  _colState.panelOpen = true;
  _colState.view = 'inbox';
  overlay.style.display = 'block';
  requestAnimationFrame(() => { panel.style.transform = 'translateX(0)'; });
  _colRenderInbox();
  _colStartPolling();
};

window._colClosePanel = function() {
  const panel   = document.getElementById('col-panel');
  const overlay = document.getElementById('col-overlay');
  if (panel)   panel.style.transform = 'translateX(100%)';
  if (overlay) overlay.style.display = 'none';
  _colState.panelOpen = false;
  _colStopPolling();
};

// ── Inbox view ───────────────────────────────────────────────────
async function _colRenderInbox() {
  const panel = document.getElementById('col-panel');
  if (!panel) return;

  panel.innerHTML = `
    <div style="padding:18px 20px 14px;border-bottom:0.5px solid rgba(0,0,0,0.08);display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
      <div>
        <div style="font-size:16px;font-weight:700;color:${DARK};letter-spacing:-0.02em;">Threads</div>
        <div style="font-size:11px;color:${MID};margin-top:1px;">Collaboration across your team</div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;">
        <button onclick="window._colOpenNewThread()" style="background:${BRAND};color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:11px;font-weight:600;cursor:pointer;font-family:inherit;">+ New</button>
        <button onclick="window._colClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;">✕</button>
      </div>
    </div>
    <div style="padding:10px 16px;border-bottom:0.5px solid rgba(0,0,0,0.06);flex-shrink:0;">
      <div style="display:flex;gap:6px;">
        <button id="col-filter-open"   onclick="window._colSetFilter('open')"     style="font-size:10px;padding:4px 12px;border-radius:20px;border:0.5px solid ${BRAND};background:${BRAND};color:#fff;cursor:pointer;font-family:inherit;font-weight:600;">Open</button>
        <button id="col-filter-resolved" onclick="window._colSetFilter('resolved')" style="font-size:10px;padding:4px 12px;border-radius:20px;border:0.5px solid rgba(0,0,0,0.12);background:#fff;color:${MID};cursor:pointer;font-family:inherit;">Resolved</button>
        <button id="col-filter-all"    onclick="window._colSetFilter('')"          style="font-size:10px;padding:4px 12px;border-radius:20px;border:0.5px solid rgba(0,0,0,0.12);background:#fff;color:${MID};cursor:pointer;font-family:inherit;">All</button>
      </div>
    </div>
    <div id="col-thread-list" style="flex:1;overflow-y:auto;"></div>
  `;

  window._colFilter = window._colFilter || 'open';
  window._colSetFilter = function(f) {
    window._colFilter = f;
    ['open','resolved','all'].forEach(id => {
      const b = document.getElementById('col-filter-' + id);
      if (!b) return;
      const isActive = f === id || (f==='' && id==='all');
      b.style.background = isActive ? BRAND : '#fff';
      b.style.color      = isActive ? '#fff' : MID;
      b.style.borderColor = isActive ? BRAND : 'rgba(0,0,0,0.12)';
    });
    _colLoadThreadList();
  };

  _colLoadThreadList();
}

async function _colLoadThreadList() {
  const listEl = document.getElementById('col-thread-list');
  if (!listEl) return;
  listEl.innerHTML = `<div style="padding:20px;text-align:center;color:${LIGHT};font-size:12px;">Loading…</div>`;
  try {
    const status = window._colFilter || 'open';
    const url = status ? `/threads?status=${status}` : '/threads';
    const threads = await colApi(url);
    _colState.threads = threads;
    if (!threads.length) {
      listEl.innerHTML = `<div style="padding:30px 20px;text-align:center;color:${LIGHT};font-size:12px;">No threads yet.<br><br><button onclick="window._colOpenNewThread()" style="background:${BRAND};color:#fff;border:none;border-radius:8px;padding:8px 16px;font-size:11px;font-weight:600;cursor:pointer;font-family:inherit;">Start a thread</button></div>`;
      return;
    }
    listEl.innerHTML = threads.map(t => `
      <div onclick="window._colOpenThread('${t.id}')" style="padding:14px 18px;border-bottom:0.5px solid rgba(0,0,0,0.05);cursor:pointer;transition:background .12s;" onmouseenter="this.style.background='${BG}'" onmouseleave="this.style.background='#fff'">
        <div style="display:flex;align-items:flex-start;gap:10px;">
          <div style="font-size:18px;flex-shrink:0;margin-top:1px;">${ctxIcon(t.context_type)}</div>
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:3px;">
              <div style="font-size:12px;font-weight:600;color:${DARK};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${esc(t.title)}</div>
              <div style="font-size:10px;color:${LIGHT};flex-shrink:0;">${fmtTime(t.updated_at)}</div>
            </div>
            <div style="display:flex;align-items:center;gap:6px;">
              <span style="font-size:10px;color:${MID};">${esc(t.context_label||t.context_key)}</span>
              <span style="color:${LIGHT};font-size:10px;">·</span>
              <span style="font-size:10px;color:${LIGHT};">${t.message_count||0} msg${t.message_count!==1?'s':''}</span>
              ${t.status==='resolved'?`<span style="font-size:9px;padding:1px 7px;border-radius:20px;background:rgba(52,199,89,0.1);color:#166534;font-weight:600;">Resolved</span>`:''}
            </div>
            <div style="font-size:10px;color:${LIGHT};margin-top:2px;">by ${esc(t.created_by_name)} · ${roleBadge(t.created_by_role)}</div>
          </div>
        </div>
      </div>
    `).join('');
  } catch(e) {
    listEl.innerHTML = `<div style="padding:20px;color:#c00;font-size:12px;">Failed to load threads: ${esc(e.message)}</div>`;
  }
}

// ── Thread view ───────────────────────────────────────────────────
window._colOpenThread = async function(threadId) {
  const panel = document.getElementById('col-panel');
  if (!panel) return;
  _colState.view = 'thread';
  _colState.activeThread = threadId;
  _colState.pendingAttachments = [];

  panel.innerHTML = `
    <div style="padding:14px 18px;border-bottom:0.5px solid rgba(0,0,0,0.08);display:flex;align-items:center;gap:10px;flex-shrink:0;">
      <button onclick="window._colBackToInbox()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:14px;">←</button>
      <div style="flex:1;min-width:0;" id="col-thread-header-info"></div>
      <button id="col-delete-btn" onclick="window._colDeleteThread('${threadId}')" title="Delete thread" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${LIGHT};font-size:13px;cursor:pointer;display:none;align-items:center;justify-content:center;">🗑</button>
      <button onclick="window._colClosePanel()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;">✕</button>
    </div>
    <div id="col-messages" style="flex:1;overflow-y:auto;padding:14px 18px;display:flex;flex-direction:column;gap:12px;"></div>
    <div id="col-composer" style="flex-shrink:0;border-top:0.5px solid rgba(0,0,0,0.08);padding:12px 16px;"></div>
  `;

  _colLoadThread(threadId);
};

async function _colLoadThread(threadId) {
  try {
    const thread = await colApi('/threads/' + threadId);
    _colState.activeThread = threadId;

    // Show delete button for admins
    const deleteBtn = document.getElementById('col-delete-btn');
    if (deleteBtn) {
      const role = window.Clerk?.user?.organizationMemberships?.[0]?.role || '';
      const isAdmin = role === 'org:admin_auth';
      deleteBtn.style.display = isAdmin ? 'flex' : 'none';
    }

    // Header
    const hdr = document.getElementById('col-thread-header-info');
    if (hdr) {
      hdr.innerHTML = `
        <div style="font-size:13px;font-weight:600;color:${DARK};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${ctxIcon(thread.context_type)} ${esc(thread.title)}</div>
        <div style="display:flex;align-items:center;gap:6px;margin-top:2px;">
          <span style="font-size:10px;color:${MID};">${esc(thread.context_label||thread.context_key)}</span>
          ${thread.status==='resolved'
            ? `<span style="font-size:9px;padding:1px 7px;border-radius:20px;background:rgba(52,199,89,0.1);color:#166534;font-weight:600;">Resolved</span>
               <button onclick="window._colSetStatus('${threadId}','open')" style="font-size:9px;color:${MID};background:none;border:none;cursor:pointer;padding:0;font-family:inherit;">Reopen</button>`
            : `<button onclick="window._colSetStatus('${threadId}','resolved')" style="font-size:9px;color:#166534;background:rgba(52,199,89,0.1);border:none;border-radius:20px;padding:1px 8px;cursor:pointer;font-weight:600;font-family:inherit;">✓ Resolve</button>`
          }
        </div>
      `;
    }

    // Messages
    _colRenderMessages(thread.messages || []);

    // Composer
    _colRenderComposer(threadId, thread.status === 'resolved');
  } catch(e) {
    const msgEl = document.getElementById('col-messages');
    if (msgEl) msgEl.innerHTML = `<div style="color:#c00;font-size:12px;">Failed to load: ${esc(e.message)}</div>`;
  }
}

window._colDeleteThread = async function(threadId) {
  if (!confirm('Delete this thread and all its messages? This cannot be undone.')) return;
  try {
    await colApi('/threads/' + threadId, { method: 'DELETE' });
    _colRefreshBadge();
    window._colBackToInbox();
  } catch(e) {
    alert('Delete failed: ' + e.message);
  }
};

function _colRenderMessages(messages) {
  const el = document.getElementById('col-messages');
  if (!el) return;
  _colState.messages = messages;

  if (!messages.length) {
    el.innerHTML = `<div style="text-align:center;color:${LIGHT};font-size:12px;padding:20px 0;">No messages yet. Start the conversation below.</div>`;
    return;
  }

  el.innerHTML = messages.map(m => {
    const isPulse = m.is_pulse === 1;
    const attachments = Array.isArray(m.attachments) ? m.attachments : [];
    const attHtml = attachments.map(a => {
      if (isImage(a.type)) {
        return `<div style="margin-top:8px;"><a href="${esc(a.url)}" target="_blank"><img src="${esc(a.url)}" alt="${esc(a.name)}" style="max-width:100%;max-height:200px;border-radius:8px;border:0.5px solid rgba(0,0,0,0.08);"></a></div>`;
      }
      return `<a href="${esc(a.url)}" target="_blank" style="display:inline-flex;align-items:center;gap:6px;margin-top:6px;padding:6px 10px;border-radius:8px;background:rgba(0,0,0,0.04);border:0.5px solid rgba(0,0,0,0.08);text-decoration:none;color:${DARK};font-size:11px;">
        <span>${fileIcon(a.name, a.type)}</span>
        <span>${esc(a.name)}</span>
      </a>`;
    }).join('');

    if (isPulse) {
      return `
        <div style="background:${PULSE_BG};border-radius:12px;padding:12px 14px;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            <div style="width:24px;height:24px;border-radius:6px;background:${BRAND};display:flex;align-items:center;justify-content:center;flex-shrink:0;">
              <svg width="12" height="12" viewBox="0 0 46 46" fill="none"><path d="M32.73 14.9L26.3 5.77a3.93 3.93 0 0 0-6.6 0l-6.43 9.13L6.1 25.7a3.93 3.93 0 0 0 0 4.38l6.44 8.98 1.18 1.65a3.93 3.93 0 0 0 6.44 0l1.18-1.65 1.66-2.32 5.19-7.25.07-.1 4.47-6.25a3.93 3.93 0 0 0 0-4.24z" fill="#fff"/></svg>
            </div>
            <span style="font-size:11px;font-weight:700;color:#fff;">Pulse</span>
            <span style="font-size:10px;color:rgba(255,255,255,0.4);">${fmtTime(m.created_at)}</span>
          </div>
          <div style="font-size:12px;color:rgba(255,255,255,0.88);line-height:1.6;white-space:pre-wrap;">${esc(m.body)}</div>
          ${attHtml}
        </div>`;
    }

    const c = ROLE_COLOR[m.author_role] || ROLE_COLOR['Member'];
    return `
      <div style="display:flex;gap:10px;align-items:flex-start;">
        <div style="width:32px;height:32px;border-radius:10px;background:${c.bg};color:${c.color};font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;">${initials(m.author_name)}</div>
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
            <span style="font-size:12px;font-weight:600;color:${DARK};">${esc(m.author_name)}</span>
            ${roleBadge(m.author_role)}
            <span style="font-size:10px;color:${LIGHT};">${fmtTime(m.created_at)}</span>
          </div>
          <div style="font-size:12px;color:${DARK};line-height:1.6;white-space:pre-wrap;">${esc(m.body)}</div>
          ${attHtml}
        </div>
      </div>`;
  }).join('');

  // Scroll to bottom
  requestAnimationFrame(() => { el.scrollTop = el.scrollHeight; });
}

function _colRenderComposer(threadId, isResolved) {
  const el = document.getElementById('col-composer');
  if (!el) return;
  if (isResolved) {
    el.innerHTML = `<div style="text-align:center;font-size:11px;color:${LIGHT};padding:8px 0;">Thread resolved. <button onclick="window._colSetStatus('${threadId}','open')" style="color:${BRAND};background:none;border:none;cursor:pointer;font-family:inherit;font-size:11px;">Reopen to reply</button></div>`;
    return;
  }
  el.innerHTML = `
    <div id="col-attach-preview" style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;"></div>
    <div style="display:flex;gap:8px;align-items:flex-end;">
      <div style="flex:1;border:0.5px solid rgba(0,0,0,0.15);border-radius:10px;overflow:hidden;background:#fff;">
        <textarea id="col-msg-input" placeholder="Write a message… (@pulse to ask Pulse)" rows="2"
          style="width:100%;border:none;outline:none;resize:none;padding:10px 12px;font-size:12px;font-family:inherit;color:${DARK};background:transparent;box-sizing:border-box;line-height:1.5;"
          onkeydown="if(event.key==='Enter'&&(event.metaKey||event.ctrlKey)){window._colSendMsg('${threadId}');}"
        ></textarea>
        <div style="padding:4px 8px 6px;display:flex;align-items:center;gap:6px;">
          <label title="Attach file" style="cursor:pointer;color:${LIGHT};font-size:16px;line-height:1;padding:2px 4px;border-radius:5px;transition:color .15s;" onmouseenter="this.style.color='${DARK}'" onmouseleave="this.style.color='${LIGHT}'">
            📎<input type="file" accept="image/*,.xlsx,.xls,.csv,.pdf" multiple style="display:none;" onchange="window._colHandleFiles(event,'${threadId}')">
          </label>
          <span style="font-size:10px;color:${LIGHT};">⌘↵ to send</span>
        </div>
      </div>
      <button onclick="window._colSendMsg('${threadId}')" id="col-send-btn"
        style="background:${BRAND};color:#fff;border:none;border-radius:10px;width:36px;height:36px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-bottom:2px;">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
      </button>
    </div>
  `;
}

function _colUpdateAttachPreview() {
  const el = document.getElementById('col-attach-preview');
  if (!el) return;
  const atts = _colState.pendingAttachments;
  if (!atts.length) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.innerHTML = atts.map((a, i) => `
    <div style="display:inline-flex;align-items:center;gap:5px;padding:4px 8px;background:${BG};border-radius:6px;font-size:10px;color:${DARK};max-width:150px;">
      <span>${fileIcon(a.name, a.type)}</span>
      <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(a.name)}</span>
      <button onclick="window._colRemoveAttachment(${i})" style="background:none;border:none;cursor:pointer;color:${LIGHT};font-size:12px;padding:0;margin-left:2px;">✕</button>
    </div>
  `).join('');
}

window._colRemoveAttachment = function(idx) {
  _colState.pendingAttachments.splice(idx, 1);
  _colUpdateAttachPreview();
};

window._colHandleFiles = async function(event, threadId) {
  const files = Array.from(event.target.files || []);
  if (!files.length) return;
  const btn = document.getElementById('col-send-btn');
  if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
  for (const file of files) {
    try {
      const fd = new FormData();
      fd.append('file', file);
      const token = await colToken();
      const headers = {};
      if (token) headers['Authorization'] = 'Bearer ' + token;
      const r = await fetch(colApiBase() + '/threads/upload', { method:'POST', headers, body:fd });
      if (!r.ok) throw new Error('Upload failed: ' + r.status);
      const att = await r.json();
      _colState.pendingAttachments.push(att);
      _colUpdateAttachPreview();
    } catch(e) { alert('Upload failed: ' + e.message); }
  }
  if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
  event.target.value = '';
};

window._colSendMsg = async function(threadId) {
  const input = document.getElementById('col-msg-input');
  const body  = input?.value?.trim() || '';
  const attachments = [..._colState.pendingAttachments];
  if (!body && !attachments.length) return;

  const btn = document.getElementById('col-send-btn');
  if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
  if (input) input.value = '';
  _colState.pendingAttachments = [];
  _colUpdateAttachPreview();

  try {
    await colApi(`/threads/${threadId}/messages`, { method:'POST', body: { body, attachments } });
    await _colLoadThread(threadId);
  } catch(e) { alert('Failed to send: ' + e.message); }
  if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
};

window._colSetStatus = async function(threadId, status) {
  try {
    await colApi(`/threads/${threadId}`, { method:'PATCH', body: { status } });
    await _colLoadThread(threadId);
    _colRefreshBadge();
  } catch(e) { alert('Failed: ' + e.message); }
};

window._colBackToInbox = function() {
  _colState.view = 'inbox';
  _colState.activeThread = null;
  _colRenderInbox();
};

// ── New thread flow ───────────────────────────────────────────────
window._colOpenNewThread = function(opts = {}) {
  const panel = document.getElementById('col-panel');
  if (!panel) { window._colOpenPanel(); setTimeout(() => window._colOpenNewThread(opts), 300); return; }

  const contextTypes = ['general','po','week','lane','invoice','supplier','sku','receiving'];

  panel.innerHTML = `
    <div style="padding:16px 20px 14px;border-bottom:0.5px solid rgba(0,0,0,0.08);display:flex;align-items:center;gap:10px;flex-shrink:0;">
      <button onclick="window._colBackToInbox()" style="width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:14px;">←</button>
      <div style="font-size:15px;font-weight:700;color:${DARK};">New Thread</div>
      <button onclick="window._colClosePanel()" style="margin-left:auto;width:28px;height:28px;border-radius:8px;border:none;background:${BG};color:${MID};font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;">✕</button>
    </div>
    <div style="flex:1;overflow-y:auto;padding:18px 20px;">
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Context Type</div>
        <select id="col-new-ctx-type" style="width:100%;border:0.5px solid rgba(0,0,0,0.15);border-radius:8px;padding:8px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;background:#fff;">
          ${contextTypes.map(t => `<option value="${t}" ${opts.context_type===t?'selected':''}>${ctxIcon(t)} ${t.charAt(0).toUpperCase()+t.slice(1)}</option>`).join('')}
        </select>
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Reference (PO number, week, etc.)</div>
        <input id="col-new-ctx-key" type="text" placeholder="e.g. MAAE000252" value="${esc(opts.context_key||'')}"
          style="width:100%;border:0.5px solid rgba(0,0,0,0.15);border-radius:8px;padding:8px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;box-sizing:border-box;"/>
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Thread Title</div>
        <input id="col-new-title" type="text" placeholder="e.g. Carton discrepancy on arrival" value="${esc(opts.title||'')}"
          style="width:100%;border:0.5px solid rgba(0,0,0,0.15);border-radius:8px;padding:8px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;box-sizing:border-box;"/>
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:600;color:${MID};text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Opening Message <span style="font-weight:400;text-transform:none;">(optional)</span></div>
        <textarea id="col-new-body" rows="4" placeholder="Describe the issue, question or context…"
          style="width:100%;border:0.5px solid rgba(0,0,0,0.15);border-radius:8px;padding:8px 10px;font-size:12px;font-family:inherit;color:${DARK};outline:none;resize:vertical;box-sizing:border-box;line-height:1.5;">${esc(opts.initial_message||'')}</textarea>
      </div>
      <div style="background:rgba(153,0,51,0.04);border:0.5px solid rgba(153,0,51,0.15);border-radius:8px;padding:10px 12px;margin-bottom:18px;display:flex;align-items:center;gap:8px;">
        <div style="width:20px;height:20px;border-radius:5px;background:${BRAND};display:flex;align-items:center;justify-content:center;flex-shrink:0;">
          <svg width="10" height="10" viewBox="0 0 46 46" fill="none"><path d="M32.73 14.9L26.3 5.77a3.93 3.93 0 0 0-6.6 0l-6.43 9.13L6.1 25.7a3.93 3.93 0 0 0 0 4.38l6.44 8.98 1.18 1.65a3.93 3.93 0 0 0 6.44 0l1.18-1.65 1.66-2.32 5.19-7.25.07-.1 4.47-6.25a3.93 3.93 0 0 0 0-4.24z" fill="#fff"/></svg>
        </div>
        <div style="font-size:11px;color:${BRAND};">Pulse will automatically join this thread and provide context.</div>
      </div>
      <button onclick="window._colCreateThread()" style="width:100%;background:${BRAND};color:#fff;border:none;border-radius:10px;padding:11px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit;">Create Thread</button>
    </div>
  `;
};

window._colCreateThread = async function() {
  const ctx_type = document.getElementById('col-new-ctx-type')?.value;
  const ctx_key  = document.getElementById('col-new-ctx-key')?.value?.trim();
  const title    = document.getElementById('col-new-title')?.value?.trim();
  const body     = document.getElementById('col-new-body')?.value?.trim();
  if (!ctx_key)  return alert('Please enter a reference.');
  if (!title)    return alert('Please enter a title.');

  // Build context snapshot from current app state if available
  let snapshot = null;
  try {
    if (window.state) {
      snapshot = {
        weekStart: window.state.weekStart,
        planRows:  (window.state.plan||[]).length,
        facility:  window.state.facility,
      };
    }
  } catch(_) {}

  try {
    const thread = await colApi('/threads', {
      method: 'POST',
      body: { context_type: ctx_type, context_key: ctx_key, context_label: ctx_key, title, initial_message: body||null, context_snapshot: snapshot },
    });
    _colRefreshBadge();
    window._colOpenThread(thread.id);
  } catch(e) { alert('Failed to create thread: ' + e.message); }
};

// ── Contextual anchor (thread bubble on data elements) ───────────
window._colStartThreadFrom = function(contextType, contextKey, contextLabel, suggestedTitle) {
  _colEnsurePanel();
  const panel   = document.getElementById('col-panel');
  const overlay = document.getElementById('col-overlay');
  _colState.panelOpen = true;
  overlay.style.display = 'block';
  requestAnimationFrame(() => { panel.style.transform = 'translateX(0)'; });
  window._colOpenNewThread({
    context_type: contextType,
    context_key:  contextKey,
    context_label: contextLabel,
    title: suggestedTitle || `${contextLabel} — discussion`,
  });
};

// ── Polling ──────────────────────────────────────────────────────
function _colStartPolling() {
  _colStopPolling();
  _colState.pollTimer = setInterval(async () => {
    await _colRefreshBadge();
    if (_colState.view === 'thread' && _colState.activeThread) {
      try {
        const thread = await colApi('/threads/' + _colState.activeThread);
        _colRenderMessages(thread.messages || []);
      } catch(_) {}
    } else if (_colState.view === 'inbox') {
      _colLoadThreadList();
    }
  }, 15000); // poll every 15s
}

function _colStopPolling() {
  if (_colState.pollTimer) { clearInterval(_colState.pollTimer); _colState.pollTimer = null; }
}

// ── Context menu on PO rows ──────────────────────────────────────
function _colInjectContextTriggers() {
  // Add right-click / long-press context to PO number cells
  document.addEventListener('contextmenu', function(e) {
    const el = e.target.closest('[data-col-po],[data-col-week],[data-col-zendesk]');
    if (!el) return;
    e.preventDefault();
    const po      = el.dataset.colPo;
    const week    = el.dataset.colWeek;
    const zendesk = el.dataset.colZendesk;
    if (po)      window._colStartThreadFrom('po',   po,      po,      `PO ${po} — discussion`);
    else if (week) window._colStartThreadFrom('week', week,  week,    `Week ${week} — discussion`);
    else if (zendesk) window._colStartThreadFrom('lane', zendesk, zendesk, `Zendesk #${zendesk} — discussion`);
  });
}

// ── Init ─────────────────────────────────────────────────────────
function _colInit() {
  _colInjectNav();
  _colEnsurePanel();
  _colInjectContextTriggers();
  _colRefreshBadge();
  // Refresh badge every 60s passively
  setInterval(_colRefreshBadge, 60000);
}

// Wait for DOM + Clerk to be ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', _colInit);
} else {
  // Small delay to let Clerk and nav render
  setTimeout(_colInit, 500);
}

// Expose for use from other modules
window._colStartThreadFrom = window._colStartThreadFrom;
window._colOpenPanel       = window._colOpenPanel;

})();
