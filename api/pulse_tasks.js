// ── Pulse tasks and mailbox (Phase 3c) ──
// Two things Pulse needs to act on its own behalf:
//
//   TASKS    Anything handed to Pulse — "chase Kerry for the HBL by Thursday", "remind me to
//            confirm the dock on Monday" — becomes a row here: title, owner, due date, status,
//            and whatever came back (a reply, a result). The Pulse panel lists them; the tools
//            create, list and close them. Scoped to the client the person was working in.
//
//   MAILBOX  Pulse sends as pulseAI@velozity.au through Resend, signed on behalf of the person
//            who asked. The agreed protocol (19 Nov): the command is the approval. Pulse reads
//            back recipient and gist, HOLDS the message for 60 seconds — "Pulse, stop" recalls
//            it — then sends. Replies come back through Resend's inbound webhook, are matched to
//            the task by the [PP-123] tag in the subject (or by sender), and show up on the task.
//
// Recipients are limited to known domains (PULSE_EMAIL_DOMAINS, default the VelOzity, ICONIC,
// EHP and Kerry domains) plus addresses Pinpoint has already notified. A stranger's address is
// refused rather than guessed at.
//
// REPLIES (Phase 3d). Pulse can answer email itself, read-only:
//   · a reply on a thread Pulse started (the [PP-n] tag) — answered as a VelOzity colleague would
//     read it, with the client's data, cc operations@;
//   · a fresh question from a KNOWN client user (a Pinpoint login, or an address on that client's
//     notification list) — answered as that client, from that client's data only, cc operations@.
//   Unknown senders get nothing. Anything needing a change becomes a task for the team. Each
//   reply goes through the same 60-second hold as every other Pulse email. Three automated
//   replies per thread per day, then it stops and leaves it to a person.
//   PULSE_EMAIL_REPLIES=off|threads|on (default off): threads = only replies on Pulse's own
//   threads; on = also client questions, for the clients in PULSE_EMAIL_REPLY_CLIENTS (comma list).
//
// Env:  PULSE_EMAIL_FROM        default "Pulse (VelOzity) <pulseAI@velozity.au>"
//       PULSE_EMAIL_DOMAINS     comma list of domains Pulse may write to
//       PULSE_EMAIL_HOLD_SECONDS  default 60
//       RESEND_WEBHOOK_SECRET   whsec_… from the Resend webhook page (inbound is refused without it)
//       RESEND_API_KEY          already set for the other senders
//       PULSE_RESEND_BASE       override https://api.resend.com (tests)
'use strict';
const crypto = require('crypto');

module.exports = function pulseTasks(deps) {
  const { app, db, authenticateRequest, requireRole, requireInternalOrg, aiLimiter, curClient, sendViaResend, audit, log = console, now = () => Date.now(), internal, lookupSender, mondayOf } = deps;

  db.exec(`
    CREATE TABLE IF NOT EXISTS pulse_task (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id   TEXT NOT NULL,
      title       TEXT NOT NULL,
      detail      TEXT,
      owner       TEXT NOT NULL DEFAULT 'pulse',     -- pulse | a person's name
      status      TEXT NOT NULL DEFAULT 'open',      -- open | waiting | replied | done | cancelled
      due_at      TEXT,                              -- YYYY-MM-DD or YYYY-MM-DDTHH:MM
      about       TEXT,                              -- movement reference / PO / week it concerns
      source      TEXT NOT NULL DEFAULT 'chat',      -- chat | voice | meeting | email
      created_by  TEXT,
      created_by_name TEXT,
      created_at  TEXT NOT NULL,
      updated_at  TEXT,
      done_at     TEXT,
      outcome     TEXT                               -- what happened, in one line
    );
    CREATE INDEX IF NOT EXISTS idx_pulse_task_client ON pulse_task(client_id, status);
    CREATE TABLE IF NOT EXISTS pulse_email (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id   TEXT NOT NULL,
      task_id     INTEGER,
      to_json     TEXT NOT NULL,
      cc_json     TEXT,
      subject     TEXT NOT NULL,
      body        TEXT NOT NULL,
      status      TEXT NOT NULL DEFAULT 'held',      -- held | sent | recalled | failed
      hold_until  TEXT,
      sent_at     TEXT,
      resend_id   TEXT,
      error       TEXT,
      created_by  TEXT,
      created_by_name TEXT,
      created_at  TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pulse_inbound (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      resend_id   TEXT UNIQUE,
      from_addr   TEXT,
      to_json     TEXT,
      subject     TEXT,
      text        TEXT,
      received_at TEXT NOT NULL,
      task_id     INTEGER,
      kind        TEXT                               -- reply | mobiledock | unmatched
    );
  `);

  const iso = () => new Date(now()).toISOString();
  let _meetings = null; const setMeetings = (m) => { _meetings = m; };
  const FROM = () => process.env.PULSE_EMAIL_FROM || 'Pulse (VelOzity) <pulseAI@velozity.au>';
  const HOLD_MS = () => Math.max(5, Number(process.env.PULSE_EMAIL_HOLD_SECONDS) || 60) * 1000;
  const DOMAINS = () => String(process.env.PULSE_EMAIL_DOMAINS || 'velozity.au,velozity.com.au,theiconic.com.au,ehplabs.com,kerrylogistics.com,kln.com').split(/[\s,]+/).map(s => s.trim().toLowerCase()).filter(Boolean);
  const addrOf = (s) => { const m = String(s || '').match(/<([^>]+)>/); return (m ? m[1] : String(s || '')).trim().toLowerCase(); };
  const validAddr = (a) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(a);
  const knownRecipient = (client, a) => {
    const dom = a.split('@')[1] || '';
    if (DOMAINS().includes(dom)) return true;
    try { if (db.prepare('SELECT 1 FROM notify_recipient WHERE client_id = ? AND lower(email) = ?').get(client, a)) return true; } catch (_) {}
    return false;
  };
  const who = (req) => ({ userId: req.auth?.userId || 'anon', orgId: req.auth?.orgId || '', orgRole: req.auth?.orgRole || '' });
  const note = (req, action, body, status, error) => { try { const w = who(req); audit && audit.run(w.userId, w.orgId, w.orgRole, action, req.path, req.method, req.ip, req.headers['user-agent'], body, status, error); } catch (_) {} };
  const shapeTask = (t) => {
    const replies = db.prepare('SELECT from_addr, subject, substr(text,1,600) text, received_at FROM pulse_inbound WHERE task_id = ? ORDER BY id DESC LIMIT 3').all(t.id);
    const emails = db.prepare('SELECT id, to_json, subject, status, hold_until, sent_at FROM pulse_email WHERE task_id = ? ORDER BY id DESC LIMIT 3').all(t.id)
      .map(e => ({ id: e.id, to: JSON.parse(e.to_json || '[]'), subject: e.subject, status: e.status, hold_until: e.hold_until, sent_at: e.sent_at }));
    return { ...t, replies, emails };
  };
  const gate = [authenticateRequest, requireRole(['admin']), requireInternalOrg];

  // ── Tasks ──
  app.get('/pulse/tasks', ...gate, (req, res) => {
    try {
      const status = String(req.query.status || 'open');
      const where = status === 'all' ? '' : (status === 'open' ? `AND status IN ('open','waiting','replied')` : 'AND status = ?');
      const rows = db.prepare(`SELECT * FROM pulse_task WHERE client_id = ? ${where} ORDER BY CASE status WHEN 'replied' THEN 0 WHEN 'open' THEN 1 WHEN 'waiting' THEN 2 ELSE 3 END, COALESCE(due_at,'9999'), id DESC LIMIT 100`)
        .all(...(where.includes('?') ? [curClient(), status] : [curClient()]));
      res.json({ ok: true, tasks: rows.map(shapeTask), open: rows.filter(t => ['open', 'waiting', 'replied'].includes(t.status)).length });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  app.post('/pulse/tasks', ...gate, (req, res) => {
    try {
      const b = req.body || {};
      const title = String(b.title || '').trim(); if (!title) return res.status(400).json({ ok: false, error: 'title required' });
      const due = b.due_at && /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?$/.test(String(b.due_at)) ? String(b.due_at) : null;
      const r = db.prepare(`INSERT INTO pulse_task (client_id, title, detail, owner, status, due_at, about, source, created_by, created_by_name, created_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(curClient(), title, b.detail ? String(b.detail).slice(0, 2000) : null, String(b.owner || 'pulse').slice(0, 80), b.status === 'waiting' ? 'waiting' : 'open', due, b.about ? String(b.about).slice(0, 120) : null,
             String(b.source || 'chat'), req.auth?.userId || null, b.created_by_name ? String(b.created_by_name).slice(0, 120) : null, iso());
      const t = db.prepare('SELECT * FROM pulse_task WHERE id = ?').get(r.lastInsertRowid);
      note(req, 'pulse_task_create', { id: t.id, title }, 200, null);
      res.json({ ok: true, task: shapeTask(t) });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  app.patch('/pulse/tasks/:id', ...gate, (req, res) => {
    try {
      const t = db.prepare('SELECT * FROM pulse_task WHERE id = ? AND client_id = ?').get(Number(req.params.id), curClient());
      if (!t) return res.status(404).json({ ok: false, error: 'no such task' });
      const b = req.body || {}; const sets = [], vals = [];
      if (b.status && ['open', 'waiting', 'replied', 'done', 'cancelled'].includes(b.status)) { sets.push('status = ?'); vals.push(b.status); if (b.status === 'done' || b.status === 'cancelled') { sets.push('done_at = ?'); vals.push(iso()); } }
      if (b.outcome !== undefined) { sets.push('outcome = ?'); vals.push(String(b.outcome || '').slice(0, 1000)); }
      if (b.due_at !== undefined) { sets.push('due_at = ?'); vals.push(b.due_at && /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?$/.test(String(b.due_at)) ? String(b.due_at) : null); }
      if (b.owner) { sets.push('owner = ?'); vals.push(String(b.owner).slice(0, 80)); }
      if (b.title) { sets.push('title = ?'); vals.push(String(b.title).slice(0, 300)); }
      if (!sets.length) return res.status(400).json({ ok: false, error: 'nothing to change' });
      sets.push('updated_at = ?'); vals.push(iso());
      db.prepare(`UPDATE pulse_task SET ${sets.join(', ')} WHERE id = ?`).run(...vals, t.id);
      note(req, 'pulse_task_update', { id: t.id, changes: b }, 200, null);
      res.json({ ok: true, task: shapeTask(db.prepare('SELECT * FROM pulse_task WHERE id = ?').get(t.id)), previous: { status: t.status, outcome: t.outcome, due_at: t.due_at, owner: t.owner, title: t.title } });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  // ── Outbound email: hold, then send ──
  app.post('/pulse/email', ...gate, aiLimiter, (req, res) => {
    try {
      const b = req.body || {}; const client = curClient();
      const to = [].concat(b.to || []).map(addrOf).filter(Boolean), cc = [].concat(b.cc || []).map(addrOf).filter(Boolean);
      if (!to.length) return res.status(400).json({ ok: false, error: 'at least one recipient' });
      const bad = to.concat(cc).filter(a => !validAddr(a)); if (bad.length) return res.status(400).json({ ok: false, error: `not an email address: ${bad.join(', ')}` });
      const unknown = to.concat(cc).filter(a => !knownRecipient(client, a));
      if (unknown.length) return res.status(422).json({ ok: false, error: `Pulse does not know ${unknown.join(', ')} — not a VelOzity, client or partner address Pinpoint has used before`, unknown });
      const subject = String(b.subject || '').trim(), body = String(b.body || '').trim();
      if (!subject || !body) return res.status(400).json({ ok: false, error: 'subject and body required' });
      let taskId = Number(b.task_id) || null;
      if (taskId && !db.prepare('SELECT 1 FROM pulse_task WHERE id = ? AND client_id = ?').get(taskId, client)) taskId = null;
      if (!taskId) {
        const r = db.prepare(`INSERT INTO pulse_task (client_id, title, detail, owner, status, due_at, about, source, created_by, created_by_name, created_at)
                              VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
          .run(client, `Email ${to.join(', ')}: ${subject}`.slice(0, 300), body.slice(0, 2000), 'pulse', 'waiting', null, b.about ? String(b.about).slice(0, 120) : null, String(b.source || 'chat'), req.auth?.userId || null, b.created_by_name || null, iso());
        taskId = Number(r.lastInsertRowid);
      } else {
        db.prepare(`UPDATE pulse_task SET status = CASE WHEN status IN ('done','cancelled') THEN status ELSE 'waiting' END, updated_at = ? WHERE id = ?`).run(iso(), taskId);
      }
      const holdUntil = new Date(now() + HOLD_MS()).toISOString();
      const r = db.prepare(`INSERT INTO pulse_email (client_id, task_id, to_json, cc_json, subject, body, status, hold_until, created_by, created_by_name, created_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(client, taskId, JSON.stringify(to), JSON.stringify(cc), subject, body, 'held', holdUntil, req.auth?.userId || null, b.created_by_name || null, iso());
      note(req, 'pulse_email_hold', { id: r.lastInsertRowid, to, subject, task_id: taskId }, 200, null);
      res.json({ ok: true, email_id: Number(r.lastInsertRowid), task_id: taskId, hold_until: holdUntil, hold_seconds: HOLD_MS() / 1000, to, cc, subject });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });
  // Programmatic hold-then-send, used by the meeting bot for its summaries. Same table, same
  // tick, same 60-second recall from the tasks strip.
  function holdEmail(o) {
    const to = [].concat(o.to || []).map(addrOf).filter(validAddr), cc = [].concat(o.cc || []).map(addrOf).filter(validAddr).filter(a => !to.includes(a));
    if (!to.length) throw new Error('no valid recipient');
    const client = o.client || 'ICONIC';
    let taskId = Number(o.task_id) || null;
    if (!taskId) {
      const r = db.prepare(`INSERT INTO pulse_task (client_id, title, detail, owner, status, due_at, about, source, created_by, created_by_name, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(client, String(o.title || `Email ${to.join(', ')}: ${o.subject}`).slice(0, 300), String(o.body || '').slice(0, 2000), 'pulse', o.task_status || 'done', null, o.about || null, o.source || 'meeting', o.created_by || 'pulse', o.created_by_name || null, iso());
      taskId = Number(r.lastInsertRowid);
      if ((o.task_status || 'done') === 'done') db.prepare(`UPDATE pulse_task SET done_at = ?, outcome = ? WHERE id = ?`).run(iso(), o.outcome || 'Sent by Pulse', taskId);
    }
    const holdUntil = new Date(now() + HOLD_MS()).toISOString();
    const r = db.prepare(`INSERT INTO pulse_email (client_id, task_id, to_json, cc_json, subject, body, status, hold_until, created_by, created_by_name, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
      .run(client, taskId, JSON.stringify(to), JSON.stringify(cc), String(o.subject || '').slice(0, 300), String(o.body || ''), 'held', holdUntil, o.created_by || 'pulse', o.created_by_name || null, iso());
    return { email_id: Number(r.lastInsertRowid), task_id: taskId, hold_until: holdUntil };
  }

  app.post('/pulse/email/:id/recall', ...gate, (req, res) => {
    try {
      const e = db.prepare('SELECT * FROM pulse_email WHERE id = ? AND client_id = ?').get(Number(req.params.id), curClient());
      if (!e) return res.status(404).json({ ok: false, error: 'no such email' });
      if (e.status !== 'held') return res.status(409).json({ ok: false, error: e.status === 'sent' ? 'Already sent — it cannot be recalled.' : `Email is ${e.status}.` });
      db.prepare(`UPDATE pulse_email SET status = 'recalled' WHERE id = ?`).run(e.id);
      if (e.task_id) db.prepare(`UPDATE pulse_task SET status = 'cancelled', done_at = ?, outcome = 'Email recalled before sending', updated_at = ? WHERE id = ? AND status = 'waiting'`).run(iso(), iso(), e.task_id);
      note(req, 'pulse_email_recall', { id: e.id, subject: e.subject }, 200, null);
      res.json({ ok: true, recalled: e.id });
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
  });

  const signature = (name) => `\n\n— Pulse, VelOzity Pinpoint${name ? `, on behalf of ${name}` : ''}\nReply to this email and I will note your answer against the request.`;
  const esc = (s) => String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  async function sendHeld(e) {
    const to = JSON.parse(e.to_json || '[]'), cc = JSON.parse(e.cc_json || '[]');
    const text = e.body + signature(e.created_by_name);
    const html = `<div style="font:14px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1C1C1E">${esc(e.body).split(/\n{2,}/).map(p => `<p>${p.replace(/\n/g, '<br>')}</p>`).join('')}<p style="color:#6E6E73;font-size:12px;margin-top:20px">— Pulse, VelOzity Pinpoint${e.created_by_name ? `, on behalf of ${esc(e.created_by_name)}` : ''}<br>Reply to this email and I will note your answer against the request.</p></div>`;
    const fromAddr = addrOf(FROM());
    const r = await sendViaResend({ from: FROM(), replyTo: fromAddr, to, cc, subject: `${e.subject} [PP-${e.task_id}]`, html, text });
    db.prepare(`UPDATE pulse_email SET status = 'sent', sent_at = ?, resend_id = ? WHERE id = ?`).run(iso(), r && r.id || null, e.id);
    try { audit && audit.run(e.created_by || 'pulse', null, null, 'pulse_email_sent', '/pulse/email', 'SEND', '', 'pulse', JSON.stringify({ id: e.id, to, subject: e.subject, task_id: e.task_id }), 200, null); } catch (_) {}
    log.log(`[pulse mail] sent #${e.id} to ${to.join(', ')} (${e.subject})`);
  }
  async function tick() {
    let due = [];
    try { due = db.prepare(`SELECT * FROM pulse_email WHERE status = 'held' AND hold_until <= ?`).all(new Date(now()).toISOString()); } catch (_) { return; }
    for (const e of due) {
      try { await sendHeld(e); }
      catch (err) {
        log.warn('[pulse mail] send failed #' + e.id + ': ' + (err && err.message));
        db.prepare(`UPDATE pulse_email SET status = 'failed', error = ? WHERE id = ?`).run(String(err && err.message || err).slice(0, 500), e.id);
        if (e.task_id) db.prepare(`UPDATE pulse_task SET status = 'open', outcome = ?, updated_at = ? WHERE id = ?`).run('Email failed to send: ' + String(err && err.message || err).slice(0, 200), iso(), e.task_id);
        try { audit && audit.run(e.created_by || 'pulse', null, null, 'pulse_email_failed', '/pulse/email', 'SEND', '', 'pulse', JSON.stringify({ id: e.id }), 500, String(err && err.message || err)); } catch (_) {}
      }
    }
  }
  const timer = setInterval(() => { tick().catch(() => {}); }, 5000); timer.unref();

  // ── Inbound: Resend webhook ──
  // Verified the Svix way: HMAC-SHA256 over "<id>.<timestamp>.<raw body>" with the base64
  // part of the whsec_ secret; the timestamp must be within five minutes.
  function verifySvix(req) {
    const secret = process.env.RESEND_WEBHOOK_SECRET || '';
    if (!secret) return { ok: false, error: 'RESEND_WEBHOOK_SECRET not set' };
    const id = req.headers['svix-id'], ts = req.headers['svix-timestamp'], sig = req.headers['svix-signature'];
    if (!id || !ts || !sig) return { ok: false, error: 'missing svix headers' };
    if (Math.abs(now() / 1000 - Number(ts)) > 300) return { ok: false, error: 'timestamp outside tolerance' };
    const raw = req.rawBody ? req.rawBody.toString('utf8') : JSON.stringify(req.body || {});
    const key = Buffer.from(String(secret).replace(/^whsec_/, ''), 'base64');
    const expected = crypto.createHmac('sha256', key).update(`${id}.${ts}.${raw}`).digest('base64');
    const ok = String(sig).split(/\s+/).some(part => { const v = part.split(',')[1] || ''; try { return v.length === expected.length && crypto.timingSafeEqual(Buffer.from(v), Buffer.from(expected)); } catch (_) { return false; } });
    return ok ? { ok: true } : { ok: false, error: 'bad signature' };
  }
  async function fetchReceived(id) {
    const base = (process.env.PULSE_RESEND_BASE || 'https://api.resend.com').replace(/\/+$/, '');
    const r = await fetch(`${base}/emails/receiving/${encodeURIComponent(id)}`, { headers: { Authorization: 'Bearer ' + (process.env.RESEND_API_KEY || '') } });
    if (!r.ok) throw new Error('resend ' + r.status);
    return r.json();
  }
  const stripQuoted = (t) => String(t || '').split(/\r?\n/).filter(l => !/^>/.test(l)).join('\n').split(/\r?\n(?:On .{5,120} wrote:|-----Original Message-----|From: )/)[0].trim();
  async function recordInbound(ev) {
    const d = (ev && ev.data) || {};
    if (!d.email_id) return { ok: false, error: 'no email_id' };
    if (db.prepare('SELECT 1 FROM pulse_inbound WHERE resend_id = ?').get(d.email_id)) return { ok: true, duplicate: true };
    let full = {}; try { full = await fetchReceived(d.email_id); } catch (e) { log.warn('[pulse mail] inbound fetch failed: ' + e.message); }
    const subject = String(full.subject || d.subject || ''), from = addrOf(full.from || d.from || ''), to = full.to || d.to || [];
    const text = stripQuoted(full.text || (full.html ? String(full.html).replace(/<style[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ') : ''));
    let taskId = null, kind = 'unmatched';
    const m = subject.match(/\[PP-(\d+)\]/);
    if (m) { const t = db.prepare('SELECT id FROM pulse_task WHERE id = ?').get(Number(m[1])); if (t) { taskId = t.id; kind = 'reply'; } }
    if (!taskId && from) {
      // No tag: the most recent email Pulse sent to this address in the last 30 days.
      const e = db.prepare(`SELECT task_id FROM pulse_email WHERE status = 'sent' AND lower(to_json) LIKE ? AND sent_at >= ? ORDER BY id DESC LIMIT 1`).get(`%"${from}"%`, new Date(now() - 30 * 86400000).toISOString());
      if (e && e.task_id) { taskId = e.task_id; kind = 'reply'; }
    }
    if (!taskId && /mobiledock/i.test(from + ' ' + subject)) kind = 'mobiledock';          // parsed by a later phase
    db.prepare(`INSERT INTO pulse_inbound (resend_id, from_addr, to_json, subject, text, received_at, task_id, kind) VALUES (?,?,?,?,?,?,?,?)`)
      .run(d.email_id, from, JSON.stringify(to), subject, text.slice(0, 20000), iso(), taskId, kind);
    if (taskId) db.prepare(`UPDATE pulse_task SET status = CASE WHEN status IN ('done','cancelled') THEN status ELSE 'replied' END, updated_at = ? WHERE id = ?`).run(iso(), taskId);
    try { audit && audit.run('pulse', null, null, 'pulse_email_received', '/pulse/inbound', 'POST', '', 'resend', JSON.stringify({ from, subject, task_id: taskId, kind }), 200, null); } catch (_) {}
    log.log(`[pulse mail] inbound from ${from} "${subject}" → ${kind}${taskId ? ' task #' + taskId : ''}`);
    // A meeting invitation forwarded to the mailbox schedules Pulse to join (Phase 4). Checked
    // on EVERY inbound email that carries a meeting link — including one from a person Pulse
    // has written to before, which would otherwise be filed as a reply to that thread.
    let meeting = null;
    const rawAll = String(full.text || text || '') + ' ' + String(full.html || '');
    if (_meetings && _meetings.MEETING_LINK && _meetings.MEETING_LINK.test(rawAll) && !/^pulseai@/i.test(from)) {
      try { meeting = await _meetings.fromInvite({ from, subject, text: full.text || text, html: full.html || '' }); } catch (e) { log.warn('[pulse mail] invite parse error: ' + (e && e.message)); }
    }
    if (meeting && !meeting.duplicate && meeting.id) {
      db.prepare(`UPDATE pulse_inbound SET kind = 'invite', task_id = NULL WHERE resend_id = ?`).run(d.email_id);
      if (taskId) db.prepare(`UPDATE pulse_task SET status = CASE WHEN status = 'replied' THEN 'waiting' ELSE status END WHERE id = ?`).run(taskId);
      return { ok: true, kind: 'invite', meeting_id: meeting.id };
    }
    if (meeting && meeting.duplicate) { db.prepare(`UPDATE pulse_inbound SET kind = 'invite' WHERE resend_id = ?`).run(d.email_id); return { ok: true, kind: 'invite', duplicate: true }; }
    let reply = null;
    try { reply = await autoReply({ resend_id: d.email_id, from, subject, text, taskId, kind }); } catch (e) { log.warn('[pulse mail] auto-reply error: ' + (e && e.message)); }
    return { ok: true, task_id: taskId, kind, reply };
  }
  // ── Auto-reply (Phase 3d) ──
  const REPLY_MODE = () => String(process.env.PULSE_EMAIL_REPLIES || 'off').toLowerCase();
  const REPLY_CLIENTS = () => String(process.env.PULSE_EMAIL_REPLY_CLIENTS || '').split(/[\s,]+/).map(x => x.trim().toUpperCase()).filter(Boolean);
  const CC = () => String(process.env.PULSE_EMAIL_CC || 'operations@velozity.au').split(/[\s,]+/).map(addrOf).filter(validAddr);
  const MAX_PER_THREAD_PER_DAY = 3;
  const isOps = (a) => DOMAINS().slice(0, 2).includes((a.split('@')[1] || '').toLowerCase()) && !/^pulseai@/i.test(a);   // velozity.au / velozity.com.au

  // Who is this sender to Pinpoint? → { client, persona: 'client'|'internal' } or null.
  async function senderIdentity(from, taskRow) {
    if (!from || /^(no-?reply|noreply|mailer-daemon|postmaster|notifications?)@/i.test(from) || /^pulseai@/i.test(from)) return null;
    // A client user is a client user wherever they write from — including on a thread Pulse
    // started with them — so they only ever get the client's view.
    const n = db.prepare('SELECT client_id FROM notify_recipient WHERE lower(email) = ? ORDER BY last_used_at DESC LIMIT 1').get(from);
    if (n) return { client: n.client_id, persona: 'client' };
    if (taskRow) {
      // On Pulse's own thread: the people Pulse wrote to may answer; the client is the task's.
      const wrote = db.prepare(`SELECT 1 FROM pulse_email WHERE task_id = ? AND status = 'sent' AND (lower(to_json) LIKE ? OR lower(cc_json) LIKE ?)`).get(taskRow.id, `%"${from}"%`, `%"${from}"%`);
      if (wrote) return { client: taskRow.client_id, persona: isOps(from) ? 'internal' : 'thread' };
    }
    if (typeof lookupSender === 'function') { try { const r = await lookupSender(from); if (r && r.client) return { client: r.client, persona: r.persona || 'client' }; } catch (e) { log.warn('[pulse mail] sender lookup failed: ' + e.message); } }
    return null;
  }
  async function autoReply(inb) {
    const mode = REPLY_MODE();
    if (mode === 'off' || !internal) return { skipped: 'off' };
    const taskRow = inb.taskId ? db.prepare('SELECT * FROM pulse_task WHERE id = ?').get(inb.taskId) : null;
    const who = await senderIdentity(inb.from, taskRow);
    if (!who) { log.log(`[pulse mail] no reply: ${inb.from} is not known`); return { skipped: 'unknown_sender' }; }
    const onThread = !!taskRow && who.persona !== 'client';
    if (!onThread) {
      if (mode !== 'on') return { skipped: 'client_replies_off' };
      if (!REPLY_CLIENTS().includes(String(who.client).toUpperCase())) return { skipped: 'client_not_enabled' };
    }
    const text = String(inb.text || '').trim();
    if (text.length < 3) return { skipped: 'empty' };
    // Thread budget: automated replies today on this task.
    let taskId = inb.taskId;
    if (taskId) {
      const n = db.prepare(`SELECT COUNT(*) n FROM pulse_email WHERE task_id = ? AND created_by = 'pulse' AND created_at >= ?`).get(taskId, new Date(now() - 86400000).toISOString()).n;
      if (n >= MAX_PER_THREAD_PER_DAY) { log.log(`[pulse mail] thread #${taskId} reached ${MAX_PER_THREAD_PER_DAY} automated replies; leaving it to a person`); return { skipped: 'thread_budget' }; }
    }
    // Ask Pulse, as the right persona, with the client pinned.
    const persona = who.persona === 'client' ? { as: 'client', client: who.client } : { as: 'internal', client: who.client };
    const ws = typeof mondayOf === 'function' ? mondayOf(new Date(now()).toISOString().slice(0, 10)) : undefined;
    const prompt = `Email from ${inb.from}${inb.subject ? ` with subject "${inb.subject.replace(/\s*\[PP-\d+\]\s*/g, ' ').trim()}"` : ''}:\n\n${text.slice(0, 4000)}`;
    const out = await internal.json('/pulse/chat', { method: 'POST', headers: { 'content-type': 'application/json', 'x-pulse-enabled': '1', 'x-pinpoint-client': who.client },
      body: JSON.stringify({ messages: [{ role: 'user', content: prompt }], currentWeek: ws ? { week_start: ws } : undefined, persona, style: 'text' }) });
    let answer = String(out.reply || '').trim();
    if (!answer || /\[\[NOREPLY\]\]/.test(answer)) { log.log('[pulse mail] nothing to answer'); return { skipped: 'noreply' }; }
    const handover = /\[\[HANDOVER\]\]/.test(answer);
    answer = answer.replace(/\[\[HANDOVER\]\]/g, '').replace(/\n{3,}/g, '\n\n').trim();
    const greet = `Hi,\n\n`;
    // Task: an existing thread keeps its task; a client question gets its own, owned by the team when handed over.
    if (!taskId) {
      const r = db.prepare(`INSERT INTO pulse_task (client_id, title, detail, owner, status, due_at, about, source, created_by, created_by_name, created_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(who.client, `${handover ? 'Follow up' : 'Answered'}: ${inb.subject || 'email from ' + inb.from}`.slice(0, 300), text.slice(0, 2000), handover ? 'VelOzity team' : 'pulse', handover ? 'open' : 'done', null, null, 'email', 'pulse', null, iso());
      taskId = Number(r.lastInsertRowid);
      if (!handover) db.prepare(`UPDATE pulse_task SET done_at = ?, outcome = 'Pulse answered by email' WHERE id = ?`).run(iso(), taskId);
      db.prepare('UPDATE pulse_inbound SET task_id = ?, kind = ? WHERE resend_id = ?').run(taskId, 'question', inb.resend_id);
    } else if (handover) {
      db.prepare(`UPDATE pulse_task SET owner = CASE WHEN owner = 'pulse' THEN 'VelOzity team' ELSE owner END, status = 'open', outcome = ?, updated_at = ? WHERE id = ?`).run('Needs a person: ' + text.slice(0, 160), iso(), taskId);
    }
    const subject = 'Re: ' + String(inb.subject || 'your email').replace(/^\s*(re|fwd?)\s*:\s*/i, '').replace(/\s*\[PP-\d+\]\s*/g, ' ').trim();
    const holdUntil = new Date(now() + HOLD_MS()).toISOString();
    const e = db.prepare(`INSERT INTO pulse_email (client_id, task_id, to_json, cc_json, subject, body, status, hold_until, created_by, created_by_name, created_at)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
      .run(who.client, taskId, JSON.stringify([inb.from]), JSON.stringify(CC().filter(c => c !== inb.from)), subject, greet + answer, 'held', holdUntil, 'pulse', null, iso());
    try { audit && audit.run('pulse', null, null, 'pulse_email_autoreply', '/pulse/inbound', 'POST', '', 'pulse', JSON.stringify({ id: e.lastInsertRowid, to: inb.from, persona: persona.as, client: who.client, handover, tools: (out.tools || []).map(t => t.tool) }), 200, null); } catch (_) {}
    log.log(`[pulse mail] auto-reply #${e.lastInsertRowid} to ${inb.from} as ${persona.as}/${who.client}${handover ? ' (handed over)' : ''}, sends at ${holdUntil}`);
    return { email_id: Number(e.lastInsertRowid), task_id: taskId, handover, persona: persona.as };
  }

  // What the mailbox has received lately — for checking why something did or did not happen.
  app.get('/pulse/inbound/recent', ...gate, (req, res) => {
    const rows = db.prepare('SELECT id, resend_id, from_addr, subject, kind, task_id, received_at, length(text) AS text_len, substr(text,1,160) AS preview FROM pulse_inbound ORDER BY id DESC LIMIT 25').all();
    res.json({ ok: true, inbound: rows, replies: REPLY_MODE(), reply_clients: REPLY_CLIENTS() });
  });
  app.post('/pulse/inbound', async (req, res) => {
    const v = verifySvix(req);
    if (!v.ok) { log.warn('[pulse mail] inbound refused: ' + v.error); return res.status(v.error.includes('not set') ? 503 : 401).json({ ok: false, error: v.error }); }
    const ev = req.body || {};
    if (ev.type !== 'email.received') return res.json({ ok: true, ignored: ev.type });
    res.json({ ok: true });                                        // answer fast; Resend retries on timeouts
    recordInbound(ev).catch(e => log.warn('[pulse mail] inbound error: ' + e.message));
  });

  return { tick, sendHeld, recordInbound, verifySvix, knownRecipient, DOMAINS, FROM, autoReply, senderIdentity, holdEmail, CC, setMeetings };
};
