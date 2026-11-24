// ── Pulse in meetings (Phase 4) ──
// Pulse joins a Teams or Google Meet call as a participant named "Pulse (VelOzity)", through a
// Recall.ai bot. The bot gives us a live, per-speaker transcript and a way to play audio into
// the call; everything else is the same Pulse: the tool loop, the voice, the mailbox.
//
//   JOINING    Paste a link in the panel ("Join a meeting"), or invite pulseAI@velozity.au —
//              the invite lands in the Pulse mailbox, the link and time are read from it, and
//              the bot is scheduled. Each meeting is pinned to one client.
//   LISTENING  Recall streams finalised utterances with the speaker's name. Pulse answers only
//              when addressed ("Pulse, …"); everything else is context — the last twenty
//              minutes are available so "the one Yash mentioned" resolves.
//   ANSWERING  The question goes through /pulse/chat in spoken style, the answer is voiced with
//              the ElevenLabs voice and played into the call. Nothing is written live: any
//              action asked for is queued as a proposal, read back ("noted for after the
//              meeting"), and confirmed one tap at a time in Pinpoint afterwards (C6).
//   AFTER      Decisions, actions, open questions → a summary emailed from the Pulse mailbox
//              (60-second hold, as every Pulse email) to the organiser; action items for people
//              become tasks. The transcript is deleted once every proposal is settled, or after
//              7 days at most; the summary stays.
//
// Env:  RECALL_API_KEY, RECALL_REGION (https://ap-northeast-1.recall.ai), RECALL_WEBHOOK_SECRET
//       PULSE_PUBLIC_BASE   default https://vasuida1.onrender.com (where Recall calls us back)
//       PULSE_MEETING_DEFAULT_CLIENT  default ICONIC (for invites with no client signal)
//       RECALL_STT                    recall (default) | deepgram | assembly — which engine Recall streams with.
//                                     deepgram/assembly need that vendor's key added in the Recall dashboard
//                                     (Transcription credentials) first; both take a key-term list so "Pulse",
//                                     "Pinpoint" and the ops vocabulary are boosted, and both handle accents
//                                     far better than Recall's low-latency engine.
//       PULSE_KEYTERMS                comma-separated extra key terms (facility codes, client names…)
//       PULSE_CONVO_SECONDS           after Pulse answers, operations questions without the name are
//                                     answered for this long (default 45; 0 = always say the name)
//       PULSE_OPS_TERMS               extra words that mark a sentence as "about our work" (see pulse_wake.js)
//       RECALL_TRANSCRIPT_PROVIDER    JSON override of the whole transcript provider block (wins over RECALL_STT)
'use strict';
const crypto = require('crypto');

// 0.3 s of silence, 32 kbps mono. Recall requires automatic_audio_output to be configured
// before the output_audio endpoint can be used; this is the inaudible default it plays.
const SILENT_MP3_B64 = 'SUQzBAAAAAAAI1RTU0UAAAAPAAADTGF2ZjYwLjE2LjEwMAAAAAAAAAAAAAAA//NwwAAAAAAAAAAAAEluZm8AAAAPAAAADgAABm0ALCwsLCwsLDw8PDw8PDxNTU1NTU1NXV1dXV1dXW1tbW1tbW19fX19fX19jo6Ojo6Ojp6enp6enp6erq6urq6urr6+vr6+vr7Pz8/Pz8/P39/f39/f3+/v7+/v7+//////////AAAAAExhdmM2MC4zMQAAAAAAAAAAAAAAACQDaQAAAAAAAAZtL+ircQAAAAAAAAAAAAAAAAD/80DEAAAAA0gAAAAATEFNRTMuMTAwVVVVVVVVVVVVVUxBTUUzLjEwMFVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/zQsRbAAADSAAAAABVVVVVVVVVVVVVVVVVVVVVVVVVVUxBTUUzLjEwMFVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/zQMSkAAADSAAAAABVVVVVVVVVVVVVVVVVVVVVVVVVTEFNRTMuMTAwVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV//NCxKMAAANIAAAAAFVVVVVVVVVVVVVVVVVVVVVVVVVVTEFNRTMuMTAwVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV//NAxKQAAANIAAAAAFVVVVVVVVVVVVVVVVVVVVVVVVVMQU1FMy4xMDBVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVX/80LEowAAA0gAAAAAVVVVVVVVVVVVVVVVVVVVVVVVVVVMQU1FMy4xMDBVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVX/80DEpAAAA0gAAAAAVVVVVVVVVVVVVVVVVVVVVVVVVUxBTUUzLjEwMFVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/zQsSjAAADSAAAAABVVVVVVVVVVVVVVVVVVVVVVVVVVUxBTUUzLjEwMFVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/zQMSkAAADSAAAAABVVVVVVVVVVVVVVVVVVVVVVVVVTEFNRTMuMTAwVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV//NCxKMAAANIAAAAAFVVVVVVVVVVVVVVVVVVVVVVVVVVTEFNRTMuMTAwVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV//NAxKQAAANIAAAAAFVVVVVVVVVVVVVVVVVVVVVVVVVMQU1FMy4xMDBVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVX/80LEowAAA0gAAAAAVVVVVVVVVVVVVVVVVVVVVVVVVVVMQU1FMy4xMDBVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVX/80DEpAAAA0gAAAAAVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/zQsSjAAADSAAAAABVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVQ==';

const wake = require('./pulse_wake');                       // shared wake-word list (also served to the browser)
const CONTEXT_MINUTES = 20, RETENTION_DAYS = 7;

module.exports = function pulseMeetings(deps) {
  const { app, db, authenticateRequest, requireRole, requireInternalOrg, curClient, internal, synthesize, mondayOf, audit, holdEmail, runToolAs, log = console, now = () => Date.now(), fetchImpl } = deps;
  const _fetch = fetchImpl || globalThis.fetch;

  db.exec(`
    CREATE TABLE IF NOT EXISTS pulse_meeting (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id     TEXT NOT NULL,
      title         TEXT,
      meeting_url   TEXT NOT NULL,
      platform      TEXT,
      scheduled_at  TEXT,
      bot_id        TEXT,
      status        TEXT NOT NULL DEFAULT 'scheduled',   -- scheduled | joining | waiting | live | ended | failed | cancelled
      started_at    TEXT,
      ended_at      TEXT,
      organiser     TEXT,
      attendees     TEXT,
      source        TEXT NOT NULL DEFAULT 'panel',
      created_by    TEXT,
      created_by_name TEXT,
      secret        TEXT NOT NULL,
      summary       TEXT,
      summary_json  TEXT,
      summary_email_id INTEGER,
      transcript_deleted_at TEXT,
      last_error    TEXT,
      open_mode     INTEGER NOT NULL DEFAULT 0,          -- 1: answer any operations question, no name needed
      created_at    TEXT NOT NULL,
      updated_at    TEXT
    );
    CREATE TABLE IF NOT EXISTS pulse_meeting_line (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      meeting_id  INTEGER NOT NULL,
      at_rel      REAL,
      speaker     TEXT,
      text        TEXT NOT NULL,
      is_question INTEGER NOT NULL DEFAULT 0,
      answer      TEXT,
      created_at  TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_pml ON pulse_meeting_line(meeting_id, id);
    CREATE TABLE IF NOT EXISTS pulse_meeting_action (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      meeting_id  INTEGER NOT NULL,
      line_id     INTEGER,
      tool        TEXT NOT NULL,
      args_json   TEXT NOT NULL,
      summary     TEXT NOT NULL,
      status      TEXT NOT NULL DEFAULT 'proposed',     -- proposed | done | dismissed | failed
      receipt     TEXT,
      settled_by  TEXT,
      settled_at  TEXT,
      created_at  TEXT NOT NULL
    );
  `);

  const iso = () => new Date(now()).toISOString();
  const KEY = () => process.env.RECALL_API_KEY || '';
  const BASE = () => String(process.env.RECALL_REGION || 'https://ap-northeast-1.recall.ai').replace(/\/+$/, '');
  const PUBLIC = () => String(process.env.PULSE_PUBLIC_BASE || 'https://vasuida1.onrender.com').replace(/\/+$/, '');
  const DEFAULT_CLIENT = () => process.env.PULSE_MEETING_DEFAULT_CLIENT || 'ICONIC';
  const enabled = () => !!KEY();
  const platformOf = (url) => /teams\.microsoft\.com|teams\.live\.com/i.test(url) ? 'teams' : /meet\.google\.com/i.test(url) ? 'meet' : /zoom\.us/i.test(url) ? 'zoom' : 'other';
  // Every link shape the three platforms issue today:
  //   Teams   https://teams.microsoft.com/l/meetup-join/19%3a…   (classic)
  //           https://teams.microsoft.com/meet/4123456789?p=…    (new short link)
  //           https://teams.live.com/meet/…                       (personal)
  //   Meet    https://meet.google.com/abc-defg-hij
  //   Zoom    https://company.zoom.us/j/123…  or /my/room
  const MEETING_LINK = /https?:\/\/(?:(?:[a-z0-9-]+\.)?teams\.microsoft\.com\/(?:l\/meetup-join|meet)\/[^\s"'<>)\]]+|teams\.live\.com\/meet\/[^\s"'<>)\]]+|meet\.google\.com\/[a-z]{3}-[a-z]{4}-[a-z]{3}(?:[?#][^\s"'<>)\]]*)?|[a-z0-9.-]*zoom\.us\/(?:j|my|s|w)\/[^\s"'<>)\]]+)/i;
  try { db.exec('ALTER TABLE pulse_meeting ADD COLUMN open_mode INTEGER NOT NULL DEFAULT 0'); } catch (_) {}
  const who = (req) => ({ userId: req.auth?.userId || 'anon', orgId: req.auth?.orgId || '', orgRole: req.auth?.orgRole || '' });
  const note = (req, action, body, status, error) => { try { const w = req ? who(req) : { userId: 'pulse', orgId: null, orgRole: null }; audit && audit.run(w.userId, w.orgId, w.orgRole, action, req ? req.path : '/pulse/meeting', req ? req.method : 'BOT', req ? req.ip : '', req ? req.headers['user-agent'] : 'recall', body, status, error); } catch (_) {} };
  const gate = [authenticateRequest, requireRole(['admin']), requireInternalOrg];
  const getM = (id) => db.prepare('SELECT * FROM pulse_meeting WHERE id = ?').get(Number(id));
  const setM = (id, patch) => { const k = Object.keys(patch); if (!k.length) return; db.prepare(`UPDATE pulse_meeting SET ${k.map(x => x + ' = ?').join(', ')}, updated_at = ? WHERE id = ?`).run(...k.map(x => patch[x]), iso(), id); };

  // ── Recall client ──
  async function recall(method, path, body) {
    if (!KEY()) throw Object.assign(new Error('Recall is not configured (RECALL_API_KEY)'), { code: 'recall_unconfigured' });
    const ctrl = new AbortController(); const tm = setTimeout(() => ctrl.abort(), 20000);
    try {
      const r = await _fetch(BASE() + path, { method, signal: ctrl.signal, headers: { Authorization: 'Token ' + KEY(), 'content-type': 'application/json', accept: 'application/json' }, body: body ? JSON.stringify(body) : undefined });
      const text = await r.text(); let json = null; try { json = text ? JSON.parse(text) : null; } catch (_) { json = { raw: text.slice(0, 300) }; }
      if (!r.ok) throw Object.assign(new Error(`Recall ${r.status}: ${text.slice(0, 200)}`), { status: r.status });
      return json;
    } finally { clearTimeout(tm); }
  }
  const BASE_KEYTERMS = ['Pulse', 'Pulse AI', 'Pinpoint', 'VelOzity', 'THE ICONIC', 'EHP Labs', 'packing list', 'FC receipt', 'Mobiledock', 'ETA', 'PO', 'container', 'customs', 'VAS', 'last mile', 'lane', 'vessel', 'departed', 'arrived', 'discharged', 'milestone'];
  function keyterms() {
    const extra = String(process.env.PULSE_KEYTERMS || '').split(',').map(s => s.trim()).filter(Boolean);
    return [...new Set([...BASE_KEYTERMS, ...extra])].slice(0, 100);
  }
  function transcriptProvider() {
    try { if (process.env.RECALL_TRANSCRIPT_PROVIDER) return JSON.parse(process.env.RECALL_TRANSCRIPT_PROVIDER); } catch (_) {}
    const stt = String(process.env.RECALL_STT || 'recall').toLowerCase();
    if (stt === 'deepgram') return { deepgram_streaming: { model: 'nova-3', language: 'en', smart_format: true, keyterm: keyterms() } };
    if (stt === 'assembly' || stt === 'assemblyai') return { assembly_ai_v3_streaming: { keyterms_prompt: keyterms() } };
    return { recallai_streaming: { mode: 'prioritize_low_latency', language_code: 'en' } };
  }
  async function createBot(m) {
    const body = {
      meeting_url: m.meeting_url, bot_name: 'Pulse (VelOzity)', metadata: { meeting_id: String(m.id), client: m.client_id },
      automatic_audio_output: { in_call_recording: { data: { kind: 'mp3', b64_data: SILENT_MP3_B64 } } },
      recording_config: {
        transcript: { provider: transcriptProvider() },
        realtime_endpoints: [{ type: 'webhook', url: `${PUBLIC()}/pulse/meeting/realtime?m=${m.id}&t=${m.secret}`, events: ['transcript.data', 'participant_events.join', 'participant_events.leave'] }],
      },
      automatic_leave: { waiting_room_timeout: 900, noone_joined_timeout: 900, everyone_left_timeout: { timeout: 60, activate_after: 60 } },
    };
    if (m.scheduled_at && new Date(m.scheduled_at).getTime() - now() > 2 * 60 * 1000) body.join_at = new Date(m.scheduled_at).toISOString();
    const bot = await recall('POST', '/api/v1/bot/', body);
    setM(m.id, { bot_id: bot.id, status: body.join_at ? 'scheduled' : 'joining', last_error: null });
    return bot;
  }

  // ── Creating a meeting ──
  function createMeeting(o) {
    const url = String(o.meeting_url || '').trim();
    const m = url.match(MEETING_LINK); if (!m) throw new Error('That is not a Teams, Google Meet or Zoom link.');
    const meeting_url = m[0].replace(/[),.;]+$/, '');
    const when = o.scheduled_at ? new Date(o.scheduled_at) : null;
    const r = db.prepare(`INSERT INTO pulse_meeting (client_id, title, meeting_url, platform, scheduled_at, status, organiser, attendees, source, created_by, created_by_name, secret, created_at)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(o.client || DEFAULT_CLIENT(), (o.title || '').slice(0, 200) || null, meeting_url, platformOf(meeting_url), when && !isNaN(when) ? when.toISOString() : null, 'scheduled',
           o.organiser || null, o.attendees ? JSON.stringify(o.attendees) : null, o.source || 'panel', o.created_by || null, o.created_by_name || null, crypto.randomBytes(16).toString('hex'), iso());
    return getM(r.lastInsertRowid);
  }
  async function startMeeting(m) {
    try { await createBot(m); return getM(m.id); }
    catch (e) { setM(m.id, { status: 'failed', last_error: String(e.message || e).slice(0, 300) }); log.warn('[pulse meeting] bot create failed #' + m.id + ': ' + e.message); throw e; }
  }

  // Invitations forwarded to the Pulse mailbox: find the join link and the start time.
  // Outlook: "When: Monday, 24 November 2026 10:00 AM-10:30 AM (UTC+10:00) ..."
  // Google:  "When  Monday 24 Nov 2026 ⋅ 10:00 – 10:30am (Australian Eastern Daylight Time)"
  const MONTHS = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };
  function parseInviteTime(text) {
    const t = String(text || '').replace(/ /g, ' ');
    const m = t.match(/\b(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})\b[^\n]{0,40}?(\d{1,2})[:.](\d{2})\s*(am|pm)?/i) || t.match(/\b([A-Za-z]{3})[a-z]*\s+(\d{1,2}),?\s+(\d{4})\b[^\n]{0,40}?(\d{1,2})[:.](\d{2})\s*(am|pm)?/i);
    if (!m) return null;
    let day, mon, year, hh, mm, ap;
    if (/^\d/.test(m[1])) { day = +m[1]; mon = MONTHS[m[2].slice(0, 3).toLowerCase()]; year = +m[3]; } else { mon = MONTHS[m[1].slice(0, 3).toLowerCase()]; day = +m[2]; year = +m[3]; }
    hh = +m[4]; mm = +m[5]; ap = (m[6] || '').toLowerCase();
    if (mon == null) return null;
    if (ap === 'pm' && hh < 12) hh += 12; if (ap === 'am' && hh === 12) hh = 0;
    // Offset: "(UTC+10:00)" if present, else Sydney for the given date.
    const off = t.match(/UTC([+-]\d{2}):?(\d{2})/i);
    let offMin;
    if (off) offMin = (Number(off[1]) * 60) + (Number(off[1]) < 0 ? -Number(off[2]) : Number(off[2]));
    else { const probe = new Date(Date.UTC(year, mon, day, 12)); const syd = new Intl.DateTimeFormat('en-US', { timeZone: 'Australia/Sydney', timeZoneName: 'shortOffset' }).formatToParts(probe).find(p => p.type === 'timeZoneName').value; const mo = syd.match(/GMT([+-]\d+)(?::(\d+))?/); offMin = mo ? Number(mo[1]) * 60 + Number(mo[2] || 0) : 600; }
    return new Date(Date.UTC(year, mon, day, hh, mm) - offMin * 60000);
  }
  // Called by the mailbox for every inbound email. Returns the meeting or null.
  async function fromInvite(inb) {
    const body = String(inb.text || '') + ' ' + String(inb.html || '');
    const link = body.match(MEETING_LINK); if (!link) return null;
    const from = String(inb.from || '').toLowerCase();
    if (!/@(velozity\.au|velozity\.com\.au)$/.test(from) && !(deps.isKnownSender && await deps.isKnownSender(from))) { log.log('[pulse meeting] invite from unknown sender ignored: ' + from); return null; }
    const url = link[0];
    if (db.prepare(`SELECT id FROM pulse_meeting WHERE meeting_url = ? AND status IN ('scheduled','joining','waiting','live')`).get(url)) return { duplicate: true };
    const plain = String(inb.text || '') || String(inb.html || '').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
    const when = parseInviteTime(plain);
    const subj = String(inb.subject || '');
    if (/^\s*(accepted|declined|tentative|canceled|cancelled)\b/i.test(subj)) { log.log('[pulse meeting] calendar response ignored: ' + subj); return null; }
    const client = (deps.clientForSender && await deps.clientForSender(from)) || DEFAULT_CLIENT();
    const title = subj.replace(/^\s*(fw|fwd|re|invitation|updated invitation|meeting invitation)\s*:\s*/ig, '').replace(/\s*@.*$/, '').trim().slice(0, 200) || 'Meeting';
    // Past by more than 90 minutes: an old invite or a forwarded thread — don't crash a finished meeting.
    if (when && when.getTime() < now() - 90 * 60 * 1000) { log.log(`[pulse meeting] invite for a past time ignored (${when.toISOString()}): ${title}`); return null; }
    const m = createMeeting({ meeting_url: url, title, client, scheduled_at: when, organiser: from, source: 'invite', created_by: 'pulse' });
    // Join now when the meeting is already on or about to start, or when the time could not be
    // read but the email is plainly an invitation; otherwise schedule the bot for the start.
    const looksLikeInvite = /invit|meeting|teams|meet\.google|zoom/i.test(subj + ' ' + plain.slice(0, 400));
    const joinNow = when ? when.getTime() <= now() + 3 * 60 * 1000 : looksLikeInvite;
    try { if (when || joinNow) await createBot(joinNow ? { ...m, scheduled_at: null } : m); }
    catch (e) { setM(m.id, { last_error: String(e.message || e).slice(0, 300) }); }
    note(null, 'pulse_meeting_invite', { id: m.id, from, when: when && when.toISOString(), title, join_now: joinNow }, 200, null);
    log.log(`[pulse meeting] invite → #${m.id} ${title} ${when ? when.toISOString() : '(no time)'}${joinNow ? ' — joining now' : ''}`);
    return getM(m.id);
  }

  // ── Routes (VelOzity admins) ──
  const shape = (m, full) => {
    const actions = db.prepare('SELECT * FROM pulse_meeting_action WHERE meeting_id = ? ORDER BY id').all(m.id).map(a => ({ ...a, args: JSON.parse(a.args_json || '{}') }));
    const out = { ...m, attendees: m.attendees ? JSON.parse(m.attendees) : [], summary_json: m.summary_json ? JSON.parse(m.summary_json) : null, secret: undefined,
      proposed: actions.filter(a => a.status === 'proposed').length, actions,
      lines: db.prepare('SELECT COUNT(*) n FROM pulse_meeting_line WHERE meeting_id = ?').get(m.id).n };
    if (full) out.transcript = db.prepare('SELECT id, at_rel, speaker, text, is_question, answer, created_at FROM pulse_meeting_line WHERE meeting_id = ? ORDER BY id').all(m.id);
    else out.recent = db.prepare('SELECT speaker, text, is_question, answer FROM pulse_meeting_line WHERE meeting_id = ? ORDER BY id DESC LIMIT 4').all(m.id).reverse();
    return out;
  };
  app.get('/pulse/meetings', ...gate, async (req, res) => {
    try { await reconcile(false); } catch (_) {}
    const rows = db.prepare(`SELECT * FROM pulse_meeting WHERE client_id = ? ORDER BY CASE status WHEN 'live' THEN 0 WHEN 'joining' THEN 0 WHEN 'waiting' THEN 0 WHEN 'scheduled' THEN 1 ELSE 2 END, COALESCE(scheduled_at, created_at) DESC LIMIT 30`).all(curClient());
    res.json({ ok: true, enabled: enabled(), meetings: rows.map(m => shape(m, false)) });
  });
  // "Check with Recall now" — forces a reconcile (the automatic one runs every two minutes).
  app.post('/pulse/meetings/reconcile', ...gate, async (req, res) => {
    try { const n = await reconcile(true); res.json({ ok: true, changed: n }); } catch (e) { res.status(502).json({ ok: false, error: String(e.message || e) }); }
  });
  app.get('/pulse/meetings/:id', ...gate, (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    res.json({ ok: true, meeting: shape(m, true) });
  });
  app.post('/pulse/meetings', ...gate, async (req, res) => {
    try {
      const b = req.body || {};
      const m = createMeeting({ meeting_url: b.meeting_url, title: b.title, client: curClient(), scheduled_at: b.scheduled_at || null, organiser: b.organiser || null, source: 'panel', created_by: req.auth?.userId || null, created_by_name: b.created_by_name || null });
      if (b.open_mode) setM(m.id, { open_mode: 1 });
      note(req, 'pulse_meeting_create', { id: m.id, url: m.meeting_url, when: m.scheduled_at, open: !!b.open_mode }, 200, null);
      if (!enabled()) { setM(m.id, { status: 'failed', last_error: 'Recall is not configured (RECALL_API_KEY)' }); return res.status(503).json({ ok: false, error: 'Meetings are not configured on the server yet.', meeting: shape(getM(m.id)) }); }
      await startMeeting(m);
      res.json({ ok: true, meeting: shape(getM(m.id)) });
    } catch (e) { res.status(400).json({ ok: false, error: String(e.message || e) }); }
  });
  // Join now: a meeting from an invite whose time could not be read, or a retry after a failure.
  app.post('/pulse/meetings/:id/join', ...gate, async (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    if (['live', 'joining', 'waiting'].includes(m.status) && m.bot_id) return res.json({ ok: true, meeting: shape(m) });
    if (!enabled()) return res.status(503).json({ ok: false, error: 'Meetings are not configured on the server yet.' });
    try { setM(m.id, { scheduled_at: null, status: 'scheduled' }); await startMeeting(getM(m.id)); note(req, 'pulse_meeting_join', { id: m.id }, 200, null); res.json({ ok: true, meeting: shape(getM(m.id)) }); }
    catch (e) { res.status(502).json({ ok: false, error: String(e.message || e) }); }
  });
  // "Answer that": the host could not get Pulse's attention (accent, cross-talk). Take what was said
  // in the last 20 s as the question; if nothing was heard, answer the next thing anyone says.
  app.post('/pulse/meetings/:id/answer', ...gate, async (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    if (!['live', 'joining', 'waiting'].includes(m.status)) return res.status(409).json({ ok: false, error: 'Pulse is not in that meeting right now.' });
    const since = new Date(now() - ANSWER_LAST_S * 1000).toISOString();
    const rows = db.prepare('SELECT id, speaker, text FROM pulse_meeting_line WHERE meeting_id = ? AND is_question = 0 AND created_at >= ? ORDER BY id DESC LIMIT 3').all(m.id, since).reverse();
    note(req, 'pulse_meeting_answer_last', { id: m.id, lines: rows.length }, 200, null);
    if (!rows.length || busy.get(m.id)) {
      listenNext.set(m.id, now() + LISTEN_NEXT_MS);
      if (!busy.get(m.id)) say(m, 'Yes — go ahead.');
      return res.json({ ok: true, mode: 'next', message: 'Pulse will answer the next thing said.' });
    }
    listenNext.delete(m.id);
    const question = rows.map(r => r.text).join(' ');
    db.prepare(`UPDATE pulse_meeting_line SET is_question = 1 WHERE id IN (${rows.map(() => '?').join(',')})`).run(...rows.map(r => r.id));
    res.json({ ok: true, mode: 'last', question });
    busy.set(m.id, true);
    try { await answer(m, rows[rows.length - 1].id, rows[rows.length - 1].speaker, question); }
    catch (e) { log.warn('[pulse meeting] answer-last failed #' + m.id + ': ' + (e && e.message)); }
    finally { busy.delete(m.id); }
  });
  // Open mode on/off from the panel (also by voice: "Pulse, open mode" / "Pulse, name only").
  app.post('/pulse/meetings/:id/mode', ...gate, async (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    const on = !!(req.body || {}).open;
    if (['live', 'joining', 'waiting'].includes(m.status) && m.bot_id) await setOpenMode(m, on, req.auth?.userId || 'panel');
    else { setM(m.id, { open_mode: on ? 1 : 0 }); note(req, 'pulse_meeting_mode', { meeting: m.id, open: on }, 200, null); }
    res.json({ ok: true, meeting: shape(getM(m.id)) });
  });
  app.post('/pulse/meetings/:id/leave', ...gate, async (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    try { if (m.bot_id) await recall('POST', `/api/v1/bot/${m.bot_id}/leave_call/`); } catch (e) { log.warn('[pulse meeting] leave failed: ' + e.message); }
    if (m.status === 'scheduled') { try { if (m.bot_id) await recall('DELETE', `/api/v1/bot/${m.bot_id}/`); } catch (_) {} setM(m.id, { status: 'cancelled' }); }
    // Leaving a meeting that is (or looks) live ends it here and now: notes, tasks and the queued
    // actions must not wait for a status webhook that may never come.
    else if (['joining', 'waiting', 'live'].includes(m.status)) { setM(m.id, { status: 'ended', ended_at: iso() }); try { await finish(getM(m.id)); } catch (e) { log.warn('[pulse meeting] finish after leave failed #' + m.id + ': ' + e.message); } }
    note(req, 'pulse_meeting_leave', { id: m.id }, 200, null);
    res.json({ ok: true, meeting: shape(getM(m.id)) });
  });
  app.post('/pulse/meetings/:id/actions/:aid/:verb', ...gate, async (req, res) => {
    const m = getM(req.params.id); if (!m || m.client_id !== curClient()) return res.status(404).json({ ok: false, error: 'no such meeting' });
    const a = db.prepare('SELECT * FROM pulse_meeting_action WHERE id = ? AND meeting_id = ?').get(Number(req.params.aid), m.id);
    if (!a) return res.status(404).json({ ok: false, error: 'no such action' });
    if (a.status !== 'proposed') return res.status(409).json({ ok: false, error: `already ${a.status}` });
    const verb = req.params.verb;
    if (verb === 'dismiss') { db.prepare(`UPDATE pulse_meeting_action SET status = 'dismissed', settled_by = ?, settled_at = ? WHERE id = ?`).run(req.auth?.userId || null, iso(), a.id); note(req, 'pulse_meeting_action_dismiss', { id: a.id, summary: a.summary }, 200, null); maybeForget(m.id); return res.json({ ok: true }); }
    if (verb !== 'confirm') return res.status(400).json({ ok: false, error: 'confirm or dismiss' });
    // Runs as the person confirming — their token, their permissions — like /pulse/confirm.
    const r = await runToolAs(req, a.tool, JSON.parse(a.args_json || '{}'));
    db.prepare(`UPDATE pulse_meeting_action SET status = ?, receipt = ?, settled_by = ?, settled_at = ? WHERE id = ?`).run(r.ok ? 'done' : 'failed', r.ok ? (r.result && r.result.receipt) || 'Done' : String(r.result && r.result.error || 'failed'), req.auth?.userId || null, iso(), a.id);
    note(req, 'pulse_meeting_action_confirm', { id: a.id, tool: a.tool, summary: a.summary, receipt: r.result && r.result.receipt }, r.ok ? 200 : 422, r.ok ? null : String(r.result && r.result.error || ''));
    maybeForget(m.id);
    if (!r.ok) return res.status(422).json({ ok: false, error: String(r.result && r.result.error || 'The action failed.') });
    res.json({ ok: true, receipt: (r.result && r.result.receipt) || 'Done.' });
  });

  // ── Live: Recall's realtime webhook (per-bot URL carries the meeting's own secret) ──
  app.post('/pulse/meeting/realtime', async (req, res) => {
    const m = getM(req.query.m);
    if (!m || String(req.query.t || '') !== m.secret) return res.status(401).json({ ok: false });
    res.json({ ok: true });                                    // answer at once; the work is async
    const ev = req.body || {};
    try {
      if (ev.event === 'transcript.data') await onUtterance(m, ev.data || {});
      else if (ev.event === 'participant_events.join' || ev.event === 'participant_events.leave') onParticipant(m, ev);
    } catch (e) { log.warn('[pulse meeting] realtime error #' + m.id + ': ' + (e && e.message)); }
  });
  function onParticipant(m, ev) {
    const p = ((ev.data || {}).data || {}).participant || {}; if (!p.name) return;
    const list = m.attendees ? JSON.parse(m.attendees) : [];
    if (ev.event.endsWith('.join') && !list.includes(p.name) && !/^pulse\b/i.test(p.name)) { list.push(p.name); setM(m.id, { attendees: JSON.stringify(list) }); }
    if (m.status !== 'live' && ev.event.endsWith('.join')) setM(m.id, { status: 'live', started_at: m.started_at || iso() });
  }
  const busy = new Map();                                      // meeting id → answering right now
  const listenNext = new Map();                                // meeting id → until (ms): answer the next thing said, no wake word
  const LISTEN_NEXT_MS = 20000, ANSWER_LAST_S = 20;
  const convo = new Map();                                     // meeting id → until (ms): Pulse is in the conversation
  const CONVO_MS = () => Math.max(0, Number(process.env.PULSE_CONVO_SECONDS ?? 45)) * 1000;
  const opsRx = wake.opsRegex();
  async function onUtterance(m, d) {
    const inner = d.data || {};
    const words = Array.isArray(inner.words) ? inner.words : [];
    const text = words.map(w => w.text).join(' ').replace(/\s+([,.?!])/g, '$1').trim();
    if (!text) return;
    const speaker = (inner.participant && inner.participant.name) || 'Someone';
    if (/^pulse\b/i.test(speaker)) return;                     // the bot's own audio, if ever transcribed
    const atRel = words.length ? Number(words[0].start_timestamp && words[0].start_timestamp.relative) || null : null;
    if (m.status !== 'live') { setM(m.id, { status: 'live', started_at: m.started_at || iso() }); m = getM(m.id); }
    let { hit: isQ, question } = wake.strip(text);
    let named = isQ;
    if (!isQ && (listenNext.get(m.id) || 0) > now()) { isQ = true; question = text; }   // host pressed "Answer that": no name needed
    // Already in the conversation, or open mode: an operations question counts without the name.
    if (!isQ && (m.open_mode || (convo.get(m.id) || 0) > now()) && wake.looksLikeQuestion(text, opsRx)) { isQ = true; question = text; }
    const line = db.prepare('INSERT INTO pulse_meeting_line (meeting_id, at_rel, speaker, text, is_question, created_at) VALUES (?,?,?,?,?,?)').run(m.id, atRel, speaker, text, isQ ? 1 : 0, iso());
    if (!isQ) return;
    listenNext.delete(m.id);
    const cmd = named ? wake.modeCommand(question) : null;
    if (cmd) { await setOpenMode(m, cmd === 'open', speaker); return; }
    if (busy.get(m.id)) { log.log('[pulse meeting] still answering; skipped: ' + question); return; }
    busy.set(m.id, true);
    try { await answer(m, Number(line.lastInsertRowid), speaker, question); }
    finally { busy.delete(m.id); }
  }
  async function setOpenMode(m, on, by) {
    setM(m.id, { open_mode: on ? 1 : 0 });
    note(null, 'pulse_meeting_mode', { meeting: m.id, open: on, by }, 200, null);
    await say(m, on ? 'Open mode — I will answer any operations question; no need to say my name.' : 'Back to name only — say Pulse when you need me.');
  }
  function contextFor(m, beforeLineId) {
    const since = new Date(now() - CONTEXT_MINUTES * 60000).toISOString();
    const rows = db.prepare('SELECT speaker, text, is_question, answer FROM pulse_meeting_line WHERE meeting_id = ? AND id < ? AND created_at >= ? ORDER BY id DESC LIMIT 80').all(m.id, beforeLineId, since).reverse();
    let out = rows.map(r => `${r.speaker}: ${r.text}${r.answer ? `\nPulse: ${r.answer}` : ''}`).join('\n');
    if (out.length > 6000) out = out.slice(-6000);
    return out;
  }
  async function say(m, text) {
    if (!m.bot_id) return false;
    try {
      const out = await synthesize(text);
      const buf = await out.buffer();
      await recall('POST', `/api/v1/bot/${m.bot_id}/output_audio/`, { kind: 'mp3', b64_data: buf.toString('base64') });
      return true;
    } catch (e) { log.warn('[pulse meeting] say failed #' + m.id + ': ' + (e && e.message)); return false; }
  }
  async function answer(m, lineId, speaker, question) {
    if (!question) { listenNext.set(m.id, now() + LISTEN_NEXT_MS); await say(m, 'Yes?'); return; }   // name alone: the next sentence is the question
    const ws = typeof mondayOf === 'function' ? mondayOf(new Date(now()).toISOString().slice(0, 10)) : undefined;
    let out;
    try {
      out = await internal.json('/pulse/chat', { method: 'POST', headers: { 'content-type': 'application/json', 'x-pulse-enabled': '1', 'x-pinpoint-client': m.client_id },
        body: JSON.stringify({ messages: [{ role: 'user', content: question }], currentWeek: ws ? { week_start: ws } : undefined, style: 'spoken',
          persona: { as: 'meeting', client: m.client_id, speaker, title: m.title, attendees: m.attendees ? JSON.parse(m.attendees) : [], context: contextFor(m, lineId) } }) });
    } catch (e) {
      log.warn('[pulse meeting] chat failed #' + m.id + ': ' + e.message);
      await say(m, 'Sorry, I could not look that up just now.');
      return;
    }
    const reply = String(out.spoken || out.reply || '').trim();
    for (const a of (out.actions || [])) {
      if (a.kind !== 'proposed') continue;
      db.prepare('INSERT INTO pulse_meeting_action (meeting_id, line_id, tool, args_json, summary, created_at) VALUES (?,?,?,?,?,?)').run(m.id, lineId, a.tool, JSON.stringify(a.args || {}), a.summary || a.tool, iso());
    }
    db.prepare('UPDATE pulse_meeting_line SET answer = ? WHERE id = ?').run(reply, lineId);
    if (CONVO_MS()) convo.set(m.id, now() + CONVO_MS());        // follow-ups need no name for a while
    note(null, 'pulse_meeting_answer', { meeting: m.id, speaker, question: question.slice(0, 200), tools: (out.tools || []).map(t => t.tool), proposed: (out.actions || []).filter(a => a.kind === 'proposed').length }, 200, null);
    if (reply) await say(m, reply);
  }

  // ── Bot status webhook (Svix-signed with RECALL_WEBHOOK_SECRET) ──
  function verifySvix(req) {
    const secret = process.env.RECALL_WEBHOOK_SECRET || '';
    if (!secret) return { ok: false, error: 'RECALL_WEBHOOK_SECRET not set' };
    // Svix sends svix-*; Recall sends the Standard Webhooks spelling (webhook-*). Same scheme.
    const h = (a, b) => req.headers[a] || req.headers[b];
    const id = h('svix-id', 'webhook-id'), ts = h('svix-timestamp', 'webhook-timestamp'), sig = h('svix-signature', 'webhook-signature');
    if (!id || !ts || !sig) return { ok: false, error: 'missing signature headers (svix-* or webhook-*)' };
    if (Math.abs(now() / 1000 - Number(ts)) > 300) return { ok: false, error: 'timestamp outside tolerance' };
    const raw = req.rawBody ? req.rawBody.toString('utf8') : JSON.stringify(req.body || {});
    const expected = crypto.createHmac('sha256', Buffer.from(String(secret).replace(/^whsec_/, ''), 'base64')).update(`${id}.${ts}.${raw}`).digest('base64');
    const ok = String(sig).split(/\s+/).some(part => { const v = part.split(',')[1] || ''; try { return v.length === expected.length && crypto.timingSafeEqual(Buffer.from(v), Buffer.from(expected)); } catch (_) { return false; } });
    return ok ? { ok: true } : { ok: false, error: 'bad signature' };
  }
  const STATUS = { 'bot.joining_call': 'joining', 'bot.in_waiting_room': 'waiting', 'bot.in_call_not_recording': 'live', 'bot.recording_permission_allowed': 'live', 'bot.in_call_recording': 'live', 'bot.call_ended': 'ended', 'bot.done': 'ended', 'bot.fatal': 'failed', 'bot.recording_permission_denied': 'failed' };
  // Probe for the Recall dashboard / a browser: confirms the path is right and whether the
  // signing secret is configured, without revealing it. A wrong URL shows as a 404 here too.
  app.get('/pulse/meeting/events', (req, res) => {
    const secret = String(process.env.RECALL_WEBHOOK_SECRET || '');
    res.json({ ok: true, endpoint: '/pulse/meeting/events', method: 'POST', secret_set: !!secret, secret_prefix_ok: !secret || /^whsec_/.test(secret), region: process.env.RECALL_REGION || null, public_base: process.env.PULSE_PUBLIC_BASE || null });
  });
  app.post('/pulse/meeting/events', async (req, res) => {
    const v = verifySvix(req);
    if (!v.ok) { log.warn('[pulse meeting] webhook refused: ' + v.error + ' (id ' + (req.get('svix-id') || req.get('webhook-id') || '-') + ', event ' + ((req.body || {}).event || '?') + ')'); return res.status(v.error.includes('not set') ? 503 : 401).json({ ok: false, error: v.error, hint: v.error.includes('not set') ? 'set RECALL_WEBHOOK_SECRET on Render to the endpoint\'s signing secret (whsec_…)' : 'RECALL_WEBHOOK_SECRET does not match this endpoint\'s signing secret — each endpoint and each Recall region has its own' }); }
    res.json({ ok: true });
    const ev = req.body || {}; const botId = ev.data && ev.data.bot && ev.data.bot.id; if (!botId) return;
    const m = db.prepare('SELECT * FROM pulse_meeting WHERE bot_id = ?').get(botId); if (!m) return;
    const next = STATUS[ev.event]; if (!next) return;
    const sub = ev.data && ev.data.data && ev.data.data.sub_code;
    const patch = {};
    if (next === 'live' && m.status !== 'live' && m.status !== 'ended') { patch.status = 'live'; patch.started_at = m.started_at || iso(); }
    else if (next === 'ended' && m.status !== 'ended') { patch.status = 'ended'; patch.ended_at = iso(); }
    else if (next === 'failed' && m.status !== 'ended') { patch.status = m.status === 'live' ? 'ended' : 'failed'; patch.last_error = `${ev.event}${sub ? ' (' + sub + ')' : ''}`; if (m.status === 'live') patch.ended_at = iso(); }
    else if (m.status !== 'live' && m.status !== 'ended') patch.status = next;
    setM(m.id, patch);
    log.log(`[pulse meeting] #${m.id} ${ev.event}${sub ? ' ' + sub : ''} → ${patch.status || m.status}`);
    if (patch.status === 'ended') { try { await finish(getM(m.id)); } catch (e) { log.warn('[pulse meeting] finish failed #' + m.id + ': ' + e.message); } }
  });

  // ── After the meeting: summary, actions, retention ──
  async function finish(m) {
    if (m.summary) return;
    const lines = db.prepare('SELECT speaker, text, answer FROM pulse_meeting_line WHERE meeting_id = ? ORDER BY id').all(m.id);
    const proposed = db.prepare(`SELECT summary FROM pulse_meeting_action WHERE meeting_id = ? AND status = 'proposed'`).all(m.id).map(a => a.summary);
    let sj = null, text = '';
    if (lines.length >= 3) {
      const transcript = lines.map(l => `${l.speaker}: ${l.text}${l.answer ? `\nPulse: ${l.answer}` : ''}`).join('\n').slice(-24000);
      try {
        const out = await internal.json('/pulse/chat', { method: 'POST', headers: { 'content-type': 'application/json', 'x-pulse-enabled': '1', 'x-pinpoint-client': m.client_id },
          body: JSON.stringify({ messages: [{ role: 'user', content: `Summarise this meeting transcript. Reply with JSON only, no prose, shaped exactly: {"summary":"3-5 sentences","decisions":["..."],"actions":[{"who":"name or team","what":"...","due":"YYYY-MM-DD or null"}],"open_questions":["..."]}. Actions already queued for Pulse: ${JSON.stringify(proposed)}. Do not repeat those in actions.\n\nTRANSCRIPT:\n${transcript}` }], persona: { as: 'summary', client: m.client_id }, style: 'text' }) });
        const raw = String(out.reply || ''); const j = raw.match(/\{[\s\S]*\}/);
        if (j) sj = JSON.parse(j[0]);
      } catch (e) { log.warn('[pulse meeting] summary model failed: ' + e.message); }
    }
    if (!sj) sj = { summary: lines.length ? `Pulse was in the meeting for ${lines.length} exchanges and answered ${lines.filter(l => l.answer).length} question${lines.filter(l => l.answer).length === 1 ? '' : 's'}.` : 'Pulse joined but heard nothing it could transcribe.', decisions: [], actions: [], open_questions: [] };
    // People's action items become tasks; Pulse's own proposals wait in the panel.
    for (const a of (sj.actions || [])) {
      try { db.prepare(`INSERT INTO pulse_task (client_id, title, detail, owner, status, due_at, about, source, created_by, created_by_name, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(m.client_id, String(a.what || '').slice(0, 300), `From meeting: ${m.title || m.meeting_url}`, String(a.who || 'VelOzity team').slice(0, 80), 'open', a.due && /^\d{4}-\d{2}-\d{2}/.test(a.due) ? a.due.slice(0, 10) : null, null, 'meeting', 'pulse', null, iso()); } catch (_) {}
    }
    const fmt = (arr, none) => arr && arr.length ? arr.map(x => `• ${typeof x === 'string' ? x : `${x.what}${x.who ? ` — ${x.who}` : ''}${x.due ? ` (by ${x.due})` : ''}`}`).join('\n') : none;
    text = `${sj.summary}\n\nDecisions\n${fmt(sj.decisions, '• none recorded')}\n\nActions\n${fmt(sj.actions, '• none')}${proposed.length ? `\n\nQueued for Pulse (confirm in Pinpoint)\n${fmt(proposed)}` : ''}\n\nOpen questions\n${fmt(sj.open_questions, '• none')}`;
    const patch = { summary: text, summary_json: JSON.stringify(sj) };
    const to = m.organiser && /@/.test(m.organiser) ? [m.organiser] : [];
    if (typeof holdEmail === 'function' && (to.length || deps.fallbackTo)) {
      try { const e = holdEmail({ client: m.client_id, to: to.length ? to : [deps.fallbackTo], subject: `Meeting notes: ${m.title || 'meeting'} — ${new Date(now()).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'Australia/Sydney' })}`, body: text, title: `Meeting notes: ${m.title || m.meeting_url}`, about: m.title, source: 'meeting' }); patch.summary_email_id = e.email_id; }
      catch (e) { log.warn('[pulse meeting] summary email failed: ' + e.message); }
    }
    setM(m.id, patch);
    note(null, 'pulse_meeting_summary', { meeting: m.id, actions: (sj.actions || []).length, proposed: proposed.length, emailed: !!patch.summary_email_id }, 200, null);
    maybeForget(m.id);
  }
  // Transcript retention: gone once every proposal is settled (and the meeting is over), or after 7 days.
  function maybeForget(id) {
    const m = getM(id); if (!m || m.status !== 'ended' || m.transcript_deleted_at) return;
    const open = db.prepare(`SELECT COUNT(*) n FROM pulse_meeting_action WHERE meeting_id = ? AND status = 'proposed'`).get(id).n;
    const old = m.ended_at && (now() - new Date(m.ended_at).getTime()) > RETENTION_DAYS * 86400000;
    if (!open || old) { db.prepare('DELETE FROM pulse_meeting_line WHERE meeting_id = ?').run(id); setM(id, { transcript_deleted_at: iso() }); if (old && open) db.prepare(`UPDATE pulse_meeting_action SET status = 'dismissed', settled_by = 'retention', settled_at = ? WHERE meeting_id = ? AND status = 'proposed'`).run(iso(), id); }
  }
  // ── Reconcile with Recall ──
  // The status webhook is the normal way a meeting ends. If it is not configured, mis-signed or
  // simply lost, a meeting would stay "live" forever and the notes would never go out — so every
  // couple of minutes, and whenever the panel lists meetings, we ask Recall what the bot is doing
  // and trust that over our own record.
  const RECONCILE_MS = 2 * 60 * 1000; let lastReconcile = 0; let reconciling = false;
  const ENDED_CODES = new Set(['call_ended', 'done', 'fatal', 'recording_permission_denied', 'analysis_done', 'media_expired']);
  async function reconcile(force) {
    if (!enabled() || reconciling) return 0; if (!force && now() - lastReconcile < RECONCILE_MS) return 0;
    lastReconcile = now(); reconciling = true; let changed = 0;
    try {
      const rows = db.prepare(`SELECT * FROM pulse_meeting WHERE status IN ('joining','waiting','live') AND bot_id IS NOT NULL`).all();
      for (const m of rows) {
        let bot; try { bot = await recall('GET', `/api/v1/bot/${m.bot_id}/`); } catch (e) { if (e.status === 404) { setM(m.id, { status: 'ended', ended_at: iso(), last_error: 'bot not found at Recall' }); changed++; try { await finish(getM(m.id)); } catch (_) {} } continue; }
        const sc = Array.isArray(bot.status_changes) ? bot.status_changes : []; const last = sc.length ? sc[sc.length - 1] : null; const code = last && String(last.code || '');
        if (!code) continue;
        if (ENDED_CODES.has(code)) {
          setM(m.id, { status: 'ended', ended_at: last.created_at || iso(), last_error: code === 'fatal' ? 'fatal' + (last.sub_code ? ' (' + last.sub_code + ')' : '') : null }); changed++;
          log.log(`[pulse meeting] #${m.id} reconciled from Recall: ${code} → ended (status webhook missing?)`);
          note(null, 'pulse_meeting_reconciled', { meeting: m.id, code }, 200, null);
          try { await finish(getM(m.id)); } catch (e) { log.warn('[pulse meeting] finish failed #' + m.id + ': ' + e.message); }
        } else if (code === 'in_call_recording' || code === 'in_call_not_recording') { if (m.status !== 'live') { setM(m.id, { status: 'live', started_at: m.started_at || iso() }); changed++; } }
        else if (code === 'in_waiting_room' && m.status !== 'waiting') { setM(m.id, { status: 'waiting' }); changed++; }
      }
    } finally { reconciling = false; }
    return changed;
  }
  setInterval(() => reconcile(false).catch(() => {}), RECONCILE_MS).unref();

  function sweep() {
    try { for (const m of db.prepare(`SELECT id FROM pulse_meeting WHERE status = 'ended' AND transcript_deleted_at IS NULL`).all()) maybeForget(m.id); } catch (_) {}
    // A scheduled meeting whose time passed with no bot (Recall unconfigured when it was created) is marked.
    try { db.prepare(`UPDATE pulse_meeting SET status = 'failed', last_error = 'never joined' WHERE status = 'scheduled' AND bot_id IS NULL AND scheduled_at < ?`).run(new Date(now() - 2 * 3600000).toISOString()); } catch (_) {}
  }
  setInterval(sweep, 60 * 60 * 1000).unref();

  return { createMeeting, startMeeting, fromInvite, parseInviteTime, onUtterance, finish, maybeForget, sweep, reconcile, verifySvix, enabled, MEETING_LINK, transcriptProvider, keyterms };
};
