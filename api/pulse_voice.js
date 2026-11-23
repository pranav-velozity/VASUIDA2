// ── Pulse voice (Phase 2) ──
// Two small routes that turn the browser's audio into text and Pulse's text into audio, plus a
// status route the UI reads once so it knows which fallbacks to use. Both vendors are called
// server-side: the keys never reach the browser, and one place meters the spend.
//
//   POST /pulse/transcribe   { audio: <base64>, mime, duration_ms, mode }  → { text, ms }
//                            OpenAI transcription. A vocabulary hint keeps container numbers,
//                            lane names and "Pulse" from being heard as something else.
//   POST /pulse/speak        { text }                                        → audio/mpeg stream
//                            ElevenLabs Flash v2.5 with the chosen voice; if that voice is not
//                            on the account it retries once with the stock fallback voice.
//   GET  /pulse/voice/status                                                 → { stt, tts, used_seconds, cap_seconds, wake_words }
//
// Nothing is stored: the audio is forwarded and dropped, the transcript goes back to the
// browser (where it becomes an ordinary chat turn), and the usage table holds only seconds
// and character counts per user per day. The audit log gets the action and sizes, not the
// words.
//
// When a vendor key is missing the route answers 503 with a stable error code and the browser
// falls back to Web Speech (recognition and synthesis), so voice still works — just worse —
// before the Render variables are set.
//
// Env:  OPENAI_API_KEY, ELEVENLABS_API_KEY, ELEVENLABS_VOICE_ID (default BVBq6HVJVdnwOMJOqvy9)
//       PULSE_VOICE_MAX_MINUTES  per user per day, default 90 (speech in + speech out)
//       PULSE_STT_BASE / PULSE_TTS_BASE  override vendor base URLs (tests point them at a stub)
//       PULSE_STT_MODEL  default gpt-transcribe (gpt-4o-mini-transcribe is deprecated, shuts Feb 2027)
'use strict';

const STT_BASE  = () => String(process.env.PULSE_STT_BASE || 'https://api.openai.com/v1').replace(/\/+$/, '');
const TTS_BASE  = () => String(process.env.PULSE_TTS_BASE || 'https://api.elevenlabs.io/v1').replace(/\/+$/, '');
const STT_MODEL = () => process.env.PULSE_STT_MODEL || 'gpt-transcribe';
const LEGACY_STT = (m) => /^(whisper-1|gpt-4o(-mini)?-transcribe(-diarize)?)$/.test(m);   // old request shape
const VOICE_ID  = () => (process.env.ELEVENLABS_VOICE_ID || 'BVBq6HVJVdnwOMJOqvy9').trim();
const FALLBACK_VOICE = 'EXAVITQu4vr4xnSDxMaL';          // "Sarah", on every ElevenLabs account
const TTS_MODEL = 'eleven_flash_v2_5';
const MAX_CLIP_SECONDS = 60;                            // one utterance; the UI cuts at 20 s
const MAX_SPEAK_CHARS  = 1500;                          // a spoken answer is 2–3 sentences
const CHARS_PER_SECOND = 15;                            // how TTS characters count toward the cap

// Words the transcriber should expect. Prompting with them is the cheapest accuracy win:
// without it "Pulse" becomes "pause", "VAS" becomes "vast" and "MSKU" becomes "Miss Q".
const KEYWORDS = ['Pulse', 'Pinpoint', 'VelOzity', 'THE ICONIC', 'EHP', 'Kerry Logistics', 'Yennora', 'Kemps Creek', 'Zendesk', 'VAS', 'PO', 'UID labels',
  'non-compliance', 'consignment', 'MSKU1234567', 'TGHU7654321', 'SQ7819', 'AWB', 'chargeable kilos', 'dock slot', 'departed', 'arrived', 'cleared', 'delivered', 'W46'];
const VOCAB = 'Logistics operations in Australia: sea and air freight for a fashion retailer. Container numbers are four letters then seven digits, ' +
  'spoken letter by letter (MSKU 1234567). Weeks are called W46 or week 46. The assistant is called Pulse. Vocabulary: ' + KEYWORDS.join(', ') + '.';

module.exports = function pulseVoice(deps) {
  const { app, db, authenticateRequest, aiLimiter, aiAllowed, audit, log = console, fetchImpl } = deps;
  const _fetch = fetchImpl || globalThis.fetch;

  db.exec(`CREATE TABLE IF NOT EXISTS pulse_voice_usage (
    user_id     TEXT NOT NULL,
    day         TEXT NOT NULL,
    stt_seconds REAL NOT NULL DEFAULT 0,
    tts_chars   INTEGER NOT NULL DEFAULT 0,
    requests    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, day)
  )`);
  const qGet = db.prepare('SELECT stt_seconds, tts_chars, requests FROM pulse_voice_usage WHERE user_id=? AND day=?');
  const qAdd = db.prepare(`INSERT INTO pulse_voice_usage (user_id, day, stt_seconds, tts_chars, requests) VALUES (?, ?, ?, ?, 1)
    ON CONFLICT(user_id, day) DO UPDATE SET stt_seconds = stt_seconds + excluded.stt_seconds, tts_chars = tts_chars + excluded.tts_chars, requests = requests + 1`);

  const today = () => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  const capSeconds = () => Math.max(0.01, Number(process.env.PULSE_VOICE_MAX_MINUTES) || 90) * 60;
  const usedSeconds = (userId) => { const r = qGet.get(userId, today()) || { stt_seconds: 0, tts_chars: 0 }; return Number(r.stt_seconds || 0) + Number(r.tts_chars || 0) / CHARS_PER_SECOND; };
  const sttOn = () => !!process.env.OPENAI_API_KEY;
  const ttsOn = () => !!process.env.ELEVENLABS_API_KEY;

  const who = (req) => ({ userId: req.auth?.userId || 'anon', orgId: req.auth?.orgId || '', orgRole: req.auth?.orgRole || '' });
  const note = (req, action, body, status, error) => {
    try { const w = who(req); audit && audit.run(w.userId, w.orgId, w.orgRole, action, req.path, 'POST', req.ip, req.headers['user-agent'], body, status, error); } catch (_) {}
  };
  const overCap = (req, res, extraSeconds) => {
    const used = usedSeconds(who(req).userId);
    if (used + extraSeconds <= capSeconds()) return false;
    res.status(429).json({ error: 'voice_cap', message: 'Voice time for today is used up. Typing still works.', used_seconds: Math.round(used), cap_seconds: capSeconds() });
    return true;
  };

  // ── status ──
  app.get('/pulse/voice/status', authenticateRequest, (req, res) => {
    if (!aiAllowed(req, res)) return;
    const used = usedSeconds(who(req).userId);
    res.json({ stt: sttOn(), tts: ttsOn(), used_seconds: Math.round(used), cap_seconds: capSeconds(), voice_id: ttsOn() ? VOICE_ID() : null, stt_model: sttOn() ? STT_MODEL() : null, wake_words: require('./pulse_wake').list() });
  });

  // ── transcribe ──
  app.post('/pulse/transcribe', authenticateRequest, aiLimiter, async (req, res) => {
    if (!aiAllowed(req, res)) return;
    const t0 = Date.now();
    const { audio, mime, duration_ms, mode } = req.body || {};
    if (!sttOn()) return res.status(503).json({ error: 'stt_unavailable', message: 'Transcription is not configured on the server.' });
    if (typeof audio !== 'string' || audio.length < 100) return res.status(400).json({ error: 'audio required', message: 'Send the clip as base64 in "audio".' });
    let buf;
    try { buf = Buffer.from(audio.replace(/^data:[^;]+;base64,/, ''), 'base64'); } catch (_) { return res.status(400).json({ error: 'bad audio' }); }
    if (!buf.length || buf.length > 8 * 1024 * 1024) return res.status(400).json({ error: 'bad audio', message: 'Clip is empty or over 8 MB.' });
    // Seconds charged against the cap: the browser's duration when it is plausible, else a
    // size-based estimate (Opus at ~32 kbps ≈ 4 KB per second).
    const secs = Math.min(MAX_CLIP_SECONDS, Math.max(0.5, Number(duration_ms) > 0 ? Number(duration_ms) / 1000 : buf.length / 4000));
    if (overCap(req, res, secs)) return;

    const type = String(mime || 'audio/webm').split(';')[0];
    const ext = { 'audio/webm': 'webm', 'audio/ogg': 'ogg', 'audio/mp4': 'mp4', 'audio/mpeg': 'mp3', 'audio/wav': 'wav', 'audio/x-wav': 'wav', 'audio/flac': 'flac' }[type] || 'webm';
    const form = new FormData();
    form.append('file', new Blob([buf], { type }), 'clip.' + ext);
    const model = STT_MODEL();
    form.append('model', model);
    form.append('prompt', VOCAB);
    if (LEGACY_STT(model)) { form.append('language', 'en'); form.append('response_format', 'json'); }
    else { form.append('languages[]', 'en'); for (const k of KEYWORDS) form.append('keywords[]', k); }
    try {
      const ctrl = new AbortController(); const tm = setTimeout(() => ctrl.abort(), 20000);
      const r = await _fetch(STT_BASE() + '/audio/transcriptions', { method: 'POST', headers: { Authorization: 'Bearer ' + process.env.OPENAI_API_KEY }, body: form, signal: ctrl.signal });
      clearTimeout(tm);
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        log.warn('[pulse voice] stt ' + r.status + ' ' + JSON.stringify(j).slice(0, 200));
        note(req, 'pulse_transcribe', { bytes: buf.length, secs, mode }, 502, 'stt_' + r.status);
        return res.status(502).json({ error: 'stt_failed', message: 'Transcription failed.', status: r.status });
      }
      const text = String(j.text || '').trim();
      qAdd.run(who(req).userId, today(), secs, 0);
      note(req, 'pulse_transcribe', { bytes: buf.length, secs, mode, chars: text.length }, 200, null);
      res.json({ text, ms: Date.now() - t0, seconds: Math.round(secs * 10) / 10 });
    } catch (e) {
      log.warn('[pulse voice] stt error ' + (e && e.message));
      note(req, 'pulse_transcribe', { bytes: buf.length, secs, mode }, 502, String(e && e.message || e));
      res.status(502).json({ error: 'stt_failed', message: e.name === 'AbortError' ? 'Transcription timed out.' : 'Transcription failed.' });
    }
  });

  // ── speak ──
  // Shared with the meeting bot (Phase 4): text → mp3 Buffer with the chosen voice, falling
  // back to the stock voice if the chosen one is not on the account.
  async function synthesize(text, opts) {
    text = String(text || '').replace(/\s+/g, ' ').trim();
    if (!text) throw new Error('nothing to say');
    if (!ttsOn()) throw Object.assign(new Error('tts_unavailable'), { code: 'tts_unavailable' });
    if (text.length > MAX_SPEAK_CHARS) text = text.slice(0, MAX_SPEAK_CHARS).replace(/\s\S*$/, '') + '.';
    const format = (opts && opts.format) || 'mp3_22050_32';
    const call = async (voice) => {
      const ctrl = new AbortController(); const tm = setTimeout(() => ctrl.abort(), 20000);
      try {
        return await _fetch(`${TTS_BASE()}/text-to-speech/${encodeURIComponent(voice)}/stream?output_format=${encodeURIComponent(format)}`, {
          method: 'POST', signal: ctrl.signal,
          headers: { 'xi-api-key': process.env.ELEVENLABS_API_KEY, 'content-type': 'application/json', accept: 'audio/mpeg' },
          body: JSON.stringify({ text, model_id: TTS_MODEL, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.2, use_speaker_boost: true } }),
        });
      } finally { clearTimeout(tm); }
    };
    let voice = VOICE_ID();
    let r = await call(voice);
    if (!r.ok && (r.status === 404 || r.status === 422 || r.status === 400) && voice !== FALLBACK_VOICE) {
      log.warn('[pulse voice] tts voice ' + voice + ' → ' + r.status + ', retrying with fallback voice');
      voice = FALLBACK_VOICE; r = await call(voice);
    }
    if (!r.ok) { const body = await r.text().catch(() => ''); throw Object.assign(new Error('tts_' + r.status), { code: 'tts_failed', status: r.status, detail: body.slice(0, 200) }); }
    return { stream: r.body, voice, text, async buffer() { return Buffer.from(await new Response(r.body).arrayBuffer()); } };
  }

  app.post('/pulse/speak', authenticateRequest, aiLimiter, async (req, res) => {
    if (!aiAllowed(req, res)) return;
    if (!ttsOn()) return res.status(503).json({ error: 'tts_unavailable', message: 'Speech is not configured on the server.' });
    let text = String((req.body || {}).text || '').replace(/\s+/g, ' ').trim();
    if (!text) return res.status(400).json({ error: 'text required' });
    if (text.length > MAX_SPEAK_CHARS) text = text.slice(0, MAX_SPEAK_CHARS).replace(/\s\S*$/, '') + '.';
    if (overCap(req, res, text.length / CHARS_PER_SECOND)) return;
    try {
      const out = await synthesize(text);
      qAdd.run(who(req).userId, today(), 0, text.length);
      note(req, 'pulse_speak', { chars: text.length, voice: out.voice }, 200, null);
      res.setHeader('content-type', 'audio/mpeg');
      res.setHeader('cache-control', 'no-store');
      res.setHeader('x-pulse-voice', out.voice === FALLBACK_VOICE ? 'fallback' : 'primary');
      // Stream the vendor's body straight through so the first words play before the last are made.
      const { Readable } = require('stream');
      Readable.fromWeb(out.stream).on('error', () => { try { res.end(); } catch (_) {} }).pipe(res);
    } catch (e) {
      log.warn('[pulse voice] tts error ' + (e && e.message) + (e.detail ? ' ' + e.detail : ''));
      note(req, 'pulse_speak', { chars: text.length }, 502, String(e && e.message || e));
      if (!res.headersSent) res.status(502).json({ error: 'tts_failed', message: e.name === 'AbortError' ? 'Speech timed out.' : 'Speech failed.', status: e.status });
    }
  });

  return { usedSeconds, capSeconds, VOICE_ID, FALLBACK_VOICE, STT_MODEL, synthesize, ttsOn };
};
