/* ── VelOzity Pinpoint — Pulse voice v5 (continuous listening in Meeting mode) ──
   Hands-free Pulse. The Spoken switch on the Pulse bar (from pulse_agent_additive.js) now does
   what its name says: on, Pulse listens through the microphone and answers out loud; off, it is
   the typed assistant it always was. There are no buttons to hold.

   TWO WAYS OF LISTENING (segmented switch next to Spoken)
   - Solo     — one person at a desk. Every utterance is a question. Say it, Pulse answers.
   - Meeting  — several people in a room. Pulse stays quiet until it hears its name:
                "Pulse, which containers are late?" The name is stripped before the question is
                sent. Anything said without it is heard, discarded and never leaves the browser
                except as the short clip the transcriber needs to hear the name.
   The choice is remembered per browser. Opening Pinpoint on #meet turns Spoken on in Meeting
   mode with the panel open, for the laptop connected to the room screen.

   HOW IT HEARS
   The microphone stays open while Spoken is on. A small level detector watches it: when the room
   gets louder than its own background for a moment a clip starts; when it falls quiet again for
   ~0.7 s (Solo) or ~1.0 s (Meeting) the clip ends and goes to /pulse/transcribe. Pulse never
   listens to itself: the detector pauses while an answer is playing and for a beat after.
   Tap the status pill while Pulse is talking to stop it. Say "say that again" to hear the last
   answer once more without asking the model.

   THE PANEL STAYS OUT OF THE WAY
   Voice never opens the chat panel — in a meeting the screen behind it is what people are
   looking at. Heard questions and spoken answers are still written to the panel for whoever
   opens it later; meanwhile the pill on the bar shows the first line of each answer for a few
   seconds, and tapping it opens the panel at the full text. Typing still opens the panel, as it
   always has, and the #meet room laptop keeps it open on purpose.

   SAYING YES (Phase 3)
   When Pulse proposes an action it asks aloud ("Shall I record arrival for M S K U… as the
   sixteenth?") and then listens for an answer for 15 seconds (25 in Meeting mode, where the
   name is not needed for the reply). "Yes", "do it", "go ahead", "confirm" runs it and the
   receipt is read back; "no", "cancel", silence or anything unclear cancels. The on-screen
   card still works as a tap at any point.

   FALLBACKS
   If the server has no transcription key it says so once (503) and the browser's own speech
   recognition takes over; if it has no voice key, the browser's own voice reads the answer.
   Either way the panel still shows every word as text, so nothing is lost when audio is not
   possible — a muted laptop in a meeting still gets its answers on screen.

   Test hook: window._pulseVoice (state, start, stop, setMode, handleUtterance, speak, vad, setWake). */
;(function () {
  'use strict';

  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E', OK = '#1F7A4D', BAD = '#B42318';
  // Wake words: the server's list (GET /pulse/voice/status → wake_words) carries the name and the
  // ways transcribers mishear it across accents (bulbs, pause, pulls…); this default is the fallback.
  const WAKE_DEFAULT = ['pulse', 'pulse ai', 'pulsar', 'paulse', 'pals', 'pause', 'pulls', 'pull', 'bulbs', 'bulb', 'bulls', 'pols', 'polls', 'pinpoint'];
  const escW = (w) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+').replace(/'/g, "['’]?");
  let WAKE, MIDWAKE;
  function setWake(words) {
    const a = (Array.isArray(words) && words.length ? words : WAKE_DEFAULT).slice().sort((x, y) => y.length - x.length).map(escW).join('|');
    WAKE = new RegExp('^\\s*(?:hey|hi|ok|okay|hello|so|um|uh|right|now)?[\\s,]*(?:' + a + ')\\b[\\s,.:!?-]*', 'i');
    MIDWAKE = new RegExp('^.{3,}?[,.;:!?—-]?\\s+(?:hey|ok|okay)?\\s*(?:' + a + ')\\b[\\s,.:!?-]*(.{3,})$', 'i');
  }
  setWake(WAKE_DEFAULT);
  const AGAIN = /^(?:pulse[\s,]*)?(?:say that again|repeat that|say again|what was that|pardon|come again)\W*$/i;
  const YES = /^(?:pulse[\s,]*)?(?:yes|yes please|yep|yeah|yup|sure|do it|go ahead|go for it|confirm|confirmed|correct|that's right|ok|okay|please do|make it so)\b[\s!.]*$/i;
  const NO = /^(?:pulse[\s,]*)?(?:no|nope|cancel|don't|do not|stop|never mind|nevermind|leave it|not now)\b/i;
  const CONFIRM_SOLO_MS = 15000, CONFIRM_MEETING_MS = 25000, FOLLOW_MS = 8000;
  const STOP = /^(?:pulse[\s,]*)?(?:stop|don't send|do not send|don't send that|recall|cancel that|hold it|wait)\b/i;
  const MIN_CLIP_MS = 450, MAX_CLIP_MS = 20000, AFTER_SPEAK_MS = 350, SNIPPET_MS = 15000;

  const css = `
    .pv-mode{display:none;align-items:center;border:.5px solid rgba(0,0,0,.14);border-radius:8px;overflow:hidden;font-size:10.5px;color:${MID};margin-right:8px;user-select:none;}
    .pv-mode span{padding:3px 8px;cursor:pointer;line-height:1;} .pv-mode span.on{background:${BRAND};color:#fff;}
    .pv-mode.show{display:inline-flex;}
    .pv-pill{display:none;align-items:center;gap:7px;font-size:11px;color:${MID};padding:3px 10px 3px 7px;border-radius:12px;border:.5px solid rgba(0,0,0,.1);background:#fff;white-space:nowrap;margin-right:8px;max-width:340px;overflow:hidden;text-overflow:ellipsis;}
    .pv-pill .pv-txt{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;} .pv-pill.snippet{cursor:pointer;color:${DARK};}
    .pv-pill.show{display:inline-flex;}
    .pv-pill .pv-dot{width:8px;height:8px;border-radius:50%;background:${LIGHT};flex:none;transition:transform .12s;}
    .pv-pill.listening .pv-dot{background:${OK};} .pv-pill.hearing .pv-dot{background:${OK};transform:scale(1.5);}
    .pv-pill.thinking .pv-dot{background:${BRAND};animation:pv-blink 1s infinite;} .pv-pill.speaking{cursor:pointer;} .pv-pill.speaking .pv-dot{background:${BRAND};animation:pv-pulse .8s infinite;}
    .pv-pill.error .pv-dot{background:${BAD};} .pv-pill.off .pv-dot{background:${LIGHT};}
    @keyframes pv-blink{50%{opacity:.3}} @keyframes pv-pulse{50%{transform:scale(1.6)}}
    .pulse-msg.user.pv-heard .pulse-msg-from::after{content:' · heard';font-weight:400;color:${LIGHT};letter-spacing:0;}
  `;
  const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style);

  // ── state ──
  const V = window._pulseVoice = {
    on: false, mode: 'solo', state: 'off', detail: '', caps: null,
    stream: null, ctx: null, analyser: null, src: null, timer: null, rec: null, chunks: [], clipStart: 0,
    busy: false, audio: null, lastSpoken: '', useWebSpeech: false, recog: null, pausedUntil: 0, lastError: '', snippet: '', snippetTimer: null,
    awaiting: null,     // { token, summary, until, timer } while a proposed action waits for a spoken yes
    queue: [], pumping: false,   // clips waiting for the transcriber (Meeting mode keeps recording)
    followUntil: 0,     // after a bare "Pulse", the next sentence counts without the name
    setWake,            // swap the wake-word list (the server's list is applied when /pulse/voice/status loads)
    log: [],
  };
  try { V.mode = localStorage.getItem('pulse_voice_mode') === 'meeting' ? 'meeting' : 'solo'; } catch (_) {}

  const apiBase = () => (document.querySelector('meta[name="api-base"]')?.content || '').replace(/\/+$/, '');
  async function token() { if (window.Clerk?.session) { try { return await window.Clerk.session.getToken(); } catch (_) {} } return null; }
  async function post(path, body, accept) {
    const t = await token();
    const h = { 'Content-Type': 'application/json' }; if (t) h.Authorization = 'Bearer ' + t; if (accept) h.Accept = accept;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    return fetch(apiBase() + path, { method: 'POST', headers: h, body: JSON.stringify(body || {}) });
  }
  async function get(path) {
    const t = await token(); const h = {}; if (t) h.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    return fetch(apiBase() + path, { headers: h });
  }

  // ── UI: status pill + mode switch, mounted next to the Spoken switch ──
  let pill, modeEl;
  function mount() {
    const bar = document.getElementById('pulse-bar'), spk = document.getElementById('pa-spoken');
    if (!bar || !spk || document.getElementById('pv-pill')) return false;
    modeEl = document.createElement('div'); modeEl.id = 'pv-mode'; modeEl.className = 'pv-mode';
    modeEl.innerHTML = '<span data-m="solo" title="One person: every sentence is a question">Solo</span><span data-m="meeting" title="A room: Pulse answers only when it hears its name">Meeting</span>';
    modeEl.querySelectorAll('span').forEach(s => { s.onclick = () => setMode(s.dataset.m); });
    pill = document.createElement('div'); pill.id = 'pv-pill'; pill.className = 'pv-pill';
    pill.innerHTML = '<span class="pv-dot"></span><span class="pv-txt"></span>';
    pill.onclick = () => { if (V.state === 'speaking') stopSpeaking(); else if (pill.classList.contains('snippet')) openPanel(); };
    spk.insertAdjacentElement('afterend', modeEl); spk.insertAdjacentElement('afterend', pill);
    paintMode();
    // The Spoken switch is owned by pulse_agent_additive; follow its on/off class.
    new MutationObserver(() => { const on = spk.classList.contains('on'); if (on !== V.on) (on ? start() : stop()); }).observe(spk, { attributes: true, attributeFilter: ['class'] });
    return true;
  }
  function paintMode() { if (modeEl) modeEl.querySelectorAll('span').forEach(s => s.classList.toggle('on', s.dataset.m === V.mode)); }
  function setMode(m) {
    V.mode = m === 'meeting' ? 'meeting' : 'solo'; paintMode();
    try { localStorage.setItem('pulse_voice_mode', V.mode); } catch (_) {}
    if (V.on && V.state === 'listening') setState('listening');
  }
  function setState(s, detail) {
    if (s === 'listening' && !detail && V.snippet && V.snippetTimer) detail = V.snippet;   // keep the last answer visible for its 15 s
    if (s !== 'listening' && s !== 'off') { if (s !== 'hearing') { clearTimeout(V.snippetTimer); V.snippetTimer = null; V.snippet = ''; } }
    V.state = s; V.detail = detail || '';
    const label = {
      off: '', starting: 'Starting microphone…',
      listening: (V.awaiting && Date.now() < V.awaiting.until) ? 'Yes or no?' : (Date.now() < V.followUntil ? 'Yes?' : (V.mode === 'meeting' ? 'Listening for "Pulse…"' : 'Listening')),
      hearing: 'Hearing you…', transcribing: 'Working out what you said…', thinking: 'Thinking…', speaking: 'Speaking — tap to stop',
      error: detail || 'Voice unavailable', paused: detail || 'Paused',
    }[s] || s;
    if (pill) { pill.className = 'pv-pill ' + s + (V.on ? ' show' : '') + (s === 'listening' && detail && detail === V.snippet ? ' snippet' : ''); pill.querySelector('.pv-txt').textContent = (detail && s !== 'error' && s !== 'paused') ? detail : label; pill.title = detail || label; }
    if (modeEl) modeEl.classList.toggle('show', V.on);
    window.dispatchEvent(new CustomEvent('pulse:voice', { detail: { state: s, detail: V.detail, mode: V.mode } }));
  }

  // ── level detector (voice activity) — pure so it can be tested without a microphone ──
  // Adaptive: the floor follows quiet frames slowly, so an air-conditioner does not count as
  // speech, and a clip starts after two loud frames in a row and ends after `hang` ms of quiet.
  V.vad = function makeVad(opts) {
    const o = Object.assign({ frameMs: 50, hangMs: 700, ratio: 3, minLevel: 0.012, maxMs: MAX_CLIP_MS }, opts || {});
    const st = { floor: 0.01, loud: 0, quietMs: 0, speaking: false, ms: 0 };
    return {
      st,
      step(rms) {
        if (!st.speaking) st.floor = st.floor * 0.95 + Math.min(rms, st.floor * 2) * 0.05;   // only quiet frames teach the floor
        const thr = Math.max(o.minLevel, st.floor * o.ratio);
        const isLoud = rms > thr;
        if (!st.speaking) {
          st.loud = isLoud ? st.loud + 1 : 0;
          if (st.loud >= 2) { st.speaking = true; st.quietMs = 0; st.ms = 0; return 'start'; }
          return 'idle';
        }
        st.ms += o.frameMs;
        st.quietMs = isLoud ? 0 : st.quietMs + o.frameMs;
        if (st.quietMs >= o.hangMs || st.ms >= o.maxMs) { st.speaking = false; st.loud = 0; return 'end'; }
        return 'speaking';
      },
    };
  };

  // ── microphone session ──
  async function start() {
    if (V.on) return;
    V.on = true; V.lastError = '';
    setState('starting');
    if (!V.caps) {
      try { const r = await get('/pulse/voice/status'); V.caps = r.ok ? await r.json() : { stt: false, tts: false, error: r.status }; }
      catch (_) { V.caps = { stt: false, tts: false }; }
      if (V.caps && V.caps.wake_words) setWake(V.caps.wake_words);
      if (V.caps.error === 409) { setState('error', 'Turn Pulse on first'); V.on = false; return; }
    }
    V.useWebSpeech = !V.caps.stt && !!(window.SpeechRecognition || window.webkitSpeechRecognition);
    if (!navigator.mediaDevices?.getUserMedia && !V.useWebSpeech) { setState('error', 'This browser has no microphone access'); return; }
    if (V.useWebSpeech) return startWebSpeech();
    try {
      V.stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    } catch (e) {
      V.lastError = String(e && e.name || e);
      setState('error', /NotAllowed|Permission|Security/i.test(V.lastError) ? 'Microphone blocked — allow it in the address bar. Typing still works.' : 'No microphone found. Typing still works.');
      return;
    }
    if (!V.on) { releaseStream(); return; }   // switched off while the permission prompt was up
    try {
      V.ctx = new (window.AudioContext || window.webkitAudioContext)();
      if (V.ctx.state === 'suspended') { try { await V.ctx.resume(); } catch (_) {} }
      V.src = V.ctx.createMediaStreamSource(V.stream);
      V.analyser = V.ctx.createAnalyser(); V.analyser.fftSize = 1024; V.src.connect(V.analyser);
    } catch (e) { setState('error', 'Audio not available in this browser'); releaseStream(); return; }
    const buf = new Float32Array(V.analyser.fftSize);
    let vad = V.vad({ hangMs: V.mode === 'meeting' ? 1000 : 700 });
    let lastMode = V.mode;
    setState('listening');
    V.timer = setInterval(() => {
      if (!V.on || V.detectorOff) return;   // detectorOff: test knob, the fake microphone hums
      if (V.mode !== lastMode) { lastMode = V.mode; vad = V.vad({ hangMs: V.mode === 'meeting' ? 1000 : 700 }); }
      // Do not hear while Pulse is talking, or just after (speaker bleed). Transcribing and
      // thinking do NOT pause the microphone: in a room people keep talking, and a sentence
      // said while the previous clip is in flight must still be caught — clips queue up.
      if (V.state === 'speaking' || V.audio || Date.now() < V.pausedUntil) { if (vad.st.speaking) { vad.st.speaking = false; discardClip(); } return; }
      V.analyser.getFloatTimeDomainData(buf);
      let s = 0; for (let i = 0; i < buf.length; i++) s += buf[i] * buf[i];
      const rms = Math.sqrt(s / buf.length);
      const ev = vad.step(rms);
      if (ev === 'start') beginClip();
      else if (ev === 'end') endClip();
      if (ev === 'start') setState('hearing'); else if (ev === 'end') setState('transcribing');
    }, 50);
    document.addEventListener('visibilitychange', onVis);
  }
  function onVis() { if (!V.on) return; if (document.hidden) { V.pausedUntil = Infinity; setState('paused', 'Paused — tab in background'); } else { V.pausedUntil = Date.now() + 300; setState('listening'); } }
  function releaseStream() { try { V.stream && V.stream.getTracks().forEach(t => t.stop()); } catch (_) {} V.stream = null; try { V.ctx && V.ctx.close(); } catch (_) {} V.ctx = V.analyser = V.src = null; }
  function stop() {
    V.on = false;
    clearInterval(V.timer); V.timer = null;
    discardClip(); releaseStream(); stopSpeaking(); clearAwaiting(); V.queue = []; V.followUntil = 0;
    if (V.recog) { try { V.recog.onend = null; V.recog.stop(); } catch (_) {} V.recog = null; }
    document.removeEventListener('visibilitychange', onVis);
    setState('off');
  }

  function beginClip() {
    if (!V.stream || V.rec) return;
    const mime = ['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus', 'audio/mp4'].find(m => window.MediaRecorder && MediaRecorder.isTypeSupported(m)) || '';
    try {
      V.chunks = []; V.clipStart = Date.now();
      V.rec = new MediaRecorder(V.stream, mime ? { mimeType: mime, audioBitsPerSecond: 32000 } : undefined);
      V.rec.ondataavailable = (e) => { if (e.data && e.data.size) V.chunks.push(e.data); };
      V.rec.start(250);
    } catch (e) { V.rec = null; setState('error', 'Recording not supported here'); }
  }
  function discardClip() { if (V.rec) { try { V.rec.ondataavailable = null; V.rec.onstop = null; V.rec.stop(); } catch (_) {} } V.rec = null; V.chunks = []; if (V.on && V.state === 'hearing') setState('listening'); }
  function endClip() {
    const rec = V.rec; if (!rec) return;
    const dur = Date.now() - V.clipStart, mime = rec.mimeType || 'audio/webm';
    rec.onstop = () => {
      const blob = new Blob(V.chunks, { type: mime }); V.rec = null; V.chunks = [];
      if (dur < MIN_CLIP_MS || blob.size < 2000) { if (V.on && V.state === 'transcribing') setState('listening'); return; }    // a cough, a door
      V.queue.push({ blob, dur }); pump();
    };
    try { rec.stop(); } catch (_) { V.rec = null; if (V.on) setState('listening'); }
  }

  // One clip at a time through the transcriber, in order, while the microphone keeps recording.
  async function pump() {
    if (V.pumping) return; V.pumping = true;
    try { while (V.on && V.queue.length) { const { blob, dur } = V.queue.shift(); await transcribeAndHandle(blob, dur); } }
    finally { V.pumping = false; }
  }
  async function transcribeAndHandle(blob, durMs) {
    if (!V.on) return;
    V.busy = true; if (V.state !== 'speaking') setState('transcribing');
    try {
      const b64 = await new Promise((ok, no) => { const r = new FileReader(); r.onload = () => ok(String(r.result).split(',')[1] || ''); r.onerror = no; r.readAsDataURL(blob); });
      const r = await post('/pulse/transcribe', { audio: b64, mime: blob.type, duration_ms: durMs, mode: V.mode });
      const j = await r.json().catch(() => ({}));
      if (r.status === 503 && (window.SpeechRecognition || window.webkitSpeechRecognition)) { V.caps.stt = false; V.busy = false; stopMicOnly(); V.useWebSpeech = true; return startWebSpeech(); }
      if (r.status === 429) { V.busy = false; return setState('error', j.message || 'Voice time for today is used up'); }
      if (!r.ok) { V.busy = false; setState('listening', 'Did not catch that'); return; }
      V.busy = false;
      await handleUtterance(j.text || '');
    } catch (e) { V.busy = false; if (V.on) setState('listening'); }
  }
  function stopMicOnly() { clearInterval(V.timer); V.timer = null; discardClip(); releaseStream(); }

  // ── browser speech recognition fallback (no server key) ──
  function startWebSpeech() {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) { setState('error', 'Transcription is not set up'); return; }
    const rg = V.recog = new SR(); rg.continuous = true; rg.interimResults = false; rg.lang = 'en-AU';
    rg.onresult = (e) => { for (let i = e.resultIndex; i < e.results.length; i++) if (e.results[i].isFinal) handleUtterance(e.results[i][0].transcript); };
    rg.onerror = (e) => { if (e.error === 'not-allowed') setState('error', 'Microphone blocked — allow it in the address bar. Typing still works.'); };
    rg.onend = () => { if (V.on && V.recog === rg) { try { rg.start(); } catch (_) {} } };   // Chrome stops after a while; keep going
    try { rg.start(); setState('listening', (V.mode === 'meeting' ? 'Listening for "Pulse…"' : 'Listening') + ' (browser speech)'); } catch (e) { setState('error', 'Could not start speech recognition'); }
  }

  // ── what to do with words ──
  // Returns what was sent (or '' if ignored), so tests and the console can see the decision.
  async function handleUtterance(raw) {
    let text = String(raw || '').trim();
    if (!text) { if (V.on) setState('listening'); return ''; }
    V.log.push({ at: Date.now(), heard: text, mode: V.mode });
    if (V.log.length > 50) V.log.shift();
    if (V.awaiting && Date.now() < V.awaiting.until) return answerProposal(text);
    // "Pulse, stop" while an email is on its 60-second hold recalls it — no wake word needed.
    if (STOP.test(text) && window._pulseCards && window._pulseCards.held().length) {
      const held = window._pulseCards.held(); for (const tk of held) { try { await window._pulseCards.stop(tk); } catch (_) {} }
      speak(held.length > 1 ? 'Stopped — nothing was sent.' : 'Stopped — the email was not sent.'); return 'stop';
    }
    if (V.mode === 'meeting') {
      const follow = Date.now() < V.followUntil;
      if (WAKE.test(text)) text = text.replace(WAKE, '').trim();
      else if (MIDWAKE.test(text)) text = text.replace(MIDWAKE, '$1').trim();      // "…so, Pulse, what's the ETA?"
      else if (!follow) { if (V.on) setState('listening'); return ''; }           // not for Pulse
      V.followUntil = 0;
      if (!text) { V.followUntil = Date.now() + FOLLOW_MS; setState('listening', 'Yes?'); speak('Yes?'); return ''; }   // name alone: listen for the next sentence
    } else {
      text = text.replace(WAKE, '').trim();                                           // "Pulse, …" is fine in Solo too
    }
    if (AGAIN.test(text)) { if (V.lastSpoken) { speak(V.lastSpoken); } else if (V.on) setState('listening'); return 'again'; }
    if (text.length < 3) { if (V.on) setState('listening'); return ''; }
    text = text.charAt(0).toUpperCase() + text.slice(1);
    // Send through the normal path so the panel, history and trace all behave as for typing.
    const input = document.getElementById('pulse-input-display');
    if (!input || typeof window.pulseSend !== 'function') return '';
    window._pulseStyle = 'spoken';
    input.value = text;
    V.busy = true; setState('thinking');
    // pulseSend opens the panel for typed questions; a heard one must not. The call is
    // synchronous up to its first await, so the swap only covers that call.
    const tp = window.togglePulse; window.togglePulse = function () {};
    let p; try { p = window.pulseSend(); } finally { window.togglePulse = tp; }
    // Mark the user bubble as heard rather than typed.
    setTimeout(() => { const chat = document.getElementById('pulse-chat'); const u = chat && chat.querySelectorAll('.pulse-msg.user'); if (u && u.length) u[u.length - 1].classList.add('pv-heard'); }, 0);
    try { await p; } catch (_) {}
    // If a spoken reply arrived, speak() now owns busy/state; otherwise (error, empty) go back to listening.
    if (V.state === 'thinking') { V.busy = false; setState(V.on ? 'listening' : 'off'); }
    return text;
  }

  // ── spoken yes / no to a proposed action ──
  // The agent module renders the card and owns the token; this drives it from what was heard.
  window.addEventListener('pulse:actions', (e) => {
    const acts = (e.detail && e.detail.actions) || [];
    const prop = acts.find(a => a.kind === 'proposed');
    if (!prop || !V.on) return;
    clearAwaiting();
    const ms = V.mode === 'meeting' ? CONFIRM_MEETING_MS : CONFIRM_SOLO_MS;
    V.awaiting = { token: prop.token, summary: prop.summary, until: Date.now() + ms + 8000 /* clock starts after Pulse finishes asking */, timer: null };
    V.awaiting.timer = setTimeout(() => { if (V.awaiting && V.awaiting.token === prop.token) { const t = prop.token; clearAwaiting(); try { window._pulseCards.cancel(t); } catch (_) {} if (V.on) setState('listening', 'No answer — cancelled'); } }, ms + 8000);
  });
  // If someone taps the card instead, stop waiting for a voice answer.
  window.addEventListener('pulse:action-settled', (e) => { if (V.awaiting && e.detail && e.detail.token === V.awaiting.token) clearAwaiting(); });
  function clearAwaiting() { if (V.awaiting) { clearTimeout(V.awaiting.timer); V.awaiting = null; } }
  async function answerProposal(text) {
    const a = V.awaiting; const t = String(text || '').trim();
    if (YES.test(t)) {
      clearAwaiting(); V.busy = true; setState('thinking', 'Doing it…');
      try { const r = await window._pulseCards.confirm(a.token); V.busy = false; speak(r && r.receipt ? r.receipt + '.' : 'Done.'); }
      catch (e) { V.busy = false; speak('That did not work: ' + String(e && e.message || 'the action failed') + '.'); }
      return 'yes';
    }
    if (NO.test(t)) { clearAwaiting(); try { window._pulseCards.cancel(a.token); } catch (_) {} speak('Cancelled.'); return 'no'; }
    // Unclear: ask once more, keep waiting; a second unclear answer cancels.
    if (!a.retried) { a.retried = true; speak('Sorry — yes or no?'); return 'unclear'; }
    clearAwaiting(); try { window._pulseCards.cancel(a.token); } catch (_) {} speak('Leaving it, then.'); return 'no';
  }

  // ── speaking ──
  // Any spoken reply plays while Spoken is on — typed questions included. The agent module
  // fires pulse:spoken with the reply written for the ear.
  window.addEventListener('pulse:spoken', (e) => { const t = e.detail && e.detail.text; if (t && window._pulseStyle === 'spoken') speak(t); });

  async function speak(text) {
    text = String(text || '').trim(); if (!text) return;
    V.lastSpoken = text;
    stopSpeaking(true);
    V.busy = true; setState('speaking');
    const done = () => { V.audio = null; V.busy = false; V.pausedUntil = Date.now() + AFTER_SPEAK_MS; if (V.on) showSnippet(text); else setState('off'); };
    if (!V.caps) { try { const r = await get('/pulse/voice/status'); V.caps = r.ok ? await r.json() : { stt: false, tts: false }; } catch (_) { V.caps = { stt: false, tts: false }; } } if (V.caps && V.caps.wake_words) setWake(V.caps.wake_words);
    if (V.caps.tts) {
      try {
        const r = await post('/pulse/speak', { text }, 'audio/mpeg');
        if (r.status === 503) { V.caps.tts = false; }
        else if (!r.ok) { const j = await r.json().catch(() => ({})); if (r.status === 429) { setState('error', j.message || 'Voice time used up'); V.busy = false; setTimeout(() => V.on && setState('listening'), 2500); return; } throw new Error(j.message || 'speak failed'); }
        else {
          const blob = await r.blob();
          const a = V.audio = new Audio(URL.createObjectURL(blob));
          a.onended = () => { URL.revokeObjectURL(a.src); if (V.audio === a) { V.audio = null; done(); } };
          a.onerror = () => { if (V.audio === a) { V.audio = null; speakWithBrowser(text, done); } };
          await a.play().catch(() => { V.audio = null; speakWithBrowser(text, done); });
          return;
        }
      } catch (e) { console.warn('[Pulse voice] speak:', e.message || e); }
    }
    speakWithBrowser(text, done);
  }
  // After an answer, with the panel closed, the bar shows the first line of it for a while.
  function panelOpen() { const p = document.getElementById('pulse-panel'); return !!(p && p.classList.contains('open')); }
  function openPanel() { if (typeof window.togglePulse === 'function') window.togglePulse(true); const c = document.getElementById('pulse-chat'); if (c) c.scrollTop = c.scrollHeight; clearSnippet(); }
  function clearSnippet() { clearTimeout(V.snippetTimer); V.snippetTimer = null; V.snippet = ''; if (V.on && V.state === 'listening') setState('listening'); }
  function showSnippet(text) {
    if (panelOpen() || !text || V.awaiting) { setState('listening'); return; }
    const first = String(text).split(/(?<=[.!?])\s+/)[0] || text;
    V.snippet = first.length > 72 ? first.slice(0, 70).replace(/\s\S*$/, '') + '…' : first;
    setState('listening', V.snippet);
    clearTimeout(V.snippetTimer); V.snippetTimer = setTimeout(clearSnippet, SNIPPET_MS);
  }
  function speakWithBrowser(text, done) {
    if (!window.speechSynthesis) { done(); return; }
    try {
      const u = new SpeechSynthesisUtterance(text); u.lang = 'en-AU'; u.rate = 1.02;
      const vs = speechSynthesis.getVoices(); const v = vs.find(x => /en-AU/i.test(x.lang) && /female|Karen|Catherine/i.test(x.name)) || vs.find(x => /en-AU/i.test(x.lang)) || vs.find(x => /^en/i.test(x.lang));
      if (v) u.voice = v;
      u.onend = u.onerror = () => done();
      speechSynthesis.cancel(); speechSynthesis.speak(u);
      V.audio = { browser: true };
    } catch (_) { done(); }
  }
  function stopSpeaking(quiet) {
    if (V.audio) { try { if (V.audio.browser) speechSynthesis.cancel(); else { V.audio.pause(); V.audio.onended = null; URL.revokeObjectURL(V.audio.src); } } catch (_) {} V.audio = null; }
    if (!quiet) { V.busy = false; V.pausedUntil = Date.now() + AFTER_SPEAK_MS; setState(V.on ? 'listening' : 'off'); }
  }

  // ── #meet: the room laptop ──
  function meetRoute() {
    if (!/^#meet\b/i.test(location.hash)) return;
    setMode('meeting');
    if (typeof window.togglePulse === 'function') window.togglePulse(true);
    const spk = document.getElementById('pa-spoken');
    if (spk && !spk.classList.contains('on')) spk.click();    // the agent module flips style; our observer starts the mic
  }

  Object.assign(V, { start, stop, setMode, handleUtterance, speak, stopSpeaking, mount, openPanel, panelOpen, clearAwaiting });

  // Mount once the agent module has put the Spoken switch on the bar (it retries for ~10 s).
  let tries = 0; const t = setInterval(() => { if (mount()) { clearInterval(t); setTimeout(meetRoute, 300); } else if (++tries > 60) clearInterval(t); }, 250);
  window.addEventListener('hashchange', meetRoute);
})();
