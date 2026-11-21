/* ── VelOzity Pinpoint — Pulse voice v1 (Phase 2) ──
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

   FALLBACKS
   If the server has no transcription key it says so once (503) and the browser's own speech
   recognition takes over; if it has no voice key, the browser's own voice reads the answer.
   Either way the panel still shows every word as text, so nothing is lost when audio is not
   possible — a muted laptop in a meeting still gets its answers on screen.

   Test hook: window._pulseVoice (state, start, stop, setMode, handleUtterance, speak, vad). */
;(function () {
  'use strict';

  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2', DARK = '#1C1C1E', OK = '#1F7A4D', BAD = '#B42318';
  const WAKE = /^\s*(?:hey|hi|ok|okay|hello)?[\s,]*(?:pulse|paulse|pals|pulls|pause)\b[\s,.:!?-]*/i;   // what transcribers make of "Pulse"
  const AGAIN = /^(?:pulse[\s,]*)?(?:say that again|repeat that|say again|what was that|pardon|come again)\W*$/i;
  const MIN_CLIP_MS = 450, MAX_CLIP_MS = 20000, AFTER_SPEAK_MS = 350;

  const css = `
    .pv-mode{display:none;align-items:center;border:.5px solid rgba(0,0,0,.14);border-radius:8px;overflow:hidden;font-size:10.5px;color:${MID};margin-right:8px;user-select:none;}
    .pv-mode span{padding:3px 8px;cursor:pointer;line-height:1;} .pv-mode span.on{background:${BRAND};color:#fff;}
    .pv-mode.show{display:inline-flex;}
    .pv-pill{display:none;align-items:center;gap:7px;font-size:11px;color:${MID};padding:3px 10px 3px 7px;border-radius:12px;border:.5px solid rgba(0,0,0,.1);background:#fff;white-space:nowrap;margin-right:8px;max-width:260px;overflow:hidden;text-overflow:ellipsis;}
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
    busy: false, audio: null, lastSpoken: '', useWebSpeech: false, recog: null, pausedUntil: 0, lastError: '',
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
    pill.onclick = () => { if (V.state === 'speaking') stopSpeaking(); };
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
    V.state = s; V.detail = detail || '';
    const label = {
      off: '', starting: 'Starting microphone…',
      listening: V.mode === 'meeting' ? 'Listening for "Pulse…"' : 'Listening',
      hearing: 'Hearing you…', transcribing: 'Working out what you said…', thinking: 'Thinking…', speaking: 'Speaking — tap to stop',
      error: detail || 'Voice unavailable', paused: detail || 'Paused',
    }[s] || s;
    if (pill) { pill.className = 'pv-pill ' + s + (V.on ? ' show' : ''); pill.querySelector('.pv-txt').textContent = (detail && s !== 'error' && s !== 'paused') ? detail : label; pill.title = detail || label; }
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
      // Do not hear while Pulse is talking/thinking, or just after (speaker bleed).
      if (V.busy || V.state === 'speaking' || Date.now() < V.pausedUntil) { if (vad.st.speaking) { vad.st.speaking = false; discardClip(); } return; }
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
    discardClip(); releaseStream(); stopSpeaking();
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
    rec.onstop = async () => {
      const blob = new Blob(V.chunks, { type: mime }); V.rec = null; V.chunks = [];
      if (dur < MIN_CLIP_MS || blob.size < 2000) { if (V.on) setState('listening'); return; }    // a cough, a door
      await transcribeAndHandle(blob, dur);
    };
    try { rec.stop(); } catch (_) { V.rec = null; if (V.on) setState('listening'); }
  }

  async function transcribeAndHandle(blob, durMs) {
    if (!V.on) return;
    V.busy = true; setState('transcribing');
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
    if (V.mode === 'meeting') {
      if (!WAKE.test(text)) { if (V.on) setState('listening'); return ''; }   // not for Pulse
      text = text.replace(WAKE, '').trim();
      if (!text) { if (V.on) setState('listening', 'Yes?'); return ''; }
    } else {
      text = text.replace(WAKE, (m) => (/^\s*(?:hey|hi|ok|okay|hello)?[\s,]*pulse/i.test(m) ? '' : m)).trim();   // "Pulse, …" is fine in Solo too
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
    const p = window.pulseSend();
    // Mark the user bubble as heard rather than typed.
    setTimeout(() => { const chat = document.getElementById('pulse-chat'); const u = chat && chat.querySelectorAll('.pulse-msg.user'); if (u && u.length) u[u.length - 1].classList.add('pv-heard'); }, 0);
    try { await p; } catch (_) {}
    // If a spoken reply arrived, speak() now owns busy/state; otherwise (error, empty) go back to listening.
    if (V.state === 'thinking') { V.busy = false; setState(V.on ? 'listening' : 'off'); }
    return text;
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
    const done = () => { V.audio = null; V.busy = false; V.pausedUntil = Date.now() + AFTER_SPEAK_MS; if (V.on) setState('listening'); else setState('off'); };
    if (!V.caps) { try { const r = await get('/pulse/voice/status'); V.caps = r.ok ? await r.json() : { stt: false, tts: false }; } catch (_) { V.caps = { stt: false, tts: false }; } }
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

  Object.assign(V, { start, stop, setMode, handleUtterance, speak, stopSpeaking, mount });

  // Mount once the agent module has put the Spoken switch on the bar (it retries for ~10 s).
  let tries = 0; const t = setInterval(() => { if (mount()) { clearInterval(t); setTimeout(meetRoute, 300); } else if (++tries > 60) clearInterval(t); }, 250);
  window.addEventListener('hashchange', meetRoute);
})();
