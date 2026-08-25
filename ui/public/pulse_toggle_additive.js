/* ── VelOzity Pinpoint — Pulse on/off v1 ──
   Pulse costs money per call, and several callers fire automatically: the Week Hub header
   flipper generates five insights on every week load, the Executive page pulls its own, and
   the finance panel has its own analysis. None of that was asked for by a user — it just
   happens, and it happens every page load.

   This gates all of it behind an explicit switch that is OFF every time Pinpoint loads.

   WHY A FETCH WRAPPER RATHER THAN EDITING EACH CALLER:
   the AI callers are spread across index.html inline script and four additive modules. A
   guard added to each one is a guard that can be forgotten by the next caller. Intercepting
   at the network boundary means anything new is covered by default — including code written
   after this file — and no existing module needs touching.

   It also attaches the x-pulse-enabled header the server now requires, so the browser and
   the server agree on a single source of truth rather than trusting the UI alone. */
;(function () {
  'use strict';

  const BRAND = '#990033', MID = '#6E6E73', LIGHT = '#AEAEB2';

  // Deliberately not persisted. "Default off on load" means every session starts closed —
  // remembering it across reloads would quietly reintroduce the charges this exists to stop.
  let _on = false;

  // Endpoints that reach the model. Matched on the path, so query strings do not matter.
  const AI_PATHS = [
    '/pulse/chat',
    '/ai/pulse',
    '/finance/insights',
    '/report/cost-utilisation/insights',
  ];
  const isAiUrl = (url) => {
    try {
      const p = new URL(String(url), location.origin).pathname;
      return AI_PATHS.some(a => p === a || p.endsWith(a));
    } catch (e) { return AI_PATHS.some(a => String(url).indexOf(a) >= 0); }
  };

  // ── network gate ──
  const _origFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    let url = '';
    try { url = typeof input === 'string' ? input : (input && input.url) || ''; } catch (e) {}
    if (!isAiUrl(url)) return _origFetch(input, init);

    if (!_on) {
      // Resolve rather than reject: callers vary in how they handle failure, and a rejected
      // promise surfaces as a red error somewhere in the UI. A clean 409 with a JSON body
      // reads as "switched off", which is what it is.
      return Promise.resolve(new Response(
        JSON.stringify({ error: 'pulse_off', message: 'Pulse is switched off.' }),
        { status: 409, headers: { 'Content-Type': 'application/json' } }));
    }
    // Merge onto whatever the caller passed. A Request object carries its own method and
    // body, so it is rebuilt rather than reduced to a URL — passing input.url alone would
    // quietly turn a POST into a GET.
    if (typeof input === 'string') {
      const o = init ? { ...init } : {};
      const h = new Headers(o.headers || {});
      h.set('x-pulse-enabled', '1');
      o.headers = h;
      return _origFetch(input, o);
    }
    try {
      const r = new Request(input, init || undefined);
      r.headers.set('x-pulse-enabled', '1');
      return _origFetch(r);
    } catch (e) { return _origFetch(input, init); }
  };

  // ── toggle UI ──
  function styles() {
    if (document.getElementById('ptg-styles')) return;
    const s = document.createElement('style'); s.id = 'ptg-styles';
    s.textContent = `
      #ptg-wrap{display:inline-flex;align-items:center;gap:7px;margin-right:10px;flex:0 0 auto;}
      #ptg-sw{position:relative;width:34px;height:19px;border-radius:20px;border:none;cursor:pointer;
              background:rgba(0,0,0,.16);transition:background .18s;flex:0 0 auto;padding:0;}
      #ptg-sw.on{background:${BRAND};}
      #ptg-sw span{position:absolute;top:2px;left:2px;width:15px;height:15px;border-radius:50%;
                   background:#fff;transition:transform .18s;box-shadow:0 1px 3px rgba(0,0,0,.2);}
      #ptg-sw.on span{transform:translateX(15px);}
      #ptg-lbl{font-size:10px;font-weight:600;color:${LIGHT};letter-spacing:.04em;
               text-transform:uppercase;white-space:nowrap;}
      #ptg-wrap.on #ptg-lbl{color:${BRAND};}
      #ptg-hint{font-size:10px;color:${LIGHT};margin-left:2px;white-space:nowrap;}
      @media (max-width:900px){ #ptg-hint{display:none;} }
    `;
    document.head.appendChild(s);
  }

  function paint() {
    const w = document.getElementById('ptg-wrap'), sw = document.getElementById('ptg-sw');
    const lbl = document.getElementById('ptg-lbl'), hint = document.getElementById('ptg-hint');
    if (!w || !sw) return;
    w.classList.toggle('on', _on);
    sw.classList.toggle('on', _on);
    sw.setAttribute('aria-checked', _on ? 'true' : 'false');
    if (lbl) lbl.textContent = _on ? 'Pulse on' : 'Pulse off';
    if (hint) hint.textContent = _on ? '' : 'switch on to use AI';
    // The input placeholder is the clearest signal that typing will do nothing.
    const inp = document.querySelector('#pulse-bar input, .pulse-input-wrap input');
    if (inp) {
      if (!inp.dataset.ptgPh) inp.dataset.ptgPh = inp.placeholder || '';
      inp.placeholder = _on ? inp.dataset.ptgPh : 'Pulse is off — switch it on to ask a question';
      inp.disabled = !_on;
      inp.style.opacity = _on ? '' : '.6';
    }
  }

  function mount() {
    const bar = document.getElementById('pulse-bar');
    if (!bar || document.getElementById('ptg-wrap')) return;
    styles();
    const w = document.createElement('div'); w.id = 'ptg-wrap';
    w.innerHTML = `<button id="ptg-sw" role="switch" aria-checked="false"
        title="Pulse uses the Claude API and is charged per request. Off by default each time Pinpoint loads."><span></span></button>
      <span id="ptg-lbl">Pulse off</span><span id="ptg-hint"></span>`;
    // Ahead of the Pulse label so the switch reads as governing everything to its right.
    const first = bar.firstElementChild;
    if (first) bar.insertBefore(w, first); else bar.appendChild(w);
    document.getElementById('ptg-sw').addEventListener('click', () => {
      _on = !_on;
      paint();
      // Let anything that suppressed itself while off have a chance to run now.
      if (_on) window.dispatchEvent(new CustomEvent('pulse:enabled'));
    });
    paint();
  }

  window.pulseIsOn = () => _on;

  function init() {
    styles(); mount();
    // The bar is rendered by the host page and may not exist yet on a cold load.
    let tries = 0;
    const t = setInterval(() => { mount(); if (document.getElementById('ptg-wrap') || ++tries > 40) clearInterval(t); }, 300);
    console.log('[pulse-toggle] v2 loaded — Pulse OFF by default');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
