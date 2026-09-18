/* ── VelOzity Pinpoint — tile polish v1 ──
   One shared stylesheet for the raised surfaces across every module.

   WHY A SEPARATE SHEET RATHER THAN EDITING EACH MODULE:
   the same shadow is declared, with small variations, in seven files. Editing all of them
   means seven chances to break a working screen for a purely visual change. This overrides
   by class name instead, so nothing existing is touched and removing this file restores the
   previous look exactly.

   SPECIFICITY: every module appends its own <style> at runtime, and a later sheet wins on
   equal specificity — which would make the result depend on module load order. Prefixing
   with `body ` raises specificity by one element, so these rules win regardless of when
   anything loads.

   ON GLASS: backdrop-filter only shows anything when there is content behind it to refract.
   Over a flat near-white page it reads as washed-out rather than glassy, so it is applied
   only to surfaces that genuinely sit over content — the modal overlays and the sticky
   header — and never to the tiles on the page background. */
;(function () {
  'use strict';

  if (document.getElementById('pp-polish')) return;

  const css = `
  :root{
    /* Two layers, not one. A tight contact shadow anchors the card to the page; a wide soft
       one gives it height. A single shadow can do one or the other, which is why the current
       tiles read as flat even though they have a shadow. */
    --pp-shadow-rest: 0 1px 2px rgba(16,18,27,.045), 0 4px 12px rgba(16,18,27,.055);
    --pp-shadow-hover: 0 2px 4px rgba(16,18,27,.06), 0 18px 38px rgba(16,18,27,.13);
    --pp-shadow-overlay: 0 8px 20px rgba(16,18,27,.10), 0 40px 80px rgba(16,18,27,.22);
    /* Decelerating curve: the card arrives quickly then settles, which reads as responding
       rather than animating. A linear or ease curve feels mechanical at this distance. */
    --pp-ease: cubic-bezier(.22,1,.36,1);
  }

  body .fwh-kpi, body .fwh-card,
  body .eg-tile, body .eg-card,
  body .aqi-tile, body .aqi-card,
  body .si-tile, body .si-card,
  body .ehp-kpi, body .ehp-card,
  body .aq-tile, body .fin-card{
    /* THE SURFACE ITSELF, not what shows through it.
       The back of an iPhone is not transparent — it is a material: a faint vertical gradient,
       a bright highlight along the top edge where light catches, a darker lip at the bottom,
       and a hairline rim. All of that is drawn on the element and needs nothing behind it,
       which is why backdrop-filter was the wrong tool for this.

       Layered inset shadows, outermost first:
         top hairline    a 1px white highlight, the lit edge
         bottom hairline a barely-there dark line, the shaded edge
         inner glow      a wide soft white bloom so the centre reads as slightly domed */
    /* Three layers, outermost first:
         1. SPECULAR SHEEN — an angled band of light across the upper left. This is the layer
            that reads as glass; a vertical fade alone never will. Sized at 220% so there is
            room for it to travel on hover.
         2. AMBIENT — a soft radial bloom from the top-left corner, the light source.
         3. BODY — the base gradient. The first attempt ran white to #f7f8fa, a 3% value
            range, which is below the threshold of visibility. This spans roughly 8% and
            cools slightly toward the bottom, the way a polished surface picks up sky. */
    background-image:
      linear-gradient(115deg,
        rgba(255,255,255,0) 28%,
        rgba(255,255,255,.75) 42%,
        rgba(255,255,255,.95) 48%,
        rgba(255,255,255,.75) 54%,
        rgba(255,255,255,0) 68%),
      radial-gradient(120% 90% at 8% 0%, rgba(255,255,255,.95) 0%, rgba(255,255,255,0) 60%),
      linear-gradient(168deg, #ffffff 0%, #f8f9fb 52%, #eef1f6 100%);
    background-size: 220% 220%, 100% 100%, 100% 100%;
    background-position: 100% 0%, 0% 0%, 0% 0%;
    background-repeat: no-repeat;
    box-shadow:
      /* Rim light: brightest along the top where the light lands, carried faintly down the
         sides, with a shaded lip beneath. This is the edge of the glass. */
      inset 0 1px 0 rgba(255,255,255,1),
      inset 1px 0 0 rgba(255,255,255,.65),
      inset -1px 0 0 rgba(255,255,255,.5),
      inset 0 -1px 0 rgba(16,18,27,.055),
      inset 0 18px 30px -20px rgba(255,255,255,1),
      var(--pp-shadow-rest);
    border-color: rgba(16,18,27,.08);
    transition: box-shadow .28s var(--pp-ease),
                transform .28s var(--pp-ease),
                border-color .28s var(--pp-ease),
                /* Slower than the lift so the highlight trails the movement — light does
                   not snap to a new position, and that lag is most of the effect. */
                background-position .55s var(--pp-ease);
    /* Deliberately NO will-change. It promotes every element to its own compositor layer,
       and with twenty-odd tiles on the Week Hub that costs more memory than the hint saves.
       The browser handles a transform transition on a handful of hovered cards perfectly
       well without it. */
  }

  /* The lift and the shadow grow together. Moving the card without growing the shadow is
     what makes a hover feel like a glitch rather than a gesture. */
  body .fwh-kpi:hover, body .fwh-card:hover,
  body .eg-tile:hover, body .eg-card:hover,
  body .aqi-tile:hover, body .aqi-card:hover,
  body .si-tile:hover, body .si-card:hover,
  body .ehp-kpi:hover, body .ehp-card:hover,
  body .aq-tile:hover, body .fin-card:hover{
    transform: translateY(-6px);
    /* The sheen SWEEPS across as the card rises — the surface turning in the light. A static
       highlight under a moving card reads as a picture of glass; a travelling one reads as
       the thing itself. */
    background-position: 0% 0%, 0% 0%, 0% 0%;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,1),
      inset 1px 0 0 rgba(255,255,255,.8),
      inset -1px 0 0 rgba(255,255,255,.65),
      inset 0 -1px 0 rgba(16,18,27,.06),
      inset 0 22px 36px -20px rgba(255,255,255,1),
      var(--pp-shadow-hover);
    border-color: rgba(16,18,27,.14);
  }

  /* A pressed card should return most of the way, not all of it — otherwise the click feels
     like the hover was cancelled. */
  body .eg-tile:active, body .aqi-tile:active, body .si-tile:active, body .aq-tile:active{
    transform: translateY(-2px);
    transition-duration: .09s;
  }

  body .eg-tile:disabled, body .eg-tile:disabled:hover{
    transform: none; box-shadow: var(--pp-shadow-rest);
  }

  /* GLASS — only .ehp-panel qualifies.
     Checked each candidate rather than applying it by name:
       .ehp-panel  floats at min(1180px,97vw) inside a dimmed overlay  -> real content behind
       .aqi-panel  width:100%, height:100% — fills the screen          -> nothing behind
       .eg-panel   width:100%, height:100% — fills the screen          -> nothing behind
       .fin-panel  a fixed slide-out drawer over the page background   -> nothing behind, and
                   its open/close animation is a transform this must not disturb
     Blurring a surface with nothing behind it produces washed-out translucency, not glass. */
  body .ehp-panel{
    box-shadow: var(--pp-shadow-overlay);
    background: rgba(255,255,255,.82);
    -webkit-backdrop-filter: saturate(180%) blur(24px);
    backdrop-filter: saturate(180%) blur(24px);
    border: .5px solid rgba(255,255,255,.6);
  }

  /* Without backdrop-filter an 82% panel over a dimmed page is unreadable. Solid, not
     translucent, is the correct fallback. */
  @supports not ((backdrop-filter: blur(1px)) or (-webkit-backdrop-filter: blur(1px))){
    body .ehp-panel{ background:#fff; }
  }

  /* The full-screen panels get the depth without the blur — there is nothing behind them to
     refract, so glass would only cost contrast. */
  body .aqi-panel, body .eg-panel{ box-shadow: var(--pp-shadow-overlay); }

  /* Reduced motion: keep the depth, drop the movement. The shadow still communicates the
     hover, so nothing is lost but the travel. */
  @media (prefers-reduced-motion: reduce){
    body .fwh-kpi, body .fwh-card,
    body .eg-tile, body .eg-card, body .aqi-tile, body .aqi-card,
    body .si-tile, body .si-card, body .ehp-kpi, body .ehp-card,
    body .aq-tile, body .fin-card{ transition: box-shadow .2s ease, border-color .2s ease; }
    body .fwh-kpi:hover, body .fwh-card:hover,
    body .eg-tile:hover, body .eg-card:hover, body .aqi-tile:hover, body .aqi-card:hover,
    body .si-tile:hover, body .si-card:hover, body .ehp-kpi:hover, body .ehp-card:hover,
    body .aq-tile:hover, body .fin-card:hover{
      transform: none;
      /* The sheen has to be pinned too. With the transition removed it would jump to the
         new position instantly, which is more jarring than the animation it replaced. */
      background-position: 100% 0%, 0% 0%, 0% 0%;
    }
  }

  /* Touch devices have no hover state, and a sticky :hover left behind after a tap looks
     broken. */
  @media (hover: none){
    body .fwh-kpi:hover, body .fwh-card:hover,
    body .eg-tile:hover, body .eg-card:hover, body .aqi-tile:hover, body .aqi-card:hover,
    body .si-tile:hover, body .si-card:hover, body .ehp-kpi:hover, body .ehp-card:hover,
    body .aq-tile:hover, body .fin-card:hover{
      transform: none;
      /* A tap leaves :hover stuck on touch devices. Without pinning the sheen, the tile
         keeps a displaced highlight until something else is tapped. */
      background-position: 100% 0%, 0% 0%, 0% 0%;
    }
  }
  `;

  const el = document.createElement('style');
  el.id = 'pp-polish';
  el.textContent = css;
  document.head.appendChild(el);
  console.log('[tile-polish] v1 loaded');
})();
