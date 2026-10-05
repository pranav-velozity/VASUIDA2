/* ── VelOzity Pinpoint — Transit Movements v1 (transit_movements_additive.js) ──
   Replaces map_live_additive.js. Same entry points — window.showMapPage / hideMapPage and
   section#page-map — so the page routing in index.html is untouched.

   Why it replaced the map. Every movement runs the same corridor, so a world map spent most of
   its pixels on ocean to show one arc, and it read its dates from the lane: a lane on no
   container still carrying a typed departure drew a ship that did not exist. Here time is the
   axis, every date comes from the consignment, and a lane on no movement is named rather than
   placed.

   What it reads, all of it from GET /consignments/board plus /alerts per live week:
     · consignment milestones — planned, actual, and who said so (a person or tracking)
     · first-promised FC date and the per-leg original plan, recomputed server-side
     · tracking facts: carrier ETA/ETD and their revisions, last free day, holds, ports
     · plan rows for the PO / SKU / unit counts, and processed units for the contents sheet

   Status is one reading, made on the server, against the first promise: within a day on
   time, 2–4 days late behind, more than 4 delayed, and held at the terminal with the last
   free day two days out or less is delayed whatever the dates say.

   Nothing here names the tracking provider. It is presented as Pinpoint tracking. */
(function () {
  'use strict';
  if (window.__TM_LOADED__) return;
  window.__TM_LOADED__ = true;

  // ── Palette (Pinpoint status colours) ──
  const INK = '#121212', MUTED = '#5F5F5F', LINE = '#E3E3E0', SOFT = '#EFEFEC';
  const GREEN = '#C7EA46', AMBER = '#F5BD25', RED = '#990033', GREY = '#8A8A8A';
  const STATUS = {
    on_time:    { c: GREEN, l: 'On time' },
    behind:     { c: AMBER, l: 'Behind' },
    delayed:    { c: RED,   l: 'Delayed' },
    not_booked: { c: GREY,  l: 'Not booked', hollow: true },
    no_quote:   { c: GREY,  l: 'No quote', hollow: true },
  };
  const STAGES = ['packing_list_ready', 'origin_cleared', 'departed', 'arrived', 'dest_cleared', 'fc_receipt'];
  const STAGE_LABEL = {
    packing_list_ready: 'Packing list ready', origin_cleared: 'Origin cleared', departed: 'Departed',
    arrived: 'Arrived', dest_cleared: 'Destination cleared', fc_receipt: 'Received at FC',
  };
  const STAGE_VERB = {
    packing_list_ready: 'packing list', origin_cleared: 'origin clearance', departed: 'departure',
    arrived: 'arrival', dest_cleared: 'destination clearance', fc_receipt: 'FC receipt',
  };
  const LIVE_WEEKS_BACK = 5;      // a movement older than this with no FC receipt is history, not live
  const EARLIER_WEEKS = 10;       // matches the reports' "last 10 weeks"
  const N_DAYS = 35;              // timeline span: two weeks back from this Monday, three ahead
  const REFRESH_MS = 5 * 60 * 1000;

  // ── Small utilities ──
  const esc = (v) => String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const MO = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const ymd = (v) => v ? String(v).slice(0, 10) : null;
  const toD = (s) => new Date(String(s).slice(0, 10) + 'T00:00:00Z');
  const addD = (s, n) => { const d = toD(s); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
  const diff = (a, b) => (a && b) ? Math.round((toD(b) - toD(a)) / 86400000) : null;
  const fmtDay = (s) => { if (!s) return '—'; const d = toD(s); if (isNaN(d)) return String(s); return `${WD[d.getUTCDay()]} ${d.getUTCDate()} ${MO[d.getUTCMonth()]}`; };
  const fmtShort = (s) => { if (!s) return '—'; const d = toD(s); if (isNaN(d)) return String(s); return `${d.getUTCDate()} ${MO[d.getUTCMonth()]}`; };
  const fmtNum = (n) => (n == null || isNaN(n)) ? '—' : Number(n).toLocaleString('en-AU');
  const plural = (n, w, p) => `${n} ${n === 1 ? w : (p || w + 's')}`;
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const mondayOf = (s) => { const d = toD(s); const dow = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - dow); return d.toISOString().slice(0, 10); };
  const isoWeek = (s) => {
    const d = toD(s); const day = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - day + 3);
    const firstThu = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
    const w = 1 + Math.round(((d - firstThu) / 86400000 - 3 + ((firstThu.getUTCDay() + 6) % 7)) / 7);
    return 'W' + w;
  };
  const todayLocal = () => {
    try { return new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' }).format(new Date()); }
    catch (_) { return new Date().toISOString().slice(0, 10); }
  };
  const timeLabel = (iso) => {
    if (!iso) return '';
    const d = new Date(iso); if (isNaN(d)) return '';
    const opts = { timeZone: 'Australia/Sydney', hour: '2-digit', minute: '2-digit', hour12: false };
    const sameDay = new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' }).format(d) === todayLocal();
    const t = new Intl.DateTimeFormat('en-AU', opts).format(d);
    if (sameDay) return t;
    const day = new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' }).format(d);
    return `${fmtShort(day)} ${t}`;
  };
  const EMAIL_RE = /^[^\s@<>,;]+@[^\s@<>,;]+\.[^\s@<>,;]+$/;

  // ── API ──
  const apiBase = () => ((document.querySelector('meta[name="api-base"]') || {}).content || '').replace(/\/+$/, '');
  async function token() {
    try { return (window.Clerk && window.Clerk.session) ? await window.Clerk.session.getToken() : null; } catch (_) { return null; }
  }
  async function req(method, path, body, opts) {
    const t = await token();
    const h = {};
    if (body) h['Content-Type'] = 'application/json';
    if (t) h.Authorization = 'Bearer ' + t;
    if (window.pinpointClient) h['x-pinpoint-client'] = window.pinpointClient;
    let r, lastErr = null;
    const tries = method === 'GET' ? 3 : 1;          // never retry a write
    for (let i = 0; i < tries; i++) {
      if (i) await new Promise(res => setTimeout(res, 500 * i));
      try {
        r = await fetch(apiBase() + path, { method, headers: h, body: body ? JSON.stringify(body) : undefined });
        lastErr = null;
        if (method === 'GET' && [502, 503, 504].includes(r.status)) { lastErr = new Error('HTTP ' + r.status); continue; }
        break;
      } catch (e) { lastErr = e; }
    }
    if (lastErr) throw lastErr;
    if (opts && opts.raw) {
      if (!r.ok) { let d = null; try { d = await r.json(); } catch (_) {} throw new Error((d && (d.message || d.error)) || ('HTTP ' + r.status)); }
      return r;
    }
    let d = null; try { d = await r.json(); } catch (_) { d = null; }
    if (!r.ok) {
      const e = new Error((d && (d.message || d.error)) || ('HTTP ' + r.status));
      e.status = r.status; e.data = d; throw e;
    }
    return d;
  }
  async function download(path, fallbackName) {
    const r = await req('GET', path, null, { raw: true });
    const blob = await r.blob();
    const cd = r.headers.get('content-disposition') || '';
    const m = cd.match(/filename="?([^";]+)"?/);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = (m && m[1]) || fallbackName;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }

  // ── Who is looking ──
  const clerkRole = () => { try { return ((window.Clerk.user.organizationMemberships || [])[0] || {}).role || ''; } catch (_) { return ''; } };
  const isInternal = () => window.pinpointIsInternal === true;
  // Confirming a date is open to VelOzity staff and the facility; a client reads.
  const canEdit = () => isInternal() || clerkRole() === 'org:supplier_auth';

  // ── State ──
  const S = {
    root: null, loading: false, error: null, board: null, alerts: [], loadedAt: 0,
    mode: 'all', view: null, viewTouched: false, corrWk: 'all', corrMore: false, full: false,
    collapsed: {}, panel: null, sel: null, wkSel: null,
    sheet: null, sheetLane: 0, contents: {}, contentsErr: {},
    notify: null, nTo: [], nDraft: '', nErr: '', nSubject: null, nBody: null, nAttach: true, nSending: false, nRecipients: null,
    amend: null, busy: null, legacy: null,
    toast: '', toastTimer: null, playedReplay: false, phase: 'now', timer: null,
  };

  // ════ Model ════
  // Everything the screen draws, derived once per render from the board and the alerts.
  function msMap(c) {
    const out = {};
    for (const m of (c.milestones || [])) out[m.stage] = m;
    return out;
  }
  const dateOf = (m) => m ? (m.actual_at || m.planned_at || null) : null;

  function refOf(c) {
    if (c.reference) return c.reference;
    return c.mode === 'Air' ? 'AWB pending' : 'Container not advised';
  }
  function shortRef(c) {
    const r = c.reference;
    if (!r) return c.mode === 'Air' ? 'AWB tbc' : (c.size_ft ? `${c.size_ft}′ tbc` : 'Box tbc');
    let s = String(r).replace(/\s+/g, '');
    if (s.includes('/')) s = s.split('/')[0];          // CA1306/25SEP → CA1306
    if (s.length <= 9) return s;
    return /^[A-Z]{4}\d/.test(s) ? `${s.slice(0, 4)}…${s.slice(-3)}` : `${s.slice(0, 3)}…${s.slice(-4)}`;
  }
  const portLabel = (p) => p ? (p.name || p.code || '') : '';

  function toMovement(c, today) {
    const ms = msMap(c);
    const air = c.mode === 'Air';
    const d = (st) => ymd(dateOf(ms[st]));
    const dep = ms.departed || {}, arr = ms.arrived || {}, fcm = ms.fc_receipt || {};
    const delivered = !!fcm.actual_at;
    const health = c.health || { key: 'on_time', label: 'On time', why: '' };
    const term = c.terminal || null;
    const held = !!(health.held);
    const route = c.route || {};
    const cs = c.contents_summary || {};
    const lanesN = (c.lanes || []).length;

    // Where it is along its route, 0 at the origin and 1 at the destination.
    // Actual dates first. Where nobody has confirmed one, the plan places it — a flight
    // booked for last week has almost certainly flown — and the pill is drawn dashed so a
    // planned position never passes for a confirmed one.
    const depReal = ymd(dep.actual_at), arrReal = ymd(arr.actual_at);
    const depAt = depReal || (d('departed') && d('departed') <= today ? d('departed') : null);
    const progWith = (arrDate) => {
      if (arrReal || delivered) return 1;
      if (!depAt) return 0;
      const a = arrDate || d('arrived');
      if (!depReal && a && a <= today) return 1;
      const span = diff(depAt, a);
      if (!span || span <= 0) return 0.5;
      return clamp(diff(depAt, today) / span, 0.05, 0.95);
    };
    const assumedPos = !delivered && !arrReal && !!((depAt && !depReal) || (!depReal && d('arrived') && d('arrived') <= today));
    const changed = c.changed_24h || {};
    const prog = progWith(null);
    const progB = ('carrier_eta' in changed && changed.carrier_eta) ? progWith(changed.carrier_eta) : prog;

    // In words, for the row and the pill.
    let where;
    const destName = portLabel(route.dest) || (air ? 'Sydney' : 'port');
    const originName = portLabel(route.origin) || 'origin';
    if (delivered) where = `Received ${fmtDay(fcm.actual_at)}`;
    else if (held) where = `Held at ${term && term.terminal ? term.terminal : destName}`;
    else if (ms.dest_cleared && ms.dest_cleared.actual_at) where = `Cleared · FC ${fmtDay(d('fc_receipt'))}`;
    else if (arr.actual_at) where = `At ${destName} · clearing`;
    else if (dep.actual_at) {
      const left = diff(today, d('arrived'));
      if (left == null) where = air ? 'In the air' : 'At sea';
      else if (left > 0) where = `${air ? 'In the air' : 'At sea'} · ${plural(left, 'day')} out`;
      else if (left === 0) where = 'Arriving today';
      else where = 'Arrival not yet confirmed';
    } else if (!c.reference) where = air ? 'Not yet booked' : 'Container not advised';
    else {
      const until = diff(today, d('departed'));
      if (until == null) where = `At ${originName}`;
      else if (until > 0) where = `${air ? 'Flies' : 'Departs'} ${fmtDay(d('departed'))}`;
      else if (until === 0) where = `${air ? 'Flies' : 'Departs'} today`;
      else where = 'Departure not yet confirmed';
    }
    const subBits = [air ? 'Air' : (c.size_ft ? `${String(c.size_ft).replace(/ft$/i, '')}′` : 'Sea'), c.carrier_name || (c.reference ? null : 'carrier not quoted'),
      plural(lanesN, 'lane'), cs.pos != null ? plural(cs.pos, 'PO') : null].filter(Boolean);
    return {
      c, uid: c.consignment_uid, air, mode: air ? 'air' : 'sea', ref: refOf(c), short: shortRef(c),
      wk: c.week_start, wkLabel: isoWeek(c.week_start), ms, d, delivered, health, st: health.key, held, term, route,
      lanes: lanesN, pos: cs.pos || 0, skus: cs.skus || 0, units: cs.planned || 0,
      fc: d('fc_receipt'), base: ymd(c.baseline_fc_at), basePlan: c.baseline_plan || null,
      changed, prog, progB, assumedPos, where, sub: subBits.join(' · '),
      routeText: (route.origin || route.dest) ? `${air ? 'Air' : 'Sea'} · ${originName} → ${destName}` : `${air ? 'Air' : 'Sea'} · port not yet known`,
      live: false,
    };
  }

  function model() {
    const B = S.board;
    if (!B) return null;
    const today = B.today || todayLocal();
    const thisWeek = B.this_week || mondayOf(today);
    const liveFloor = addD(thisWeek, -7 * LIVE_WEEKS_BACK);
    const all = (B.consignments || []).map(c => toMovement(c, today));
    for (const m of all) m.live = !m.delivered && m.c.status !== 'closed' && m.wk >= liveFloor && m.wk <= addD(thisWeek, 7);
    const byMode = (m) => S.mode === 'all' || m.mode === S.mode;

    // Weeks with something still moving: the timeline shows every movement of those weeks,
    // delivered ones included, so a week reads whole.
    const liveWeeks = [...new Set(all.filter(m => m.live).map(m => m.wk))].sort();
    const timeline = all.filter(m => liveWeeks.includes(m.wk) && byMode(m))
      .sort((a, b) => a.wk.localeCompare(b.wk) || (a.mode === b.mode ? 0 : a.mode === 'sea' ? -1 : 1) || String(a.fc || '').localeCompare(String(b.fc || '')));

    // Earlier weeks for the corridor: closed weeks first, then weeks before tracking began.
    const earlier = [];
    for (let i = 1; i <= EARLIER_WEEKS + 1 && earlier.length < EARLIER_WEEKS; i++) {
      const w = addD(thisWeek, -7 * i);
      if (liveWeeks.includes(w)) continue;
      const inWeek = all.filter(m => m.wk === w);
      const before = B.tracking_started ? w < B.tracking_started : true;
      earlier.push({ wk: w, label: isoWeek(w), date: `wk of ${fmtShort(w)}`, n: inWeek.filter(byMode).length, before: before && !inWeek.length });
    }
    const unassigned = (B.unassigned || []).filter(u => S.mode === 'all' || String(u.freight || '').toLowerCase() === S.mode);
    return { B, today, thisWeek, all, liveWeeks, timeline, earlier, unassigned, byMode };
  }

  // The day each stage sits on, in this phase. Before a replay, an estimate revised in the
  // last day is put back to the value it replaced; the stages downstream of it move with it
  // unless someone has already recorded them.
  function stageDates(m, phase) {
    const out = {};
    const ch = m.changed || {};
    let shiftDep = 0, shiftArr = 0;
    if (phase === 'before') {
      if (ch.carrier_etd && m.c.carrier_etd) shiftDep = diff(m.c.carrier_etd, ch.carrier_etd) || 0;
      if (ch.carrier_eta && m.c.carrier_eta) shiftArr = diff(m.c.carrier_eta, ch.carrier_eta) || 0;
      else shiftArr = shiftDep;
    }
    for (const st of STAGES) {
      const x = m.ms[st];
      if (!x) continue;
      let v = ymd(x.actual_at || x.planned_at);
      if (!x.actual_at && v) {
        if (st === 'departed' && shiftDep) v = addD(v, shiftDep);
        if ((st === 'arrived' || st === 'dest_cleared' || st === 'fc_receipt') && shiftArr) v = addD(v, shiftArr);
      }
      out[st] = { v, actual: !!x.actual_at, state: x.state, planned: ymd(x.planned_at) };
    }
    return out;
  }

  // Door to door, packing list → received at FC, leg by leg.
  function d2dOf(m) {
    const now = stageDates(m, 'now');
    const P = m.basePlan;
    const first = now.packing_list_ready && now.packing_list_ready.v;
    const dep = now.departed && now.departed.v, arr = now.arrived && now.arrived.v, fc = now.fc_receipt && now.fc_receipt.v;
    if (!first || !dep || !arr || !fc) return null;
    const act = [diff(first, dep), diff(dep, arr), diff(arr, fc)];
    const done = [!!(now.departed && now.departed.actual), !!(now.arrived && now.arrived.actual), !!(now.fc_receipt && now.fc_receipt.actual)];
    let plan = null, planIsRule = false;
    if (P && P.packing_list_ready && P.departed && P.arrived && P.fc_receipt) {
      plan = [diff(P.packing_list_ready, P.departed), diff(P.departed, P.arrived), diff(P.arrived, P.fc_receipt)];
    } else {
      // Never quoted: the only plan there is, is the rule. Said so rather than dressed up.
      const pd = (st) => m.ms[st] && ymd(m.ms[st].planned_at);
      if (pd('packing_list_ready') && pd('departed') && pd('arrived') && pd('fc_receipt')) {
        plan = [diff(pd('packing_list_ready'), pd('departed')), diff(pd('departed'), pd('arrived')), diff(pd('arrived'), pd('fc_receipt'))];
        planIsRule = true;
      }
    }
    if (!plan || plan.some(v => v == null) || act.some(v => v == null)) return null;
    const tp = plan[0] + plan[1] + plan[2], ta = act[0] + act[1] + act[2];
    return { plan, act, done, tp, ta, delta: ta - tp, est: done.filter(x => !x).length, planIsRule };
  }

  // ════ Highlights ════
  // What has happened — not what is wrong. Open problems are already on the screen (the
  // timeline, the status colours, the panel); repeating them here buried the news. This is the
  // last seven days of change across the movements in play: a carrier moving a date, a ship
  // leaving or arriving, a hold, someone confirming a milestone, a client being told.
  const SEV = { high: RED, medium: AMBER, low: '#6E6E73', good: GREEN };
  const SEV_TEXT = { high: RED, medium: '#8A6D00', low: MUTED, good: '#4A6A00' };
  const HL_DAYS = 7;
  function ago(t) {
    const mins = Math.round((Date.now() - t) / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const h = Math.round(mins / 60);
    if (h < 24) return `${h}h ago`;
    return timeLabel(new Date(t).toISOString());
  }
  function highlights(M) {
    // One line per movement. A container that departed, had its ETA revised twice and had a
    // milestone confirmed is one piece of news, not five: the line leads with its most
    // important change and folds the rest into "Also". ETA revisions are netted — first value
    // to latest — so a date that moved and came back is not reported as a slip.
    const since = Date.now() - HL_DAYS * 86400000;
    const out = [];
    const PRI = { hold: 0, later: 1, lfd: 2, actual: 3, earlier: 4, confirmed: 5, transshipment: 6, notified: 7, other: 8 };
    for (const m of M.all) {
      if (!M.byMode(m)) continue;
      const recentDelivery = m.delivered && m.fc && diff(m.fc, M.today) <= HL_DAYS;
      if (!m.live && !recentDelivery) continue;
      const evs = (m.c.events || []).map(e => Object.assign({ t: Date.parse(e.at) }, e))
        .filter(e => e.t && e.t >= since && e.kind !== 'backfill').sort((a, b) => a.t - b.t);
      const n = m.c.last_notification;
      if (!evs.length && !(n && Date.parse(n.sent_at) >= since)) continue;
      const items = [];
      // Net each estimate field across the window.
      for (const field of ['carrier_eta', 'carrier_etd']) {
        const es = evs.filter(e => e.kind === 'estimate' && (e.field || 'carrier_eta') === field);
        if (!es.length) continue;
        const from = (es.find(e => e.old_value) || {}).old_value || null, to = es[es.length - 1].new_value;
        const w = field === 'carrier_etd' ? 'departure' : 'arrival';
        const dd = from && to ? diff(from, to) : null;
        const last = es[es.length - 1].t;
        if (dd > 0) items.push({ k: 'later', t: last, sev: 'high', tag: `+${dd}d`, head: `Carrier moved ${w} ${fmtDay(from)} → ${fmtDay(to)}`, phrase: `${w} moved to ${fmtDay(to)}`, short: `${w} moved to ${fmtShort(to)}` });
        else if (dd < 0) items.push({ k: 'earlier', t: last, sev: 'good', tag: `−${-dd}d`, head: `Carrier brought ${w} forward ${fmtDay(from)} → ${fmtDay(to)}`, phrase: `${w} forward to ${fmtDay(to)}`, short: `${w} forward to ${fmtShort(to)}` });
        else if (!from && to) items.push({ k: 'other', t: last, sev: 'low', tag: 'Estimate', head: `Carrier set ${w} for ${fmtDay(to)}`, phrase: `${w} estimate ${fmtDay(to)}`, short: `${w} estimate ${fmtShort(to)}` });
        // Moved and came back within the window: nothing to report.
      }
      for (const e of evs) {
        if (e.kind === 'estimate') continue;
        if (e.kind === 'actual') {
          const word = ({ departed: 'Departed', arrived: 'Arrived', dest_cleared: 'Available for pickup' })[e.stage] || 'Tracked';
          items.push({ k: 'actual', t: e.t, sev: 'good', tag: word === 'Available for pickup' ? 'Available' : word, head: e.text, phrase: `${word.toLowerCase()} ${e.date ? fmtDay(e.date) : ''}`.trim(), short: `${word.toLowerCase()} ${e.date ? fmtShort(e.date) : ''}`.trim() });
        } else if (e.kind === 'hold') items.push({ k: 'hold', t: e.t, sev: 'high', tag: 'Hold', head: e.text, phrase: `hold at the terminal${e.lfd ? ` · last free day ${fmtDay(e.lfd)}` : ''}`, short: 'hold reported' });
        else if (e.kind === 'lfd') items.push({ k: 'lfd', t: e.t, sev: 'medium', tag: 'LFD', head: e.text, phrase: e.lfd ? `last free day ${fmtDay(e.lfd)}` : 'last free day set', short: e.lfd ? `last free day ${fmtShort(e.lfd)}` : 'last free day set' });
        else if (e.kind === 'confirmed') items.push({ k: 'confirmed', t: e.t, sev: 'good', tag: 'Confirmed', head: e.text, phrase: `${(STAGE_VERB[e.stage] || 'milestone')} confirmed${e.date ? ` · ${fmtDay(e.date)}` : ''}`, short: `${(STAGE_VERB[e.stage] || 'milestone')} confirmed` });
        else if (e.kind === 'transshipment') items.push({ k: 'transshipment', t: e.t, sev: 'low', tag: 'Transship', head: e.text, phrase: 'transshipment', short: 'transshipment' });
        else items.push({ k: 'other', t: e.t, sev: 'low', tag: '', head: e.text, phrase: e.text.charAt(0).toLowerCase() + e.text.slice(1), short: e.text.toLowerCase() });
      }
      if (n && Date.parse(n.sent_at) >= since) items.push({ k: 'notified', t: Date.parse(n.sent_at), sev: 'low', tag: 'Notified', head: `Client notified (${plural(n.to_count, 'recipient')})`, phrase: `client notified · ${plural(n.to_count, 'recipient')}`, short: 'client notified' });
      if (!items.length) continue;
      // Same kind twice (two confirmations): keep the latest of each kind.
      const byKind = new Map();
      for (const it of items) { const key = it.k + '|' + it.head; if (!byKind.has(key) || byKind.get(key).t < it.t) byKind.set(key, it); }
      const list = [...byKind.values()].sort((a, b) => (PRI[a.k] - PRI[b.k]) || (b.t - a.t));
      const lead = list[0];
      const rest = [...new Set(list.slice(1).map(x => x.short).filter(Boolean))];
      const latest = Math.max(...list.map(x => x.t));
      out.push({
        uid: m.uid, wk: m.wkLabel, wkStart: m.wk, ref: m.ref, mode: m.mode, kind: lead.k, phrase: lead.phrase || lead.head,
        attention: ['hold', 'later', 'lfd'].includes(lead.k), also: rest,
        title: `${m.ref} — ${lead.head}`,
        sub: rest.length ? `Also: ${rest.slice(0, 3).join(' · ')}${rest.length > 3 ? ` · +${rest.length - 3} more` : ''}` : `${m.routeText} · now ${m.where.charAt(0).toLowerCase()}${m.where.slice(1)}`,
        tag: lead.tag, sev: lead.sev, at: latest, when: ago(latest),
      });
    }
    return out.sort((a, b) => b.at - a.at).slice(0, 40);
  }

  // ── What needs a person now ──
  // Standing state, not events: a movement is listed once, for its most pressing issue, for
  // as long as the issue stands. Delayed beats held beats behind beats not booked beats a
  // milestone past its planned date with nobody confirming it.
  function attentionItems(M) {
    const out = [];
    for (const m of M.all) {
      if (!m.live || !M.byMode(m)) continue;
      const h = m.health || {};
      const late = h.late;
      let it = null;
      if (h.key === 'delayed') it = { k: 'delayed', sev: 'high', tag: late != null && late > 0 ? `+${late}d` : (m.held ? 'Held' : 'Delayed'), phrase: m.held && h.lfd_in != null && h.lfd_in <= 2 ? `held · last free day ${fmtDay(m.term.lfd)}` : `FC ${fmtDay(m.fc)} · ${plural(late, 'day')} later than first promised` };
      else if (m.held) it = { k: 'held', sev: h.lfd_in != null && h.lfd_in <= 2 ? 'high' : 'medium', tag: 'Hold', phrase: `${(m.term.holds || []).join(', ').toLowerCase() || 'hold'} at ${m.term.terminal || 'the terminal'}${m.term.lfd ? ` · last free day ${fmtDay(m.term.lfd)}` : ''}` };
      else if (h.key === 'behind') it = { k: 'behind', sev: 'medium', tag: `+${late}d`, phrase: `FC ${fmtDay(m.fc)} · ${plural(late, 'day')} later than first promised` };
      else if (h.key === 'not_booked') {
        const dep = m.d('departed'); const until = dep ? diff(M.today, dep) : null;
        if (until != null && until <= 7) it = { k: 'unbooked', sev: until < 0 ? 'high' : 'medium', tag: 'Not booked', phrase: `${m.air ? 'flies' : 'departs'} ${fmtDay(dep)} — no ${m.air ? 'AWB' : 'container number'} yet` };
      }
      if (!it) {
        const st = STAGES.find(s => m.ms[s] && !m.ms[s].actual_at && m.ms[s].planned_at && ymd(m.ms[s].planned_at) < M.today);
        if (st) { const days = diff(ymd(m.ms[st].planned_at), M.today); it = { k: 'unconfirmed', sev: days > 3 ? 'medium' : 'low', tag: `${days}d`, phrase: `${STAGE_VERB[st]} not confirmed · due ${fmtDay(m.ms[st].planned_at)}` }; }
      }
      if (!it) continue;
      out.push({ uid: m.uid, wk: m.wkLabel, wkStart: m.wk, ref: m.ref, mode: m.mode, kind: it.k, sev: it.sev, tag: it.tag, phrase: it.phrase, attention: true, when: m.wkLabel, at: 0,
        title: `${m.ref} — ${it.phrase}`, sub: `${m.routeText} · now ${m.where.charAt(0).toLowerCase()}${m.where.slice(1)}`, also: [] });
    }
    if (M.unassigned.length) {
      const legacy = M.unassigned.filter(u => u.legacy_dates).length;
      out.push({ uid: 'none', wk: '', wkStart: '', ref: plural(M.unassigned.length, 'lane'), mode: 'none', kind: 'unassigned', sev: legacy ? 'medium' : 'low', tag: 'No movement',
        phrase: `on no container or flight${legacy ? ` · ${legacy} with old lane dates` : ''}`, attention: true, when: '', at: 0, title: `${plural(M.unassigned.length, 'lane')} on no movement`, sub: '', also: [] });
    }
    const R = { delayed: 0, held: 1, behind: 2, unbooked: 3, unconfirmed: 4, unassigned: 5 };
    return out.sort((a, b) => (R[a.kind] - R[b.kind]) || (b.sev === 'high') - (a.sev === 'high'));
  }

  // ════ Styles ════
  function styles() {
    if (document.getElementById('tm-styles')) return;
    if (!document.getElementById('tm-fonts')) {
      const l = document.createElement('link');
      l.id = 'tm-fonts'; l.rel = 'stylesheet';
      l.href = 'https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500;600&display=swap';
      document.head.appendChild(l);
    }
    const s = document.createElement('style');
    s.id = 'tm-styles';
    s.textContent = `
.tm{font-family:'Geist','Helvetica Neue',Helvetica,Arial,sans-serif;color:${INK};box-sizing:border-box;padding:24px 28px 48px;max-width:1600px;margin:0 auto;display:flex;flex-direction:column;gap:20px;-webkit-font-smoothing:antialiased}
.tm *{box-sizing:border-box}
.tm-mono{font-family:'Geist Mono',ui-monospace,Menlo,monospace}
/* Zero-specificity reset: a plain .tm button rule outranked every pill and chip class, which
   is what blew their text up and made their backgrounds transparent. */
.tm :where(button){font:inherit;color:inherit;background:none;border:0;padding:0;margin:0;text-align:left;cursor:pointer}
.tm :where(button,.tm-chip,.tm-wchip,.tm-pill,.tm-pillbtn){white-space:nowrap}
.tm button:focus-visible,.tm input:focus-visible,.tm textarea:focus-visible,.tm select:focus-visible{outline:2px solid ${RED};outline-offset:2px}
.tm button[disabled]{cursor:not-allowed;opacity:.55}
.tm-card{background:#fff;border:1px solid ${LINE};border-radius:12px;overflow:hidden}
.tm-h2{margin:0;font-size:15px;font-weight:600}
.tm-note{font-size:13px;color:${MUTED}}
.tm-cap{font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:${MUTED}}
.tm-row:hover{background:#FAFAF8}
.tm-head{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap}
.tm-head h1{margin:0;font-size:34px;font-weight:600;letter-spacing:-.02em;line-height:1}
.tm-kicker{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:${MUTED};font-weight:500}
.tm-pillbtn{display:inline-flex;align-items:center;gap:6px;height:30px;padding:0 12px;border-radius:15px;border:1px solid #D6D6D2;background:#fff;font-size:12px;font-weight:500}
.tm-btn{height:38px;padding:0 14px;border-radius:8px;border:1px solid #D6D6D2;background:#fff;font-size:12.5px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.tm-btn-p{height:42px;padding:0 16px;border-radius:8px;background:${INK};color:#fff;font-size:13px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.tm-btn-s{height:42px;padding:0 16px;border-radius:8px;background:#fff;border:1px solid #D6D6D2;font-size:13px;font-weight:500;display:inline-flex;align-items:center;gap:8px}
.tm-seg{display:flex;background:#E7E7E4;border-radius:8px;padding:3px;gap:2px}
.tm-seg button{height:32px;padding:0 14px;border-radius:6px;font-size:12.5px;font-weight:500;color:${MUTED}}
.tm-seg button[aria-pressed="true"]{background:#fff;color:${INK};box-shadow:0 1px 2px rgba(0,0,0,.08)}
.tm-hl{display:grid;grid-template-columns:3px 54px minmax(0,1fr) minmax(0,auto);gap:14px;align-items:center;padding:0 calc(20px + 3%) 0 20px;border-top:1px solid ${SOFT};height:64px;width:100%}
.tm-hl .bar{align-self:stretch;margin:10px 0;border-radius:2px}
.tm-hl .t{font-size:13.5px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tm-hl .s{font-size:12.5px;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tm-hl .r{display:flex;flex-direction:column;align-items:flex-end;gap:4px;max-width:200px;min-width:0}
.tm-tick{position:relative;overflow:hidden}
.tm-tick-inner{display:flex;flex-direction:column;animation:tmTick var(--tm-dur,60s) linear infinite}
.tm-tick:hover .tm-tick-inner,.tm-tick:focus-within .tm-tick-inner{animation-play-state:paused}
.tm-live{width:6px;height:6px;border-radius:50%;background:${GREEN};display:inline-block;flex-shrink:0;animation:tmPulse 1.8s ease-in-out infinite}
.tm-dark{background:${INK};color:#fff;border-radius:12px;padding:18px 20px 20px;display:flex;flex-direction:column}
.tm-dseg{display:flex;background:#262626;border-radius:8px;padding:3px;gap:2px}
.tm-dseg button{height:30px;padding:0 12px;border-radius:6px;font-size:12px;font-weight:500;color:#B5B5B5}
.tm-dseg button[aria-pressed="true"]{background:#fff;color:${INK}}
.tm-iconbtn{width:36px;height:36px;border-radius:8px;border:1px solid #3A3A3A;display:inline-flex;align-items:center;justify-content:center}
.tm-wchip{display:inline-flex;align-items:center;gap:6px;height:30px;padding:0 10px;border-radius:15px;font-size:11.5px;font-weight:500;color:#D9D9D9;border:1px solid #3A3A3A}
.tm-wchip[aria-pressed="true"]{background:#fff;color:${INK};border-color:#fff}
.tm-wchip .n{font-size:11px;font-weight:600;min-width:16px;height:16px;padding:0 4px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;background:#333;color:#B5B5B5}
.tm-wchip[aria-pressed="true"] .n{background:${INK};color:#fff}
.tm-menu{position:absolute;top:38px;left:0;z-index:5;width:270px;background:#fff;color:${INK};border-radius:10px;box-shadow:0 12px 32px rgba(0,0,0,.35);padding:6px;display:flex;flex-direction:column}
.tm-menu button{display:flex;justify-content:space-between;align-items:center;gap:10px;min-height:40px;padding:0 10px;border-radius:6px;width:100%}
.tm-menu button:hover{background:#F4F4F2}
.tm-route{display:grid;grid-template-columns:140px minmax(0,1fr) 150px;align-items:center;height:48px;border-top:1px solid #262626}
.tm-track{position:relative;height:48px}
.tm-pill{position:absolute;top:50%;z-index:1;display:flex;align-items:center;gap:6px;height:26px;padding:0 10px 0 7px;border-radius:13px;font-size:11.5px;white-space:nowrap;background:#1E1E1E;color:#fff;transition:left 1.2s cubic-bezier(.2,.7,.2,1)}
.tm-pill.sel{background:#fff;color:${INK}}
.tm-pill.assumed{border-style:dashed!important}
.tm-wk{font-size:10px;font-weight:600;padding:1px 4px;border-radius:3px;background:rgba(255,255,255,.16)}
.tm-pill.sel .tm-wk{background:${INK};color:#fff}
.tm-anim{transition:left 1.2s cubic-bezier(.2,.7,.2,1),width 1.2s cubic-bezier(.2,.7,.2,1),top 1.2s cubic-bezier(.2,.7,.2,1),opacity .5s ease}
.tm-tl-grid{display:grid;grid-template-columns:280px minmax(0,1fr)}
.tm-trow{display:grid;grid-template-columns:280px minmax(0,1fr);height:100px;position:relative;margin:8px 0;border-radius:10px;box-shadow:inset 0 0 0 1px ${LINE}}
.tm-trow.sel{background:rgba(153,0,51,.04);box-shadow:inset 3px 0 0 ${RED},inset 0 0 0 1px ${LINE}}
.tm-pulse-done{animation:tmRingDone 2.4s ease-out infinite}
.tm-pulse-next{animation:tmRingNext 2.4s ease-out infinite}
.tm-whead{display:grid;grid-template-columns:280px minmax(0,1fr);height:64px;background:#F7F7F5;border-bottom:1px solid ${LINE};border-top:1px solid ${LINE};position:relative}
.tm-badge{font-size:13px;font-weight:600;padding:3px 8px;border-radius:5px;background:#fff;border:1px solid #D6D6D2}
.tm-badge.cur{background:${INK};color:#fff;border-color:${INK}}
.tm-legend{display:flex;flex-wrap:wrap;gap:20px;padding:14px 20px;border-top:1px solid ${LINE};font-size:12px;color:${MUTED}}
.tm-legend span{display:flex;align-items:center;gap:8px}
.tm-ov{position:fixed;inset:0;z-index:2147483000;display:flex}
.tm-ov-dim{position:absolute;inset:0;background:rgba(18,18,18,.28);animation:tmFade .2s ease;cursor:default}
.tm-panel{position:relative;margin-left:auto;width:100%;max-width:520px;height:100%;overflow-y:auto;background:#fff;display:flex;flex-direction:column;box-shadow:-20px 0 60px rgba(0,0,0,.18);animation:tmIn .28s cubic-bezier(.2,.7,.2,1)}
.tm-pnav{position:sticky;top:0;z-index:2;background:#fff;display:flex;justify-content:space-between;align-items:center;gap:12px;padding:10px 14px;border-bottom:1px solid ${SOFT}}
.tm-sq{width:40px;height:40px;border-radius:8px;border:1px solid #D6D6D2;display:inline-flex;align-items:center;justify-content:center}
.tm-sec{padding:16px 22px 4px;display:flex;flex-direction:column}
.tm-li{display:grid;gap:10px;align-items:center;padding:9px 0;border-top:1px solid #F3F3F1;font-size:13px}
.tm-chip{display:inline-flex;justify-content:center;align-items:center;height:22px;padding:0 8px;border-radius:11px;font-size:11px;font-weight:600;white-space:nowrap}
.tm-link{display:flex;align-items:center;gap:6px;font-size:13px;font-weight:500;text-decoration:underline;text-underline-offset:3px;min-height:32px}
.tm-modal-wrap{position:fixed;inset:0;z-index:2147483100;background:rgba(18,18,18,.45);display:flex;justify-content:center;align-items:flex-start;padding:40px 24px;overflow-y:auto}
.tm-modal{background:#fff;border-radius:14px;width:100%;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 24px 64px rgba(0,0,0,.25)}
.tm-field{height:40px;padding:0 12px;border:1px solid #D6D6D2;border-radius:8px;font:inherit;font-size:13px;color:${INK};width:100%;background:#fff}
.tm-toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);z-index:2147483200;background:${INK};color:#fff;padding:12px 18px;border-radius:10px;font-size:13px;box-shadow:0 10px 30px rgba(0,0,0,.25);animation:tmFade .2s ease;max-width:90vw}
.tm-skel{background:linear-gradient(90deg,#EEE 0,#F6F6F4 50%,#EEE 100%);background-size:200% 100%;animation:tmSk 1.2s infinite;border-radius:10px}
@keyframes tmIn{from{transform:translateX(28px);opacity:0}to{transform:none;opacity:1}}
@keyframes tmFade{from{opacity:0}to{opacity:1}}
@keyframes tmRingDone{0%{box-shadow:0 0 0 2px #fff,0 0 0 2px ${GREEN}}70%,100%{box-shadow:0 0 0 2px #fff,0 0 0 12px ${GREEN}00}}
@keyframes tmRingNext{0%{box-shadow:0 0 0 2px #fff,0 0 0 2px rgba(18,18,18,.5)}70%,100%{box-shadow:0 0 0 2px #fff,0 0 0 12px rgba(18,18,18,0)}}
@keyframes tmTick{from{transform:translateY(0)}to{transform:translateY(-50%)}}
@keyframes tmPulse{0%,100%{box-shadow:0 0 0 0 ${GREEN}99}50%{box-shadow:0 0 0 4px ${GREEN}00}}
@keyframes tmSk{from{background-position:200% 0}to{background-position:-200% 0}}
@media (max-width:900px){.tm{padding:16px 12px 40px}.tm-route{grid-template-columns:96px minmax(0,1fr) 96px}.tm-head h1{font-size:28px}}
@media (prefers-reduced-motion:reduce){.tm *,.tm-ov *,.tm-modal-wrap *{transition:none!important;animation:none!important}.tm-tick{overflow-y:auto!important}}
`;
    document.head.appendChild(s);
  }

  // ── Icons (inline stroke SVG) ──
  const I = {
    ship: (s = 16) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M2 21c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1 .6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1"/><path d="M19.4 17.5 21 13l-9-4-9 4 1.6 4.5"/><path d="M12 9V3"/><path d="M8 5h8"/></svg>`,
    plane: (s = 16) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M17.8 19.2 16 11l3.5-3.5C21 6 21.5 4 21 3c-1-.5-3 0-4.5 1.5L13 8 4.8 6.2c-.5-.1-.9.1-1.1.5l-.3.5c-.2.5-.1 1 .3 1.3L9 12l-2 3H4l-1 1 3 2 2 3 1-1v-3l3-2 3.5 5.3c.3.4.8.5 1.3.3l.5-.2c.4-.3.6-.7.5-1.2z"/></svg>`,
    none: (s = 16) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><circle cx="12" cy="12" r="9" stroke-dasharray="3 3"/></svg>`,
    x: (s = 16) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>`,
    out: (s = 13) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"/><path d="M8 7h9v9"/></svg>`,
    chev: (s = 14, rot = 0) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="transition:transform .25s ease;transform:rotate(${rot}deg);flex-shrink:0" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>`,
    left: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m15 18-6-6 6-6"/></svg>`,
    right: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m9 18 6-6-6-6"/></svg>`,
    expand: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>`,
    replay: () => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/></svg>`,
    arrow: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg>`,
    tick: () => `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>`,
    dl: () => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg>`,
  };
  const modeIcon = (m, s) => m === 'air' ? I.plane(s) : m === 'sea' ? I.ship(s) : I.none(s);
  const dot = (key, size) => {
    const st = STATUS[key] || STATUS.on_time;
    return `<span style="width:${size}px;height:${size}px;border-radius:50%;flex-shrink:0;display:inline-block;${st.hollow ? `border:2px solid ${st.c}` : `background:${st.c}`}"></span>`;
  };

  // ── Animation ──
  // An element that moves in a replay is drawn at its "before" position with its "now"
  // position stashed beside it; runAnimation() then sets the new values and CSS carries it.
  function A(base, props) {
    const before = S.phase === 'before';
    let st = base;
    const now = {};
    for (const k of Object.keys(props)) {
      const [b, n] = props[k];
      st += `${k}:${before ? b : n};`;
      if (before && b !== n) now[k] = n;
    }
    const attr = Object.keys(now).length ? ` data-tm-anim="${esc(JSON.stringify(now))}"` : '';
    return `style="${esc(st)}"${attr}`;
  }
  function runAnimation() {
    const els = S.root ? S.root.querySelectorAll('[data-tm-anim]') : [];
    if (!els.length) { S.phase = 'now'; return false; }
    requestAnimationFrame(() => requestAnimationFrame(() => {
      // Re-query: a render in between replaces the elements, and those must move too.
      const ov = document.getElementById('tm-overlay');
      const all = [...(S.root ? S.root.querySelectorAll('[data-tm-anim]') : []), ...(ov ? ov.querySelectorAll('[data-tm-anim]') : [])];
      for (const el of all) {
        try { Object.assign(el.style, JSON.parse(el.getAttribute('data-tm-anim'))); } catch (_) {}
        el.removeAttribute('data-tm-anim');
      }
      S.phase = 'now';
    }));
    return true;
  }

  // ════ Header ════
  const CLIENT_NAME = { ICONIC: 'THE ICONIC', EHP: 'EHPlabs' };
  function renderHeader(M) {
    const live = M.all.filter(m => m.live && M.byMode(m));
    const wks = [...new Set(live.map(m => m.wkLabel))];
    const lanes = live.reduce((a, m) => a + m.lanes, 0), pos = live.reduce((a, m) => a + m.pos, 0);
    const sea = live.filter(m => m.mode === 'sea').length, air = live.length - sea;
    const summary = live.length
      ? `${plural(live.length, 'movement')} across ${wks.length > 1 ? `${wks[0]}–${wks[wks.length - 1]}` : wks[0]} · ${plural(lanes, 'lane')} · ${plural(pos, 'PO')} in play — ${sea} sea, ${air} air`
      : 'Nothing is moving right now.';
    const client = CLIENT_NAME[M.B.client_id] || M.B.client_id || '';
    const tracked = M.B.tracking_last_event_at;
    const seg = (k, l) => `<button data-act="mode" data-v="${k}" aria-pressed="${S.mode === k}">${l}</button>`;
    const rep = (fn, l) => `<button class="tm-pillbtn" data-act="report" data-v="${fn}">${l} ${I.out()}</button>`;
    return `
<div class="tm-head">
  <div style="display:flex;flex-direction:column;gap:6px">
    <div class="tm-kicker">Pinpoint${client ? ' · ' + esc(client) : ''}</div>
    <h1>Transit Movements</h1>
    <div class="tm-note" style="font-size:14px">${esc(summary)}</div>
    <nav aria-label="Related reports" style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:6px">
      <span style="font-size:12px;color:${MUTED};margin-right:4px">Reports</span>
      ${rep('__openTransitHistory', 'Transit performance')}${rep('__openLastMileHistory', 'Last mile')}${rep('__openWeeklyHistory', 'Last 10 weeks')}
    </nav>
  </div>
  <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <button class="tm-btn" data-act="replay">${I.replay()} Replay since yesterday</button>
    <div class="tm-seg" role="group" aria-label="Mode">${seg('all', 'All')}${seg('sea', 'Sea')}${seg('air', 'Air')}</div>
    ${tracked ? `<div style="display:flex;align-items:center;gap:8px;height:40px;padding:0 14px;background:#fff;border:1px solid ${LINE};border-radius:8px;font-size:13px">
      <span style="width:8px;height:8px;border-radius:50%;background:${INK};display:inline-block"></span>
      <span style="font-weight:500">Pinpoint tracking</span><span style="color:${MUTED}">· updated ${esc(timeLabel(tracked))}</span></div>` : ''}
  </div>
</div>`;
  }

  // ════ Highlights ════
  // Two short lists, not one long one: what needs attention (dates moved later, holds, last
  // free days) beside what progressed (departed, arrived, confirmed, brought forward). One
  // line per movement, four per list until "View all". The detail lives in the row's tooltip
  // and the movement panel, not in the row.
  const HL_CAP = 4;
  const HL_COUNTS = [['late', 'late'], ['held', 'held'], ['unconfirmed', 'not confirmed'], ['progress', 'moved in 7 days'], ['confirmed', 'confirmed']];
  function hlMatches(h, f) {
    if (!f) return true;
    if (f === 'late') return h.kind === 'delayed' || h.kind === 'behind' || h.kind === 'later';
    if (f === 'held') return h.kind === 'held' || h.kind === 'hold' || h.kind === 'lfd';
    if (f === 'unconfirmed') return h.kind === 'unconfirmed' || h.kind === 'unbooked' || h.kind === 'unassigned';
    if (f === 'progress') return h.kind === 'actual' || h.kind === 'earlier' || h.kind === 'transshipment';
    if (f === 'confirmed') return h.kind === 'confirmed';
    return true;
  }
  function hlRow(h) {
    const tip = [h.title, h.also && h.also.length ? 'Also: ' + h.also.join(' · ') : '', h.sub && !String(h.sub).startsWith('Also') ? h.sub : ''].filter(Boolean).join('\n');
    return `<button class="tm-row" data-act="pick" data-uid="${esc(h.uid)}" title="${esc(tip)}" style="display:grid;grid-template-columns:20px auto minmax(0,1fr) auto auto;gap:10px;align-items:center;padding:0 16px;height:44px;width:100%;border-top:1px solid ${SOFT}">
        <span style="display:inline-flex;color:${MUTED}">${modeIcon(h.mode === 'none' ? 'none' : h.mode, 15)}</span>
        <span class="tm-mono" style="font-size:12.5px;font-weight:600;white-space:nowrap">${esc(h.ref)}</span>
        <span style="font-size:13px;color:${INK};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(h.phrase)}</span>
        ${h.tag ? `<span style="height:20px;padding:0 8px;border-radius:10px;font-size:11px;font-weight:600;display:inline-flex;align-items:center;white-space:nowrap;${h.sev === 'high' ? `background:${RED};color:#fff` : h.sev === 'medium' ? `background:${AMBER};color:${INK}` : h.sev === 'good' ? `background:${GREEN};color:${INK}` : `background:${SOFT};color:${INK}`}">${esc(h.tag)}</span>` : '<span></span>'}
        <span style="font-size:11.5px;color:${MUTED};white-space:nowrap;min-width:56px;text-align:right">${esc(h.when || '')}</span>
      </button>`;
  }
  function hlList(title, list, key) {
    const open = !!(S.hlOpen && S.hlOpen[key]);
    const shown = open ? list : list.slice(0, HL_CAP);
    return `<div data-tm-hl="${key}" style="display:flex;flex-direction:column;min-width:0;border:1px solid ${LINE};border-radius:10px;overflow:hidden">
      <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 16px;background:#FAFAF8">
        <span style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};font-weight:600">${title}</span>
        <span class="tm-mono" style="font-size:12px;color:${MUTED}">${list.length}</span></div>
      ${shown.length ? shown.map(hlRow).join('') : `<div style="padding:14px 16px;font-size:13px;color:${MUTED};border-top:1px solid ${SOFT}">${key === 'attn' ? 'Nothing needs a person right now.' : `Nothing moved in the last ${HL_DAYS} days.`}</div>`}
      ${list.length > HL_CAP ? `<button data-act="hl-more" data-v="${key}" style="padding:10px 16px;font-size:12.5px;font-weight:500;border-top:1px solid ${SOFT};text-align:left">${open ? 'Show fewer' : `View all ${list.length}`}</button>` : ''}
    </div>`;
  }
  function renderHighlights(H, A) {
    const f = S.hlFilter || null;
    // Left: standing issues. An event-driven warning (a date moved later, a hold) joins the
    // left list only for a movement that has no standing issue already listed there.
    const have = new Set(A.map(a => a.uid));
    const left = A.concat(H.filter(h => h.attention && !have.has(h.uid)));
    const right = H.filter(h => !h.attention);
    const all = left.concat(right);
    const counts = HL_COUNTS.map(([k, l]) => [k, l, all.filter(h => hlMatches(h, k)).length]);
    const strip = counts.map(([k, l, n]) => `<button data-act="hl-filter" data-v="${k}" aria-pressed="${f === k}" style="display:inline-flex;align-items:center;gap:6px;height:28px;padding:0 10px;border-radius:14px;font-size:12.5px;${f === k ? `background:${INK};color:#fff` : `background:${SOFT};color:${INK}`}${n ? '' : ';opacity:.5'}"><b class="tm-mono" style="font-weight:600">${n}</b> ${l}</button>`).join('');
    const L = left.filter(h => hlMatches(h, f)), Rr = right.filter(h => hlMatches(h, f));
    return `
<section class="tm-card" aria-label="Highlights">
  <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 20px 12px;gap:12px;flex-wrap:wrap">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <h2 class="tm-h2">Highlights</h2>
      <span class="tm-note">What needs a person, and what moved</span>
    </div>
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">${strip}${f ? `<button data-act="hl-filter" data-v="" style="font-size:12px;text-decoration:underline;text-underline-offset:3px;margin-left:4px;min-height:28px">Clear</button>` : ''}</div>
  </div>
  ${!all.length ? `<div style="padding:0 20px 18px;font-size:14px;color:${MUTED}">Nothing needs a person, and nothing has changed in the last ${HL_DAYS} days.</div>`
    : `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:14px;padding:0 20px 18px">
        ${hlList('Needs attention · now', L, 'attn')}${hlList(`Progress · last ${HL_DAYS} days`, Rr, 'prog')}</div>`}
</section>`;
  }

  // ════ Where everything is ════
  function corridorSet(M) {
    const w = S.corrWk;
    if (w === 'all') return M.all.filter(m => m.live && M.byMode(m));
    return M.all.filter(m => m.wk === w && M.byMode(m));
  }
  function routeKey(m) {
    const o = m.route.origin || {}, d = m.route.dest || {};
    return `${m.mode}|${o.code || o.name || '?'}|${d.code || d.name || '?'}`;
  }
  function renderCorridor(M, full) {
    const set = corridorSet(M);
    const view = S.view || 'routes';
    const isHist = S.corrWk !== 'all' && !M.liveWeeks.includes(S.corrWk);
    const vt = (k, l) => `<button data-act="view" data-v="${k}" aria-pressed="${view === k}">${l}</button>`;
    const chips = [['all', 'All', M.all.filter(m => m.live && M.byMode(m)).length]]
      .concat(M.liveWeeks.map(w => [w, isoWeek(w), M.all.filter(m => m.wk === w && M.byMode(m)).length]))
      .map(([k, l, n]) => `<button class="tm-wchip" data-act="cw" data-v="${k}" aria-pressed="${S.corrWk === k}" style="${n ? '' : 'opacity:.45'}"><span class="tm-mono">${l}</span><span class="n">${n}</span></button>`).join('');
    const earlierItems = M.earlier.map(e => `<button role="menuitem" data-act="cw" data-v="${e.wk}">
        <span style="display:flex;align-items:center;gap:8px"><span class="tm-mono" style="font-size:12px;font-weight:600">${e.label}</span><span style="font-size:12px;color:${MUTED}">${esc(e.date)}</span></span>
        <span style="font-size:12px;${e.before ? `color:${GREY};font-style:italic` : `font-weight:500`}">${e.before ? 'before tracking' : e.n ? plural(e.n, 'movement') : 'no movements'}</span></button>`).join('');
    const legend = ['on_time', 'behind', 'delayed', 'not_booked'].map(k =>
      `<span style="display:flex;align-items:center;gap:6px;font-size:12px;color:#D9D9D9">${dot(k, 10)}${k === 'not_booked' ? 'Not booked or no quote' : STATUS[k].l}</span>`).join('')
      + `<span style="display:flex;align-items:center;gap:6px;font-size:12px;color:#D9D9D9"><span style="width:18px;height:10px;border-radius:5px;border:1.5px dashed #9C9C9C;display:inline-block"></span>Position from plan — not yet confirmed</span>`;
    const head = `
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;padding-bottom:10px">
    <div style="display:flex;flex-direction:column;gap:2px;min-width:0">
      <h2 class="tm-h2" style="font-size:${full ? 20 : 15}px">Where everything is</h2>
      <div style="font-size:12px;color:#B5B5B5">${view === 'routes' ? 'One line per route · position from tracking, else from confirmed dates' : 'Positions estimated along each route from tracking and confirmed dates — not live GPS'}</div>
      <div role="group" aria-label="Show week" style="display:flex;flex-wrap:wrap;align-items:center;gap:4px;margin-top:10px">
        <span style="font-size:12px;color:#9C9C9C;margin-right:6px">Week</span>${chips}
        <span style="position:relative;display:inline-flex">
          <button class="tm-wchip" data-act="earlier-toggle" aria-haspopup="true" aria-expanded="${S.corrMore}" style="${isHist ? `background:#fff;color:${INK}` : 'border-style:dashed!important'}">
            <span class="tm-mono">${isHist ? isoWeek(S.corrWk) : 'Earlier'}</span>${I.chev(12)}</button>
          ${S.corrMore ? `<div class="tm-menu" role="menu">${earlierItems || `<div style="padding:10px;font-size:12px;color:${MUTED}">No earlier weeks.</div>`}
            <div style="font-size:11px;color:${MUTED};padding:8px 10px 4px;border-top:1px solid ${SOFT};margin-top:4px">Older weeks: Transit performance report</div></div>` : ''}
        </span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:8px">${legend}</div>
    </div>
    <div style="display:flex;align-items:center;gap:10px">
      <div class="tm-dseg" role="group" aria-label="View">${vt('routes', 'Routes')}${vt('map', 'Map')}</div>
      <button class="tm-iconbtn" data-act="${full ? 'full-close' : 'full-open'}" aria-label="${full ? 'Close full screen' : 'Full screen'}">${full ? I.x() : I.expand()}</button>
    </div>
  </div>`;
    let body = '';
    const preTracking = isHist && (M.earlier.find(e => e.wk === S.corrWk) || {}).before;
    const empty = !set.length ? `<div style="padding:28px 0;text-align:center;font-size:13px;color:#9C9C9C;border-top:1px solid #262626">${
      preTracking ? `${isoWeek(S.corrWk)} is before movement tracking began, so there are no positions to show. Transit performance still covers it.`
                  : `Nothing ${S.mode === 'all' ? '' : 'by ' + S.mode + ' '}${S.corrWk === 'all' ? 'is moving' : 'in ' + isoWeek(S.corrWk)}.`}</div>` : '';
    const histNote = isHist && set.length ? `<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;padding:8px 0 10px;font-size:12px;color:#B5B5B5;flex-wrap:wrap">
        <span>${isoWeek(S.corrWk)} is closed — colour shows how each movement finished against plan.</span>
        <button data-act="week-report" data-v="${S.corrWk}" style="font-size:12px;font-weight:500;color:#fff;text-decoration:underline;text-underline-offset:3px;min-height:32px">Transit performance for this week</button></div>` : '';
    if (view === 'routes') body = histNote + (empty || renderRoutes(set));
    else body = histNote + `<div style="display:grid;grid-template-columns:minmax(0,1fr) 260px;gap:18px;align-items:start" class="tm-mapgrid">
        <div data-tm-map="${full ? 'full' : 'inline'}" style="position:relative;height:${full ? 'calc(100vh - 230px)' : '460px'};min-height:320px;background:#161616;border-radius:10px;overflow:hidden">
          <div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:12px;color:#777" data-tm-maploading>Loading map…</div>
        </div>
        <div style="display:flex;flex-direction:column;max-height:${full ? 'calc(100vh - 230px)' : '460px'};overflow-y:auto">${empty ? `<div style="font-size:13px;color:#9C9C9C;padding:12px">${empty.replace(/<[^>]+>/g, '')}</div>` : ''}
          ${set.map(m => `<button data-act="${m.live ? 'pick' : 'hist'}" data-uid="${esc(m.uid)}" data-v="${m.wk}" data-dbl="${esc(m.uid)}" style="display:flex;flex-direction:column;gap:3px;padding:10px 12px;border-radius:8px;min-height:52px;width:100%;border-top:1px solid #262626;${S.sel === m.uid ? 'background:#262626' : ''}">
            <span style="display:flex;justify-content:space-between;gap:8px"><span style="display:flex;align-items:center;gap:8px">${dot(m.st, 9)}<span class="tm-mono" style="font-size:12px;font-weight:600">${esc(m.ref)}</span></span><span class="tm-mono" style="font-size:11px;color:#9C9C9C">${m.wkLabel}</span></span>
            <span style="font-size:12px;color:#B5B5B5">${esc(m.where)}</span></button>`).join('')}
        </div></div>`;
    return `<section class="tm-dark" aria-label="Where everything is" ${full ? 'style="width:100%;max-width:1480px;max-height:calc(100vh - 48px);overflow:auto;box-shadow:0 30px 80px rgba(0,0,0,.5);border-radius:14px;padding:22px 26px 24px"' : ''}>${head}${body}</section>`;
  }

  function renderRoutes(set) {
    const groups = new Map();
    for (const m of set) {
      const k = routeKey(m);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(m);
    }
    // At most three to a line. A route carrying more is drawn as several lines, newest week
    // first, so pills never stack on top of one another.
    const PER_LINE = 3;
    const order = [];
    for (const [k, all] of [...groups.entries()].sort((a, b) => (a[0].startsWith('sea') ? 0 : 1) - (b[0].startsWith('sea') ? 0 : 1))) {
      const sorted = all.slice().sort((a, b) => b.wk.localeCompare(a.wk) || b.prog - a.prog);
      const parts = Math.ceil(sorted.length / PER_LINE);
      for (let i = 0; i < parts; i++) order.push([k, sorted.slice(i * PER_LINE, (i + 1) * PER_LINE), i, parts]);
    }
    return order.map(([k, list, part, parts]) => {
      const m0 = list[0], o = m0.route.origin, d = m0.route.dest;
      const air = m0.mode === 'air';
      const via = [];
      for (const m of list) for (const v of (m.route.via || [])) { const l = v.code ? v.code.replace(/^[A-Z]{2}(?=[A-Z]{3}$)/, '') : v.name; if (l && !via.includes(l)) via.push(l); }
      const seenB = {}, seenN = {};
      const pills = list.map(m => {
        const pB = m.progB, pN = m.prog;
        const pos = (p, seen) => {
          const slot = Math.round(p * 20);
          seen[slot] = (seen[slot] || 0) + 1; const shift = (seen[slot] - 1) * 136;
          return p <= 0.02 ? [`calc(${p * 100}% + ${10 + shift}px)`, 'translate(0,-50%)']
               : p >= 0.98 ? [`calc(${p * 100}% - ${10 + shift}px)`, 'translate(-100%,-50%)']
               : [`calc(${p * 100}% + ${shift}px)`, 'translate(-50%,-50%)'];
        };
        const [ln, tr] = pos(pN, seenN);
        const lb = S.phase === 'before' ? pos(pB, seenB)[0] : ln;
        const st = STATUS[m.st] || STATUS.on_time;
        return `<button class="tm-pill tm-anim${S.sel === m.uid ? ' sel' : ''}${m.assumedPos ? ' assumed' : ''}" title="${esc(m.assumedPos ? `${m.where} — placed by plan; not yet confirmed` : m.where)}" data-act="${m.live ? 'pick' : 'hist'}" data-uid="${esc(m.uid)}" data-v="${m.wk}" data-dbl="${esc(m.uid)}"
          aria-label="${esc(`${m.wkLabel} ${m.ref}, ${m.where}`)}" ${A(`transform:${tr};border:1.5px solid ${st.c};`, { left: [lb, ln] })}>
          <span style="display:inline-flex;color:${S.sel === m.uid ? INK : st.c}" aria-hidden="true">${modeIcon(m.mode, 13)}</span><span class="tm-mono tm-wk">${m.wkLabel}</span><span class="tm-mono">${esc(m.short)}</span></button>`;
      }).join('');
      const trails = list.filter(m => m.prog > 0.02 || m.progB > 0.02).map(m => {
        const st = STATUS[m.st] || STATUS.on_time;
        return `<div class="tm-anim" ${A(`position:absolute;left:0;top:50%;height:3px;transform:translateY(-50%);border-radius:2px;background:${st.c};`, { width: [`${m.progB * 100}%`, `${m.prog * 100}%`] })}></div>`;
      }).join('');
      const cont = part > 0;
      const end = (p, alignRight) => `<div style="display:flex;flex-direction:column;gap:1px;${alignRight ? 'align-items:flex-end;text-align:right' : ''};min-width:0;${cont ? 'opacity:.45' : ''}">
          <span class="tm-mono" style="font-size:13px;font-weight:600">${esc(p ? (p.code || '') : '') || '—'}</span>
          <span style="font-size:11px;color:#9C9C9C;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(p ? (p.name || '') : 'Port not yet known')}${parts > 1 ? ` · ${part + 1}/${parts}` : ''}</span></div>`;
      return `<div class="tm-route">${end(o, false)}
        <div class="tm-track">
          <div style="position:absolute;left:0;right:0;top:50%;border-top:1.5px ${air ? 'dashed' : 'solid'} #4A4A4A"></div>
          ${trails}
          <div style="position:absolute;left:0;top:50%;width:9px;height:9px;border-radius:50%;background:#fff;transform:translate(-50%,-50%)"></div>
          <div style="position:absolute;right:0;top:50%;width:9px;height:9px;border-radius:50%;background:#fff;transform:translate(50%,-50%)"></div>
          ${via.length ? `<div style="position:absolute;top:50%;left:${air ? 50 : 24}%;width:7px;height:7px;border-radius:50%;background:${INK};border:1.5px solid #8A8A8A;transform:translate(-50%,-50%)"></div>
            <div class="tm-mono" style="position:absolute;top:2px;left:${air ? 50 : 24}%;transform:translateX(-50%);font-size:10px;color:#9C9C9C">via ${esc(via.join(', '))}</div>` : ''}
          ${pills}
        </div>${end(d, true)}</div>`;
    }).join('');
  }

  // ════ Map ════
  // The real country shapes the old Live Map loaded, drawn dark, with each route traced along
  // the way ships actually go: through the Luzon Strait, east of the Philippines, down past New
  // Guinea into the Coral Sea. Air is a great circle. Positions are estimated, and say so.
  const PORTS = {
    CNYTN: [114.27, 22.57], CNSHK: [113.88, 22.48], CNSZX: [114.1, 22.5], CNSZN: [114.1, 22.5], CNHKG: [114.17, 22.3], HKHKG: [114.17, 22.3],
    CNNSA: [113.6, 22.75], CNCAN: [113.26, 23.13], CNXMN: [118.07, 24.48], CNFOC: [119.3, 26.07], CNNGB: [121.84, 29.93], CNSHA: [121.8, 31.2],
    TWKHH: [120.28, 22.61], TWTXG: [120.5, 24.27], SGSIN: [103.85, 1.26], MYPKG: [101.4, 3.0], MYTPP: [103.55, 1.36], VNSGN: [106.7, 10.77],
    VNHPH: [106.7, 20.85], THLCH: [100.88, 13.08], PHMNL: [120.95, 14.6], IDJKT: [106.88, -6.1], KRPUS: [129.04, 35.1],
    AUSYD: [151.21, -33.97], AUBTB: [151.22, -33.97], AUMEL: [144.92, -37.84], AUBNE: [153.17, -27.38], AUFRE: [115.75, -32.05], AUADL: [138.5, -34.8],
    NZAKL: [174.78, -36.84],
    SZX: [113.81, 22.64], HKG: [113.92, 22.31], CAN: [113.3, 23.39], PVG: [121.8, 31.14], SIN: [103.99, 1.36], TPE: [121.23, 25.08],
    SYD: [151.18, -33.94], MEL: [144.84, -37.67], BNE: [153.12, -27.38], PER: [115.97, -31.94],
  };
  const NAMES = { yantian: 'CNYTN', shekou: 'CNSHK', shenzhen: 'CNSZX', 'hong kong': 'CNHKG', nansha: 'CNNSA', guangzhou: 'CNCAN', xiamen: 'CNXMN',
    ningbo: 'CNNGB', shanghai: 'CNSHA', kaohsiung: 'TWKHH', singapore: 'SGSIN', 'port klang': 'MYPKG', sydney: 'AUSYD', botany: 'AUSYD',
    melbourne: 'AUMEL', brisbane: 'AUBNE', fremantle: 'AUFRE', adelaide: 'AUADL', busan: 'KRPUS' };
  function coordOf(p) {
    if (!p) return null;
    const c = p.code && PORTS[String(p.code).toUpperCase()];
    if (c) return c;
    const n = String(p.name || '').toLowerCase();
    for (const k of Object.keys(NAMES)) if (n.includes(k)) return PORTS[NAMES[k]];
    return null;
  }
  function routeLonLat(m) {
    const o = coordOf(m.route.origin), d = coordOf(m.route.dest);
    if (!o || !d) return null;
    if (m.mode === 'air' && window.d3) {
      const f = d3.geoInterpolate(o, d); const pts = [];
      for (let i = 0; i <= 24; i++) pts.push(f(i / 24));
      return pts;
    }
    const pts = [o];
    for (const v of (m.route.via || [])) { const c = coordOf(v); if (c) pts.push(c); }
    const last = pts[pts.length - 1];
    const toEastAus = d[0] > 140;
    if (toEastAus) {
      if (last[1] > 15) pts.push([121, 20.8], [125, 17]);
      else if (last[0] < 112) pts.push([109, 4], [118, 6], [123, 7]);
      pts.push([128, 10], [134, 3], [146, -1.5], [153.5, -5], [154.5, -12], [155, -22]);
      if (d[1] < -30) pts.push([153.8, -30]);
      if (d[1] < -36) pts.push([151.5, -36.8], [149.5, -38.6]);
    } else {
      if (last[1] > 12) pts.push([113, 15], [110, 8]);
      pts.push([106.5, -2.5], [115.6, -8.9], [114, -20]);
    }
    pts.push(d);
    return pts;
  }

  let _libs = null, _land = null;
  function loadScript(src) {
    return new Promise((res, rej) => {
      const s = document.createElement('script'); s.src = src; s.onload = res; s.onerror = rej;
      setTimeout(() => rej(new Error('timed out loading ' + src)), 15000);   // a blocked CDN must not hang the view
      document.head.appendChild(s);
    });
  }
  async function mapLibs() {
    if (!_libs) _libs = (async () => {
      if (!window.d3) await loadScript('https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js');
      if (!window.topojson) await loadScript('https://cdn.jsdelivr.net/npm/topojson@3/dist/topojson.min.js');
      const topo = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r => r.json());
      _land = topojson.feature(topo, topo.objects.countries);
    })().catch(e => { _libs = null; throw e; });
    return _libs;
  }

  async function drawMaps(M) {
    const hosts = S.root ? [...S.root.querySelectorAll('[data-tm-map]')] : [];
    const ov = document.getElementById('tm-overlay');
    if (ov) hosts.push(...ov.querySelectorAll('[data-tm-map]'));
    if (!hosts.length) return;
    try { await mapLibs(); }
    catch (e) {
      for (const h of hosts) { const l = h.querySelector('[data-tm-maploading]'); if (l) l.textContent = 'Map unavailable — the Routes view has the same information.'; }
      return;
    }
    const set = corridorSet(M);
    for (const host of hosts) {
      if (!host.isConnected) continue;
      const w = host.clientWidth, h = host.clientHeight;
      if (!w || !h) continue;
      const proj = d3.geoMercator().fitExtent([[16, 16], [w - 16, h - 16]], { type: 'MultiPoint', coordinates: [[96, 31], [158, -41]] });
      const dpr = window.devicePixelRatio || 1;
      const cv = document.createElement('canvas');
      cv.width = w * dpr; cv.height = h * dpr; cv.style.cssText = `position:absolute;inset:0;width:${w}px;height:${h}px`;
      const ctx = cv.getContext('2d'); ctx.scale(dpr, dpr);
      const gp = d3.geoPath(proj, ctx);
      ctx.beginPath(); gp(_land); ctx.fillStyle = '#232323'; ctx.fill(); ctx.strokeStyle = '#2E2E2E'; ctx.lineWidth = 1; ctx.stroke();
      ctx.beginPath(); gp({ type: 'LineString', coordinates: [[90, 0], [165, 0]] }); ctx.setLineDash([3, 5]); ctx.strokeStyle = '#2C2C2C'; ctx.stroke(); ctx.setLineDash([]);

      // Routes, then pins.
      const svgNS = 'http://www.w3.org/2000/svg';
      const svg = document.createElementNS(svgNS, 'svg');
      svg.setAttribute('width', w); svg.setAttribute('height', h);
      svg.style.cssText = 'position:absolute;inset:0;pointer-events:none';
      const drawn = new Set(), ports = new Map();
      const paths = new Map();
      for (const m of set) {
        const ll = routeLonLat(m);
        if (!ll) continue;
        const px = ll.map(p => proj(p));
        paths.set(m.uid, px);
        const k = routeKey(m);
        if (!drawn.has(k)) {
          drawn.add(k);
          const p = document.createElementNS(svgNS, 'path');
          p.setAttribute('d', 'M' + px.map(q => q[0].toFixed(1) + ',' + q[1].toFixed(1)).join(' L'));
          p.setAttribute('fill', 'none'); p.setAttribute('stroke', '#5C5C5C'); p.setAttribute('stroke-width', '1.5');
          p.setAttribute('stroke-linejoin', 'round');
          if (m.mode === 'air') p.setAttribute('stroke-dasharray', '4 5');
          svg.appendChild(p);
        }
        for (const [port, pt] of [[m.route.origin, px[0]], [m.route.dest, px[px.length - 1]]]) {
          const name = port && (port.name || port.code);
          if (name && !ports.has(name)) ports.set(name, pt);
        }
      }
      for (const [name, pt] of ports) {
        const c = document.createElementNS(svgNS, 'circle');
        c.setAttribute('cx', pt[0]); c.setAttribute('cy', pt[1]); c.setAttribute('r', 3.5); c.setAttribute('fill', '#fff'); svg.appendChild(c);
        const t = document.createElementNS(svgNS, 'text');
        const right = pt[0] < w * 0.6;
        t.setAttribute('x', pt[0] + (right ? 8 : -8)); t.setAttribute('y', pt[1] + 4); t.setAttribute('fill', '#9C9C9C');
        t.setAttribute('font-size', '11'); t.setAttribute('text-anchor', right ? 'start' : 'end'); t.setAttribute('font-family', 'Geist, sans-serif');
        t.textContent = name; svg.appendChild(t);
      }
      host.innerHTML = '';
      host.appendChild(cv); host.appendChild(svg);

      const along = (px, t) => {
        const L = []; let tot = 0;
        for (let i = 1; i < px.length; i++) { const l = Math.hypot(px[i][0] - px[i - 1][0], px[i][1] - px[i - 1][1]); L.push(l); tot += l; }
        let dd = clamp(t, 0, 1) * tot;
        for (let i = 0; i < L.length; i++) {
          if (dd <= L[i] || i === L.length - 1) { const f = L[i] ? Math.min(1, dd / L[i]) : 0; return [px[i][0] + (px[i + 1][0] - px[i][0]) * f, px[i][1] + (px[i + 1][1] - px[i][1]) * f]; }
          dd -= L[i];
        }
        return px[px.length - 1];
      };
      const stackN = {}, stackB = {};
      for (const m of set) {
        const px = paths.get(m.uid);
        if (!px) continue;
        const [xn, yn] = along(px, m.prog);
        const kn = Math.round(xn / 30) + ':' + Math.round(yn / 30); stackN[kn] = (stackN[kn] || 0) + 1;
        const ynS = yn + (stackN[kn] - 1) * 28;
        let xb = xn, ybS = ynS;
        if (S.phase === 'before' && m.progB !== m.prog) {
          const [x2, y2] = along(px, m.progB);
          const kb = Math.round(x2 / 30) + ':' + Math.round(y2 / 30); stackB[kb] = (stackB[kb] || 0) + 1;
          xb = x2; ybS = y2 + (stackB[kb] - 1) * 28;
        }
        const st = STATUS[m.st] || STATUS.on_time;
        const leftSide = xn > w * 0.68;
        const b = document.createElement('button');
        b.className = 'tm-anim';
        b.setAttribute('data-act', m.live ? 'pick' : 'hist');
        b.setAttribute('data-uid', m.uid); b.setAttribute('data-v', m.wk); b.setAttribute('data-dbl', m.uid);
        b.setAttribute('aria-label', `${m.wkLabel} ${m.ref}, ${m.where}`);
        const sel = S.sel === m.uid;
        b.style.cssText = `position:absolute;z-index:1;left:${xb}px;top:${ybS}px;transform:translate(${leftSide ? 'calc(-100% + 9px)' : '-9px'},-50%);display:flex;align-items:center;gap:6px;height:24px;padding:${leftSide ? '0 4px 0 9px' : '0 9px 0 4px'};border-radius:12px;font-size:11px;white-space:nowrap;${leftSide ? 'flex-direction:row-reverse;' : ''}${sel ? `background:#fff;color:${INK};` : `background:rgba(30,30,30,.95);color:#fff;border:1.5px ${m.assumedPos ? 'dashed' : 'solid'} ${st.c};`}`;
        if (m.assumedPos) b.title = `${m.where} — placed by plan; not yet confirmed`;
        b.innerHTML = `<span style="display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;border-radius:50%;background:${st.c}${st.hollow ? '55' : ''};color:${m.st === 'delayed' ? '#fff' : INK};box-shadow:0 0 0 3px ${st.c}40" aria-hidden="true">${modeIcon(m.mode, 11)}</span><span class="tm-mono tm-wk" style="${sel ? `background:${INK};color:#fff` : ''}">${m.wkLabel}</span><span class="tm-mono">${esc(m.short)}</span>`;
        host.appendChild(b);
        if (xb !== xn || ybS !== ynS) requestAnimationFrame(() => requestAnimationFrame(() => { b.style.left = xn + 'px'; b.style.top = ynS + 'px'; }));
      }
      const unmapped = set.filter(m => !paths.has(m.uid)).length;
      if (unmapped) {
        const n = document.createElement('div');
        n.style.cssText = 'position:absolute;left:12px;bottom:10px;font-size:11px;color:#8A8A8A';
        n.textContent = `${plural(unmapped, 'movement')} not shown — port not yet known`;
        host.appendChild(n);
      }
    }
  }

  // ════ Timeline by execution week ════
  // Weeks start collapsed: each header still shows its movements' FC dates, late ones in red,
  // and a week opens with one click.
  const isShut = (k) => (k in S.collapsed) ? !!S.collapsed[k] : true;
  const ORIGIN = {
    complete:    { l: 'Complete',    ink: '#4A6A00', mark: `background:${GREEN};border:1.5px solid ${INK}` },
    in_progress: { l: 'In progress', ink: INK,       mark: `background:#fff;border:1.5px solid ${INK}` },
    not_started: { l: 'Not started', ink: MUTED,     mark: `background:#fff;border:1.5px dashed #8A8A8A` },
    at_risk:     { l: 'At risk',     ink: '#8A6D00', mark: `background:${AMBER};border:1.5px solid ${INK}` },
    past_due:    { l: 'Past due',    ink: RED,       mark: `background:#fff;border:2.5px solid ${RED}` },
  };
  const MS_SHORT = { packing_list_ready: 'Packed', origin_cleared: 'Cleared', departed: 'Departed', arrived: 'Arrived', dest_cleared: 'Cleared', fc_receipt: 'FC' };
  function renderTimeline(M) {
    const start = addD(mondayOf(M.today), -14);
    const TODAY = diff(start, M.today);
    const pct = (x) => clamp(((x + 0.5) / N_DAYS) * 100, 0, 100);
    const ix = (s) => s ? diff(start, s) : null;

    const axis = [], bands = [];
    for (let i = 0; i < N_DAYS; i++) {
      const s = addD(start, i), d = toD(s), dow = d.getUTCDay(), isMon = dow === 1, isToday = i === TODAY;
      axis.push(`<div style="position:absolute;top:6px;left:${pct(i)}%;transform:translateX(-50%);display:flex;flex-direction:column;align-items:center;gap:1px">
        <span class="tm-mono" style="font-size:11px;${isToday ? `background:${INK};color:#fff;border-radius:4px;padding:0 4px;font-weight:600` : isMon ? `color:${INK};font-weight:600` : 'color:#8A8A8A'}">${d.getUTCDate()}</span>
        <span style="font-size:10px;color:${MUTED}">${isMon ? 'Mon' : d.getUTCDate() === 1 ? MO[d.getUTCMonth()] : ''}</span></div>`);
      if (dow === 6) bands.push(`<div style="position:absolute;top:0;bottom:0;left:${i / N_DAYS * 100}%;width:${2 / N_DAYS * 100}%;background:#F6F6F3"></div>`);
    }

    const markStyle = (st, planned) => {
      if (st === 'carrier') return `width:12px;height:12px;background:${GREEN};border:1.5px solid ${INK};transform:translate(-50%,-50%) rotate(45deg);box-shadow:0 0 0 2px #fff;`;
      if (st === 'confirmed' || st === 'amended') return `width:13px;height:13px;border-radius:50%;background:${GREEN};border:1.5px solid ${INK};transform:translate(-50%,-50%);box-shadow:0 0 0 2px #fff;`;
      if (planned != null && planned < M.today) return `width:13px;height:13px;border-radius:50%;background:#fff;border:2.5px solid ${RED};transform:translate(-50%,-50%);`;
      return `width:12px;height:12px;border-radius:50%;background:#fff;border:1.5px dashed #8A8A8A;transform:translate(-50%,-50%);`;
    };
    const stateWord = (x) => x.state === 'carrier' ? 'tracked' : x.state === 'confirmed' ? 'confirmed' : x.state === 'amended' ? 'amended' : (x.planned && x.planned < M.today ? 'assumed, past due' : 'assumed');

    function moveRow(m) {
      const N = stageDates(m, 'now'), B = S.phase === 'before' ? stageDates(m, 'before') : N;
      const P = (st) => ({ n: ix(N[st] && N[st].v), b: ix(B[st] && B[st].v) });
      const out = [], marks = [], labels = [];
      const first = STAGES.find(st => N[st] && N[st].v);
      const pDep = P('departed'), pArr = P('arrived'), pFc = P('fc_receipt'), pFirst = first ? P(first) : null;
      // Where it actually is. Lime runs only to the last milestone someone confirmed or the
      // carrier reported — elapsed time is not progress. From there to today is either
      // "in progress" (the next milestone isn't due yet) or "overdue" (it was due and nobody
      // has said it happened). Everything after today is plan.
      const present = STAGES.filter(st => N[st] && N[st].v);
      let lastDone = null;
      for (const st of present) if (N[st].actual) lastDone = st;
      const nextUp = m.delivered ? null : present.find((st, i) => !N[st].actual && (lastDone == null || i > present.indexOf(lastDone)));
      const doneX = lastDone ? ix(N[lastDone].v) : null;
      const overX = nextUp && N[nextUp].v < M.today ? ix(N[nextUp].v) : null;
      const kindAt = (x) => (doneX != null && x < doneX) ? 'done' : x < TODAY ? ((overX != null && x >= overX) ? 'over' : 'prog') : 'future';
      const LOOK = {
        done: (h) => `height:${h + 2}px;background:${GREEN};box-shadow:0 0 0 1px ${INK};`,
        prog: (h) => `height:${h}px;background:${INK};opacity:.5;`,
        over: (h) => `height:${h + 1}px;background:repeating-linear-gradient(90deg,${RED} 0 6px,transparent 6px 10px);`,
        future: (h) => `height:${h}px;background:repeating-linear-gradient(90deg,#9A9A96 0 5px,transparent 5px 9px);`,
      };
      const legs = first && pDep.n != null && pArr.n != null && pFc.n != null
        ? [[pFirst.n, pDep.n, 2], [pDep.n, pArr.n, 6], [pArr.n, pFc.n, 2]] : [];
      for (const [s0, e0, h] of legs) {
        if (e0 <= s0) continue;
        const cuts = [s0, e0, doneX, overX, TODAY].filter(v => v != null && v >= s0 && v <= e0);
        const pts = [...new Set(cuts)].sort((a, b) => a - b);
        for (let i = 0; i < pts.length - 1; i++) {
          const a = pts[i], b = pts[i + 1];
          if (b - a < 0.01) continue;
          const L = pct(a), R = pct(b);
          if (R - L < 0.05) continue;
          out.push(`<div style="position:absolute;top:50%;transform:translateY(-50%);left:${L}%;width:${R - L}%;${LOOK[kindAt((a + b) / 2)](h)}"></div>`);
        }
      }
      // A pill on each leg that has an answer: finished legs against their plan, the overdue
      // stretch, and a sea leg whose carrier ETA has moved. Legs still running on plan stay quiet.
      const pills = [];
      const pill = (x0, x1, text, col, ink) => {
        const a = Math.max(x0, -0.5), b = Math.min(x1, N_DAYS - 0.6);
        if (b - a < 3) return;
        pills.push(`<div title="${esc(text)}" style="position:absolute;top:50%;left:${pct((a + b) / 2)}%;transform:translate(-50%,-50%);z-index:1;height:17px;padding:0 7px;border-radius:9px;background:#fff;border:1.5px solid ${col};color:${ink};font-size:10px;font-weight:600;white-space:nowrap;display:flex;align-items:center">${esc(text)}</div>`);
      };
      const P0 = m.basePlan;
      const legDefs = [[first, 'departed'], ['departed', 'arrived'], ['arrived', 'fc_receipt']];
      for (const [a, b] of legDefs) {
        if (!a || !N[a] || !N[b]) continue;
        const xa = ix(N[a].v), xb = ix(N[b].v);
        if (N[a].actual && N[b].actual && P0 && P0[a] && P0[b]) {
          const dd = diff(N[a].v, N[b].v) - diff(P0[a], P0[b]);
          if (dd > 1) pill(xa, xb, `+${dd}d`, RED, RED);
          else if (dd < -1) pill(xa, xb, `${dd}d`, GREEN, '#4A6A00');
          else pill(xa, xb, 'On plan', GREEN, '#4A6A00');
        } else if (a === 'departed' && N.departed.actual && !N.arrived.actual && !m.air && m.c.carrier_eta && P0 && P0.arrived) {
          const dd = diff(P0.arrived, m.c.carrier_eta);
          if (dd > 1) pill(xa, xb, `ETA +${dd}d`, AMBER, '#8A6D00');
        }
      }
      if (overX != null) {
        const days = diff(N[nextUp].v, M.today);
        const what = STAGE_VERB[nextUp];
        pill(overX, TODAY, `${what.charAt(0).toUpperCase() + what.slice(1)} not confirmed · ${days}d`, RED, RED);
      }
      if (m.base && pFc.n != null) {
        const bx = ix(m.base);
        const on = pFc.n > bx, onB = pFc.b > bx;
        const w = (x) => `${Math.max(0, pct(x) - pct(bx))}%`;
        // Along the bottom edge, clear of the milestone labels.
        out.push(`<div class="tm-anim" title="${esc(`First promised ${fmtDay(m.base)}`)}" ${A(`position:absolute;bottom:9px;height:4px;left:${pct(bx)}%;background:${RED};border-radius:2px;`, { width: [w(pFc.b), w(pFc.n)], opacity: [onB ? '1' : '0', on ? '1' : '0'] })}></div>`);
        labels.push(`<div class="tm-anim" ${A(`position:absolute;bottom:4px;transform:translateX(6px);font-size:11px;font-weight:600;color:${RED};white-space:nowrap;`, { left: [`${pct(Math.max(pFc.b, bx))}%`, `${pct(Math.max(pFc.n, bx))}%`], opacity: [onB ? '1' : '0', on ? '1' : '0'] })}>${on ? `+${pFc.n - bx}d vs first promise` : ''}</div>`);
      }
      const via = (m.route.via || []).map(v => v.code ? v.code.replace(/^[A-Z]{2}(?=[A-Z]{3}$)/, '') : v.name).filter(Boolean);
      if (via.length && pDep.n != null && pArr.n != null) {
        labels.push(`<div style="position:absolute;top:5px;left:${pct((pDep.n + pArr.n) / 2)}%;transform:translateX(-50%);font-size:11px;color:${MUTED};white-space:nowrap">via ${esc(via.join(', '))}</div>`);
      }
      if (m.term && m.term.lfd && !m.delivered && !m.term.available_at) {
        const lx = ix(m.term.lfd);
        if (lx >= 0 && lx < N_DAYS) {
          marks.push(`<div title="Last free day ${esc(fmtDay(m.term.lfd))}" style="position:absolute;top:8px;bottom:8px;left:${pct(lx)}%;width:0;border-left:2px solid ${RED}"></div>`);
          labels.push(`<div style="position:absolute;top:6px;left:calc(${pct(lx)}% + 5px);font-size:11px;font-weight:600;color:${RED};white-space:nowrap">LFD ${esc(fmtDay(m.term.lfd))}</div>`);
        }
      }
      // The last milestone achieved and the next one due pulse together — same rhythm, same
      // moment — so the eye reads "here, and next". Delivered movements are still.
      // Every marker is named. Labels sit below the line, or above it when below is taken,
      // placed in order of importance — FC receipt, then where it is now and what is next —
      // so when milestones a day apart cannot all be labelled, the ones that matter are.
      const DAY_PX = 20;
      const taken = { below: [], above: [] };
      const fits = (slot, a, b) => taken[slot].every(([x, y]) => b <= x || a >= y);
      const order = present.slice().sort((p, q) => {
        const rank = (st) => st === 'fc_receipt' ? 0 : st === lastDone ? 1 : st === nextUp ? 2 : (st === 'departed' || st === 'arrived') ? 3 : 4;
        return rank(p) - rank(q);
      });
      const labelFor = {};
      const early = present.filter(st => ix(N[st].v) < -0.5);
      if (early.length) {
        const st = early[early.length - 1], x = N[st];
        const overdue = early.some(s => !N[s].actual && N[s].planned && N[s].planned < M.today);
        const pulse = m.delivered ? '' : early.includes(nextUp) ? ' tm-pulse-next' : early.includes(lastDone) ? ' tm-pulse-done' : '';
        const tip = early.map(s => `${STAGE_LABEL[s]} · ${fmtDay(N[s].v)} · ${stateWord(N[s])}`).join('\n');
        marks.push(`<div class="${pulse.trim()}" title="${esc(tip)}" style="position:absolute;top:50%;left:6px;box-sizing:border-box;${markStyle(x.actual ? x.state : 'assumed', x.planned).replace('translate(-50%,-50%)', 'translate(0,-50%)')}"></div>`);
        const text = `← ${MS_SHORT[st]} ${fmtShort(x.v)}${early.length > 1 ? ` +${early.length - 1}` : ''}`;
        labels.push(`<div title="${esc(tip)}" style="position:absolute;top:calc(50% + 11px);left:4px;font-size:10.5px;white-space:nowrap;${x.actual ? `color:${INK};font-weight:600` : overdue ? `color:${RED};font-weight:600` : `color:${MUTED}`}">${esc(text)}</div>`);
        taken.below.push([-0.5, -0.5 + (text.length * 6.2 + 10) / DAY_PX]);
      }
      for (const st of order) {
        const x = N[st], xn = ix(x.v);
        if (xn > N_DAYS - 0.6 || xn < -0.5) continue;
        const text = st === 'fc_receipt' ? `FC ${fmtShort(x.v)}` : MS_SHORT[st];
        const half = (text.length * 6.2 + 8) / DAY_PX / 2;
        const a = xn - half - 0.1, b = xn + half + 0.1;
        const slot = fits('below', a, b) ? 'below' : fits('above', a, b) ? 'above' : null;
        if (!slot) continue;
        taken[slot].push([a, b]);
        labelFor[st] = { slot, text };
      }
      for (const st of present) {
        const x = N[st];
        const xn = ix(x.v), xb = ix(B[st] && B[st].v);
        if (xn > N_DAYS - 0.6 || xn < -0.5) continue;
        const pulse = !m.delivered && (st === lastDone ? ' tm-pulse-done' : st === nextUp ? ' tm-pulse-next' : '');
        marks.push(`<div class="tm-anim${pulse}" title="${esc(`${STAGE_LABEL[st]} · ${fmtDay(x.v)} · ${stateWord(x)}`)}" ${A(`position:absolute;top:50%;box-sizing:border-box;${markStyle(x.actual ? x.state : 'assumed', x.planned)}`, { left: [`${pct(xb)}%`, `${pct(xn)}%`] })}></div>`);
        const L = labelFor[st];
        if (!L) continue;
        const overdue = !x.actual && x.planned && x.planned < M.today;
        const tone = x.actual ? `color:${INK};font-weight:600` : overdue ? `color:${RED};font-weight:600` : `color:${MUTED};font-weight:500`;
        labels.push(`<div class="tm-anim" ${A(`position:absolute;${L.slot === 'below' ? 'top:calc(50% + 11px)' : 'top:calc(50% - 25px)'};transform:translateX(-50%);font-size:${st === 'fc_receipt' ? 11 : 10.5}px;white-space:nowrap;${tone};`, { left: [`${pct(xb)}%`, `${pct(xn)}%`] })}>${esc(L.text)}</div>`);
      }
      if (pFc.n != null && pFc.n > N_DAYS - 0.6) labels.push(`<div style="position:absolute;top:5px;right:8px;font-size:11px;font-weight:600;white-space:nowrap">FC ${esc(fmtDay(m.fc))} →</div>`);
      else if (pFc.n != null && pFc.n < -0.5) labels.push(`<div style="position:absolute;top:5px;left:8px;font-size:11px;font-weight:600;white-space:nowrap">← FC ${esc(fmtDay(m.fc))}</div>`);
      const on = S.sel === m.uid;
      const tone = m.st === 'delayed' ? `color:${RED};font-weight:600` : m.st === 'behind' ? `color:${INK};font-weight:600` : m.delivered ? `color:${MUTED};font-weight:500` : `color:${INK};font-weight:500`;
      return `<div class="tm-trow${on ? ' sel' : ''}">
        <button data-act="pick" data-uid="${esc(m.uid)}" data-dbl="${esc(m.uid)}" style="display:flex;gap:12px;align-items:center;padding:12px 16px 12px 20px;height:100%;width:100%">
          <span style="width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;flex-shrink:0;${on ? `background:${INK};color:#fff` : `background:#F0F0ED;color:${INK}`}">${modeIcon(m.mode)}</span>
          <span style="display:flex;flex-direction:column;gap:3px;min-width:0">
            <span class="tm-mono" style="font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m.ref)}</span>
            <span style="font-size:12px;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m.sub)}</span>
            <span style="font-size:12px;white-space:nowrap;display:flex;align-items:center;gap:6px;${tone}">${dot(m.st, 8)}${esc(m.where)}</span>
          </span>
        </button>
        <div style="position:relative;height:100%">${out.join('')}${pills.join('')}${marks.join('')}${labels.join('')}</div>
      </div>`;
    }

    // Ex-factory and VAS sit on the week's own row, above and below its execution bar, at the
    // day they completed — or at their target while still open — so a week reads from the
    // factory gate to the FC.
    function originChips(w) {
      const O = (M.B.weeks_origin || {})[w];
      if (!O) return '';
      const chip = (k, o, text, pos) => {
        const st = ORIGIN[o.status] || ORIGIN.in_progress;
        const at = o.done_at || o.target;
        const x = ix(at);
        let place, arrowL = '', arrowR = '';
        if (x == null) return '';
        if (x < -0.5) { place = 'left:6px;'; arrowL = '← '; }
        else if (x > N_DAYS - 0.6) { place = 'right:6px;'; arrowR = ' →'; }
        else place = `left:${pct(x)}%;transform:translateX(-8px);`;
        let note = st.l;
        if (o.status === 'complete' && k === 'recv' && o.late_pos) note = `Complete · ${plural(o.late_pos, 'PO')} late`;
        else if (o.status === 'complete' && o.done_at && o.target && o.done_at > o.target) note = `Complete · ${plural(diff(o.target, o.done_at), 'day')} late`;
        const tip = k === 'recv'
          ? `Ex-factory (received): ${o.pos_received} of ${o.pos} POs received${o.closed_by_tick ? `, ${o.closed_by_tick} closed by the lane tick` : ''}${o.late_pos ? `, ${o.late_pos} after their due date` : ''}. Target ${fmtDay(o.target)}${o.done_at ? ` · done ${fmtDay(o.done_at)}` : ''}.`
          : `VAS: ${o.lanes_complete} of ${o.lanes} lanes complete · ${fmtNum(o.units_applied)} of ${fmtNum(o.units_planned)} units applied. Target ${fmtDay(o.target)}${o.done_at ? ` · done ${fmtDay(o.done_at)}` : ''}.`;
        return `<div title="${esc(tip)}" style="position:absolute;${pos};${place}display:flex;align-items:center;gap:6px;height:20px;padding:0 8px 0 4px;border-radius:10px;background:#fff;border:1px solid ${LINE};font-size:11px;white-space:nowrap;z-index:1">
          <span style="width:11px;height:11px;border-radius:50%;box-sizing:border-box;flex-shrink:0;${st.mark}"></span>
          <span>${arrowL}<b style="font-weight:600">${esc(text)}</b>${arrowR}</span>
          <span style="font-weight:600;color:${st.ink}">${esc(note)}</span></div>`;
      };
      return chip('recv', O.received, `Ex-factory ${O.received.pct}%`, 'top:4px')
           + chip('vas', O.vas, `VAS ${O.vas.lanes_complete}/${O.vas.lanes} lanes · ${O.vas.units_pct}%`, 'bottom:4px');
    }
    function weekHeader(w, list) {
      const shut = isShut(w);
      const lanes = list.reduce((a, m) => a + m.lanes, 0), pos = list.reduce((a, m) => a + m.pos, 0);
      const L = pct(Math.max(-0.5, ix(w) - 0.5)), R = pct(ix(w) + 6.5);
      const mini = shut ? list.map(m => {
        const x = ix(m.fc); if (x == null) return '';
        const late = m.base && m.fc > m.base;
        const pos2 = x > N_DAYS - 1 ? 'right:8px;transform:translateY(-50%);' : x < 0 ? 'left:8px;transform:translateY(-50%);' : `left:${pct(x)}%;transform:translate(-50%,-50%);`;
        return `<div title="${esc(`${m.ref} · FC ${fmtDay(m.fc)}`)}" class="tm-mono" style="position:absolute;top:50%;${pos2}height:20px;padding:0 7px;border-radius:10px;display:flex;align-items:center;font-size:10px;font-weight:600;white-space:nowrap;${late ? `background:${RED};color:#fff` : `background:#fff;border:1px solid #CFCFCB`}">${esc(m.short)}</div>`;
      }).join('') : '';
      return `<div class="tm-whead">
        <div style="display:flex;align-items:center;gap:2px;padding:0 8px 0 6px;height:100%;min-width:0">
          <button data-act="wk-toggle" data-v="${w}" aria-expanded="${!shut}" aria-label="${shut ? 'Expand' : 'Collapse'} ${isoWeek(w)}" style="width:32px;height:32px;border-radius:6px;display:flex;align-items:center;justify-content:center;flex-shrink:0">${I.chev(14, shut ? -90 : 0)}</button>
          <button class="tm-row" data-act="wk-open" data-v="${w}" style="display:flex;align-items:center;gap:10px;height:34px;padding:0 8px;border-radius:6px;min-width:0">
            <span class="tm-mono tm-badge${w === M.thisWeek ? ' cur' : ''}">${isoWeek(w)}</span>
            <span style="font-size:12px;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">wk of ${fmtShort(w)} · ${plural(list.length, 'movement')} · ${plural(lanes, 'lane')} · ${plural(pos, 'PO')}</span>
          </button>
        </div>
        <div style="position:relative">
          ${R > 0 && L < 100 ? `<div style="position:absolute;top:50%;height:4px;transform:translateY(-50%);left:${L}%;width:${Math.max(0, R - L)}%;background:#CFCFCB;border-radius:2px"></div>
          ` : ''}
          ${originChips(w)}
          ${mini}
        </div>
      </div>`;
    }

    const rows = [];
    const weeks = [...new Set(M.timeline.map(m => m.wk))];
    for (const w of weeks) {
      const list = M.timeline.filter(m => m.wk === w);
      rows.push(weekHeader(w, list));
      if (!isShut(w)) for (const m of list) rows.push(moveRow(m));
    }
    if (M.unassigned.length) {
      const shut = isShut('none');
      const wks = [...new Set(M.unassigned.map(u => isoWeek(u.week_start)))];
      const pos = M.unassigned.reduce((a, u) => a + (u.pos || 0), 0);
      const legacy = M.unassigned.filter(u => u.legacy_dates).length;
      rows.push(`<div class="tm-whead">
        <div style="display:flex;align-items:center;gap:2px;padding:0 8px 0 6px;height:100%">
          <button data-act="wk-toggle" data-v="none" aria-expanded="${!shut}" aria-label="${shut ? 'Expand' : 'Collapse'} lanes on no movement" style="width:32px;height:32px;border-radius:6px;display:flex;align-items:center;justify-content:center">${I.chev(14, shut ? -90 : 0)}</button>
          <button class="tm-row" data-act="pick" data-uid="none" style="display:flex;align-items:center;gap:10px;height:34px;padding:0 8px;border-radius:6px">
            <span class="tm-mono" style="font-size:12px;font-weight:600;padding:3px 8px;border-radius:5px;background:${legacy ? RED : MUTED};color:#fff">No movement</span>
            <span style="font-size:12px;color:${MUTED}">${esc(wks.join(', '))} · ${plural(M.unassigned.length, 'lane')} · ${plural(pos, 'PO')}</span>
          </button>
        </div><div></div></div>`);
      if (!shut) rows.push(`<div class="tm-trow${S.sel === 'none' ? ' sel' : ''}" style="background:repeating-linear-gradient(135deg,#FAFAF8 0 8px,#F4F4F1 8px 16px)">
        <button data-act="pick" data-uid="none" style="display:flex;gap:12px;align-items:center;padding:12px 16px 12px 20px;height:100%;width:100%">
          <span style="width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:#fff;color:${MUTED};border:1px solid ${LINE}">${I.none()}</span>
          <span style="display:flex;flex-direction:column;gap:3px">
            <span class="tm-mono" style="font-size:13px;font-weight:600">Not on a movement</span>
            <span style="font-size:12px;color:${MUTED}">${plural(M.unassigned.length, 'lane')} · ${plural(pos, 'PO')}${legacy ? ` · ${legacy} with old lane dates` : ''}</span>
            <span style="font-size:12px;font-weight:600;color:${legacy ? RED : INK}">Assign in Transit &amp; Clearing</span>
          </span>
        </button>
        <div style="position:relative;height:100%"><div style="position:absolute;left:12px;right:12px;top:18px;bottom:18px;border:1px dashed #BDBDB8;border-radius:8px;display:flex;align-items:center;padding:0 14px;font-size:13px;color:${MUTED};background:#fff">Not placed on the timeline. Dates typed on a lane are ignored until the lane joins a movement.</div></div>
      </div>`);
    }
    const anyOpen = weeks.some(w => !isShut(w)) || (M.unassigned.length && !isShut('none'));
    const empty = !M.timeline.length && !M.unassigned.length;
    return `
<section class="tm-card" aria-label="Timeline by execution week">
  <div style="display:flex;justify-content:space-between;align-items:baseline;padding:16px 20px 6px;gap:12px;flex-wrap:wrap">
    <h2 class="tm-h2">Timeline by execution week</h2>
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <span class="tm-note">Double-click a movement for its POs and SKUs · green is done, dashed is still a plan</span>
      ${M.timeline.length ? `<button class="tm-btn" style="height:32px;font-size:12px" data-act="export-all">${I.dl()} Export POs &amp; SKUs</button>` : ''}
      <button class="tm-btn" style="height:32px;font-size:12px" data-act="collapse-all">${anyOpen ? 'Collapse all' : 'Expand all'}</button>
    </div>
  </div>
  ${empty ? `<div style="padding:24px 20px 28px;font-size:14px;color:${MUTED}">No movements in play. When a container or flight is set up in Transit &amp; Clearing it appears here.</div>` : `
  <div style="overflow-x:auto">
    <div style="min-width:980px">
      <div class="tm-tl-grid" style="border-bottom:1px solid ${LINE}"><div></div><div style="position:relative;height:40px">${axis.join('')}</div></div>
      <div style="position:relative">
        <div style="position:absolute;left:280px;right:0;top:0;bottom:0;pointer-events:none">${bands.join('')}
          <div style="position:absolute;top:0;bottom:0;left:${pct(TODAY)}%;width:0;border-left:1.5px solid ${INK}"></div></div>
        ${rows.join('')}
      </div>
    </div>
  </div>`}
  <div class="tm-legend">
    <span><span style="width:9px;height:9px;background:${GREEN};border:1.5px solid ${INK};transform:rotate(45deg);display:inline-block;box-sizing:border-box"></span>Live carrier event</span>
    <span><span style="width:10px;height:10px;border-radius:50%;background:${GREEN};border:1.5px solid ${INK};display:inline-block;box-sizing:border-box"></span>Confirmed by a person</span>
    <span><span style="width:22px;height:6px;background:${GREEN};box-shadow:0 0 0 1px ${INK};display:inline-block"></span>Done — confirmed or tracked</span>
    <span><span style="width:22px;height:6px;background:${INK};opacity:.5;display:inline-block"></span>In progress</span>
    <span><span style="width:22px;height:6px;background:repeating-linear-gradient(90deg,${RED} 0 6px,transparent 6px 10px);display:inline-block"></span>Not confirmed — past its planned date</span>
    <span><span style="width:10px;height:10px;border-radius:50%;border:1.5px dashed #8A8A8A;display:inline-block;box-sizing:border-box"></span>Assumed</span>
    <span><span style="width:10px;height:10px;border-radius:50%;border:2px solid ${RED};display:inline-block;box-sizing:border-box"></span>Assumed, past due</span>
    <span><span style="width:18px;height:6px;background:${RED};display:inline-block"></span>Slip against first promise</span>
    <span><span style="width:18px;height:4px;background:#CFCFCB;display:inline-block"></span>Execution week</span>
    <span><span class="tm-pulse-done" style="width:10px;height:10px;border-radius:50%;background:${GREEN};border:1.5px solid ${INK};display:inline-block;box-sizing:border-box"></span><span class="tm-pulse-next" style="width:10px;height:10px;border-radius:50%;border:1.5px dashed #8A8A8A;display:inline-block;box-sizing:border-box;margin-left:-2px"></span>Pulsing: last achieved and next due</span>
  </div>
</section>`;
  }

  // ════ Slide-over panels ════
  const chipFor = (x, today) => {
    const b = 'class="tm-chip" style="';
    if (x.state === 'carrier') return `<span ${b}background:${INK};color:#fff;gap:5px" title="Recorded from the carrier’s own event, as it happened"><span class="tm-live"></span>Live · carrier</span>`;
    if (x.state === 'confirmed') return `<span ${b}border:1px solid ${INK}">Confirmed</span>`;
    if (x.state === 'amended') return `<span ${b}border:1px solid ${INK}">Amended</span>`;
    const p = ymd(x.planned_at);
    if (p && p < today) return `<span ${b}border:1.5px solid ${RED};color:${RED}">Not confirmed</span>`;
    if (p && p === today) return `<span ${b}border:1px dashed #8A8A8A;color:${INK}">Due today</span>`;
    return `<span ${b}border:1px dashed #8A8A8A;color:${MUTED}">Assumed</span>`;
  };
  const statusChip = (m) => {
    const st = STATUS[m.st] || STATUS.on_time;
    const label = m.held && m.st !== 'delayed' ? 'Held' : m.delivered ? `Received · ${st.l.toLowerCase()}` : st.l;
    const strong = m.st === 'delayed' || m.st === 'behind';
    return `<span style="display:inline-flex;align-items:center;gap:6px;height:24px;padding:0 10px;border-radius:12px;font-size:12px;font-weight:600;${strong ? `background:${st.c};color:${m.st === 'behind' ? INK : '#fff'}` : `background:${SOFT};color:${INK}`}">${strong ? '' : dot(m.st, 8)}${esc(label)}</span>`;
  };

  function storyOf(m, M) {
    const c = m.c, carrier = c.carrier_name || 'The carrier';
    const N = stageDates(m, 'now');
    const prevEta = c.estimate_prev && c.estimate_prev.carrier_eta;
    const drift = m.base && m.fc ? diff(m.base, m.fc) : null;
    const driftTxt = drift == null ? '' : drift > 0 ? `${plural(drift, 'day')} later than first promised` : drift < 0 ? `${plural(-drift, 'day')} earlier than first promised` : 'as first promised';
    if (m.delivered) return `Received at the FC on ${fmtDay(m.fc)}${driftTxt ? ', ' + driftTxt : ''}.`;
    if (!c.reference) return `No ${m.air ? 'AWB' : 'container number'} yet, so it can’t be tracked. Every date here comes from the ${m.air ? 'air' : 'sea'} rule until it’s booked.`;
    const parts = [];
    if (m.held && m.term) {
      parts.push(`${m.ref} is being held at ${m.term.terminal || 'the terminal'}${m.term.holds.length ? ` (${m.term.holds.join(', ').toLowerCase()})` : ''}.`);
      if (m.term.lfd) parts.push(`The last free day is ${fmtDay(m.term.lfd)}, and destination clearance hasn’t been confirmed.`);
      return parts.join(' ');
    }
    const dep = m.ms.departed, arr = m.ms.arrived;
    if (dep && dep.actual_at) {
      const late = dep.planned_at && m.basePlan && m.basePlan.departed ? diff(m.basePlan.departed, dep.actual_at) : null;
      parts.push(`Left ${portLabel(m.route.origin) || 'origin'} ${fmtDay(dep.actual_at)}${late > 0 ? `, ${plural(late, 'day')} after its planned departure` : ''}.`);
      if (arr && arr.actual_at) parts.push(`Arrived ${fmtDay(arr.actual_at)}.`);
      else if (m.air) parts.push(`Due into ${portLabel(m.route.dest) || 'Sydney'} ${fmtDay(N.arrived && N.arrived.v)}. Air isn’t tracked automatically, so arrival stays assumed until someone confirms it.`);
      else parts.push(prevEta ? `${carrier} revised arrival from ${fmtDay(prevEta)} to ${fmtDay(N.arrived && N.arrived.v)}.` : `${carrier} expects arrival ${fmtDay(N.arrived && N.arrived.v)}.`);
    } else {
      parts.push(`${m.air ? 'Flies' : 'Departs'} ${fmtDay(N.departed && N.departed.v)}${m.air ? '' : ` from ${portLabel(m.route.origin) || 'origin'}`}.`);
      if (!c.transit_confirmed) parts.push(`Transit is still the ${c.transit_days || ''}-day default, not a carrier quote, so the dates are a guess.`);
    }
    parts.push(`FC receipt ${fmtDay(m.fc)}${driftTxt ? ', ' + driftTxt : ''}.`);
    return parts.join(' ');
  }

  function d2dBlock(dd, mode) {
    if (!dd) return '';
    const names = ['Origin', mode === 'air' ? 'Flight' : 'Sea', 'Destination'];
    const scale = Math.max(dd.tp, dd.ta) || 1;
    const bar = (vals, actual) => vals.map((v, i) => {
      const w = `calc(${(v / scale) * 100}% - 2px)`;
      const look = !actual ? `background:${['#E3E3E0', '#CFCFCB', '#E3E3E0'][i]}`
        : dd.done[i] ? `background:${GREEN};box-shadow:inset 0 0 0 1px ${INK}` : `background:repeating-linear-gradient(135deg,#fff 0 4px,#ECECE9 4px 8px);border:1.5px dashed #8A8A8A`;
      return `<div title="${esc(`${names[i]} ${actual ? (dd.done[i] ? 'actual' : 'estimated') : 'plan'} ${v}d`)}" class="tm-mono" style="flex:0 0 ${w};${look};border-radius:3px;font-size:11px;${actual ? 'font-weight:600;' : ''}display:flex;align-items:center;justify-content:center;overflow:hidden;box-sizing:border-box;${actual && v > dd.plan[i] ? `color:${RED}` : ''}">${v >= 2 ? v + 'd' : ''}</div>`;
    }).join('');
    const dl = dd.delta;
    return `<div style="padding:16px 22px 18px;border-bottom:1px solid ${SOFT};display:flex;flex-direction:column;gap:12px">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px"><span class="tm-cap">Door to door</span><span style="font-size:12px;color:${MUTED}">packing list → received at FC</span></div>
      <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px">
        <div style="display:flex;flex-direction:column;gap:2px"><span style="font-size:12px;color:${MUTED}">D2D plan</span><span class="tm-mono" style="font-size:26px;font-weight:600;letter-spacing:-.02em">${dd.tp}d</span></div>
        <div style="display:flex;flex-direction:column;gap:2px"><span style="font-size:12px;color:${MUTED}">${dd.est === 0 ? 'D2D actual' : dd.est === 3 ? 'D2D projected' : 'D2D actual + est.'}</span><span class="tm-mono" style="font-size:26px;font-weight:600;letter-spacing:-.02em">${dd.ta}d</span></div>
        <div style="display:flex;flex-direction:column;gap:2px"><span style="font-size:12px;color:${MUTED}">Variance</span><span class="tm-mono" style="font-size:26px;font-weight:600;letter-spacing:-.02em;${dl > 0 ? `color:${RED}` : ''}">${dl === 0 ? '±0d' : dl > 0 ? '+' + dl + 'd' : dl + 'd'}</span></div>
      </div>
      <div style="display:flex;flex-direction:column;gap:8px">
        <div style="display:grid;grid-template-columns:48px minmax(0,1fr);gap:10px;align-items:center"><span style="font-size:12px;color:${MUTED}">Plan</span><div style="display:flex;gap:3px;height:22px">${bar(dd.plan, false)}</div></div>
        <div style="display:grid;grid-template-columns:48px minmax(0,1fr);gap:10px;align-items:center"><span style="font-size:12px;color:${MUTED}">Actual</span><div style="display:flex;gap:3px;height:22px">${bar(dd.act, true)}</div></div>
      </div>
      <span style="font-size:12px;color:${MUTED}">${names.join(' · ')}${dd.est ? ' — hatched legs are still estimates, not yet confirmed or tracked' : ' — every leg confirmed or tracked'}${dd.planIsRule ? '. No frozen plan yet: the plan shown is the rule default.' : '.'}</span>
    </div>`;
  }

  function navBar(ids, i, unit) {
    return `<div class="tm-pnav">
      <div style="display:flex;align-items:center;gap:6px">
        <button class="tm-sq" data-act="nav" data-v="-1" aria-label="Previous ${unit}">${I.left()}</button>
        <button class="tm-sq" data-act="nav" data-v="1" aria-label="Next ${unit}">${I.right()}</button>
        <span style="font-size:12px;color:${MUTED};margin-left:4px">${i >= 0 ? `${i + 1} of ${plural(ids.length, unit)}` : ''}</span>
      </div>
      <button class="tm-sq" data-act="panel-close" aria-label="Close">${I.x()}</button>
    </div>`;
  }
  function navIds(M) {
    if (S.panel === 'wk') return [...new Set(M.timeline.map(m => m.wk))];
    const ids = M.timeline.map(m => m.uid);
    if (M.unassigned.length) ids.push('none');
    if (S.sel && !ids.includes(S.sel)) ids.unshift(S.sel);
    return ids;
  }

  function renderMovementPanel(M) {
    const ids = navIds(M), idx = ids.indexOf(S.sel);
    if (S.sel === 'none') return renderUnassignedPanel(M, ids, idx);
    const m = M.all.find(x => x.uid === S.sel);
    if (!m) return '';
    const c = m.c;
    const N = stageDates(m, 'now');
    const dd = d2dOf(m);
    const prevEta = c.estimate_prev && c.estimate_prev.carrier_eta;
    const drift = m.base && m.fc ? diff(m.base, m.fc) : null;
    const facts = [];
    facts.push({ k: m.delivered ? 'Received at FC' : 'FC receipt', v: fmtDay(m.fc), n: m.base ? (drift ? `first promise ${fmtDay(m.base)}` : 'as first promised') : 'no frozen plan yet', late: drift > 0 });
    if (!m.air && c.carrier_eta) facts.push({ k: 'Carrier ETA', v: fmtDay(c.carrier_eta), n: prevEta ? `was ${fmtDay(prevEta)}` : 'from tracking', late: prevEta && c.carrier_eta > prevEta });
    const tr = c.transit || {};
    facts.push({ k: c.transit_confirmed ? 'Transit quoted' : 'Transit', v: c.transit_days != null ? plural(Number(c.transit_days), 'day') : '—',
      n: c.transit_confirmed ? (tr.baseline != null ? `baseline ${tr.baseline}${tr.requotes ? ` · ${plural(tr.requotes, 're-quote')}` : ''}` : 'carrier quote') : 'default — not a quote', late: !c.transit_confirmed });
    if (tr.achieved != null) facts.push({ k: 'Achieved', v: plural(tr.achieved, 'day'), n: tr.variance ? `${tr.variance > 0 ? '+' : ''}${tr.variance} against the first quote` : 'as quoted', late: tr.variance > 0 });
    else if (!m.air && c.carrier_eta && N.departed && N.departed.actual) {
      const implied = diff(N.departed.v, c.carrier_eta);
      if (implied != null && c.transit_days != null) facts.push({ k: 'Carrier implies', v: plural(implied, 'day'), n: implied > c.transit_days ? `+${implied - c.transit_days} against the quote` : 'within the quote', late: implied > c.transit_days });
    }
    const cs = c.contents_summary || {};
    const term = m.term;
    const showTerm = term && !m.air && (term.lfd || (term.holds && term.holds.length) || term.terminal);
    const due = STAGES.find(st => { const x = m.ms[st]; return x && x.state === 'assumed' && x.planned_at && ymd(x.planned_at) <= M.today; });
    const actions = [];
    if (canEdit() && !m.delivered && c.reference) {
      if (due) actions.push(`<button class="tm-btn-p" data-act="confirm" data-v="${due}" ${S.busy ? 'disabled' : ''}>Confirm ${STAGE_VERB[due]}</button>`);
      actions.push(`<button class="tm-btn-s" data-act="amend-open">Amend a date</button>`);
    }
    if (isInternal() && c.reference) actions.push(`<button class="${!due && (m.st === 'delayed' || m.st === 'behind' || m.held) ? 'tm-btn-p' : 'tm-btn-s'}" data-act="notify-open">Send notification</button>`);
    if (canEdit()) actions.push(`<button class="tm-btn-s" data-act="open-transit" data-v="${m.wk}">${c.reference ? 'Open in Transit &amp; Clearing' : (m.air ? 'Add AWB' : 'Add container &amp; quote')}</button>`);
    const amend = S.amend && S.amend.uid === m.uid ? `
      <div style="margin:0 22px 8px;padding:14px;border:1px solid ${LINE};border-radius:10px;display:flex;flex-direction:column;gap:10px">
        <div style="font-size:13px;font-weight:600">Record a different date</div>
        <div style="display:grid;grid-template-columns:minmax(0,1fr) 150px;gap:8px">
          <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;color:${MUTED}">Stage
            <select class="tm-field" id="tm-amend-stage">${STAGES.map(st => `<option value="${st}" ${S.amend.stage === st ? 'selected' : ''}>${STAGE_LABEL[st]}</option>`).join('')}</select></label>
          <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;color:${MUTED}">Date
            <input class="tm-field" type="date" id="tm-amend-date" value="${esc(S.amend.date || '')}"></label>
        </div>
        ${S.amend.err ? `<div style="font-size:12px;color:${RED};font-weight:500">${esc(S.amend.err)}</div>` : ''}
        <div style="display:flex;gap:8px"><button class="tm-btn-p" data-act="amend-save" ${S.busy ? 'disabled' : ''}>Save</button><button class="tm-btn-s" data-act="amend-cancel">Cancel</button></div>
      </div>` : '';
    return `${navBar(ids, idx, 'movement')}
      <div style="padding:20px 22px 16px;display:flex;flex-direction:column;gap:10px;border-bottom:1px solid ${SOFT}">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
          <span style="display:flex;align-items:center;gap:8px;min-width:0">
            <span class="tm-mono" style="font-size:12px;font-weight:600;padding:3px 7px;border-radius:5px;background:${INK};color:#fff">${m.wkLabel}</span>
            <span style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m.routeText)}</span>
          </span>${statusChip(m)}
        </div>
        <div class="tm-mono" style="font-size:22px;font-weight:600;letter-spacing:-.01em">${esc(m.ref)}</div>
        <div style="font-size:13px;color:${MUTED}">${esc([m.sub, c.vessel].filter(Boolean).join(' · '))}</div>
        <p style="margin:4px 0 0;font-size:15px;line-height:1.45">${esc(storyOf(m, M))}</p>
        ${m.health && m.health.why && (m.st === 'delayed' || m.st === 'behind') ? `<div style="font-size:12px;color:${MUTED}">${esc(m.health.why)}</div>` : ''}
      </div>
      ${d2dBlock(dd, m.mode)}
      <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));border-bottom:1px solid ${SOFT}">
        ${facts.map(f => `<div style="padding:14px 22px;border-top:1px solid #F3F3F1;display:flex;flex-direction:column;gap:4px">
          <span style="font-size:12px;color:${MUTED}">${esc(f.k)}</span>
          <span style="font-size:20px;font-weight:600;letter-spacing:-.01em;${f.late ? `color:${RED}` : ''}">${esc(f.v)}</span>
          <span style="font-size:12px;color:${MUTED}">${esc(f.n)}</span></div>`).join('')}
      </div>
      <button data-act="contents" data-uid="${esc(m.uid)}" style="margin:16px 22px 0;padding:14px 16px;border-radius:10px;border:1px solid #D6D6D2;display:flex;justify-content:space-between;align-items:center;gap:12px;min-height:64px">
        <span style="display:flex;flex-direction:column;gap:3px"><span style="font-size:14px;font-weight:600">What’s inside</span>
          <span class="tm-mono" style="font-size:12px;color:${MUTED}">${cs.pos != null ? `${plural(cs.pos, 'PO')} · ${plural(cs.skus || 0, 'SKU')} · ${fmtNum(cs.planned)} units` : plural(m.lanes, 'lane')}</span></span>
        <span style="display:flex;align-items:center;gap:6px;font-size:13px;font-weight:500">POs, SKUs, units ${I.arrow()}</span>
      </button>
      ${showTerm ? `<div style="margin:16px 22px 0;padding:14px 16px;border-radius:10px;background:#F8F0F3;display:flex;flex-direction:column;gap:8px">
        <div style="font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:${RED};font-weight:600">At the terminal</div>
        ${[['Terminal', term.terminal], ['Last free day', term.lfd ? fmtDay(term.lfd) : null], ['Holds', term.holds && term.holds.length ? term.holds.join(', ') : 'None reported'], ['Available for pickup', term.available_at ? fmtDay(term.available_at) : 'Not yet']]
          .filter(r => r[1]).map(r => `<div style="display:flex;justify-content:space-between;gap:12px;font-size:13px"><span style="color:${MUTED}">${r[0]}</span><span style="font-weight:500;text-align:right">${esc(r[1])}</span></div>`).join('')}
      </div>` : ''}
      <div class="tm-sec"><div class="tm-cap" style="padding-bottom:4px">Milestones</div>
        ${!m.air && term && term.tracking
          ? `<div style="display:flex;align-items:center;gap:8px;font-size:12px;color:${INK};padding:2px 0 8px"><span class="tm-live"></span><span><b style="font-weight:600">Live carrier tracking.</b> <span style="color:${MUTED}">Departure, arrival and availability come from the carrier’s own events as they happen${term.updated_at ? ` · last update ${esc(timeLabel(String(term.updated_at).replace(' ', 'T') + (String(term.updated_at).includes('Z') ? '' : 'Z')))}` : ''}.</span></span></div>`
          : `<div style="font-size:12px;color:${MUTED};padding:2px 0 8px">${m.air ? 'Air isn’t tracked automatically — each date is confirmed by the team.' : 'Not yet subscribed to carrier tracking — dates are planned or confirmed by the team.'}</div>`}
        ${STAGES.filter(st => m.ms[st]).map(st => { const x = m.ms[st]; return `<div class="tm-li" style="grid-template-columns:minmax(0,1fr) 80px 118px">
          <span>${STAGE_LABEL[st]}</span><span class="tm-mono" style="font-size:12px;${x.actual_at ? `color:${INK};font-weight:500` : `color:${MUTED}`}">${esc(fmtShort(x.actual_at || x.planned_at))}</span>${chipFor(x, M.today)}</div>`; }).join('')}
      </div>
      <div class="tm-sec"><div style="display:flex;justify-content:space-between" class="tm-cap"><span>Lanes on this movement</span></div>
        ${(c.lanes || []).map(k => { const p = String(k).split('||'); return `<div class="tm-li" style="grid-template-columns:minmax(0,1fr) auto"><span style="display:flex;flex-direction:column;gap:1px;min-width:0"><span style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(p[0] || k)}</span><span class="tm-mono" style="font-size:11px;color:${MUTED}">ZD ${esc(p[1] || '—')} · ${esc(p[2] || '')}</span></span></div>`; }).join('') || `<div style="font-size:13px;color:${MUTED};padding:8px 0">No lanes assigned.</div>`}
      </div>
      ${(c.events || []).length ? `<div class="tm-sec"><div class="tm-cap" style="padding-bottom:6px">${m.air ? 'Recorded' : 'Tracking events'}</div>
        ${c.events.map(e => `<div class="tm-li" style="grid-template-columns:92px minmax(0,1fr);align-items:start"><span class="tm-mono" style="font-size:12px;color:${MUTED}">${esc(timeLabel(e.at))}</span><span>${esc(e.text)}</span></div>`).join('')}</div>` : ''}
      <button class="tm-link" data-act="week-report" data-v="${m.wk}" style="margin:14px 22px 0">Transit performance from ${m.wkLabel} ${I.out()}</button>
      ${c.last_notification ? `<div style="margin:16px 22px 0;padding:10px 14px;border-radius:8px;background:#F1F8E4;box-shadow:inset 0 0 0 1px ${INK};font-size:13px;display:flex;gap:8px;align-items:center">${I.tick()}<span>Notification sent to ${plural(c.last_notification.to_count, 'recipient')} · ${esc(timeLabel(c.last_notification.sent_at))}</span></div>` : ''}
      ${amend}
      ${actions.length ? `<div style="padding:18px 22px 24px;display:flex;gap:10px;flex-wrap:wrap">${actions.join('')}</div>` : '<div style="height:24px"></div>'}`;
  }

  function renderUnassignedPanel(M, ids, idx) {
    const U = M.unassigned;
    const legacy = U.filter(u => u.legacy_dates);
    const legacyWeeks = [...new Set(legacy.map(u => u.week_start))];
    const L = S.legacy;
    return `${navBar(ids, idx, 'movement')}
      <div style="padding:20px 22px 16px;display:flex;flex-direction:column;gap:10px;border-bottom:1px solid ${SOFT}">
        <div style="display:flex;justify-content:space-between;align-items:center"><span class="tm-cap">Unassigned</span>
          <span style="height:24px;padding:0 10px;border-radius:12px;font-size:12px;font-weight:600;display:inline-flex;align-items:center;${legacy.length ? `background:${RED};color:#fff` : `background:${SOFT}`}">${plural(U.length, 'lane')}</span></div>
        <div class="tm-mono" style="font-size:22px;font-weight:600">Not on a movement</div>
        <p style="margin:4px 0 0;font-size:15px;line-height:1.45">These lanes aren’t on any container or flight.${legacy.length ? ` ${plural(legacy.length, 'lane')} still carr${legacy.length === 1 ? 'ies' : 'y'} dates typed onto the lane before dates moved to the movement — the old map read those and drew a ship that didn’t exist. Here they’re held aside until assigned.` : ' Assign them to a container or flight when it’s booked.'}</p>
      </div>
      <div class="tm-sec"><div class="tm-cap" style="display:flex;justify-content:space-between;padding-bottom:6px"><span>Lanes on no movement</span><span>POs</span></div>
        ${U.map(u => `<div class="tm-li" style="grid-template-columns:minmax(0,1fr) auto"><span style="display:flex;flex-direction:column;gap:1px;min-width:0"><span>${esc(u.supplier)}</span>
          <span class="tm-mono" style="font-size:11px;color:${MUTED}">${isoWeek(u.week_start)} · ZD ${esc(u.zendesk || '—')} · ${esc(u.freight || '')}${u.legacy_dates ? ` · old lane dates${u.legacy_departed ? ` (dep ${fmtShort(u.legacy_departed)})` : ''}` : ''}</span></span>
          <span class="tm-mono" style="font-size:13px">${u.pos || 0}</span></div>`).join('')}
      </div>
      ${L && L.preview ? `<div style="margin:16px 22px 0;padding:14px 16px;border:1px solid ${LINE};border-radius:10px;font-size:13px;display:flex;flex-direction:column;gap:10px">
        <div>This removes ${plural(L.preview.dates, 'typed lane date')} across ${esc(L.preview.weeks.map(isoWeek).join(', '))}. Container records, shipment references, notes, customs holds and everything invoicing reads are kept.</div>
        ${L.err ? `<div style="color:${RED};font-weight:500">${esc(L.err)}</div>` : ''}
        <div style="display:flex;gap:8px"><button class="tm-btn-p" data-act="legacy-apply" ${S.busy ? 'disabled' : ''}>Remove the old dates</button><button class="tm-btn-s" data-act="legacy-cancel">Cancel</button></div></div>` : ''}
      <div style="padding:18px 22px 24px;display:flex;gap:10px;flex-wrap:wrap">
        ${canEdit() ? `<button class="tm-btn-p" data-act="open-transit" data-v="${esc((U[0] || {}).week_start || M.thisWeek)}">Assign in Transit &amp; Clearing</button>` : ''}
        ${isInternal() && legacyWeeks.length && !(L && L.preview) ? `<button class="tm-btn-s" data-act="legacy-dry" ${S.busy ? 'disabled' : ''}>Clear old lane dates</button>` : ''}
      </div>`;
  }

  const ORIGIN_PILL = {
    complete: ['Complete', `background:${GREEN};color:${INK}`], in_progress: ['In progress', `background:${SOFT};color:${INK}`],
    not_started: ['Not started', `background:${SOFT};color:${MUTED}`], at_risk: ['At risk', `background:${AMBER};color:${INK}`],
    past_due: ['Past due', `background:${RED};color:#fff`],
  };
  function originPanel(O) {
    if (!O) return '';
    const pill = (s) => { const p = ORIGIN_PILL[s] || ORIGIN_PILL.in_progress; return `<span style="height:22px;padding:0 9px;border-radius:11px;font-size:11px;font-weight:600;display:inline-flex;align-items:center;${p[1]}">${p[0]}</span>`; };
    const r = O.received, v = O.vas;
    const row = (title, big, status, lines) => `<div style="display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:start;padding:12px 0;border-top:1px solid #F3F3F1">
        <div style="display:flex;flex-direction:column;gap:3px"><span style="font-size:13px;font-weight:600">${title}</span>
          ${lines.filter(Boolean).map(l => `<span style="font-size:12px;color:${MUTED}">${esc(l)}</span>`).join('')}</div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px"><span class="tm-mono" style="font-size:22px;font-weight:600">${big}</span>${pill(status)}</div></div>`;
    return `<div class="tm-sec" style="padding-bottom:12px;border-bottom:1px solid ${SOFT}">
      <div style="display:flex;justify-content:space-between;align-items:baseline" class="tm-cap"><span>Origin</span><span style="text-transform:none;letter-spacing:0">factory gate → VAS done</span></div>
      ${row('Ex-factory (received)', `${r.pct}%`, r.status, [
        `${r.pos_received} of ${plural(r.pos, 'PO')} received${r.closed_by_tick ? ` · ${r.closed_by_tick} closed by the lane tick` : ''}`,
        r.late_pos ? `${plural(r.late_pos, 'PO')} received after ${r.late_pos === 1 ? 'its' : 'their'} due date` : null,
        r.done_at ? `Done ${fmtDay(r.done_at)} · target ${fmtDay(r.target)}` : `Target ${fmtDay(r.target)}`])}
      ${row('VAS complete', `${v.lanes_complete}/${v.lanes}`, v.status, [
        `${plural(v.lanes_complete, 'lane')} of ${v.lanes} complete · ${fmtNum(v.units_applied)} of ${fmtNum(v.units_planned)} units applied (${v.units_pct}%)`,
        v.done_at ? `Done ${fmtDay(v.done_at)} · target ${fmtDay(v.target)}` : `Target ${fmtDay(v.target)}`])}
      ${(O.suppliers_behind || []).length ? `<div style="font-size:12px;color:${MUTED};padding-top:4px">Furthest behind: ${esc(O.suppliers_behind.map(s => `${s.name} ${s.pct}%`).join(' · '))}</div>` : ''}
    </div>`;
  }

  function renderWeekPanel(M) {
    const ids = navIds(M), idx = ids.indexOf(S.wkSel);
    const list = M.timeline.filter(m => m.wk === S.wkSel);
    if (!list.length) return '';
    const lanes = list.reduce((a, m) => a + m.lanes, 0), pos = list.reduce((a, m) => a + m.pos, 0);
    const modes = ['sea', 'air'].map(md => {
      const xs = list.filter(m => m.mode === md).map(d2dOf).filter(Boolean);
      if (!xs.length) return '';
      const tp = Math.round(xs.reduce((a, x) => a + x.tp, 0) / xs.length), ta = Math.round(xs.reduce((a, x) => a + x.ta, 0) / xs.length), dl = ta - tp;
      return `<span style="font-size:13px;font-weight:600">${md === 'sea' ? 'Sea' : 'Air'}</span>
        <span class="tm-mono" style="font-size:24px;font-weight:600">${tp}d</span><span class="tm-mono" style="font-size:24px;font-weight:600">${ta}d</span>
        <span class="tm-mono" style="font-size:24px;font-weight:600;${dl > 0 ? `color:${RED}` : ''}">${dl === 0 ? '±0d' : dl > 0 ? '+' + dl + 'd' : dl + 'd'}</span>`;
    }).join('');
    const allDone = list.every(m => m.delivered);
    const hl = highlights(M).filter(h => h.wk === isoWeek(S.wkSel));
    return `${navBar(ids, idx, 'week')}
      <div style="padding:20px 22px 16px;display:flex;flex-direction:column;gap:8px;border-bottom:1px solid ${SOFT}">
        <span style="display:flex;align-items:center;gap:8px"><span class="tm-mono" style="font-size:12px;font-weight:600;padding:3px 7px;border-radius:5px;background:${INK};color:#fff">${isoWeek(S.wkSel)}</span><span class="tm-cap">Execution week</span></span>
        <div style="font-size:22px;font-weight:600;letter-spacing:-.01em">Week of ${fmtShort(S.wkSel)}</div>
        <div style="font-size:13px;color:${MUTED}">${plural(list.length, 'movement')} · ${plural(lanes, 'lane')} · ${plural(pos, 'PO')}</div>
      </div>
      ${originPanel((M.B.weeks_origin || {})[S.wkSel])}
      ${modes ? `<div style="padding:16px 22px 18px;border-bottom:1px solid ${SOFT};display:flex;flex-direction:column;gap:12px">
        <div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px"><span class="tm-cap">Door to door · average</span><span style="font-size:12px;color:${MUTED}">packing list → received at FC</span></div>
        <div style="display:grid;grid-template-columns:56px repeat(3,minmax(0,1fr));gap:4px 12px;align-items:baseline">
          <span></span><span style="font-size:12px;color:${MUTED}">D2D plan</span><span style="font-size:12px;color:${MUTED}">D2D actual</span><span style="font-size:12px;color:${MUTED}">Variance</span>${modes}</div>
        <span style="font-size:12px;color:${MUTED}">Averaged by mode — sea and air are never blended${allDone ? '.' : '. Actuals include legs not yet confirmed or tracked.'}</span>
      </div>` : ''}
      <div class="tm-sec"><div class="tm-cap" style="padding-bottom:6px">Movements in this week</div>
        ${list.map(m => { const dd = d2dOf(m); return `<button class="tm-row" data-act="pick" data-uid="${esc(m.uid)}" style="display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;padding:12px 8px;margin:0 -8px;border-top:1px solid #F3F3F1;min-height:60px;border-radius:6px">
          <span style="display:flex;flex-direction:column;gap:3px;min-width:0"><span style="display:flex;align-items:center;gap:8px">${dot(m.st, 8)}<span class="tm-mono" style="font-size:13px;font-weight:600">${esc(m.ref)}</span></span>
          <span style="font-size:12px;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m.where)} · ${plural(m.lanes, 'lane')} · ${plural(m.pos, 'PO')}</span></span>
          <span style="display:flex;flex-direction:column;gap:3px;align-items:flex-end">${dd ? `<span class="tm-mono" style="font-size:13px;font-weight:600;${dd.delta > 0 ? `color:${RED}` : ''}">${dd.tp}d → ${dd.ta}d${dd.delta > 0 ? '  +' + dd.delta : ''}</span>` : ''}<span style="font-size:12px;color:${MUTED}">FC ${esc(fmtDay(m.fc))}</span></span></button>`; }).join('')}
      </div>
      ${hl.length ? `<div class="tm-sec"><div class="tm-cap" style="padding-bottom:6px">Updates this week</div>
        ${hl.map(h => `<button class="tm-row" data-act="pick" data-uid="${esc(h.uid)}" style="display:grid;grid-template-columns:3px minmax(0,1fr);gap:12px;padding:10px 8px;margin:0 -8px;border-top:1px solid #F3F3F1;border-radius:6px;min-height:52px">
          <span style="border-radius:2px;background:${SEV[h.sev]}"></span><span style="display:flex;flex-direction:column;gap:2px"><span style="font-size:13px;font-weight:500">${esc(h.title)}</span><span style="font-size:12px;color:${MUTED}">${esc(h.sub)}</span></span></button>`).join('')}</div>` : ''}
      <div style="padding:16px 22px 24px"><button class="tm-link" data-act="week-report" data-v="${S.wkSel}">Transit performance from ${isoWeek(S.wkSel)} ${I.out()}</button></div>`;
  }

  // ════ Contents sheet ════
  function renderSheet(M) {
    const m = M.all.find(x => x.uid === S.sheet);
    if (!m) return '';
    const data = S.contents[m.uid], err = S.contentsErr[m.uid];
    const drift = m.base && m.fc ? diff(m.base, m.fc) : null;
    const head = `<div style="padding:22px 26px 18px;display:flex;justify-content:space-between;gap:20px;align-items:flex-start;border-bottom:1px solid ${SOFT};flex-wrap:wrap">
        <div style="display:flex;flex-direction:column;gap:6px">
          <span style="display:flex;align-items:center;gap:8px"><span class="tm-mono" style="font-size:12px;font-weight:600;padding:3px 7px;border-radius:5px;background:${INK};color:#fff">${m.wkLabel}</span><span class="tm-cap">${esc(m.routeText)}</span></span>
          <span class="tm-mono" style="font-size:24px;font-weight:600">${esc(m.ref)}</span>
          <span style="font-size:14px;font-weight:600;${drift > 0 ? `color:${RED}` : `color:${MUTED}`}">${drift > 0 ? `FC now ${fmtDay(m.fc)} — ${plural(drift, 'day')} after first promise (${fmtDay(m.base)})` : `FC ${m.delivered ? 'received' : ''} ${fmtDay(m.fc)}${m.base ? (drift < 0 ? ` · ${plural(-drift, 'day')} early` : ' · as first promised') : ''}`}</span>
        </div>
        <div style="display:flex;gap:10px;align-items:center">
          <button class="tm-btn-s" data-act="sheet-xlsx" data-uid="${esc(m.uid)}">${I.dl()} Export XLSX</button>
          <button class="tm-sq" data-act="sheet-close" aria-label="Close" style="width:44px;height:44px">${I.x()}</button>
        </div></div>`;
    if (err) return `<div class="tm-modal" style="max-width:1180px">${head}<div style="padding:30px 26px;color:${RED};font-size:14px">${esc(err)}</div></div>`;
    if (!data) return `<div class="tm-modal" style="max-width:1180px">${head}<div style="padding:26px;display:flex;flex-direction:column;gap:10px"><div class="tm-skel" style="height:60px"></div><div class="tm-skel" style="height:300px"></div></div></div>`;
    const lanes = data.lanes || [];
    const late = (l) => !!(l.latest_arrival && m.fc && m.fc > l.latest_arrival);
    const riskPos = lanes.filter(late).reduce((a, l) => a + l.pos.length, 0);
    const li = clamp(S.sheetLane, 0, Math.max(0, lanes.length - 1));
    const lane = lanes[li];
    const t = data.totals || {};
    const tiles = [
      ['POs', fmtNum(t.pos)], ['SKUs', fmtNum(t.skus)],
      [data.processed_available ? 'Units processed / planned' : 'Units planned', data.processed_available ? `${fmtNum(t.processed)} / ${fmtNum(t.planned)}` : fmtNum(t.planned)],
      ['POs past latest arrival', String(riskPos), riskPos ? RED : null],
    ];
    const grid = 'display:grid;grid-template-columns:150px minmax(0,1fr) 110px 110px;gap:12px;align-items:center';
    const table = lane ? lane.pos.map(p => {
      const rows = [`<div style="${grid};padding:10px 22px;border-top:1px solid ${SOFT};background:#FAFAF8">
          <span class="tm-mono" style="font-size:13px;font-weight:600">${esc(p.po)}</span><span style="font-size:12px">${plural(p.skus.length, 'SKU')}</span>
          <span class="tm-mono" style="font-size:13px;font-weight:600;text-align:right">${fmtNum(p.planned)}</span>
          <span class="tm-mono" style="font-size:13px;font-weight:600;text-align:right">${p.processed == null ? '—' : fmtNum(p.processed)}</span></div>`];
      for (const s of p.skus) rows.push(`<div style="${grid};padding:7px 22px">
          <span></span><span style="display:flex;gap:10px;min-width:0"><span class="tm-mono" style="font-size:12px;white-space:nowrap">${esc(s.sku)}</span><span style="font-size:12px;color:${MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(s.description || '')}</span></span>
          <span class="tm-mono" style="font-size:12px;text-align:right;color:${MUTED}">${fmtNum(s.planned)}</span>
          <span class="tm-mono" style="font-size:12px;text-align:right;color:${s.processed != null && s.processed < s.planned ? RED : MUTED}">${s.processed == null ? '—' : fmtNum(s.processed)}</span></div>`);
      return rows.join('');
    }).join('') : '';
    return `<div class="tm-modal" role="dialog" aria-label="Movement contents" style="max-width:1180px">${head}
      <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));border-bottom:1px solid ${SOFT}">
        ${tiles.map(x => `<div style="padding:16px 26px;display:flex;flex-direction:column;gap:4px;border-right:1px solid #F3F3F1"><span style="font-size:12px;color:${MUTED}">${x[0]}</span><span class="tm-mono" style="font-size:22px;font-weight:600;${x[2] ? `color:${x[2]}` : ''}">${x[1]}</span></div>`).join('')}
      </div>
      ${!lanes.length ? `<div style="padding:30px 26px;font-size:14px;color:${MUTED}">No lanes are assigned to this movement yet.</div>` : `
      <div style="display:grid;grid-template-columns:300px minmax(0,1fr);min-height:460px">
        <div style="border-right:1px solid ${SOFT};display:flex;flex-direction:column;padding:8px 0;max-height:520px;overflow-y:auto">
          ${lanes.map((l, i) => `<button data-act="sheet-lane" data-v="${i}" style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 18px;min-height:52px;width:100%;${i === li ? `background:#F7F2F4;box-shadow:inset 3px 0 0 ${RED}` : ''}">
            <span style="display:flex;flex-direction:column;gap:2px;min-width:0"><span style="font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(l.supplier)}</span>
            <span class="tm-mono" style="font-size:11px;color:${MUTED}">ZD ${esc(l.zendesk || '—')} · ${plural(l.pos.length, 'PO')}</span></span>
            ${late(l) ? `<span style="font-size:11px;font-weight:600;color:#fff;background:${RED};padding:2px 7px;border-radius:9px">Late</span>` : ''}</button>`).join('')}
        </div>
        <div style="display:flex;flex-direction:column;min-width:0">
          <div style="display:flex;justify-content:space-between;align-items:center;padding:14px 22px;border-bottom:1px solid ${SOFT};gap:12px;flex-wrap:wrap">
            <div style="display:flex;flex-direction:column;gap:2px"><span style="font-size:15px;font-weight:600">${esc(lane.supplier)}</span>
              <span style="font-size:12px;${late(lane) ? `color:${RED};font-weight:600` : `color:${MUTED}`}">${lane.latest_arrival ? (late(lane) ? `Lane latest arrival ${fmtDay(lane.latest_arrival)} — misses by ${plural(diff(lane.latest_arrival, m.fc), 'day')}` : `Within its latest arrival date (${fmtDay(lane.latest_arrival)})`) : 'No latest arrival date on this lane'}</span></div>
            <span class="tm-mono" style="font-size:12px;color:${MUTED}">${plural(lane.pos.length, 'PO')} · ${plural(lane.skus || 0, 'SKU')} · ${fmtNum(lane.planned)} units</span>
          </div>
          <div style="${grid};padding:10px 22px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};border-bottom:1px solid ${SOFT}"><span>PO</span><span>SKU</span><span style="text-align:right">Planned</span><span style="text-align:right">Processed</span></div>
          <div style="max-height:440px;overflow-y:auto" data-tm-scroll="sheet">${table || `<div style="padding:20px 22px;font-size:13px;color:${MUTED}">No plan rows match this lane.</div>`}</div>
        </div>
      </div>`}
    </div>`;
  }

  // ════ Notification ════
  function draftFor(m, M) {
    const c = m.c;
    const N = stageDates(m, 'now');
    const drift = m.base && m.fc ? diff(m.base, m.fc) : null;
    const prevEta = c.estimate_prev && c.estimate_prev.carrier_eta;
    const dock = ((S.alerts || []).find(a => a.consignment_uid === m.uid && a.dock_at) || {}).dock_at || null;
    const route = `${portLabel(m.route.origin) || 'origin'} → ${portLabel(m.route.dest) || 'destination'}`;
    let subject;
    if (m.held && m.term) subject = `${m.ref} (${m.wkLabel}) — held at ${m.term.terminal || 'the terminal'}`;
    else if (drift > 0) subject = `${m.ref} (${m.wkLabel}) — delivery to FC now ${fmtDay(m.fc)}, ${plural(drift, 'day')} later than planned`;
    else if (drift < 0) subject = `${m.ref} (${m.wkLabel}) — delivery to FC now ${fmtDay(m.fc)}, earlier than planned`;
    else subject = `${m.ref} (${m.wkLabel}) — status update`;
    const p = ['Hi team,'];
    if (m.held && m.term) p.push(`${m.ref} (${m.wkLabel}, ${route}) is being held at ${m.term.terminal || 'the terminal'}${m.term.holds.length ? ` — ${m.term.holds.join(', ').toLowerCase()} hold` : ''}.${m.term.lfd ? ` The last free day is ${fmtDay(m.term.lfd)}.` : ''}`);
    else if (prevEta && c.carrier_eta && !m.air) p.push(`${c.carrier_name || 'The carrier'} has revised the arrival of ${m.ref} (${m.wkLabel}, ${route}) from ${fmtDay(prevEta)} to ${fmtDay(c.carrier_eta)}.`);
    else p.push(`An update on ${m.ref} (${m.wkLabel}, ${route}): ${m.where.toLowerCase()}.`);
    if (m.fc) p.push(`We now expect delivery to the FC on ${fmtDay(m.fc)}${drift > 0 ? `, ${plural(drift, 'day')} later than the ${fmtDay(m.base)} we first planned` : drift < 0 ? `, ${plural(-drift, 'day')} earlier than first planned` : m.base ? ', as planned' : ''}.${dock && m.fc > dock ? ` Please move the dock booking for ${fmtDay(dock)}.` : ''}`);
    p.push(`The ${m.air ? 'shipment' : 'container'} carries ${plural(m.lanes, 'lane')} and ${plural(m.pos, 'PO')}.${S.nAttach ? ' The full PO and SKU list is attached.' : ''}`);
    p.push('We’ll update you again if the date changes.');
    p.push('— VelOzity Operations');
    return { subject, body: p.join('\n\n') };
  }

  function renderNotify(M) {
    const m = M.all.find(x => x.uid === S.notify);
    if (!m) return '';
    const d = draftFor(m, M);
    const subject = S.nSubject != null ? S.nSubject : d.subject;
    const body = S.nBody != null ? S.nBody : d.body;
    const edited = S.nSubject != null || S.nBody != null;
    const n = S.nTo.length;
    return `<div class="tm-modal" role="dialog" aria-label="Send notification" style="max-width:640px">
      <div style="padding:20px 24px 14px;display:flex;justify-content:space-between;align-items:flex-start;gap:16px;border-bottom:1px solid ${SOFT}">
        <div style="display:flex;flex-direction:column;gap:4px"><span style="font-size:18px;font-weight:600">Send notification</span>
          <span style="font-size:13px;color:${MUTED}">${esc(`${m.wkLabel} · ${m.ref} · ${plural(m.lanes, 'lane')} · ${plural(m.pos, 'PO')}`)}</span></div>
        <button class="tm-sq" data-act="notify-close" aria-label="Close">${I.x()}</button>
      </div>
      <div style="padding:16px 24px;display:flex;flex-direction:column;gap:14px">
        <div style="display:grid;grid-template-columns:64px minmax(0,1fr);gap:12px;align-items:center;font-size:13px"><span style="color:${MUTED}">From</span><span class="tm-mono">operations@velozity.au</span></div>
        <div style="display:grid;grid-template-columns:64px minmax(0,1fr);gap:12px;align-items:start;font-size:13px">
          <label for="tm-to" style="color:${MUTED};padding-top:10px">To</label>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="display:flex;flex-wrap:wrap;gap:6px;padding:6px;border:1px solid #D6D6D2;border-radius:8px;min-height:44px;align-items:center">
              ${S.nTo.map(e => `<span style="display:inline-flex;align-items:center;gap:4px;height:28px;padding:0 4px 0 10px;border-radius:14px;background:#F0F0ED;font-size:12px"><span class="tm-mono">${esc(e)}</span>
                <button data-act="chip-remove" data-v="${esc(e)}" aria-label="Remove ${esc(e)}" style="width:22px;height:22px;border-radius:11px;display:flex;align-items:center;justify-content:center">${I.x(12)}</button></span>`).join('')}
              <input id="tm-to" type="email" value="${esc(S.nDraft)}" placeholder="Add an email, press Enter" autocomplete="off" list="tm-to-list" style="flex:1 1 180px;min-width:160px;height:30px;border:0;outline:none;font:inherit;font-size:13px;background:transparent">
              <datalist id="tm-to-list">${(S.nRecipients || []).filter(e => !S.nTo.includes(e)).map(e => `<option value="${esc(e)}"></option>`).join('')}</datalist>
            </div>
            ${S.nErr ? `<span style="font-size:12px;color:${RED};font-weight:500">${esc(S.nErr)}</span>` : ''}
            <span style="font-size:12px;color:${MUTED}">Remembered for this client — recipients you add are offered next time.</span>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:64px minmax(0,1fr);gap:12px;align-items:center;font-size:13px">
          <label for="tm-subj" style="color:${MUTED}">Subject</label>
          <input id="tm-subj" class="tm-field" type="text" value="${esc(subject)}" style="font-weight:500"></div>
        <div style="display:flex;flex-direction:column;gap:6px">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:10px">
            <label for="tm-body" id="tm-body-note" style="font-size:12px;color:${MUTED}">${edited ? 'Edited — your changes are sent as written' : 'Drafted from this movement’s dates — edit as needed'}</label>
            <button data-act="notify-reset" id="tm-reset" style="font-size:12px;font-weight:500;text-decoration:underline;text-underline-offset:3px;min-height:28px;${edited ? '' : 'display:none'}">Reset to draft</button>
          </div>
          <textarea id="tm-body" rows="11" style="width:100%;padding:14px 16px;border:1px solid #D6D6D2;border-radius:10px;background:#FAFAF8;font:inherit;font-size:14px;line-height:1.5;color:${INK};resize:vertical">${esc(body)}</textarea>
        </div>
        <label style="display:flex;align-items:center;gap:10px;font-size:13px;min-height:32px;cursor:pointer">
          <input id="tm-attach" type="checkbox" ${S.nAttach ? 'checked' : ''} style="width:18px;height:18px;accent-color:${INK};margin:0">
          <span>Attach the PO &amp; SKU list (XLSX) — <span class="tm-mono">${plural(m.pos, 'PO')}</span></span></label>
      </div>
      <div style="padding:14px 24px 20px;border-top:1px solid ${SOFT};display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
        <span style="font-size:12px;color:${MUTED}">Replies go to operations@velozity.au · logged on this movement</span>
        <div style="display:flex;gap:10px">
          <button class="tm-btn-s" data-act="notify-close">Cancel</button>
          <button class="tm-btn-p" data-act="notify-send" ${!n || S.nSending ? 'disabled' : ''}>${S.nSending ? 'Sending…' : n ? `Send to ${plural(n, 'recipient')}` : 'Add a recipient'}</button>
        </div>
      </div>
    </div>`;
  }

  // ════ Overlays ════
  function overlayHost() {
    let ov = document.getElementById('tm-overlay');
    if (!ov) { ov = document.createElement('div'); ov.id = 'tm-overlay'; ov.className = 'tm'; ov.style.cssText = 'padding:0;max-width:none;margin:0;display:contents'; document.body.appendChild(ov); bind(ov); }
    return ov;
  }
  function renderOverlays(M) {
    const ov = overlayHost();
    const keep = {};
    for (const el of ov.querySelectorAll('[data-tm-scroll]')) keep[el.getAttribute('data-tm-scroll')] = el.scrollTop;
    const panelEl = ov.querySelector('.tm-panel');
    const panelScroll = panelEl ? panelEl.scrollTop : 0;
    const prevPanelKey = ov.getAttribute('data-panel-key');
    let html = '';
    if (S.full) html += `<div class="tm-ov" style="z-index:2147482900;align-items:center;justify-content:center;padding:24px"><div class="tm-ov-dim" data-act="full-close" style="background:rgba(10,10,10,.6)"></div><div style="position:relative;width:100%;display:flex;justify-content:center">${renderCorridor(M, true)}</div></div>`;
    let panelKey = '';
    // The week report sits under the movement panel and the contents sheet, so both open on top of it.
    if (S.report) html += `<div class="tm-modal-wrap" data-act="report-bg" style="z-index:2147482950;align-items:center;padding:4vh 2vw">${renderWeekReport(M)}</div>`;
    if (S.panel === 'mv' && S.sel) { panelKey = 'mv:' + S.sel; html += `<div class="tm-ov"><div class="tm-ov-dim" data-act="panel-close"></div><aside class="tm-panel" role="dialog" aria-label="Movement detail" ${prevPanelKey === panelKey ? 'style="animation:none"' : ''}>${renderMovementPanel(M)}</aside></div>`; }

    if (S.sheet) html += `<div class="tm-modal-wrap" data-act="sheet-bg">${renderSheet(M)}</div>`;
    if (S.notify) html += `<div class="tm-modal-wrap" style="z-index:2147483150">${renderNotify(M)}</div>`;
    if (S.toast) html += `<div class="tm-toast" role="status">${esc(S.toast)}</div>`;
    ov.innerHTML = html;
    ov.setAttribute('data-panel-key', panelKey);
    const np = ov.querySelector('.tm-panel');
    if (np && prevPanelKey === panelKey) np.scrollTop = panelScroll;
    for (const el of ov.querySelectorAll('[data-tm-scroll]')) { const k = el.getAttribute('data-tm-scroll'); if (k in keep) el.scrollTop = keep[k]; }
    const lock = S.full || S.panel || S.sheet || S.notify || S.report;
    document.documentElement.style.overflow = lock ? 'hidden' : '';
  }

  // ════ Week report ════
  // Everything about one execution week in one place, for everyone: how it went in a few
  // lines, the journey from factory gate to FC, the numbers, each movement, what changed,
  // and what is not yet on a container. Opened from the week pills or a week row.
  const RANK = { delayed: 0, behind: 1, on_time: 2, no_quote: 3, not_booked: 4 };
  function reportWeeks(M) {
    const w = new Set(M.liveWeeks);
    if ((M.B.weeks_origin || {})[M.thisWeek] || M.all.some(m => m.wk === M.thisWeek)) w.add(M.thisWeek);
    return [...w].sort();
  }
  function renderWeekPills(M) {
    const weeks = reportWeeks(M);
    if (!weeks.length) return '';
    const pills = weeks.map(w => {
      const list = M.all.filter(m => m.wk === w);
      const worst = list.slice().sort((a, b) => (RANK[a.st] ?? 9) - (RANK[b.st] ?? 9))[0];
      return `<button data-act="report-open" data-v="${w}" title="${esc(`Week report · ${isoWeek(w)} · week of ${fmtShort(w)}`)}" style="display:inline-flex;align-items:center;gap:7px;height:32px;padding:0 12px;border-radius:16px;background:#fff;border:1px solid #D6D6D2;font-size:12.5px;font-weight:600">
        ${worst ? dot(worst.st, 8) : `<span style="width:8px;height:8px;border-radius:50%;border:1.5px solid ${GREY};display:inline-block"></span>`}
        <span class="tm-mono">${isoWeek(w)}</span>
        <span style="font-weight:500;color:${MUTED}">${list.length}</span></button>`;
    }).join('');
    return `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:-6px">
      <span style="font-size:12px;color:${MUTED};margin-right:2px">Week reports</span>${pills}</div>`;
  }

  async function openWeekReport(ws) {
    S.report = ws; S.panel = null;
    render();
    if (!S.summaries) S.summaries = {};
    const cur = S.summaries[ws];
    if (cur && (cur.loading || (Date.now() - cur.at < 60000))) return;
    S.summaries[ws] = { loading: true, at: Date.now() };
    let out = { at: Date.now() };
    try {
      const g = await req('GET', `/consignments/week-summary?week=${encodeURIComponent(ws)}`);
      out.plain = g.plain; out.summary = g.summary; out.generated_at = g.generated_at;
      if (!g.summary && g.can_write) {
        try {
          const p = await req('POST', '/consignments/week-summary/generate', { week: ws });
          if (p && p.summary) { out.summary = p.summary; out.generated_at = p.generated_at; }
          else if (p && p.discarded) out.note = 'Pulse’s draft didn’t match the data exactly, so the plain summary is shown.';
        } catch (e) {
          out.note = (e.data && (e.data.error === 'pulse_off' || e.data.error === 'pulse_disabled'))
            ? 'Switch Pulse on for a written summary of this week.' : null;
        }
      }
    } catch (e) { out.err = 'Couldn’t load the summary.'; }
    S.summaries[ws] = out;
    if (S.report === ws) render();
  }

  function journeyStep(label, value, sub, status) {
    const P = ORIGIN_PILL[status] || (status === 'not_confirmed' ? ['Not confirmed', `background:#fff;color:${RED};box-shadow:inset 0 0 0 1.5px ${RED}`]
      : status === 'upcoming' ? ['Upcoming', `background:${SOFT};color:${MUTED}`] : ORIGIN_PILL.in_progress);
    return `<div style="flex:1 1 150px;min-width:140px;display:flex;flex-direction:column;gap:6px;padding:14px 16px;border:1px solid ${LINE};border-radius:10px;background:#fff">
      <span style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};font-weight:600">${esc(label)}</span>
      <span class="tm-mono" style="font-size:22px;font-weight:600">${esc(value)}</span>
      <span style="font-size:12px;color:${MUTED};min-height:16px">${esc(sub || '')}</span>
      <span style="align-self:flex-start;height:22px;padding:0 9px;border-radius:11px;font-size:11px;font-weight:600;display:inline-flex;align-items:center;${P[1]}">${P[0]}</span></div>`;
  }
  function stageStep(list, st, label, M) {
    const n = list.length;
    if (!n) return journeyStep(label, '—', 'No movements yet', 'not_started');
    const done = list.filter(m => m.ms[st] && m.ms[st].actual_at);
    const late = list.filter(m => m.ms[st] && !m.ms[st].actual_at && m.ms[st].planned_at && ymd(m.ms[st].planned_at) < M.today);
    const dates = done.map(m => ymd(m.ms[st].actual_at)).sort();
    const next = list.filter(m => m.ms[st] && !m.ms[st].actual_at).map(m => ymd(m.ms[st].planned_at)).filter(Boolean).sort();
    const status = done.length === n ? 'complete' : late.length ? 'not_confirmed' : done.length ? 'in_progress' : 'upcoming';
    const sub = done.length === n ? `Last ${fmtDay(dates[dates.length - 1])}` : late.length ? `${plural(late.length, 'movement')} past the planned date` : next.length ? `Next planned ${fmtDay(next[0])}` : '';
    return journeyStep(label, `${done.length}/${n}`, sub, status);
  }

  function movementCard(m, M) {
    const st = STATUS[m.st] || STATUS.on_time;
    const drift = m.base && m.fc ? diff(m.base, m.fc) : null;
    const dots = STAGES.filter(s => m.ms[s]).map(s => {
      const x = m.ms[s]; const p = ymd(x.planned_at);
      const look = x.actual_at ? `background:${GREEN};border:1.5px solid ${INK}` : (p && p < M.today) ? `background:#fff;border:2px solid ${RED}` : `background:#fff;border:1.5px dashed #8A8A8A`;
      return `<span title="${esc(`${STAGE_LABEL[s]} · ${fmtDay(x.actual_at || x.planned_at)} · ${x.actual_at ? (x.state === 'carrier' ? 'live carrier event' : 'confirmed') : (p && p < M.today ? 'not confirmed' : 'planned')}`)}" style="width:12px;height:12px;border-radius:50%;box-sizing:border-box;flex-shrink:0;${look}"></span>`;
    }).join(`<span style="flex:1;height:2px;background:#E3E3E0;min-width:6px"></span>`);
    const cs = m.c.contents_summary || {};
    return `<div style="border:1px solid ${LINE};border-radius:12px;padding:14px 16px;display:flex;flex-direction:column;gap:10px;background:#fff">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:10px">
        <span style="display:flex;align-items:center;gap:10px;min-width:0"><span style="display:inline-flex;color:${INK}">${modeIcon(m.mode, 16)}</span>
          <span class="tm-mono" style="font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m.ref)}</span></span>
        ${statusChip(m)}</div>
      <div style="font-size:12px;color:${MUTED}">${esc(m.routeText)} · ${esc(m.where)}</div>
      <div style="display:flex;align-items:center;gap:0">${dots}</div>
      <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;font-size:13px">
        <span><b style="font-weight:600">FC ${esc(fmtDay(m.fc))}</b>${drift ? ` <span style="color:${drift > 0 ? RED : '#4A6A00'};font-weight:600">${drift > 0 ? '+' : ''}${drift}d vs first promise</span>` : m.base ? ` <span style="color:${MUTED}">as first promised</span>` : ''}</span>
        <span class="tm-mono" style="font-size:12px;color:${MUTED}">${plural(m.lanes, 'lane')} · ${plural(cs.pos || 0, 'PO')} · ${fmtNum(cs.planned || 0)} units</span></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="tm-btn" style="height:32px;font-size:12px" data-act="contents" data-uid="${esc(m.uid)}">POs &amp; SKUs</button>
        <button class="tm-btn" style="height:32px;font-size:12px" data-act="pick" data-uid="${esc(m.uid)}">Details</button></div>
    </div>`;
  }

  function renderWeekReport(M) {
    const ws = S.report;
    const weeks = reportWeeks(M);
    const list = M.all.filter(m => m.wk === ws).sort((a, b) => (RANK[a.st] ?? 9) - (RANK[b.st] ?? 9));
    const O = (M.B.weeks_origin || {})[ws];
    const sm = (S.summaries || {})[ws] || { loading: true };
    const idx = weeks.indexOf(ws);
    const cs = list.reduce((a, m) => { const x = m.c.contents_summary || {}; a.lanes += m.lanes; a.pos += x.pos || 0; a.skus += x.skus || 0; a.units += x.planned || 0; return a; }, { lanes: 0, pos: 0, skus: 0, units: 0 });
    const measured = list.filter(m => ['on_time', 'behind', 'delayed'].includes(m.st));
    const onTime = measured.filter(m => m.st === 'on_time').length;
    const sea = list.filter(m => m.mode === 'sea').length, air = list.length - sea;
    const modes = ['sea', 'air'].map(md => {
      const xs = list.filter(m => m.mode === md).map(d2dOf).filter(Boolean);
      if (!xs.length) return '';
      const tp = Math.round(xs.reduce((a, x) => a + x.tp, 0) / xs.length), ta = Math.round(xs.reduce((a, x) => a + x.ta, 0) / xs.length), dl = ta - tp;
      return `<span style="font-size:13px;font-weight:600">${md === 'sea' ? 'Sea' : 'Air'}</span><span class="tm-mono" style="font-size:20px;font-weight:600">${tp}d</span><span class="tm-mono" style="font-size:20px;font-weight:600">${ta}d</span><span class="tm-mono" style="font-size:20px;font-weight:600;${dl > 0 ? `color:${RED}` : ''}">${dl === 0 ? '±0d' : dl > 0 ? '+' + dl + 'd' : dl + 'd'}</span>`;
    }).join('');
    const H = highlights(M).filter(h => h.wkStart === ws);
    const un = (M.B.unassigned || []).filter(u => u.week_start === ws);
    const num = (k, v, sub) => `<div style="display:flex;flex-direction:column;gap:2px;padding:12px 16px;border-right:1px solid #F3F3F1;min-width:110px"><span style="font-size:12px;color:${MUTED}">${k}</span><span class="tm-mono" style="font-size:22px;font-weight:600">${v}</span>${sub ? `<span style="font-size:11.5px;color:${MUTED}">${sub}</span>` : ''}</div>`;
    const context = sm.loading ? `<div class="tm-skel" style="height:54px"></div>`
      : `<p style="margin:0;font-size:16px;line-height:1.55">${esc(sm.summary || sm.plain || 'No summary yet.')}</p>
         <div style="font-size:12px;color:${MUTED}">${sm.summary ? `Written by Pulse from Pinpoint data${sm.generated_at ? ` · ${esc(timeLabel(sm.generated_at))}` : ''}` : esc(sm.note || 'Summary from Pinpoint data')}</div>`;
    const sec = (title, inner, right) => `<section style="display:flex;flex-direction:column;gap:12px"><div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px"><h3 style="margin:0;font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:${MUTED};font-weight:600">${title}</h3>${right || ''}</div>${inner}</section>`;
    return `<div class="tm-modal" role="dialog" aria-label="Week report ${isoWeek(ws)}" style="max-width:none;width:min(1400px,80vw);max-height:88vh;overflow-y:auto">
      <div style="position:sticky;top:0;z-index:2;background:#fff;display:flex;justify-content:space-between;align-items:center;gap:16px;padding:18px 28px;border-bottom:1px solid ${SOFT};flex-wrap:wrap">
        <div style="display:flex;align-items:center;gap:12px;min-width:0">
          <span class="tm-mono" style="font-size:15px;font-weight:700;padding:4px 9px;border-radius:6px;background:${INK};color:#fff">${isoWeek(ws)}</span>
          <div style="display:flex;flex-direction:column;gap:2px"><span style="font-size:20px;font-weight:600">Week of ${fmtDay(ws)}</span>
            <span style="font-size:12.5px;color:${MUTED}">${plural(list.length, 'movement')} · ${sea} sea, ${air} air · execution week report</span></div>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <button class="tm-sq" data-act="report-nav" data-v="-1" aria-label="Previous week" ${idx <= 0 ? 'disabled' : ''}>${I.left()}</button>
          <button class="tm-sq" data-act="report-nav" data-v="1" aria-label="Next week" ${idx < 0 || idx >= weeks.length - 1 ? 'disabled' : ''}>${I.right()}</button>
          ${list.length ? `<button class="tm-btn-s" data-act="report-xlsx" data-v="${ws}">${I.dl()} Download XLSX</button>` : ''}
          <button class="tm-sq" data-act="report-close" aria-label="Close">${I.x()}</button>
        </div>
      </div>
      <div style="padding:22px 28px 30px;display:flex;flex-direction:column;gap:26px">
        <div style="display:flex;flex-direction:column;gap:8px;padding:18px 20px;border-radius:12px;background:#FAFAF8;border:1px solid ${LINE}">${context}</div>
        ${sec('Journey — factory gate to FC Sydney', `<div style="display:flex;gap:10px;flex-wrap:wrap">
          ${O ? journeyStep('Ex-factory', `${O.received.pct}%`, `${O.received.pos_received} of ${plural(O.received.pos, 'PO')}${O.received.done_at ? ` · done ${fmtDay(O.received.done_at)}` : ` · target ${fmtDay(O.received.target)}`}`, O.received.status) : ''}
          ${O ? journeyStep('VAS', `${O.vas.lanes_complete}/${O.vas.lanes}`, `${O.vas.units_pct}% units${O.vas.done_at ? ` · done ${fmtDay(O.vas.done_at)}` : ` · target ${fmtDay(O.vas.target)}`}`, O.vas.status) : ''}
          ${stageStep(list, 'departed', 'Departed', M)}${stageStep(list, 'arrived', 'Arrived', M)}${stageStep(list, 'fc_receipt', 'Received at FC', M)}
        </div>`)}
        ${sec('The numbers', `<div style="display:flex;flex-wrap:wrap;border:1px solid ${LINE};border-radius:10px;overflow:hidden;background:#fff">
          ${num('Movements', list.length, `${sea} sea · ${air} air`)}${num('Lanes', cs.lanes)}${num('POs', fmtNum(cs.pos))}${num('SKUs', fmtNum(cs.skus))}${num('Units', fmtNum(cs.units))}
          ${num('On time', measured.length ? `${onTime}/${measured.length}` : '—', 'against first promise')}</div>
          ${modes ? `<div style="display:grid;grid-template-columns:56px repeat(3,minmax(0,140px));gap:4px 14px;align-items:baseline"><span></span><span style="font-size:12px;color:${MUTED}">D2D plan</span><span style="font-size:12px;color:${MUTED}">D2D actual</span><span style="font-size:12px;color:${MUTED}">Variance</span>${modes}</div>
          <div style="font-size:12px;color:${MUTED}">Door to door, packing list → received at FC, averaged by mode.</div>` : ''}`)}
        ${sec('Movements', list.length ? `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:12px">${list.map(m => movementCard(m, M)).join('')}</div>`
          : `<div style="font-size:14px;color:${MUTED}">No containers or flights have been set up for this week yet.</div>`)}
        ${sec('What changed', H.length ? `<div style="border:1px solid ${LINE};border-radius:10px;overflow:hidden">${H.map(hlRow).join('')}</div>`
          : `<div style="font-size:14px;color:${MUTED}">Nothing changed in the last ${HL_DAYS} days.</div>`, `<span class="tm-note">last ${HL_DAYS} days</span>`)}
        ${un.length ? sec('Lanes on no movement', `<div style="border:1px solid ${LINE};border-radius:10px;overflow:hidden">${un.map(u => `<div style="display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;padding:10px 16px;border-top:1px solid ${SOFT};font-size:13px"><span>${esc(u.supplier)} <span class="tm-mono" style="font-size:11.5px;color:${MUTED}">· ZD ${esc(u.zendesk || '—')} · ${esc(u.freight || '')}</span></span><span class="tm-mono">${plural(u.pos || 0, 'PO')}</span></div>`).join('')}</div>
          <div style="font-size:12px;color:${MUTED}">Not yet on a container or flight, so not part of the journey above.</div>`) : ''}
      </div>
    </div>`;
  }

  // ════ Render ════
  function render() {
    if (!S.root) return;
    if (S.view == null) S.view = 'map';          // one view for everyone; Routes a click away
    if (S.error && !S.board) {
      S.root.innerHTML = `<div class="tm-card" style="padding:28px;display:flex;flex-direction:column;gap:12px;align-items:flex-start">
        <h1 style="margin:0;font-size:28px;font-weight:600">Transit Movements</h1>
        <div style="font-size:14px;color:${RED}">Couldn’t load movements: ${esc(S.error)}</div>
        <button class="tm-btn-p" data-act="retry">Try again</button></div>`;
      renderOverlays({ all: [], timeline: [], unassigned: [], liveWeeks: [], earlier: [] });
      return;
    }
    if (!S.board) {
      S.root.innerHTML = `<div style="display:flex;flex-direction:column;gap:20px">
        <div class="tm-skel" style="height:90px;max-width:520px"></div><div class="tm-skel" style="height:180px"></div>
        <div class="tm-skel" style="height:220px;background-color:#222"></div><div class="tm-skel" style="height:420px"></div></div>`;
      return;
    }
    const M = model();
    const H = highlights(M), A = attentionItems(M);
    const scrollX = (S.root.querySelector('[data-tm-tlscroll]') || {}).scrollLeft || 0;
    S.root.innerHTML = renderHeader(M) + renderWeekPills(M) + renderHighlights(H, A)
      + renderTimeline(M).replace('<div style="overflow-x:auto">', '<div style="overflow-x:auto" data-tm-tlscroll>')
      + (S.full ? '' : renderCorridor(M, false));
    const tl = S.root.querySelector('[data-tm-tlscroll]'); if (tl) tl.scrollLeft = scrollX;
    renderOverlays(M);
    const animating = runAnimation();
    if ((S.view === 'map') || S.full) drawMaps(M);
    return animating;
  }

  function toast(msg) {
    S.toast = msg;
    clearTimeout(S.toastTimer);
    S.toastTimer = setTimeout(() => { S.toast = ''; const t = document.querySelector('#tm-overlay .tm-toast'); if (t) t.remove(); }, 3600);
    const M = S.board ? model() : null;
    if (M) renderOverlays(M);
  }

  // ════ Data ════
  async function load(opts) {
    const quiet = opts && opts.quiet;
    if (S.loading) return;
    S.loading = true; S.error = null;
    if (!quiet && !S.board) render();
    try {
      const board = await req('GET', '/consignments/board');
      const today = board.today || todayLocal();
      const floor = addD(board.this_week || mondayOf(today), -7 * LIVE_WEEKS_BACK);
      const weeks = [...new Set((board.consignments || []).filter(c => {
        const fc = (c.milestones || []).find(x => x.stage === 'fc_receipt') || {};
        return !fc.actual_at && c.week_start >= floor;
      }).map(c => c.week_start))];
      const alerts = [];
      await Promise.all(weeks.map(w => req('GET', '/alerts?week=' + encodeURIComponent(w))
        .then(r => alerts.push(...((r && r.alerts) || []))).catch(() => {})));
      S.board = board; S.alerts = alerts; S.loadedAt = Date.now();
      if (S.sel && S.sel !== 'none' && !board.consignments.some(c => c.consignment_uid === S.sel)) { S.sel = null; if (S.panel === 'mv') S.panel = null; }
      // The overnight replay plays once, on the first load, and only if something moved.
      const moved = (board.consignments || []).some(c => c.changed_24h && Object.keys(c.changed_24h).length);
      if (!S.playedReplay && moved && !quiet) { S.playedReplay = true; S.phase = 'before'; }
    } catch (e) {
      S.error = e.status === 403 ? 'this client does not use Transit Movements.' : (e.message || String(e));
    } finally {
      S.loading = false;
    }
    if (quiet && (S.notify || S.amend)) return;   // never redraw under someone typing
    render();
  }

  function replay() {
    const moved = S.board && (S.board.consignments || []).some(c => c.changed_24h && Object.keys(c.changed_24h).length);
    if (!moved) { toast('Nothing has moved since yesterday.'); return; }
    S.phase = 'before';
    render();
  }

  // ════ Actions ════
  const step = (k) => {
    const M = model(); const ids = navIds(M);
    if (!ids.length) return;
    const cur = S.panel === 'wk' ? S.wkSel : S.sel;
    const i = ids.indexOf(cur);
    const j = ((i < 0 ? 0 : i + k) % ids.length + ids.length) % ids.length;
    if (S.panel === 'wk') S.wkSel = ids[j]; else { S.sel = ids[j]; S.amend = null; S.legacy = null; }
    render();
  };

  async function openContents(uid) {
    if (!uid || uid === 'none') return;
    S.sheet = uid; S.sheetLane = 0; render();
    if (S.contents[uid]) return;
    try {
      const d = await req('GET', `/consignments/${encodeURIComponent(uid)}/contents`);
      S.contents[uid] = d; delete S.contentsErr[uid];
    } catch (e) { S.contentsErr[uid] = 'Couldn’t load the contents: ' + (e.message || e); }
    if (S.sheet === uid) render();
  }

  async function openNotify() {
    if (!isInternal()) return;
    S.notify = S.sel; S.nDraft = ''; S.nErr = ''; S.nSubject = null; S.nBody = null; S.nAttach = true; S.nSending = false;
    render();
    try {
      const r = await req('GET', '/consignments/notify/recipients');
      S.nRecipients = r.recipients || [];
      if (!S.nTo.length && (r.last || []).length) S.nTo = r.last.slice();
    } catch (_) { S.nRecipients = S.nRecipients || []; }
    if (S.notify) { render(); focusTo(); }
  }
  const focusTo = () => setTimeout(() => { const i = document.getElementById('tm-to'); if (i) i.focus(); }, 0);
  function addRecipient() {
    const v = String(S.nDraft || '').trim().replace(/[,;]+$/, '').toLowerCase();
    if (!v) return true;
    if (!EMAIL_RE.test(v)) { S.nErr = `“${v}” isn’t a valid email address.`; render(); focusTo(); return false; }
    if (!S.nTo.includes(v)) S.nTo = S.nTo.concat(v);
    S.nDraft = ''; S.nErr = '';
    render(); focusTo();
    return true;
  }
  async function sendNotify() {
    const M = model(); const m = M.all.find(x => x.uid === S.notify);
    if (!m || S.nSending) return;
    if (S.nDraft.trim() && !addRecipient()) return;
    if (!S.nTo.length) { S.nErr = 'Add at least one recipient.'; render(); return; }
    const d = draftFor(m, M);
    const subject = (S.nSubject != null ? S.nSubject : d.subject).trim();
    const body = (S.nBody != null ? S.nBody : d.body).trim();
    if (!subject || !body) { S.nErr = !subject ? 'A subject is required.' : 'The message is empty.'; render(); return; }
    S.nSending = true; S.nErr = ''; render();
    try {
      const r = await req('POST', `/consignments/${encodeURIComponent(m.uid)}/notify`, { to: S.nTo, subject, body, attach: S.nAttach });
      const c = S.board.consignments.find(x => x.consignment_uid === m.uid);
      if (c) c.last_notification = { sent_at: r.sent_at, to_count: (r.to || S.nTo).length, subject };
      S.notify = null; S.nSending = false;
      toast(`Sent to ${plural((r.to || S.nTo).length, 'recipient')}.`);
      render();
    } catch (e) {
      S.nSending = false; S.nErr = e.message || String(e); render();
    }
  }

  async function confirmStage(stage, date) {
    const uid = S.sel; if (!uid || S.busy) return;
    S.busy = 'milestone'; render();
    try {
      await req('POST', `/consignments/${encodeURIComponent(uid)}/milestone`, date ? { stage, actual_at: date } : { stage });
      S.amend = null; S.busy = null;
      toast(`${STAGE_LABEL[stage]} ${date ? 'recorded' : 'confirmed'}.`);
      await load({ quiet: true });
      render();
    } catch (e) {
      S.busy = null;
      const msg = e.data && e.data.error === 'details_required' ? (e.data.message || 'Enter the quoted transit and references first.') : (e.message || String(e));
      if (S.amend) { S.amend.err = msg; render(); } else toast(msg);
    }
  }

  async function legacyDry() {
    const M = model();
    const weeks = [...new Set(M.unassigned.filter(u => u.legacy_dates).map(u => u.week_start))].sort();
    if (!weeks.length || S.busy) return;
    S.busy = 'legacy'; render();
    try {
      let dates = 0;
      for (const w of weeks) { const r = await req('POST', '/consignments/legacy/clear', { week_start: w, confirm: false }); dates += (r.plan_blob_dates || 0) + (r.lane_actual_rows || 0); }
      S.legacy = { preview: { weeks, dates } };
    } catch (e) { S.legacy = { preview: { weeks, dates: 0 }, err: e.message || String(e) }; }
    S.busy = null; render();
  }
  async function legacyApply() {
    if (!S.legacy || !S.legacy.preview || S.busy) return;
    S.busy = 'legacy'; render();
    try {
      for (const w of S.legacy.preview.weeks) await req('POST', '/consignments/legacy/clear', { week_start: w, confirm: true });
      S.legacy = null; S.busy = null;
      toast('Old lane dates removed.');
      await load({ quiet: true }); render();
    } catch (e) { S.busy = null; S.legacy.err = e.message || String(e); render(); }
  }

  function openReport(fn, week) {
    const f = window[fn];
    if (typeof f === 'function') { try { f(week ? { week } : undefined); return; } catch (e) { console.warn('[transit] report failed', e); } }
    if (typeof window.show === 'function') window.show('#reports');
  }
  function openTransit(week) {
    S.panel = null; S.sheet = null; S.notify = null;
    renderOverlays(model() || { all: [], timeline: [], unassigned: [], liveWeeks: [], earlier: [] });
    document.documentElement.style.overflow = '';
    try { if (week && typeof window.setWeek === 'function') window.setWeek(week); } catch (_) {}
    if (typeof window.show === 'function') window.show('#week-hub');
    let n = 0;
    const t = setInterval(() => {
      const el = document.getElementById('cg-root');
      if (el || ++n > 20) { clearInterval(t); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
    }, 250);
  }

  function act(a, el, ev) {
    const v = el.getAttribute('data-v'), uid = el.getAttribute('data-uid');
    switch (a) {
      case 'retry': load(); break;
      case 'mode': S.mode = v; render(); break;
      case 'replay': replay(); break;
      case 'report': openReport(v); break;
      case 'week-report': openReport('__openTransitHistory', v); break;
      case 'view': S.view = v; S.viewTouched = true; render(); break;
      case 'full-open': S.full = true; S.corrMore = false; render(); break;
      case 'full-close': S.full = false; S.corrMore = false; render(); break;
      case 'cw': S.corrWk = v; S.corrMore = false; render(); break;
      case 'earlier-toggle': S.corrMore = !S.corrMore; render(); break;
      case 'pick': S.sel = uid; S.panel = 'mv'; S.amend = null; S.legacy = null; S.corrMore = false; render(); break;
      case 'hist': openReport('__openTransitHistory', v); break;
      case 'wk-toggle': { const shut = (v in S.collapsed) ? !!S.collapsed[v] : true; S.collapsed = Object.assign({}, S.collapsed, { [v]: !shut }); render(); break; }
      case 'hl-all': S.hlAll = !S.hlAll; render(); break;
      case 'wk-open': case 'report-open': openWeekReport(v); break;
      case 'report-close': S.report = null; render(); break;
      case 'report-bg': if (ev.target === el) { S.report = null; render(); } break;
      case 'report-nav': { const ws = reportWeeks(model()); const i = ws.indexOf(S.report) + Number(v); if (i >= 0 && i < ws.length) openWeekReport(ws[i]); break; }
      case 'report-xlsx': {
        const uids = model().all.filter(m => m.wk === v).map(m => m.uid).join(',');
        download(`/consignments/contents.xlsx?uids=${encodeURIComponent(uids)}`, `${isoWeek(v)}_POs.xlsx`).catch(e => toast('Export failed: ' + (e.message || e)));
        break;
      }
      case 'hl-filter': S.hlFilter = v || null; render(); break;
      case 'hl-more': S.hlOpen = Object.assign({}, S.hlOpen, { [v]: !(S.hlOpen && S.hlOpen[v]) }); render(); break;
      case 'collapse-all': {
        const keys = model().liveWeeks.concat(['none']);
        const shut = (k) => (k in S.collapsed) ? !!S.collapsed[k] : true;
        const anyOpen = keys.some(k => !shut(k));
        const c = {}; for (const k of keys) c[k] = anyOpen;
        S.collapsed = c; render(); break;
      }
      case 'panel-close': S.panel = null; S.amend = null; S.legacy = null; render(); break;
      case 'nav': step(Number(v)); break;
      case 'contents': openContents(uid); break;
      case 'sheet-close': S.sheet = null; render(); break;
      case 'sheet-bg': if (ev.target === el) { S.sheet = null; render(); } break;
      case 'sheet-lane': S.sheetLane = Number(v) || 0; render(); break;
      case 'sheet-xlsx': {
        const m = model().all.find(x => x.uid === uid);
        download(`/consignments/${encodeURIComponent(uid)}/contents.xlsx`, `${(m && m.ref) || 'movement'}_POs.xlsx`).catch(e => toast('Export failed: ' + (e.message || e)));
        break;
      }
      case 'export-all': {
        const uids = model().timeline.map(m => m.uid).join(',');
        download(`/consignments/contents.xlsx?uids=${encodeURIComponent(uids)}`, 'transit_movements.xlsx').catch(e => toast('Export failed: ' + (e.message || e)));
        break;
      }
      case 'notify-open': openNotify(); break;
      case 'notify-close': S.notify = null; render(); break;
      case 'chip-remove': S.nTo = S.nTo.filter(x => x !== v); render(); focusTo(); break;
      case 'notify-reset': S.nSubject = null; S.nBody = null; render(); break;
      case 'notify-send': sendNotify(); break;
      case 'confirm': confirmStage(v); break;
      case 'amend-open': {
        const m = model().all.find(x => x.uid === S.sel);
        const due = m && STAGES.find(st => m.ms[st] && m.ms[st].state === 'assumed');
        S.amend = { uid: S.sel, stage: due || 'departed', date: '' }; render(); break;
      }
      case 'amend-cancel': S.amend = null; render(); break;
      case 'amend-save': {
        const st = (document.getElementById('tm-amend-stage') || {}).value || (S.amend && S.amend.stage);
        const dt = (document.getElementById('tm-amend-date') || {}).value || '';
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dt)) { S.amend = Object.assign({}, S.amend, { stage: st, err: 'Choose the date it happened.' }); render(); break; }
        if (dt > todayLocal()) { S.amend = Object.assign({}, S.amend, { stage: st, date: dt, err: 'That date is in the future — only something that has happened can be recorded.' }); render(); break; }
        S.amend = Object.assign({}, S.amend, { stage: st, date: dt, err: null });
        confirmStage(st, dt); break;
      }
      case 'open-transit': openTransit(v); break;
      case 'legacy-dry': legacyDry(); break;
      case 'legacy-apply': legacyApply(); break;
      case 'legacy-cancel': S.legacy = null; render(); break;
      default: break;
    }
  }

  // ════ Events ════
  function bind(host) {
    if (host.__tmBound) return;
    host.__tmBound = true;
    host.addEventListener('click', (ev) => {
      const el = ev.target.closest('[data-act]');
      if (!el || !host.contains(el)) {
        if (S.corrMore && !ev.target.closest('.tm-menu')) { S.corrMore = false; render(); }
        return;
      }
      if (el.disabled) return;
      act(el.getAttribute('data-act'), el, ev);
    });
    host.addEventListener('dblclick', (ev) => {
      const el = ev.target.closest('[data-dbl]');
      if (!el || !host.contains(el)) return;
      const uid = el.getAttribute('data-dbl');
      const m = S.board && model().all.find(x => x.uid === uid);
      if (m && m.live) { S.sel = uid; S.panel = 'mv'; openContents(uid); }
    });
    host.addEventListener('input', (ev) => {
      const t = ev.target;
      if (t.id === 'tm-to') { S.nDraft = t.value; if (S.nErr) { S.nErr = ''; } return; }
      if (t.id === 'tm-subj' || t.id === 'tm-body') {
        if (t.id === 'tm-subj') S.nSubject = t.value; else S.nBody = t.value;
        const note = document.getElementById('tm-body-note'), reset = document.getElementById('tm-reset');
        if (note) note.textContent = 'Edited — your changes are sent as written';
        if (reset) reset.style.display = '';
      }
    });
    host.addEventListener('change', (ev) => {
      const t = ev.target;
      if (t.id === 'tm-attach') {
        S.nAttach = !!t.checked;
        if (S.nBody == null) render();      // the drafted line about the attachment follows the box
      }
      if (t.id === 'tm-to' && t.value && EMAIL_RE.test(t.value.trim())) { S.nDraft = t.value; addRecipient(); }
    });
    host.addEventListener('keydown', (ev) => {
      const t = ev.target;
      if (t.id === 'tm-to') {
        if (ev.key === 'Enter' || ev.key === ',' || ev.key === ';') { ev.preventDefault(); S.nDraft = t.value; addRecipient(); }
        else if (ev.key === 'Backspace' && !t.value && S.nTo.length) { S.nTo = S.nTo.slice(0, -1); render(); focusTo(); }
      }
    });
  }

  // Escape closes the topmost thing that is open, and only while this page is showing.
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Escape' || !S.root || !S.root.isConnected) return;
    const pg = document.getElementById('page-map');
    if (!pg || pg.style.display === 'none') return;
    if (S.notify) S.notify = null;
    else if (S.sheet) S.sheet = null;
    else if (S.corrMore) S.corrMore = false;
    else if (S.panel) { S.panel = null; S.amend = null; S.legacy = null; }
    else if (S.report) S.report = null;
    else if (S.full) S.full = false;
    else return;
    render();
  });

  let _resizeT = null;
  window.addEventListener('resize', () => {
    clearTimeout(_resizeT);
    _resizeT = setTimeout(() => { if (S.board && (S.view === 'map' || S.full) && S.root && S.root.isConnected) drawMaps(model()); }, 250);
  });
  // The default view follows who is looking, which is known only once tenancy resolves.
  window.addEventListener('tenancy:ready', () => { if (S.board) render(); });

  // ════ Mount ════
  function mount(host) {
    styles();
    host.innerHTML = '';
    const root = document.createElement('div');
    root.className = 'tm';
    host.appendChild(root);
    S.root = root;
    bind(root);
    // index.html hides this page by setting its style directly, without calling hideMapPage.
    // The overlays live on <body>, so watch the page and take them down with it.
    try {
      if (S.observer) S.observer.disconnect();
      S.observer = new MutationObserver(() => {
        const hidden = host.style.display === 'none' || host.classList.contains('hidden');
        if (hidden && (S.panel || S.sheet || S.notify || S.full || S.report)) window.hideMapPage();
      });
      S.observer.observe(host, { attributes: true, attributeFilter: ['style', 'class'] });
    } catch (_) {}
    render();
    load();
    clearInterval(S.timer);
    S.timer = setInterval(() => {
      const pg = document.getElementById('page-map');
      if (pg && pg.style.display !== 'none' && !document.hidden) load({ quiet: true });
    }, REFRESH_MS);
  }

  window.showMapPage = function () {
    let pg = document.getElementById('page-map');
    if (!pg) {
      pg = document.createElement('section');
      pg.id = 'page-map';
      pg.style.cssText = 'padding:0;display:block;';
      const main = document.querySelector('main.vo-wrap') || document.querySelector('main') || document.body;
      main.appendChild(pg);
      mount(pg);
    } else if (!S.root || !pg.contains(S.root)) {
      mount(pg);
    } else if (Date.now() - S.loadedAt > 2 * 60 * 1000) {
      load({ quiet: true });
    }
    pg.classList.remove('hidden');
    pg.style.display = 'block';
    if (S.board) renderOverlays(model());
  };

  window.hideMapPage = function () {
    const pg = document.getElementById('page-map');
    if (pg) { pg.classList.add('hidden'); pg.style.display = 'none'; }
    // Overlays live on <body>; they must not outlive the page.
    S.panel = null; S.sheet = null; S.notify = null; S.full = false; S.corrMore = false; S.report = null;
    const ov = document.getElementById('tm-overlay'); if (ov) ov.innerHTML = '';
    document.documentElement.style.overflow = '';
  };

  // For support: window.__transitMovements.reload()
  window.__transitMovements = { reload: () => load(), state: () => S, version: '1' };
  console.log('[transit-movements] v1 loaded');
})();
