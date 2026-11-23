// Pulse wake words — one list for the meeting bot (pulse_meetings.js) and the browser voice
// module (which reads it from GET /pulse/voice/status as wake_words).
//
// Transcribers hear "Pulse" differently depending on the speaker's accent, so the list carries
// the common mishearings as well as the name. Anything here is accepted at the start of a
// sentence ("Bulbs, what's the ETA?") or after a pause mid-sentence ("…so, pause, when does it land?").
//
//   PULSE_WAKE_WORDS   comma-separated extras, e.g. "pulsar,paul,pauls"  (added to the defaults)
//   PULSE_WAKE_DROP    comma-separated defaults to switch off if one keeps false-triggering
//
// Kept out on purpose: "plus", "post", "pass", "please" — they start too many ordinary sentences.
'use strict';

// Kept out as well: "paul"/"pauls" (a participant may be called Paul), "false", "boss", "balls".
const DEFAULTS = [
  'pulse', 'pulses', "pulse's", 'pulse ai', 'pulse a i', 'pulse i', 'pulsar', 'pulsa', 'pulsey', 'puls', 'pulsing', 'pulsate',
  'paulse', 'palse', 'pals', 'pause', 'paws', 'pols', 'polls', 'pulls', 'pull', 'pulp',
  'bulbs', 'bulb', 'bulls', 'bull', 'bolts', 'pills', 'hulls', 'pelts', 'pulis', 'pulsy', 'pals ai', 'pause ai',
  'pinpoint', 'pin point',
];

function list() {
  const extra = String(process.env.PULSE_WAKE_WORDS || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  const drop = new Set(String(process.env.PULSE_WAKE_DROP || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean));
  const seen = new Set(); const out = [];
  for (const w of [...DEFAULTS, ...extra]) { if (!drop.has(w) && !seen.has(w)) { seen.add(w); out.push(w); } }
  return out;
}
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+').replace(/'/g, "['’]?");
// Longest first so "pulse ai" wins over "pulse".
const alt = (words) => words.slice().sort((a, b) => b.length - a.length).map(esc).join('|');

function regexes(words = list()) {
  const a = alt(words);
  return {
    // "Pulse, what's the ETA?"  "Hey bulbs, …"  "Okay pause …"
    wake: new RegExp(`^\\s*(?:hey|hi|ok|okay|hello|so|um|uh|right|now)?[\\s,]*(?:${a})\\b[\\s,.:!?-]*`, 'i'),
    // "…so, Pulse, what's the ETA?"  — the name after a clause, followed by a real question.
    mid: new RegExp(`^.{3,}?[,.;:!?—-]?\\s+(?:hey|ok|okay)?\\s*(?:${a})\\b[\\s,.:!?-]*(.{3,})$`, 'i'),
  };
}
// Returns { hit, question } — hit false when the text is not for Pulse.
function strip(text, rx = regexes()) {
  const t = String(text || '');
  if (rx.wake.test(t)) return { hit: true, question: t.replace(rx.wake, '').trim() };
  if (rx.mid.test(t)) return { hit: true, question: t.replace(rx.mid, '$1').trim() };
  return { hit: false, question: t.trim() };
}


// ── Does this sound like an operations question? ──
// Used when Pulse is already in the conversation (it answered in the last PULSE_CONVO_SECONDS) or a
// meeting is in open mode: an utterance without the name is answered if it both asks something and
// is about our work. "Shall we break for lunch?" asks but is not ours; "the container is late" is
// ours but does not ask. Both stay unanswered.
const ASKS = /^(?:and |so |ok |okay |also |then |but |um |uh )*(?:what|what's|whats|which|when|when's|where|where's|who|who's|how|how's|why|is|are|was|were|has|have|had|do|does|did|can|could|will|would|should|any|anything|tell me|give me|show me|show us|check|look up|pull up|remind|list|find|get me|do we|did we|have we)\b/i;
const OPS_DEFAULT = ['week', 'wk', 'po', 'pos', 'purchase order', 'order', 'orders', 'container', 'containers', 'eta', 'etd', 'vessel', 'lane', 'lanes', 'shipment', 'shipments', 'movement', 'movements',
  'customs', 'cleared', 'clearance', 'depart', 'departs', 'arrive', 'arrives', 'land', 'landed', 'landing', 'sail', 'sailing', 'berth', 'departed', 'departure', 'arrived', 'arrival', 'arriving', 'discharged', 'delivered', 'delivery', 'receipt', 'received', 'receiving', 'dispatch', 'dispatched',
  'packing list', 'fc', 'warehouse', 'facility', 'dock', 'mobiledock', 'booking', 'truck', 'last mile', 'carton', 'cartons', 'units', 'skus', 'sku', 'label', 'labels', 'vas', 'assembly', 'invoice', 'invoices', 'cost', 'rate',
  'alert', 'alerts', 'late', 'delayed', 'delay', 'on hold', 'at risk', 'status', 'plan', 'planned', 'progress', 'complete', 'completion', 'behind', 'on track', 'pending', 'outstanding', 'open',
  'task', 'tasks', 'note', 'email', 'summary', 'iconic', 'ehp', 'supplier', 'zendesk', 'ticket', 'transit', 'door to door', 'air', 'sea', 'freight'];
function opsTerms() {
  const extra = String(process.env.PULSE_OPS_TERMS || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  return [...new Set([...OPS_DEFAULT, ...extra])];
}
function opsRegex(terms = opsTerms()) {
  return new RegExp(`\\b(?:(?:${alt(terms)})(?:s|es|ed|ing)?|[a-z]{4}\\d{6,7}|w\\d{1,2}|\\d{4,}|\\d{1,2}(?:st|nd|rd|th)?\\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*)\\b`, 'i');
}
function looksLikeQuestion(text, rx = opsRegex()) {
  const t = String(text || '').trim();
  if (t.split(/\s+/).length < 3) return false;
  const asks = ASKS.test(t) || /\?\s*$/.test(t);
  return asks && rx.test(t);
}

// Spoken mode commands, after the name: "Pulse, open mode" / "Pulse, name only".
const OPEN_ON = /^(?:go |switch to |turn on |enable |start )?(?:open mode|open|free mode|conversation mode|answer everything|answer everyone|listen to everything)\b/i;
const OPEN_OFF = /^(?:go |switch to |turn off |disable |stop |back to )?(?:name only|name mode|closed mode|close mode|wake word only|only when called|stop open mode|open mode off|quiet mode)\b/i;
function modeCommand(question) { const q = String(question || '').trim(); if (OPEN_ON.test(q)) return 'open'; if (OPEN_OFF.test(q)) return 'name'; return null; }

module.exports = { modeCommand, DEFAULTS, list, regexes, strip, OPS_DEFAULT, opsTerms, opsRegex, looksLikeQuestion, ASKS };
