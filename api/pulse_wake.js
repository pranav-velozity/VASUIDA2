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

module.exports = { DEFAULTS, list, regexes, strip };
