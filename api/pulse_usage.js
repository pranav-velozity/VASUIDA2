// Pulse usage meter — every model call recorded with its tokens and an estimated cost, so the
// cost work can be measured rather than guessed, and admins can see what Pulse spends.
//
//   record({ feature, model, usage, userId, client, ms })   called after each messages.create
//   wrap(anthropicClient)                                   records calls made outside /pulse/chat
//                                                           (finance insights, reports…) as 'other'
//   GET /pulse/usage?days=30   (VelOzity admins)            → { today, days:[{day, usd, calls, in, out, cache_read…}],
//                                                              by_feature:[…], by_model:[…] }
//
// Prices are USD per million tokens and can be overridden with PULSE_PRICES, e.g.
//   {"claude-sonnet-4-6":{"in":3,"out":15,"cw":3.75,"cr":0.3}}
'use strict';

const DEFAULT_PRICES = {
  'claude-sonnet-4-6': { in: 3, out: 15, cw: 3.75, cr: 0.30 },
  'claude-sonnet-4-5': { in: 3, out: 15, cw: 3.75, cr: 0.30 },
  'claude-haiku-4-5': { in: 1, out: 5, cw: 1.25, cr: 0.10 },
  'claude-opus-4-1': { in: 15, out: 75, cw: 18.75, cr: 1.50 },
};
function prices() {
  let o = {}; try { o = JSON.parse(process.env.PULSE_PRICES || '{}'); } catch (_) {}
  return Object.assign({}, DEFAULT_PRICES, o);
}
function priceFor(model) {
  const p = prices(); if (p[model]) return p[model];
  const k = Object.keys(p).find(x => String(model || '').startsWith(x.replace(/-\d+$/, ''))); if (k) return p[k];
  return /haiku/i.test(model) ? p['claude-haiku-4-5'] : /opus/i.test(model) ? p['claude-opus-4-1'] : p['claude-sonnet-4-6'];
}
function cost(model, u) {
  const p = priceFor(model);
  return ((u.in || 0) * p.in + (u.out || 0) * p.out + (u.cw || 0) * p.cw + (u.cr || 0) * p.cr) / 1e6;
}

module.exports = function install(deps) {
  const { app, db, authenticateRequest, requireRole, requireInternalOrg, log = console, now = () => Date.now() } = deps;
  db.exec(`
    CREATE TABLE IF NOT EXISTS pulse_usage (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      at        TEXT NOT NULL,
      day       TEXT NOT NULL,              -- YYYY-MM-DD in Australia/Sydney
      feature   TEXT NOT NULL,              -- chat | insights | meeting | meeting_summary | email | eval | other
      model     TEXT NOT NULL,
      client_id TEXT,
      user_id   TEXT,
      in_tokens INTEGER NOT NULL DEFAULT 0,
      out_tokens INTEGER NOT NULL DEFAULT 0,
      cache_write INTEGER NOT NULL DEFAULT 0,
      cache_read INTEGER NOT NULL DEFAULT 0,
      usd       REAL NOT NULL DEFAULT 0,
      ms        INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_pulse_usage_day ON pulse_usage(day);
  `);
  const sydDay = (t) => new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(t));
  const ins = db.prepare(`INSERT INTO pulse_usage (at, day, feature, model, client_id, user_id, in_tokens, out_tokens, cache_write, cache_read, usd, ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);

  function record(o) {
    try {
      const u = o.usage || {};
      const t = { in: Number(u.input_tokens) || 0, out: Number(u.output_tokens) || 0, cw: Number(u.cache_creation_input_tokens) || 0, cr: Number(u.cache_read_input_tokens) || 0 };
      const usd = cost(o.model, t);
      ins.run(new Date(now()).toISOString(), sydDay(now()), String(o.feature || 'other').slice(0, 40), String(o.model || '').slice(0, 60), o.client ? String(o.client).slice(0, 40) : null, o.userId ? String(o.userId).slice(0, 80) : null, t.in, t.out, t.cw, t.cr, usd, o.ms != null ? Math.round(o.ms) : null);
      return usd;
    } catch (e) { log.warn('[pulse usage] record failed: ' + (e && e.message)); return 0; }
  }
  // Calls made through the shared client but not by /pulse/chat (which records itself with a
  // feature name) are tagged 'other'. /pulse/chat sets _pulseFeature on the request options.
  function wrap(client) {
    if (!client || !client.messages || client.messages.__metered) return client;
    const orig = client.messages.create.bind(client.messages);
    client.messages.create = async function (params, options) {
      const t0 = now(); const feature = (options && options._pulseFeature) || 'other';
      const meta = (options && options._pulseMeta) || {};
      const opts = options ? Object.assign({}, options) : undefined; if (opts) { delete opts._pulseFeature; delete opts._pulseMeta; }
      const r = await orig(params, opts && Object.keys(opts).length ? opts : undefined);
      if (r && r.usage) record({ feature, model: params && params.model, usage: r.usage, client: meta.client, userId: meta.userId, ms: now() - t0 });
      return r;
    };
    client.messages.__metered = true;
    return client;
  }

  const gate = [authenticateRequest, requireRole(['admin']), requireInternalOrg];
  app.get('/pulse/usage', ...gate, (req, res) => {
    const days = Math.min(90, Math.max(1, Number(req.query.days) || 30));
    const since = sydDay(now() - (days - 1) * 86400000);
    const sum = 'SUM(usd) usd, COUNT(*) calls, SUM(in_tokens) in_tokens, SUM(out_tokens) out_tokens, SUM(cache_write) cache_write, SUM(cache_read) cache_read';
    const byDay = db.prepare(`SELECT day, ${sum} FROM pulse_usage WHERE day >= ? GROUP BY day ORDER BY day`).all(since);
    const byFeature = db.prepare(`SELECT feature, ${sum} FROM pulse_usage WHERE day >= ? GROUP BY feature ORDER BY usd DESC`).all(since);
    const byModel = db.prepare(`SELECT model, ${sum} FROM pulse_usage WHERE day >= ? GROUP BY model ORDER BY usd DESC`).all(since);
    const today = db.prepare(`SELECT ${sum} FROM pulse_usage WHERE day = ?`).get(sydDay(now())) || {};
    const total = byDay.reduce((s, d) => s + (d.usd || 0), 0);
    const r2 = (v) => Math.round((v || 0) * 10000) / 10000;
    const shape = (r) => ({ ...r, usd: r2(r.usd), cache_hit: (r.in_tokens + r.cache_read + r.cache_write) ? Math.round(100 * (r.cache_read || 0) / ((r.in_tokens || 0) + (r.cache_read || 0) + (r.cache_write || 0))) : 0 });
    res.json({ ok: true, days, since, today: shape({ ...today, usd: today.usd || 0 }), total_usd: r2(total), avg_per_day_usd: r2(total / days), by_day: byDay.map(shape), by_feature: byFeature.map(shape), by_model: byModel.map(shape), prices: prices() });
  });

  return { record, wrap, cost, priceFor, prices };
};
