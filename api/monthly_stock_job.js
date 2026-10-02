/* ── VelOzity Pinpoint — Monthly In Stock & In-Transit report ──
   The stock status workbook, for a calendar month, in the client's inbox at 5am on the 1st.

   Three things make this different from the weekly report, and each one is deliberate:

     · it runs on CALENDAR months, not reporting weeks. A month that starts mid-week is what
       a client's finance team recognises; a Monday-anchored range is not.

     · lines that received nothing are left out. A stock report listing goods that never
       arrived is a different report, and zero-value rows make the total untrustworthy.

     · every line carries the SUPPLIER's cost, not a landed cost. Shipping is VelOzity's, and
       adding it would state a number the client never paid.

   On timing: the cron fires hourly and this decides whether to act, because a fixed UTC
   expression drifts by an hour twice a year when Sydney changes zone — which is exactly how
   the monthly client report came to be an hour short of its own window.

   Mount:
     app.use('/ops', require('./monthly_stock_job')({ express, db, authenticateRequest,
       auditLog, curClient, internal, sendViaResend, ExcelJS, STOCK_STATUS, logger }));
*/
'use strict';

module.exports = function mountMonthlyStock(deps) {
  const { express, db, authenticateRequest, auditLog, internal, sendViaResend, logger } = deps;
  const router = express.Router();
  const log = logger || console;

  const TZ = process.env.MONTHLY_STOCK_TZ || 'Australia/Sydney';
  const SEND_HOUR = Number(process.env.MONTHLY_STOCK_HOUR || 5);   // 5am, local to the client

  db.exec(`
    CREATE TABLE IF NOT EXISTS monthly_stock_log (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      ran_at   TEXT NOT NULL DEFAULT (datetime('now')),
      month    TEXT,
      trigger  TEXT,
      outcome  TEXT NOT NULL,          -- sent | skipped | failed
      reason   TEXT,
      detail   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ms_log_month ON monthly_stock_log(month);
  `);

  const writeLog = (row) => {
    try {
      db.prepare(`INSERT INTO monthly_stock_log (month, trigger, outcome, reason, detail)
                  VALUES (?,?,?,?,?)`)
        .run(row.month || null, row.trigger || null, row.outcome, row.reason || null,
             row.detail ? JSON.stringify(row.detail).slice(0, 2000) : null);
    } catch (e) { log.warn('[monthly-stock] could not write the run log:', e.message); }
  };

  // Local date parts for the client's zone, so the schedule survives daylight saving without
  // anyone editing a cron expression twice a year.
  function localParts(at) {
    const f = new Intl.DateTimeFormat('en-AU', { timeZone: TZ,
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hour12: false });
    const p = Object.fromEntries(f.formatToParts(at || new Date()).map(x => [x.type, x.value]));
    return { y: +p.year, m: +p.month, d: +p.day, h: +p.hour };
  }

  // The month that just closed, and its first and last calendar days.
  function previousMonth(at) {
    const p = localParts(at);
    const y = p.m === 1 ? p.y - 1 : p.y;
    const m = p.m === 1 ? 12 : p.m - 1;
    return `${y}-${String(m).padStart(2, '0')}`;
  }
  function monthRange(month) {
    const [y, m] = month.split('-').map(Number);
    const first = `${month}-01`;
    // Day zero of the next month is the last day of this one, and it handles February.
    const last = new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10);
    return { from: first, to: last };
  }

  const fmtMonth = (month) => new Date(month + '-01T00:00:00Z')
    .toLocaleDateString('en-GB', { month: 'long', year: 'numeric', timeZone: 'UTC' });

  async function build(month) {
    const { from, to } = monthRange(month);
    // Through the route, so the file the client receives is the same one anybody downloads.
    const buf = await internal.buffer(
      `/report/stock-status?from=${from}&to=${to}&format=xlsx`);
    return { buffer: buf, from, to,
             filename: `In_Stock_and_In_Transit_${month}.xlsx` };
  }

  function recipients() {
    const split = (v) => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
    return { to: split(process.env.MONTHLY_STOCK_TO), cc: split(process.env.MONTHLY_STOCK_CC) };
  }

  async function send(month, opts) {
    const o = opts || {};
    const { to, cc } = recipients();
    const from = process.env.MONTHLY_STOCK_FROM
      || process.env.MONTHLY_CLIENT_REPORT_FROM || process.env.EXCEPTION_EMAIL_FROM;

    if (!to.length || !from) return { sent: false, reason: 'no_recipients' };

    const file = await build(month);

    // An empty month means something did not finish, not that nothing arrived all month.
    // Sending a blank workbook would be read as a statement about the business.
    if (!file.buffer || file.buffer.length < 4000) {
      return { sent: false, reason: 'empty_month', month,
               message: `${month} produced an empty workbook. Nothing was sent.` };
    }

    const label = fmtMonth(month);
    const subject = `Monthly — In Stock & In-Transit report — ${label}`;
    const lines = [
      subject, '',
      `Period: ${file.from} to ${file.to}`,
      '',
      'Attached is the stock position for the month: every line received, with the supplier',
      'cost per unit and the total for what arrived. Lines that received nothing are not',
      'listed.',
    ];
    const html = `
      <div style="font-family:-apple-system,Segoe UI,sans-serif;color:#1C1C1E;max-width:640px;">
        <div style="font-size:16px;font-weight:700;">In Stock &amp; In-Transit</div>
        <div style="font-size:12px;color:#6E6E73;margin-top:2px;">${file.from} to ${file.to}</div>
        <p style="font-size:14px;line-height:1.6;margin:16px 0;">
          Attached is the stock position for ${label}: every line that received units, with the
          supplier cost per unit and the total for what arrived. Lines that received nothing
          are not listed.</p>
        <div style="font-size:13px;color:#6E6E73;">${file.filename}</div>
      </div>`;

    if (o.dryRun) {
      return { sent: false, dryRun: true, month, to, cc,
               period: { from: file.from, to: file.to },
               filename: file.filename, bytes: file.buffer.length };
    }

    const r = await sendViaResend({ from, to, cc, subject, html, text: lines.join('\n'),
      attachments: [{ filename: file.filename, content: file.buffer.toString('base64') }] });

    log.log(`[monthly-stock] ${month} sent to ${to.join(', ')} (${file.buffer.length} bytes)`);
    return { sent: true, month, to, cc, resend_id: r && r.id,
             period: { from: file.from, to: file.to },
             filename: file.filename, bytes: file.buffer.length };
  }

  const sentKey = (month) => `stock_sent_${month}`;
  const alreadySent = (month) => !!db.prepare(
    `SELECT 1 x FROM client_capability WHERE client_id='__meta' AND capability=?`).get(sentKey(month));

  router.post('/monthly-stock-report/run', (req, res, next) => {
    const secret = process.env.LANE_CRON_SECRET;
    if (secret && req.headers['x-lane-cron-secret'] === secret) return next();
    return authenticateRequest(req, res, next);
  }, auditLog('run_monthly_stock_report'), async (req, res) => {
    const cron = !!req.headers['x-lane-cron-secret'];
    const dryRun = String(req.query.dryRun || '') === '1';
    const force = String(req.query.force || '') === '1';
    const month = String(req.query.month || '').trim() || previousMonth();

    try {
      if (!/^\d{4}-\d{2}$/.test(month)) {
        return res.status(400).json({ ok: false, error: 'month must be YYYY-MM' });
      }

      const p = localParts();
      // On or after the hour on the 1st, rather than exactly at it. An hourly cron that
      // misses its single hour — a deploy, a cold start — would otherwise skip the month in
      // silence, which is precisely how the other monthly report went unnoticed for months.
      if (cron && !force && !(p.d === 1 && p.h >= SEND_HOUR)) {
        const when = `${p.y}-${String(p.m).padStart(2, '0')}-${String(p.d).padStart(2, '0')} ${p.h}:00`;
        writeLog({ trigger: 'cron', outcome: 'skipped', reason: 'not_due', detail: { local: when } });
        return res.json({ ok: true, skipped: true, reason: 'not_due', local: when });
      }

      if (!dryRun && !force && alreadySent(month)) {
        writeLog({ month, trigger: cron ? 'cron' : 'manual', outcome: 'skipped', reason: 'already_sent' });
        return res.json({ ok: true, skipped: true, reason: 'already_sent', month });
      }

      const out = await send(month, { dryRun });
      if (out.sent) {
        db.prepare(`INSERT OR IGNORE INTO client_capability (client_id, capability, enabled)
                    VALUES ('__meta', ?, 1)`).run(sentKey(month));
      }
      writeLog({ month, trigger: dryRun ? 'dry_run' : (cron ? 'cron' : 'manual'),
                 outcome: out.sent ? 'sent' : 'skipped', reason: out.sent ? null : out.reason,
                 detail: { to: out.to, bytes: out.bytes, period: out.period } });
      if (!out.sent && !out.dryRun) log.warn(`[monthly-stock] ${month} NOT sent: ${out.reason}`);
      res.json({ ok: true, trigger: cron ? 'cron' : 'manual', local_hour: p.h, ...out });
    } catch (e) {
      log.error('[monthly-stock] run failed:', e);
      writeLog({ month, trigger: cron ? 'cron' : 'manual', outcome: 'failed',
                 reason: String(e.message || e) });
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router.get('/monthly-stock-report/status', authenticateRequest, (req, res) => {
    try {
      const runs = db.prepare('SELECT * FROM monthly_stock_log ORDER BY id DESC LIMIT 40').all();
      const sent = db.prepare(`SELECT capability FROM client_capability
                                WHERE client_id='__meta' AND capability LIKE 'stock_sent_%'`).all()
        .map(r => String(r.capability).replace('stock_sent_', '')).sort();
      const { to, cc } = recipients();
      const p = localParts();
      res.json({ ok: true,
        months_sent: sent,
        recipients: { to, cc,
          from: process.env.MONTHLY_STOCK_FROM || process.env.MONTHLY_CLIENT_REPORT_FROM
             || process.env.EXCEPTION_EMAIL_FROM || null },
        schedule: { zone: TZ, hour: SEND_HOUR, day: 1,
                    local_now: `${p.y}-${String(p.m).padStart(2, '0')}-${String(p.d).padStart(2, '0')} ${p.h}:00` },
        recent_runs: runs });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  router._internals = { previousMonth, monthRange, localParts, send, build };
  log.log('[monthly-stock] In Stock & In-Transit report mounted');
  return router;
};
