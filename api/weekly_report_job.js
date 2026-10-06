/* ── VelOzity Pinpoint — weekly reporting job v1 ──
   Replaces the email someone sends by hand every Monday morning for the week just closed.

   Runs 13:00 America/Chicago on Sunday, which is Monday breakfast in Sydney where the
   recipients are. Chicago local time is used rather than a fixed UTC offset, so the send does
   not drift an hour when daylight saving changes at either end.

   Order matters: BUILD → VALIDATE → PUBLISH → SEND. The email reports the transfer that
   actually happened, read back from the push log, rather than asserting one that was about
   to happen. If validation fails the file is not pushed, the email still goes, and it says
   what was held and why — losing a week's push is recoverable, a malformed Advanced PO
   landing in THE ICONIC's production WMS at 4am is less so.

   Every dependency is injected. This module decides what happens and in what order; it does
   not know how to query, render, upload or send. That keeps it testable without a database,
   an SFTP server or a mail provider.
*/
'use strict';

module.exports = function createWeeklyReportJob(deps) {
  const {
    // data
    buildWorkbook,          // (weekStart) -> { buffer, filename, counts, missing }
    buildApo,               // (weekStart) -> { csv, filename, rows, metrics }
    buildDiscrepancyPdf,    // (weekStart, { cost }) -> { buffer, filename }
    buildDiscrepancyXlsx,   // (weekStart, { cost }) -> { buffer, filename }
    buildStockStatus,       // (weekStart) -> { buffer, filename }
    buildSupplierSummary,   // (weekStart) -> { buffer, filename }
    weekFigures,            // (weekStart) -> the numbers the summary is written from
    // actions
    publishToIconic,        // ({ filename, csv, weekStart }) -> { ok, remotePath, bytes, error }
    sendMail,               // ({ from, to, cc, subject, html, text, attachments }) -> { id }
    pulseNarrative,         // (context) -> string | null
    // plumbing
    logEmail,               // (row) -> void
    recentApoRowCounts,     // (weekStart, n) -> number[]  the last n weeks, for the sanity check
    now,                    // () -> Date  (injected so the schedule can be tested)
    logger,
  } = deps;

  const log = logger || console;
  // Configurable so a change of hour does not need a deploy.
  const TZ = deps.tz || 'America/Chicago';
  const SEND_HOUR = Number.isFinite(deps.sendHour) ? deps.sendHour : 13;   // 1pm Chicago
  const SEND_DOW = Number.isFinite(deps.sendDow) ? deps.sendDow : 0;       // Sunday

  // ── The reporting week ──
  // Derived from the week that just closed, never from "today" in a timezone. At the send
  // moment Chicago says Sunday (the last day of week 38) while Sydney already says Monday
  // (the first day of week 39) — reading the local clock would label every email one week
  // ahead, every week.
  function reportingWeek(at) {
    const d = new Date(at.getTime());
    // Monday of the week containing the send instant, in UTC terms, then step back if the
    // send lands on the Sunday that closes the week being reported.
    const local = new Date(new Intl.DateTimeFormat('en-CA', { timeZone: TZ }).format(d) + 'T00:00:00Z');
    const dow = (local.getUTCDay() + 6) % 7;          // 0 = Monday
    const monday = new Date(local.getTime());
    monday.setUTCDate(monday.getUTCDate() - dow);
    return monday.toISOString().slice(0, 10);
  }

  const addDays = (ymd, n) => {
    const d = new Date(ymd + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };

  function isoWeek(ymd) {
    const d = new Date(ymd + 'T00:00:00Z');
    const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    t.setUTCDate(t.getUTCDate() - ((t.getUTCDay() + 6) % 7) + 3);
    const f = new Date(Date.UTC(t.getUTCFullYear(), 0, 4));
    f.setUTCDate(f.getUTCDate() - ((f.getUTCDay() + 6) % 7) + 3);
    return 1 + Math.round((t - f) / (7 * 86400000));
  }

  const fmtDay = (value) => {
    const raw = String(value == null ? '' : value).trim();
    if (!raw) return '';
    const datePart = raw.slice(0, 10);              // 'YYYY-MM-DD' out of a date or a timestamp
    const d = new Date(datePart + 'T00:00:00Z');
    if (isNaN(d.getTime())) return raw;             // show what we were given, never 'Invalid Date'
    return d.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  };

  // ── Validation ──
  // The only thing standing between a bad file and THE ICONIC's WMS once this runs unattended.
  // Each check answers "would a person have noticed this before clicking Publish?".
  function validateApo(built, history) {
    const problems = [];
    const rows = built.rows.length;

    if (!rows) problems.push('The file has no rows.');

    const past = (history || []).filter(n => Number.isFinite(n) && n > 0);
    if (past.length >= 2) {
      const avg = past.reduce((a, b) => a + b, 0) / past.length;
      // A week genuinely varies. An order of magnitude does not.
      if (rows < avg * 0.4) {
        problems.push(`Row count ${rows} is well below the recent average of ${Math.round(avg)}.`);
      } else if (rows > avg * 2.5) {
        problems.push(`Row count ${rows} is well above the recent average of ${Math.round(avg)}.`);
      }
    }

    const m = built.metrics || {};
    if (m['Unmatched UID rows (no Plan match)'] > rows * 0.1) {
      problems.push(`${m['Unmatched UID rows (no Plan match)']} UID rows had no plan match.`);
    }
    if (m['Rows with freight TBD'] > 0) {
      problems.push(`${m['Rows with freight TBD']} rows have no freight mode, so no delivery date.`);
    }

    // Structural checks: the importer reads positionally, so a missing field is not survivable.
    const bad = built.rows.filter(r => !String(r.po_number || '').trim() || !String(r.sku || '').trim());
    if (bad.length) problems.push(`${bad.length} rows are missing a PO or SKU.`);

    return problems;
  }

  // ── The narrative ──
  // Pulse writes from figures already computed, not from raw tables, so it cannot contradict
  // the attachments beneath it. If it is slow or unavailable the deterministic version goes
  // instead — the email is never held up for it.
  // Said plainly under the summary, so nobody has to wonder whether a person wrote it.
  const CREDIT_PULSE = 'Summary by Pulse AI \u2014 Powered by Anthropic Claude';
  const CREDIT_TEMPLATE = 'Summary generated from this week\u2019s recorded figures';

  async function narrativeFor(figures) {
    const fallback = templateNarrative(figures);
    if (typeof pulseNarrative !== 'function') {
      return { text: fallback, source: 'template', credit: CREDIT_TEMPLATE };
    }
    try {
      const out = await Promise.race([
        pulseNarrative(figures),
        new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 10000)),
      ]);
      const text = String(out || '').trim();
      if (!text) return { text: fallback, source: 'template', credit: CREDIT_TEMPLATE };
      return { text, source: 'pulse', credit: CREDIT_PULSE };
    } catch (e) {
      log.warn('[weekly] pulse narrative unavailable:', e.message);
      return { text: fallback, source: 'template_after_pulse_error', credit: CREDIT_TEMPLATE };
    }
  }

  function templateNarrative(f) {
    const parts = [];
    if (f.plannedUnits || f.appliedUnits) {
      const pct = f.plannedUnits ? Math.round((f.appliedUnits / f.plannedUnits) * 100) : null;
      parts.push(`${(f.appliedUnits || 0).toLocaleString()} units applied against ${(f.plannedUnits || 0).toLocaleString()} planned`
        + (pct != null ? ` (${pct}%).` : '.'));
    }
    if (f.posLate) parts.push(`${f.posLate} purchase order${f.posLate === 1 ? '' : 's'} arrived after the baseline.`);
    if (f.discrepancySuppliers) {
      parts.push(`${f.discrepancySuppliers} supplier${f.discrepancySuppliers === 1 ? '' : 's'} have discrepancies against PO — detail attached.`);
    }
    if (f.containersInTransit) parts.push(`${f.containersInTransit} consignments are in transit.`);
    return parts.join(' ') || 'No activity recorded for this week.';
  }

  // ── The email ──
  function renderEmail({ weekStart, narrative, figures, freight, push, counts }) {
    const wk = isoWeek(weekStart);
    const subject = `Week ${wk} — Reports and Data`;

    const pushBlock = push.skipped
      ? { title: 'Advanced PO — not published', detail: push.reason, bad: true }
      : push.ok
        ? { title: 'Advanced PO — published to THE ICONIC',
            detail: `${push.filename} · ${push.rows.toLocaleString()} rows · ${(push.bytes / 1024 / 1024).toFixed(2)} MB · delivered to ${push.remotePath}`,
            bad: false }
        : { title: 'Advanced PO — transfer failed', detail: push.error || 'Unknown error.', bad: true };

    // One line per consignment: what it is, what is aboard, when it moved. `detail` carries
    // whatever identifies the box for this client — size and vessel, or the flight.
    const freightLines = (freight || []).map(f =>
      [`${f.reference} · ${f.mode}`,
       f.detail || null,
       f.poCount ? `${f.poCount} PO${f.poCount === 1 ? '' : 's'}` : null,
       f.zendesk ? `Zendesk ${f.zendesk}` : null,
       f.etd ? `ETD ${fmtDay(f.etd)}` : null,
       f.eta ? `ETA ${fmtDay(f.eta)}` : null,
      ].filter(Boolean).join(' · '));

    const text = [
      `Week ${wk} — Reports and Data`,
      `${fmtDay(weekStart)} to ${fmtDay(addDays(weekStart, 6))}`,
      '',
      narrative.text,
      narrative.credit || '',
      '',
      'THIS WEEK',
      `  Planned units      ${(figures.plannedUnits || 0).toLocaleString()}`,
      `  Applied units      ${(figures.appliedUnits || 0).toLocaleString()}`,
      `  POs received       ${figures.posReceived || 0}`,
      `  POs late           ${figures.posLate || 0}`,
      `  Mobile bins        ${(figures.bins || 0).toLocaleString()}`,
      '',
      freightLines.length ? 'FREIGHT' : '',
      ...freightLines.map(l => '  ' + l),
      '',
      pushBlock.title.toUpperCase(),
      '  ' + pushBlock.detail,
      '',
      'ATTACHED',
      ...Object.entries(counts).map(([k, v]) => `  ${k} — ${v}`),
    ].filter(l => l !== '').join('\n');

    const esc = (v) => String(v == null ? '' : v)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    const html = `
<div style="font-family:-apple-system,Segoe UI,sans-serif;color:#1C1C1E;max-width:680px;">
  <div style="font-size:17px;font-weight:700;">Week ${wk} — Reports and Data</div>
  <div style="font-size:12px;color:#6E6E73;margin-top:2px;">${esc(fmtDay(weekStart))} to ${esc(fmtDay(addDays(weekStart, 6)))}</div>

  <p style="font-size:14px;line-height:1.6;margin:16px 0 4px;">${esc(narrative.text)}</p>
  <div style="font-size:11px;color:#8E8E93;margin:0 0 14px;">${esc(narrative.credit || '')}</div>

  <table style="border-collapse:collapse;margin:16px 0;font-size:13px;">
    ${[['Planned units', (figures.plannedUnits || 0).toLocaleString()],
       ['Applied units', (figures.appliedUnits || 0).toLocaleString()],
       ['POs received', figures.posReceived || 0],
       ['POs late', figures.posLate || 0],
       ['Mobile bins', (figures.bins || 0).toLocaleString()]].map(([k, v]) => `
      <tr><td style="padding:4px 18px 4px 0;color:#6E6E73;">${esc(k)}</td>
          <td style="padding:4px 0;font-weight:600;">${esc(v)}</td></tr>`).join('')}
  </table>

  ${freightLines.length ? `
    <div style="font-size:13px;font-weight:600;margin-top:18px;">Freight</div>
    <ul style="font-size:13px;line-height:1.7;padding-left:18px;margin:6px 0;">
      ${freightLines.map(l => `<li>${esc(l)}</li>`).join('')}
    </ul>` : ''}

  <div style="margin-top:18px;padding:12px 14px;border-radius:8px;
       background:${pushBlock.bad ? '#FDF2F2' : '#F3F8F4'};
       border:1px solid ${pushBlock.bad ? '#E6C4C4' : '#CADDCF'};">
    <div style="font-size:13px;font-weight:600;">${esc(pushBlock.title)}</div>
    <div style="font-size:12px;color:#6E6E73;margin-top:3px;">${esc(pushBlock.detail)}</div>
  </div>

  <div style="font-size:13px;font-weight:600;margin-top:18px;">Attached</div>
  <ul style="font-size:13px;line-height:1.7;padding-left:18px;margin:6px 0;">
    ${Object.entries(counts).map(([k, v]) => `<li>${esc(k)} — ${esc(v)}</li>`).join('')}
  </ul>
</div>`;

    return { subject, html, text };
  }

  // ── The run ──
  async function run(options) {
    const opts = options || {};
    const at = opts.at ? new Date(opts.at) : now();
    const weekStart = opts.weekStart || reportingWeek(at);
    const dryRun = !!opts.dryRun;
    const trigger = opts.trigger || 'cron';

    log.log(`[weekly] week ${weekStart} (ISO ${isoWeek(weekStart)}), trigger ${trigger}${dryRun ? ', dry run' : ''}`);

    // 1 ── Build everything first. Nothing is sent or pushed until all of it exists, so a
    //      half-built email cannot go out claiming attachments it does not have.
    const workbook = await buildWorkbook(weekStart);
    if (workbook.missing && workbook.missing.length) {
      throw new Error('The workbook is missing sheets: ' + workbook.missing.join(', ')
        + '. Nothing sent — an incomplete workbook reads as a quiet week.');
    }

    const apo = await buildApo(weekStart);
    const omitted = [];     // reports with nothing to report this week — named in the email, not attached
    const attachments = [
      { name: workbook.filename, content: workbook.buffer, label: 'Pinpoint report' },
      { name: apo.filename, content: Buffer.from(apo.csv, 'utf8'), label: 'Advanced PO' },
    ];

    // A builder that is not offered is skipped rather than attached empty. The discrepancy
    // report has no PDF generator, so it goes out once as a workbook.
    for (const [fn, label] of [
      [buildStockStatus && (() => buildStockStatus(weekStart)), 'Stock status'],
      [buildSupplierSummary && (() => buildSupplierSummary(weekStart)), 'Receiving supplier summary'],
      [buildDiscrepancyPdf && (() => buildDiscrepancyPdf(weekStart, { cost: true })), 'Supplier discrepancy (PDF)'],
      [buildDiscrepancyXlsx && (() => buildDiscrepancyXlsx(weekStart, { cost: true })), 'Supplier discrepancy'],
    ]) {
      if (!fn) continue;
      const out = await fn();
      if (out && out.none) { omitted.push({ label, note: out.note || 'Nothing to report this week' }); continue; }
      if (attachments.some(a => a.name === out.filename)) continue;   // never the same file twice
      attachments.push({ name: out.filename, content: out.buffer, label });
    }

    // 2 ── Validate before anything leaves the building.
    const history = (typeof recentApoRowCounts === 'function')
      ? await recentApoRowCounts(weekStart, 4) : [];
    const problems = validateApo(apo, history);

    // 3 ── Publish. Before the email, so the status reported is the one that happened.
    let push;
    if (problems.length) {
      push = { skipped: true, filename: apo.filename, rows: apo.rows.length,
               reason: 'Held: ' + problems.join(' ') + ' The file is attached for review; nothing was sent to THE ICONIC.' };
      log.warn('[weekly] APO held back:', problems.join(' '));
    } else if (dryRun) {
      push = { skipped: true, filename: apo.filename, rows: apo.rows.length, reason: 'Dry run — not published.' };
    } else {
      try {
        const r = await publishToIconic({ filename: apo.filename, csv: apo.csv, weekStart });
        push = r && r.ok
          ? { ok: true, filename: apo.filename, rows: apo.rows.length,
              bytes: r.bytes || Buffer.byteLength(apo.csv, 'utf8'), remotePath: r.remotePath || '' }
          : { ok: false, filename: apo.filename, rows: apo.rows.length, error: (r && r.error) || 'Unknown error.' };
      } catch (e) {
        push = { ok: false, filename: apo.filename, rows: apo.rows.length, error: String(e.message || e) };
      }
      if (!push.ok) log.error('[weekly] SFTP push failed:', push.error);
    }

    // 4 ── Compose.
    const figures = await weekFigures(weekStart);
    const narrative = await narrativeFor(figures);
    const counts = {};
    for (const a of attachments) counts[a.label] = a.name;
    for (const o of omitted) counts[o.label] = o.note;

    const { subject, html, text } = renderEmail({
      weekStart, narrative, figures, freight: figures.freight || [], push, counts,
    });

    // 5 ── Send. A held or failed push is an internal matter: the client list gets the
    //      reports, and operations gets told the file did not go.
    const to = opts.to || [];
    const cc = opts.cc || [];
    const ops = opts.ops || [];

    const result = { weekStart, subject, push, narrative_source: narrative.source,
                     workbook_counts: workbook.counts || null,
                     attachments: attachments.map(a => ({ name: a.name, bytes: a.content.length })) };

    if (dryRun) {
      log.log('[weekly] dry run — not sending.');
      return Object.assign(result, { sent: false, dryRun: true, html, text });
    }

    const payload = attachments.map(a => ({
      filename: a.name,
      content: Buffer.isBuffer(a.content) ? a.content.toString('base64') : a.content,
    }));

    const sent = await sendMail({
      to, cc, subject, html, text, attachments: payload,
    });
    result.sent = true;
    result.message_id = sent && sent.id;

    logEmail({
      kind: 'weekly_general', week_start: weekStart, subject,
      to: to.join(','), cc: cc.join(','), narrative_source: narrative.source,
      trigger_source: trigger, message_id: result.message_id || null,
      attachments: attachments.length, push_status: push.ok ? 'published' : (push.skipped ? 'held' : 'failed'),
    });

    // Operations hears about a push that did not happen, in its own mail, so the client list
    // is not told about a validation rule they have no way to act on.
    if (ops.length && (push.skipped || push.ok === false)) {
      await sendMail({
        to: ops, subject: `[action] ${subject} — Advanced PO not published`,
        text: `The week ${isoWeek(weekStart)} reports went out, but the Advanced PO was not published.\n\n`
          + (push.reason || push.error || '') + `\n\nThe file is attached.`,
        html: `<p>The week ${isoWeek(weekStart)} reports went out, but the Advanced PO was not published.</p>`
          + `<p>${String(push.reason || push.error || '')}</p>`,
        attachments: [{ filename: apo.filename, content: Buffer.from(apo.csv, 'utf8').toString('base64') }],
      });
      result.ops_notified = true;
    }

    log.log(`[weekly] sent ${subject} — ${attachments.length} attachments, push ${push.ok ? 'ok' : (push.skipped ? 'held' : 'failed')}`);
    return result;
  }

  // ── Schedule ──
  // Checked every 15 minutes against Chicago local time rather than computed as a UTC offset,
  // so the hour holds across daylight saving at both ends.
  function shouldRunAt(at, lastRunWeek) {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: TZ, weekday: 'short', hour: 'numeric', hour12: false,
    }).formatToParts(at);
    const wd = parts.find(p => p.type === 'weekday').value;
    const hr = Number(parts.find(p => p.type === 'hour').value);
    const dowMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
    // On or after the window rather than exactly inside it. A process that was down at 1pm
    // used to skip the week in silence; now it sends when it comes back.
    const dow = dowMap[wd];
    const past = dow === SEND_DOW ? hr >= SEND_HOUR : dow > SEND_DOW;
    if (!past) return false;
    return reportingWeek(at) !== lastRunWeek;      // one send per reporting week
  }

  function start(options) {
    const opts = options || {};
    // Seeded from the send log, not from memory. A restart inside the send window used to
    // begin with no history and send the week a second time.
    let lastRunWeek = null;
    if (typeof deps.lastSentWeek === 'function') {
      try {
        lastRunWeek = deps.lastSentWeek() || null;
        if (lastRunWeek) log.log('[weekly] last sent week on record: ' + lastRunWeek);
      } catch (e) { log.warn('[weekly] could not read the send log:', e.message); }
    }
    const tick = async () => {
      try {
        const at = now();
        if (!shouldRunAt(at, lastRunWeek)) return;

        // Checked again against the log rather than trusting memory alone.
        const wk = reportingWeek(at);
        if (typeof deps.lastSentWeek === 'function') {
          try {
            if (deps.lastSentWeek() === wk) { lastRunWeek = wk; return; }
          } catch (_) { /* if the log cannot be read, memory is the fallback */ }
        }
        lastRunWeek = wk;
        await run(Object.assign({}, opts, { trigger: 'cron', at }));
      } catch (e) {
        log.error('[weekly] run failed:', e.message);
        if (typeof deps.logFailure === 'function') { try { deps.logFailure({ week_start: reportingWeek(now()), error: String(e.message || e) }); } catch (_) {} }
        if (opts.ops && opts.ops.length) {
          try {
            await sendMail({ to: opts.ops, subject: '[failed] Weekly reports did not send',
              text: 'The weekly reporting job failed.\n\n' + String(e.stack || e.message || e),
              html: '<p>The weekly reporting job failed.</p><pre>' + String(e.message || e) + '</pre>' });
          } catch (_) { /* the log is the last resort */ }
        }
      }
    };
    const timer = setInterval(tick, 15 * 60 * 1000);
    if (timer.unref) timer.unref();
    log.log('[weekly] scheduled — Sundays 13:00 ' + TZ);
    return { stop: () => clearInterval(timer), tick };
  }

  return { run, start, shouldRunAt, reportingWeek, isoWeek, validateApo, renderEmail, templateNarrative };
};
