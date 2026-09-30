/* ── VelOzity Pinpoint — transit alerts v1 ──
   One alert record, three places it can be shown: the live map, the exceptions panel, and
   email. Three separate implementations would give three definitions of "delayed" that
   disagree, and people stop believing all of them.

   Alerts are DERIVED, not stored. There is no queue to drain and nothing to keep in sync with
   reality: the consignment is the truth, and an alert is a reading of it. What is stored is
   only what cannot be derived — that somebody has seen it, and that an email already went, so
   the same slip is not reported twice.

   What fires, and the reasoning:

     · the FC date crossed a day boundary after the dock was booked. Not every revision: the
       dock is booked a week out against a day, so an alert is worth sending when the day
       changes and not when the hour does. Alerting on every carrier revision is how a channel
       becomes noise inside a fortnight.

     · a stage is days past its plan with nobody confirming. Either it is late or nobody
       recorded it, and both need a person.

     · a consignment is still running on a default transit. Its ETA is a guess, and anything
       booked against it is a guess too.

   Mount:
     app.use('/alerts', require('./alert_routes')({ express, db, authenticateRequest,
       requireRole, auditLog, curClient }));
*/
'use strict';

module.exports = function mountAlerts(deps) {
  const { express, db, authenticateRequest, requireRole, auditLog, curClient } = deps;
  const router = express.Router();

  db.exec(`
    -- Only what cannot be derived: who has seen it, and what has already been sent.
    CREATE TABLE IF NOT EXISTS alert_state (
      client_id       TEXT NOT NULL,
      consignment_uid TEXT NOT NULL,
      kind            TEXT NOT NULL,
      signature       TEXT NOT NULL,   -- the specific shape of this alert; a new slip is a new alert
      acknowledged_at TEXT,
      acknowledged_by TEXT,
      notified_at     TEXT,
      PRIMARY KEY (client_id, consignment_uid, kind, signature)
    );
  `);

  const KIND = {
    fc_moved:     { label: 'Delivery date moved', weight: 3 },
    unconfirmed:  { label: 'Milestone unconfirmed', weight: 2 },
    no_quote:     { label: 'No carrier quote', weight: 1 },
  };

  const today = () => new Date().toISOString().slice(0, 10);
  const dayDiff = (a, b) => {
    if (!a || !b) return null;
    const x = new Date(String(a).slice(0, 10) + 'T00:00:00Z');
    const y = new Date(String(b).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(x) || isNaN(y)) return null;
    return Math.round((y - x) / 86400000);
  };

  /**
   * Reads the consignments for a week and returns what is worth telling somebody.
   * `dockBookings` maps consignment_uid -> the date the dock was booked for, mirrored from
   * the client's system into the last-mile schedule. Without one, an FC move is still
   * reported, but as information rather than as exposed labour.
   */
  function derive(consignments, dockBookings) {
    const out = [];
    const docks = dockBookings || {};

    for (const c of consignments) {
      const ref = c.reference || 'not advised';
      const conf = c.confidence || {};

      // ── The delivery date has moved ──
      const drift = typeof conf.drift === 'number' ? conf.drift : null;
      if (c.transit_confirmed && drift != null && Math.abs(drift) >= 1) {
        const dock = docks[c.consignment_uid] || null;
        const missesDock = dock && c.eta_fc ? dayDiff(dock, c.eta_fc) : null;
        out.push({
          kind: 'fc_moved',
          consignment_uid: c.consignment_uid,
          reference: ref,
          // The signature is the fact, not the moment. A date that moves again is a new alert;
          // the same date re-read a hundred times is not.
          signature: `${c.baseline_fc_at || ''}->${c.eta_fc || ''}`,
          severity: (missesDock != null && missesDock > 0) ? 'high' : (drift >= 3 ? 'high' : 'medium'),
          headline: `${ref} now arrives ${c.eta_fc}, ${Math.abs(drift)} day${Math.abs(drift) === 1 ? '' : 's'} `
                  + `${drift > 0 ? 'later' : 'earlier'} than promised`,
          detail: [
            `${(c.lanes || []).length} lane${(c.lanes || []).length === 1 ? '' : 's'} aboard`,
            c.baseline_fc_at ? `first promised ${c.baseline_fc_at}` : null,
            dock ? `dock booked ${dock}` : 'no dock booking recorded',
          ].filter(Boolean).join(' · '),
          // What to do about it, which is the part an alert usually leaves out.
          action: (missesDock != null && missesDock > 0)
            ? `Arrives ${missesDock} day${missesDock === 1 ? '' : 's'} after the dock booking — rebook or hold.`
            : (dock ? 'Still inside the booked window.' : 'Book the dock against the new date.'),
          week_start: c.week_start,
          eta_fc: c.eta_fc,
          dock_at: dock,
        });
      }

      // ── Somebody has not confirmed something ──
      const overdue = (c.milestones || [])
        .filter(m => m.state === 'assumed' && m.planned_at && m.planned_at <= today())
        .map(m => ({ stage: m.stage, planned: m.planned_at, days: dayDiff(m.planned_at, today()) }))
        .sort((a, b) => b.days - a.days);
      if (overdue.length) {
        const worst = overdue[0];
        out.push({
          kind: 'unconfirmed',
          consignment_uid: c.consignment_uid,
          reference: ref,
          signature: `${worst.stage}@${worst.planned}`,
          severity: worst.days >= 3 ? 'high' : 'medium',
          headline: `${ref} — ${overdue.length} milestone${overdue.length === 1 ? '' : 's'} unconfirmed`,
          detail: `${worst.stage.replace(/_/g, ' ')} planned ${worst.planned}, ${worst.days} day`
                + `${worst.days === 1 ? '' : 's'} ago`,
          action: 'Confirm it happened on plan, or record the date it did.',
          week_start: c.week_start,
        });
      }

      // ── Running on a guess ──
      if (!c.transit_confirmed) {
        out.push({
          kind: 'no_quote',
          consignment_uid: c.consignment_uid,
          reference: ref,
          signature: 'no_quote',
          severity: 'low',
          headline: `${ref} has no carrier quote`,
          detail: 'Dates are computed from a default transit time.',
          action: 'Enter the quoted transit so the ETA means something.',
          week_start: c.week_start,
        });
      }
    }

    const rank = { high: 0, medium: 1, low: 2 };
    out.sort((a, b) => (rank[a.severity] - rank[b.severity])
      || (KIND[b.kind].weight - KIND[a.kind].weight));
    return out;
  }

  function withState(alerts) {
    const client = curClient();
    const rows = db.prepare('SELECT * FROM alert_state WHERE client_id = ?').all(client);
    const by = new Map(rows.map(r => [`${r.consignment_uid}|${r.kind}|${r.signature}`, r]));
    return alerts.map(a => {
      const st = by.get(`${a.consignment_uid}|${a.kind}|${a.signature}`) || {};
      return { ...a, label: KIND[a.kind].label,
               acknowledged_at: st.acknowledged_at || null,
               acknowledged_by: st.acknowledged_by || null,
               notified_at: st.notified_at || null };
    });
  }

  // The dock date lives in the client's system and is mirrored into the last-mile schedule,
  // so it is read from there rather than asked for again.
  function dockBookings(ws) {
    const map = {};
    try {
      const rows = db.prepare(`SELECT facility, data FROM flow_week WHERE week_start = ?`).all(ws);
      for (const r of rows) {
        let blob; try { blob = JSON.parse(r.data); } catch (_) { continue; }
        const receipts = (blob && blob.lastmile_receipts) || {};
        for (const [uid, rec] of Object.entries(receipts)) {
          const when = rec && (rec.scheduled_for || rec.scheduled_local);
          if (when) map[uid] = String(when).slice(0, 10);
        }
      }
    } catch (_) { /* a week with no last-mile data simply has no bookings */ }
    return map;
  }

  async function forWeek(ws) {
    const client = curClient();
    const rows = db.prepare(`SELECT * FROM consignment WHERE client_id = ? AND week_start = ?`)
      .all(client, ws);
    if (!rows.length) return [];

    // The consignment shape the alert reader needs, assembled here rather than by calling
    // our own HTTP route: the job and the screen would otherwise disagree about tenancy.
    const shaped = rows.map(c => {
      const ms = db.prepare('SELECT * FROM consignment_milestone WHERE consignment_uid = ?')
        .all(c.consignment_uid);
      const lanes = db.prepare('SELECT lane_key FROM consignment_lane WHERE consignment_uid = ?')
        .all(c.consignment_uid).map(x => x.lane_key);
      const fc = ms.find(m => m.stage === 'fc_receipt');
      const etaFc = fc ? (fc.actual_at || fc.planned_at) : null;
      const drift = (c.baseline_fc_at && etaFc) ? dayDiff(c.baseline_fc_at, etaFc) : null;
      return {
        consignment_uid: c.consignment_uid, reference: c.reference, week_start: c.week_start,
        transit_confirmed: !!c.transit_confirmed, baseline_fc_at: c.baseline_fc_at,
        eta_fc: etaFc, lanes,
        confidence: { drift },
        milestones: ms.map(m => ({ stage: m.stage, planned_at: m.planned_at,
                                   actual_at: m.actual_at, state: m.state })),
      };
    });

    return withState(derive(shaped, dockBookings(ws)));
  }

  // ── Read ──
  router.get('/', authenticateRequest, auditLog('view_alerts'), async (req, res) => {
    try {
      const ws = String(req.query.week || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ws)) {
        return res.status(400).json({ ok: false, error: 'week must be YYYY-MM-DD.' });
      }
      const all = await forWeek(ws);
      const open = all.filter(a => !a.acknowledged_at);
      res.json({
        ok: true, week_start: ws,
        counts: {
          total: all.length, open: open.length,
          high: open.filter(a => a.severity === 'high').length,
          acknowledged: all.length - open.length,
        },
        alerts: all,
      });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // ── Acknowledge ──
  // Not a delete. The alert stays derivable; acknowledging records that somebody has dealt
  // with this particular slip, and a further slip produces a new signature and reappears.
  router.post('/ack', authenticateRequest, requireRole(['admin', 'supplier']),
    auditLog('ack_alert'), (req, res) => {
    try {
      const b = req.body || {};
      const uid = String(b.consignment_uid || '').trim();
      const kind = String(b.kind || '').trim();
      const sig = String(b.signature || '').trim();
      if (!uid || !KIND[kind]) return res.status(400).json({ ok: false, error: 'consignment_uid and a valid kind are required.' });

      db.prepare(`INSERT INTO alert_state (client_id, consignment_uid, kind, signature,
                    acknowledged_at, acknowledged_by)
                  VALUES (@c,@u,@k,@s,@at,@by)
                  ON CONFLICT(client_id, consignment_uid, kind, signature) DO UPDATE SET
                    acknowledged_at = @at, acknowledged_by = @by`)
        .run({ c: curClient(), u: uid, k: kind, s: sig,
               at: b.clear ? null : new Date().toISOString(),
               by: b.clear ? null : ((req.auth && req.auth.userId) || null) });
      res.json({ ok: true });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  // What has not been emailed yet, so the sender never repeats itself. Marking happens only
  // after a send succeeds — a failed send must be retried, not silently swallowed.
  router.getUnnotified = async function (ws, minSeverity) {
    const rank = { high: 0, medium: 1, low: 2 };
    const floor = rank[minSeverity || 'medium'];
    const all = await forWeek(ws);
    return all.filter(a => !a.notified_at && !a.acknowledged_at && rank[a.severity] <= floor);
  };

  router.markNotified = function (alerts) {
    const client = curClient();
    const at = new Date().toISOString();
    const stmt = db.prepare(`INSERT INTO alert_state (client_id, consignment_uid, kind, signature, notified_at)
                             VALUES (?,?,?,?,?)
                             ON CONFLICT(client_id, consignment_uid, kind, signature)
                             DO UPDATE SET notified_at = excluded.notified_at`);
    const tx = db.transaction(() => {
      for (const a of alerts) stmt.run(client, a.consignment_uid, a.kind, a.signature, at);
    });
    tx();
  };

  router.forWeek = forWeek;
  router.derive = derive;
  console.log('[alerts] engine v1 mounted');
  return router;
};
