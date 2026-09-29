/* ── VelOzity Pinpoint — lane slip diagnostic v1 ──
   Answers one question before anything is built: where does transit time actually go?

   The plan is to spend money on carrier event tracking to catch vessel delays. That is only
   worth doing if the vessel legs are where the time is lost. If most of it sits landside —
   customs, deconsolidation, transport — an aggregator fixes the wrong leg and the labour is
   still wasted.

   Two things it is careful about:

     · autofilled dates are excluded from the slip figures. The autofill job writes projected
       dates into lane_actual_dates with source='auto_filled', so including them would compare
       the plan against a projection of itself and produce a confident, meaningless answer.
       They are counted and reported separately, because how much of the table is guesses is
       itself worth knowing.

     · slip is measured per stage as the time that stage ADDED, not the variance it inherited.
       A sailing that departs four days late and then runs perfectly has one problem, not six.

   Mount:
     app.use('/lanes', require('./lane_slip_report')({ express, db, authenticateRequest,
       auditLog, curClient }));

   GET /lanes/slip?weeks=12
*/
'use strict';

module.exports = function mountLaneSlip(deps) {
  const { express, db, authenticateRequest, auditLog } = deps;
  const router = express.Router();

  // In the order a consignment passes through them. The gap between two consecutive stages
  // is the leg, and a leg belongs either to the carrier or to us.
  const STAGES = [
    { key: 'packing_list_ready', label: 'Packing list ready', planned: 'planned_packing_list_ready_at' },
    { key: 'origin_cleared', label: 'Origin cleared', planned: 'planned_origin_cleared_at' },
    { key: 'departed', label: 'Departed', planned: 'planned_departed_at' },
    { key: 'arrived', label: 'Arrived', planned: 'planned_arrived_at' },
    { key: 'dest_cleared', label: 'Destination cleared', planned: 'planned_dest_cleared_at' },
    { key: 'fc_receipt', label: 'FC receipt', planned: 'planned_fc_receipt_at' },
  ];

  // Which side of the fence each leg falls on. This is the whole point of the report: the
  // aggregator can only help with the legs the carrier controls.
  const OWNER = {
    packing_list_ready: 'supplier',
    origin_cleared: 'origin landside',
    departed: 'carrier',        // ready to on-board: the ETD slip
    arrived: 'carrier',         // the sea or air leg itself
    dest_cleared: 'destination landside',
    fc_receipt: 'destination landside',
  };

  const dayDiff = (a, b) => {
    if (!a || !b) return null;
    const x = new Date(String(a).slice(0, 10) + 'T00:00:00Z');
    const y = new Date(String(b).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(x) || isNaN(y)) return null;
    return Math.round((y - x) / 86400000);
  };

  const addWeeks = (ws, n) => {
    const d = new Date(ws + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 7 * n);
    return d.toISOString().slice(0, 10);
  };
  const mondayOf = (d) => {
    const x = new Date(d);
    x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7));
    return x.toISOString().slice(0, 10);
  };

  const median = (a) => {
    if (!a.length) return null;
    const s = a.slice().sort((x, y) => x - y);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : Math.round(((s[m - 1] + s[m]) / 2) * 10) / 10;
  };
  const mean = (a) => a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 10) / 10 : null;

  router.get('/slip', authenticateRequest, auditLog('lane_slip_report'), (req, res) => {
    try {
      const weeks = Math.min(52, Math.max(1, Number(req.query.weeks) || 12));
      const from = addWeeks(mondayOf(new Date()), -weeks);

      const snaps = db.prepare(`SELECT * FROM lane_planned_snapshots WHERE week_start >= ?`).all(from);
      const acts = db.prepare(`SELECT lane_key, week_start, stage, actual_at, source
                                 FROM lane_actual_dates WHERE week_start >= ?`).all(from);

      // How much of the actuals table is real. Reported whatever the answer, because it
      // decides how much weight the rest of this deserves.
      const bySource = {};
      for (const a of acts) bySource[a.source] = (bySource[a.source] || 0) + 1;
      const real = acts.filter(a => a.source !== 'auto_filled');

      const actBy = new Map();
      for (const a of real) {
        const k = a.lane_key + '||' + a.week_start;
        if (!actBy.has(k)) actBy.set(k, {});
        actBy.get(k)[a.stage] = a.actual_at;
      }

      const added = {};            // stage -> [days added]
      for (const s of STAGES) added[s.key] = [];
      let lanesWithAny = 0, lanesComplete = 0;

      for (const snap of snaps) {
        const got = actBy.get(snap.lane_key + '||' + snap.week_start);
        if (!got) continue;
        lanesWithAny++;
        let inherited = 0, seen = 0;
        for (const s of STAGES) {
          const plan = snap[s.planned], act = got[s.key];
          if (!plan || !act) continue;
          const total = dayDiff(plan, act);
          if (total == null) continue;
          added[s.key].push(total - inherited);
          inherited = total;
          seen++;
        }
        if (seen === STAGES.length) lanesComplete++;
      }

      const stages = STAGES.map(s => ({
        stage: s.key,
        label: s.label,
        owner: OWNER[s.key],
        observations: added[s.key].length,
        mean_days_added: mean(added[s.key]),
        median_days_added: median(added[s.key]),
        worst_days_added: added[s.key].length ? Math.max(...added[s.key]) : null,
        // How often this stage is the one adding time, rather than inheriting it.
        share_late: added[s.key].length
          ? Math.round(added[s.key].filter(v => v > 0).length / added[s.key].length * 100) : null,
      }));

      const byOwner = {};
      for (const s of stages) {
        if (!s.observations) continue;
        const o = byOwner[s.owner] || (byOwner[s.owner] = { days_added: 0, observations: 0 });
        o.days_added += (s.mean_days_added || 0);
        o.observations += s.observations;
      }
      const totalAdded = Object.values(byOwner).reduce((n, o) => n + Math.max(0, o.days_added), 0);
      const ownerSplit = Object.entries(byOwner).map(([owner, o]) => ({
        owner,
        mean_days_added: Math.round(o.days_added * 10) / 10,
        share_of_delay: totalAdded > 0 ? Math.round(Math.max(0, o.days_added) / totalAdded * 100) : null,
      })).sort((a, b) => b.mean_days_added - a.mean_days_added);

      const carrier = ownerSplit.find(o => o.owner === 'carrier');

      res.json({
        ok: true,
        window: { weeks, from },
        confidence: {
          actual_rows_by_source: bySource,
          real_rows_used: real.length,
          autofilled_rows_ignored: (bySource.auto_filled || 0),
          lanes_with_any_real_date: lanesWithAny,
          lanes_with_every_stage: lanesComplete,
          // Said plainly rather than left for the reader to work out.
          note: real.length < 40
            ? 'Too few manually recorded dates to draw a conclusion. Confirm-or-amend needs to land first.'
            : (lanesComplete < 10
              ? 'Enough rows to indicate a direction, too few complete lanes to be certain.'
              : 'Enough complete lanes to act on.'),
        },
        stages,
        by_owner: ownerSplit,
        // Readable without unfolding the arrays.
        readout: [
          `${real.length} real dates, ${(bySource.auto_filled || 0)} autofilled and ignored`,
          `${lanesWithAny} lanes have at least one real date, ${lanesComplete} have all ${STAGES.length}`,
          `${(real.length && lanesWithAny) ? (real.length / lanesWithAny).toFixed(1) : 0} stages recorded per lane, of ${STAGES.length}`,
          ...stages.filter(x => x.observations).map(x =>
            `${x.label}: ${x.observations} obs, ${x.mean_days_added > 0 ? '+' : ''}${x.mean_days_added}d mean (${x.owner})`),
          ...stages.filter(x => !x.observations).map(x => `${x.label}: no paired plan and actual (${x.owner})`),
        ],
        verdict: (() => {
          if (real.length < 40) {
            return 'Not enough manually recorded dates to say where the time goes.';
          }
          if (!lanesComplete) {
            return `No lane has all ${STAGES.length} stages recorded, so no consignment can be `
              + 'followed end to end. Where the delay sits cannot be established from this data.';
          }
          if (!carrier || carrier.share_of_delay == null) {
            return 'The carrier legs have no measured slip in this window — not enough paired '
              + 'plan and actual dates on those stages to judge.';
          }
          return carrier.share_of_delay >= 50
            ? `The carrier legs account for ${carrier.share_of_delay}% of the delay — event tracking would address the majority of it.`
            : `The carrier legs account for ${carrier.share_of_delay}% of the delay — most of the time is lost landside, where an aggregator would not help.`;
        })(),
      });
    } catch (e) {
      console.error('[lanes/slip] failed:', e);
      res.status(500).json({ ok: false, error: String(e.message || e) });
    }
  });

  console.log('[lanes] slip diagnostic v1 mounted');
  return router;
};
