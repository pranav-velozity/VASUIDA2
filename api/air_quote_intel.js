// Air quote pricing intelligence — the numbers behind "what should we charge for this one".
//
// Everything here is arithmetic over the quotes the client has already decided; nothing is
// estimated by a model. Pulse reads the result through get_quote_intel and explains it.
//
// The frame: the client has a revealed tolerance — the price per chargeable kg they approve
// and the price they decline — and that tolerance does not move when the partner's cost
// moves. So the recommendation starts from where the client's win curve is strong, and the
// margin is whatever falls out: fat when the partner comes in low (the opportunity), thin
// when the partner comes in high (the warning). The rate-card markup is shown beside it as
// the baseline the team has always used.
//
//   GET /air-quotes/:id/intel      (VelOzity admins)     → the intel object
//   GET /air-quotes/intel?ref=…    (same; by reference)  — what Pulse's tool calls
//
// Confidence: the win curve needs decided quotes. Under 10 decisions only benchmarks are
// shown; 10–24 is flagged thin; 25+ is treated as a curve.
'use strict';

module.exports = function install(deps) {
  const { app, db, authenticateRequest, requireRole, requireInternalOrg, curClient, aqMarkupDefault } = deps;
  const r2 = (n) => n == null || !isFinite(n) ? null : Math.round(n * 100) / 100;
  const r1 = (n) => n == null || !isFinite(n) ? null : Math.round(n * 10) / 10;
  const r4 = (n) => n == null || !isFinite(n) ? null : Math.round(n * 10000) / 10000;
  const median = (xs) => { if (!xs.length) return null; const s = xs.slice().sort((a, b) => a - b); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
  const mean = (xs) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
  const sd = (xs) => { if (xs.length < 2) return null; const m = mean(xs); return Math.sqrt(xs.reduce((a, x) => a + (x - m) * (x - m), 0) / (xs.length - 1)); };
  const laneKey = (q) => [String(q.origin || '').trim().toLowerCase(), String(q.destination || '').trim().toLowerCase()].join('→');
  const kgBand = (kg) => kg < 100 ? '<100 kg' : kg < 300 ? '100–300 kg' : kg < 1000 ? '300–1,000 kg' : '1,000+ kg';
  const DAYS = Number(process.env.AQ_INTEL_WINDOW_DAYS || 180);

  function compute(clientId, q) {
    const since = new Date(Date.now() - DAYS * 86400000).toISOString().slice(0, 10);
    const k = db.prepare('SELECT * FROM air_quote_cost WHERE quote_id=?').get(q.id) || {};
    const lines = db.prepare('SELECT * FROM air_quote_cost_line WHERE quote_id=?').all(q.id);
    const lineCost = lines.reduce((a, l) => a + (Number(l.cost_amount) || 0), 0);
    const cost = lineCost > 0 ? lineCost : (k.cost_amount != null ? Number(k.cost_amount) : null);
    const kg = Number(q.chargeable_kg) || 0, units = Number(q.units) || 0;
    const costKg = cost != null && kg > 0 ? cost / kg : null;
    const dflt = aqMarkupDefault(clientId);

    // History: every quote on this client with a cost, plus the decided ones with a sell.
    const hist = db.prepare(`SELECT q.id, q.state, q.vendor_key, q.vendor_raw, q.origin, q.destination, q.chargeable_kg, q.units,
                                    q.sell_amount, q.decided_at, q.created_at, k.cost_amount, k.transit_mode
                             FROM air_quote q LEFT JOIN air_quote_cost k ON k.quote_id = q.id
                             WHERE q.client_id = ? AND q.id <> ? AND q.chargeable_kg > 0 AND q.created_at >= ?`).all(clientId, q.id, since);
    const costed = hist.filter(h => h.cost_amount != null && h.cost_amount > 0).map(h => ({ ...h, cost_kg: h.cost_amount / h.chargeable_kg }));
    const decided = hist.filter(h => (h.state === 'approved' || h.state === 'declined') && h.sell_amount > 0).map(h => ({ ...h, sell_kg: h.sell_amount / h.chargeable_kg, sell_unit: h.units > 0 ? h.sell_amount / h.units : null, won: h.state === 'approved' }));
    const approved = decided.filter(h => h.won);
    const vendorApproved = approved.filter(h => h.vendor_key === q.vendor_key);
    const laneApproved = approved.filter(h => laneKey(h) === laneKey(q));
    const wavg = (xs, num, den) => { const d = xs.reduce((a, x) => a + x[den], 0); return d > 0 ? xs.reduce((a, x) => a + x[num], 0) / d : null; };
    const bench = {
      client: { sell_kg: r2(wavg(approved, 'sell_amount', 'chargeable_kg')), sell_kg_median: r2(median(approved.map(h => h.sell_kg))), n: approved.length,
                sell_unit: r4(wavg(approved.filter(h => h.units > 0), 'sell_amount', 'units')), n_units: approved.filter(h => h.units > 0).length },
      vendor: { name: q.vendor_raw, sell_kg: r2(wavg(vendorApproved, 'sell_amount', 'chargeable_kg')), n: vendorApproved.length,
                sell_unit: r4(wavg(vendorApproved.filter(h => h.units > 0), 'sell_amount', 'units')) },
      lane: { key: laneKey(q), label: [q.origin, q.destination].filter(Boolean).join(' → ') || null, sell_kg: r2(wavg(laneApproved, 'sell_amount', 'chargeable_kg')), n: laneApproved.length },
    };
    // The anchor: the client's own average, falling back to the vendor's if the client-wide one is empty.
    const anchorKg = bench.client.sell_kg != null && bench.client.n >= 3 ? bench.client.sell_kg : (bench.vendor.sell_kg != null ? bench.vendor.sell_kg : null);
    const anchorSrc = bench.client.sell_kg != null && bench.client.n >= 3 ? 'client' : (bench.vendor.sell_kg != null ? 'vendor' : null);

    // Partner cost context on this vendor: where today's cost sits, how volatile they are, and
    // what explains the spread (shipment size, transit mode).
    const vendorCosted = costed.filter(h => h.vendor_key === q.vendor_key);
    const vcost = vendorCosted.map(h => h.cost_kg);
    const partner = {
      cost_kg: r2(costKg), n: vendorCosted.length,
      median_kg: r2(median(vcost)), min_kg: r2(vcost.length ? Math.min(...vcost) : null), max_kg: r2(vcost.length ? Math.max(...vcost) : null),
      vs_median_pct: costKg != null && vcost.length ? Math.round((costKg - median(vcost)) / median(vcost) * 100) : null,
      rank_low: costKg != null && vcost.length ? 1 + vcost.filter(x => x < costKg).length : null,     // 1 = the lowest seen
      volatility_pct: vcost.length >= 3 && mean(vcost) ? Math.round(sd(vcost) / mean(vcost) * 100) : null,
      by_size: Object.entries(vendorCosted.reduce((m, h) => { const b = kgBand(h.chargeable_kg); (m[b] = m[b] || []).push(h.cost_kg); return m; }, {})).map(([band, xs]) => ({ band, median_kg: r2(median(xs)), n: xs.length })),
      by_mode: Object.entries(vendorCosted.reduce((m, h) => { const b = h.transit_mode || 'unspecified'; (m[b] = m[b] || []).push(h.cost_kg); return m; }, {})).map(([mode, xs]) => ({ mode, median_kg: r2(median(xs)), n: xs.length })),
      this_size: kgBand(kg),
    };
    const sizeMatch = partner.by_size.find(b => b.band === partner.this_size);
    partner.vs_size_median_pct = costKg != null && sizeMatch && sizeMatch.median_kg ? Math.round((costKg - sizeMatch.median_kg) / sizeMatch.median_kg * 100) : null;

    // Win curve: each decided quote's price position against the anchor, bucketed, smoothed.
    const BUCKETS = [{ lo: -Infinity, hi: 0.90, mid: 0.85, label: 'under 90%' }, { lo: 0.90, hi: 1.00, mid: 0.95, label: '90–100%' }, { lo: 1.00, hi: 1.10, mid: 1.05, label: '100–110%' }, { lo: 1.10, hi: 1.25, mid: 1.175, label: '110–125%' }, { lo: 1.25, hi: Infinity, mid: 1.35, label: 'over 125%' }];
    const curve = [];
    if (anchorKg) {
      for (const b of BUCKETS) {
        const g = decided.filter(h => { const r = h.sell_kg / anchorKg; return r >= b.lo && r < b.hi; });
        const won = g.filter(h => h.won).length;
        curve.push({ band: b.label, mid: b.mid, n: g.length, won, win_pct: g.length ? Math.round(won / g.length * 100) : null, p: (won + 1) / (g.length + 2) });   // Laplace-smoothed
      }
    }
    const overall = decided.length ? decided.filter(h => h.won).length / decided.length : null;
    const pWin = (ratio) => {
      if (!curve.length || decided.length < 10) return overall;
      // Interpolate between bucket midpoints; outside the ends, hold the end value.
      const pts = curve.map(c => ({ x: c.mid, y: c.n ? c.p : null })).filter(p => p.y != null);
      if (!pts.length) return overall;
      if (ratio <= pts[0].x) return pts[0].y; if (ratio >= pts[pts.length - 1].x) return pts[pts.length - 1].y;
      for (let i = 1; i < pts.length; i++) if (ratio <= pts[i].x) { const a = pts[i - 1], b = pts[i]; return a.y + (b.y - a.y) * (ratio - a.x) / (b.x - a.x); }
      return overall;
    };
    const confidence = decided.length >= 25 ? 'ok' : decided.length >= 10 ? 'thin' : 'none';

    // Ceiling: the lowest /kg this client has declined — on this vendor, this lane, anywhere.
    const declined = decided.filter(h => !h.won);
    const minOf = (xs) => xs.length ? r2(Math.min(...xs.map(h => h.sell_kg))) : null;
    const ceiling = { vendor_kg: minOf(declined.filter(h => h.vendor_key === q.vendor_key)), lane_kg: minOf(declined.filter(h => laneKey(h) === laneKey(q))), client_kg: minOf(declined), n_declined: declined.length };
    const ceilingKg = ceiling.vendor_kg ?? ceiling.lane_kg ?? ceiling.client_kg;

    // Options. Each is a sell total with everything that follows from it.
    const option = (key, label, why, sell) => {
      if (sell == null || cost == null || !(sell > 0)) return null;
      const sellKg = kg > 0 ? sell / kg : null, ratio = anchorKg && sellKg ? sellKg / anchorKg : null, p = ratio != null ? pWin(ratio) : overall;
      return { key, label, why, sell: r2(sell), markup_pct: r1((sell - cost) / cost * 100), gross_margin_pct: r1((sell - cost) / sell * 100), margin: r2(sell - cost),
               sell_kg: r2(sellKg), sell_unit: units > 0 ? r4(sell / units) : null, vs_anchor_pct: ratio != null ? Math.round((ratio - 1) * 100) : null,
               win_pct: p != null && confidence !== 'none' ? Math.round(p * 100) : null, expected_margin: p != null && confidence !== 'none' ? r2((sell - cost) * p) : null,
               over_ceiling: ceilingKg != null && sellKg != null && sellKg > ceilingKg };
    };
    const options = [];
    if (cost != null) {
      options.push(option('rate_card', 'Rate card', `${dflt}% markup, the default`, cost * (1 + dflt / 100)));
      if (anchorKg && kg > 0) options.push(option('hold_kg', 'Hold the average /kg', `prices at the ${anchorSrc === 'client' ? "client's" : "vendor's"} usual ${r2(anchorKg)}/kg`, anchorKg * kg));
      const unitAnchor = bench.client.sell_unit != null && bench.client.n_units >= 3 ? bench.client.sell_unit : bench.vendor.sell_unit;
      if (unitAnchor && units > 0) options.push(option('hold_unit', 'Hold the average /unit', `prices at the usual ${unitAnchor}/unit`, unitAnchor * units));
      if (anchorKg && kg > 0 && confidence !== 'none') {
        // Scan the price range for the best margin × win likelihood.
        let best = null;
        const lo = Math.max(cost * 1.02, anchorKg * kg * 0.7), hi = Math.max(anchorKg * kg * 1.5, cost * (1 + dflt / 100) * 1.2);
        for (let s = lo; s <= hi; s += Math.max(1, (hi - lo) / 160)) {
          const o = option('best_ev', 'Best expected margin', '', s);
          if (o && (best == null || (o.expected_margin || 0) > (best.expected_margin || 0))) best = o;
        }
        if (best) { best.why = `margin × win likelihood peaks here (${best.win_pct}% likely)`; options.push(best); }
      }
    }
    const opts = options.filter(Boolean);

    // Headline: opportunity or squeeze, in one sentence a reviewer can act on.
    let headline = null, kind = 'neutral';
    const holdKg = opts.find(o => o.key === 'hold_kg'), rc = opts.find(o => o.key === 'rate_card');
    if (partner.vs_median_pct != null && holdKg && rc) {
      if (partner.vs_median_pct <= -5 && holdKg.gross_margin_pct > rc.gross_margin_pct)
        { kind = 'opportunity'; headline = `Partner is ${Math.abs(partner.vs_median_pct)}% under their median on ${q.vendor_raw}. Holding the usual ${r2(anchorKg)}/kg gives ${holdKg.gross_margin_pct}% gross margin against the rate card's ${rc.gross_margin_pct}%.`; }
      else if (partner.vs_median_pct >= 5 && rc.vs_anchor_pct != null && rc.vs_anchor_pct > 5)
        { kind = 'squeeze'; headline = `Partner is ${partner.vs_median_pct}% over their median on ${q.vendor_raw}. The rate card prices ${rc.vs_anchor_pct}% above what the client usually pays${rc.over_ceiling ? ' and past a price they have declined before' : ''}; holding the average leaves ${holdKg.gross_margin_pct}% gross margin.`; }
    }
    if (!headline && rc && rc.over_ceiling) { kind = 'squeeze'; headline = `The rate card lands at ${rc.sell_kg}/kg, above ${ceilingKg}/kg the client has declined before.`; }
    if (!headline && partner.n === 0) headline = `First costed quote on ${q.vendor_raw} in ${DAYS} days — no partner benchmark yet.`;
    if (!headline && cost == null) headline = 'No partner cost yet — benchmarks only.';

    return {
      quote: { id: q.id, ref: q.ref, state: q.state, vendor: q.vendor_raw, lane: bench.lane.label, chargeable_kg: kg, units: units || null, cost: r2(cost), cost_kg: r2(costKg), cost_unit: units > 0 && cost != null ? r4(cost / units) : null, markup_default_pct: dflt, current_markup_pct: k.markup_pct ?? null },
      window_days: DAYS, headline, kind,
      anchor: { sell_kg: r2(anchorKg), source: anchorSrc },
      benchmarks: bench, partner, ceiling,
      win_curve: { confidence, decided: decided.length, overall_win_pct: overall != null ? Math.round(overall * 100) : null, buckets: curve.map(c => ({ band: c.band, n: c.n, win_pct: c.win_pct })) },
      options: opts,
      recommended: (opts.find(o => o.key === 'best_ev' && !o.over_ceiling) || opts.find(o => o.key === 'hold_kg' && !o.over_ceiling) || opts.find(o => o.key === 'rate_card') || null)?.key || null,
    };
  }

  const gate = [authenticateRequest, requireRole(['admin']), requireInternalOrg];
  const handle = (req, res, q) => {
    if (!q) return res.status(404).json({ error: 'not_found' });
    try { res.json({ ok: true, intel: compute(q.client_id, q) }); }
    catch (e) { res.status(500).json({ error: String(e.message || e) }); }
  };
  app.get('/air-quotes/intel', ...gate, (req, res) => {
    const c = curClient(); const ref = String(req.query.ref || '').trim();
    if (!ref) return res.status(400).json({ error: 'ref required' });
    const q = db.prepare('SELECT * FROM air_quote WHERE client_id=? AND (ref=? OR id=?) ORDER BY created_at DESC LIMIT 1').get(c, ref, ref);
    handle(req, res, q);
  });
  app.get('/air-quotes/:id/intel', ...gate, (req, res) => {
    const q = db.prepare('SELECT * FROM air_quote WHERE id=? AND client_id=?').get(req.params.id, curClient());
    handle(req, res, q);
  });
  return { compute };
};
