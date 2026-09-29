/* ── VelOzity Pinpoint — Advanced PO routes v1 ──
   Gives the server the ability to produce the Advanced PO file, which until now only the
   browser could do. The Sunday job needs it; nothing else changes.

   It calls the SAME builder the Reports screen calls (./apo_builder), so the file generated
   here and the file a person reviews on screen are the same bytes by construction. There is
   a golden test that rebuilds a real published week and compares byte for byte.

   Mount with one line in server.js:
     app.use('/apo', require('./apo_routes')({ express, db, authenticateRequest, requireRole,
                                               auditLog, curClient, scopeSql, tenantReadIds }));

   Endpoints:
     GET  /apo/build?week=YYYY-MM-DD[&format=csv]  build for a week (internal only)
     GET  /apo/settings                            the settings a build will use
     PUT  /apo/settings                            store them, so the unattended run and the
                                                   screen cannot disagree
*/
'use strict';

const APO = require('./apo_builder');

module.exports = function mountAPO(deps) {
  const {
    express, db, authenticateRequest, requireRole, auditLog,
    curClient, scopeSql, tenantReadIds,
  } = deps;

  const router = express.Router();

  // Settings are stored rather than read off the form, because the Sunday job has no form to
  // read. The screen writes here when someone changes one, so the unattended build uses what
  // a person last chose instead of a constant buried in code.
  db.exec(`
    CREATE TABLE IF NOT EXISTS apo_settings (
      client_id  TEXT PRIMARY KEY,
      data       TEXT NOT NULL,
      updated_at TEXT,
      updated_by TEXT
    );
  `);

  const internalOnly = (req, res) => {
    const role = (req.auth && req.auth.orgRole) || '';
    if (role !== 'org:admin_auth') {
      res.status(403).json({ error: 'forbidden', message: 'The Advanced PO is internal to VelOzity.' });
      return false;
    }
    return true;
  };

  function settingsFor(client) {
    try {
      const row = db.prepare('SELECT data FROM apo_settings WHERE client_id = ?').get(client);
      if (!row) return {};
      const parsed = JSON.parse(row.data);
      return (parsed && typeof parsed === 'object') ? parsed : {};
    } catch (e) {
      console.warn('[apo] settings unreadable, falling back to defaults', e.message);
      return {};
    }
  }

  const mondayOf = (ymd) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    if (isNaN(d.getTime())) return null;
    const dow = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - dow);
    return d.toISOString().slice(0, 10);
  };
  const addDays = (ymd, n) => {
    const d = new Date(String(ymd) + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };

  // The same two reads the screen makes, done directly against the database.
  function loadInputs(req, weekStart) {
    const client = curClient(req);

    const planRow = db.prepare('SELECT data FROM plans WHERE week_start = ? AND client_id = ?')
      .get(weekStart, client);
    let planRows = [];
    if (planRow && planRow.data) {
      try { planRows = JSON.parse(planRow.data) || []; }
      catch (e) { throw new Error('The plan for week ' + weekStart + ' could not be read: ' + e.message); }
    }

    // Records carry no client_id of their own, so they are scoped the way /records scopes
    // them rather than by date alone — otherwise another client's UIDs would land in the file.
    const params = [];
    let sql = 'SELECT po_number, sku_code, uid, mobile_bin FROM records WHERE 1=1';
    const sc = scopeSql(tenantReadIds(req));
    sql += sc.clause;
    params.push(...sc.params);
    sql += ' AND date_local >= ? AND date_local <= ? AND status = ?';
    params.push(weekStart, addDays(weekStart, 6), 'complete');

    const records = db.prepare(sql).all(...params);
    return { planRows, records, client };
  }

  // ── Build ──
  router.get('/build', authenticateRequest, auditLog('apo_build'), (req, res) => {
    try {
      if (!internalOnly(req, res)) return;

      const raw = String(req.query.week || req.query.week_start || '').trim();
      const weekStart = mondayOf(raw);
      if (!weekStart) return res.status(400).json({ error: 'invalid', message: 'week must be YYYY-MM-DD.' });

      const { planRows, records, client } = loadInputs(req, weekStart);
      if (!planRows.length) {
        return res.status(409).json({ error: 'no_plan',
          message: 'No plan loaded for week ' + weekStart + '. Nothing can be built until it is.' });
      }
      if (!records.length) {
        return res.status(409).json({ error: 'no_records',
          message: 'No completed UID records for week ' + weekStart + '.' });
      }

      const built = APO.build({ weekStart, planRows, records, settings: settingsFor(client) });

      if (String(req.query.format || '').toLowerCase() === 'csv') {
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="' + built.filename + '"');
        return res.send(built.csv);
      }

      res.json({
        ok: true,
        week_start: weekStart,
        filename: built.filename,
        rows: built.rows.length,
        bytes: Buffer.byteLength(built.csv, 'utf8'),
        metrics: built.metrics,
        csv: built.csv,
      });
    } catch (e) {
      res.status(500).json({ error: 'build_failed', message: String(e.message || e) });
    }
  });

  // ── Settings ──
  router.get('/settings', authenticateRequest, (req, res) => {
    if (!internalOnly(req, res)) return;
    const stored = settingsFor(curClient(req));
    res.json({ defaults: APO.APO_DEFAULTS, stored, effective: Object.assign({}, APO.APO_DEFAULTS, stored) });
  });

  router.put('/settings', authenticateRequest, requireRole(['admin']), auditLog('apo_settings'), (req, res) => {
    try {
      if (!internalOnly(req, res)) return;
      const body = (req.body && typeof req.body === 'object') ? req.body : {};

      // Only the keys the builder understands are kept: anything else would be stored,
      // never read, and quietly imply it had an effect.
      const allowed = Object.keys(APO.APO_DEFAULTS);
      const clean = {};
      for (const k of allowed) if (body[k] !== undefined && body[k] !== '') clean[k] = body[k];

      db.prepare(`INSERT INTO apo_settings (client_id, data, updated_at, updated_by)
                  VALUES (@client, @data, @at, @by)
                  ON CONFLICT(client_id) DO UPDATE SET data = @data, updated_at = @at, updated_by = @by`)
        .run({
          client: curClient(req),
          data: JSON.stringify(clean),
          at: new Date().toISOString(),
          by: (req.auth && req.auth.userId) || null,
        });

      res.json({ ok: true, stored: clean, effective: Object.assign({}, APO.APO_DEFAULTS, clean) });
    } catch (e) {
      res.status(500).json({ error: 'save_failed', message: String(e.message || e) });
    }
  });

  console.log('[apo] module v1 mounted');
  return router;
};
