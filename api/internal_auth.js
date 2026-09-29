/* ── VelOzity Pinpoint — internal loopback auth v1 ──
   Lets the weekly job call this server's own endpoints for the two reports whose computation
   lives inside route handlers (stock status, supplier discrepancy — ~425 lines between them).

   The alternative was moving those 425 lines out of the handlers that produce the numbers
   everyone depends on. This is the smaller risk: the job gets byte-identical files to the
   ones the screens produce, and the reporting code is not touched at all.

   What keeps it safe, and the reasons each one is here:

     · the secret is generated at boot with crypto.randomBytes and held in memory only.
       Nothing is written to disk, the database or the environment, and it dies with the
       process, so there is no credential to leak or rotate.

     · the request must arrive on the loopback interface. A forwarded request carries
       X-Forwarded-For, so anything that reached us through Render's proxy is refused even
       if it somehow knew the secret.

     · comparison is timing-safe, so the secret cannot be recovered a byte at a time.

     · it grants exactly one identity — a job user with admin role — and every request it
       makes is audit-logged like any other, under userId 'weekly-report-job', so an
       internal call is distinguishable from a person's in the audit trail.

   The honest caveat: anything else running inside this container could use it. Today nothing
   does. If that ever changes, this needs revisiting.
*/
'use strict';

const crypto = require('crypto');

module.exports = function createInternalAuth(options) {
  const opts = options || {};
  const secret = crypto.randomBytes(32).toString('hex');
  const headerName = 'x-internal-token';
  const userId = opts.userId || 'weekly-report-job';
  const orgRole = opts.orgRole || 'org:admin_auth';
  const log = opts.logger || console;

  const LOOPBACK = new Set(['127.0.0.1', '::1', '::ffff:127.0.0.1']);

  function isLoopback(req) {
    // Trust the socket, not the headers: req.ip honours trust-proxy and can be spoofed
    // upstream, while the raw socket address cannot be.
    const addr = (req.socket && req.socket.remoteAddress) || '';
    if (!LOOPBACK.has(addr)) return false;
    // A genuinely local call has not been through a proxy. If it carries forwarding headers,
    // it came from outside and was relayed — refuse it.
    if (req.headers['x-forwarded-for'] || req.headers['x-forwarded-host']) return false;
    return true;
  }

  function matches(given) {
    const a = Buffer.from(String(given || ''), 'utf8');
    const b = Buffer.from(secret, 'utf8');
    if (a.length !== b.length) return false;            // timingSafeEqual requires equal length
    return crypto.timingSafeEqual(a, b);
  }

  /**
   * Returns true when this request is the job calling itself, and stamps req.auth so the
   * rest of the stack treats it as an authenticated admin. Returns false otherwise, and the
   * caller carries on to the normal authentication path.
   */
  function accept(req) {
    const given = req.headers[headerName];
    if (!given) return false;
    if (!isLoopback(req)) {
      log.warn('[internal-auth] token presented from a non-loopback address — refused');
      return false;
    }
    if (!matches(given)) {
      log.warn('[internal-auth] token mismatch from loopback — refused');
      return false;
    }
    if (!opts.orgId) {
      log.warn('[internal-auth] no internal organisation resolved — tenancy-guarded routes will refuse this call');
    }
    // Borrows VelOzity's own internal org, so the job is treated exactly as a VelOzity admin
    // would be: same tenancy, same capabilities, nothing invented for it.
    req.auth = { userId, orgRole, orgId: opts.orgId || null, internal: true };
    return true;
  }

  // Fetch one of our own endpoints as the job.
  async function call(path, init) {
    const port = opts.port;
    if (!port) throw new Error('internal auth has no port — set it once the server is listening');
    const url = `http://127.0.0.1:${port}${path}`;
    const res = await fetch(url, Object.assign({}, init, {
      headers: Object.assign({ [headerName]: secret }, (init && init.headers) || {}),
    }));
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`internal ${path} -> ${res.status} ${body.slice(0, 200)}`);
    }
    return res;
  }

  const json = async (path, init) => (await call(path, init)).json();
  const buffer = async (path, init) => Buffer.from(await (await call(path, init)).arrayBuffer());

  return {
    accept, call, json, buffer,
    setPort: (p) => { opts.port = p; },
    setOrgId: (id) => { opts.orgId = id || null; },
    get orgId() { return opts.orgId || null; },
    headerName,
  };
};
