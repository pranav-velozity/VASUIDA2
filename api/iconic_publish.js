/* ── VelOzity Pinpoint — ICONIC SFTP publish v1 ──
   The upload that puts the Advanced PO on THE ICONIC's SFTP, lifted out of the
   POST /iconic/publish handler so the weekly job can call it too.

   Nothing about the transfer changes. Same .tmp-then-rename, same host-key handling, same R2
   archive, same rows written to iconic_push_log. The route keeps its validation, its auth and
   its responses — it just stops owning the upload.

   Extracted rather than copied for the obvious reason: a second implementation of a write to
   a client's production WMS is the last place two versions should be allowed to drift.

   Named iconic_publish, NOT iconic_sftp: the SSH private key is a file called `iconic_sftp`
   with no extension sitting in this same folder, and Node resolves the extensionless file
   first — requiring './iconic_sftp' loads the key, tries to parse it as JavaScript and kills
   the process at startup.

   Mount:
     const iconicPublisher = require('./iconic_publish')({ fs, ICONIC_SFTP, r2Client,
       PutObjectCommand, R2_BUCKET, logPush: _iconicLogSafe, alert: _iconicAlert });
*/
'use strict';

module.exports = function createIconicPublisher(deps) {
  const { fs, ICONIC_SFTP, r2Client, PutObjectCommand, R2_BUCKET, logPush, alert, logger } = deps;
  const log = logger || console;

  // A filename with a path in it would write outside the agreed directory. Kept here beside
  // the upload so anything calling the publisher gets the check, not only the HTTP route.
  function safeFilename(name) {
    const s = String(name || '').trim();
    if (!s) return null;
    if (s.includes('/') || s.includes('\\') || s.includes('..')) return null;
    if (!/^[A-Za-z0-9._-]+\.csv$/.test(s)) return null;
    return s;
  }

  /**
   * @param {object} input  { filename, csv, weekStart, userId }
   * @returns {Promise<{ok:boolean, remotePath?:string, bytes?:number, r2Key?:string,
   *                     error?:string, status?:number}>}
   *   Never throws for an expected failure: the caller (a route or an unattended job) gets a
   *   result it can report either way, and the log row is written before it returns.
   */
  async function publish(input) {
    const filename = safeFilename(input && input.filename);
    const csv = (input && typeof input.csv === 'string') ? input.csv : '';
    const weekStart = String((input && input.weekStart) || '').trim() || null;
    const userId = (input && input.userId) || null;

    if (!filename) {
      return { ok: false, status: 400,
        error: 'Invalid or missing filename (expected a single *.csv name with no path).' };
    }
    if (!csv.trim()) {
      return { ok: false, status: 400, error: 'Empty CSV — nothing to publish.' };
    }

    const buffer = Buffer.from(csv, 'utf8');
    const remoteFinal = `${ICONIC_SFTP.remoteDir}/${filename}`;
    const remoteTmp = `${remoteFinal}.tmp`;

    // The key is read from the read-only Render Secret File and its CONTENTS handed to ssh2,
    // which sidesteps the 0600-permission problem of passing a path.
    let privateKey;
    try {
      privateKey = fs.readFileSync(ICONIC_SFTP.keyPath);
    } catch (e) {
      const msg = `SFTP private key not readable at ${ICONIC_SFTP.keyPath}: ${e.message || e}`;
      log.error('[iconic/publish]', msg);
      logPush([filename, buffer.length, weekStart, remoteFinal, null, 'failed', msg, userId]);
      alert(filename, msg);
      return { ok: false, status: 500, error: msg };
    }

    let SftpClient;
    try { SftpClient = require('ssh2-sftp-client'); }
    catch (e) {
      const msg = 'ssh2-sftp-client is not installed on the server.';
      log.error('[iconic/publish]', msg, e.message || e);
      logPush([filename, buffer.length, weekStart, remoteFinal, null, 'failed', msg, userId]);
      return { ok: false, status: 500, error: msg };
    }

    const connectCfg = {
      host: ICONIC_SFTP.host,
      port: ICONIC_SFTP.port,
      username: ICONIC_SFTP.username,
      privateKey,
      readyTimeout: 20000,
    };
    // Optional host-key pinning. Until the fingerprint is provided we connect without
    // verification, and say so in the log rather than staying quiet about it.
    if (ICONIC_SFTP.fingerprint) {
      connectCfg.hostHash = 'sha256';
      const norm = s => String(s || '').replace(/^sha256:/i, '').replace(/[:\s]/g, '').toLowerCase();
      const want = norm(ICONIC_SFTP.fingerprint);
      connectCfg.hostVerifier = (hashedKey) => norm(hashedKey) === want;
    } else {
      log.warn('[iconic/publish] connecting WITHOUT host-key verification — set ICONIC_SFTP_HOST_FINGERPRINT to harden.');
    }

    const sftp = new SftpClient();
    try {
      await sftp.connect(connectCfg);
      // .tmp then rename, so THE ICONIC never ingests a half-written file.
      await sftp.put(buffer, remoteTmp);
      try { if (await sftp.exists(remoteFinal)) await sftp.delete(remoteFinal); } catch (_) {}
      await sftp.rename(remoteTmp, remoteFinal);
      await sftp.end();
    } catch (e) {
      try { await sftp.end(); } catch (_) {}
      const msg = `SFTP push failed: ${e.message || e}`;
      log.error('[iconic/publish]', msg);
      logPush([filename, buffer.length, weekStart, remoteFinal, null, 'failed', msg, userId]);
      alert(filename, msg);
      return { ok: false, status: 502, error: msg };
    }

    // Durable archive to R2. Best effort: the push has already succeeded, and losing the
    // archive is not a reason to report the transfer as failed.
    let r2Key = null;
    try {
      const ts = new Date().toISOString().replace(/[:.]/g, '-');
      r2Key = `iconic-outbox/${weekStart || 'unknown-week'}/${ts}-${filename}`;
      await r2Client.send(new PutObjectCommand({
        Bucket: R2_BUCKET, Key: r2Key, Body: buffer, ContentType: 'text/csv',
      }));
    } catch (e) {
      log.error('[iconic/publish] R2 archive failed (push still succeeded):', e.message || e);
      r2Key = null;
    }

    logPush([filename, buffer.length, weekStart, remoteFinal, r2Key, 'success', null, userId]);
    log.log(`[iconic/publish] OK ${remoteFinal} (${buffer.length} bytes) by ${userId || 'job'}`);
    return { ok: true, remotePath: remoteFinal, bytes: buffer.length, r2Key };
  }

  return { publish, safeFilename };
};
