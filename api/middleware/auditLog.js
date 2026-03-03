// middleware/auditLog.js — Audit Logging for Security & Compliance
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

// Initialize audit log database
const DB_DIR = process.env.DB_DIR || path.join(__dirname, '../data');
fs.mkdirSync(DB_DIR, { recursive: true }); // ← ADD THIS LINE
const AUDIT_DB = path.join(DB_DIR, 'audit_log.sqlite');

const auditDb = new Database(AUDIT_DB);

auditDb.pragma('journal_mode = WAL');

// Create audit log table
auditDb.exec(`
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp TEXT NOT NULL DEFAULT (datetime('now')),
  user_id TEXT,
  org_id TEXT,
  org_role TEXT,
  action TEXT NOT NULL,
  resource TEXT NOT NULL,
  method TEXT NOT NULL,
  ip_address TEXT,
  user_agent TEXT,
  request_body TEXT,
  response_status INTEGER,
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_org ON audit_log(org_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
`);

const insertLog = auditDb.prepare(`
  INSERT INTO audit_log (
    user_id, org_id, org_role, action, resource, method,
    ip_address, user_agent, request_body, response_status, error
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// Audit logging middleware
function auditLog(action) {
  return (req, res, next) => {
    const originalJson = res.json.bind(res);
    const originalSend = res.send.bind(res);

    // Capture response
    res.json = function(data) {
      logAudit(req, res, action, null);
      return originalJson(data);
    };

    res.send = function(data) {
      logAudit(req, res, action, null);
      return originalSend(data);
    };

    // Capture errors
    res.on('finish', () => {
      if (res.statusCode >= 400) {
        logAudit(req, res, action, `HTTP ${res.statusCode}`);
      }
    });

    next();
  };
}

function logAudit(req, res, action, error) {
  try {
    const userId = req.auth?.userId || 'anonymous';
    const orgId = req.auth?.orgId || null;
    const orgRole = req.auth?.orgRole || null;
    const resource = req.path;
    const method = req.method;
    const ipAddress = req.ip || req.headers['x-forwarded-for'] || 'unknown';
    const userAgent = req.headers['user-agent'] || 'unknown';
    
    // Sanitize request body (remove sensitive data)
    let requestBody = null;
    if (req.body && Object.keys(req.body).length > 0) {
      const sanitized = { ...req.body };
      // Remove sensitive fields
      delete sanitized.password;
      delete sanitized.token;
      delete sanitized.api_key;
      requestBody = JSON.stringify(sanitized).slice(0, 5000); // Limit size
    }

    const responseStatus = res.statusCode;

    insertLog.run(
      userId,
      orgId,
      orgRole,
      action,
      resource,
      method,
      ipAddress,
      userAgent,
      requestBody,
      responseStatus,
      error
    );
  } catch (err) {
    console.error('Failed to write audit log:', err);
  }
}

// Query audit logs (for admin use)
function getAuditLogs(filters = {}) {
  let query = 'SELECT * FROM audit_log WHERE 1=1';
  const params = [];

  if (filters.userId) {
    query += ' AND user_id = ?';
    params.push(filters.userId);
  }

  if (filters.orgId) {
    query += ' AND org_id = ?';
    params.push(filters.orgId);
  }

  if (filters.action) {
    query += ' AND action = ?';
    params.push(filters.action);
  }

  if (filters.startDate) {
    query += ' AND timestamp >= ?';
    params.push(filters.startDate);
  }

  if (filters.endDate) {
    query += ' AND timestamp <= ?';
    params.push(filters.endDate);
  }

  query += ' ORDER BY timestamp DESC LIMIT ?';
  params.push(filters.limit || 100);

  return auditDb.prepare(query).all(...params);
}

module.exports = {
  auditLog,
  getAuditLogs,
  logAudit
};
