// middleware/rateLimiter.js — Rate Limiting Protection

const rateLimit = require('express-rate-limit');

// General API rate limiter
// Raised to 500/15min — the app fires ~8-10 requests on load per page navigation
// Admin and API roles are always skipped
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 500,
  message: { error: 'Too many requests, please try again later' },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    // Skip for admin and API roles — checked after auth middleware runs
    const role = req.auth?.orgRole;
    return role === 'org:admin_auth' || role === 'org:api_auth';
  }
});

// Strict rate limiter for write operations (50 requests per 15 minutes)
const writeOpLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 50,
  message: { error: 'Too many write operations, please slow down' },
  skip: (req) => {
    const role = req.auth?.orgRole;
    return role === 'org:admin_auth' || role === 'org:api_auth';
  }
});

// Upload rate limiter (10 uploads per hour)
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 10,
  message: { error: 'Upload limit exceeded, try again later' },
  skip: (req) => {
    const role = req.auth?.orgRole;
    return role === 'org:admin_auth' || role === 'org:api_auth';
  }
});

module.exports = {
  apiLimiter,
  writeOpLimiter,
  uploadLimiter
};
