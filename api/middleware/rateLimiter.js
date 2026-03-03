// middleware/rateLimiter.js — Rate Limiting Protection

const rateLimit = require('express-rate-limit');

// General API rate limiter (100 requests per 15 minutes per IP)
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100,
  message: { error: 'Too many requests, please try again later' },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    // Skip rate limiting for admin users
    return req.auth?.orgRole === 'org:admin_auth';
  }
});

// Strict rate limiter for write operations (20 requests per 15 minutes)
const writeOpLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { error: 'Too many write operations, please slow down' },
  skip: (req) => {
    // Skip for admin and API roles
    const role = req.auth?.orgRole;
    return role === 'org:admin_auth' || role === 'org:api_auth';
  }
});

// Upload rate limiter (5 uploads per hour)
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 5,
  message: { error: 'Upload limit exceeded, try again later' }
});

module.exports = {
  apiLimiter,
  writeOpLimiter,
  uploadLimiter
};
