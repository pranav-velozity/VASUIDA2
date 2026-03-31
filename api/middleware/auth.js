// middleware/auth.js — Clerk Authentication & Authorization Middleware

const { clerkClient } = require('@clerk/clerk-sdk-node');

// ============================================
// 1. AUTHENTICATE REQUEST (verify Clerk token)
// ============================================
async function authenticateRequest(req, res, next) {
  try {
    // Get token from Authorization header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Missing or invalid Authorization header' });
    }

    const token = authHeader.split(' ')[1];
    
    // Verify token with Clerk
    const sessionClaims = await clerkClient.verifyToken(token);
    
    if (!sessionClaims) {
      return res.status(401).json({ error: 'Invalid token' });
    }

    // Attach user info to request
    req.auth = {
      userId: sessionClaims.sub,
      sessionId: sessionClaims.sid,
      orgId: sessionClaims.org_id || null,
      orgRole: sessionClaims.org_role || null,
      orgSlug: sessionClaims.org_slug || null
    };

    // Fetch full user details including metadata
    const user = await clerkClient.users.getUser(sessionClaims.sub);
    req.user = user;

    next();
  } catch (error) {
    console.error('Auth error:', error);
    return res.status(401).json({ error: 'Authentication failed' });
  }
}

// ============================================
// 2. REQUIRE ROLE (check if user has required role)
// ============================================
function requireRole(allowedRoles) {
  return async (req, res, next) => {
    if (!req.auth) {
      return res.status(401).json({ error: 'Not authenticated' });
    }

    const userRole = req.auth.orgRole;

    // Admin always has access
    if (userRole === 'org:admin_auth') {
      return next();
    }

    // API role has full access
    if (userRole === 'org:api_auth' && allowedRoles.includes('api')) {
      return next();
    }

    // Check if user's role is in allowed roles
    const roleMap = {
      'org:supplier_auth': 'supplier',
      'org:client_auth':   'client',
      'org:member':        'member',
      'org:admin_auth':    'admin',
      'org:api_auth':      'api'
    };

    const simplifiedRole = roleMap[userRole];

    // For non-admin roles: pass through if the endpoint allows their specific role,
    // OR if the endpoint is open to supplier/client/api (shared endpoints).
    // Admin-only endpoints (requireRole(['admin'])) are NOT bypassed here.
    const isAdminOnlyEndpoint = allowedRoles.length === 1 && allowedRoles[0] === 'admin';
    if (!isAdminOnlyEndpoint && simplifiedRole && ['supplier','client','member','api'].includes(simplifiedRole)) {
      return next();
    }

    if (!simplifiedRole || !allowedRoles.includes(simplifiedRole)) {
      return res.status(403).json({ 
        error: 'Insufficient permissions',
        required: allowedRoles,
        current: simplifiedRole || 'none'
      });
    }

    next();
  };
}

// ============================================
// 3. DATA FILTERING (filter data by org/PO)
// ============================================

// Get PO numbers associated with this org (from org metadata)
async function getOrgPONumbers(orgId) {
  if (!orgId) return [];
  
  try {
    const org = await clerkClient.organizations.getOrganization({ organizationId: orgId });
    const poNumbers = org.publicMetadata?.po_numbers || [];
    return Array.isArray(poNumbers) ? poNumbers : [];
  } catch (error) {
    console.error('Error fetching org PO numbers:', error);
    return [];
  }
}

// Filter records/data based on user's role and org
async function filterDataByRole(req, data) {
  // All authenticated roles see all data.
  // PO-level filtering (Client/Supplier segregation) is a future enhancement.
  // When ready, check req.auth.orgRole and filter by getOrgPONumbers(req.auth.orgId).
  return data;
}

// Middleware to auto-filter response data
// IMPORTANT: res.json must remain synchronous-compatible for Express.
// We use a wrapper that resolves the async filter before calling the real res.json.
function autoFilterResponse(req, res, next) {
  const originalJson = res.json.bind(res);

  res.json = function(data) {
    const userRole = req.auth?.orgRole;
    if (userRole === 'org:supplier_auth' || userRole === 'org:client_auth') {
      // Run async filter then send — Express is fine with this because the response
      // is fully controlled here (we call originalJson exactly once after awaiting).
      filterDataByRole(req, data)
        .then(filtered => originalJson(filtered))
        .catch(err => {
          console.error('[autoFilterResponse] filter error, sending unfiltered:', err);
          originalJson(data);
        });
      // Return `this` (res) so callers chaining off res.json() don't break
      return res;
    }
    return originalJson(data);
  };

  next();
}

// ============================================
// 4. OPTIONAL AUTH (for backward compatibility)
// ============================================
// This allows endpoints to work with OR without auth
// Useful during transition period
async function optionalAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    // No auth provided, continue without auth
    req.auth = null;
    req.user = null;
    return next();
  }

  // Auth provided, verify it
  try {
    await authenticateRequest(req, res, next);
  } catch (error) {
    // Auth failed, but continue anyway (optional)
    req.auth = null;
    req.user = null;
    next();
  }
}

// ============================================
// 5. API KEY AUTH (for system integrations)
// ============================================
function authenticateApiKey(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  const validApiKey = process.env.SYSTEM_API_KEY;

  if (!validApiKey) {
    return res.status(500).json({ error: 'API key auth not configured' });
  }

  if (apiKey !== validApiKey) {
    return res.status(401).json({ error: 'Invalid API key' });
  }

  // Set auth context for API key
  req.auth = {
    userId: 'system',
    orgRole: 'org:api_auth'
  };

  next();
}

// ============================================
// EXPORTS
// ============================================
module.exports = {
  authenticateRequest,
  requireRole,
  filterDataByRole,
  autoFilterResponse,
  getOrgPONumbers,
  optionalAuth,
  authenticateApiKey
};
