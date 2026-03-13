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
      'org:client_auth': 'client',
      'org:admin_auth': 'admin',
      'org:api_auth': 'api'
    };

    const simplifiedRole = roleMap[userRole];
    
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
  const userRole = req.auth?.orgRole;
  
  // Admin and API see everything
  if (userRole === 'org:admin_auth' || userRole === 'org:api_auth') {
    return data;
  }

  // For suppliers and clients, filter by their PO numbers
  if (userRole === 'org:supplier_auth' || userRole === 'org:client_auth') {
    const allowedPOs = await getOrgPONumbers(req.auth.orgId);

    // Helper: does this record belong to an allowed PO?
    const poAllowed = (r) => {
      const po = r.po_number ?? r.po ?? null;
      return po !== null && allowedPOs.includes(String(po));
    };

    if (allowedPOs.length === 0) {
      // No POs assigned — return empty for PO-keyed data; pass through
      // non-PO data (bins, week-level summaries) unchanged.
      if (Array.isArray(data)) {
        const hasPOField = data.some(r => 'po_number' in r || 'po' in r);
        return hasPOField ? [] : data;
      }
      if (data && typeof data === 'object' && ('po_number' in data)) return null;
      return data;
    }

    // Flat array — filter only if items have a PO field.
    // Arrays without po_number (e.g. bins rows keyed by mobile_bin) pass through
    // unfiltered so they are not silently wiped.
    if (Array.isArray(data)) {
      const hasPOField = data.length > 0 && data.some(r => 'po_number' in r || 'po' in r);
      if (!hasPOField) return data;
      return data.filter(poAllowed);
    }

    // Plain object handling
    if (data && typeof data === 'object') {
      // Single flat record with a po_number field
      if ('po_number' in data) {
        return poAllowed(data) ? data : null;
      }

      // Envelope object (e.g. { by_po: [...], by_day: [...], total_units: N })
      // Deep-filter any array values whose items contain po_number/po fields.
      const filtered = { ...data };
      for (const [key, val] of Object.entries(filtered)) {
        if (Array.isArray(val) && val.length > 0 &&
            ('po_number' in val[0] || 'po' in val[0])) {
          filtered[key] = val.filter(poAllowed);
        }
      }
      return filtered;
    }
  }

  return data;
}

// Middleware to auto-filter response data
function autoFilterResponse(req, res, next) {
  const originalJson = res.json.bind(res);
  
  res.json = async function(data) {
    // Only filter if user is supplier or client
    const userRole = req.auth?.orgRole;
    if (userRole === 'org:supplier_auth' || userRole === 'org:client_auth') {
      const filtered = await filterDataByRole(req, data);
      return originalJson(filtered);
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
