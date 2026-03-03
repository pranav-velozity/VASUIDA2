// middleware/validation.js — Input Validation & Sanitization

// Sanitize string inputs (prevent injection attacks)
function sanitizeString(str) {
  if (!str) return '';
  return String(str)
    .trim()
    .replace(/[<>]/g, '') // Remove potential HTML tags
    .slice(0, 500); // Limit length
}

// Validate PO number format
function isValidPO(po) {
  if (!po) return false;
  const sanitized = sanitizeString(po);
  // PO should be alphanumeric with hyphens/underscores, 3-50 chars
  return /^[A-Za-z0-9_-]{3,50}$/.test(sanitized);
}

// Validate SKU format
function isValidSKU(sku) {
  if (!sku) return false;
  const sanitized = sanitizeString(sku);
  return /^[A-Za-z0-9_-]{2,50}$/.test(sanitized);
}

// Validate UID format
function isValidUID(uid) {
  if (!uid) return false;
  const sanitized = sanitizeString(uid);
  return /^[A-Za-z0-9_-]{3,100}$/.test(sanitized);
}

// Validate date format (YYYY-MM-DD)
function isValidDate(date) {
  if (!date) return false;
  return /^\d{4}-\d{2}-\d{2}$/.test(date);
}

// Middleware: Validate records input
function validateRecordInput(req, res, next) {
  const { po_number, sku_code, uid } = req.body;

  const errors = [];

  if (po_number && !isValidPO(po_number)) {
    errors.push('Invalid PO number format');
  }

  if (sku_code && !isValidSKU(sku_code)) {
    errors.push('Invalid SKU code format');
  }

  if (uid && !isValidUID(uid)) {
    errors.push('Invalid UID format');
  }

  if (errors.length > 0) {
    return res.status(400).json({ error: 'Validation failed', details: errors });
  }

  // Sanitize inputs
  if (req.body.po_number) req.body.po_number = sanitizeString(req.body.po_number);
  if (req.body.sku_code) req.body.sku_code = sanitizeString(req.body.sku_code);
  if (req.body.uid) req.body.uid = sanitizeString(req.body.uid);
  if (req.body.mobile_bin) req.body.mobile_bin = sanitizeString(req.body.mobile_bin);
  if (req.body.sscc_label) req.body.sscc_label = sanitizeString(req.body.sscc_label);

  next();
}

// Middleware: Validate bulk operations
function validateBulkInput(req, res, next) {
  if (!Array.isArray(req.body)) {
    return res.status(400).json({ error: 'Request body must be an array' });
  }

  if (req.body.length > 1000) {
    return res.status(400).json({ error: 'Bulk operation limited to 1000 items' });
  }

  next();
}

module.exports = {
  sanitizeString,
  isValidPO,
  isValidSKU,
  isValidUID,
  isValidDate,
  validateRecordInput,
  validateBulkInput
};
