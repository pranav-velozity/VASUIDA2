// server.js — VelOzity UID Ops Backend (Express + SQLite + SSE)


const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const ExcelJS = require('exceljs');
const Database = require('better-sqlite3');
const { randomUUID } = require('crypto');


// ---- Config ----
const PORT = process.env.PORT || 4000;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*'; // set to your Netlify domain in prod
const DB_DIR = process.env.DB_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DB_DIR, { recursive: true });
const DB_FILE = process.env.DB_FILE || path.join(DB_DIR, 'uid_ops.sqlite');


// ---- App ----
const app = express();


// --- Global CORS ---
const allowList = (ALLOWED_ORIGIN || '*')
.split(',')
.map(s => s.trim())
.filter(Boolean);


const corsOptions = {
origin: (origin, cb) => {
if (!origin) return cb(null, true); // same-origin/curl
if (allowList.includes('*') || allowList.includes(origin)) return cb(null, true);
if (allowList.some(a => a.endsWith('.netlify.app') && origin.endsWith('.netlify.app'))) return cb(null, true);
return cb(new Error('Not allowed by CORS: ' + origin));
},
methods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'],
allowedHeaders: ['Content-Type','Authorization']
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions));
app.use(express.json({ limit: '10mb' }));


// --- Time helpers (America/Chicago) ---
function chicagoISOFromDate(d = new Date()) {
try {
const fmt = new Intl.DateTimeFormat('en-US', {
timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit'
});
const parts = fmt.formatToParts(d);
const y = parts.find(p => p.type === 'year')?.value;
const m = parts.find(p => p.type === 'month')?.value;
const dd = parts.find(p => p.type === 'day')?.value;
if (y && m && dd) return `${y}-${m}-${dd}`;
} catch {}
return d.toISOString().slice(0, 10);
}
const todayChicagoISO = () => chicagoISOFromDate(new Date());


function toISODate(v) {
if (v == null) return '';
if (typeof v === 'number' && isFinite(v)) { // Excel serial
const base = new Date(Date.UTC(1899, 11, 30));
const ms = Math.round(v * 86400000);
const d = new Date(base.getTime() + ms);
return d.toISOString().slice(0, 10);
}
const s = String(v).trim();
if (!s) return '';
if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
});