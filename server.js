// ---------- TradeLocker BrandSocket Relay (Accounts-only: Equity/HWM/DD/Balance) ----------
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { io } = require('socket.io-client');

// ---- Boot diagnostics ----
const BOOT_AT = new Date().toISOString();
const SERVER = process.env.TL_SERVER || 'https://api.tradelocker.com';
const TYPE = process.env.TL_ENV || 'LIVE';
const KEY = process.env.TL_BRAND_KEY;
const PORT = Number(process.env.PORT || 8080);
console.log('[BOOT]', { BOOT_AT, TYPE, SERVER, KEY_SET: !!KEY, PORT });

// ---- App setup ----
const app = express();
app.use(cors());
app.use(express.json());

// ---- Config defaults ----
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS || 1000);
const REFRESH_MS = Number(process.env.REFRESH_MS || 5000);
const STALE_MS = Number(process.env.STALE_MS || 120000);
const SELECT_TTL_MS = Number(process.env.SELECT_TTL_MS || 10 * 60 * 1000);
const READ_TOKEN = process.env.READ_TOKEN || '';

if (!KEY) {
  console.error('Missing TL_BRAND_KEY');
  process.exit(1);
}

// ---- State ----
const state = new Map();
const selections = new Map();
const ddSubs = new Set();
const evtSubs = new Set();
let lastBrandEventAt = 0;
let lastConnectError = '';
let initialSyncDone = false;

// ---- Helpers ----
const num = (x) => {
  const n = parseFloat(x);
  return Number.isFinite(n) ? n : 0;
};
const makeToken = () =>
  Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);

function parseAccountsParam(q) {
  if (Array.isArray(q)) q = q.join(',');
  const raw = String(q || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  return new Set(raw);
}
function getSelectionFromToken(selToken) {
  if (!selToken) return null;
  const rec = selections.get(String(selToken));
  if (!rec) return null;
  rec.expiresAt = Date.now() + SELECT_TTL_MS;
  return rec.set;
}
function checkToken(req, res) {
  if (!READ_TOKEN) return true;
  const provided = String(req.query.token || req.headers['x-read-token'] || '');
  if (provided === READ_TOKEN) return true;
  res.status(401).json({ ok: false, error: 'unauthorized' });
  return false;
}

setInterval(() => {
  const now = Date.now();
  for (const [t, v] of selections) if (v.expiresAt <= now) selections.delete(t);
}, 60000);

function parseBalance(m) {
  for (const k of ['balance', 'accountBalance', 'cash', 'cashBalance']) {
    const v = m?.[k];
    if (v !== undefined && v !== null && v !== '') {
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
  }
  return NaN;
}

// ---- Account handling ----
function updateAccount(m) {
  const id =
    m.accountId || m.account?.id || m.accId || String(m?.accountID || '');
  if (!id) return;

  const eq = num(m.equity ?? m.Equity);
  const cur = m.currency || m.Currency || 'USD';
  const now = Date.now();

  if (!state.has(id)) {
    state.set(id, {
      hwm: eq,
      equity: eq,
      maxDD: 0,
      currency: cur,
      updatedAt: now,
      balance: NaN,
    });
  }
  const s = state.get(id);
  const bal = parseBalance(m);
  if (Number.isFinite(bal)) s.balance = bal;

  if (eq > s.hwm) s.hwm = eq;
  s.equity = eq;
  s.currency = cur;
  s.updatedAt = now;

  const dd = s.hwm > 0 ? (s.hwm - s.equity) / s.hwm : 0;
  if (dd > s.maxDD) s.maxDD = dd;
  pushDD(s, id);
}

function buildDDPayload(id) {
  const s = state.get(id);
  if (!s) return null;
  const dd = s.hwm > 0 ? (s.hwm - s.equity) / s.hwm : 0;
  let instPct = null;
  if (Number.isFinite(s.balance) && s.balance > 0)
    instPct = ((s.equity - s.balance) / s.balance) * 100;

  return {
    type: 'account',
    accountId: id,
    equity: Number(s.equity.toFixed(2)),
    hwm: Number(s.hwm.toFixed(2)),
    dd,
    ddPct: Number((dd * 100).toFixed(2)),
    maxDDPct: Number((s.maxDD * 100).toFixed(2)),
    balance: Number.isFinite(s.balance)
      ? Number(s.balance.toFixed(2))
      : null,
    instPct: instPct !== null ? Number(instPct.toFixed(2)) : null,
    currency: s.currency,
    updatedAt: s.updatedAt,
    serverTime: Date.now(),
  };
}
function pushDD(_s, accountId) {
  const payload = buildDDPayload(accountId);
  if (!payload) return;
  for (const sub of ddSubs) {
    if (sub.filter.size && !sub.filter.has(accountId)) continue;
    sub.res.write(`data: ${JSON.stringify(payload)}\n\n`);
  }
}
function broadcastEvent(m) {
  const acct = m.accountId || m.account?.id || m.accId || '';
  const line = `data: ${JSON.stringify(m)}\n\n`;
  for (const sub of evtSubs) {
    if (sub.filter?.size && acct && !sub.filter.has(acct)) continue;
    sub.res.write(line);
  }
}

// ---- Brand Socket ----
const socket = io(SERVER + '/brand-socket', {
  path: '/brand-api/socket.io',
  transports: ['websocket'],
  query: { type: TYPE },
  extraHeaders: { 'brand-api-key': KEY },
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  randomizationFactor: 0.5,
  timeout: 20000,
  forceNew: true,
});

socket.on('connect', () => {
  console.log('[BrandSocket] connected', socket.id);
  lastConnectError = '';
});
socket.on('disconnect', (reason) =>
  console.warn('[BrandSocket] disconnected', reason)
);
socket.on('connect_error', (err) => {
  lastConnectError =
    (err && (err.message || String(err))) || 'connect_error';
  console.error('[BrandSocket] connect_error', lastConnectError);
});
socket.on('error', (e) => console.error('[BrandSocket] error', e));

socket.on('stream', (m) => {
  lastBrandEventAt = Date.now();
  const t = (m?.type || '').toString().toUpperCase();

  if (t === 'PROPERTY' && (m?.name || '').toUpperCase() === 'SYNCEND') {
    initialSyncDone = true;
    for (const sub of ddSubs)
      sub.res.write(`event: syncEnd\ndata: {"ok":true}\n\n`);
    return;
  }

  if (t === 'ACCOUNTSTATUS' || t === 'ACCOUNT' || t === 'ACCOUNT_UPDATE')
    updateAccount(m);

  broadcastEvent(m);
});

setInterval(() => {
  if (!STALE_MS) return;
  if (!socket.connected) {
    const age = Date.now() - (lastBrandEventAt || 0);
    if (age > STALE_MS) {
      console.warn(
        '[Watchdog] Disconnected & stale for',
        age,
        'ms. Exiting for restart.'
      );
      process.exit(1);
    }
  }
}, 30000);

// ---- Routes ----
app.get('/health', (_req, res) =>
  res.json({ ok: true, env: TYPE, knownAccounts: state.size })
);

app.get('/brand/status', (_req, res) =>
  res.json({
    env: TYPE,
    server: SERVER,
    connected: socket.connected,
    knownAccounts: state.size,
    lastBrandEventAt,
    lastConnectError,
    initialSyncDone,
    now: Date.now(),
  })
);

// runtime env check (guarded)
app.get('/_debug/env', (req, res) => {
  const tok = String(req.query.token || req.headers['x-read-token'] || '');
  if (READ_TOKEN && tok !== READ_TOKEN)
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  res.json({
    ok: true,
    TYPE,
    SERVER,
    KEY_SET: !!KEY,
    PORT: process.env.PORT || 8080,
    NODE_VERSION: process.version,
    PWD: process.cwd(),
  });
});

// ---- Error hardening ----
process.on('unhandledRejection', (r) =>
  console.error('[unhandledRejection]', r)
);
process.on('uncaughtException', (e) => {
  console.error('[uncaughtException]', e);
  process.exit(1);
});

// ---- Start ----
app.listen(PORT, () =>
  console.log(`BrandSocket relay listening on :${PORT}`)
);
