// TradeLocker BrandSocket Relay — resilient version
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { io } = require('socket.io-client');

const app = express();
app.use(cors());
app.use(express.json());

// -------- Config --------
const SERVER = process.env.TL_SERVER || 'https://api.tradelocker.com'; // LIVE default
const TYPE   = process.env.TL_ENV    || 'LIVE';                        // 'LIVE' | 'DEMO'
const KEY    = process.env.TL_BRAND_KEY || '';
const PORT   = Number(process.env.PORT || 8080);
const READ_TOKEN    = process.env.READ_TOKEN || '';
const HEARTBEAT_MS  = Number(process.env.HEARTBEAT_MS || 1000);
const REFRESH_MS    = Number(process.env.REFRESH_MS   || 5000);
// Disable stale-exit by default; use >0 to enable watchdog reconnects instead of process exit
const STALE_MS      = Number(process.env.STALE_MS     || 0);
const SELECT_TTL_MS = Number(process.env.SELECT_TTL_MS || 600000);

// -------- State --------
/** ACCOUNTS: id -> summary */
const ACCOUNTS = new Map();
/** POSITIONS: accountId -> Map<positionId, obj> */
const POSITIONS = new Map();
/** ORDERS: accountId -> Map<orderId, obj> */
const ORDERS = new Map();
/** QUOTES: instrument -> last quote */
const QUOTES = new Map();
/** selections: token -> { set:Set<string>, expiresAt:number } */
const selections = new Map();

// SSE subscribers
const subs = {
  accounts: new Set(),  // {res, filter:Set, ping, refresh}
  positions: new Set(),
  orders: new Set(),
  quotes: new Set(),
  events: new Set(),    // raw socket events
};

let lastBrandEventAt = 0;
let lastConnectError = '';
let initialSyncDone = false;

// -------- Utils --------
const num = (x)=>{ const n = parseFloat(x); return Number.isFinite(n) ? n : 0; };
const j = (o)=> `data: ${JSON.stringify(o)}\n\n`;
const makeToken = ()=> Math.random().toString(36).slice(2)+Math.random().toString(36).slice(2);
function ensureMap(m,k){ if(!m.has(k)) m.set(k,new Map()); return m.get(k); }
function parseSet(v){ if(Array.isArray(v)) v=v.join(','); return new Set(String(v||'').split(',').map(s=>s.trim()).filter(Boolean)); }
function parseBalance(m){ for(const k of ['balance','accountBalance','cash','cashBalance','Balance']){ const n=Number(m?.[k]); if(Number.isFinite(n)) return n; } return NaN; }
function sse(res){ res.writeHead(200, {'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','Access-Control-Allow-Origin':'*','X-Accel-Buffering':'no'}); }
function heartbeat(res){ return setInterval(()=>res.write(`: ping\n\n`), HEARTBEAT_MS); }
function tokenSet(tok){ if(!tok) return null; const r=selections.get(String(tok)); if(!r) return null; r.expiresAt=Date.now()+SELECT_TTL_MS; return r.set; }
function checkToken(req,res){ if(!READ_TOKEN) return true; const p=String(req.query.token||req.headers['x-read-token']||''); if(p===READ_TOKEN) return true; res.status(401).json({ok:false,error:'unauthorized'}); return false; }
setInterval(()=>{ const now=Date.now(); for(const [t,v] of selections) if(v.expiresAt<=now) selections.delete(t); }, 60_000);

// -------- Builders --------
function buildAccountPayload(id){
  const s=ACCOUNTS.get(id); if(!s) return null;
  const dd = s.hwm>0 ? (s.hwm - s.equity)/s.hwm : 0;
  let instPct=null; if(Number.isFinite(s.balance)&&s.balance>0) instPct=((s.equity-s.balance)/s.balance)*100;
  return {
    type:'account', accountId:id,
    equity:+s.equity.toFixed(2), hwm:+s.hwm.toFixed(2),
    dd, ddPct:+(dd*100).toFixed(2), maxDDPct:+((s.maxDD||0)*100).toFixed(2),
    balance:Number.isFinite(s.balance)?+s.balance.toFixed(2):null,
    instPct:instPct!==null?+instPct.toFixed(2):null,
    currency:s.currency, marginAvailable:s.marginAvailable??null, marginUsed:s.marginUsed??null,
    credit:s.credit??null, blockedBalance:s.blockedBalance??null,
    updatedAt:s.updatedAt, serverTime:Date.now()
  };
}

// -------- Updaters --------
function updateAccount(m){
  const id = m.accountId || m.account?.id || m.accId || String(m?.accountID||''); if(!id) return;
  const now=Date.now(); const cur=m.currency||m.Currency||'USD'; const eq=num(m.equity??m.Equity);
  const bal=parseBalance(m);

  let s=ACCOUNTS.get(id);
  if(!s) s={ hwm:eq, equity:eq, maxDD:0, currency:cur, balance:NaN, updatedAt:now };
  s.currency=cur; s.equity=eq; if(Number.isFinite(bal)) s.balance=bal;
  const marginAvailable=num(m.marginAvailable); if(Number.isFinite(marginAvailable)) s.marginAvailable=marginAvailable;
  const marginUsed=num(m.marginUsed); if(Number.isFinite(marginUsed)) s.marginUsed=marginUsed;
  const credit=num(m.credit); if(Number.isFinite(credit)) s.credit=credit;
  const blocked=num(m.blockedBalance); if(Number.isFinite(blocked)) s.blockedBalance=blocked;

  if(eq>(s.hwm||0)) s.hwm=eq;
  const dd=s.hwm>0?(s.hwm-s.equity)/s.hwm:0; if(dd>(s.maxDD||0)) s.maxDD=dd;
  s.updatedAt=now; ACCOUNTS.set(id,s);

  for(const sub of subs.accounts){ if(sub.filter.size && !sub.filter.has(id)) continue; sub.res.write(j(buildAccountPayload(id))); }
}
function upsertPosition(m){
  const accountId=m.accountId||m.account?.id; const id=m.positionId||m.id; if(!accountId||!id) return;
  const bucket=ensureMap(POSITIONS, accountId); bucket.set(String(id), m);
  for(const sub of subs.positions){ if(sub.filter.size && !sub.filter.has(accountId)) continue; sub.res.write(j({type:'position', accountId, position:m})); }
}
function closePosition(m){
  const accountId=m.accountId||m.account?.id; const id=m.positionId||m.id; const bucket=ensureMap(POSITIONS, accountId); bucket.delete(String(id));
  for(const sub of subs.positions){ if(sub.filter.size && !sub.filter.has(accountId)) continue; sub.res.write(j({type:'positionClosed', accountId, positionId:id, payload:m})); }
}
function upsertOrder(m){
  const accountId=m.accountId||m.account?.id; const id=m.orderId||m.id; if(!accountId||!id) return;
  const bucket=ensureMap(ORDERS, accountId); bucket.set(String(id), m);
  for(const sub of subs.orders){ if(sub.filter.size && !sub.filter.has(accountId)) continue; sub.res.write(j({type:'order', accountId, order:m})); }
}
function removeOrder(m){
  const accountId=m.accountId||m.account?.id; const id=m.orderId||m.id; const bucket=ensureMap(ORDERS, accountId); bucket.delete(String(id));
  for(const sub of subs.orders){ if(sub.filter.size && !sub.filter.has(accountId)) continue; sub.res.write(j({type:'orderRemoved', accountId, orderId:id, payload:m})); }
}
function upsertQuote(q){
  const instrument=q.instrument||q.symbol; if(!instrument) return;
  QUOTES.set(instrument, q);
  for(const sub of subs.quotes){ if(sub.filter.size && !sub.filter.has(instrument)) continue; sub.res.write(j({type:'quote', instrument, quote:q})); }
}

// -------- Socket setup --------
function createNoopSocket(){
  return {
    connected:false,
    on:()=>{},
    emit:()=>{},
    disconnect:()=>{},
    connect:()=>{}
  };
}

const socket = KEY
  ? io(SERVER + '/brand-socket', {
      path: '/brand-api/socket.io',
      transports: ['websocket'],
      query: { type: TYPE },                     // LIVE or DEMO
      extraHeaders: { 'brand-api-key': KEY },    // required header
      reconnection: true, reconnectionAttempts: Infinity,
      reconnectionDelay: 1000, reconnectionDelayMax: 10000, randomizationFactor: 0.5,
      timeout: 20000, forceNew: true,
    })
  : createNoopSocket();

if (!KEY) {
  console.error('[Startup] Missing TL_BRAND_KEY; running without upstream. Set TL_BRAND_KEY to enable streaming.');
}

// Connection + error channel
socket.on && socket.on('connect', ()=>{
  lastConnectError='';
  console.log('[BrandSocket] connected');
  if (!lastBrandEventAt) lastBrandEventAt = Date.now();
});
socket.on && socket.on('disconnect', (r)=> console.warn('[BrandSocket] disconnected', r));
socket.on && socket.on('connect_error', (e)=>{
  lastConnectError=(e?.message||String(e)||'connect_error');
  console.error('[BrandSocket] connect_error', lastConnectError);
  // gentle backoff
  try { setTimeout(()=>socket.connect && socket.connect(), 5000); } catch {}
});
socket.on && socket.on('error', (e)=> console.error('[BrandSocket] error', e));

// Main stream
socket.on && socket.on('stream', (m)=>{
  lastBrandEventAt=Date.now();
  const t=String(m?.type||'').toUpperCase();

  if(t==='PROPERTY' && String(m?.name||'').toUpperCase()==='SYNCEND'){
    initialSyncDone=true;
    for(const set of Object.values(subs)) for(const s of set) s.res.write(`event: syncEnd\n${j({ok:true})}`);
    return;
  }
  if(t==='ACCOUNTSTATUS' || t==='ACCOUNT' || t==='ACCOUNT_UPDATE') updateAccount(m);
  if(t==='POSITION' || t==='POSITION_UPDATE') upsertPosition(m);
  if(t==='CLOSEPOSITION' || t==='POSITION_CLOSED') closePosition(m);
  if(t==='OPENORDER' || t==='ORDER' || t==='ORDER_UPDATE') upsertOrder(m);
  if(t==='ORDER_REMOVED' || t==='ORDER_CANCELED' || t==='ORDER_CANCELLED') removeOrder(m);

  for(const s of subs.events) s.res.write(j(m));
});

// Quotes stream
socket.on && socket.on('subscriptions', (payload)=>{ if(payload && (payload.instrument||payload.symbol)) upsertQuote(payload); });

// -------- Watchdog (reconnect, no exits) --------
setInterval(()=>{
  if(!STALE_MS) return;
  const age=Date.now()-(lastBrandEventAt||0);
  if(age>STALE_MS){
    console.warn('[Watchdog] stale', age, 'ms; forcing reconnect');
    try { socket.disconnect && socket.disconnect(); } catch {}
    try { socket.connect && socket.connect(); } catch {}
    lastBrandEventAt = Date.now();
  }
}, 30_000);

// -------- Routes: selections --------
app.post('/select/accounts', (req,res)=>{
  if(!checkToken(req,res)) return;
  const arr=Array.isArray(req.body?.accounts)?req.body.accounts:[];
  const set=new Set(arr.map(String).map(s=>s.trim()).filter(Boolean));
  if(!set.size) return res.status(400).json({ok:false,error:'no accounts'});
  const token=makeToken(); selections.set(token,{set,expiresAt:Date.now()+SELECT_TTL_MS});
  res.json({ok:true, token, count:set.size, ttlMs:SELECT_TTL_MS});
});
app.post('/select/instruments', (req,res)=>{
  if(!checkToken(req,res)) return;
  const arr=Array.isArray(req.body?.instruments)?req.body.instruments:[];
  const set=new Set(arr.map(String).map(s=>s.trim()).filter(Boolean));
  if(!set.size) return res.status(400).json({ok:false,error:'no instruments'});
  const token=makeToken(); selections.set(token,{set,expiresAt:Date.now()+SELECT_TTL_MS});
  res.json({ok:true, token, count:set.size, ttlMs:SELECT_TTL_MS});
});

// -------- Routes: quotes subscribe/unsubscribe --------
app.post('/quotes/subscribe', (req,res)=>{
  if(!checkToken(req,res)) return;
  const action=String(req.body?.action||'').toUpperCase();
  const instrument=String(req.body?.instrument||'').trim();
  if(!instrument || !['SUBSCRIBE','UNSUBSCRIBE'].includes(action)) return res.status(400).json({ok:false,error:'bad action or instrument'});
  socket.emit && socket.emit('subscriptions.publish', { action, instrument }); // upstream
  res.json({ ok:true, action, instrument });
});

// -------- SSE helpers --------
function openSSE(res, hello){ sse(res); res.write(`event: hello\n${j(hello)}`); return heartbeat(res); }

// Accounts SSE
app.get('/stream/accounts', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const ping=openSSE(res, {ok:true, env:TYPE, initialSyncDone});
  const sub={res, filter, ping, refresh:null}; subs.accounts.add(sub);
  const ids=filter.size?Array.from(filter):Array.from(ACCOUNTS.keys());
  for(const id of ids){ const p=buildAccountPayload(id); if(p) res.write(j(p)); }
  if(REFRESH_MS>0) sub.refresh=setInterval(()=>{
    const out=[]; const list=filter.size?Array.from(filter):Array.from(ACCOUNTS.keys());
    for(const id of list){ const p=buildAccountPayload(id); if(p) out.push(p); }
    res.write(`event: batch\n${j({serverTime:Date.now(), accounts:out})}`);
  }, REFRESH_MS);
  req.on('close', ()=>{ clearInterval(sub.ping); if(sub.refresh) clearInterval(sub.refresh); subs.accounts.delete(sub); });
});

// Positions SSE
app.get('/stream/positions', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const ping=openSSE(res, {ok:true, env:TYPE, initialSyncDone});
  const sub={res, filter, ping}; subs.positions.add(sub);
  const ids=filter.size?Array.from(filter):Array.from(POSITIONS.keys());
  for(const acc of ids){ const bucket=POSITIONS.get(acc)||new Map(); for(const p of bucket.values()) res.write(j({type:'position', accountId:acc, position:p})); }
  req.on('close', ()=>{ clearInterval(sub.ping); subs.positions.delete(sub); });
});

// Orders SSE
app.get('/stream/orders', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const ping=openSSE(res, {ok:true, env:TYPE, initialSyncDone});
  const sub={res, filter, ping}; subs.orders.add(sub);
  const ids=filter.size?Array.from(filter):Array.from(ORDERS.keys());
  for(const acc of ids){ const bucket=ORDERS.get(acc)||new Map(); for(const o of bucket.values()) res.write(j({type:'order', accountId:acc, order:o})); }
  req.on('close', ()=>{ clearInterval(sub.ping); subs.orders.delete(sub); });
});

// Quotes SSE
app.get('/stream/quotes', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.instruments||req.query['instruments[]']);
  const ping=openSSE(res, {ok:true, env:TYPE, initialSyncDone});
  const sub={res, filter, ping}; subs.quotes.add(sub);
  const keys=filter.size?Array.from(filter):Array.from(QUOTES.keys());
  for(const k of keys){ const q=QUOTES.get(k); if(q) res.write(j({type:'quote', instrument:k, quote:q})); }
  req.on('close', ()=>{ clearInterval(sub.ping); subs.quotes.delete(sub); });
});

// Raw events SSE
app.get('/stream/events', (req,res)=>{
  if(!checkToken(req,res)) return;
  const ping=openSSE(res, {ok:true, env:TYPE, initialSyncDone});
  const sub={res, filter:new Set(), ping}; subs.events.add(sub);
  req.on('close', ()=>{ clearInterval(sub.ping); subs.events.delete(sub); });
});

// -------- Snapshots --------
app.get('/state/accounts', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const out=[]; for(const [id] of ACCOUNTS){ if(filter.size && !filter.has(id)) continue; const p=buildAccountPayload(id); if(p) out.push(p); }
  out.sort((a,b)=> b.equity - a.equity);
  res.json({ env:TYPE, count:out.length, accounts:out, initialSyncDone });
});
app.get('/state/positions', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const out={}; for(const [acc,bucket] of POSITIONS){ if(filter.size && !filter.has(acc)) continue; out[acc]=Array.from(bucket.values()); }
  res.json({ env:TYPE, positions:out, initialSyncDone });
});
app.get('/state/orders', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.accounts||req.query['accounts[]']);
  const out={}; for(const [acc,bucket] of ORDERS){ if(filter.size && !filter.has(acc)) continue; out[acc]=Array.from(bucket.values()); }
  res.json({ env:TYPE, orders:out, initialSyncDone });
});
app.get('/state/quotes', (req,res)=>{
  if(!checkToken(req,res)) return;
  const sel=tokenSet(req.query.sel); const filter= sel || parseSet(req.query.instruments||req.query['instruments[]']);
  const out={}; for(const [inst,q] of QUOTES){ if(filter.size && !filter.has(inst)) continue; out[inst]=q; }
  res.json({ env:TYPE, quotes:out, initialSyncDone });
});

// -------- Health & status --------
app.get('/health', (_req,res)=> res.json({ ok:true, env:TYPE, connected: !!socket.connected, accounts: ACCOUNTS.size }));
app.get('/brand/status', (_req,res)=> res.json({
  env: TYPE, server: SERVER, connected: !!socket.connected,
  knownAccounts: ACCOUNTS.size, lastBrandEventAt, lastConnectError, initialSyncDone, now: Date.now(),
  keySet: !!KEY
}));

// Debug env
app.get('/_debug/env', (req,res)=>{
  const tok=String(req.query.token||req.headers['x-read-token']||'');
  if(READ_TOKEN && tok!==READ_TOKEN) return res.status(401).json({ok:false,error:'unauthorized'});
  res.json({ ok:true, TYPE, SERVER, KEY_SET: !!KEY, PORT: process.env.PORT, NODE_VERSION: process.version });
});

// -------- Hardening --------
process.on('unhandledRejection', (r)=> console.error('[unhandledRejection]', r));
// Do not exit; log and keep the service alive
process.on('uncaughtException', (e)=>{ console.error('[uncaughtException]', e); });

// -------- Start --------
app.listen(PORT, ()=> console.log(`BrandSocket Relay listening on :${PORT}`));
