// TRiX Turbo Relay — persistent WebSocket relay for TRiX Turbo Browser.
// Pure event-driven routing. No polling loops for messages.
// One port serves:  GET /health  (JSON)  and  WebSocket at /ws.
//
// Run:  npm install && node server.js   (PORT env, default 8080)
// Deploy: see DEPLOY.md (Fly.io recommended; Railway alternative).
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const SESSION_TTL_MS = Number(process.env.SESSION_TTL_MS || 60 * 60 * 1000); // sessions kept 60 min after last disconnect
const QUEUE_MAX = 20;                    // max queued commands per session while phone offline
const QUEUE_TTL_MS = Number(process.env.QUEUE_TTL_MS || 60 * 1000); // queued command TTL: 60s
const COMMAND_TIMEOUT_MS = Number(process.env.COMMAND_TIMEOUT_MS || 30 * 1000); // pending command timeout: 30s
const RATE_LIMIT_PER_SEC = 10;           // max commands/sec per session (Muse -> phone)
const PING_INTERVAL_MS = 30 * 1000;      // ws keepalive ping
const CODE_RE = /^[A-Z0-9]{6}$/i;

const startedAt = Date.now();

// session code -> { code, phone: ws|null, muse: Set<ws>, queue: [], lastActivity, expiryTimer }
const sessions = new Map();
// `${session}:${id}` -> { sender: ws, timer: Timeout }
const pending = new Map();
// ws -> { role, session, code }
const connMeta = new WeakMap();

function now() { return Date.now(); }

function getOrCreateSession(code) {
  let s = sessions.get(code);
  if (!s) {
    s = { code, phone: null, muse: new Set(), queue: [], lastActivity: now(), expiryTimer: null };
    sessions.set(code, s);
  }
  s.lastActivity = now();
  return s;
}

// Schedule session expiry 60 min after it becomes fully empty. Cancelled on any re-join.
function armExpiry(s) {
  clearExpiry(s);
  s.expiryTimer = setTimeout(() => {
    if (!s.phone && s.muse.size === 0) {
      // Fail any still-queued commands before deleting.
      for (const q of s.queue) {
        clearTimeout(q.ttlTimer);
        safeSend(q.sender, { id: q.msg.id, error: 'phone_offline' });
      }
      s.queue = [];
      sessions.delete(s.code);
    }
  }, SESSION_TTL_MS);
  if (s.expiryTimer.unref) s.expiryTimer.unref();
}
function clearExpiry(s) {
  if (s.expiryTimer) { clearTimeout(s.expiryTimer); s.expiryTimer = null; }
}

function safeSend(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try { ws.send(JSON.stringify(obj)); return true; } catch { return false; }
}

// ---- Rate limit: sliding window of command timestamps per session ----
const rateWindows = new Map(); // code -> [timestamps]
function rateLimited(code) {
  const t = now();
  let w = rateWindows.get(code);
  if (!w) { w = []; rateWindows.set(code, w); }
  while (w.length && w[0] <= t - 1000) w.shift();
  if (w.length >= RATE_LIMIT_PER_SEC) return true;
  w.push(t);
  return false;
}

// ---- Queueing (phone offline) ----
function enqueue(s, msg, sender) {
  if (s.queue.length >= QUEUE_MAX) {
    safeSend(sender, { id: msg.id, error: 'queue_full' });
    return;
  }
  const entry = { msg, sender, enqueuedAt: now(), ttlTimer: null };
  entry.ttlTimer = setTimeout(() => {
    const i = s.queue.indexOf(entry);
    if (i !== -1) s.queue.splice(i, 1);
    safeSend(sender, { id: msg.id, error: 'phone_offline' });
    pending.delete(`${s.code}:${msg.id}`);
  }, QUEUE_TTL_MS);
  if (entry.ttlTimer.unref) entry.ttlTimer.unref();
  s.queue.push(entry);
}

function flushQueue(s) {
  if (!s.phone || s.phone.readyState !== WebSocket.OPEN) return;
  const queued = s.queue.splice(0, s.queue.length);
  for (const q of queued) {
    clearTimeout(q.ttlTimer);
    s.phone.send(JSON.stringify(q.msg));
  }
}

// ---- Pending command tracking (Muse -> phone -> result) ----
function trackPending(s, msg, sender) {
  if (msg.id === undefined || msg.id === null) return;
  const key = `${s.code}:${msg.id}`;
  const old = pending.get(key);
  if (old) clearTimeout(old.timer);
  const timer = setTimeout(() => {
    pending.delete(key);
    safeSend(sender, { id: msg.id, error: 'timeout' });
  }, COMMAND_TIMEOUT_MS);
  if (timer.unref) timer.unref();
  pending.set(key, { sender, timer });
}

function resolvePending(s, msg) {
  if (msg.id === undefined || msg.id === null) return false;
  const key = `${s.code}:${msg.id}`;
  const p = pending.get(key);
  if (!p) return false;
  clearTimeout(p.timer);
  pending.delete(key);
  safeSend(p.sender, msg);
  return true;
}

// ---- HTTP server: /health + upgrade ----
const server = http.createServer((req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (req.method === 'GET' && url.pathname === '/health') {
    let phones = 0, museClients = 0;
    for (const s of sessions.values()) {
      if (s.phone) phones++;
      museClients += s.muse.size;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      service: 'trix-turbo-relay',
      version: '1.0.0',
      sessions: sessions.size,
      phones_connected: phones,
      muse_clients: museClients,
      uptime_s: Math.floor((Date.now() - startedAt) / 1000),
    }));
    return;
  }
  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ service: 'trix-turbo-relay', version: '1.0.0', ws: '/ws', health: '/health' }));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not_found' }));
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  // Keepalive: ping every 30s; terminate dead peers. (Timer per connection, not a message poll.)
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const pingTimer = setInterval(() => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* closed */ }
  }, PING_INTERVAL_MS);
  if (pingTimer.unref) pingTimer.unref();

  ws.on('message', (data, isBinary) => {
    // ---- Binary frame: screenshot from phone -> forward as-is to Muse clients ----
    if (isBinary) {
      const meta = connMeta.get(ws);
      if (!meta || meta.role !== 'phone' || !meta.session) return; // only phones send binary
      const s = sessions.get(meta.session);
      if (!s) return;
      for (const m of s.muse) {
        if (m.readyState === WebSocket.OPEN) { try { m.send(data); } catch { /* ignore */ } }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString('utf8')); } catch {
      safeSend(ws, { error: 'bad_json' });
      return;
    }
    if (!msg || typeof msg !== 'object') { safeSend(ws, { error: 'bad_message' }); return; }

    // ---- Registration / join (first message on a connection) ----
    if (msg.type === 'register' || msg.type === 'join') {
      const code = String(msg.session || '').toUpperCase();
      if (!CODE_RE.test(code)) { safeSend(ws, { error: 'invalid_code' }); return; }
      const s = getOrCreateSession(code);
      clearExpiry(s);
      if (msg.type === 'register') {
        // Phone registers. Replace any stale phone connection for this session.
        if (s.phone && s.phone !== ws && s.phone.readyState === WebSocket.OPEN) {
          try { s.phone.close(1000, 'replaced'); } catch { /* ignore */ }
        }
        s.phone = ws;
        connMeta.set(ws, { role: 'phone', session: code, device: String(msg.device || '') });
        safeSend(ws, { type: 'registered', session: code });
        flushQueue(s); // deliver queued commands now that the phone is back
      } else {
        s.muse.add(ws);
        connMeta.set(ws, { role: 'muse', session: code });
        safeSend(ws, {
          type: 'joined', session: code,
          phone_online: !!(s.phone && s.phone.readyState === WebSocket.OPEN),
        });
      }
      return;
    }

    const meta = connMeta.get(ws);
    if (!meta || !meta.session) { safeSend(ws, { error: 'not_registered' }); return; }
    const s = sessions.get(meta.session);
    if (!s) { safeSend(ws, { error: 'session_expired' }); return; }
    s.lastActivity = now();

    if (meta.role === 'muse') {
      // ---- Muse -> phone: a command. Must carry an id so the result can be routed back. ----
      if (msg.id === undefined || msg.id === null || !msg.type) {
        safeSend(ws, { error: 'bad_command', detail: 'command requires id + type' });
        return;
      }
      if (rateLimited(s.code)) { safeSend(ws, { id: msg.id, error: 'rate_limited' }); return; }
      trackPending(s, msg, ws);
      if (s.phone && s.phone.readyState === WebSocket.OPEN) {
        s.phone.send(JSON.stringify(msg));
      } else {
        enqueue(s, msg, ws); // queued; delivered on reconnect or phone_offline after 60s TTL
      }
      return;
    }

    // ---- Phone -> Muse: result / error for a command id, or unsolicited event ----
    if (msg.id !== undefined && msg.id !== null && ('result' in msg || 'error' in msg)) {
      if (!resolvePending(s, msg)) {
        // No waiter (e.g. timed out): still broadcast so nothing is silently lost.
        for (const m of s.muse) safeSend(m, msg);
      }
      return;
    }
    // Unsolicited phone event (e.g. {type:"event", name:"..."}): broadcast to Muse clients.
    for (const m of s.muse) safeSend(m, msg);
  });

  ws.on('close', () => {
    clearInterval(pingTimer);
    const meta = connMeta.get(ws);
    if (!meta || !meta.session) return;
    const s = sessions.get(meta.session);
    if (!s) return;
    if (meta.role === 'phone' && s.phone === ws) s.phone = null;
    if (meta.role === 'muse') s.muse.delete(ws);
    connMeta.delete(ws);
    // If the session is now fully empty, keep it 60 min (queued commands + re-pair), then expire.
    if (!s.phone && s.muse.size === 0) armExpiry(s);
  });

  ws.on('error', () => { /* close handler cleans up */ });
});

server.listen(PORT, () => {
  console.log(`trix-turbo-relay listening on :${PORT}  (ws /ws, http /health)`);
});
