// Vanish server: serves the page, relays encrypted messages & WebRTC signaling.
// Never stores anything. A room lives only in memory and is deleted when the last person leaves.
const http = require('http'), fs = require('fs'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const html = fs.readFileSync(__dirname + '/index.html');
const MAX_PEERS = +process.env.MAX_PEERS || 8;
const rooms = new Map(); // roomId -> Map(peerId -> ws)

// ICE servers sent to clients. Add TURN via env vars for users behind strict firewalls:
// TURN_URL="turn:host:3478,turns:host:5349"  TURN_USER=...  TURN_PASS=...
const iceServers = () => {
  const s = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }];
  if (process.env.TURN_URL) s.push({ urls: process.env.TURN_URL.split(',').map(x => x.trim()), username: process.env.TURN_USER, credential: process.env.TURN_PASS });
  return s;
};

const base = { 'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY' };
const srv = http.createServer((req, res) => {
  const path = req.url.split('?')[0];
  if (path === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (path === '/ice') { res.writeHead(200, { ...base, 'Content-Type': 'application/json' }); return res.end(JSON.stringify(iceServers())); }
  if (path !== '/' && path !== '/index.html') { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
  res.writeHead(200, {
    ...base,
    'Content-Type': 'text/html; charset=utf-8',
    'Content-Security-Policy': "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' ws: wss:; media-src 'self' blob:; img-src 'self' data:; frame-ancestors 'none'",
    'Permissions-Policy': 'camera=(self), microphone=(self)',
  });
  res.end(html);
});

const wss = new WebSocketServer({ server: srv, maxPayload: 256 * 1024 });

// Heartbeat: keeps connections alive through proxies (Render, etc.) and removes dead ones.
const hb = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.dead) { ws.terminate(); continue; }
    ws.dead = true; ws.ping();
  }
}, 25000);
wss.on('close', () => clearInterval(hb));

wss.on('connection', (ws) => {
  const id = crypto.randomBytes(4).toString('hex');
  let room = null, tokens = 100, last = Date.now();
  ws.dead = false;
  ws.on('pong', () => { ws.dead = false; });
  ws.on('error', () => {});
  const send = (w, o) => { if (w.readyState === 1) w.send(JSON.stringify(o)); };

  ws.on('message', (raw) => {
    // simple rate limit: ~20 messages/sec sustained, bursts up to 100
    const now = Date.now(); tokens = Math.min(100, tokens + (now - last) / 50); last = now;
    if (tokens < 1) return; tokens--;

    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'join' && !room && typeof m.room === 'string' && /^[\w-]{8,64}$/.test(m.room)) {
      if ((rooms.get(m.room)?.size || 0) >= MAX_PEERS) { send(ws, { type: 'full' }); return ws.close(); }
      room = m.room;
      if (!rooms.has(room)) rooms.set(room, new Map());
      const r = rooms.get(room);
      send(ws, { type: 'welcome', id, peers: [...r.keys()] });
      for (const w of r.values()) send(w, { type: 'joined', id });
      r.set(id, ws);
      return;
    }
    if (!room) return;
    const r = rooms.get(room);
    if (m.type === 'signal' && typeof m.to === 'string' && m.data && typeof m.data === 'object') {
      const t = r.get(m.to); if (t) send(t, { type: 'signal', from: id, data: m.data });
    } else if (m.type === 'chat' && m.data && typeof m.data.iv === 'string' && typeof m.data.ct === 'string') {
      for (const [p, w] of r) if (p !== id) send(w, { type: 'chat', from: id, data: { iv: m.data.iv, ct: m.data.ct } });
    }
  });

  ws.on('close', () => {
    if (!room) return;
    const r = rooms.get(room); if (!r) return;
    r.delete(id);
    if (!r.size) rooms.delete(room);               // last one out: room destroyed
    else for (const w of r.values()) send(w, { type: 'left', id });
  });
});

process.on('uncaughtException', e => console.error('Error:', e));
process.on('SIGTERM', () => { wss.close(); srv.close(() => process.exit(0)); });

const PORT = process.env.PORT || 3000;
srv.listen(PORT, () => console.log('Running on port ' + PORT));
