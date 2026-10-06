// Relay-only server: never stores messages, never sees keys or plaintext.
// A room exists only while at least one person is connected; when the last
// person leaves, the room (in memory only) is deleted.
const http = require('http'), fs = require('fs'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const html = fs.readFileSync(__dirname + '/index.html');
const rooms = new Map(); // roomId -> Map(peerId -> ws)

const srv = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-store',
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
  });
  res.end(html);
});

const wss = new WebSocketServer({ server: srv, maxPayload: 256 * 1024 });

wss.on('connection', (ws) => {
  const id = crypto.randomBytes(4).toString('hex');
  let room = null;
  const send = (w, o) => w.readyState === 1 && w.send(JSON.stringify(o));

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join' && !room && /^[\w-]{8,64}$/.test(m.room)) {
      if ((rooms.get(m.room)?.size || 0) >= 8) { send(ws, { type: 'full' }); return ws.close(); }
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
    if (m.type === 'signal') {
      const t = r.get(m.to); if (t) send(t, { type: 'signal', from: id, data: m.data });
    } else if (m.type === 'chat') {
      for (const [p, w] of r) if (p !== id) send(w, { type: 'chat', from: id, data: m.data });
    }
  });

  ws.on('close', () => {
    if (!room) return;
    const r = rooms.get(room);
    r.delete(id);
    if (!r.size) rooms.delete(room);           // last one out: room data destroyed
    else for (const w of r.values()) send(w, { type: 'left', id });
  });
});

srv.listen(process.env.PORT || 3000, () => console.log('Running on port ' + (process.env.PORT || 3000)));
