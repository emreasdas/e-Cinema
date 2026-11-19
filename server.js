const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html okunamadi'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Bulunamadi');
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

// rooms: Map<code, { code, clients:Set<ws>, state:{playing,time,updatedAt}|null, source:{kind,value,label}|null }>
const rooms = new Map();
let nextId = 1;

const fmt = (s) => {
  s = Math.max(0, Math.floor(s));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  const mm = String(m).padStart(h ? 2 : 1, '0'), ss = String(sec).padStart(2, '0');
  return h ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
};
const nowTime = (st) => (st.playing ? st.time + (Date.now() - st.updatedAt) / 1000 : st.time);
const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const broadcast = (room, obj, except) => {
  for (const c of room.clients) if (c !== except) send(c, obj);
};
const userList = (room) => [...room.clients].map((c) => c.userName);
const clean = (s, max) => String(s || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, max);

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  room.clients.delete(ws);
  ws.room = null;
  if (room.clients.size === 0) {
    rooms.delete(room.code);
  } else {
    broadcast(room, { type: 'system', text: `${ws.userName} ayrıldı` });
    broadcast(room, { type: 'users', list: userList(room) });
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.id = String(nextId++);
  ws.room = null;
  ws.userName = 'Misafir';
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.type !== 'string') return;

    if (m.type === 'ping') return send(ws, { type: 'pong' });

    if (m.type === 'join') {
      if (ws.room) return;
      const code = clean(m.room, 40).toLowerCase();
      if (!code) return;
      ws.userName = clean(m.name, 24) || 'Misafir';
      let room = rooms.get(code);
      if (!room) { room = { code, clients: new Set(), state: null, source: null }; rooms.set(code, room); }
      room.clients.add(ws);
      ws.room = room;
      send(ws, { type: 'welcome', id: ws.id, room: code });
      broadcast(room, { type: 'system', text: `${ws.userName} katıldı` }, ws);
      broadcast(room, { type: 'users', list: userList(room) });
      return;
    }

    const room = ws.room;
    if (!room) return;

    if (m.type === 'getState') {
      const reply = { type: 'state', source: room.source || null, empty: !room.state };
      if (room.state) { reply.playing = room.state.playing; reply.time = nowTime(room.state); }
      send(ws, reply);
      send(ws, { type: 'users', list: userList(room) });
      return;
    }

    if (m.type === 'sync') {
      const time = Number(m.time);
      if (!['play', 'pause', 'seek'].includes(m.action) || !Number.isFinite(time) || time < 0) return;
      const prev = room.state || { playing: false };
      const playing = m.action === 'play' ? true : m.action === 'pause' ? false : prev.playing;
      room.state = { playing, time, updatedAt: Date.now() };
      const verb = { play: 'oynattı', pause: 'durdurdu', seek: 'ileri/geri sardı' }[m.action];
      broadcast(room, {
        type: 'sync', action: m.action, playing, time,
        text: `${ws.userName} ${verb} (${fmt(time)})`,
      }, ws);
      return;
    }

    if (m.type === 'source') {
      const s = m.source || {};
      const kind = s.kind;
      const value = clean(s.value, 2000);
      const label = clean(s.label, 200);
      if (!['url', 'youtube', 'file'].includes(kind) || !value) return;
      if (kind === 'url' && !/^https?:\/\//i.test(value)) return;
      if (kind === 'youtube' && !/^[\w-]{11}$/.test(value)) return;
      room.source = { kind, value, label };
      room.state = { playing: false, time: 0, updatedAt: Date.now() };
      broadcast(room, {
        type: 'source', source: room.source,
        text: `${ws.userName} yeni video seçti: ${label || kind}`,
      }, ws);
      return;
    }

    if (m.type === 'chat') {
      const text = clean(m.text, 500);
      if (!text) return;
      broadcast(room, { type: 'chat', fromId: ws.id, name: ws.userName, text, ts: Date.now() });
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

// Kopmuş bağlantıları temizle
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`Sunucu ${PORT} portunda çalışıyor. Tarayıcıda şunu aç: http://localhost:${PORT}`));
