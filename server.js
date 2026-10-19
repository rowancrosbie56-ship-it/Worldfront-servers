'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, 'public');
const PORT = Number(process.env.PORT || 3000);
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();
const clientRooms = new WeakMap();
const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript; charset=utf-8', '.css':'text/css; charset=utf-8', '.json':'application/json; charset=utf-8', '.svg':'image/svg+xml', '.png':'image/png', '.ico':'image/x-icon' };
function send(ws, obj) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function newCode() {
  for (let attempt=0; attempt<100; attempt++) {
    const bytes = crypto.randomBytes(8);
    const code = [...bytes].map(b => CODE_CHARS[b % CODE_CHARS.length]).join('');
    if (!rooms.has(code)) return code;
  }
  throw new Error('Could not allocate a room code');
}
function detach(ws) {
  const info = clientRooms.get(ws);
  if (!info) return;
  clientRooms.delete(ws);
  const room = rooms.get(info.code);
  if (!room) return;
  if (info.role === 'host' && room.host === ws) {
    rooms.delete(info.code);
    if (room.guest) { send(room.guest, {_server:'error', message:'The host left; the room has closed.'}); clientRooms.delete(room.guest); room.guest.close(1000, 'Host left'); }
  } else if (info.role === 'guest' && room.guest === ws) {
    room.guest = null;
    send(room.host, {_server:'peer-left'});
  }
}
const server = http.createServer((req, res) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, {'Content-Type':'text/plain; charset=utf-8'}); return res.end('Method not allowed'); }
  let pathname;
  try { pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname); } catch { res.writeHead(400); return res.end('Bad request'); }
  if (pathname === '/') pathname = '/index.html';
  const full = path.resolve(PUBLIC, '.' + pathname);
  if (!full.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(err.code === 'ENOENT' ? 404 : 500, {'Content-Type':'text/plain; charset=utf-8'}); return res.end(err.code === 'ENOENT' ? 'Not found' : 'Server error'); }
    res.writeHead(200, {'Content-Type':MIME[path.extname(full)] || 'application/octet-stream', 'X-Content-Type-Options':'nosniff', 'Cache-Control':'no-store', 'Content-Security-Policy':"default-src 'self' https: data: blob:; script-src 'self' 'unsafe-inline' https:; style-src 'self' 'unsafe-inline' https:; connect-src 'self' ws: wss: https:; img-src 'self' https: data: blob:;"});
    if (req.method === 'HEAD') return res.end();
    res.end(data);
  });
});
const wss = new WebSocketServer({ noServer:true, maxPayload: 1 * 1024 * 1024 });
server.on('upgrade', (req, socket, head) => {
  let pathname;
  try { pathname = new URL(req.url, 'http://localhost').pathname; } catch { socket.destroy(); return; }
  if (pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});
wss.on('connection', ws => {
  let registered = false;
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { send(ws, {_server:'error', message:'Invalid message format.'}); return; }
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) return;
    if (msg._server === 'host') {
      if (registered) return send(ws, {_server:'error', message:'This connection already has a role.'});
      try {
        const code = newCode();
        rooms.set(code, { code, host:ws, guest:null, createdAt:Date.now() });
        clientRooms.set(ws, {code, role:'host'}); registered = true;
        send(ws, {_server:'hosted', code});
      } catch { send(ws, {_server:'error', message:'Could not create a room. Try again.'}); }
      return;
    }
    if (msg._server === 'join') {
      if (registered) return send(ws, {_server:'error', message:'This connection already has a role.'});
      const code = String(msg.code || '').toUpperCase().replace(/\s+/g, '');
      const room = rooms.get(code);
      if (!room || room.host.readyState !== WebSocket.OPEN) return send(ws, {_server:'error', message:'Room not found. Check the code and ask the host to create a new room.'});
      if (room.guest && room.guest.readyState === WebSocket.OPEN) return send(ws, {_server:'error', message:'That room already has a guest.'});
      room.guest = ws; clientRooms.set(ws, {code, role:'guest'}); registered = true;
      send(ws, {_server:'joined', code}); send(room.host, {_server:'peer-joined'});
      return;
    }
    const info = clientRooms.get(ws);
    if (!info) return send(ws, {_server:'error', message:'Create or join a room first.'});
    const room = rooms.get(info.code);
    if (!room) return send(ws, {_server:'error', message:'Room has closed.'});
    const recipient = info.role === 'host' ? room.guest : room.host;
    if (recipient && recipient.readyState === WebSocket.OPEN) recipient.send(JSON.stringify(msg));
  });
  ws.on('close', () => detach(ws));
  ws.on('error', () => detach(ws));
});
// Remove abandoned empty rooms and keep the process responsive during idle periods.
setInterval(() => {
  for (const [code, room] of rooms) {
    if (room.host.readyState !== WebSocket.OPEN) { rooms.delete(code); continue; }
    if (!room.guest && Date.now() - room.createdAt > 6 * 60 * 60 * 1000) rooms.delete(code);
  }
  for (const ws of wss.clients) if (ws.readyState === WebSocket.OPEN) ws.ping();
}, 60_000).unref();
server.listen(PORT, '0.0.0.0', () => console.log(`Worldfront server listening on port ${PORT}`));
