// Nightfall Wisps room server: private host-created rooms over WebSocket.
// It owns rooms, usernames, host/kick rules and reconnect tokens, and relays
// game traffic between the host and its players. It also serves index.html.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 4;
const GRACE_MS = 45000;        // time a dropped player may reclaim their seat
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const ADJ = ['Hollow', 'Ashen', 'Pale', 'Gloom', 'Ember', 'Misty', 'Silent', 'Frost', 'Wicked', 'Lantern', 'Grim', 'Dusk', 'Mossy', 'Cursed', 'Faint', 'Wild'];
const NOUN = ['Wisp', 'Moth', 'Raven', 'Ghoul', 'Warden', 'Specter', 'Wraith', 'Fox', 'Owl', 'Shade', 'Harrier', 'Reaper', 'Stag', 'Crow', 'Sprite', 'Nomad'];

const rooms = new Map();       // code -> room
const rnd = n => crypto.randomInt(n);
const pick = a => a[rnd(a.length)];
const send = (ws, m) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); };

function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_CHARS[rnd(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}
function cleanName(s) {
  s = String(s || '').replace(/[^A-Za-z0-9 _\-]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);
  return s.length >= 2 ? s : '';
}
function nameTaken(room, name, exceptId) {
  const k = name.toLowerCase();
  for (const p of room.players.values()) if (p.id !== exceptId && p.name.toLowerCase() === k) return true;
  return false;
}
function randomName(room) {
  for (let i = 0; i < 60; i++) {
    const n = pick(ADJ) + pick(NOUN) + (i < 30 ? '' : rnd(90) + 10);
    if (!nameTaken(room, n)) return n;
  }
  return 'Wisp' + crypto.randomBytes(3).toString('hex');
}
const publicPlayers = room => [...room.players.values()].map(p => ({ id: p.id, name: p.name, look: p.look, online: !!p.ws }));
const broadcast = (room, m, exceptId) => { for (const p of room.players.values()) if (p.id !== exceptId) send(p.ws, m); };
const roster = room => broadcast(room, { t: 'roster', players: publicPlayers(room), host: room.hostId, started: room.started });

function sanitizeLook(l) {
  const o = {};
  if (l && typeof l === 'object') for (const k of ['cloak', 'trim', 'eyes', 'hat', 'bolt']) {
    const v = Number(l[k]); o[k] = Number.isInteger(v) && v >= 0 && v < 12 ? v : 0;
  }
  return o;
}
function destroyIfEmpty(room) {
  if ([...room.players.values()].some(p => p.ws || p.graceTimer)) return;
  rooms.delete(room.code);
}
function removePlayer(room, p, why) {
  clearTimeout(p.graceTimer);
  room.players.delete(p.id);
  if (why === 'kick') room.banned.add(p.token);
  if (p.id === room.hostId) {                       // host gone => the world is gone
    broadcast(room, { t: 'closed', reason: 'The host left the room.' });
    for (const q of room.players.values()) { clearTimeout(q.graceTimer); if (q.ws) { q.ws.room = null; q.ws.pid = null; } }
    rooms.delete(room.code);
    return;
  }
  send(room.players.get(room.hostId)?.ws, { t: 'left', id: p.id });
  roster(room);
  destroyIfEmpty(room);
}
function dropConnection(room, p) {
  p.ws = null;
  roster(room);
  if (p.id === room.hostId) broadcast(room, { t: 'hostlost' });
  else send(room.players.get(room.hostId)?.ws, { t: 'plost', id: p.id });
  p.graceTimer = setTimeout(() => { p.graceTimer = null; if (!p.ws) removePlayer(room, p, 'timeout'); }, GRACE_MS);
}
function attach(ws, room, p) {
  clearTimeout(p.graceTimer); p.graceTimer = null;
  if (p.ws && p.ws !== ws) { p.ws.room = null; p.ws.pid = null; try { p.ws.close(); } catch (e) {} }
  p.ws = ws; ws.room = room.code; ws.pid = p.id;
  send(ws, { t: 'joined', code: room.code, you: p.id, token: p.token, host: room.hostId, players: publicPlayers(room), started: room.started, seed: room.seed });
  roster(room);
  if (p.id === room.hostId) broadcast(room, { t: 'hostback' }, p.id);
  else send(room.players.get(room.hostId)?.ws, { t: 'pback', id: p.id });
}
function newPlayer(room, look) {
  const id = crypto.randomBytes(4).toString('hex');
  return { id, token: crypto.randomBytes(12).toString('hex'), name: randomName(room), look: sanitizeLook(look), ws: null, graceTimer: null };
}

function onMessage(ws, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  const room = ws.room ? rooms.get(ws.room) : null;
  const me = room && room.players.get(ws.pid);

  if (m.t === 'create') {
    if (room) return;
    const code = newCode();
    const r = { code, hostId: null, started: false, players: new Map(), banned: new Set(), seed: rnd(1e9) };
    const p = newPlayer(r, m.look); r.hostId = p.id; r.players.set(p.id, p); rooms.set(code, r);
    return attach(ws, r, p);
  }
  if (m.t === 'join') {
    if (room) return;
    const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    const r = rooms.get(code);
    if (!r) return send(ws, { t: 'error', code: 'noroom', msg: 'No room with that code. Check it and try again.' });
    const back = m.token && [...r.players.values()].find(p => p.token === m.token);   // reconnect
    if (back) return attach(ws, r, back);
    if (m.token && r.banned.has(m.token)) return send(ws, { t: 'error', code: 'banned', msg: 'The host removed you from this room.' });
    if (r.players.size >= MAX_PLAYERS) return send(ws, { t: 'error', code: 'full', msg: 'That room is full (4 players).' });
    const p = newPlayer(r, m.look); r.players.set(p.id, p);
    return attach(ws, r, p);
  }
  if (!room || !me) return;

  switch (m.t) {
    case 'look': me.look = sanitizeLook(m.look); roster(room); break;
    case 'rename': {
      const n = cleanName(m.name);
      if (!n) return send(ws, { t: 'error', code: 'name', msg: 'Names need 2–16 letters, numbers, spaces, - or _.' });
      if (nameTaken(room, n, me.id)) return send(ws, { t: 'error', code: 'name', msg: 'Someone in this room already has that name.' });
      me.name = n; roster(room); break;
    }
    case 'start':
      if (me.id !== room.hostId) return;
      room.started = true; room.seed = rnd(1e9);
      broadcast(room, { t: 'start', seed: room.seed, players: publicPlayers(room) }); break;
    case 'lobby':                                   // host returns everyone to the lobby
      if (me.id !== room.hostId) return;
      room.started = false; broadcast(room, { t: 'lobby' }); roster(room); break;
    case 'kick': {
      if (me.id !== room.hostId || m.id === me.id) return;
      const p = room.players.get(m.id); if (!p) return;
      send(p.ws, { t: 'kicked' }); if (p.ws) { p.ws.room = null; p.ws.pid = null; }
      removePlayer(room, p, 'kick'); break;
    }
    case 'leave': {
      ws.room = null; ws.pid = null; me.ws = null;
      removePlayer(room, me, 'leave'); send(ws, { t: 'left_room' }); break;
    }
    case 'g':                                       // game traffic: client -> host, host -> all/one
      if (typeof m.d === 'undefined') return;
      if (me.id === room.hostId) {
        const out = { t: 'g', d: m.d };
        if (m.to) send(room.players.get(m.to)?.ws, out); else broadcast(room, out, me.id);
      } else send(room.players.get(room.hostId)?.ws, { t: 'g', from: me.id, d: m.d });
      break;
    case 'ping': send(ws, { t: 'pong', c: m.c }); break;
  }
}

// ---- HTTP: serve the game itself so one deploy gives you everything ----
const INDEX = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(INDEX, (err, buf) => {
      if (err) { res.writeHead(404); return res.end('index.html not found next to server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(buf);
    });
  } else if (url === '/health') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('Not found'); }
});
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });
wss.on('connection', ws => {
  ws.alive = true; ws.room = null; ws.pid = null;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => { try { onMessage(ws, raw.toString()); } catch (e) { console.error(e); } });
  ws.on('close', () => {
    const room = ws.room && rooms.get(ws.room); const p = room && room.players.get(ws.pid);
    if (p && p.ws === ws) dropConnection(room, p);
  });
  ws.on('error', () => {});
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} } }, 15000);
server.listen(PORT, () => console.log('Nightfall Wisps rooms on http://localhost:' + PORT));
