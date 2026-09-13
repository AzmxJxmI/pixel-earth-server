const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 12 * 1024 * 1024,
  pingTimeout: 20000,
  pingInterval: 10000
});

const PORT = process.env.PORT || 3000;
const rooms = new Map();
const socketRoom = new Map();
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const RECONNECT_GRACE_MS = 30000;

function makeRoomCode() {
  for (let attempt = 0; attempt < 100; attempt++) {
    let code = '';
    for (let i = 0; i < 6; i++) code += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  return String(Date.now()).slice(-6);
}
function safeName(name) {
  const s = String(name || '').trim().replace(/\s+/g, ' ');
  return (s || 'Player').slice(0, 24);
}
function safeSessionId(value) {
  const s = String(value || '').trim().replace(/[^a-zA-Z0-9._:-]/g, '');
  return (s || `anon-${Date.now()}-${Math.random().toString(36).slice(2)}`).slice(0, 96);
}
function population(room) {
  return room.players.size + room.reservations.size;
}
function reservationRows(room) {
  return [...room.reservations.values()].map(r => ({
    socketId: '',
    name: r.name,
    countryId: r.countryId || 0,
    isHost: !!r.isHost,
    reconnecting: true
  }));
}
function roomState(room) {
  return {
    code: room.code,
    hostSocketId: room.hostSocketId || '',
    players: [
      ...[...room.players.entries()].map(([socketId, p]) => ({
        socketId,
        name: p.name,
        countryId: p.countryId || 0,
        isHost: socketId === room.hostSocketId,
        reconnecting: false
      })),
      ...reservationRows(room)
    ]
  };
}
function broadcastRoomState(room) {
  io.to(room.code).emit('room_state', roomState(room));
}
function closeRoom(room, reason) {
  io.to(room.code).emit('room_closed', { reason });
  rooms.delete(room.code);
  for (const sid of room.players.keys()) socketRoom.delete(sid);
  for (const r of room.reservations.values()) if (r.timer) clearTimeout(r.timer);
  room.reservations.clear();
}
function emitPlayerLeft(room, oldSocketId, countryId, reason) {
  if (room.hostSocketId && room.players.has(room.hostSocketId)) {
    io.to(room.hostSocketId).emit('player_left', { socketId: oldSocketId, countryId, reason });
  }
}
function expireReservation(code, sessionId) {
  const room = rooms.get(code);
  if (!room) return;
  const r = room.reservations.get(sessionId);
  if (!r) return;
  room.reservations.delete(sessionId);
  if (r.isHost) {
    closeRoom(room, 'The room host did not reconnect in time.');
    return;
  }
  broadcastRoomState(room);
  emitPlayerLeft(room, r.oldSocketId, r.countryId, 'reconnect timeout');
  if (!population(room)) rooms.delete(code);
}
function reserveDisconnectedPlayer(socket, room, player) {
  const isHost = room.hostSocketId === socket.id || player.sessionId === room.hostSessionId;
  const old = room.reservations.get(player.sessionId);
  if (old?.timer) clearTimeout(old.timer);
  room.players.delete(socket.id);
  socketRoom.delete(socket.id);
  if (isHost) room.hostSocketId = '';
  const reservation = {
    name: player.name,
    countryId: player.countryId || 0,
    sessionId: player.sessionId,
    isHost,
    oldSocketId: socket.id,
    timer: null
  };
  reservation.timer = setTimeout(() => expireReservation(room.code, player.sessionId), RECONNECT_GRACE_MS);
  room.reservations.set(player.sessionId, reservation);
  broadcastRoomState(room);
}
function removeImmediately(socket, room, player, reason) {
  const wasHost = room.hostSocketId === socket.id || player.sessionId === room.hostSessionId;
  room.players.delete(socket.id);
  socketRoom.delete(socket.id);
  try { socket.leave(room.code); } catch (_) {}
  if (wasHost) {
    closeRoom(room, 'The room host left.');
    return;
  }
  broadcastRoomState(room);
  emitPlayerLeft(room, socket.id, player.countryId || 0, reason);
  if (!population(room)) rooms.delete(room.code);
}
function leaveCurrentRoom(socket, reason = 'left', allowReconnect = false) {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  const room = rooms.get(code);
  const player = room?.players.get(socket.id);
  if (!room || !player) {
    socketRoom.delete(socket.id);
    return;
  }
  if (allowReconnect) reserveDisconnectedPlayer(socket, room, player);
  else removeImmediately(socket, room, player, reason);
}
function findConnectedSession(room, sessionId) {
  for (const [sid, p] of room.players) if (p.sessionId === sessionId) return { sid, player: p };
  return null;
}
function restoreSession(socket, room, sessionId, suppliedName, ack = () => {}) {
  let oldSocketId = '';
  let name = safeName(suppliedName);
  let countryId = 0;
  let isHost = false;

  const reserved = room.reservations.get(sessionId);
  if (reserved) {
    if (reserved.timer) clearTimeout(reserved.timer);
    room.reservations.delete(sessionId);
    oldSocketId = reserved.oldSocketId || '';
    name = reserved.name || name;
    countryId = reserved.countryId || 0;
    isHost = !!reserved.isHost;
  } else {
    const live = findConnectedSession(room, sessionId);
    if (!live) return false;
    oldSocketId = live.sid;
    name = live.player.name || name;
    countryId = live.player.countryId || 0;
    isHost = live.sid === room.hostSocketId || sessionId === room.hostSessionId;
    if (live.sid !== socket.id) {
      room.players.delete(live.sid);
      socketRoom.delete(live.sid);
      try { io.sockets.sockets.get(live.sid)?.disconnect(true); } catch (_) {}
    }
  }

  room.players.set(socket.id, { name, countryId, sessionId });
  socketRoom.set(socket.id, room.code);
  socket.join(room.code);
  if (isHost) {
    room.hostSocketId = socket.id;
    room.hostSessionId = sessionId;
  }

  ack({ ok: true, code: room.code, isHost, countryId, oldSocketId, room: roomState(room) });
  broadcastRoomState(room);

  if (isHost) {
    io.to(socket.id).emit('player_rejoined', { oldSocketId, newSocketId: socket.id, countryId, isHost: true });
  } else if (room.hostSocketId && room.players.has(room.hostSocketId)) {
    io.to(room.hostSocketId).emit('player_rejoined', { oldSocketId, newSocketId: socket.id, countryId, isHost: false });
    io.to(room.hostSocketId).emit('snapshot_requested', { targetSocketId: socket.id });
  }
  return true;
}

app.get('/', (req, res) => res.type('text').send('Pixel Earth multiplayer server V6 is online.'));
app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size, reconnectGraceMs: RECONNECT_GRACE_MS }));

io.on('connection', socket => {
  socket.on('create_room', (payload = {}, ack = () => {}) => {
    leaveCurrentRoom(socket, 'switched rooms', false);
    const code = makeRoomCode();
    const sessionId = safeSessionId(payload.sessionId);
    const room = {
      code,
      hostSocketId: socket.id,
      hostSessionId: sessionId,
      players: new Map(),
      reservations: new Map()
    };
    room.players.set(socket.id, { name: safeName(payload.name), countryId: 0, sessionId });
    rooms.set(code, room);
    socketRoom.set(socket.id, code);
    socket.join(code);
    ack({ ok: true, code, isHost: true, countryId: 0, room: roomState(room) });
    broadcastRoomState(room);
  });

  socket.on('join_room', (payload = {}, ack = () => {}) => {
    const code = String(payload.code || '').trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'Room not found.' });
    const sessionId = safeSessionId(payload.sessionId);
    if (room.reservations.has(sessionId) || findConnectedSession(room, sessionId)) {
      leaveCurrentRoom(socket, 'switched rooms', false);
      if (restoreSession(socket, room, sessionId, payload.name, ack)) return;
    }
    if (population(room) >= 8) return ack({ ok: false, error: 'Room is full (8 players max).' });

    leaveCurrentRoom(socket, 'switched rooms', false);
    room.players.set(socket.id, { name: safeName(payload.name), countryId: 0, sessionId });
    socketRoom.set(socket.id, code);
    socket.join(code);
    ack({ ok: true, code, isHost: false, countryId: 0, room: roomState(room) });
    broadcastRoomState(room);
    if (room.hostSocketId) io.to(room.hostSocketId).emit('snapshot_requested', { targetSocketId: socket.id });
  });

  socket.on('rejoin_room', (payload = {}, ack = () => {}) => {
    const code = String(payload.code || '').trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'Room no longer exists.' });
    const sessionId = safeSessionId(payload.sessionId);
    if (!restoreSession(socket, room, sessionId, payload.name, ack)) {
      return ack({ ok: false, error: 'Your reconnect reservation expired or was not found.' });
    }
  });

  socket.on('leave_room', () => leaveCurrentRoom(socket, 'left', false));

  socket.on('select_country', (payload = {}, ack = () => {}) => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'Not in a room.' });
    const player = room.players.get(socket.id);
    if (!player) return ack({ ok: false, error: 'Player not found.' });
    if (player.countryId) return ack({ ok: false, error: 'Your country is already locked for this room.' });

    const countryId = Number(payload.countryId) || 0;
    if (countryId <= 0 || countryId > 255) return ack({ ok: false, error: 'Invalid country.' });
    for (const [sid, other] of room.players) {
      if (sid !== socket.id && other.countryId === countryId) return ack({ ok: false, error: 'That country is already controlled by another player.' });
    }
    for (const r of room.reservations.values()) {
      if (r.sessionId !== player.sessionId && r.countryId === countryId) return ack({ ok: false, error: 'That country is reserved for a reconnecting player.' });
    }

    player.countryId = countryId;

    // HARD REAL-PLAYER OWNERSHIP LOCK: tell the host before the guest receives
    // the successful selection callback. This stops host AI from getting another
    // development tick on the newly claimed country.
    if (room.hostSocketId && room.hostSocketId !== socket.id) {
      io.to(room.hostSocketId).emit('human_country_lock', {
        countryId,
        socketId: socket.id,
        name: player.name
      });
    }
    broadcastRoomState(room);
    if (room.hostSocketId) io.to(room.hostSocketId).emit('human_country_changed', roomState(room));
    ack({ ok: true, countryId });
  });

  socket.on('request_snapshot', () => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId === socket.id || !room.hostSocketId) return;
    io.to(room.hostSocketId).emit('snapshot_requested', { targetSocketId: socket.id });
  });

  socket.on('host_snapshot', payload => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !payload) return;
    const target = String(payload.targetSocketId || '');
    if (!room.players.has(target)) return;
    io.to(target).emit('snapshot', payload.snapshot);
  });

  socket.on('player_action', action => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId === socket.id) return;
    const player = room.players.get(socket.id);
    if (!player || !player.countryId) return;
    if (!room.hostSocketId || !room.players.has(room.hostSocketId)) {
      socket.emit('client_event', { type: 'hud', payload: { type: 'hud', title: 'Host reconnecting', body: 'The room host is temporarily offline. Your action was not applied; try again when the host reconnects.' } });
      return;
    }
    io.to(room.hostSocketId).emit('remote_action', {
      sourceSocketId: socket.id,
      countryId: player.countryId,
      action: action || {}
    });
  });

  socket.on('capture_batch', payload => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id) return;

    const rows = Array.isArray(payload)
      ? payload
      : (Array.isArray(payload?.rows) ? payload.rows : []);
    if (!rows.length) return;

    const safeRows = rows.slice(0, 5000);
    const firstRev = Number(safeRows[0]?.[0]) || 0;
    const lastRev = Number(safeRows[safeRows.length - 1]?.[0]) || firstRev;

    socket.to(code).emit('capture_batch', {
      fromRev: Number(payload?.fromRev) || firstRev,
      toRev: Number(payload?.toRev) || lastRev,
      rows: safeRows
    });
  });
  socket.on('small_state', state => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !state) return;
    socket.to(code).emit('small_state', state);
  });
  socket.on('base_state', state => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id) return;
    socket.to(code).emit('base_state', Array.isArray(state) ? state : []);
  });
  socket.on('repair_state', state => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !state) return;
    socket.to(code).emit('repair_state', state);
  });
  socket.on('wall_event', evt => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !evt) return;
    socket.to(code).emit('wall_event', evt);
  });

  // Host-only direct UI channel. Private diplomacy/trade/call prompts no longer
  // ride inside room-wide small_state packets.
  socket.on('host_client_event', (evt = {}, ack = () => {}) => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !evt) return ack({ ok: false, error: 'not_host' });
    const target = String(evt.targetSocketId || '');
    if (!target || !room.players.has(target)) return ack({ ok: false, error: 'target_offline' });
    io.to(target).emit('client_event', {
      type: String(evt.type || '').slice(0, 40),
      eventId: Number(evt.eventId) || 0,
      payload: evt.payload && typeof evt.payload === 'object' ? evt.payload : {}
    });
    ack({ ok: true });
  });

  socket.on('disconnect', () => leaveCurrentRoom(socket, 'disconnected', true));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Pixel Earth multiplayer V6 listening on port ${PORT}`);
});
