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

function roomState(room) {
  return {
    code: room.code,
    hostSocketId: room.hostSocketId,
    players: [...room.players.entries()].map(([socketId, p]) => ({
      socketId,
      name: p.name,
      countryId: p.countryId || 0,
      isHost: socketId === room.hostSocketId
    }))
  };
}

function broadcastRoomState(room) {
  io.to(room.code).emit('room_state', roomState(room));
}

function leaveCurrentRoom(socket, reason = 'left') {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  const room = rooms.get(code);
  socketRoom.delete(socket.id);
  try { socket.leave(code); } catch (_) {}
  if (!room) return;

  const wasHost = room.hostSocketId === socket.id;
  room.players.delete(socket.id);

  if (wasHost) {
    io.to(code).emit('room_closed', { reason: 'The room host disconnected.' });
    rooms.delete(code);
    for (const sid of room.players.keys()) socketRoom.delete(sid);
    return;
  }

  if (!room.players.size) {
    rooms.delete(code);
    return;
  }

  broadcastRoomState(room);
  io.to(room.hostSocketId).emit('player_left', { socketId: socket.id, reason });
}

app.get('/', (req, res) => {
  res.type('text').send('Pixel Earth multiplayer server is online.');
});
app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size }));

io.on('connection', socket => {
  socket.on('create_room', (payload = {}, ack = () => {}) => {
    leaveCurrentRoom(socket, 'switched rooms');
    const code = makeRoomCode();
    const room = {
      code,
      hostSocketId: socket.id,
      players: new Map()
    };
    room.players.set(socket.id, { name: safeName(payload.name), countryId: 0 });
    rooms.set(code, room);
    socketRoom.set(socket.id, code);
    socket.join(code);
    ack({ ok: true, code, isHost: true, room: roomState(room) });
    broadcastRoomState(room);
  });

  socket.on('join_room', (payload = {}, ack = () => {}) => {
    const code = String(payload.code || '').trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'Room not found.' });
    if (room.players.size >= 8) return ack({ ok: false, error: 'Room is full (8 players max).' });

    leaveCurrentRoom(socket, 'switched rooms');
    room.players.set(socket.id, { name: safeName(payload.name), countryId: 0 });
    socketRoom.set(socket.id, code);
    socket.join(code);
    ack({ ok: true, code, isHost: false, room: roomState(room) });
    broadcastRoomState(room);
    io.to(room.hostSocketId).emit('snapshot_requested', { targetSocketId: socket.id });
  });

  socket.on('leave_room', () => leaveCurrentRoom(socket));

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
      if (sid !== socket.id && other.countryId === countryId) {
        return ack({ ok: false, error: 'That country is already controlled by another player.' });
      }
    }

    player.countryId = countryId;
    ack({ ok: true, countryId });
    broadcastRoomState(room);
    io.to(room.hostSocketId).emit('human_country_changed', roomState(room));
  });

  socket.on('request_snapshot', () => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId === socket.id) return;
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
    io.to(room.hostSocketId).emit('remote_action', {
      sourceSocketId: socket.id,
      countryId: player.countryId,
      action: action || {}
    });
  });

  socket.on('capture_batch', captures => {
    const code = socketRoom.get(socket.id);
    const room = rooms.get(code);
    if (!room || room.hostSocketId !== socket.id || !Array.isArray(captures) || !captures.length) return;
    socket.to(code).emit('capture_batch', captures.slice(0, 5000));
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

  socket.on('disconnect', () => leaveCurrentRoom(socket, 'disconnected'));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Pixel Earth multiplayer server listening on port ${PORT}`);
});
