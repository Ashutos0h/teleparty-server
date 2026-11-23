const express = require('express');
const http = require('node:http');
const { createHmac } = require('node:crypto');
const { Server } = require('socket.io');
const cors = require('cors');
const Redis = require('ioredis');

const MAX_MEMBERS = 5;
const RECOVERY_MS = 30_000;
const PLAYBACK_TTL_SECONDS = 86_400;
const recoverableReasons = new Set(['transport error', 'transport close', 'ping timeout', 'forced close']);
const rooms = new Map();
const pendingLeaves = new Map();
const pendingLookups = new Map();
const turnUrls = (process.env.TURN_URLS || '').split(',').map(url => url.trim())
  .filter(url => /^turns?:[^\s]+$/.test(url));
const turnSecret = process.env.TURN_SHARED_SECRET;
const turnConfigured = Boolean(turnSecret && turnUrls.length);
const turnIssuance = new Map();
const TURN_TTL_SECONDS = 6 * 3600;
const TURN_WINDOW_MS = 60_000;
const TURN_LIMIT_PER_IP = 30;

const redis = process.env.REDIS_URL ? new Redis(process.env.REDIS_URL, {
  enableOfflineQueue: false,
  maxRetriesPerRequest: 0,
  commandTimeout: 1500
}) : null;
redis?.on('error', err => console.warn('Playback storage unavailable:', err.message));
if (!turnConfigured) console.warn('TURN is unconfigured. Calls will use direct ICE candidates only.');

function playbackKey(roomId) {
  return `room:${roomId}:playback`;
}

async function readPlayback(roomId) {
  if (redis?.status !== 'ready') return null;
  try {
    const saved = await redis.get(playbackKey(roomId));
    if (saved) return JSON.parse(saved);
    const legacy = await redis.hgetall(`room:${roomId}:state`);
    return Object.keys(legacy).length ? legacy : null;
  } catch {
    return null;
  }
}

function savePlayback(roomId, state, time) {
  const room = rooms.get(roomId);
  if (!room || !Number.isFinite(time) || time < 0) return;
  const playback = { state, time, updatedAt: Date.now() };
  room.playback = playback;
  if (redis?.status === 'ready') {
    redis.set(playbackKey(roomId), JSON.stringify(playback), 'EX', PLAYBACK_TTL_SECONDS).catch(() => {});
  }
}

function getTurnCredentials(socket) {
  if (!turnConfigured) return null;
  const now = Date.now();
  if (socket.data.turn?.expiresAt > now + 5 * TURN_WINDOW_MS) return socket.data.turn;

  // Render supplies the connecting address in the final X-Forwarded-For entry.
  const forwarded = process.env.RENDER === 'true' && socket.handshake.headers['x-forwarded-for'];
  const ip = (typeof forwarded === 'string' && forwarded.split(',').at(-1).trim()) || socket.handshake.address;
  let usage = turnIssuance.get(ip);
  if (!usage || usage.until <= now) {
    usage = { count: 0, until: now + TURN_WINDOW_MS };
    turnIssuance.set(ip, usage);
  }
  if (usage.count >= TURN_LIMIT_PER_IP) return null;
  usage.count++;
  const expiry = Math.floor(now / 1000) + TURN_TTL_SECONDS;
  const username = `${expiry}:${socket.id}`;
  const credential = createHmac('sha1', turnSecret).update(username).digest('base64');
  socket.data.turn = { iceServers: [{ urls: turnUrls, username, credential }], expiresAt: expiry * 1000 };
  return socket.data.turn;
}

const cleanup = setInterval(() => {
  for (const [ip, usage] of turnIssuance) {
    if (usage.until <= Date.now()) turnIssuance.delete(ip);
  }
}, TURN_WINDOW_MS);
cleanup.unref();

const app = express();
app.use(cors());
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'Teleparty Clone Server', activeRooms: rooms.size, turnConfigured });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  connectionStateRecovery: { maxDisconnectionDuration: RECOVERY_MS, skipMiddlewares: false },
  pingInterval: 20_000,
  pingTimeout: 30_000,
  connectTimeout: 20_000
});

function finishLookup(targetId, payload) {
  const pending = pendingLookups.get(targetId);
  if (!pending) return;
  clearTimeout(pending.timeout);
  pendingLookups.delete(targetId);
  if (payload) pending.callback(payload);
}

function partyChannel(roomId) {
  return `party:${roomId}`;
}

function removeMember(roomId, userId) {
  const room = rooms.get(roomId);
  if (!room?.users.delete(userId)) return;
  io.to(partyChannel(roomId)).emit('user_left', { userId, totalMembers: room.users.size });
  if (room.users.size === 0) rooms.delete(roomId);
}

function leaveRoom(socket) {
  const roomId = socket.data.roomId;
  if (!roomId) return;
  socket.leave(partyChannel(roomId));
  delete socket.data.roomId;
  removeMember(roomId, socket.id);
}

function validRoomId(roomId) {
  return typeof roomId === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(roomId);
}

io.on('connection', socket => {
  const pendingLeave = pendingLeaves.get(socket.id);
  if (pendingLeave) {
    clearTimeout(pendingLeave);
    pendingLeaves.delete(socket.id);
  }
  if (socket.recovered && !rooms.get(socket.data.roomId)?.users.has(socket.id)) {
    if (socket.data.roomId) socket.leave(partyChannel(socket.data.roomId));
    delete socket.data.roomId;
  }
  let joinAttempt = 0;

  socket.on('join_room', async (roomId, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    if (!validRoomId(roomId)) return respond({ error: 'Invalid room id.' });
    const attempt = ++joinAttempt;
    const saved = rooms.has(roomId) ? null : await readPlayback(roomId);
    if (!socket.connected || attempt !== joinAttempt) return respond({ error: 'Join superseded.' });

    let room = rooms.get(roomId);
    if (room && !room.users.has(socket.id) && room.users.size >= MAX_MEMBERS) {
      return respond({ error: 'Room is full! Maximum 5 members allowed.' });
    }
    if (socket.data.roomId !== roomId) leaveRoom(socket);
    if (!room) {
      room = { users: new Set(), playback: saved };
      rooms.set(roomId, room);
    }

    const alreadyJoined = room.users.has(socket.id);
    // Keep recovering members in the roster so peers can retain the live call.
    const existingUsers = [...room.users].filter(id => id !== socket.id);
    socket.join(partyChannel(roomId));
    socket.data.roomId = roomId;
    room.users.add(socket.id);
    const totalMembers = room.users.size;
    respond({
      success: true, roomId, totalMembers, existingUsers,
      recoveredState: room.playback,
      turn: getTurnCredentials(socket)
    });
    // A room with just its host also needs a packet offset to resume its session.
    socket.emit('room_ready', roomId);
    if (!alreadyJoined) socket.to(partyChannel(roomId)).emit('user_joined', { userId: socket.id, totalMembers });
  });

  socket.on('get_turn_credentials', callback => {
    if (typeof callback === 'function') callback(socket.data.roomId ? getTurnCredentials(socket) : null);
  });

  socket.on('lookup_room', (roomId, callback) => {
    if (typeof callback !== 'function') return;
    if (!validRoomId(roomId)) return callback({ error: 'Invalid room id.' });
    const room = rooms.get(roomId);
    const hostId = room && [...room.users].find(id => io.sockets.sockets.has(id));
    if (!hostId) return callback({ error: 'Room host is offline. Try again when they reconnect.' });
    finishLookup(socket.id, { error: 'Lookup superseded.' });
    const timeout = setTimeout(() => {
      finishLookup(socket.id, { error: 'Could not find the host video URL.' });
    }, 5_000);
    pendingLookups.set(socket.id, { callback, timeout, hostId });
    io.to(hostId).emit('chat_message', {
      isSystemCommand: true, command: 'REQUEST_URL', requesterId: socket.id, userId: socket.id
    });
  });

  socket.on('webrtc_signal', payload => {
    const room = rooms.get(socket.data.roomId);
    const userId = payload?.targetUserId;
    if (!room?.users.has(userId) || userId === socket.id || !payload.signal) return;
    io.to(userId).emit('webrtc_signal', { callerId: socket.id, signal: payload.signal, generation: payload.generation });
  });

  for (const event of ['video_play', 'video_pause', 'video_seek']) {
    socket.on(event, data => {
      const roomId = socket.data.roomId;
      if (!roomId || !data || data.heartbeat) return;
      socket.to(partyChannel(roomId)).emit(event, data);
      const paused = event === 'video_pause' || (event === 'video_seek' && data.paused);
      savePlayback(roomId, paused ? 'paused' : 'playing', Number(data.time));
    });
  }

  socket.on('video_sync', data => {
    if (socket.data.roomId && data) socket.to(partyChannel(socket.data.roomId)).volatile.emit('video_sync', data);
  });

  socket.on('chat_message', data => {
    if (!data || typeof data !== 'object') return;
    if (data.isSystemCommand && data.command === 'URL_RESPONSE') {
      if (pendingLookups.get(data.targetId)?.hostId === socket.id && typeof data.url === 'string') {
        finishLookup(data.targetId, { url: data.url });
      }
      if (socket.data.roomId && rooms.get(socket.data.roomId)?.users.has(data.targetId)) {
        io.to(data.targetId).emit('chat_message', { ...data, userId: socket.id });
      }
      return;
    }
    if (!socket.data.roomId) return;
    let recipients = socket.to(partyChannel(socket.data.roomId));
    if (data.isSystemCommand && data.command === 'VIDEO_SYNC') recipients = recipients.volatile;
    recipients.emit('chat_message', { ...data, userId: socket.id });
  });

  socket.on('disconnect', reason => {
    finishLookup(socket.id);
    const roomId = socket.data.roomId;
    if (!roomId) return;
    if (!recoverableReasons.has(reason)) {
      removeMember(roomId, socket.id);
      return;
    }
    const timer = setTimeout(() => {
      pendingLeaves.delete(socket.id);
      removeMember(roomId, socket.id);
    }, RECOVERY_MS);
    timer.unref();
    pendingLeaves.set(socket.id, timer);
  });
});

const PORT = Number(process.env.PORT ?? 3000);
server.listen(PORT, () => console.log(`Server listening on port ${server.address().port}`));

process.once('SIGTERM', () => {
  io.close(() => redis?.disconnect());
});
