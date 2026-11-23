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
  res.json({ status: 'ok', service: 'Teleparty Clone Server', activeRooms: rooms.size, turnConfigured, presenceVersion: 2 });
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

function removeMember(roomId, participantKey, socketId) {
  const room = rooms.get(roomId);
  const member = room?.members.get(participantKey);
  if (member?.socketId !== socketId) return;
  clearTimeout(member.expiry);
  room.members.delete(participantKey);
  io.to(partyChannel(roomId)).emit('user_left', { userId: socketId, totalMembers: room.members.size });
  if (room.members.size === 0) rooms.delete(roomId);
}

function leaveRoom(socket, participantKey) {
  const roomId = socket.data.roomId;
  if (!roomId) return;
  socket.leave(partyChannel(roomId));
  delete socket.data.roomId;
  removeMember(roomId, participantKey, socket.id);
}

function memberSocketIds(room) {
  return room ? [...room.members.values()].map(member => member.socketId) : [];
}

function validRoomId(roomId) {
  return typeof roomId === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(roomId);
}

io.on('connection', socket => {
  const { participantToken, instanceId } = socket.handshake.auth || {};
  const hasIdentity = typeof participantToken === 'string' &&
    /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test(participantToken) &&
    typeof instanceId === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(instanceId);
  const participantKey = hasIdentity ? `participant:${participantToken}` : `socket:${socket.id}`;
  if (socket.recovered && socket.data.roomId) {
    const member = rooms.get(socket.data.roomId)?.members.get(participantKey);
    if (member?.socketId === socket.id) {
      clearTimeout(member.expiry);
      member.expiry = null;
    } else {
      socket.leave(partyChannel(socket.data.roomId));
      delete socket.data.roomId;
    }
  }
  let joinAttempt = 0;

  socket.on('join_room', async (roomId, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    if (!validRoomId(roomId)) return respond({ error: 'Invalid room id.' });
    const attempt = ++joinAttempt;
    const saved = rooms.has(roomId) ? null : await readPlayback(roomId);
    if (!socket.connected || attempt !== joinAttempt) return respond({ error: 'Join superseded.' });

    let room = rooms.get(roomId);
    const previous = room?.members.get(participantKey);
    if (previous?.retiredInstances.has(instanceId)) {
      return respond({ code: 'PARTICIPANT_REPLACED', error: 'This party is already open in another tab.' });
    }
    if (room && !previous && room.members.size >= MAX_MEMBERS) {
      return respond({ error: 'Room is full! Maximum 5 members allowed.' });
    }
    if (socket.data.roomId !== roomId) leaveRoom(socket, participantKey);
    if (!room) {
      room = { members: new Map(), playback: saved };
      rooms.set(roomId, room);
    }

    const alreadyJoined = previous?.socketId === socket.id;
    const replacedId = previous && !alreadyJoined ? previous.socketId : null;
    clearTimeout(previous?.expiry);
    const retiredInstances = previous?.retiredInstances || new Set();
    if (replacedId && previous.instanceId !== instanceId) {
      retiredInstances.add(previous.instanceId);
      if (retiredInstances.size > 32) retiredInstances.delete(retiredInstances.values().next().value);
    }
    room.members.set(participantKey, { socketId: socket.id, instanceId, retiredInstances, expiry: null });
    socket.join(partyChannel(roomId));
    socket.data.roomId = roomId;
    const existingUsers = memberSocketIds(room).filter(id => id !== socket.id);
    const totalMembers = room.members.size;
    if (replacedId) {
      const replaced = io.sockets.sockets.get(replacedId);
      if (replaced) {
        replaced.leave(partyChannel(roomId));
        delete replaced.data.roomId;
        replaced.emit('party_replaced', { roomId });
        replaced.disconnect();
      }
    }
    respond({
      success: true, roomId, totalMembers, existingUsers, resumed: alreadyJoined,
      recoveredState: room.playback,
      turn: getTurnCredentials(socket)
    });
    // A room with just its host also needs a packet offset to resume its session.
    socket.emit('room_ready', roomId);
    const recipients = socket.to(partyChannel(roomId));
    if (replacedId) {
      recipients.emit('user_left', { userId: replacedId, totalMembers, reconnecting: true });
    }
    if (!alreadyJoined) {
      recipients.emit('user_joined', { userId: socket.id, totalMembers, reconnecting: Boolean(replacedId) });
    }
  });

  socket.on('get_turn_credentials', callback => {
    if (typeof callback === 'function') callback(socket.data.roomId ? getTurnCredentials(socket) : null);
  });

  socket.on('lookup_room', (roomId, callback) => {
    if (typeof callback !== 'function') return;
    if (!validRoomId(roomId)) return callback({ error: 'Invalid room id.' });
    const room = rooms.get(roomId);
    const hostId = memberSocketIds(room).find(id => io.sockets.sockets.has(id));
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
    if (!memberSocketIds(room).includes(userId) || userId === socket.id || !payload.signal) return;
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
      if (memberSocketIds(rooms.get(socket.data.roomId)).includes(data.targetId)) {
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
    const member = rooms.get(roomId)?.members.get(participantKey);
    if (member?.socketId !== socket.id) return;
    if (!recoverableReasons.has(reason)) {
      removeMember(roomId, participantKey, socket.id);
      return;
    }
    member.expiry = setTimeout(() => removeMember(roomId, participantKey, socket.id), RECOVERY_MS);
    member.expiry.unref();
  });
});

const PORT = Number(process.env.PORT ?? 3000);
server.listen(PORT, () => console.log(`Server listening on port ${server.address().port}`));

process.once('SIGTERM', () => {
  io.close(() => redis?.disconnect());
});
