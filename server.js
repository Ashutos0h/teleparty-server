const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

// Room state
const rooms = {};
const MAX_MEMBERS = 5;

// Health check — required by Render to verify the server is alive
app.get('/', (req, res) => {
  res.status(200).json({ status: 'ok', service: 'Teleparty Clone Server', activeRooms: Object.keys(rooms).length });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on('join_room', (roomId, callback) => {
    if (!rooms[roomId]) {
      rooms[roomId] = { users: new Set() };
    }

    const room = rooms[roomId];

    if (room.users.size >= MAX_MEMBERS) {
      console.log(`User ${socket.id} blocked. Room ${roomId} is full.`);
      if (callback) callback({ error: 'Room is full! Maximum 5 members allowed.' });
      return;
    }

    socket.join(roomId);

    // Get list of existing users before adding self, so the new user knows who to call
    const existingUsers = Array.from(room.users);

    room.users.add(socket.id);
    socket.roomId = roomId;

    console.log(`User ${socket.id} joined room ${roomId}. Total members: ${room.users.size}`);

    // Notify existing users that someone new joined
    socket.to(roomId).emit('user_joined', { userId: socket.id, totalMembers: room.users.size });

    if (callback) callback({ success: true, roomId, totalMembers: room.users.size, existingUsers });
  });

  // WebRTC Signaling
  socket.on('webrtc_signal', (payload) => {
    io.to(payload.targetUserId).emit('webrtc_signal', {
      callerId: socket.id,
      signal: payload.signal
    });
  });

  // Video sync events
  socket.on('video_play', (data) => {
    if (socket.roomId) socket.to(socket.roomId).emit('video_play', data);
  });

  socket.on('video_pause', (data) => {
    if (socket.roomId) socket.to(socket.roomId).emit('video_pause', data);
  });

  socket.on('video_seek', (data) => {
    if (socket.roomId) socket.to(socket.roomId).emit('video_seek', data);
  });

  // Text Chat
  socket.on('chat_message', (data) => {
    if (socket.roomId) {
      socket.to(socket.roomId).emit('chat_message', {
        ...data,
        userId: socket.id
      });
    }
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    if (socket.roomId && rooms[socket.roomId]) {
      rooms[socket.roomId].users.delete(socket.id);
      socket.to(socket.roomId).emit('user_left', { userId: socket.id, totalMembers: rooms[socket.roomId].users.size });

      if (rooms[socket.roomId].users.size === 0) {
        delete rooms[socket.roomId];
        console.log(`Room ${socket.roomId} deleted.`);
      }
    }
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Teleparty Clone Server running on port ${PORT}`);
});
