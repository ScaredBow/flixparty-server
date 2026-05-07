// ─── FlixParty Sync Server ────────────────────────────────────────
// Lightweight WebSocket server for real-time video sync and chat.
// Deploy to Railway, Render, Fly.io, or any Node.js host.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ── HTTP server (health checks + upgrade to WS) ──
const httpServer = http.createServer((req, res) => {
  // Health check endpoint for Railway / uptime monitors
  if (req.url === '/health' || req.url === '/') {
    const roomCount = rooms.size;
    let totalMembers = 0;
    for (const [, room] of rooms) {
      totalMembers += room.members.size;
    }
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({
      status: 'ok',
      uptime: process.uptime(),
      rooms: roomCount,
      members: totalMembers
    }));
    return;
  }
  res.writeHead(404);
  res.end();
});

// ── WebSocket server ──
const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomCode, { members: Map<ws, { nickname, joinedAt }> }>
const rooms = new Map();

// Clean up stale rooms every 5 minutes
setInterval(() => {
  for (const [code, room] of rooms) {
    // Remove dead connections
    for (const [ws] of room.members) {
      if (ws.readyState !== 1) {
        room.members.delete(ws);
      }
    }
    // Delete empty rooms
    if (room.members.size === 0) {
      rooms.delete(code);
      console.log(`[cleanup] Room ${code} removed`);
    }
  }
}, 5 * 60 * 1000);

wss.on('connection', (ws, req) => {
  let currentRoom = null;
  let currentNickname = 'Anonymous';

  // Keepalive ping every 30s (prevents Railway from killing idle connections)
  const keepalive = setInterval(() => {
    if (ws.readyState === 1) ws.ping();
  }, 30000);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {

      // ── Join Room ──
      case 'join': {
        const code = (msg.room || '').toUpperCase();
        const nick = msg.nickname || 'Anonymous';
        if (!code) return;

        // Leave previous room if any
        leaveRoom(ws, currentRoom, currentNickname);

        currentRoom = code;
        currentNickname = nick;

        if (!rooms.has(code)) {
          rooms.set(code, { members: new Map() });
        }
        const room = rooms.get(code);
        room.members.set(ws, { nickname: nick, joinedAt: Date.now() });

        console.log(`[${code}] ${nick} joined (${room.members.size} members)`);

        // Notify others
        broadcast(code, { type: 'joined', nickname: nick }, ws);
        broadcastMembers(code);

        // Ask existing member for video state so new joiner can sync
        for (const [peer] of room.members) {
          if (peer !== ws && peer.readyState === 1) {
            peer.send(JSON.stringify({ type: 'state_request' }));
            break;
          }
        }
        break;
      }

      // ── Video sync events ──
      case 'play':
      case 'pause':
      case 'seek':
      case 'sync': {
        if (!currentRoom) return;
        broadcast(currentRoom, msg, ws);
        break;
      }

      // ── State response (host → new joiner) ──
      case 'state_response': {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        if (!room) return;
        // Send to most recently joined member
        let newest = null;
        let newestTime = 0;
        for (const [peer, info] of room.members) {
          if (peer !== ws && info.joinedAt > newestTime) {
            newest = peer;
            newestTime = info.joinedAt;
          }
        }
        if (newest && newest.readyState === 1) {
          newest.send(JSON.stringify(msg));
        }
        break;
      }

      // ── Chat ──
      case 'chat': {
        if (!currentRoom) return;
        broadcast(currentRoom, {
          type: 'chat',
          nickname: currentNickname,
          text: (msg.text || '').slice(0, 500),
          color: msg.color
        }, ws);
        break;
      }
    }
  });

  ws.on('close', () => {
    clearInterval(keepalive);
    leaveRoom(ws, currentRoom, currentNickname);
  });

  ws.on('error', () => {
    clearInterval(keepalive);
    leaveRoom(ws, currentRoom, currentNickname);
  });
});

// ── Helpers ──

function leaveRoom(ws, roomCode, nickname) {
  if (!roomCode) return;
  const room = rooms.get(roomCode);
  if (!room) return;

  room.members.delete(ws);
  console.log(`[${roomCode}] ${nickname} left (${room.members.size} members)`);

  broadcast(roomCode, { type: 'left', nickname }, ws);
  broadcastMembers(roomCode);

  if (room.members.size === 0) {
    rooms.delete(roomCode);
    console.log(`[${roomCode}] Room deleted (empty)`);
  }
}

function broadcast(roomCode, msg, excludeWs) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const payload = JSON.stringify(msg);
  for (const [peer] of room.members) {
    if (peer !== excludeWs && peer.readyState === 1) {
      peer.send(payload);
    }
  }
}

function broadcastMembers(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const memberList = [];
  for (const [, info] of room.members) {
    memberList.push(info.nickname);
  }
  const payload = JSON.stringify({ type: 'members', members: memberList });
  for (const [peer] of room.members) {
    if (peer.readyState === 1) {
      peer.send(payload);
    }
  }
}

// ── Start ──
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`🎬 FlixParty server listening on port ${PORT}`);
});
