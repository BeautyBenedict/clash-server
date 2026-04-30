// ============================================================
// server.js — Clash Arena real-time backend
// Run with: node server.js
// Keeps shared game sessions in memory.
// Both browsers connect here via Socket.io for instant sync.
// ============================================================

const { createServer } = require("http");
const { Server }       = require("socket.io");

const PORT = 3001;

// ── In-memory game sessions ───────────────────────────────
// { gameId: { gameId, gameType, players[], phase, prizePool, results[], lastUpdated } }
const sessions = {};

function getSession(gameId, gameType) {
  if (!sessions[gameId] || sessions[gameId].phase === "results") {
    sessions[gameId] = {
      gameId,
      gameType: gameType || "rps",
      players: [],
      phase: "lobby",
      results: [],
      prizePool: 0,
      lastUpdated: Date.now(),
    };
  }
  return sessions[gameId];
}

// ── HTTP server + Socket.io ───────────────────────────────
const httpServer = createServer((req, res) => {
  // Simple health check endpoint
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", sessions: Object.keys(sessions).length }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, {
  cors: {
    origin: "*", // allow Next.js dev server on port 3000
    methods: ["GET", "POST"],
  },
});

io.on("connection", (socket) => {
  console.log(`[+] Client connected: ${socket.id}`);

  // ── Player joins a game ───────────────────────────────
  socket.on("join_game", ({ gameId, gameType, walletAddress, agentName, agentDescription, axlPubKey }) => {
    const session = getSession(gameId, gameType);

    // Add player if not already in
    const already = session.players.find(p => p.walletAddress === walletAddress.toLowerCase());
    if (!already) {
      session.players.push({
        walletAddress: walletAddress.toLowerCase(),
        agentId: `agent-${Date.now()}`,
        agentName,
        agentDescription,
        axlPubKey: axlPubKey || null,
        score: 0,
      });
      session.prizePool += 0.001;
      session.lastUpdated = Date.now();
      console.log(`[join] ${agentName} (${walletAddress.slice(0,8)}) → ${gameId} | players: ${session.players.length}`);
    }

    // Put this socket in the game's room
    socket.join(gameId);

    // Send the full current session back to everyone in this game
    io.to(gameId).emit("session_update", session);
  });

  // ── Player leaves a game ──────────────────────────────
  socket.on("leave_game", ({ gameId, walletAddress }) => {
    const session = sessions[gameId];
    if (session && session.phase !== "results") {
      session.players = session.players.filter(p => p.walletAddress !== walletAddress.toLowerCase());
      session.prizePool = Math.max(0, session.prizePool - 0.001);
      session.lastUpdated = Date.now();
      socket.leave(gameId);
      io.to(gameId).emit("session_update", session);
      console.log(`[leave] ${walletAddress.slice(0,8)} left ${gameId} | players: ${session.players.length}`);
    }
  });

  // ── Chat / move message broadcast ────────────────────
  socket.on("game_event", ({ gameId, event }) => {
    // Broadcast to everyone else in the room
    socket.to(gameId).emit("game_event", event);
  });

  // ── Battle results ────────────────────────────────────
  socket.on("battle_results", ({ gameId, results, players }) => {
    const session = sessions[gameId];
    if (session) {
      session.phase    = "results";
      session.results  = results;
      session.players  = players; // has scores + moves
      session.lastUpdated = Date.now();
      io.to(gameId).emit("session_update", session);
      console.log(`[results] ${gameId} finished. Winner: ${results[0]?.agentName}`);
    }
  });

  // ── Request current session state ────────────────────
  socket.on("get_session", ({ gameId, gameType }) => {
    const session = getSession(gameId, gameType);
    socket.join(gameId);
    socket.emit("session_update", session);
  });

  // ── Reset a finished game ─────────────────────────────
  socket.on("reset_game", ({ gameId, gameType }) => {
    sessions[gameId] = {
      gameId,
      gameType: gameType || "rps",
      players: [],
      phase: "lobby",
      results: [],
      prizePool: 0,
      lastUpdated: Date.now(),
    };
    io.to(gameId).emit("session_update", sessions[gameId]);
    console.log(`[reset] ${gameId}`);
  });

  socket.on("disconnect", () => {
    console.log(`[-] Client disconnected: ${socket.id}`);
  });
});

httpServer.listen(PORT, () => {
  console.log(`\n✅ Clash Arena server running on http://localhost:${PORT}`);
  console.log(`   Health check: http://localhost:${PORT}/health`);
  console.log(`   Waiting for players...\n`);
});