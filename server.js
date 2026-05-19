// ============================================================
// server.js — Clash Arena Backend v5
// © Beauty Benedict. All rights reserved.
// FIXES v5:
//   1. Agent store synced per wallet — cross-device support
//   2. Queue counts broadcast to ALL sockets on every change
// ============================================================

const { createServer } = require("http");
const { Server }       = require("socket.io");

const PORT     = process.env.PORT     || 3001;
const GROQ_KEY = process.env.GROQ_API_KEY;

if (!GROQ_KEY) {
  console.warn("⚠️  GROQ_API_KEY not set — agents will use fallback logic");
}

const GAME_TYPES = {
  "rps":        { name: "Rock Paper Scissors", rounds: 3 },
  "quick-chat": { name: "Quick Chat Battle",   rounds: 3 },
  "rps-blitz":  { name: "Blitz RPS",           rounds: 5 },
};

const ROOM_SIZES = [2, 5, 8];

const PRIZE_SPLITS = {
  2: [1.0],
  5: [0.70, 0.30],
  8: [0.60, 0.30, 0.10],
};

const queues      = {};
const rooms       = {};
const playerRoom  = {};
const agentStore  = {};  // walletAddress -> UserAgent[]

let roomCounter = 0;

function makeRoomId() {
  return `room-${++roomCounter}-${Date.now().toString(36)}`;
}

function getQueueKey(gameType, size) {
  return `${gameType}-${size}`;
}

function broadcastQueueCounts(io) {
  const counts = {};
  ROOM_SIZES.forEach(size => {
    Object.keys(GAME_TYPES).forEach(gameType => {
      const key = getQueueKey(gameType, size);
      counts[key] = queues[key]?.length ?? 0;
    });
  });
  io.emit("queue_counts", counts);
}

const RPS_MOVES = ["rock", "paper", "scissors"];

async function askGroqForMove(agent, opponents, round, totalRounds, gameType, roundHistory) {
  if (!GROQ_KEY) return fallbackMove(agent, gameType);

  const opponentList = opponents
    .map(o => `- ${o.agentName}: "${o.agentDescription}"`)
    .join("\n");

  const historyText = roundHistory.length > 0
    ? roundHistory.map(r =>
        `Round ${r.round}: You played ${r.myMove || "?"}, result: ${r.myScore > 0 ? "scored " + r.myScore + " pts" : "0 pts"}`
      ).join("\n")
    : "This is the first round.";

  const isRPS = gameType !== "quick-chat";

  const systemPrompt = isRPS
    ? `You are an AI agent named "${agent.agentName}" in a competitive arena game called Clash.
Your personality: "${agent.agentDescription}"
You are playing Rock Paper Scissors against other AI agents.
You must pick ONE move: rock, paper, or scissors.
Respond ONLY with valid JSON: {"move": "rock"|"paper"|"scissors", "reasoning": "one short sentence (max 12 words) in character"}`
    : `You are an AI agent named "${agent.agentName}" in a quick-chat battle.
Your personality: "${agent.agentDescription}"
Respond ONLY with valid JSON: {"move": "chat", "reasoning": "one punchy in-character statement (max 15 words)"}`;

  const userPrompt = `Round ${round} of ${totalRounds}.\nOpponents:\n${opponentList}\nHistory:\n${historyText}\nChoose your move.`;

  try {
    const response = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROQ_KEY}` },
      body: JSON.stringify({
        model: "llama3-8b-8192", max_tokens: 120, temperature: 0.85,
        messages: [{ role: "system", content: systemPrompt }, { role: "user", content: userPrompt }],
      }),
    });
    if (!response.ok) { console.error(`[groq] HTTP ${response.status}`); return fallbackMove(agent, gameType); }
    const data    = await response.json();
    const raw     = data.choices?.[0]?.message?.content ?? "";
    const cleaned = raw.replace(/```json|```/g, "").trim();
    const parsed  = JSON.parse(cleaned);
    const move      = RPS_MOVES.includes(parsed.move) ? parsed.move : fallbackMove(agent, gameType).move;
    const reasoning = (parsed.reasoning ?? "").slice(0, 100);
    return { move, reasoning };
  } catch (err) {
    console.error("[groq] parse error:", err.message);
    return fallbackMove(agent, gameType);
  }
}

function fallbackMove(agent, gameType) {
  if (gameType === "quick-chat") {
    const lines = ["My words cut deeper than any blade.", "You cannot match my eloquence.", "I was born for this arena.", "Logic and wit — my weapons of choice.", "Your personality is no match for mine."];
    return { move: "chat", reasoning: lines[Math.floor(Math.random() * lines.length)] };
  }
  const desc = (agent.agentDescription || "").toLowerCase();
  const seed = Math.random();
  let move;
  if (desc.includes("aggressive") || desc.includes("attack") || desc.includes("strong")) {
    move = seed < 0.5 ? "rock" : seed < 0.8 ? "scissors" : "paper";
  } else if (desc.includes("defend") || desc.includes("safe") || desc.includes("careful")) {
    move = seed < 0.5 ? "paper" : seed < 0.8 ? "rock" : "scissors";
  } else if (desc.includes("chaos") || desc.includes("random") || desc.includes("unpredictable")) {
    move = RPS_MOVES[Math.floor(Math.random() * 3)];
  } else if (desc.includes("smart") || desc.includes("analyt") || desc.includes("logic")) {
    move = seed < 0.4 ? "paper" : seed < 0.7 ? "scissors" : "rock";
  } else {
    move = RPS_MOVES[Math.floor(seed * 3)];
  }
  return { move, reasoning: "" };
}

function resolveRPS(a, b) {
  if (a === b) return 0;
  if ((a === "rock" && b === "scissors") || (a === "scissors" && b === "paper") || (a === "paper" && b === "rock")) return 1;
  return -1;
}

function createRoom(gameType, size, players) {
  const roomId = makeRoomId();
  const room = {
    roomId, gameType, size,
    players: players.map(p => ({ ...p, score: 0, roundWins: 0, roundHistory: [] })),
    phase: "battle", currentRound: 1,
    totalRounds: GAME_TYPES[gameType]?.rounds ?? 3,
    roundResults: [], finalResults: [],
    prizePool: players.length * 1,
    startedAt: Date.now(), lastUpdated: Date.now(),
  };
  rooms[roomId] = room;
  players.forEach(p => { playerRoom[p.walletAddress] = roomId; });
  return room;
}

function tryMatch(io, gameType, size) {
  const key = getQueueKey(gameType, size);
  if (!queues[key]) return;
  while (queues[key].length >= size) {
    const matched = queues[key].splice(0, size);
    const room    = createRoom(gameType, size, matched);
    console.log(`[match] ${gameType}-${size} → ${room.roomId} | ${matched.map(p => p.agentName).join(", ")}`);
    matched.forEach(p => {
      const socket = io.sockets.sockets.get(p.socketId);
      if (socket) socket.join(room.roomId);
    });
    io.to(room.roomId).emit("matched", { roomId: room.roomId, room, message: `${matched.length} agents matched. Battle begins in 2 seconds…` });
    broadcastQueueCounts(io);
    setTimeout(() => startRound(io, room.roomId), 2000);
  }
}

async function startRound(io, roomId) {
  const room = rooms[roomId];
  if (!room) return;
  io.to(roomId).emit("round_start", { round: room.currentRound, totalRounds: room.totalRounds, message: `Round ${room.currentRound} of ${room.totalRounds} — Agents are thinking…` });
  await resolveRound(io, roomId);
}

async function resolveRound(io, roomId) {
  const room = rooms[roomId];
  if (!room) return;
  const players = room.players;
  const moves = {}, reasonings = {};

  await Promise.all(players.map(async (player) => {
    const opponents = players.filter(p => p.walletAddress !== player.walletAddress);
    const result    = await askGroqForMove(player, opponents, room.currentRound, room.totalRounds, room.gameType, player.roundHistory || []);
    moves[player.walletAddress]      = result.move;
    reasonings[player.walletAddress] = result.reasoning;
  }));

  players.forEach(player => {
    const reasoning = reasonings[player.walletAddress];
    if (reasoning) io.to(roomId).emit("game_event", { agentName: player.agentName, walletAddress: player.walletAddress, content: reasoning });
  });

  const roundScores = {};
  players.forEach(p => { roundScores[p.walletAddress] = 0; });

  if (room.gameType === "quick-chat") {
    players.forEach(p => {
      roundScores[p.walletAddress] = p.agentDescription.length + (reasonings[p.walletAddress] ?? "").length * 2 + Math.floor(Math.random() * 30);
    });
    for (let i = 0; i < players.length; i++) {
      for (let j = i + 1; j < players.length; j++) {
        const a = players[i], b = players[j];
        if (roundScores[a.walletAddress] > roundScores[b.walletAddress])      roundScores[a.walletAddress] += 2;
        else if (roundScores[b.walletAddress] > roundScores[a.walletAddress]) roundScores[b.walletAddress] += 2;
        else { roundScores[a.walletAddress] += 1; roundScores[b.walletAddress] += 1; }
      }
    }
  } else {
    for (let i = 0; i < players.length; i++) {
      for (let j = i + 1; j < players.length; j++) {
        const a = players[i], b = players[j];
        const result = resolveRPS(moves[a.walletAddress], moves[b.walletAddress]);
        if (result === 1)       roundScores[a.walletAddress] += 2;
        else if (result === -1) roundScores[b.walletAddress] += 2;
        else { roundScores[a.walletAddress] += 1; roundScores[b.walletAddress] += 1; }
      }
    }
  }

  players.forEach(p => {
    p.score += roundScores[p.walletAddress];
    p.roundHistory = p.roundHistory || [];
    p.roundHistory.push({ round: room.currentRound, myMove: moves[p.walletAddress], myScore: roundScores[p.walletAddress] });
  });

  const roundWinnerPlayer = [...players].sort((a, b) => roundScores[b.walletAddress] - roundScores[a.walletAddress])[0];
  if (roundWinnerPlayer) roundWinnerPlayer.roundWins = (roundWinnerPlayer.roundWins || 0) + 1;

  const roundResult = {
    round: room.currentRound, moves, reasonings, roundScores,
    roundWinner: roundWinnerPlayer?.agentName ?? "Draw",
    playerStandings: [...players].sort((a, b) => b.score - a.score).map(p => ({
      agentName: p.agentName, walletAddress: p.walletAddress, score: p.score,
      roundScore: roundScores[p.walletAddress], move: moves[p.walletAddress], reasoning: reasonings[p.walletAddress] ?? "",
    })),
  };

  room.roundResults.push(roundResult);
  room.lastUpdated = Date.now();
  io.to(roomId).emit("round_result", roundResult);
  console.log(`[round] ${roomId} R${room.currentRound} → ${roundWinnerPlayer?.agentName ?? "Draw"}`);

  if (room.currentRound >= room.totalRounds) {
    setTimeout(() => finishGame(io, roomId), 2000);
  } else {
    room.currentRound++;
    room.lastUpdated = Date.now();
    setTimeout(() => startRound(io, roomId), 3000);
  }
}

async function finishGame(io, roomId) {
  const room = rooms[roomId];
  if (!room) return;
  const sorted = [...room.players].sort((a, b) => b.score - a.score);
  const splits = PRIZE_SPLITS[room.size] ?? PRIZE_SPLITS[8];

  const finalResults = sorted.map((p, idx) => ({
    rank: idx + 1, walletAddress: p.walletAddress, agentName: p.agentName,
    score: p.score, roundWins: p.roundWins || 0,
    prize:     idx < splits.length ? `${splits[idx] * 100}%` : "0%",
    prizeUSDC: idx < splits.length ? (room.prizePool * splits[idx]).toFixed(2) : "0",
  }));

  room.finalResults = finalResults;
  room.phase        = "results";
  room.lastUpdated  = Date.now();

  // Update win/loss in agentStore
  finalResults.forEach(result => {
    const wallet = result.walletAddress;
    if (!agentStore[wallet]) return;
    const playerData = room.players.find(p => p.walletAddress === wallet);
    if (!playerData) return;
    const agentIdx = agentStore[wallet].findIndex(a => a.name === playerData.agentName);
    if (agentIdx === -1) return;
    const agent = agentStore[wallet][agentIdx];
    if (result.rank === 1) agent.wins = (agent.wins || 0) + 1;
    else if (result.rank > 3) agent.losses = (agent.losses || 0) + 1;
    else agent.draws = (agent.draws || 0) + 1;
    agentStore[wallet][agentIdx] = agent;
  });

  let victorySummary = "";
  try {
    const winner = sorted[0];
    if (GROQ_KEY && winner) {
      const res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROQ_KEY}` },
        body: JSON.stringify({ model: "llama3-8b-8192", max_tokens: 60, temperature: 0.9, messages: [{ role: "user", content: `You are ${winner.agentName}, personality: "${winner.agentDescription}". You just won a battle arena. Say one short victory line, max 12 words, in character.` }] }),
      });
      const d = await res.json();
      victorySummary = d.choices?.[0]?.message?.content?.trim() ?? "";
    }
  } catch { /* ignore */ }

  io.to(roomId).emit("game_over", {
    roomId, finalResults, prizePool: room.prizePool, totalRounds: room.totalRounds,
    roundResults: room.roundResults, victorySummary,
    winner1: sorted[0]?.walletAddress ?? null, winner2: sorted[1]?.walletAddress ?? null, winner3: sorted[2]?.walletAddress ?? null,
  });

  console.log(`[done] ${roomId} 🥇 ${finalResults[0]?.agentName} — ${finalResults[0]?.prizeUSDC} USDC`);

  setTimeout(() => {
    delete rooms[roomId];
    finalResults.forEach(r => delete playerRoom[r.walletAddress]);
    console.log(`[cleanup] ${roomId} removed`);
  }, 600_000);
}

const httpServer = createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: "ok", version: "5.0", network: "ARC Testnet", rooms: Object.keys(rooms).length, queues: Object.fromEntries(Object.entries(queues).map(([k, v]) => [k, v.length])), groq: GROQ_KEY ? "connected" : "missing" }));
});

const io = new Server(httpServer, { cors: { origin: "*", methods: ["GET", "POST"] } });

io.on("connection", (socket) => {
  console.log(`[+] socket ${socket.id}`);
  // Push current counts to this new socket immediately
  broadcastQueueCounts(io);

  // ── Agent sync ─────────────────────────────────────────
  // Client calls this right after wallet connects, passing its localStorage agents.
  // Server merges: if server has more recent/more agents, those win.
  // Server always replies with the authoritative list.
  socket.on("sync_agents", ({ walletAddress, agents: clientAgents }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet) return;
    const serverAgents = agentStore[wallet] || [];
    if (serverAgents.length === 0 && Array.isArray(clientAgents) && clientAgents.length > 0) {
      // First device to connect — seed server from client's localStorage
      agentStore[wallet] = clientAgents;
    }
    // Always send back the server's list (may be richer from other devices)
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentStore[wallet] || [] });
    console.log(`[agents] synced ${(agentStore[wallet] || []).length} for ${wallet}`);
  });

  socket.on("add_agent", ({ walletAddress, agent }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !agent) return;
    if (!agentStore[wallet]) agentStore[wallet] = [];
    if (!agentStore[wallet].find(a => a.id === agent.id)) agentStore[wallet].push(agent);
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentStore[wallet] });
    console.log(`[agents] added "${agent.name}" for ${wallet}`);
  });

  socket.on("delete_agent", ({ walletAddress, agentId }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !agentId) return;
    if (agentStore[wallet]) agentStore[wallet] = agentStore[wallet].filter(a => a.id !== agentId);
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentStore[wallet] ?? [] });
  });

  // ── Join queue ─────────────────────────────────────────
  socket.on("join_queue", ({ gameType, size, walletAddress, agentName, agentDescription }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !gameType || !size) return;
    leaveEverything(socket, wallet, io);
    const key = getQueueKey(gameType, size);
    if (!queues[key]) queues[key] = [];
    if (queues[key].find(p => p.walletAddress === wallet)) return;
    const player = { walletAddress: wallet, agentName, agentDescription, socketId: socket.id, score: 0, roundWins: 0, roundHistory: [] };
    queues[key].push(player);
    socket.data.queueKey = key;
    socket.data.wallet   = wallet;
    socket.join(key);
    const waiting = queues[key].length;
    socket.emit("queue_joined", { gameType, size, waiting, message: waiting === 1 ? `Waiting for ${size - 1} more player${size - 1 > 1 ? "s" : ""}…` : `${waiting} / ${size} players in queue…` });
    broadcastQueueCounts(io);
    console.log(`[queue] ${agentName} → ${key} (${waiting}/${size})`);
    tryMatch(io, gameType, size);
  });

  socket.on("get_queue_status", ({ gameType, size }) => {
    const key = getQueueKey(gameType, size);
    socket.emit("queue_update", { waiting: queues[key]?.length ?? 0, size });
  });

  socket.on("game_event", ({ roomId, event }) => { socket.to(roomId).emit("game_event", event); });

  socket.on("leave", ({ walletAddress }) => {
    leaveEverything(socket, walletAddress?.toLowerCase(), io);
    broadcastQueueCounts(io);
  });

  socket.on("rejoin_room", ({ roomId, walletAddress }) => {
    const room = rooms[roomId];
    if (!room) { socket.emit("room_not_found"); return; }
    socket.join(roomId);
    socket.emit("room_state", room);
  });

  socket.on("disconnect", () => {
    if (socket.data?.wallet) { leaveEverything(socket, socket.data.wallet, io); broadcastQueueCounts(io); }
    console.log(`[-] socket ${socket.id}`);
  });
});

function leaveEverything(socket, wallet, io) {
  if (!wallet) return;
  Object.keys(queues).forEach(key => {
    const before = queues[key].length;
    queues[key] = queues[key].filter(p => p.walletAddress !== wallet);
    if (queues[key].length < before) {
      const size = parseInt(key.split("-").pop());
      io.to(key).emit("queue_update", { waiting: queues[key].length, size });
    }
  });
  socket.rooms.forEach(r => { if (r !== socket.id) socket.leave(r); });
  delete playerRoom[wallet];
}

httpServer.listen(PORT, () => {
  console.log(`✅ Clash Arena server v5 → port ${PORT}`);
  console.log(`   Network    : ARC Testnet (Chain 5042002)`);
  console.log(`   Game types : ${Object.keys(GAME_TYPES).join(", ")}`);
  console.log(`   Room sizes : ${ROOM_SIZES.join(", ")} players`);
  console.log(`   Groq AI    : ${GROQ_KEY ? "✅ connected" : "⚠️  missing key"}`);
});