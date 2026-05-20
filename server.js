// ============================================================
// server.js — Clash Arena Backend v6
// © Beauty Benedict. All rights reserved.
//
// FIXES:
//  1. agentStore keyed by agent.id — merges from all devices,
//     never overwrites. Same wallet on PC + phone = union of agents.
//  2. broadcastQueueCounts() uses io.emit() (ALL sockets).
//     Every open browser sees live counts the moment anyone joins.
//  3. Queue counts pushed immediately on new connection.
//  4. Multiple simultaneous rooms per game type — 100 players
//     in "rps-2" automatically fills as many 1v1 rooms as needed.
//     No waiting for a previous game to finish.
// ============================================================

const { createServer } = require("http");
const { Server }       = require("socket.io");

const PORT     = process.env.PORT     || 3001;
const GROQ_KEY = process.env.GROQ_API_KEY;

if (!GROQ_KEY) console.warn("⚠️  GROQ_API_KEY not set — agents will use fallback logic");

const GAME_TYPES = {
  "rps":        { name: "Rock Paper Scissors", rounds: 3 },
  "quick-chat": { name: "Quick Chat Battle",   rounds: 3 },
  "rps-blitz":  { name: "Blitz RPS",           rounds: 5 },
};
const ROOM_SIZES   = [2, 5, 8];
const PRIZE_SPLITS = {
  2: [1.0],
  5: [0.70, 0.30],
  8: [0.60, 0.30, 0.10],
};

// ── State ─────────────────────────────────────────────────
const queues     = {};  // "rps-2": [player, ...]
const rooms      = {};  // roomId -> Room
const playerRoom = {};  // walletAddress -> roomId

// Agent store: wallet -> { agentId -> agent }
// Object map so merging from multiple devices is safe (no overwrite).
const agentStore = {};

let roomCounter = 0;
const makeRoomId  = () => `room-${++roomCounter}-${Date.now().toString(36)}`;
const qKey        = (gt, sz) => `${gt}-${sz}`;

// ── Broadcast queue counts to EVERY connected socket ──────
// This is what makes the waiting room show live counts on all devices.
function broadcastQueueCounts(io) {
  const counts = {};
  ROOM_SIZES.forEach(sz => {
    Object.keys(GAME_TYPES).forEach(gt => {
      counts[qKey(gt, sz)] = queues[qKey(gt, sz)]?.length ?? 0;
    });
  });
  io.emit("queue_counts", counts); // io.emit = ALL sockets, no room filter
}

// ── Agent helpers ─────────────────────────────────────────
function mergeAgents(wallet, incoming) {
  if (!agentStore[wallet]) agentStore[wallet] = {};
  if (!Array.isArray(incoming)) return;
  for (const a of incoming) {
    if (!a?.id) continue;
    const cur = agentStore[wallet][a.id];
    // Keep the copy with the most recent createdAt (handles edits)
    if (!cur || (a.createdAt ?? 0) >= (cur.createdAt ?? 0)) {
      agentStore[wallet][a.id] = a;
    }
  }
}
const agentList = (wallet) => Object.values(agentStore[wallet] ?? {});

// ── Groq AI ───────────────────────────────────────────────
const RPS_MOVES = ["rock", "paper", "scissors"];

async function askGroq(agent, opponents, round, totalRounds, gameType, history) {
  if (!GROQ_KEY) return fallback(agent, gameType);
  const isRPS = gameType !== "quick-chat";
  const sys = isRPS
    ? `You are AI agent "${agent.agentName}". Personality: "${agent.agentDescription}". Play Rock Paper Scissors. Respond ONLY valid JSON: {"move":"rock"|"paper"|"scissors","reasoning":"max 12 words in character"}`
    : `You are AI agent "${agent.agentName}". Personality: "${agent.agentDescription}". Quick-chat battle. Respond ONLY valid JSON: {"move":"chat","reasoning":"max 15 words in character"}`;
  const usr = `Round ${round}/${totalRounds}. Opponents: ${opponents.map(o => `${o.agentName}: "${o.agentDescription}"`).join(" | ")}. History: ${history.length ? history.map(r => `R${r.round}:${r.myMove},${r.myScore}pts`).join(",") : "first round"}.`;
  try {
    const r = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROQ_KEY}` },
      body: JSON.stringify({ model: "llama3-8b-8192", max_tokens: 120, temperature: 0.85, messages: [{ role: "system", content: sys }, { role: "user", content: usr }] }),
    });
    if (!r.ok) return fallback(agent, gameType);
    const d = await r.json();
    const p = JSON.parse(d.choices?.[0]?.message?.content?.replace(/```json|```/g, "").trim() ?? "{}");
    return { move: RPS_MOVES.includes(p.move) ? p.move : fallback(agent, gameType).move, reasoning: (p.reasoning ?? "").slice(0, 100) };
  } catch { return fallback(agent, gameType); }
}

function fallback(agent, gameType) {
  if (gameType === "quick-chat") {
    const lines = ["My words cut deeper than any blade.", "You cannot match my eloquence.", "I was born for this arena.", "Logic and wit — my weapons of choice."];
    return { move: "chat", reasoning: lines[Math.floor(Math.random() * lines.length)] };
  }
  const d = (agent.agentDescription || "").toLowerCase(), s = Math.random();
  let move;
  if (d.includes("aggressive") || d.includes("attack") || d.includes("strong")) move = s < 0.5 ? "rock" : s < 0.8 ? "scissors" : "paper";
  else if (d.includes("defend") || d.includes("safe") || d.includes("careful"))  move = s < 0.5 ? "paper" : s < 0.8 ? "rock" : "scissors";
  else if (d.includes("chaos") || d.includes("random") || d.includes("unpred"))  move = RPS_MOVES[Math.floor(Math.random() * 3)];
  else if (d.includes("smart") || d.includes("analyt") || d.includes("logic"))   move = s < 0.4 ? "paper" : s < 0.7 ? "scissors" : "rock";
  else move = RPS_MOVES[Math.floor(s * 3)];
  return { move, reasoning: "" };
}

function resolveRPS(a, b) {
  if (a === b) return 0;
  if ((a === "rock" && b === "scissors") || (a === "scissors" && b === "paper") || (a === "paper" && b === "rock")) return 1;
  return -1;
}

// ── Room ──────────────────────────────────────────────────
function createRoom(gameType, size, players) {
  const roomId = makeRoomId();
  const room = {
    roomId, gameType, size,
    players: players.map(p => ({ ...p, score: 0, roundWins: 0, roundHistory: [] })),
    phase: "battle", currentRound: 1,
    totalRounds: GAME_TYPES[gameType]?.rounds ?? 3,
    roundResults: [], finalResults: [],
    prizePool: players.length,
    startedAt: Date.now(), lastUpdated: Date.now(),
  };
  rooms[roomId] = room;
  players.forEach(p => { playerRoom[p.walletAddress] = roomId; });
  return room;
}

// ── Matchmaking — creates as many rooms as the queue allows ──
// If 100 players join rps-2, this fires 50 rooms simultaneously.
function tryMatch(io, gameType, size) {
  const key = qKey(gameType, size);
  if (!queues[key]) return;
  while (queues[key].length >= size) {
    const matched = queues[key].splice(0, size);
    const room    = createRoom(gameType, size, matched);
    console.log(`[match] ${gameType}x${size} → ${room.roomId} | ${matched.map(p => p.agentName).join(", ")}`);
    matched.forEach(p => {
      const sock = io.sockets.sockets.get(p.socketId);
      if (sock) sock.join(room.roomId);
    });
    io.to(room.roomId).emit("matched", { roomId: room.roomId, room, message: `${matched.length} agents matched! Battle begins in 2 seconds…` });
    broadcastQueueCounts(io); // update counts after splicing
    setTimeout(() => startRound(io, room.roomId), 2000);
  }
}

// ── Rounds ────────────────────────────────────────────────
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

  await Promise.all(players.map(async p => {
    const r = await askGroq(p, players.filter(x => x.walletAddress !== p.walletAddress), room.currentRound, room.totalRounds, room.gameType, p.roundHistory || []);
    moves[p.walletAddress]      = r.move;
    reasonings[p.walletAddress] = r.reasoning;
  }));

  players.forEach(p => {
    if (reasonings[p.walletAddress]) io.to(roomId).emit("game_event", { agentName: p.agentName, walletAddress: p.walletAddress, content: reasonings[p.walletAddress] });
  });

  const scores = {};
  players.forEach(p => { scores[p.walletAddress] = 0; });

  if (room.gameType === "quick-chat") {
    players.forEach(p => { scores[p.walletAddress] = p.agentDescription.length + (reasonings[p.walletAddress] ?? "").length * 2 + Math.floor(Math.random() * 30); });
    for (let i = 0; i < players.length; i++) for (let j = i + 1; j < players.length; j++) {
      const a = players[i], b = players[j];
      if      (scores[a.walletAddress] > scores[b.walletAddress]) scores[a.walletAddress] += 2;
      else if (scores[b.walletAddress] > scores[a.walletAddress]) scores[b.walletAddress] += 2;
      else { scores[a.walletAddress]++; scores[b.walletAddress]++; }
    }
  } else {
    for (let i = 0; i < players.length; i++) for (let j = i + 1; j < players.length; j++) {
      const a = players[i], b = players[j], r = resolveRPS(moves[a.walletAddress], moves[b.walletAddress]);
      if      (r  >  0) scores[a.walletAddress] += 2;
      else if (r  <  0) scores[b.walletAddress] += 2;
      else { scores[a.walletAddress]++; scores[b.walletAddress]++; }
    }
  }

  players.forEach(p => {
    p.score += scores[p.walletAddress];
    (p.roundHistory = p.roundHistory || []).push({ round: room.currentRound, myMove: moves[p.walletAddress], myScore: scores[p.walletAddress] });
  });

  const winner = [...players].sort((a, b) => scores[b.walletAddress] - scores[a.walletAddress])[0];
  if (winner) winner.roundWins = (winner.roundWins || 0) + 1;

  const result = {
    round: room.currentRound, moves, reasonings, roundScores: scores,
    roundWinner: winner?.agentName ?? "Draw",
    playerStandings: [...players].sort((a, b) => b.score - a.score).map(p => ({
      agentName: p.agentName, walletAddress: p.walletAddress, score: p.score,
      roundScore: scores[p.walletAddress], move: moves[p.walletAddress], reasoning: reasonings[p.walletAddress] ?? "",
    })),
  };

  room.roundResults.push(result);
  room.lastUpdated = Date.now();
  io.to(roomId).emit("round_result", result);
  console.log(`[round] ${roomId} R${room.currentRound} → ${winner?.agentName ?? "Draw"}`);

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
  const finalResults = sorted.map((p, i) => ({
    rank: i + 1, walletAddress: p.walletAddress, agentName: p.agentName,
    score: p.score, roundWins: p.roundWins || 0,
    prize:     i < splits.length ? `${splits[i] * 100}%` : "0%",
    prizeUSDC: i < splits.length ? (room.prizePool * splits[i]).toFixed(2) : "0",
  }));
  room.finalResults = finalResults; room.phase = "results"; room.lastUpdated = Date.now();

  // Update win/loss in agentStore so next sync reflects real stats
  finalResults.forEach(res => {
    const wallet = res.walletAddress;
    if (!agentStore[wallet]) return;
    const pd = room.players.find(p => p.walletAddress === wallet);
    if (!pd) return;
    const agent = Object.values(agentStore[wallet]).find(a => a.name === pd.agentName);
    if (!agent) return;
    if      (res.rank === 1) agent.wins   = (agent.wins   || 0) + 1;
    else if (res.rank  >  3) agent.losses = (agent.losses || 0) + 1;
    else                      agent.draws  = (agent.draws  || 0) + 1;
    agentStore[wallet][agent.id] = agent;
  });

  let victorySummary = "";
  try {
    const w = sorted[0];
    if (GROQ_KEY && w) {
      const r = await fetch("https://api.groq.com/openai/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROQ_KEY}` },
        body: JSON.stringify({ model: "llama3-8b-8192", max_tokens: 60, temperature: 0.9, messages: [{ role: "user", content: `You are ${w.agentName}, personality: "${w.agentDescription}". You just won a battle arena. Say one victory line, max 12 words, in character.` }] }),
      });
      victorySummary = (await r.json()).choices?.[0]?.message?.content?.trim() ?? "";
    }
  } catch { /* ignore */ }

  io.to(roomId).emit("game_over", {
    roomId, finalResults, prizePool: room.prizePool, totalRounds: room.totalRounds,
    roundResults: room.roundResults, victorySummary,
    winner1: sorted[0]?.walletAddress ?? null,
    winner2: sorted[1]?.walletAddress ?? null,
    winner3: sorted[2]?.walletAddress ?? null,
  });
  console.log(`[done] ${roomId} 🥇 ${finalResults[0]?.agentName}`);
  setTimeout(() => { delete rooms[roomId]; finalResults.forEach(r => delete playerRoom[r.walletAddress]); }, 600_000);
}

// ── HTTP health ───────────────────────────────────────────
const httpServer = createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "ok", version: "6.0",
    rooms: Object.keys(rooms).length,
    queues: Object.fromEntries(Object.entries(queues).map(([k, v]) => [k, v.length])),
    groq: GROQ_KEY ? "connected" : "missing",
  }));
});

// ── Socket.io ─────────────────────────────────────────────
const io = new Server(httpServer, { cors: { origin: "*", methods: ["GET", "POST"] } });

io.on("connection", (socket) => {
  console.log(`[+] ${socket.id}`);
  broadcastQueueCounts(io); // give this socket current counts immediately

  // ── AGENT SYNC ────────────────────────────────────────
  // Client sends its full localStorage agent list on wallet connect.
  // Server merges by agent.id — never overwrites, always union.
  // Replies with the merged authoritative list for this wallet.
  socket.on("sync_agents", ({ walletAddress, agents }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet) return;
    mergeAgents(wallet, agents);
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentList(wallet) });
    console.log(`[agents] synced ${agentList(wallet).length} for ${wallet}`);
  });

  socket.on("add_agent", ({ walletAddress, agent }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !agent?.id) return;
    if (!agentStore[wallet]) agentStore[wallet] = {};
    agentStore[wallet][agent.id] = agent;
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentList(wallet) });
    console.log(`[agents] added "${agent.name}" for ${wallet}`);
  });

  socket.on("delete_agent", ({ walletAddress, agentId }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !agentId) return;
    if (agentStore[wallet]) delete agentStore[wallet][agentId];
    socket.emit("agents_synced", { walletAddress: wallet, agents: agentList(wallet) });
  });

  // ── JOIN QUEUE ────────────────────────────────────────
  socket.on("join_queue", ({ gameType, size, walletAddress, agentName, agentDescription }) => {
    const wallet = walletAddress?.toLowerCase();
    if (!wallet || !gameType || !size) return;
    leaveEverything(socket, wallet, io);

    const key = qKey(gameType, size);
    if (!queues[key]) queues[key] = [];
    if (queues[key].find(p => p.walletAddress === wallet)) return; // already queued

    const player = { walletAddress: wallet, agentName, agentDescription, socketId: socket.id, score: 0, roundWins: 0, roundHistory: [] };
    queues[key].push(player);
    socket.data.queueKey = key;
    socket.data.wallet   = wallet;
    socket.join(key);

    const waiting = queues[key].length;
    socket.emit("queue_joined", {
      gameType, size, waiting,
      message: `${waiting} / ${size} in queue${waiting === 1 ? " — waiting for opponents…" : "…"}`,
    });

    broadcastQueueCounts(io); // tell EVERY socket the new count
    console.log(`[queue] ${agentName} → ${key} (${waiting}/${size})`);
    tryMatch(io, gameType, size);
  });

  socket.on("get_queue_status", ({ gameType, size }) => {
    socket.emit("queue_update", { waiting: queues[qKey(gameType, size)]?.length ?? 0, size });
  });

  socket.on("game_event", ({ roomId, event }) => { socket.to(roomId).emit("game_event", event); });

  socket.on("leave", ({ walletAddress }) => {
    leaveEverything(socket, walletAddress?.toLowerCase(), io);
    broadcastQueueCounts(io);
  });

  socket.on("rejoin_room", ({ roomId }) => {
    const room = rooms[roomId];
    if (!room) { socket.emit("room_not_found"); return; }
    socket.join(roomId); socket.emit("room_state", room);
  });

  socket.on("disconnect", () => {
    if (socket.data?.wallet) { leaveEverything(socket, socket.data.wallet, io); broadcastQueueCounts(io); }
    console.log(`[-] ${socket.id}`);
  });
});

function leaveEverything(socket, wallet, io) {
  if (!wallet) return;
  Object.keys(queues).forEach(key => {
    const before = queues[key].length;
    queues[key] = queues[key].filter(p => p.walletAddress !== wallet);
    if (queues[key].length < before) {
      io.to(key).emit("queue_update", { waiting: queues[key].length, size: parseInt(key.split("-").pop()) });
    }
  });
  socket.rooms.forEach(r => { if (r !== socket.id) socket.leave(r); });
  delete playerRoom[wallet];
}

httpServer.listen(PORT, () => {
  console.log(`✅ Clash Arena server v6 → port ${PORT}`);
  console.log(`   Game types : ${Object.keys(GAME_TYPES).join(", ")}`);
  console.log(`   Room sizes : ${ROOM_SIZES.join(", ")} players`);
  console.log(`   Groq AI    : ${GROQ_KEY ? "✅ connected" : "⚠️  missing key"}`);
});