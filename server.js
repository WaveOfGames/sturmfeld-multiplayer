'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID, randomInt } = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const MAX_PLAYERS = 50;
const COUNTDOWN_SECONDS = 20;
const clients = new Map();
let waiting = [];
let currentMatch = null;
let countdown = null;
let countdownEndsAt = 0;
let roundNumber = 0;

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, service: 'sturmfeld-multiplayer', connected: clients.size, waiting: waiting.length, inMatch: currentMatch ? currentMatch.players.length : 0 }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 });
server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/ws') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

function send(ws, type, data = {}) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type, ...data }));
}
function sendToIds(ids, type, data = {}) {
  for (const id of ids) {
    const c = clients.get(id);
    if (c) send(c.ws, type, data);
  }
}
function broadcastTeamRoster(teamId) {
  if (!teamId) return;
  const members = [...clients.values()]
    .filter(c => c.teamId === teamId && c.status !== 'new')
    .map(c => ({ id: c.id, name: c.name, look: c.look || 0, status: c.status, alive: c.lastState ? c.lastState.alive !== false : true }));
  for (const c of clients.values()) if (c.teamId === teamId) send(c.ws, 'teamRoster', { members });
}
function broadcastLobby() {
  const snapshot = {
    waiting: waiting.length,
    inMatch: currentMatch ? currentMatch.players.length : 0,
    maxPlayers: MAX_PLAYERS,
    round: currentMatch ? currentMatch.round : roundNumber,
    countdown: countdown ? Math.max(0, Math.ceil((countdownEndsAt - Date.now()) / 1000)) : null
  };
  for (const c of clients.values()) send(c.ws, 'lobby', snapshot);
}
function queueClient(id) {
  const c = clients.get(id);
  if (!c || waiting.includes(id)) return;
  c.status = 'waiting';
  waiting.push(id);
  send(c.ws, 'queued', { position: waiting.length });
  if (!currentMatch) scheduleStart();
  broadcastLobby();
}
function scheduleStart() {
  if (countdown || currentMatch || waiting.length === 0) return;
  countdownEndsAt = Date.now() + COUNTDOWN_SECONDS * 1000;
  countdown = setTimeout(startMatch, COUNTDOWN_SECONDS * 1000);
  const tick = setInterval(() => {
    if (!countdown) { clearInterval(tick); return; }
    broadcastLobby();
  }, 1000);
  broadcastLobby();
}
function startMatch() {
  if (countdown) clearTimeout(countdown);
  countdown = null;
  // Ganze Teams gemeinsam einteilen; ein Team wird niemals auf zwei Runden verteilt.
  const groups = new Map();
  for (const id of waiting) {
    const c = clients.get(id);
    if (!c) continue;
    const key = c.teamId || id;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(id);
  }
  const picked = [];
  const pickedSet = new Set();
  for (const group of groups.values()) {
    if (picked.length + group.length <= MAX_PLAYERS) {
      for (const id of group) { picked.push(id); pickedSet.add(id); }
    }
  }
  waiting = waiting.filter(id => !pickedSet.has(id));
  if (!picked.length) { broadcastLobby(); return; }
  roundNumber += 1;
  currentMatch = {
    id: randomUUID(),
    round: roundNumber,
    seed: randomInt(1, 0x7fffffff),
    mapIndex: clients.get(picked[0])?.mapIndex || 0,
    players: picked,
    ended: new Set(),
    startedAt: Date.now()
  };
  for (const id of picked) {
    const c = clients.get(id);
    c.status = 'playing';
    c.matchId = currentMatch.id;
    c.lastState = null;
  }
  const roster = picked.map(id => ({ id, name: clients.get(id)?.name || 'Spieler', teamId: clients.get(id)?.teamId || id }));
  sendToIds(picked, 'matchStart', {
    matchId: currentMatch.id,
    round: currentMatch.round,
    players: roster,
    humanPlayers: picked.length,
    botsToFill: MAX_PLAYERS - picked.length,
    seed: currentMatch.seed,
    mapIndex: currentMatch.mapIndex
  });
  // Spieler, die währenddessen warten, bleiben für die nächste Runde in der Warteschlange.
  broadcastLobby();
}
function finishMatch() {
  if (!currentMatch) return;
  const finished = currentMatch;
  sendToIds(finished.players, 'matchEnd', { matchId: finished.id, round: finished.round });
  // Nach der Runde bleiben Spieler im Ergebnisbildschirm. Sie treten erst auf
  // eigenen Wunsch wieder der Warteschlange bei.
  for (const id of finished.players) {
    const c = clients.get(id);
    if (c) { c.status = 'finished'; c.matchId = null; c.lastState = null; }
  }
  currentMatch = null;
  broadcastLobby();
}
function removeFromWaiting(id) {
  waiting = waiting.filter(x => x !== id);
}
function removeClient(id) {
  const c = clients.get(id);
  if (!c) return;
  removeFromWaiting(id);
  clients.delete(id);
  if (currentMatch && currentMatch.players.includes(id)) {
    currentMatch.players = currentMatch.players.filter(x => x !== id);
    sendToIds(currentMatch.players, 'playerLeft', { id, name: c.name, teamId: c.teamId || id });
    for (const mate of clients.values()) {
      if (mate.teamId === c.teamId && mate.matchId === currentMatch.id) send(mate.ws, 'teamMateLeft', { id, name: c.name });
    }
    if (currentMatch.players.length === 0 || currentMatch.ended.size >= currentMatch.players.length) {
      finishMatch();
    }
  }
  broadcastTeamRoster(c.teamId);
  broadcastLobby();
}

wss.on('connection', ws => {
  const id = randomUUID();
  const client = { id, ws, name: 'Spieler', status: 'new', matchId: null, lastState: null, mapIndex: 0, hitWindow: 0, hitCount: 0, lastStormAt: 0 };
  clients.set(id, client);
  send(ws, 'welcome', { id, maxPlayers: MAX_PLAYERS, countdownSeconds: COUNTDOWN_SECONDS });
  broadcastLobby();

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'requeue') {
      if (!currentMatch && client.status === 'finished') queueClient(id);
      return;
    }

    if (msg.type === 'join') {
      const name = String(msg.name || 'Spieler').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 14);
      client.name = name || 'Spieler';
      client.look = Math.max(0, Math.min(30, Math.floor(Number(msg.look) || 0)));
      const requestedTeam = String(msg.teamCode || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      if (requestedTeam) {
        const members = [...clients.values()].filter(c => c.id !== id && c.teamId === requestedTeam && c.status !== 'new');
        if (members.length >= 4) {
          send(ws, 'teamError', { message: 'Dieses Team ist bereits voll (maximal 4 Spieler).' });
          return;
        }
        client.teamId = requestedTeam;
      } else {
        client.teamId = id; // Ohne Code spielt man in einem eigenen Team.
      }
      client.mapIndex = Math.max(0, Math.min(4, Math.floor(Number(msg.mapIndex) || 0)));
      if (client.status === 'new') queueClient(id);
      broadcastTeamRoster(client.teamId);
      return;
    }

    if (msg.type === 'playerState') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const p = msg.state || {};
      const state = {};
      for (const k of ['x', 'y', 'z', 'yaw', 'pitch']) {
        const n = Number(p[k]);
        if (!Number.isFinite(n)) return;
        state[k] = Math.max(-10000, Math.min(10000, n));
      }
      state.phase = String(p.phase || 'ground').slice(0, 12);
      state.look = Math.max(0, Math.min(30, Math.floor(Number(p.look) || 0)));
      state.alive = p.alive !== false;
      state.hp = Math.max(0, Math.min(100, Number(p.hp) || 0));
      state.shield = Math.max(0, Math.min(100, Number(p.shield) || 0));
      state.name = client.name;
      state.look = client.look || 0;
      state.teamId = client.teamId || id;
      const wasAlive = !client.lastState || client.lastState.alive !== false;
      client.lastState = state;
      if (wasAlive && !state.alive) {
        for (const mate of clients.values()) {
          if (mate.id !== id && mate.teamId === client.teamId && mate.matchId === currentMatch.id) {
            send(mate.ws, 'teamMateDead', { id, name: client.name });
          }
        }
        broadcastTeamRoster(client.teamId);
      }
      for (const otherId of currentMatch.players) {
        if (otherId === id) continue;
        const other = clients.get(otherId);
        if (other) send(other.ws, 'playerState', { id, state });
      }
      return;
    }

    if (msg.type === 'playerHit') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const targetId = String(msg.targetId || '');
      if (!currentMatch.players.includes(targetId) || targetId === id) return;
      const targetClient = clients.get(targetId);
      if (targetClient && targetClient.teamId && targetClient.teamId === client.teamId) return; // Kein Friendly Fire
      const now = Date.now();
      if (now - client.hitWindow > 1000) { client.hitWindow = now; client.hitCount = 0; }
      if (++client.hitCount > 40) return;
      const amount = Math.max(1, Math.min(100, Math.round(Number(msg.amount) || 1)));
      const target = clients.get(targetId);
      if (target) send(target.ws, 'playerHit', { targetId, fromId: id, fromName: client.name, amount, head: !!msg.head });
      return;
    }

    if (msg.type === 'stormState') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] !== id) return;
      const now = Date.now();
      if (now - client.lastStormAt < 100) return;
      client.lastStormAt = now;
      const p = msg.storm || {};
      const storm = {};
      for (const k of ['r','cx','cz','phase','t','r0','r1','tick']) {
        const n = Number(p[k]);
        if (!Number.isFinite(n)) return;
        storm[k] = Math.max(-10000, Math.min(10000, n));
      }
      storm.state = p.state === 'shrink' ? 'shrink' : 'wait';
      storm.done = !!p.done;
      for (const k of ['c0','c1']) {
        if (!p[k] || !Number.isFinite(Number(p[k].x)) || !Number.isFinite(Number(p[k].z))) return;
        storm[k] = { x: Number(p[k].x), z: Number(p[k].z) };
      }
      sendToIds(currentMatch.players.filter(x => x !== id), 'stormState', { storm });
      return;
    }

    if (msg.type === 'botSnapshot') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] !== id) return;
      const bots = Array.isArray(msg.bots) ? msg.bots.slice(0, 50).map(b => {
        const q = {};
        for (const k of ['i','x','y','z','yaw','hp','shield','mag']) {
          const n = Number(b[k]);
          if (!Number.isFinite(n)) return null;
          q[k] = Math.max(-10000, Math.min(10000, n));
        }
        q.alive = !!b.alive;
        return q;
      }).filter(Boolean) : [];
      sendToIds(currentMatch.players.filter(x => x !== id), 'botSnapshot', { bots });
      return;
    }

    if (msg.type === 'botHit') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] === id) return;
      const host = clients.get(currentMatch.players[0]);
      if (host) send(host.ws, 'botHit', {
        fromId: id,
        botIndex: Math.max(0, Math.min(50, Math.floor(Number(msg.botIndex) || 0))),
        amount: Math.max(1, Math.min(100, Math.round(Number(msg.amount) || 1))),
        head: !!msg.head
      });
      return;
    }

    if (msg.type === 'roundEnd') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const winnerTeamId = String(msg.winnerTeamId || '');
      if (winnerTeamId && winnerTeamId === client.teamId && client.lastState && client.lastState.alive) {
        sendToIds(currentMatch.players, 'teamWon', { teamId: winnerTeamId });
      }
      currentMatch.ended.add(id);
      send(ws, 'roundEndReceived', { waitingFor: currentMatch.players.length - currentMatch.ended.size });
      if (currentMatch.ended.size >= currentMatch.players.length) finishMatch();
      return;
    }

    if (msg.type === 'requestSnapshot') {
      if (!currentMatch || client.status !== 'playing') return;
      const snapshot = [];
      for (const otherId of currentMatch.players) {
        if (otherId === id) continue;
        const other = clients.get(otherId);
        if (other && other.lastState) snapshot.push({ id: otherId, state: other.lastState });
      }
      send(ws, 'snapshot', { players: snapshot });
    }
  });

  ws.on('close', () => removeClient(id));
  ws.on('error', () => removeClient(id));
});

// Heartbeat: erkennt getrennte Browser-Verbindungen zuverlässig.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);
wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Sturmfeld Multiplayer läuft auf Port ${PORT}`);
});
server.on('close', () => clearInterval(heartbeat));
