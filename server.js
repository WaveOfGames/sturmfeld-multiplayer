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
  const groups = new Map();
  for (const id of waiting) {
    const c = clients.get(id);
    if (!c) continue;
    const key = c.teamId || id;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(id);
  }
  const first = waiting.map(id => clients.get(id)).find(Boolean);
  const duelQueue = !!(first && first.mapIndex === 5);
  const picked = [];
  const pickedSet = new Set();
  for (const group of groups.values()) {
    const allDuel = group.length > 0 && group.every(id => clients.get(id)?.mapIndex === 5);
    if (duelQueue) {
      if (allDuel && group.length === 2) {
        picked.push(...group); group.forEach(id => pickedSet.add(id)); break;
      }
    } else if (!allDuel && picked.length + group.length <= MAX_PLAYERS) {
      picked.push(...group); group.forEach(id => pickedSet.add(id));
    }
  }
  waiting = waiting.filter(id => !pickedSet.has(id));
  if (!picked.length) {
    broadcastLobby();
    if (waiting.length) scheduleStart();
    return;
  }
  const isDuel = duelQueue;
  roundNumber += 1;
  const selectedMaps = [...new Set(picked.map(id => clients.get(id)?.mapIndex).filter(n => Number.isInteger(n) && n >= 0 && n <= 4))];
  const chosenMapIndex = isDuel ? 5 : (selectedMaps.length ? selectedMaps[randomInt(0, selectedMaps.length)] : 0);
  currentMatch = {
    id: randomUUID(), round: roundNumber, seed: randomInt(1, 0x7fffffff),
    mapIndex: chosenMapIndex, duel: isDuel, players: picked, ended: new Set(),
    openedChests: new Set(), takenPickups: new Set(), aliveBots: isDuel ? 0 : null,
    lastBotSnapshot: null, lastStormState: null, winnerTeamId: null, startedAt: Date.now()
  };
  for (const id of picked) {
    const c = clients.get(id);
    c.status = 'playing'; c.matchId = currentMatch.id; c.lastState = null; c.lastHitBy = null;
  }
  const roster = picked.map(id => ({ id, name: clients.get(id)?.name || 'Spieler', teamId: isDuel ? id : (clients.get(id)?.teamId || id) }));
  sendToIds(picked, 'matchStart', {
    matchId: currentMatch.id, round: currentMatch.round, players: roster,
    humanPlayers: picked.length, botsToFill: isDuel ? 0 : MAX_PLAYERS - picked.length,
    seed: currentMatch.seed, mapIndex: currentMatch.mapIndex, duel: isDuel
  });
  broadcastLobby();
  if (waiting.length) scheduleStart();
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
function cleanLoot(input) {
  if (!Array.isArray(input)) return [];
  const allowed = new Set(['weapon','ammo','medkit','shield','speed']);
  return input.slice(0, 24).filter(x => x && allowed.has(x[0])).map(x => {
    const d = x[1] && typeof x[1] === 'object' ? x[1] : {};
    if (x[0] === 'weapon') return ['weapon', { w: ['pistol','smg','ar','shotgun','sniper'].includes(d.w) ? d.w : 'pistol', r: Math.max(0, Math.min(5, Math.floor(Number(d.r)||0))) }];
    if (x[0] === 'ammo') return ['ammo', { t: ['light','shell','heavy'].includes(d.t) ? d.t : 'light', n: Math.max(1, Math.min(100, Math.floor(Number(d.n)||1))) }];
    return [x[0], {}];
  });
}
function evaluateWinner() {
  if (!currentMatch || currentMatch.winnerTeamId || currentMatch.aliveBots !== 0) return;
  if (currentMatch.duel && currentMatch.players.some(id => { const c = clients.get(id); return !c || !c.lastState; })) return;
  const aliveTeams = new Set();
  for (const id of currentMatch.players) {
    const c = clients.get(id);
    if (c && c.lastState && c.lastState.alive) aliveTeams.add(currentMatch.duel ? id : (c.teamId || id));
  }
  if (aliveTeams.size === 1) {
    const teamId = [...aliveTeams][0];
    currentMatch.winnerTeamId = teamId;
    sendToIds(currentMatch.players, 'teamWon', { teamId });
  }
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
    const wasHost = currentMatch.players[0] === id;
    currentMatch.players = currentMatch.players.filter(x => x !== id);
    if (wasHost && currentMatch.players.length) {
      // Der nächste Spieler übernimmt Bot-KI, Sturm und Snapshot-Synchronisierung.
      sendToIds(currentMatch.players, 'hostChanged', { hostId: currentMatch.players[0] });
    }
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

    if (msg.type === 'teamJoin' || msg.type === 'join') {
      const name = String(msg.name || client.name || 'Spieler').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 14);
      client.name = name || 'Spieler';
      client.look = Math.max(0, Math.min(30, Math.floor(Number(msg.look) || 0)));
      client.mapIndex = Math.max(0, Math.min(5, Math.floor(Number(msg.mapIndex) || 0)));
      const requestedTeam = String(msg.teamCode || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      if (client.mapIndex === 5 && !requestedTeam) { send(ws, 'teamError', { message: 'Die 1v1-Arena ist nur mit Team-Code spielbar.' }); return; }
      if (requestedTeam) {
        const members = [...clients.values()].filter(c => c.id !== id && c.teamId === requestedTeam && c.status !== 'new');
        const teamLimit = client.mapIndex === 5 ? 2 : 4;
        if (members.length >= teamLimit) {
          send(ws, 'teamError', { message: client.mapIndex === 5 ? 'Die 1v1-Arena erlaubt maximal zwei Spieler pro Team-Code.' : 'Dieses Team ist bereits voll (maximal 4 Spieler).' });
          return;
        }
        client.teamId = requestedTeam;
      } else {
        client.teamId = id; // Ohne Code spielt man in einem eigenen Team.
      }
      if (msg.type === 'join' && (client.status === 'new' || client.status === 'teamLobby')) queueClient(id);
      else if (msg.type === 'teamJoin' && client.status === 'new') client.status = 'teamLobby';
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
      state.teamId = currentMatch.duel ? id : (client.teamId || id);
      const wasAlive = !client.lastState || client.lastState.alive !== false;
      client.lastState = state;
      if (wasAlive && !state.alive) {
        if (currentMatch.duel && client.lastHitBy && client.lastHitBy !== id) { const killer = clients.get(client.lastHitBy); if (killer && killer.matchId === currentMatch.id) send(killer.ws, 'killAward', { killerId: killer.id, victimId: id, points: 1 }); }
        for (const mate of clients.values()) {
          if (mate.id !== id && mate.teamId === client.teamId && mate.matchId === currentMatch.id) {
            send(mate.ws, 'teamMateDead', { id, name: client.name });
          }
        }
        broadcastTeamRoster(client.teamId);
      }
      evaluateWinner();
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
      if (!currentMatch.duel && targetClient && targetClient.teamId && targetClient.teamId === client.teamId) return; // Kein Friendly Fire außerhalb des Duells
      const now = Date.now();
      if (now - client.hitWindow > 1000) { client.hitWindow = now; client.hitCount = 0; }
      if (++client.hitCount > 40) return;
      const amount = Math.max(1, Math.min(100, Math.round(Number(msg.amount) || 1)));
      const target = clients.get(targetId);
      if (target) { target.lastHitBy = id; send(target.ws, 'playerHit', { targetId, fromId: id, fromName: client.name, amount, head: !!msg.head }); }
      return;
    }

    if (msg.type === 'killAward') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] !== id) return;
      const killerId = String(msg.killerId || '');
      if (!currentMatch.players.includes(killerId) || killerId === id) return;
      const killer = clients.get(killerId);
      if (killer) send(killer.ws, 'killAward', { killerId });
      return;
    }

    if (msg.type === 'chestOpen') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const chestId = Math.max(0, Math.min(500, Math.floor(Number(msg.chestId))));
      if (currentMatch.openedChests.has(chestId)) return;
      currentMatch.openedChests.add(chestId);
      const loot = cleanLoot(msg.loot);
      const pickupPrefix = 'chest-' + chestId;
      sendToIds(currentMatch.players, 'chestOpened', { chestId, loot, pickupPrefix });
      return;
    }

    if (msg.type === 'lootSpawn') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] !== id) return;
      const x = Number(msg.x), z = Number(msg.z), rot = Number(msg.rot);
      if (![x,z,rot].every(Number.isFinite)) return;
      const loot = cleanLoot(msg.loot);
      const spawnId = randomUUID();
      sendToIds(currentMatch.players, 'lootSpawn', { x: Math.max(-1000,Math.min(1000,x)), z: Math.max(-1000,Math.min(1000,z)), rot, loot, boss: !!msg.boss, spawnId });
      return;
    }

    if (msg.type === 'teamMarkerSet') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const x = Number(msg.x), z = Number(msg.z);
      if (!Number.isFinite(x) || !Number.isFinite(z)) return;
      const marker = { ownerId: id, name: client.name, x: Math.max(-10000,Math.min(10000,x)), z: Math.max(-10000,Math.min(10000,z)) };
      for (const mateId of currentMatch.players) {
        const mate = clients.get(mateId);
        if (mate && mateId !== id && mate.teamId === client.teamId) send(mate.ws, 'teamMarkerSet', marker);
      }
      return;
    }

    if (msg.type === 'teamMarkerRemove') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      for (const mateId of currentMatch.players) {
        const mate = clients.get(mateId);
        if (mate && mateId !== id && mate.teamId === client.teamId) send(mate.ws, 'teamMarkerRemove', { ownerId: id });
      }
      return;
    }

    if (msg.type === 'pickupTake') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const pickupId = String(msg.pickupId || '').slice(0, 100);
      if (!pickupId || currentMatch.takenPickups.has(pickupId)) return;
      currentMatch.takenPickups.add(pickupId);
      sendToIds(currentMatch.players, 'pickupTaken', { pickupId, playerId: id });
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
      currentMatch.lastStormState = storm;
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
      currentMatch.aliveBots = bots.filter(b => b.alive).length;
      currentMatch.lastBotSnapshot = { bots };
      sendToIds(currentMatch.players.filter(x => x !== id), 'botSnapshot', { bots });
      evaluateWinner();
      return;
    }

    if (msg.type === 'botHit') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id || currentMatch.players[0] === id) return;
      const host = clients.get(currentMatch.players[0]);
      if (host) send(host.ws, 'botHit', {
        fromId: id,
        botIndex: Math.max(0, Math.min(50, Math.floor(Number(msg.botIndex) || 0))),
        amount: Math.max(1, Math.min(100, Math.round(Number(msg.amount) || 1))),
        head: !!msg.head,
        fromName: client.name,
        teamId: client.teamId || id
      });
      return;
    }

    if (msg.type === 'roundEnd') {
      if (!currentMatch || client.status !== 'playing' || client.matchId !== currentMatch.id) return;
      const winnerTeamId = String(msg.winnerTeamId || '');
      const expectedWinner = currentMatch.duel ? id : (client.teamId || id);
      if (winnerTeamId && winnerTeamId === expectedWinner && client.lastState && client.lastState.alive) {
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
      if (currentMatch.lastBotSnapshot) send(ws, 'botSnapshot', currentMatch.lastBotSnapshot);
      if (currentMatch.lastStormState) send(ws, 'stormState', { storm: currentMatch.lastStormState });
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
