# Sturmfeld – Multiplayer-Server (erster Schritt)

Dieser Server ist die Online-Grundlage für Sturmfeld:

- WebSocket-Verbindungen für mehrere Browser
- Warteschlange für Spieler, die während einer laufenden Runde dazukommen
- maximal 50 echte Spieler pro Runde
- 20-Sekunden-Countdown vor dem Rundenstart
- Meldung, wie viele Bot-Plätze bis 50 frei bleiben
- Weiterleitung von Spielerpositionen an die anderen Spieler
- Health-Check unter `/health`

**Wichtig:** Das ist zunächst der Server-Unterbau. Dein vorhandenes HTML-Spiel ist noch nicht mit ihm verbunden. Auch Bot-KI, Treffer, Loot, Sturm und Rundenende sind damit noch nicht gemeinsam synchronisiert. Das kommt in den nächsten Integrationsschritten; dieser Server allein macht das Spiel noch nicht zu einem fertigen Multiplayer-Spiel.

## Lokal starten (nur zum Testen)

```bash
npm install
npm start
```

Danach ist der Status unter `http://localhost:10000/health` erreichbar und WebSocket unter `ws://localhost:10000/ws`.

## Online bereitstellen

Der Server benötigt Node.js und WebSocket-Unterstützung. Für den ersten Test kann ein Render-Web-Service verwendet werden:

- Build command: `npm install`
- Start command: `npm start`
- Node-Version: mindestens 20
- WebSocket-Adresse später: `wss://DEIN-SERVICE.onrender.com/ws`

Auf dem kostenlosen Render-Tarif kann ein Dienst nach 15 Minuten ohne eingehenden Verkehr schlafen und beim nächsten Zugriff ungefähr eine Minute zum Aufwachen brauchen. Für einen zuverlässigen Spielbetrieb ist der kostenlose Tarif deshalb nur zum Testen gedacht.
