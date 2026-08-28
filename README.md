# iobroker-python

Python-SDK für ioBroker-Adapter. Spricht direkt das Redis-Wire-Protokoll der
States- und Objects-Datenbank — ein Python-Prozess wird damit zum gleichrangigen
Adapter neben jedem Node-Adapter, ohne Brücke und ohne Umweg.

> **Status: 0.1.0, früher Entwurf.** Die Wire-Ebene ist gegen eine laufende
> Installation verifiziert (js-controller 7.2.3, `jsonl`-Datenbanken). Die API
> kann sich noch ändern.

## Installation

```bash
pip install iobroker
```

## Ein Adapter in dreißig Zeilen

```python
from iobroker import Adapter, State

class MyAdapter(Adapter):
    async def on_ready(self):
        await self.set_object_not_exists("temperature", {
            "type": "state",
            "common": {
                "name": "Temperatur", "type": "number",
                "role": "value.temperature", "unit": "°C",
                "read": True, "write": False,
            },
        })
        await self.subscribe_states("*")
        await self.set_state("info.connection", True, ack=True)

    async def on_state_change(self, id: str, state: State | None):
        # ack=False heißt: jemand will etwas schalten.
        if state and not state.ack:
            self.log.info(f"Befehl auf {id}: {state.val}")

    async def on_message(self, msg):
        if msg.command == "ping":
            await self.reply(msg, {"pong": True})

MyAdapter("myadapter").run()
```

Ein lauffähiges Beispiel steht in [`examples/minimal_adapter.py`](examples/minimal_adapter.py).

## Verbindungsdaten

Der Adapter liest sie in dieser Reihenfolge:

1. Umgebungsvariablen `IOB_STATES_HOST/PORT/DB/PASS/TYPE` und `IOB_OBJECTS_*`
   — so wird der `py-controller` sie später durchreichen.
2. `IOB_CONFIG` mit dem Pfad zur `iobroker.json`.
3. Die üblichen Installationspfade.

Instanznummer und Loglevel kommen aus `--instance` / `--loglevel` oder aus
`IOB_INSTANCE` / `IOB_LOGLEVEL` — dieselben Argumente, die js-controller heute
schon an Node-Adapter übergibt.

## Was der eingebaute Server anders macht als Redis

Im Standard-Setup redet man nicht mit echtem Redis, sondern mit dem
Redis-Protokollserver in js-controller (Ports 9000 und 9001). Der weicht an
mehreren Stellen ab. Alle folgenden Punkte sind an einer laufenden Installation
am Draht nachgewiesen, nicht aus der Dokumentation abgeleitet — `tools/probe.py`
prüft sie für die eigene Installation nach.

| Abweichung | Auswirkung | Behandlung im SDK |
|---|---|---|
| **Kommandos müssen kleingeschrieben sein.** Der Server dispatcht ohne `toLowerCase()` (`db-base/redisHandler.js`), registriert seine Handler aber nur klein. ioredis sendet zufällig klein, redis-py sendet groß. | `GET …` → `-Error GET NOT SUPPORTED`, `get …` → `4`. Ohne Behandlung scheitert der erste Befehl. | `connection.py` hängt sich vor den Command-Packer von redis-py. Synchron über `_command_packer`, asynchron über `pack_command` — redis-py benutzt je nach Modus einen anderen Weg. |
| **Kein `HELLO`.** redis-py verhandelt RESP3 beim Verbinden. | Verbindungsaufbau scheitert mit `HELLO NOT SUPPORTED`. | `protocol=2`, dazu `lib_name=None` gegen `CLIENT SETINFO`. |
| **Kein `PING`** auf der States-DB. | Übliche Verbindungstests schlagen fehl. | Verbindungstest über `get meta.states.protocolVersion` — die Version muss ohnehin geprüft werden. |
| **Kein `SCAN`** auf der States-DB. Die Objects-DB kann `scan`, `sscan`, `sadd`, `eval`. | Schlüssel müssen mit `keys` gesucht werden. | `DbConfig.is_builtin` unterscheidet; gegen echtes Redis ist `keys` blockierend und muss vermieden werden. |
| **Pub/Sub liefert den Kanal ohne `io.`-Präfix.** Echtes Redis liefert ihn mit. | Wer stur das Präfix abschneidet, verstümmelt IDs. | Das SDK toleriert beides — genau wie der JS-Client. |
| **Abgelaufene States melden sich anders.** Kein `__keyevent@0__:expired`; stattdessen kommt `null` auf dem State-Kanal selbst. | Gegen echtes Redis wäre ein zusätzliches Abo nötig. | `null` wird als „State weg“ an `on_state_change` gemeldet. |

Dazu eine Eigenschaft, die kein Fehler, aber wichtig ist: **die Rechteprüfung
sitzt im JS-Client, nicht im Datenbankserver.** Wer direkt auf der
Redis-Verbindung sitzt, hat faktisch Adminrechte. Das gilt für Node-Adapter
genauso — nur kommt bei Python fremder Code aus PyPI mit in den Prozess.

## Capability-Probe

```bash
python tools/probe.py
```

Meldet für die eigene Installation, was die beiden Datenbanken können, und macht
anschließend einen vollständigen Round-Trip: Objekt anlegen, State schreiben,
Änderung empfangen. Räumt mit `--cleanup` wieder auf.

## Lebenszyklus

`alive`, `connected`, `uptime` und `memRss` schreibt der Adapter selbst über die
States-DB — genauso wie ein Node-Adapter. Der Stopp läuft über den
`sigKill`-State: setzt der Controller ihn auf `-1`, beendet sich der Adapter
geordnet. Damit funktioniert das Anhalten auch unter Windows, wo es kein
`SIGTERM` gibt.

## Entwicklung

```bash
python -m venv .venv && .venv/Scripts/activate   # Windows
pip install -e ".[dev]"
python examples/minimal_adapter.py --instance 0
```

## Lizenz

MIT
