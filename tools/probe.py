#!/usr/bin/env python3
"""
Phase-1-Machbarkeitsnachweis fuer eine Python-Laufzeit in ioBroker.

Zweck: beantwortet die eine Frage, bei der ein Irrtum alles Weitere entwertet --
verhaelt sich die Redis-Wire-Ebene aus Python heraus so wie im Konzept beschrieben?

Das Skript tut zweierlei:

  1. CAPABILITY-PROBE  -- was kann die DB dieser Installation wirklich?
     Protokollversion, unterstuetzte Kommandos, Keyspace-Notifications, Sets.
     Das Ergebnis ist die Kompatibilitaetsmatrix, gegen die das SDK gebaut wird.

  2. ROUND-TRIP        -- Objekt anlegen, State schreiben, Aenderung empfangen.
     Danach steht das Objekt im Admin-Objektbrowser unter "pyprobe.0".

Benutzung:
    pip install redis
    python probe_iobroker.py                    # Config automatisch suchen
    python probe_iobroker.py --config PFAD      # iobroker.json explizit
    python probe_iobroker.py --cleanup          # Testobjekte wieder entfernen

Schreibt ausschliesslich unterhalb des Namespace "pyprobe.0" und laesst
alles andere unangetastet.
"""

import argparse
import json
import os
import sys
import time

import warnings

try:
    import redis
except ImportError:
    sys.exit("Fehlt: pip install redis")

warnings.filterwarnings("ignore", category=DeprecationWarning, module="redis")


def _lower_cmd(args):
    """Setzt den Kommandonamen (args[0]) auf Kleinschreibung."""
    if args and isinstance(args[0], (str, bytes)):
        head = args[0].decode() if isinstance(args[0], bytes) else args[0]
        return (head.lower(),) + tuple(args[1:])
    return tuple(args)


class _LowercasePacker:
    """Haengt sich vor den Command-Packer von redis-py."""

    def __init__(self, inner):
        self._inner = inner

    def pack(self, *args):
        return self._inner.pack(*_lower_cmd(args))

    def pack_commands(self, commands):
        return self._inner.pack_commands([_lower_cmd(c) for c in commands])

    def __getattr__(self, name):
        return getattr(self._inner, name)


class IoBrokerConnection(redis.connection.Connection):
    """Verbindung, die zum eingebauten ioBroker-Redis-Server passt.

    Der Server in js-controller dispatcht Kommandos ohne toLowerCase()
    (db-base/redisHandler.js:139), registriert seine Handler aber ausschliesslich
    kleingeschrieben. ioredis sendet zufaellig klein, redis-py sendet gross --
    deshalb scheitert sonst schon das erste GET mit "GET NOT SUPPORTED".

    Am Draht nachgewiesen:  GET -> "-Error GET NOT SUPPORTED"  /  get -> "4"

    Der Packer ist der richtige Hebel: er liegt unter send_command UND unter
    den Pipelines, greift also auch fuer MULTI/EXEC und Pub/Sub.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._command_packer = _LowercasePacker(self._command_packer)


NS = "pyprobe.0"
FROM = f"system.adapter.{NS}"

CONFIG_CANDIDATES = [
    "/opt/iobroker/iobroker-data/iobroker.json",
    "C:/pWork/iobroker-data/iobroker.json",
    "./iobroker-data/iobroker.json",
    "../iobroker-data/iobroker.json",
]

OK = "  OK   "
NO = " FEHLT "
WARN = " WARN  "


def find_config(explicit):
    if explicit:
        if not os.path.isfile(explicit):
            sys.exit(f"Keine Datei: {explicit}")
        return explicit
    for c in CONFIG_CANDIDATES:
        if os.path.isfile(c):
            return c
    sys.exit("iobroker.json nicht gefunden -- Pfad mit --config angeben.")


def connect(section, cfg_path):
    """Baut einen Redis-Client aus dem states-/objects-Abschnitt der iobroker.json."""
    with open(cfg_path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    part = cfg[section]
    opts = part.get("options") or {}
    pool = redis.ConnectionPool(
        connection_class=IoBrokerConnection,
        host=part.get("host", "127.0.0.1"),
        port=int(part.get("port", 9000 if section == "states" else 9001)),
        db=int(opts.get("db") or 0),
        password=opts.get("auth_pass") or None,
        decode_responses=True,
        socket_timeout=5,
        # Der eingebaute Server kennt weder HELLO (RESP3-Handshake) noch
        # CLIENT SETINFO. Beides muss aus bleiben, sonst scheitert schon
        # der Verbindungsaufbau mit "HELLO NOT SUPPORTED".
        protocol=2,
        lib_name=None,
        lib_version=None,
    )
    return redis.Redis(connection_pool=pool), part


def probe_command(label, fn):
    """Fuehrt ein Kommando aus und meldet, ob der Server es beherrscht."""
    try:
        fn()
        print(f"[{OK}] {label}")
        return True
    except redis.exceptions.ResponseError as exc:
        print(f"[{NO}] {label}  ->  {exc}")
        return False
    except Exception as exc:  # noqa: BLE001 - hier ist genau das gewollt
        print(f"[{WARN}] {label}  ->  {type(exc).__name__}: {exc}")
        return False


def section(title):
    print(f"\n--- {title} " + "-" * max(0, 56 - len(title)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="Pfad zur iobroker.json")
    ap.add_argument("--cleanup", action="store_true", help="Testobjekte entfernen und beenden")
    ap.add_argument("--listen", type=float, default=3.0, help="Sekunden auf Events warten")
    args = ap.parse_args()

    cfg_path = find_config(args.config)
    print(f"Config: {cfg_path}")

    states, s_cfg = connect("states", cfg_path)
    objects, o_cfg = connect("objects", cfg_path)
    print(f"States : {s_cfg.get('type')} @ {s_cfg.get('host')}:{s_cfg.get('port')}")
    print(f"Objects: {o_cfg.get('type')} @ {o_cfg.get('host')}:{o_cfg.get('port')}")

    obj_id = f"cfg.o.{NS}.temperature"
    state_id = f"io.{NS}.temperature"

    if args.cleanup:
        objects.delete(obj_id)
        objects.publish(obj_id, "null")
        states.delete(state_id)
        states.publish(state_id, "null")
        try:
            objects.srem("cfg.s.object.type.state", obj_id)
        except redis.exceptions.ResponseError:
            pass
        print("\nAufgeraeumt.")
        return 0

    # ------------------------------------------------------------------
    section("Verbindung und Protokollversion")
    # ------------------------------------------------------------------
    ok = True
    for name, client in (("states", states), ("objects", objects)):
        # Kein PING: der eingebaute Server implementiert es nicht.
        # Der Protokollversions-Schluessel ist gleichzeitig der Verbindungstest.
        try:
            ver = client.get(f"meta.{name}.protocolVersion")
        except Exception as exc:  # noqa: BLE001
            print(f"[{NO}] {name}: keine Verbindung -> {exc}")
            return 2
        if ver == "4":
            print(f"[{OK}] {name}: Protokollversion {ver}")
        else:
            print(f"[{WARN}] {name}: Protokollversion {ver!r} -- erwartet '4'")
            ok = False

    # ------------------------------------------------------------------
    section("Kommandoumfang States-DB")
    # ------------------------------------------------------------------
    probe_command("SET / GET", lambda: states.set("io.__probe__", "1") and states.get("io.__probe__"))
    probe_command("SETEX (expire)", lambda: states.setex("io.__probe_ex__", 60, "1"))
    probe_command("MGET", lambda: states.mget(["io.__probe__"]))
    probe_command("KEYS", lambda: states.keys("io.__probe*"))
    has_multi = probe_command(
        "MULTI/EXEC (set + publish in einem Roundtrip)",
        lambda: states.pipeline(transaction=True).set("io.__probe__", "1").publish("io.__probe__", "1").execute(),
    )
    has_scan = probe_command("SCAN", lambda: states.scan(cursor=0, match="io.__probe*", count=10))
    has_notify = probe_command(
        "CONFIG SET notify-keyspace-events (fuer abgelaufene States)",
        lambda: states.config_set("notify-keyspace-events", "Ex"),
    )
    states.delete("io.__probe__", "io.__probe_ex__")

    # ------------------------------------------------------------------
    section("Kommandoumfang Objects-DB")
    # ------------------------------------------------------------------
    has_obj_scan = probe_command("SCAN", lambda: objects.scan(cursor=0, match="cfg.o.system.adapter.*", count=10))
    has_sets = probe_command("SSCAN auf cfg.s.object.type.state", lambda: objects.sscan("cfg.s.object.type.state", 0, count=5))
    has_eval = probe_command("EVAL (Lua, fuer getObjectView)", lambda: objects.eval("return 1", 0))
    use_sets_raw = objects.get("meta.objects.features.useSets")
    use_sets = bool(int(use_sets_raw or "0"))
    print(f"[{OK}] meta.objects.features.useSets = {use_sets_raw!r}  ->  Index-Sets {'aktiv' if use_sets else 'inaktiv'}")

    # ------------------------------------------------------------------
    section("Round-Trip: Objekt anlegen")
    # ------------------------------------------------------------------
    now = int(time.time() * 1000)
    obj = {
        "_id": f"{NS}.temperature",
        "type": "state",
        "common": {
            "name": "Probe-Temperatur",
            "type": "number",
            "role": "value.temperature",
            "unit": "\u00b0C",
            "read": True,
            "write": False,
        },
        "native": {},
        "from": FROM,
        "ts": now,
    }
    obj_json = json.dumps(obj)
    objects.set(obj_id, obj_json)
    objects.publish(obj_id, obj_json)
    if use_sets and has_sets:
        objects.sadd("cfg.s.object.type.state", obj_id)
    print(f"[{OK}] {obj_id} geschrieben -- im Admin unter '{NS}.temperature' sichtbar")

    # ------------------------------------------------------------------
    section("Round-Trip: State schreiben und Aenderung empfangen")
    # ------------------------------------------------------------------
    sub = states.pubsub()
    sub.psubscribe(f"io.{NS}.*")
    # Die Bestaetigung der Subscription abholen, damit wir nichts verpassen
    deadline = time.time() + 2
    while time.time() < deadline:
        if sub.get_message(timeout=0.2) is not None:
            break

    value = 21.5
    now = int(time.time() * 1000)
    state = {"val": value, "ack": True, "ts": now, "lc": now, "q": 0, "from": FROM}
    state_json = json.dumps(state)

    if has_multi:
        pipe = states.pipeline(transaction=True)
        pipe.set(state_id, state_json)
        pipe.publish(state_id, state_json)
        pipe.execute()
        print(f"[{OK}] State via MULTI geschrieben: {value} \u00b0C")
    else:
        states.set(state_id, state_json)
        states.publish(state_id, state_json)
        print(f"[{WARN}] State ohne MULTI geschrieben (zwei Roundtrips)")

    readback = states.get(state_id)
    print(f"[{OK}] Rueckgelesen: {readback}")

    received = None
    deadline = time.time() + args.listen
    while time.time() < deadline:
        msg = sub.get_message(timeout=0.3)
        if msg and msg.get("type") == "pmessage":
            received = msg
            break
    if received:
        print(f"[{OK}] Event empfangen auf '{received['channel']}': {received['data']}")
    else:
        print(f"[{NO}] Kein Event innerhalb von {args.listen}s -- PSUBSCRIBE pruefen")
    sub.close()

    # ------------------------------------------------------------------
    section("Ergebnis")
    # ------------------------------------------------------------------
    print(f"States  : MULTI={'ja' if has_multi else 'NEIN'}  SCAN={'ja' if has_scan else 'nein'}  "
          f"Keyspace-Events={'ja' if has_notify else 'nein'}")
    print(f"Objects : SCAN={'ja' if has_obj_scan else 'nein'}  Sets={'ja' if has_sets else 'nein'}  "
          f"EVAL={'ja' if has_eval else 'nein'}")
    print()
    if not has_notify:
        print("  -> Keyspace-Events fehlen: abgelaufene States kommen NICHT als Event.")
        print("     Das SDK braucht dafuer einen Fallback (Ablaufzeit clientseitig mitfuehren).")
    if not has_scan:
        print("  -> States-DB ohne SCAN: das SDK muss mit KEYS arbeiten. Bei echtem Redis")
        print("     ist KEYS blockierend -- also je nach Servertyp umschalten.")
    if not has_eval or not has_sets:
        print("  -> getObjectView kann nicht ueber Lua laufen; clientseitig filtern.")
    print(f"\nAufraeumen mit: python {os.path.basename(__file__)} --cleanup")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
