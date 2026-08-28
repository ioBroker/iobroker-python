#!/usr/bin/env python3
"""Capability probe for the ioBroker Redis wire layer.

Answers the one question a wrong assumption would invalidate everything else
on: does the wire layer behave from Python the way the SDK assumes?

The script does two things:

  1. CAPABILITY PROBE  -- what can this installation's databases actually do?
     Protocol version, supported commands, keyspace notifications, index sets.
     The result is the compatibility matrix the SDK is built against.

  2. ROUND TRIP        -- create an object, write a state, receive the change.
     Afterwards the object shows up in the admin object browser as "pyprobe.0".

Usage:
    pip install redis
    python tools/probe.py                  # locate the config automatically
    python tools/probe.py --config PATH    # point at iobroker.json explicitly
    python tools/probe.py --cleanup        # remove the test objects again

Writes exclusively below the "pyprobe.0" namespace and leaves everything else
untouched.

The connection quirks live in ``iobroker.connection`` rather than being
repeated here -- one place to fix when the server changes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import redis
except ImportError:
    sys.exit("Missing dependency: pip install redis")

from iobroker.connection import connect, find_config, load_db_config  # noqa: E402

NS = "pyprobe.0"
FROM = f"system.adapter.{NS}"

OK = "  OK   "
NO = " MISSING"
WARN = " WARN  "


def probe_command(label, fn) -> bool:
    """Run a command and report whether the server supports it."""
    try:
        fn()
        print(f"[{OK}] {label}")
        return True
    except redis.exceptions.ResponseError as exc:
        print(f"[{NO}] {label}  ->  {exc}")
        return False
    except Exception as exc:  # noqa: BLE001 - a broad catch is the point here
        print(f"[{WARN}] {label}  ->  {type(exc).__name__}: {exc}")
        return False


def section(title: str) -> None:
    print(f"\n--- {title} " + "-" * max(0, 56 - len(title)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="path to iobroker.json")
    ap.add_argument("--cleanup", action="store_true", help="remove test objects and exit")
    ap.add_argument("--listen", type=float, default=3.0, help="seconds to wait for events")
    args = ap.parse_args()

    cfg_path = find_config(args.config)
    print(f"Config: {cfg_path}")

    states_cfg = load_db_config("states", cfg_path)
    objects_cfg = load_db_config("objects", cfg_path)
    states = connect(states_cfg)
    objects = connect(objects_cfg)
    print(f"States : {states_cfg.kind} @ {states_cfg.host}:{states_cfg.port}")
    print(f"Objects: {objects_cfg.kind} @ {objects_cfg.host}:{objects_cfg.port}")

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
        print("\nCleaned up.")
        return 0

    # ------------------------------------------------------------------
    section("Connection and protocol version")
    # ------------------------------------------------------------------
    ok = True
    for name, client in (("states", states), ("objects", objects)):
        # No PING: the built-in server does not implement it. The protocol
        # version key doubles as the connection test.
        try:
            version = client.get(f"meta.{name}.protocolVersion")
        except Exception as exc:  # noqa: BLE001
            print(f"[{NO}] {name}: no connection -> {exc}")
            return 2
        if version == "4":
            print(f"[{OK}] {name}: protocol version {version}")
        else:
            print(f"[{WARN}] {name}: protocol version {version!r} -- expected '4'")
            ok = False

    # ------------------------------------------------------------------
    section("Command support: states DB")
    # ------------------------------------------------------------------
    probe_command("SET / GET", lambda: states.set("io.__probe__", "1") and states.get("io.__probe__"))
    # SETEX needs a valid state object: the server parses the payload and
    # attaches the expiry to it.
    probe_command(
        "SETEX (expire)",
        lambda: states.execute_command(
            "setex", "io.__probe_ex__", 60,
            json.dumps({"val": 1, "ack": True, "ts": int(time.time() * 1000), "q": 0, "from": FROM}),
        ),
    )
    probe_command("MGET", lambda: states.mget(["io.__probe__"]))
    probe_command("KEYS", lambda: states.keys("io.__probe*"))
    has_multi = probe_command(
        "MULTI/EXEC (set + publish in one round trip)",
        lambda: states.pipeline(transaction=True).set("io.__probe__", "1").publish("io.__probe__", "1").execute(),
    )
    has_scan = probe_command("SCAN", lambda: states.scan(cursor=0, match="io.__probe*", count=10))
    accepts_notify = probe_command(
        "CONFIG SET notify-keyspace-events (accepted?)",
        lambda: states.config_set("notify-keyspace-events", "Ex"),
    )
    states.delete("io.__probe__", "io.__probe_ex__")

    # ------------------------------------------------------------------
    section("Command support: objects DB")
    # ------------------------------------------------------------------
    has_obj_scan = probe_command("SCAN", lambda: objects.scan(cursor=0, match="cfg.o.system.adapter.*", count=10))
    has_sets = probe_command("SSCAN on cfg.s.object.type.state", lambda: objects.sscan("cfg.s.object.type.state", 0, count=5))
    has_eval = probe_command("EVAL (Lua, for getObjectView)", lambda: objects.eval("return 1", 0))
    use_sets_raw = objects.get("meta.objects.features.useSets")
    use_sets = bool(int(use_sets_raw or "0"))
    print(f"[{OK}] meta.objects.features.useSets = {use_sets_raw!r}  ->  index sets {'on' if use_sets else 'off'}")

    # ------------------------------------------------------------------
    section("Round trip: create object")
    # ------------------------------------------------------------------
    now = int(time.time() * 1000)
    obj = {
        "_id": f"{NS}.temperature",
        "type": "state",
        "common": {
            "name": "Probe temperature",
            "type": "number",
            "role": "value.temperature",
            "unit": "°C",
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
    print(f"[{OK}] {obj_id} written -- visible in admin as '{NS}.temperature'")

    # ------------------------------------------------------------------
    section("Round trip: write state and receive change")
    # ------------------------------------------------------------------
    sub = states.pubsub()
    sub.psubscribe(f"io.{NS}.*")
    # Drain the subscription confirmation so we do not miss the message.
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
        print(f"[{OK}] state written via MULTI: {value} °C")
    else:
        states.set(state_id, state_json)
        states.publish(state_id, state_json)
        print(f"[{WARN}] state written without MULTI (two round trips)")

    print(f"[{OK}] read back: {states.get(state_id)}")

    received = None
    deadline = time.time() + args.listen
    while time.time() < deadline:
        msg = sub.get_message(timeout=0.3)
        if msg and msg.get("type") == "pmessage":
            received = msg
            break
    if received:
        # Note the channel name: the built-in server strips the "io." prefix,
        # real Redis keeps it. The SDK tolerates both.
        print(f"[{OK}] event received on '{received['channel']}': {received['data']}")
    else:
        print(f"[{NO}] no event within {args.listen}s -- check PSUBSCRIBE")
    sub.close()

    # ------------------------------------------------------------------
    section("Summary")
    # ------------------------------------------------------------------
    print(f"States  : MULTI={'yes' if has_multi else 'NO'}  SCAN={'yes' if has_scan else 'no'}  "
          f"CONFIG accepted={'yes' if accepts_notify else 'no'}")
    print(f"Objects : SCAN={'yes' if has_obj_scan else 'no'}  sets={'yes' if has_sets else 'no'}  "
          f"EVAL={'yes' if has_eval else 'no'}")
    print()
    if not has_scan:
        print("  -> States DB without SCAN: the SDK must fall back to KEYS. Against real")
        print("     Redis, KEYS blocks -- so switch behaviour based on server type.")
    if not has_eval or not has_sets:
        print("  -> getObjectView cannot run through Lua here; filter client side.")
    if accepts_notify:
        print("  -> CONFIG SET was accepted, which does NOT mean keyspace events are")
        print("     emitted. On the built-in server they are not: an expired state")
        print("     arrives as 'null' on its own channel instead.")
    print(f"\nClean up with: python {os.path.basename(__file__)} --cleanup")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
