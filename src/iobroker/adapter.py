"""Die Adapter-Basisklasse.

Bewusst nah an ``@iobroker/adapter-core``: gleiche Begriffe, gleiche
Lebenszyklus-Haken, gleiche ``ack``-Semantik. Wer ioBroker-Adapter kennt, soll
nichts Neues lernen muessen ausser der Sprache.

    from iobroker import Adapter

    class MyAdapter(Adapter):
        async def on_ready(self):
            await self.set_object_not_exists("temperature", {...})
            await self.subscribe_states("*")

        async def on_state_change(self, id, state):
            ...

    MyAdapter("myadapter").run()
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import time
from typing import Any

from .connection import (
    LOG_PREFIX,
    MESSAGE_PREFIX,
    OBJECTS_PREFIX,
    SETS_PREFIX,
    STATES_PREFIX,
    check_protocol,
    connect_async,
    load_db_config,
)
from .types import Message, State, now_ms

__all__ = ["Adapter"]

_LEVELS = {"silly": 10, "debug": 10, "info": 20, "warn": 30, "error": 40}


class Adapter:
    """Basisklasse fuer einen Python-Adapter."""

    def __init__(self, name: str, instance: int | None = None) -> None:
        self.name = name
        self.instance = instance if instance is not None else _read_instance()
        self.namespace = f"{name}.{self.instance}"
        self.instance_id = f"system.adapter.{self.namespace}"

        self.config: dict[str, Any] = {}
        self.log = _Log(self)

        self._states: Any = None
        self._objects: Any = None
        self._sub: Any = None
        self._pump: asyncio.Task | None = None
        self._alive: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._started = time.time()
        self._loglevel = os.environ.get("IOB_LOGLEVEL") or _read_loglevel()

        self._builtin_states = True

    # -- Lebenszyklus, zum Ueberschreiben ---------------------------------

    async def on_ready(self) -> None:
        """Verbindung steht, Konfiguration ist geladen."""

    async def on_state_change(self, id: str, state: State | None) -> None:
        """Ein abonnierter State hat sich geaendert. ``None`` heisst geloescht."""

    async def on_object_change(self, id: str, obj: dict[str, Any] | None) -> None:
        """Ein abonniertes Objekt hat sich geaendert."""

    async def on_message(self, msg: Message) -> None:
        """Eine Nachricht aus der Messagebox."""

    async def on_unload(self) -> None:
        """Letzte Gelegenheit aufzuraeumen, bevor der Prozess endet."""

    # -- Start ------------------------------------------------------------

    def run(self) -> None:
        """Startet den Adapter und blockiert bis zum Ende."""
        try:
            asyncio.run(self._main())
        except KeyboardInterrupt:
            pass

    async def _main(self) -> None:
        states_cfg = load_db_config("states")
        objects_cfg = load_db_config("objects")
        self._builtin_states = states_cfg.is_builtin

        self._states = connect_async(states_cfg)
        self._objects = connect_async(objects_cfg)
        await check_protocol(self._states, "states")
        await check_protocol(self._objects, "objects")

        await self._load_config()
        self._install_signal_handlers()

        self._sub = self._states.pubsub()
        # Die eigene Messagebox und das Stopp-Signal des Controllers.
        await self._sub.psubscribe(f"{MESSAGE_PREFIX}{self.instance_id}")
        await self._sub.psubscribe(f"{STATES_PREFIX}{self.instance_id}.sigKill")
        self._pump = asyncio.create_task(self._pump_events())

        await self.set_state("info.connection", False, ack=True)
        await self._set_alive(True)
        self._alive = asyncio.create_task(self._heartbeat())

        self.log.info(f"Adapter {self.namespace} gestartet (PID {os.getpid()})")
        try:
            await self.on_ready()
            await self._stopping.wait()
        finally:
            await self._shutdown()

    async def _load_config(self) -> None:
        """Liest ``native`` aus dem Instanzobjekt in ``self.config``."""
        obj = await self.get_foreign_object(self.instance_id)
        if obj:
            self.config = obj.get("native") or {}
            common = obj.get("common") or {}
            if common.get("loglevel"):
                self._loglevel = common["loglevel"]

    # -- States -----------------------------------------------------------

    def _abs(self, id: str) -> str:
        """Ergaenzt den eigenen Namespace, wenn die ID nicht schon absolut ist."""
        if id.startswith(f"{self.namespace}.") or id.startswith("system."):
            return id
        return f"{self.namespace}.{id}"

    async def set_state(
        self, id: str, val: Any, ack: bool = False, expire: int | None = None
    ) -> None:
        """Schreibt einen eigenen State."""
        await self.set_foreign_state(self._abs(id), val, ack=ack, expire=expire)

    async def set_foreign_state(
        self, id: str, val: Any, ack: bool = False, expire: int | None = None
    ) -> None:
        """Schreibt einen beliebigen State.

        Schreiben und Benachrichtigen laufen in einem MULTI -- der JS-Client
        macht es genauso, und setState ist der heisseste Pfad im System.
        """
        state = val if isinstance(val, State) else State(val=val, ack=ack)
        state.from_ = state.from_ or self.instance_id
        payload = json.dumps(state.to_wire())
        key = f"{STATES_PREFIX}{id}"

        pipe = self._states.pipeline(transaction=True)
        if expire:
            pipe.setex(key, int(expire), payload)
        else:
            pipe.set(key, payload)
        pipe.publish(key, payload)
        await pipe.execute()

    async def get_state(self, id: str) -> State | None:
        return await self.get_foreign_state(self._abs(id))

    async def get_foreign_state(self, id: str) -> State | None:
        raw = await self._states.get(f"{STATES_PREFIX}{id}")
        if not raw:
            return None
        return State.from_wire(json.loads(raw))

    async def subscribe_states(self, pattern: str = "*") -> None:
        """Abonniert eigene States."""
        await self.subscribe_foreign_states(f"{self.namespace}.{pattern}")

    async def subscribe_foreign_states(self, pattern: str) -> None:
        await self._sub.psubscribe(f"{STATES_PREFIX}{pattern}")

    # -- Objekte ----------------------------------------------------------

    async def get_object(self, id: str) -> dict[str, Any] | None:
        return await self.get_foreign_object(self._abs(id))

    async def get_foreign_object(self, id: str) -> dict[str, Any] | None:
        raw = await self._objects.get(f"{OBJECTS_PREFIX}{id}")
        return json.loads(raw) if raw else None

    async def set_object(self, id: str, obj: dict[str, Any]) -> None:
        await self.set_foreign_object(self._abs(id), obj)

    async def set_foreign_object(self, id: str, obj: dict[str, Any]) -> None:
        """Legt ein Objekt an oder ueberschreibt es.

        Pflegt die Index-Sets mit, sofern die Installation sie benutzt --
        sonst fehlen die Objekte spaeter in ``getObjectView``.
        """
        obj = dict(obj)
        obj["_id"] = id
        obj.setdefault("native", {})
        obj["from"] = self.instance_id
        obj["ts"] = now_ms()
        payload = json.dumps(obj)
        key = f"{OBJECTS_PREFIX}{id}"

        await self._objects.set(key, payload)
        await self._objects.publish(key, payload)

        if obj.get("type"):
            use_sets = await self._objects.get("meta.objects.features.useSets")
            if use_sets and int(use_sets):
                with contextlib.suppress(Exception):
                    await self._objects.sadd(
                        f"{SETS_PREFIX}object.type.{obj['type']}", key
                    )

    async def set_object_not_exists(self, id: str, obj: dict[str, Any]) -> bool:
        """Legt das Objekt nur an, wenn es noch nicht existiert.

        Gibt zurueck, ob geschrieben wurde -- so bleiben Nutzeranpassungen an
        ``common`` bei jedem Start erhalten.
        """
        if await self.get_object(id) is not None:
            return False
        await self.set_object(id, obj)
        return True

    # -- Nachrichten ------------------------------------------------------

    async def send_to(
        self, target: str, command: str, message: Any = None
    ) -> None:
        """Schickt eine Nachricht an eine andere Instanz."""
        payload = {
            "command": command,
            "message": message,
            "from": self.instance_id,
            "callback": None,
        }
        await self._states.publish(
            f"{MESSAGE_PREFIX}system.adapter.{target}", json.dumps(payload)
        )

    async def reply(self, msg: Message, result: Any) -> None:
        """Beantwortet eine Nachricht, die eine Antwort erwartet."""
        if not msg.callback:
            return
        callback = dict(msg.callback)
        callback["ack"] = True
        payload = {
            "command": msg.command,
            "message": result,
            "from": self.instance_id,
            "callback": callback,
        }
        await self._states.publish(f"{MESSAGE_PREFIX}{msg.from_}", json.dumps(payload))

    # -- Ereignisschleife -------------------------------------------------

    async def _pump_events(self) -> None:
        """Verteilt eingehende Pub/Sub-Nachrichten auf die Lebenszyklus-Haken."""
        try:
            async for raw in self._sub.listen():
                if raw.get("type") != "pmessage":
                    continue
                channel: str = raw["channel"]
                data: str = raw["data"]
                try:
                    await self._dispatch(channel, data)
                except Exception:  # noqa: BLE001
                    self.log.error(f"Fehler beim Verarbeiten von {channel}", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.log.error(f"Ereignisschleife abgebrochen: {exc}")
            self._stopping.set()

    async def _dispatch(self, channel: str, data: str) -> None:
        # Messagebox
        if channel.startswith(MESSAGE_PREFIX):
            payload = json.loads(data)
            await self.on_message(
                Message(
                    command=payload.get("command", ""),
                    message=payload.get("message"),
                    from_=payload.get("from", ""),
                    callback=payload.get("callback"),
                    _id=payload.get("_id", 0),
                )
            )
            return

        # Der eingebaute Server liefert den Kanal ohne "io."-Praefix, echtes
        # Redis mit. Der JS-Client toleriert beides -- wir auch.
        state_id = channel[len(STATES_PREFIX):] if channel.startswith(STATES_PREFIX) else channel

        # Stopp-Signal des Controllers: sigKill == -1 heisst "beende dich".
        if state_id == f"{self.instance_id}.sigKill":
            if data and data != "null":
                with contextlib.suppress(Exception):
                    if int(json.loads(data).get("val", 0)) == -1:
                        self.log.info("sigKill empfangen -- beende Adapter")
                        self._stopping.set()
            return

        # Ablauf: der eingebaute Server publiziert beim Ablauf "null" auf dem
        # State-Kanal selbst. Echtes Redis meldet stattdessen ueber
        # __keyevent@<db>__:expired -- dort waere ein eigenes Abo noetig.
        if not data or data == "null":
            await self.on_state_change(state_id, None)
            return

        await self.on_state_change(state_id, State.from_wire(json.loads(data)))

    # -- Lebenszeichen ----------------------------------------------------

    async def _set_alive(self, alive: bool) -> None:
        await self.set_foreign_state(f"{self.instance_id}.alive", alive, ack=True)

    async def _heartbeat(self) -> None:
        """Haelt alive/uptime/memRss aktuell -- wie ein Node-Adapter auch."""
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(15)
                if self._stopping.is_set():
                    break
                await self._set_alive(True)
                await self.set_foreign_state(
                    f"{self.instance_id}.uptime", int(time.time() - self._started), ack=True
                )
                rss = _rss_mb()
                if rss is not None:
                    await self.set_foreign_state(
                        f"{self.instance_id}.memRss", rss, ack=True
                    )
        except asyncio.CancelledError:
            raise

    # -- Ende -------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stopping.set)

    def stop(self) -> None:
        """Beendet den Adapter geordnet."""
        self._stopping.set()

    async def _shutdown(self) -> None:
        self.log.info(f"Adapter {self.namespace} wird beendet")
        with contextlib.suppress(Exception):
            await self.on_unload()
        for task in (self._alive, self._pump):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        with contextlib.suppress(Exception):
            await self.set_state("info.connection", False, ack=True)
        with contextlib.suppress(Exception):
            await self._set_alive(False)
        for closable in (self._sub, self._states, self._objects):
            if closable is not None:
                with contextlib.suppress(Exception):
                    await closable.aclose()


class _Log:
    """Logger, der nach stdout und auf den ``log.``-Kanal schreibt.

    Beides wird gebraucht: der Kanal erreicht die Log-Transporter, stdout faengt
    alles auf, was fremde Bibliotheken ungefiltert ausgeben.
    """

    def __init__(self, adapter: "Adapter") -> None:
        self._adapter = adapter
        self._py = logging.getLogger(adapter.namespace)
        if not self._py.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(name)s %(message)s"))
            self._py.addHandler(handler)
            self._py.setLevel(logging.DEBUG)

    def _emit(self, severity: str, message: str, **kwargs: Any) -> None:
        threshold = _LEVELS.get(self._adapter._loglevel, 20)
        if _LEVELS.get(severity, 20) < threshold:
            return
        getattr(self._py, "warning" if severity == "warn" else severity, self._py.info)(
            message, **kwargs
        )
        states = self._adapter._states
        if states is None:
            return
        payload = json.dumps(
            {
                "message": message,
                "severity": severity,
                "from": self._adapter.instance_id,
                "ts": now_ms(),
            }
        )
        with contextlib.suppress(Exception):
            asyncio.get_running_loop().create_task(
                states.publish(f"{LOG_PREFIX}{self._adapter.instance_id}", payload)
            )

    def silly(self, message: str, **kw: Any) -> None:
        self._emit("silly", message, **kw)

    def debug(self, message: str, **kw: Any) -> None:
        self._emit("debug", message, **kw)

    def info(self, message: str, **kw: Any) -> None:
        self._emit("info", message, **kw)

    def warn(self, message: str, **kw: Any) -> None:
        self._emit("warn", message, **kw)

    def error(self, message: str, **kw: Any) -> None:
        self._emit("error", message, **kw)


# --------------------------------------------------------------------------
# Hilfsfunktionen
# --------------------------------------------------------------------------

def _cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--instance", default=None)
    parser.add_argument("--loglevel", default=None)
    known, _ = parser.parse_known_args()
    return known


def _read_instance() -> int:
    """Instanznummer aus ``--instance`` oder ``IOB_INSTANCE``."""
    env = os.environ.get("IOB_INSTANCE")
    if env is not None:
        return int(env)
    value = _cli().instance
    return int(value) if value is not None else 0


def _read_loglevel() -> str:
    return _cli().loglevel or "info"


def _rss_mb() -> float | None:
    """Speicherverbrauch in MB, ohne harte Abhaengigkeit auf psutil."""
    try:
        import resource  # POSIX

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux liefert KB, macOS Bytes.
        return round(usage / (1024 if sys.platform != "darwin" else 1024 * 1024), 2)
    except ImportError:
        try:
            import psutil  # optional, u.a. fuer Windows

            return round(psutil.Process().memory_info().rss / (1024 * 1024), 2)
        except Exception:  # noqa: BLE001
            return None
    except Exception:  # noqa: BLE001
        return None
