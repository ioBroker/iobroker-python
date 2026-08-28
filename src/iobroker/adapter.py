"""The adapter base class.

Deliberately close to ``@iobroker/adapter-core``: same vocabulary, same
lifecycle hooks, same ``ack`` semantics. Anyone who knows ioBroker adapters
should have nothing new to learn but the language.

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
    """Base class for a Python adapter."""

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

    # -- Lifecycle hooks, meant to be overridden --------------------------

    async def on_ready(self) -> None:
        """Connection is up and the configuration has been loaded."""

    async def on_state_change(self, id: str, state: State | None) -> None:
        """A subscribed state changed. ``None`` means deleted or expired."""

    async def on_object_change(self, id: str, obj: dict[str, Any] | None) -> None:
        """A subscribed object changed."""

    async def on_message(self, msg: Message) -> None:
        """A message arrived through the messagebox."""

    async def on_unload(self) -> None:
        """Last chance to clean up before the process ends."""

    # -- Startup ----------------------------------------------------------

    def run(self) -> None:
        """Start the adapter and block until it stops."""
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
        # Our own messagebox and the controller's stop signal.
        await self._sub.psubscribe(f"{MESSAGE_PREFIX}{self.instance_id}")
        await self._sub.psubscribe(f"{STATES_PREFIX}{self.instance_id}.sigKill")
        self._pump = asyncio.create_task(self._pump_events())

        await self.set_state("info.connection", False, ack=True)
        await self._set_alive(True)
        self._alive = asyncio.create_task(self._heartbeat())

        self.log.info(f"Adapter {self.namespace} started (PID {os.getpid()})")
        try:
            await self.on_ready()
            await self._stopping.wait()
        finally:
            await self._shutdown()

    async def _load_config(self) -> None:
        """Read ``native`` from the instance object into ``self.config``."""
        obj = await self.get_foreign_object(self.instance_id)
        if obj:
            self.config = obj.get("native") or {}
            common = obj.get("common") or {}
            if common.get("loglevel"):
                self._loglevel = common["loglevel"]

    # -- States -----------------------------------------------------------

    def _abs(self, id: str) -> str:
        """Prepend our own namespace unless the id is already absolute."""
        if id.startswith(f"{self.namespace}.") or id.startswith("system."):
            return id
        return f"{self.namespace}.{id}"

    async def set_state(
        self, id: str, val: Any, ack: bool = False, expire: int | None = None
    ) -> None:
        """Write one of our own states."""
        await self.set_foreign_state(self._abs(id), val, ack=ack, expire=expire)

    async def set_foreign_state(
        self, id: str, val: Any, ack: bool = False, expire: int | None = None
    ) -> None:
        """Write an arbitrary state.

        Writing and publishing go into a single MULTI -- the JS client does the
        same, and setState is the hottest path in the system.
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
        """Subscribe to our own states."""
        await self.subscribe_foreign_states(f"{self.namespace}.{pattern}")

    async def subscribe_foreign_states(self, pattern: str) -> None:
        await self._sub.psubscribe(f"{STATES_PREFIX}{pattern}")

    # -- Objects ----------------------------------------------------------

    async def get_object(self, id: str) -> dict[str, Any] | None:
        return await self.get_foreign_object(self._abs(id))

    async def get_foreign_object(self, id: str) -> dict[str, Any] | None:
        raw = await self._objects.get(f"{OBJECTS_PREFIX}{id}")
        return json.loads(raw) if raw else None

    async def set_object(self, id: str, obj: dict[str, Any]) -> None:
        await self.set_foreign_object(self._abs(id), obj)

    async def set_foreign_object(self, id: str, obj: dict[str, Any]) -> None:
        """Create or overwrite an object.

        Maintains the index sets when the installation uses them -- otherwise
        the objects would be missing from ``getObjectView`` later on.
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
        """Create the object only if it does not exist yet.

        Returns whether it was written -- this is what keeps user edits to
        ``common`` from being reset on every start.
        """
        if await self.get_object(id) is not None:
            return False
        await self.set_object(id, obj)
        return True

    # -- Messages ---------------------------------------------------------

    async def send_to(
        self, target: str, command: str, message: Any = None
    ) -> None:
        """Send a message to another instance."""
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
        """Answer a message that expects a reply."""
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

    # -- Event loop -------------------------------------------------------

    async def _pump_events(self) -> None:
        """Route incoming pub/sub messages to the lifecycle hooks."""
        try:
            async for raw in self._sub.listen():
                if raw.get("type") != "pmessage":
                    continue
                channel: str = raw["channel"]
                data: str = raw["data"]
                try:
                    await self._dispatch(channel, data)
                except Exception:  # noqa: BLE001
                    self.log.error(f"Failed to handle {channel}", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.log.error(f"Event loop aborted: {exc}")
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

        # The built-in server delivers the channel without the "io." prefix,
        # real Redis delivers it with. The JS client tolerates both -- so do we.
        state_id = channel[len(STATES_PREFIX):] if channel.startswith(STATES_PREFIX) else channel

        # Controller stop signal: sigKill == -1 means "terminate yourself".
        if state_id == f"{self.instance_id}.sigKill":
            if data and data != "null":
                with contextlib.suppress(Exception):
                    if int(json.loads(data).get("val", 0)) == -1:
                        self.log.info("sigKill received -- shutting down")
                        self._stopping.set()
            return

        # Expiry: the built-in server publishes "null" on the state channel
        # itself. Real Redis instead reports through __keyevent@<db>__:expired,
        # which would need a separate subscription.
        if not data or data == "null":
            await self.on_state_change(state_id, None)
            return

        await self.on_state_change(state_id, State.from_wire(json.loads(data)))

    # -- Heartbeat --------------------------------------------------------

    async def _set_alive(self, alive: bool) -> None:
        await self.set_foreign_state(f"{self.instance_id}.alive", alive, ack=True)

    async def _heartbeat(self) -> None:
        """Keep alive/uptime/memRss current -- just like a Node adapter does."""
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

    # -- Shutdown ---------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stopping.set)

    def stop(self) -> None:
        """Shut the adapter down in an orderly fashion."""
        self._stopping.set()

    async def _shutdown(self) -> None:
        self.log.info(f"Adapter {self.namespace} is shutting down")
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
    """Logger writing both to stdout and to the ``log.`` channel.

    Both are needed: the channel reaches the log transporters, while stdout
    catches everything third-party libraries print unfiltered.
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
# Helpers
# --------------------------------------------------------------------------

def _cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--instance", default=None)
    parser.add_argument("--loglevel", default=None)
    known, _ = parser.parse_known_args()
    return known


def _read_instance() -> int:
    """Instance number from ``--instance`` or ``IOB_INSTANCE``."""
    env = os.environ.get("IOB_INSTANCE")
    if env is not None:
        return int(env)
    value = _cli().instance
    return int(value) if value is not None else 0


def _read_loglevel() -> str:
    return _cli().loglevel or "info"


def _rss_mb() -> float | None:
    """Memory usage in MB, without a hard dependency on psutil."""
    try:
        import resource  # POSIX

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KB, macOS reports bytes.
        return round(usage / (1024 if sys.platform != "darwin" else 1024 * 1024), 2)
    except ImportError:
        try:
            import psutil  # optional, mainly for Windows

            return round(psutil.Process().memory_info().rss / (1024 * 1024), 2)
        except Exception:  # noqa: BLE001
            return None
    except Exception:  # noqa: BLE001
        return None
