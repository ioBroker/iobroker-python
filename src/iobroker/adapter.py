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
import traceback
from typing import Any

from .connection import (
    FILES_PREFIX,
    LOG_PREFIX,
    MESSAGE_PREFIX,
    OBJECTS_PREFIX,
    SETS_PREFIX,
    STATES_PREFIX,
    check_protocol,
    connect_async,
    load_db_config,
)
from .protection import strip_protected
from .files import (
    FILE_SEPARATOR,
    FileMeta,
    file_key,
    guess_mime_type,
    normalize_name,
    split_file_key,
)
from .crypto import decrypt, decrypt_native
from .exit_codes import ExitCode
from .types import Message, State, now_ms

__all__ = ["Adapter"]

_LEVELS = {"silly": 10, "debug": 10, "info": 20, "warn": 30, "error": 40}

#: How long the states the heartbeat refreshes stay valid, in seconds.
#: adapter-core uses ``statisticsInterval / 1000 + 10``, and that interval defaults to 15 s --
#: the same 15 s this SDK's heartbeat runs at. The expiry is what makes a process that was
#: killed outright stop claiming to be alive, instead of leaving the state true forever.
_STATUS_EXPIRE_SECONDS = 25


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
        self._exit_code = int(ExitCode.NO_ERROR)
        self._loglevel = os.environ.get("IOB_LOGLEVEL") or _read_loglevel()

        self._builtin_states = True
        #: Whether the link to the databases is up. Reported as
        #: ``system.adapter.<ns>.connected``, which is a different thing from the adapter's own
        #: ``info.connection`` -- that one says whether it reached the device or service it
        #: talks to, this one whether ioBroker can reach the adapter at all.
        self.connected = False
        self._secret: str | None = None
        # The installation's default ACL, applied to objects created without one -- exactly what the
        # JS objects client does. Loaded at startup and kept current through the system.config
        # subscription below. ``None`` means "not configured" (older installs); then nothing is
        # stamped, matching JS.
        self._default_new_acl: dict[str, Any] | None = None

        # Kept so subscriptions can be restored after a reconnect. redis-py restores its own
        # record, but the built-in server echoes patterns back in a different form than it was
        # given, so relying on that bookkeeping is not safe here.
        self._state_patterns: set[str] = set()
        self._object_patterns: set[str] = set()
        # The patterns the adapter needs for itself: its messagebox, the controller's stop signal,
        # ``system.config``. They live in the two sets above so a reconnect restores them, and are
        # listed here so ``unsubscribe_*`` will not take them away -- an adapter that has
        # unsubscribed its own sigKill can no longer be stopped, and nothing about that symptom
        # would point back at the call that caused it.
        self._internal_patterns: set[str] = set()
        self._osub: Any = None
        self._opump: asyncio.Task | None = None
        # Opened on first use: file content must not be decoded as text.
        self._files: Any = None
        self._objects_cfg: Any = None

    # -- Lifecycle hooks, meant to be overridden --------------------------

    async def on_ready(self) -> None:
        """Connection is up and the configuration has been loaded."""

    async def on_state_change(self, id: str, state: State | None) -> None:
        """A subscribed state changed. ``None`` means deleted or expired."""

    async def on_object_change(self, id: str, obj: dict[str, Any] | None) -> None:
        """A subscribed object changed."""

    async def on_message(self, msg: Message) -> None:
        """A message arrived through the messagebox."""

    async def on_file_change(self, id: str, name: str, size: int | None) -> None:
        """A subscribed file changed. ``size`` is its new length; ``None`` means deleted.

        ``id`` owns the file and ``name`` is the path within it -- the same split
        :meth:`write_file` takes, not the ``id$%$name`` key the database stores it under.

        The content is deliberately not carried: ioBroker publishes only the size, so a handler
        that wants the file reads it with :meth:`read_file`. That keeps a large file out of every
        subscriber's socket when most of them only need to know that it moved.
        """

    async def on_log(self, entry: dict[str, Any]) -> None:
        """A log line from somewhere else in the system.

        Only reaches an adapter that called :meth:`subscribe_logs`. In ioBroker this is what a log
        transporter does -- an adapter that collects the log rather than writing to it.
        """

    async def on_unload(self) -> None:
        """Last chance to clean up before the process ends."""

    # -- Startup ----------------------------------------------------------

    def run(self) -> None:
        """Start the adapter and block until it stops, then exit with the code the controller
        understands (see ``doc/PYTHON.md``): ``0`` on a clean stop, ``11`` when the adapter asked
        to terminate, ``6`` on an exception it did not handle.
        """
        try:
            asyncio.run(self._main())
        except KeyboardInterrupt:
            # Ctrl-C / SIGINT is a graceful stop, same as the controller's sigKill -1.
            return
        except Exception:
            # The controller restarts on this and counts it towards restart-loop detection. The
            # traceback goes to stderr, which the controller forwards to its log at error level --
            # writing it through the (now torn-down) states connection is no longer possible here.
            traceback.print_exc()
            sys.exit(int(ExitCode.UNCAUGHT_EXCEPTION))

        if self._exit_code:
            sys.exit(self._exit_code)

    async def _main(self) -> None:
        states_cfg = load_db_config("states")
        objects_cfg = load_db_config("objects")
        self._builtin_states = states_cfg.is_builtin

        self._objects_cfg = objects_cfg
        self._states = connect_async(states_cfg)
        self._objects = connect_async(objects_cfg)
        await check_protocol(self._states, "states")
        await check_protocol(self._objects, "objects")

        await self._load_config()
        await self._load_default_acl()
        self._install_signal_handlers()

        # Our own messagebox and the controller's stop signal.
        self._state_patterns.add(f"{MESSAGE_PREFIX}{self.instance_id}")
        self._state_patterns.add(f"{STATES_PREFIX}{self.instance_id}.sigKill")

        # Watch system.config so a change to defaultNewAcl made in admin takes effect without a
        # restart -- the JS objects client subscribes to the very same object for the very same
        # reason.
        self._object_patterns.add(f"{OBJECTS_PREFIX}system.config")

        self._internal_patterns = self._state_patterns | self._object_patterns

        self._sub = await self._open_subscription(self._states, self._state_patterns)
        self._pump = asyncio.create_task(self._run_pump("states"))

        self._osub = await self._open_subscription(self._objects, self._object_patterns)
        self._opump = asyncio.create_task(self._run_pump("objects"))

        await self.set_state("info.connection", False, ack=True)
        await self._set_alive(True)
        await self._set_connected(True)
        self._alive = asyncio.create_task(self._heartbeat())

        self.log.info(f"Adapter {self.namespace} started (PID {os.getpid()})")
        try:
            await self.on_ready()
            await self._stopping.wait()
        finally:
            await self._shutdown()

    async def _load_config(self) -> None:
        """Read ``native`` from the instance object into ``self.config``.

        Entries listed in ``common.encryptedNative`` arrive decrypted, so an adapter reads a
        password the same way it reads a hostname. Doing it here rather than leaving it to every
        adapter is what keeps credentials from being used in their encrypted form by accident.
        """
        obj = await self.get_foreign_object(self.instance_id)

        if not obj:
            return

        self.config = obj.get("native") or {}
        common = obj.get("common") or {}

        if common.get("loglevel"):
            self._loglevel = common["loglevel"]

        encrypted = common.get("encryptedNative")

        if encrypted:
            secret = await self.get_system_secret()
            try:
                self.config = decrypt_native(secret, self.config, encrypted)
            except Exception as exc:  # noqa: BLE001
                # Carrying on with encrypted values would fail later at the device, far from the
                # cause, so say it here.
                self.log.error(f"Cannot decrypt the configuration: {exc}")

    async def get_system_secret(self) -> str:
        """Read the system secret used to encrypt configuration values."""
        if self._secret is None:
            obj = await self.get_foreign_object("system.config")
            self._secret = ((obj or {}).get("native") or {}).get("secret") or ""

        return self._secret

    async def get_encrypted_config(self, key: str) -> str | None:
        """Read a single encrypted value from the configuration.

        Only needed for values not listed in ``common.encryptedNative``; everything listed there is
        already decrypted in ``self.config``.

        :param key: name of the entry in ``native``
        """
        value = self.config.get(key)

        if not isinstance(value, str) or not value:
            return None

        return decrypt(await self.get_system_secret(), value)

    async def _load_default_acl(self) -> None:
        """Read ``system.config.common.defaultNewAcl`` once at startup.

        An installation that has never configured it leaves ``self._default_new_acl`` at ``None``,
        and then no acl is stamped -- the same behaviour the JS client shows on such installs.
        """
        obj = await self.get_foreign_object("system.config")
        acl = ((obj or {}).get("common") or {}).get("defaultNewAcl")
        if acl:
            self._default_new_acl = acl

    def _default_acl_for(self, obj_type: str | None) -> dict[str, Any]:
        """The acl to stamp on a new object of ``obj_type`` from ``defaultNewAcl``.

        Mirrors the JS objects client: ``file`` is dropped (it is the default for files, not for
        the object itself) and ``state`` is kept only for states.
        """
        acl = dict(self._default_new_acl or {})
        acl.pop("file", None)
        if obj_type != "state":
            acl.pop("state", None)
        return acl

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

        ``lc`` ("last change") only moves when the value actually changed; an unchanged write keeps
        the previous one. That distinction is what tells an adapter or a script that a reading is
        new rather than merely refreshed, so a sensor polled every 30 seconds does not look like it
        changes every 30 seconds. Reading the previous state for it is what the JS client does too,
        so the extra round-trip is part of the contract rather than an addition.
        """
        state = val if isinstance(val, State) else State(val=val, ack=ack)
        state.from_ = state.from_ or self.instance_id
        key = f"{STATES_PREFIX}{id}"

        wire = state.to_wire()

        # Only when the caller did not pin lc itself. The result goes into `wire`, never back onto
        # `state`: a caller reusing one State object for repeated writes would otherwise carry a
        # pinned lc forward for good.
        if state.lc is None:
            previous = None
            with contextlib.suppress(Exception):
                raw = await self._states.get(key)
                if raw:
                    previous = json.loads(raw)
            if previous and previous.get("lc") and _same_value(previous.get("val"), state.val):
                wire["lc"] = previous["lc"]

        payload = json.dumps(wire)

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
        full = f"{STATES_PREFIX}{pattern}"
        self._state_patterns.add(full)
        await self._sub.psubscribe(full)

    async def subscribe_objects(self, pattern: str = "*") -> None:
        """Subscribe to changes of our own objects."""
        await self.subscribe_foreign_objects(f"{self.namespace}.{pattern}")

    async def subscribe_foreign_objects(self, pattern: str) -> None:
        """Subscribe to changes of arbitrary objects.

        Needed to notice configuration changes made in the admin UI while the adapter runs.
        """
        full = f"{OBJECTS_PREFIX}{pattern}"
        self._object_patterns.add(full)
        await self._osub.psubscribe(full)

    async def unsubscribe_states(self, pattern: str = "*") -> None:
        """Stop receiving changes of our own states."""
        await self.unsubscribe_foreign_states(f"{self.namespace}.{pattern}")

    async def unsubscribe_foreign_states(self, pattern: str) -> None:
        """Stop receiving changes matching ``pattern``.

        The pattern has to be the one that was subscribed, character for character: this removes a
        subscription, it does not cancel every subscription a pattern would overlap. Unsubscribing
        ``hue.0.*`` leaves ``hue.0.lamp.level`` in place, exactly as it does in the JS adapter and
        in Redis itself.

        Unknown patterns are ignored rather than reported. Removing a subscription that is not
        there is what the caller wanted either way, and a script engine tearing down a script
        should not have to know whether a neighbour still holds the same pattern.
        """
        await self._unsubscribe(f"{STATES_PREFIX}{pattern}", self._state_patterns, self._sub)

    async def unsubscribe_objects(self, pattern: str = "*") -> None:
        """Stop receiving changes of our own objects."""
        await self.unsubscribe_foreign_objects(f"{self.namespace}.{pattern}")

    async def unsubscribe_foreign_objects(self, pattern: str) -> None:
        """Stop receiving object changes matching ``pattern``."""
        await self._unsubscribe(f"{OBJECTS_PREFIX}{pattern}", self._object_patterns, self._osub)

    async def subscribe_files(self, pattern: str = "*") -> None:
        """Watch files below our own namespace."""
        await self.subscribe_foreign_files(self.namespace, pattern)

    async def subscribe_foreign_files(self, id: str, pattern: str = "*") -> None:
        """Watch files of ``id``, delivered to :meth:`on_file_change`.

        ``id`` is the object that owns the files -- ``vis-2.0``, ``admin.admin`` -- and ``pattern``
        matches the path inside it, so ``subscribe_foreign_files("vis-2.0", "main/*")``.

        Files travel on the objects connection: they live in the objects database, keyed
        ``cfg.f.<id>$%$<name>``. That is why this sits beside the object subscriptions rather than
        the state ones.

        The pattern carries the ``$%$data`` suffix, which is not decoration: it is what the JS
        client subscribes with, and each backend needs it for a different reason. Real Redis
        delivers on the data key itself, so without the suffix nothing matches at all; the built-in
        server strips the suffix again before registering, so it accepts the same string. Leaving
        it off is silent on both -- a subscription that exists and never fires.
        """
        full = f"{FILES_PREFIX}{id}{FILE_SEPARATOR}{pattern}{FILE_SEPARATOR}data"
        self._object_patterns.add(full)
        await self._osub.psubscribe(full)

    async def unsubscribe_files(self, pattern: str = "*") -> None:
        """Stop watching files below our own namespace."""
        await self.unsubscribe_foreign_files(self.namespace, pattern)

    async def unsubscribe_foreign_files(self, id: str, pattern: str = "*") -> None:
        """Stop watching files of ``id`` matching ``pattern``."""
        await self._unsubscribe(
            f"{FILES_PREFIX}{id}{FILE_SEPARATOR}{pattern}{FILE_SEPARATOR}data",
            self._object_patterns,
            self._osub,
        )

    async def subscribe_logs(self, pattern: str = "*") -> None:
        """Receive the log of other adapters, delivered to :meth:`on_log`.

        What ioBroker calls a log transporter. The pattern names instances, so ``"*"`` is the whole
        system and ``"system.adapter.hue.0"`` is one adapter.

        A host only forwards its adapters' logs to the database once something has asked for them,
        which an adapter announces with ``common.logTransporter`` in its io-package.json.
        Subscribing without that setting is not an error -- the subscription simply stays quiet,
        which is a confusing way to find out, so it is worth saying here.
        """
        full = f"{LOG_PREFIX}{pattern}"
        self._state_patterns.add(full)
        await self._sub.psubscribe(full)

    async def unsubscribe_logs(self, pattern: str = "*") -> None:
        """Stop receiving other adapters' log."""
        await self._unsubscribe(f"{LOG_PREFIX}{pattern}", self._state_patterns, self._sub)

    async def _unsubscribe(self, full: str, patterns: set[str], sub: Any) -> None:
        """Drop one recorded pattern and tell the server, if there is one yet.

        Both halves matter. The set is what a reconnect replays, so a pattern left in it would come
        back on the next outage; the server call is what stops the traffic now. Doing only one of
        them is the kind of bug that shows up hours later as a handler firing for something the
        script no longer watches.
        """
        if full in self._internal_patterns:
            self.log.warn(f"refusing to unsubscribe {full!r}: the adapter needs it to work")
            return

        if full not in patterns:
            return

        patterns.discard(full)

        # Before `run()` there is no connection, and after a reconnect the pattern is simply not
        # replayed. Neither is an error -- the recorded set is the source of truth.
        if sub is not None:
            with contextlib.suppress(Exception):
                await sub.punsubscribe(full)

    # -- Objects ----------------------------------------------------------

    async def get_object(self, id: str) -> dict[str, Any] | None:
        return await self.get_foreign_object(self._abs(id))

    async def get_foreign_object(self, id: str) -> dict[str, Any] | None:
        """Read an arbitrary object.

        Entries another adapter listed in ``common.protectedNative`` are removed on the way out.
        ioBroker enforces that in the client, not in the database, and this SDK talks to the
        database directly -- so without doing it here, reading a foreign instance object would hand
        over exactly what the flag exists to withhold.
        """
        raw = await self._objects.get(f"{OBJECTS_PREFIX}{id}")

        if not raw:
            return None

        return strip_protected(self.name, id, json.loads(raw))

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
        key = f"{OBJECTS_PREFIX}{id}"

        # ACL: a new object that carries no acl of its own inherits the installation's default,
        # just like the JS objects client does. An acl already present on the incoming object, or
        # on the object being overwritten, is left untouched -- overwriting must not silently reset
        # rights a user changed. The extra read happens only when there is a default to apply and
        # the caller supplied no acl, so the common create-with-acl path stays a single write.
        if self._default_new_acl and not obj.get("acl"):
            old_acl = None
            existing = await self._objects.get(key)
            if existing:
                with contextlib.suppress(Exception):
                    old_acl = json.loads(existing).get("acl")
            obj["acl"] = old_acl or self._default_acl_for(obj.get("type"))

        payload = json.dumps(obj)

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

    async def extend_object(self, id: str, patch: dict[str, Any]) -> None:
        """Merge a patch into an existing object.

        Shallow per section: ``common`` and ``native`` are merged key by key, everything else is
        replaced. That is what ``extendObject`` does in JavaScript, and adapters rely on it to
        change one field without rewriting an object a user may have edited.
        """
        current = await self.get_foreign_object(self._abs(id)) or {}
        merged = {**current}

        for key, value in patch.items():
            if key in ("common", "native") and isinstance(value, dict):
                merged[key] = {**(current.get(key) or {}), **value}
            else:
                merged[key] = value

        await self.set_foreign_object(self._abs(id), merged)

    async def delete_object(self, id: str) -> None:
        """Remove one of our own objects."""
        await self.delete_foreign_object(self._abs(id))

    async def delete_foreign_object(self, id: str) -> None:
        """Remove an arbitrary object, keeping the type index in step."""
        key = f"{OBJECTS_PREFIX}{id}"
        obj = await self.get_foreign_object(id)

        await self._objects.delete(key)
        await self._objects.publish(key, "null")

        # Leaving the id in the type set would make it show up in views pointing at nothing.
        if obj and obj.get("type"):
            with contextlib.suppress(Exception):
                await self._objects.srem(f"{SETS_PREFIX}object.type.{obj['type']}", key)

    async def delete_state(self, id: str) -> None:
        """Remove one of our own states."""
        await self.delete_foreign_state(self._abs(id))

    async def delete_foreign_state(self, id: str) -> None:
        """Remove an arbitrary state."""
        key = f"{STATES_PREFIX}{id}"
        await self._states.delete(key)
        await self._states.publish(key, "null")

    async def get_object_view(
        self, design: str, view: str, startkey: str = "", endkey: str = "香"
    ) -> list[dict[str, Any]]:
        """List objects of one type within an id range.

        Only the ``system`` design is supported, where the view name is the object type -- that is
        what adapters actually use. The JavaScript client runs Lua for this; here the type index
        sets are read instead, because the states database cannot run Lua at all and the objects
        database only sometimes can. Where the sets are switched off it falls back to scanning.

        :param design: must be ``system``
        :param view: object type, e.g. ``state``, ``channel``, ``device``, ``instance``
        :param startkey: lowest id to include
        :param endkey: highest id to include
        :returns: the matching objects
        """
        if design != "system":
            raise ValueError(f'Only the "system" design is supported, got "{design}"')

        keys = await self._view_keys(view)
        ids = sorted(k[len(OBJECTS_PREFIX) :] for k in keys)
        wanted = [f"{OBJECTS_PREFIX}{i}" for i in ids if startkey <= i <= endkey]

        if not wanted:
            return []

        raw = await self._objects.mget(wanted)
        objects = [json.loads(r) for r in raw if r]

        return [
            stripped
            for obj in objects
            if (stripped := strip_protected(self.name, obj.get("_id", ""), obj)) is not None
        ]

    async def _view_keys(self, view: str) -> list[str]:
        """Collect the object keys of one type, from the index when there is one."""
        # Never ask the built-in servers: they do not implement smembers, and the attempt is
        # not silent -- the controller logs "smembers NOT SUPPORTED" for every view an adapter
        # opens. Suppressing the exception below still leaves that line in the user's log.
        # (sadd, used when writing objects, *is* supported there, so only this side is gated.)
        use_sets = None
        if not self._objects_cfg.is_builtin:
            use_sets = await self._objects.get("meta.objects.features.useSets")

        if use_sets and int(use_sets):
            with contextlib.suppress(Exception):
                members = await self._objects.smembers(f"{SETS_PREFIX}object.type.{view}")
                if members:
                    return list(members)

        # No index: walk everything and filter. Expensive, but the alternative is returning
        # nothing on an installation that has the sets switched off.
        keys: list[str] = []
        cursor = 0

        while True:
            cursor, batch = await self._objects.scan(cursor=cursor, match=f"{OBJECTS_PREFIX}*", count=500)
            keys.extend(batch)
            if cursor == 0:
                break

        if not keys:
            return []

        raw = await self._objects.mget(keys)

        return [
            key
            for key, value in zip(keys, raw)
            if value and json.loads(value).get("type") == view
        ]

    async def get_adapter_objects(self) -> dict[str, dict[str, Any]]:
        """Read every object in our own namespace, keyed by id."""
        pattern = f"{OBJECTS_PREFIX}{self.namespace}.*"
        keys = await self._objects.keys(pattern)

        if not keys:
            return {}

        raw = await self._objects.mget(keys)

        return {
            key[len(OBJECTS_PREFIX) :]: json.loads(value)
            for key, value in zip(keys, raw)
            if value
        }

    async def get_object_list(self) -> dict[str, dict[str, Any]]:
        """Read every object in the system, keyed by id -- the counterpart of ``getObjectList``.

        A script engine needs this: resolving an object's name, its channel or the enums it belongs
        to has to answer synchronously while a handler runs, and that is only possible from a cache
        held in the process. The JS script engine loads the same way ("requesting all objects") and
        keeps it current through a subscription.

        Protected native entries are stripped, exactly as in :meth:`get_foreign_object` -- a bulk
        read must not be the loophole around ``protectedNative``.
        """
        keys = await self._objects.keys(f"{OBJECTS_PREFIX}*")
        if not keys:
            return {}

        objects: dict[str, dict[str, Any]] = {}
        # In batches: a large installation has tens of thousands of objects, and one mget with all
        # of them builds a single reply big enough to matter on a small machine.
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            for key, value in zip(batch, await self._objects.mget(batch)):
                if not value:
                    continue
                id = key[len(OBJECTS_PREFIX) :]
                objects[id] = strip_protected(self.name, id, json.loads(value))

        return objects

    # -- Files ------------------------------------------------------------

    def _file_client(self) -> Any:
        """The byte-mode connection used for file content.

        Opened lazily: most adapters never touch files, and a second connection per adapter is not
        free on a small installation.
        """
        if self._files is None:
            self._files = connect_async(self._objects_cfg, decode=False)

        return self._files

    async def write_file(
        self, id: str, name: str, data: bytes | str, mime_type: str | None = None
    ) -> None:
        """Store a file in the object database.

        Files live in the database rather than on disk, which is what makes them survive a backup
        and reach every host in a multihost setup.

        :param id: owning id; pass the adapter namespace for its own files
        :param name: path within that id, e.g. ``icons/lamp.png``
        :param data: content, as bytes or text
        :param mime_type: override the type derived from the extension
        """
        is_text = isinstance(data, str)
        payload = data.encode() if is_text else data
        guessed, binary = guess_mime_type(name, is_text)
        now = now_ms()

        existing = await self.read_file_meta(id, name)
        meta = FileMeta(
            size=len(payload),
            mime_type=mime_type or guessed,
            binary=binary,
            created_at=existing.created_at if existing else now,
            modified_at=now,
            acl=existing.acl if existing else None,
        )

        data_key = file_key(FILES_PREFIX, id, name, "data")
        meta_key = file_key(FILES_PREFIX, id, name, "meta")

        client = self._file_client()
        await client.set(data_key, payload)
        # The JavaScript client publishes the byte length here, not the content.
        await client.publish(data_key, str(len(payload)))
        await self._objects.set(meta_key, json.dumps(meta.to_wire()))

    async def read_file(self, id: str, name: str) -> bytes | None:
        """Read a file's content.

        Always bytes, never text: the caller knows whether it stored an image or a JSON document,
        this layer does not, and guessing would corrupt one of the two.

        :param id: owning id
        :param name: path within that id
        """
        return await self._file_client().get(file_key(FILES_PREFIX, id, name, "data"))

    async def read_file_meta(self, id: str, name: str) -> FileMeta | None:
        """Read what is recorded about a file, without its content.

        :param id: owning id
        :param name: path within that id
        """
        raw = await self._objects.get(file_key(FILES_PREFIX, id, name, "meta"))

        return FileMeta.from_wire(json.loads(raw)) if raw else None

    async def unlink(self, id: str, name: str) -> None:
        """Delete a file, both its content and its metadata.

        :param id: owning id
        :param name: path within that id
        """
        data_key = file_key(FILES_PREFIX, id, name, "data")

        await self._file_client().delete(data_key)
        # Announce it, exactly as the JS client's `_delBinaryState` does. Without this a subscriber
        # on real Redis never learns the file is gone: a DEL notifies nobody there. The built-in
        # server does publish on the delete itself, which is what makes the omission invisible
        # until an installation moves to Redis.
        await self._file_client().publish(data_key, "null")
        await self._objects.delete(file_key(FILES_PREFIX, id, name, "meta"))

    async def read_dir(self, id: str, path: str = "") -> list[dict[str, Any]]:
        """List one level of the file store.

        The built-in server does not glob file keys the way it globs object keys: it treats the
        pattern as a directory and answers with one level of entries. Measured rather than assumed,
        because getting the shape wrong returns an empty list instead of an error --

            keys("cfg.f.<id>$%$*")          -> the top level
            keys("cfg.f.<id>$%$icons/*")    -> inside "icons"
            keys("cfg.f.<id>$%$icons")      -> nothing at all

        Subdirectories come back as a synthetic ``<dir>/_data.json`` entry, which is how they are
        told apart from files here.

        :param id: owning id
        :param path: directory within that id; empty for the top level
        :returns: one entry per name, each with ``file`` and ``is_dir``
        """
        prefix = normalize_name(path)

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        keys = await self._objects.keys(f"{FILES_PREFIX}{id}{FILE_SEPARATOR}{prefix}*")
        entries: dict[str, bool] = {}

        for key in keys:
            parts = split_file_key(FILES_PREFIX, key)

            if not parts:
                continue

            name = parts[1]

            # Only the meta half is listed; data and meta describe the same entry.
            if parts[2] != "meta":
                continue

            relative = name[len(prefix) :] if prefix and name.startswith(prefix) else name

            if relative.endswith("_data.json"):
                directory = relative[: -len("_data.json")].rstrip("/")
                if directory:
                    entries[directory] = True
            elif relative:
                entries[relative] = False

        return [{"file": name, "is_dir": is_dir} for name, is_dir in sorted(entries.items())]

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

    async def _open_subscription(self, client: Any, patterns: set[str]) -> Any:
        """Open a pub/sub connection and apply the recorded patterns."""
        sub = client.pubsub()

        for pattern in patterns:
            await sub.psubscribe(pattern)

        return sub

    async def _run_pump(self, kind: str) -> None:
        """Consume one pub/sub stream for as long as the adapter runs.

        Measured, not assumed: redis-py restores a dropped subscription by itself, and an adapter
        survives the database going away and coming back without any help from here. This loop is
        the backstop for the case where an error does escape ``listen()`` -- previously that ended
        the adapter, which cost a process restart and everything it held in memory.

        The backoff is capped rather than unbounded: an adapter that retries every 30 seconds
        during a long outage is preferable to one that has backed off to an hour and stays dark
        long after the database returned.
        """
        delay = 1.0

        while not self._stopping.is_set():
            sub = self._sub if kind == "states" else self._osub
            patterns = self._state_patterns if kind == "states" else self._object_patterns

            # listen() over a subscription with no patterns returns at once, which would turn this
            # into a busy loop. Most adapters never subscribe to objects, so this is the normal
            # case rather than an edge one.
            if not patterns:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=1.0)
                continue

            try:
                async for raw in sub.listen():
                    if raw.get("type") != "pmessage":
                        continue

                    delay = 1.0  # a message proves the connection works

                    try:
                        await self._dispatch(raw["channel"], raw["data"])
                    except Exception:  # noqa: BLE001
                        self.log.error(f"Failed to handle {raw['channel']}", exc_info=True)

                if self._stopping.is_set():
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if self._stopping.is_set():
                    return
                self.log.warn(f"{kind} subscription lost ({exc}); reconnecting in {delay:.0f}s")

            if self._stopping.is_set():
                return

            # Wait, but wake immediately when the adapter is asked to stop -- otherwise a shutdown
            # during an outage would hang for the length of the backoff.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)

            if self._stopping.is_set():
                return

            delay = min(delay * 2, 30.0)

            try:
                client = self._states if kind == "states" else self._objects

                with contextlib.suppress(Exception):
                    await sub.aclose()

                new_sub = await self._open_subscription(client, patterns)

                if kind == "states":
                    self._sub = new_sub
                else:
                    self._osub = new_sub

                # The backoff is deliberately not reset here. Reopening a subscription succeeds
                # even while the database is still gone, so resetting on that would keep retrying
                # every second for the whole outage. Only a message that actually arrives proves
                # the connection works, and that is where it resets.
                self.log.info(f"{kind} subscription reopened ({len(patterns)} pattern(s))")
            except Exception as exc:  # noqa: BLE001
                self.log.warn(f"Could not reopen the {kind} subscription: {exc}")

    async def _dispatch(self, channel: str, data: str) -> None:
        # Files travel on the objects connection as well, under their own prefix. Checked before
        # objects because `cfg.f.` and `cfg.o.` only differ in one character, and getting the order
        # wrong would hand a file change to `on_object_change` as an object that will not parse.
        if channel.startswith(FILES_PREFIX):
            rest = channel[len(FILES_PREFIX) :]
            owner, _, name = rest.partition(FILE_SEPARATOR)
            # The server appends `$%$meta` or `$%$data` to say which half changed; a subscriber
            # wants the file, not the storage detail.
            for suffix in (f"{FILE_SEPARATOR}meta", f"{FILE_SEPARATOR}data"):
                name = name.removesuffix(suffix)
            # The payload is the new byte length, or "null" for a deletion -- not the content.
            size = None if not data or data == "null" else json.loads(data)
            await self.on_file_change(owner, name, size)
            return

        # Object changes arrive on the objects connection and keep their prefix, unlike states.
        if channel.startswith(OBJECTS_PREFIX):
            obj_id = channel[len(OBJECTS_PREFIX) :]
            obj = None if not data or data == "null" else json.loads(data)
            # Keep the cached default ACL current: a change made in admin must reach new objects
            # without a restart, exactly as the JS client keeps it in step.
            if obj_id == "system.config":
                acl = ((obj or {}).get("common") or {}).get("defaultNewAcl")
                if acl:
                    self._default_new_acl = acl
            await self.on_object_change(obj_id, obj)
            return

        # Normalise the channel before deciding what it is. The two servers disagree about the
        # "io." prefix, and in opposite directions:
        #
        #   built-in server   states -> "pyexample.0.temp"     messagebox -> "io.messagebox.…"
        #   real Redis        states -> "io.pyexample.0.temp"  messagebox -> "messagebox.…"
        #
        # Stripping a leading "io." first and only then looking at the prefix covers both. Checking
        # for "messagebox." on the raw channel silently misses every message on the built-in server
        # and routes it into the state branch instead.
        if channel.startswith(STATES_PREFIX):
            channel = channel[len(STATES_PREFIX) :]

        # Someone else's log line. Like the messagebox, this has to be tested after the "io."
        # prefix has been stripped, because the two servers disagree about carrying it.
        if channel.startswith(LOG_PREFIX):
            with contextlib.suppress(json.JSONDecodeError):
                await self.on_log(json.loads(data))
            return

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

        # Whatever is left is a state id: the prefix was already removed above.
        state_id = channel

        # Controller stop signal. -1 means "terminate yourself"; any other value
        # is the PID of the process the controller believes it is supervising.
        # When that is not us, another supervisor took over (the instance was
        # started twice) and the stale process -- us -- must go, exactly like
        # adapter-core behaves. Only our own PID means "keep running".
        if state_id == f"{self.instance_id}.sigKill":
            if data and data != "null":
                with contextlib.suppress(Exception):
                    val = int(json.loads(data).get("val", 0))
                    if val == -1:
                        self.log.info("sigKill received -- shutting down")
                        self._stopping.set()
                    elif val != os.getpid():
                        self.log.warn(
                            f"sigKill carries PID {val}, ours is {os.getpid()} -- "
                            "another process supervises this instance now, shutting down"
                        )
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
        # Only the "true" carries an expiry, exactly as adapter-core does it: an explicit false
        # written on shutdown has to stay, while a true has to lapse if nothing refreshes it.
        await self.set_foreign_state(
            f"{self.instance_id}.alive",
            alive,
            ack=True,
            expire=_STATUS_EXPIRE_SECONDS if alive else None,
        )

    async def _set_connected(self, connected: bool) -> None:
        """Report the database link, the state admin shows next to an instance.

        adapter-core writes this in the same status report as ``alive``; without it an instance
        looks half-started in admin -- running, but never connected.
        """
        self.connected = connected
        await self.set_foreign_state(
            f"{self.instance_id}.connected",
            connected,
            ack=True,
            expire=_STATUS_EXPIRE_SECONDS if connected else None,
        )

    async def _heartbeat(self) -> None:
        """Keep alive/connected/uptime/memRss current -- just like a Node adapter does."""
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(15)
                if self._stopping.is_set():
                    break
                await self._set_alive(True)
                if self.connected:
                    await self._set_connected(True)
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
        """Shut the adapter down in an orderly fashion (exit code 0)."""
        self._stopping.set()

    def terminate(
        self, reason: str | None = None, exit_code: int = int(ExitCode.ADAPTER_REQUESTED_TERMINATION)
    ) -> None:
        """Ask the controller to stop this instance and **not** restart it.

        Sets the process exit code (``11`` -- "planned stop" -- by default) and starts an orderly
        shutdown, the same way ``adapter.terminate()`` does in the Node.js framework. Use it for an
        adapter that has finished its work (``once``/``schedule``) or that detected a condition
        under which it must not keep running.

        :param reason: logged as the reason for the stop
        :param exit_code: the code to exit with; ``START_IMMEDIATELY_AFTER_STOP`` (156) asks for a
            restart after 1 s instead
        """
        self.log.info(f"Terminating: {reason}" if reason else "Terminating")
        self._exit_code = int(exit_code)
        self._stopping.set()

    async def _shutdown(self) -> None:
        self.log.info(f"Adapter {self.namespace} is shutting down")
        with contextlib.suppress(Exception):
            await self.on_unload()
        for task in (self._alive, self._pump, self._opump):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        with contextlib.suppress(Exception):
            await self.set_state("info.connection", False, ack=True)
        with contextlib.suppress(Exception):
            await self._set_alive(False)
        with contextlib.suppress(Exception):
            await self._set_connected(False)
        for closable in (self._sub, self._osub, self._states, self._objects, self._files):
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

def _same_value(previous: Any, current: Any) -> bool:
    """Whether two state values count as unchanged, the way the JS client compares them.

    ``isDeepStrictEqual`` in the JS client keeps ``True`` and ``1`` apart, while Python's ``==``
    considers them equal -- and a switch flipping between ``1`` and ``True`` is exactly the case
    that would otherwise silently stop moving ``lc``.
    """
    if isinstance(previous, bool) != isinstance(current, bool):
        return False
    return previous == current


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
