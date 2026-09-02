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
        self._secret: str | None = None

        # Kept so subscriptions can be restored after a reconnect. redis-py restores its own
        # record, but the built-in server echoes patterns back in a different form than it was
        # given, so relying on that bookkeeping is not safe here.
        self._state_patterns: set[str] = set()
        self._object_patterns: set[str] = set()
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

        self._objects_cfg = objects_cfg
        self._states = connect_async(states_cfg)
        self._objects = connect_async(objects_cfg)
        await check_protocol(self._states, "states")
        await check_protocol(self._objects, "objects")

        await self._load_config()
        self._install_signal_handlers()

        # Our own messagebox and the controller's stop signal.
        self._state_patterns.add(f"{MESSAGE_PREFIX}{self.instance_id}")
        self._state_patterns.add(f"{STATES_PREFIX}{self.instance_id}.sigKill")

        self._sub = await self._open_subscription(self._states, self._state_patterns)
        self._pump = asyncio.create_task(self._run_pump("states"))

        self._osub = await self._open_subscription(self._objects, self._object_patterns)
        self._opump = asyncio.create_task(self._run_pump("objects"))

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
        await self._file_client().delete(file_key(FILES_PREFIX, id, name, "data"))
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
        # Object changes arrive on the objects connection and keep their prefix, unlike states.
        if channel.startswith(OBJECTS_PREFIX):
            obj_id = channel[len(OBJECTS_PREFIX) :]
            obj = None if not data or data == "null" else json.loads(data)
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
        for task in (self._alive, self._pump, self._opump):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        with contextlib.suppress(Exception):
            await self.set_state("info.connection", False, ack=True)
        with contextlib.suppress(Exception):
            await self._set_alive(False)
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
