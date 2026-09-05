"""Shared plumbing for the database-backed tests.

Two kinds of helpers live here: drivers that write states and objects the way
js-controller does (SET plus PUBLISH on the same key), and an ``Adapter``
subclass that records every callback so tests can assert on what reached it.

The publish/wait helpers retry. A PSUBSCRIBE is confirmed asynchronously, so a
publish issued right after subscribing can be processed by the server before
the subscription is -- the message is then simply gone. Retrying the publish is
harmless here because every caller filters with a predicate and the payloads
are idempotent.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Awaitable, Callable

import pytest

from iobroker.adapter import Adapter
from iobroker.types import now_ms


def only_real_redis(db: Any, why: str) -> None:
    """Skip on the built-in backend -- for tests whose *assertions* need Redis
    commands the built-in servers do not implement (ttl, smembers, ...)."""
    if db.is_builtin_server:
        pytest.skip(f"needs a real Redis: {why}")


def only_builtin(db: Any, why: str) -> None:
    """Skip on the Redis backend -- for behaviour only the built-in servers show."""
    if not db.is_builtin_server:
        pytest.skip(f"built-in server behaviour: {why}")


def wire_state(val: Any, ack: bool = False, **extra: Any) -> str:
    """A state payload in the shape js-controller writes."""
    now = now_ms()
    return json.dumps(
        {"val": val, "ack": ack, "ts": now, "lc": now, "q": 0, "from": "system.host.test", **extra}
    )


async def write_state(client: Any, id: str, payload: str) -> None:
    """Write and publish a state the way the JS states client does."""
    key = f"io.{id}"
    await client.set(key, payload)
    await client.publish(key, payload)


async def write_object(client: Any, id: str, obj: dict[str, Any]) -> None:
    """Write and publish an object the way the JS objects client does."""
    key = f"cfg.o.{id}"
    payload = json.dumps(obj)
    await client.set(key, payload)
    await client.publish(key, payload)


async def delete_state(client: Any, id: str) -> None:
    """Delete and signal a state the way the JS states client does.

    Both halves are needed for a backend-agnostic deletion: real Redis notifies
    subscribers only on the PUBLISH, while the built-in server notifies on the
    DEL and deliberately swallows a bare PUBLISH on the states namespace. Doing
    both delivers exactly one deletion on either backend.
    """
    key = f"io.{id}"
    await client.delete(key)
    await client.publish(key, "null")


async def delete_object(client: Any, id: str) -> None:
    """Delete and signal an object the way the JS objects client does (see
    :func:`delete_state` for why both DEL and PUBLISH are sent)."""
    key = f"cfg.o.{id}"
    await client.delete(key)
    await client.publish(key, "null")


async def read_state(client: Any, id: str) -> dict[str, Any] | None:
    """A state as stored, parsed but not converted -- for asserting on the wire shape."""
    raw = await client.get(f"io.{id}")
    return json.loads(raw) if raw else None


async def expect_only_marker(
    queue: asyncio.Queue,
    marker: Callable[[Any], bool],
    forbidden: Callable[[Any], bool],
    timeout: float = 8.0,
) -> None:
    """Assert that ``forbidden`` never arrives, using ``marker`` as proof of delivery.

    A plain "wait and see nothing" cannot tell an unsubscribed pattern from a slow one -- it passes
    just as well when the whole pipeline is dead. So the caller publishes the forbidden event first
    and the marker second: pub/sub keeps order on a connection, so by the time the marker has
    arrived the forbidden one has had its chance and did not take it.
    """
    deadline = time.monotonic() + timeout

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError("the marker never arrived -- the subscription is not alive")
        try:
            item = await asyncio.wait_for(queue.get(), remaining)
        except asyncio.TimeoutError:
            raise AssertionError("the marker never arrived -- the subscription is not alive") from None

        if forbidden(item):
            raise AssertionError(f"an unsubscribed pattern still delivered {item!r}")
        if marker(item):
            return


async def expect_event(
    queue: asyncio.Queue, pred: Callable[[Any], bool] | None = None, timeout: float = 8.0
) -> Any:
    """The next queued event matching ``pred``; skips what does not match."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError("expected event did not arrive in time")
        try:
            item = await asyncio.wait_for(queue.get(), remaining)
        except asyncio.TimeoutError:
            raise AssertionError("expected event did not arrive in time") from None
        if pred is None or pred(item):
            return item


async def drive(
    publish: Callable[[], Awaitable[Any]],
    queue: asyncio.Queue,
    pred: Callable[[Any], bool] | None = None,
    attempts: int = 4,
    wait: float = 2.0,
) -> Any:
    """Publish and wait for the resulting event, retrying the publish (see module docstring)."""
    for attempt in range(attempts):
        await publish()
        try:
            return await expect_event(queue, pred, timeout=wait)
        except AssertionError:
            if attempt == attempts - 1:
                raise AssertionError(f"event did not arrive after {attempts} publishes") from None


async def expect_pmessage(
    ps: Any,
    publish: Callable[[], Awaitable[Any]],
    pred: Callable[[dict], bool] | None = None,
    attempts: int = 4,
    wait: float = 2.0,
) -> dict:
    """Like :func:`drive`, for a raw redis-py pubsub instead of a Recorder queue."""
    for _ in range(attempts):
        await publish()
        deadline = time.monotonic() + wait
        while (remaining := deadline - time.monotonic()) > 0:
            msg = await ps.get_message(ignore_subscribe_messages=True, timeout=min(remaining, 1.0))
            if msg and msg.get("type") == "pmessage" and (pred is None or pred(msg)):
                return msg
    raise AssertionError(f"pmessage did not arrive after {attempts} publishes")


class Recorder(Adapter):
    """An adapter that records every callback, for asserting on afterwards."""

    def __init__(self, name: str, instance: int | None = None) -> None:
        super().__init__(name, instance=instance)
        self.ready = asyncio.Event()
        self.state_events: asyncio.Queue = asyncio.Queue()
        self.object_events: asyncio.Queue = asyncio.Queue()
        self.messages: asyncio.Queue = asyncio.Queue()
        self.unloaded = False

    async def on_ready(self) -> None:
        self.ready.set()

    async def on_state_change(self, id: str, state: Any) -> None:
        await self.state_events.put((id, state))

    async def on_object_change(self, id: str, obj: Any) -> None:
        await self.object_events.put((id, obj))

    async def on_message(self, msg: Any) -> None:
        await self.messages.put(msg)

    async def on_unload(self) -> None:
        self.unloaded = True
