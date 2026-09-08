"""Against a running Redis Sentinel: the client follows the master when it moves.

A sentinel setup is the one topology where the database's address is not configuration but a
question asked at connect time -- and where the answer changes while an adapter is running. Both
halves are measured here rather than reasoned about:

* :class:`TestDiscovery` -- the address is discovered at all, through the same
  ``load_db_config`` -> ``connect_async`` path an adapter takes.
* :class:`TestFailover` -- after the master moves, a client that was already open writes to the
  *new* one, and an adapter that was already subscribed keeps receiving states.

Skipped unless ``IOB_TEST_SENTINELS`` names at least one sentinel (``host:port``, comma
separated). CI starts a master, a replica and a sentinel and sets it, together with
``IOB_TEST_REQUIRE_SENTINEL=1`` so that a broken setup fails the job instead of passing as
"everything skipped".

The failover is *forced* (``SENTINEL FAILOVER``) rather than provoked by killing the master: it
takes a second instead of the ``down-after-milliseconds`` wait, and what is under test is the
client's reaction to a moved master, not the sentinel's ability to notice a dead one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid

import pytest
import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from iobroker.connection import PROTOCOL_VERSION, connect_async, load_db_config
from support import Recorder, drive, wire_state, write_state

SENTINELS = os.environ.get("IOB_TEST_SENTINELS", "").strip()
MASTER_NAME = os.environ.get("IOB_TEST_SENTINEL_NAME", "mymaster")
REQUIRED = os.environ.get("IOB_TEST_REQUIRE_SENTINEL")

if not SENTINELS:
    pytest.skip(
        "no sentinels configured -- set IOB_TEST_SENTINELS=host:port[,host:port]",
        allow_module_level=True,
    )


def _first_sentinel() -> tuple[str, int]:
    """Address of the sentinel the tests talk to directly, to ask about and force a failover."""
    host, _, port = SENTINELS.split(",")[0].strip().rpartition(":")
    return host, int(port)


@pytest.fixture
def sentinel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment js-controller sets for a sentinel installation."""
    for section in ("STATES", "OBJECTS"):
        for suffix in ("HOST", "PORT", "DB", "PASS"):
            monkeypatch.delenv(f"IOB_{section}_{suffix}", raising=False)
        monkeypatch.setenv(f"IOB_{section}_SENTINELS", SENTINELS)
        monkeypatch.setenv(f"IOB_{section}_SENTINEL_NAME", MASTER_NAME)
        monkeypatch.setenv(f"IOB_{section}_TYPE", "redis")
    for var in ("IOB_CONFIG", "IOB_INSTANCE", "IOB_LOGLEVEL"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
async def sentinel_client():
    """A plain connection to the first sentinel, for asking who the master is."""
    host, port = _first_sentinel()
    # Short and without retries: when there is no sentinel the point is to skip, and redis-py's
    # default of three attempts with backoff turns that into half a minute of waiting per test.
    client = aioredis.Redis(
        host=host,
        port=port,
        decode_responses=True,
        protocol=2,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry=Retry(NoBackoff(), 0),
    )
    try:
        try:
            await client.ping()
        except Exception as exc:  # noqa: BLE001
            message = f"no sentinel at {host}:{port}: {exc}"
            if REQUIRED:
                pytest.fail(message)
            pytest.skip(message)
        yield client
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


@pytest.fixture
async def master(sentinel_env, sentinel_client):
    """A sentinel-backed client on the master, on a database an adapter can start against.

    The two protocol keys are what ``check_protocol`` reads before anything else; a bare Redis
    has neither, and js-controller would have written them. The session fixture in conftest.py
    does the same for the ordinary backends.
    """
    client = connect_async(load_db_config("states"))
    try:
        await client.set("meta.states.protocolVersion", PROTOCOL_VERSION)
        await client.set("meta.objects.protocolVersion", PROTOCOL_VERSION)
        yield client
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


async def _master_address(sentinel: aioredis.Redis) -> tuple[str, int]:
    """Who the sentinels currently consider the master."""
    address = await sentinel.sentinel_get_master_addr_by_name(MASTER_NAME)
    if not address:
        raise AssertionError(f"the sentinel monitors no group named {MASTER_NAME!r}")
    return address[0], int(address[1])


async def _force_failover(sentinel: aioredis.Redis, timeout: float = 60.0) -> tuple[str, int]:
    """Ask the sentinels to promote a replica, and wait until they have.

    :param sentinel: a connection to one of the sentinels
    :param timeout: how long to wait for the promotion, in seconds
    :returns: the address of the new master
    :raises AssertionError: when the master has not moved within ``timeout``
    """
    before = await _master_address(sentinel)
    await sentinel.execute_command("SENTINEL", "FAILOVER", MASTER_NAME)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        await asyncio.sleep(0.5)
        now = await _master_address(sentinel)
        if now != before:
            return now

    raise AssertionError(f"the master was still {before} after {timeout:.0f}s")


async def _follow_the_failover(
    client: aioredis.Redis, expected: tuple[str, int], timeout: float = 90.0
) -> None:
    """Wait until an already-open client is talking to the new master, writing as it goes.

    A pool with a healthy connection does not re-check who the master is: redis-py resolves the
    address on *connect*, so a client follows a failover only once its socket to the old master
    goes away. Measured against a real sentinel, that sequence is

    * +1s   the sentinels answer with the promoted replica,
    * +11s  the sentinels have reconfigured the old master into a replica of it,
    * then   the next command reconnects and lands on the new master -- with no error surfacing.

    The ten seconds in the middle are Sentinel's own reconfiguration, not a client problem: any
    client, ioredis in js-controller included, writes to the old master during them. Which is why
    this waits rather than asserting straight after the promotion, and why it keeps issuing
    commands -- an idle client has no reason to notice anything at all.

    :param client: the client that was open before the failover
    :param expected: address the sentinels now name
    :param timeout: how long to allow, in seconds
    :raises AssertionError: when the client is still on the old master after ``timeout``
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last: Exception | None = None

    while loop.time() < deadline:
        with contextlib.suppress(Exception):
            await client.set("io.pytest.0.sentinel-probe", wire_state(0))

        if client.connection_pool.master_address == expected:
            return

        await asyncio.sleep(1)

    raise AssertionError(
        f"the client was still on {client.connection_pool.master_address} after {timeout:.0f}s"
        f", not {expected} ({last!r})"
    )


class TestDiscovery:
    """The ordinary path: an adapter's configuration turns into a working connection."""

    async def test_the_master_is_found_and_answers(self, master, sentinel_client) -> None:
        cfg = load_db_config("states")
        assert cfg.uses_sentinel, "the environment did not produce a sentinel configuration"

        key = f"io.pytest.0.sentinel-{uuid.uuid4().hex}"
        await master.set(key, wire_state(1))

        assert json.loads(await master.get(key))["val"] == 1
        # It went where the sentinels point, rather than to an address of its own.
        assert master.connection_pool.master_address == await _master_address(sentinel_client)


class TestFailover:
    """What a moved master does to connections that were already open."""

    async def test_an_open_client_writes_to_the_new_master(
        self, master, sentinel_client
    ) -> None:
        before = await _master_address(sentinel_client)
        await master.set(f"io.pytest.0.before-{uuid.uuid4().hex}", wire_state(1))
        assert master.connection_pool.master_address == before

        after = await _force_failover(sentinel_client)
        assert after != before, "the failover did not move the master"

        await _follow_the_failover(master, after)

        # And it is a working connection, not merely a re-resolved address. Nobody rebuilt this
        # client: it is the same object that was writing to the other server a moment ago.
        key = f"io.pytest.0.after-{uuid.uuid4().hex}"
        await master.set(key, wire_state(2))

        assert json.loads(await master.get(key))["val"] == 2
        assert master.connection_pool.master_address == after

    async def test_an_adapter_keeps_receiving_states(self, master, sentinel_client) -> None:
        """The point of the exercise: a running adapter does not need restarting.

        A failover reaches the adapter as a dropped subscription, which ``_run_pump`` reopens and
        re-subscribes -- the same machinery ``test_reconnect.py`` measures against a cut wire.
        Here it meets the other half: the reopened connection lands on a *different* server.
        """
        adapter = Recorder("pytestsentinel", instance=0)
        task = asyncio.create_task(adapter._main())
        ready = asyncio.create_task(adapter.ready.wait())
        done, _ = await asyncio.wait(
            {task, ready}, timeout=30, return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            ready.cancel()
            raise AssertionError(f"adapter ended during startup: {task.exception()}")
        if not done:
            ready.cancel()
            task.cancel()
            raise AssertionError("adapter did not become ready within 30s")

        try:
            await adapter.subscribe_foreign_states("pytestsentinel.0.*")

            await drive(
                lambda: write_state(master, "pytestsentinel.0.x", wire_state(1)),
                adapter.state_events,
                lambda ev: ev[0] == "pytestsentinel.0.x" and ev[1] is not None and ev[1].val == 1,
            )

            await _force_failover(sentinel_client)

            # drive() keeps re-publishing, which carries the assertion across the pump's backoff
            # -- and the publisher has to follow the master too, so this exercises both ends.
            await drive(
                lambda: write_state(master, "pytestsentinel.0.x", wire_state(2)),
                adapter.state_events,
                lambda ev: ev[0] == "pytestsentinel.0.x" and ev[1] is not None and ev[1].val == 2,
                attempts=20,
                wait=3.0,
            )

            assert not adapter._stopping.is_set(), "the failover stopped the adapter"
        finally:
            adapter.stop()
            try:
                await asyncio.wait_for(task, timeout=15)
            except Exception:  # noqa: BLE001
                task.cancel()
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await task
