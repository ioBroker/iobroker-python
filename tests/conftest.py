"""Fixtures for the test suite.

The unit tests run everywhere. The database-backed tests run against **two
backends** and are parametrized over both:

* ``redis`` -- a real Redis, the way a large installation runs. Taken from the
  environment: ``IOB_TEST_REDIS_HOST`` (default 127.0.0.1),
  ``IOB_TEST_REDIS_PORT`` (6379), ``IOB_TEST_REDIS_DB`` (15).
* ``builtin`` -- the databases built into js-controller (jsonl flavour), the
  way a default installation runs. Started as a private Node.js process from
  ``tests/builtin/server.mjs``; needs ``node`` and a ``npm ci`` in
  ``tests/builtin``.

A backend that is not available skips its half of the tests; set
``IOB_TEST_REQUIRE_REDIS=1`` / ``IOB_TEST_REQUIRE_BUILTIN=1`` (CI does) to turn
that skip into a failure, so a broken setup cannot pass as green.

Safety: the Redis database is flushed between tests, so it must be dedicated to
this suite. If it holds keys the suite cannot have written, everything
database-backed is refused rather than risking someone's installation. Note
that Redis pub/sub is server-wide, not per database -- running against the
Redis of a live ioBroker leaves its data untouched, but subscribers there may
briefly see events for the ``pytest*`` namespaces this suite publishes under.
The built-in servers are entirely private (fresh ports, temp data dir).
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis as redis_sync

from iobroker.adapter import Adapter
from iobroker.connection import (
    PROTOCOL_VERSION,
    DbConfig,
    check_protocol,
    connect,
    connect_async,
)
from support import Recorder

TEST_HOST = os.environ.get("IOB_TEST_REDIS_HOST", "127.0.0.1")
TEST_PORT = int(os.environ.get("IOB_TEST_REDIS_PORT", "6379"))
TEST_DB = int(os.environ.get("IOB_TEST_REDIS_DB", "15"))

BUILTIN_DIR = Path(__file__).parent / "builtin"

#: Keys the suite writes whose names carry no test namespace.
_ALLOWED_EXACT = {
    "meta.states.protocolVersion",
    "meta.objects.protocolVersion",
    "meta.objects.features.useSets",
    "cfg.o.system.config",
}


def _foreign_keys(client: redis_sync.Redis) -> list[str]:
    """Keys in the Redis database this suite cannot have written."""
    foreign = []
    for key in client.scan_iter(count=1000):
        name = key.decode() if isinstance(key, bytes) else key
        if name in _ALLOWED_EXACT or "pytest" in name or name.startswith("cfg.s.object.type."):
            continue
        foreign.append(name)
    return foreign


def _unavailable(reason: str, require_var: str) -> None:
    if os.environ.get(require_var):
        pytest.fail(reason)
    pytest.skip(reason)


class Backend:
    """One way of hosting the two databases: where they are and how to reset them.

    ``states``/``objects`` are separate configurations on purpose -- against a
    real Redis they point at the same server, against the built-in servers they
    are two different ports, just like on a real installation.
    """

    def __init__(self, kind: str, states: DbConfig, objects: DbConfig) -> None:
        self.kind = kind
        self.states = states
        self.objects = objects
        # The SDK's own sync client for housekeeping: it lowercases commands,
        # which the built-in servers require and a real Redis does not mind.
        self.states_sync = connect(states)
        self.objects_sync = connect(objects)

    @property
    def is_builtin_server(self) -> bool:
        return self.kind == "builtin"

    def clean(self) -> None:
        """A defined, empty starting state with the protocol versions in place."""
        if self.kind == "redis":
            self.states_sync.flushdb()
        else:
            # The built-in servers know no flushdb; everything in them is ours.
            for key in self.states_sync.keys("io.*"):
                with contextlib.suppress(Exception):
                    self.states_sync.delete(key)
            for key in self.objects_sync.keys("cfg.o.*"):
                with contextlib.suppress(Exception):
                    self.objects_sync.delete(key)
            # "0" reads the same as "absent" for every client, including the JS one.
            self.objects_sync.set("meta.objects.features.useSets", "0")

        self.states_sync.set("meta.states.protocolVersion", PROTOCOL_VERSION)
        self.objects_sync.set("meta.objects.protocolVersion", PROTOCOL_VERSION)

    def close(self) -> None:
        for client in (self.states_sync, self.objects_sync):
            with contextlib.suppress(Exception):
                client.close()


@pytest.fixture(scope="session")
def _redis_backend() -> Backend:
    probe = redis_sync.Redis(
        host=TEST_HOST,
        port=TEST_PORT,
        db=TEST_DB,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=10,
    )
    try:
        probe.ping()
    except Exception as exc:  # noqa: BLE001
        _unavailable(
            f"no Redis at {TEST_HOST}:{TEST_PORT} ({exc}) -- the Redis-backed tests need one",
            "IOB_TEST_REQUIRE_REDIS",
        )

    foreign = _foreign_keys(probe)
    probe.close()
    if foreign:
        _unavailable(
            f"Redis database {TEST_DB} at {TEST_HOST}:{TEST_PORT} holds {len(foreign)} key(s) "
            f"this suite did not write (e.g. {foreign[:3]}) -- refusing to flush it. "
            "Point IOB_TEST_REDIS_DB at an empty database.",
            "IOB_TEST_REQUIRE_REDIS",
        )

    cfg = DbConfig(host=TEST_HOST, port=TEST_PORT, db=TEST_DB, password=None, kind="redis")
    backend = Backend("redis", cfg, cfg)
    yield backend
    with contextlib.suppress(Exception):
        backend.clean()
    backend.close()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_ports(proc: subprocess.Popen, ports: list[int], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    remaining = list(ports)
    while remaining:
        if proc.poll() is not None:
            raise RuntimeError(f"server process ended with {proc.returncode} before listening")
        if time.monotonic() > deadline:
            raise TimeoutError(f"ports {remaining} did not open within {timeout}s")
        for port in list(remaining):
            with socket.socket() as sock:
                sock.settimeout(0.25)
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    remaining.remove(port)
        if remaining:
            time.sleep(0.2)


@pytest.fixture(scope="session")
def _builtin_backend(tmp_path_factory: pytest.TempPathFactory) -> Backend:
    if shutil.which("node") is None:
        _unavailable(
            "node is not available -- the built-in server tests need Node.js",
            "IOB_TEST_REQUIRE_BUILTIN",
        )
    if not (BUILTIN_DIR / "node_modules").is_dir():
        _unavailable(
            f"{BUILTIN_DIR / 'node_modules'} is missing -- run 'npm ci' in tests/builtin",
            "IOB_TEST_REQUIRE_BUILTIN",
        )

    states_port, objects_port = _free_port(), _free_port()
    data_dir = tmp_path_factory.mktemp("builtin-db")
    # The server's own output goes to a file, never to a pipe: an unread stderr
    # pipe deadlocks the child the moment it fills (~64 KB), which would freeze
    # the whole backend. The file is there to read when a test needs to know
    # why the server misbehaved.
    log_path = data_dir / "server.log"
    log_handle = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            shutil.which("node"),
            "server.mjs",
            f"--states-port={states_port}",
            f"--objects-port={objects_port}",
            f"--data-dir={data_dir}",
        ],
        cwd=BUILTIN_DIR,
        stdin=subprocess.PIPE,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_ports(proc, [states_port, objects_port], timeout=30)
    except Exception as exc:  # noqa: BLE001
        with contextlib.suppress(Exception):
            proc.kill()
        log_handle.close()
        tail = ""
        with contextlib.suppress(Exception):
            tail = log_path.read_text(encoding="utf-8")[-2000:]
        _unavailable(
            f"could not start the built-in databases: {exc}\n{tail}", "IOB_TEST_REQUIRE_BUILTIN"
        )

    backend = Backend(
        "builtin",
        DbConfig(host="127.0.0.1", port=states_port, db=0, password=None, kind="jsonl"),
        DbConfig(host="127.0.0.1", port=objects_port, db=0, password=None, kind="jsonl"),
    )
    yield backend
    backend.close()
    # Closing stdin is the shutdown signal; the hard kill is only the backstop.
    with contextlib.suppress(Exception):
        proc.stdin.close()
    try:
        proc.wait(timeout=10)
    except Exception:  # noqa: BLE001
        proc.kill()
    with contextlib.suppress(Exception):
        log_handle.close()


@pytest.fixture(scope="session", params=["redis", "builtin"])
def _backend(request: pytest.FixtureRequest) -> Backend:
    return request.getfixturevalue(f"_{request.param}_backend")


@pytest.fixture
def db(_backend: Backend) -> Backend:
    """A cleaned backend with the protocol version keys seeded."""
    _backend.clean()
    return _backend


@pytest.fixture
async def raw(db: Backend):
    """A decoded async client on the **states** database, for driving it the way
    js-controller would."""
    client = connect_async(db.states)
    yield client
    await client.aclose()


@pytest.fixture
async def raw_objects(db: Backend):
    """A decoded async client on the **objects** database."""
    client = connect_async(db.objects)
    yield client
    await client.aclose()


@pytest.fixture
async def adapter(db: Backend):
    """An ``Adapter`` wired to the databases without its event loop running.

    CRUD does not need the pumps; the lifecycle tests run the real thing via
    ``run_adapter``.
    """
    a = Adapter("pytest", instance=0)
    a._objects_cfg = db.objects
    a._states = connect_async(db.states)
    a._objects = connect_async(db.objects)
    await check_protocol(a._states, "states")
    await check_protocol(a._objects, "objects")
    yield a
    for client in (a._states, a._objects, a._files):
        if client is not None:
            await client.aclose()


@pytest.fixture
async def run_adapter(db: Backend, monkeypatch: pytest.MonkeyPatch):
    """Factory that starts a complete adapter -- config, subscriptions, pumps -- and
    tears it down again.

    The connection settings go through the ``IOB_*`` environment variables,
    which also exercises the path py-controller will use in production.
    """
    import asyncio

    for section, cfg in (("STATES", db.states), ("OBJECTS", db.objects)):
        monkeypatch.setenv(f"IOB_{section}_HOST", cfg.host)
        monkeypatch.setenv(f"IOB_{section}_PORT", str(cfg.port))
        monkeypatch.setenv(f"IOB_{section}_DB", str(cfg.db))
        monkeypatch.setenv(f"IOB_{section}_TYPE", cfg.kind)
        monkeypatch.delenv(f"IOB_{section}_PASS", raising=False)
    for var in ("IOB_CONFIG", "IOB_INSTANCE", "IOB_LOGLEVEL"):
        monkeypatch.delenv(var, raising=False)

    started: list[tuple[Recorder, asyncio.Task]] = []

    async def start(name: str = "pytestrun", instance: int = 0) -> tuple[Recorder, asyncio.Task]:
        a = Recorder(name, instance=instance)
        task = asyncio.create_task(a._main())
        ready = asyncio.create_task(a.ready.wait())
        done, _ = await asyncio.wait({task, ready}, timeout=15, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            ready.cancel()
            raise AssertionError(f"adapter ended during startup: {task.exception()}")
        if not done:
            ready.cancel()
            task.cancel()
            raise AssertionError("adapter did not become ready within 15s")
        started.append((a, task))
        return a, task

    yield start

    for a, task in started:
        a.stop()
        try:
            await asyncio.wait_for(task, timeout=10)
        except Exception:  # noqa: BLE001
            task.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await task
