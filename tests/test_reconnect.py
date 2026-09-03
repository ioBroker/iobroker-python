"""Surviving an outage: the pumps reconnect and re-subscribe after the connection drops.

``_run_pump`` carries the retry logic an adapter depends on when the database goes away and comes
back. Its docstring says the behaviour was measured rather than assumed -- this is the measurement,
automated.

The database server itself is never killed: it is shared by the whole session, and killing it would
take every later test with it. Instead the adapter connects through a TCP relay that can drop every
connection on command. That reproduces exactly what the adapter sees (its socket dies, reconnects
fail for a while, then succeed) while the server keeps running and its data intact -- and it works
the same on both backends.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from support import Recorder, drive, wire_state, write_object, write_state


class Relay:
    """A TCP relay in front of a database, so a test can cut the wire and mend it again."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self._server: asyncio.AbstractServer | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._open = True
        self.port = 0

    @classmethod
    async def start(cls, host: str, port: int) -> "Relay":
        relay = cls(host, port)
        relay._server = await asyncio.start_server(relay._handle, "127.0.0.1", 0)
        relay.port = relay._server.sockets[0].getsockname()[1]
        return relay

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if not self._open:
            writer.close()
            return
        try:
            target_reader, target_writer = await asyncio.open_connection(*self._target)
        except Exception:  # noqa: BLE001
            writer.close()
            return

        self._writers.update({writer, target_writer})

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            except Exception:  # noqa: BLE001
                pass
            finally:
                with contextlib.suppress(Exception):
                    dst.close()

        await asyncio.gather(
            pipe(reader, target_writer), pipe(target_reader, writer), return_exceptions=True
        )

    def cut(self) -> None:
        """Drop every live connection and refuse new ones -- the database is 'gone'."""
        self._open = False
        for writer in list(self._writers):
            with contextlib.suppress(Exception):
                writer.close()
        self._writers.clear()

    def mend(self) -> None:
        """Let connections through again -- the database is 'back'."""
        self._open = True

    async def close(self) -> None:
        self.cut()
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()


@pytest.fixture
async def through_a_relay(db, monkeypatch: pytest.MonkeyPatch):
    """Start an adapter whose two connections run through relays the test can cut.

    Yields ``(adapter, states_relay, objects_relay)``.
    """
    states_relay = await Relay.start(db.states.host, db.states.port)
    objects_relay = await Relay.start(db.objects.host, db.objects.port)

    for section, cfg, relay in (
        ("STATES", db.states, states_relay),
        ("OBJECTS", db.objects, objects_relay),
    ):
        monkeypatch.setenv(f"IOB_{section}_HOST", "127.0.0.1")
        monkeypatch.setenv(f"IOB_{section}_PORT", str(relay.port))
        monkeypatch.setenv(f"IOB_{section}_DB", str(cfg.db))
        monkeypatch.setenv(f"IOB_{section}_TYPE", cfg.kind)
        monkeypatch.delenv(f"IOB_{section}_PASS", raising=False)
    for var in ("IOB_CONFIG", "IOB_INSTANCE", "IOB_LOGLEVEL"):
        monkeypatch.delenv(var, raising=False)

    adapter = Recorder("pytestrecon", instance=0)
    task = asyncio.create_task(adapter._main())
    ready = asyncio.create_task(adapter.ready.wait())
    done, _ = await asyncio.wait({task, ready}, timeout=20, return_when=asyncio.FIRST_COMPLETED)
    if task in done:
        ready.cancel()
        raise AssertionError(f"adapter ended during startup: {task.exception()}")
    if not done:
        ready.cancel()
        task.cancel()
        raise AssertionError("adapter did not become ready within 20s")

    yield adapter, states_relay, objects_relay

    adapter.stop()
    try:
        await asyncio.wait_for(task, timeout=15)
    except Exception:  # noqa: BLE001
        task.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await task
    await states_relay.close()
    await objects_relay.close()


class TestStateSubscriptionSurvivesAnOutage:
    async def test_events_resume_after_the_connection_drops(
        self, through_a_relay, raw
    ) -> None:
        adapter, states_relay, _ = through_a_relay
        await adapter.subscribe_foreign_states("pytestrecon.0.*")

        # Baseline: the subscription works before the outage.
        await drive(
            lambda: write_state(raw, "pytestrecon.0.x", wire_state(1)),
            adapter.state_events,
            lambda ev: ev[0] == "pytestrecon.0.x" and ev[1] is not None and ev[1].val == 1,
        )

        states_relay.cut()
        # Give the pump a moment to notice the dead socket before the database "returns".
        await asyncio.sleep(2)
        states_relay.mend()

        # The pattern was registered before the outage; it must be back in force afterwards.
        # drive() keeps re-publishing, which carries us across the reconnect backoff.
        await drive(
            lambda: write_state(raw, "pytestrecon.0.x", wire_state(2)),
            adapter.state_events,
            lambda ev: ev[0] == "pytestrecon.0.x" and ev[1] is not None and ev[1].val == 2,
            attempts=12,
            wait=3.0,
        )

    async def test_the_adapter_stays_alive_through_the_outage(
        self, through_a_relay, raw
    ) -> None:
        # A dropped connection used to end the adapter, which cost a process restart and
        # everything it held in memory.
        adapter, states_relay, _ = through_a_relay
        await adapter.subscribe_foreign_states("pytestrecon.0.*")

        states_relay.cut()
        await asyncio.sleep(3)
        states_relay.mend()

        assert not adapter._stopping.is_set(), "the outage stopped the adapter"

        await drive(
            lambda: write_state(raw, "pytestrecon.0.alive", wire_state("back")),
            adapter.state_events,
            lambda ev: ev[0] == "pytestrecon.0.alive" and ev[1] is not None,
            attempts=12,
            wait=3.0,
        )


class TestObjectSubscriptionSurvivesAnOutage:
    async def test_object_events_resume_after_the_connection_drops(
        self, through_a_relay, raw_objects
    ) -> None:
        adapter, _, objects_relay = through_a_relay
        await adapter.subscribe_foreign_objects("pytestrecon.0.*")

        await drive(
            lambda: write_object(
                raw_objects,
                "pytestrecon.0.o",
                {"type": "state", "common": {"name": "before"}, "native": {}},
            ),
            adapter.object_events,
            lambda ev: ev[0] == "pytestrecon.0.o"
            and ev[1] is not None
            and ev[1]["common"]["name"] == "before",
        )

        objects_relay.cut()
        await asyncio.sleep(2)
        objects_relay.mend()

        await drive(
            lambda: write_object(
                raw_objects,
                "pytestrecon.0.o",
                {"type": "state", "common": {"name": "after"}, "native": {}},
            ),
            adapter.object_events,
            lambda ev: ev[0] == "pytestrecon.0.o"
            and ev[1] is not None
            and ev[1]["common"]["name"] == "after",
            attempts=12,
            wait=3.0,
        )


class TestWritesRecover:
    async def test_a_write_succeeds_again_once_the_database_is_back(
        self, through_a_relay
    ) -> None:
        # Not only the subscription: the ordinary command connection must recover too, otherwise
        # the adapter would keep receiving events it can no longer act on.
        adapter, states_relay, _ = through_a_relay

        states_relay.cut()
        with contextlib.suppress(Exception):
            await adapter.set_state("recovered", "no", ack=True)
        states_relay.mend()

        last = None
        for _ in range(12):
            try:
                await adapter.set_state("recovered", "yes", ack=True)
                if (await adapter.get_state("recovered")) is not None:
                    break
            except Exception as exc:  # noqa: BLE001
                last = exc
            await asyncio.sleep(1)
        else:
            raise AssertionError(f"writes did not recover: {last}")

        state = await adapter.get_state("recovered")
        assert state is not None and state.val == "yes"
