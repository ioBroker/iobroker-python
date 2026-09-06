"""A complete adapter against a real database -- start to stop.

This is the counterpart of js-controller's ``test/lib/testAdapter.ts`` family:
the adapter runs its real ``_main`` (configuration, subscriptions, pumps), the
tests drive it from the outside the way js-controller and other adapters
would, and every observation is made through the database or the callbacks.
"""

from __future__ import annotations

import asyncio
import json
import os

from iobroker.crypto import decrypt
from iobroker.types import now_ms
from support import (
    only_real_redis,
    delete_object,
    delete_state,
    drive,
    expect_event,
    expect_only_marker,
    expect_pmessage,
    read_state,
    wire_state,
    write_object,
    write_state,
)


class TestStartup:
    async def test_presence_is_written(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()

        alive = await read_state(raw, f"{a.instance_id}.alive")
        connection = await read_state(raw, f"{a.namespace}.info.connection")

        assert alive["val"] is True and alive["ack"] is True
        assert alive["from"] == a.instance_id
        assert connection["val"] is False and connection["ack"] is True

    async def test_instance_configuration_is_loaded(self, run_adapter, raw_objects) -> None:
        await write_object(
            raw_objects,
            "system.adapter.pytestcfg.0",
            {
                "_id": "system.adapter.pytestcfg.0",
                "type": "instance",
                "common": {"loglevel": "debug"},
                "native": {"host": "device.local", "interval": 30},
            },
        )

        a, _task = await run_adapter("pytestcfg")

        assert a.config == {"host": "device.local", "interval": 30}
        assert a._loglevel == "debug"


class TestStateSubscriptions:
    async def test_own_pattern_delivers_changes(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_states("*")

        id, state = await drive(
            lambda: write_state(raw, f"{a.namespace}.temp", wire_state(21.5, ack=True)),
            a.state_events,
            lambda e: e[0] == f"{a.namespace}.temp",
        )

        assert state.val == 21.5
        assert state.ack is True

    async def test_deletion_arrives_as_none(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_states("*")
        await write_state(raw, f"{a.namespace}.gone", wire_state(1))
        await drive(
            lambda: write_state(raw, f"{a.namespace}.gone", wire_state(1)),
            a.state_events,
            lambda e: e[0] == f"{a.namespace}.gone" and e[1] is not None,
        )

        _id, state = await drive(
            lambda: delete_state(raw, f"{a.namespace}.gone"),
            a.state_events,
            lambda e: e[0] == f"{a.namespace}.gone" and e[1] is None,
        )

        assert state is None

    async def test_foreign_pattern_delivers_changes(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_states("pytestext.0.*")

        id, state = await drive(
            lambda: write_state(raw, "pytestext.0.reading", wire_state(3)),
            a.state_events,
            lambda e: e[0] == "pytestext.0.reading",
        )

        assert state.val == 3

    async def test_the_own_write_comes_back_through_the_subscription(self, run_adapter) -> None:
        # An adapter hears its own setState like every other subscriber -- the JS
        # stack behaves the same, and dedupe is the adapter author's business.
        a, _task = await run_adapter()
        await a.subscribe_states("*")

        _id, state = await drive(
            lambda: a.set_state("echo", 1, ack=True),
            a.state_events,
            lambda e: e[0] == f"{a.namespace}.echo",
        )

        assert state.val == 1


class TestUnsubscribing:
    """Taking a subscription back -- both halves of it.

    The server call is what stops the traffic now; removing the pattern from the recorded set is
    what keeps it from coming back on the next reconnect, since that set is what gets replayed.
    A test that only checked the first half would pass for hours and then fail after an outage.
    """

    async def test_a_state_pattern_stops_delivering(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_states("pytestext.0.*")
        await a.subscribe_foreign_states("pytestmark.0.*")

        # Prove it was live before, or the test below proves nothing.
        await drive(
            lambda: write_state(raw, "pytestext.0.reading", wire_state(1)),
            a.state_events,
            lambda e: e[0] == "pytestext.0.reading",
        )

        await a.unsubscribe_foreign_states("pytestext.0.*")

        await write_state(raw, "pytestext.0.reading", wire_state(2))
        await write_state(raw, "pytestmark.0.ping", wire_state(1))

        await expect_only_marker(
            a.state_events,
            marker=lambda e: e[0] == "pytestmark.0.ping",
            forbidden=lambda e: e[0] == "pytestext.0.reading",
        )

    async def test_the_pattern_is_not_replayed_after_a_reconnect(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_states("pytestext.0.*")
        assert "io.pytestext.0.*" in a._state_patterns

        await a.unsubscribe_foreign_states("pytestext.0.*")

        assert "io.pytestext.0.*" not in a._state_patterns, (
            "a pattern left in the recorded set comes back on the next reconnect"
        )

    async def test_the_own_namespace_form(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_states("*")
        assert f"io.{a.namespace}.*" in a._state_patterns

        await a.unsubscribe_states("*")

        assert f"io.{a.namespace}.*" not in a._state_patterns

    async def test_an_object_pattern(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_objects("pytestext.0.*")
        assert "cfg.o.pytestext.0.*" in a._object_patterns

        await a.unsubscribe_foreign_objects("pytestext.0.*")

        assert "cfg.o.pytestext.0.*" not in a._object_patterns

    async def test_only_the_exact_pattern_goes(self, run_adapter) -> None:
        # Removing a subscription, not cancelling everything it would overlap -- the same rule
        # Redis itself follows, and the one the JS adapter follows.
        a, _task = await run_adapter()
        await a.subscribe_foreign_states("pytestext.0.*")
        await a.subscribe_foreign_states("pytestext.0.reading")

        await a.unsubscribe_foreign_states("pytestext.0.*")

        assert "io.pytestext.0.reading" in a._state_patterns

    async def test_an_unknown_pattern_is_ignored(self, run_adapter) -> None:
        # A script engine tearing a script down should not have to know whether a neighbour still
        # holds the same pattern.
        a, _task = await run_adapter()
        await a.unsubscribe_foreign_states("never.0.subscribed")

    async def test_the_adapters_own_patterns_are_refused(self, run_adapter) -> None:
        # sigKill is how the controller stops this process. An adapter that unsubscribed it would
        # simply stop responding to `iobroker stop`, and nothing about that symptom points here.
        a, _task = await run_adapter()
        sig = f"io.{a.instance_id}.sigKill"
        assert sig in a._state_patterns

        await a.unsubscribe_foreign_states(f"{a.instance_id}.sigKill")

        assert sig in a._state_patterns

    async def test_every_internal_pattern_survives(self, run_adapter) -> None:
        # Named individually rather than through a wildcard: `unsubscribe_foreign_states("*")`
        # would not touch them anyway, because a pattern is removed by its exact text. The guard
        # has to hold against someone naming one of them precisely.
        a, _task = await run_adapter()
        internal = set(a._internal_patterns)
        assert internal, "the adapter records its own patterns at startup"

        for pattern in internal:
            await a._unsubscribe(pattern, a._state_patterns, a._sub)
            await a._unsubscribe(pattern, a._object_patterns, a._osub)

        assert internal <= a._state_patterns | a._object_patterns, (
            "without its messagebox or sigKill the adapter answers nothing and cannot be stopped"
        )


class TestFileSubscriptions:
    """Files live in the objects database, so they arrive on the objects connection.

    The pattern carries a ``$%$data`` suffix, which each backend needs for a different reason:
    real Redis publishes on the data key itself, and the built-in server strips the suffix again
    before registering. Leaving it off is silent on both -- which is why there is a test.
    """

    async def test_a_write_reaches_the_callback(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_files("pytest.0", "*")

        id, name, size = await drive(
            lambda: a.write_file("pytest.0", "icons/lamp.png", b"abc"),
            a.file_events,
            lambda e: e[1] == "icons/lamp.png",
        )

        assert id == "pytest.0", "the owner is separated from the path, not left as one key"
        assert size == 3, "ioBroker publishes the new length, not the content"

    async def test_a_deletion_arrives_as_none(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_files("pytest.0", "*")
        await a.write_file("pytest.0", "doomed.txt", b"x")

        _id, _name, size = await drive(
            lambda: a.unlink("pytest.0", "doomed.txt"),
            a.file_events,
            lambda e: e[1] == "doomed.txt" and e[2] is None,
        )

        assert size is None

    async def test_the_pattern_narrows(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_files("pytest.0", "icons/*")

        await a.write_file("pytest.0", "elsewhere/other.txt", b"no")
        await a.write_file("pytest.0", "icons/marker.png", b"yes")

        await expect_only_marker(
            a.file_events,
            marker=lambda e: e[1] == "icons/marker.png",
            forbidden=lambda e: e[1] == "elsewhere/other.txt",
        )

    async def test_unsubscribing_stops_it(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_files("pytest.0", "watched/*")
        await a.subscribe_foreign_files("pytest.0", "marker/*")

        await drive(
            lambda: a.write_file("pytest.0", "watched/one.txt", b"1"),
            a.file_events,
            lambda e: e[1] == "watched/one.txt",
        )

        await a.unsubscribe_foreign_files("pytest.0", "watched/*")

        await a.write_file("pytest.0", "watched/two.txt", b"2")
        await a.write_file("pytest.0", "marker/ping.txt", b"3")

        await expect_only_marker(
            a.file_events,
            marker=lambda e: e[1] == "marker/ping.txt",
            forbidden=lambda e: e[1] == "watched/two.txt",
        )

    async def test_the_pattern_is_not_replayed_after_a_reconnect(self, run_adapter) -> None:
        a, _task = await run_adapter()
        await a.subscribe_foreign_files("pytest.0", "*")
        assert any("pytest.0" in p for p in a._object_patterns if p.startswith("cfg.f."))

        await a.unsubscribe_foreign_files("pytest.0", "*")

        assert not [p for p in a._object_patterns if p.startswith("cfg.f.")]


class TestLogSubscriptions:
    """What ioBroker calls a log transporter: an adapter that collects the log.

    The channel is named after the *receiver*. A collector raises its own `.logging` flag and
    listens on `log.<its own instance id>`; every adapter in the system reads those flags and
    publishes each of its records to every raised one. So there is no pattern to pass and nothing
    to name -- a collector gets all of it or none.
    """

    async def test_the_flag_and_the_subscription_are_both_raised(self, run_adapter, raw) -> None:
        # Both halves matter. Without the flag nobody sends; without the subscription the system
        # publishes into a channel nobody reads.
        a, _task = await run_adapter()

        await a.subscribe_logs()

        flag = await read_state(raw, f"{a.instance_id}.logging")
        assert flag is not None and flag["val"] is True

        entry = await drive(
            lambda: raw.publish(
                f"log.{a.instance_id}",
                json.dumps({"severity": "warn", "message": "lamp unreachable", "from": "hue.0"}),
            ),
            a.log_events,
            lambda e: e.get("message") == "lamp unreachable",
        )

        assert entry["severity"] == "warn"
        assert entry["from"] == "hue.0"

    async def test_unsubscribing_lowers_the_flag_and_stops_it(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_logs()
        await a.subscribe_foreign_states("marker.0.*")

        await drive(
            lambda: raw.publish(f"log.{a.instance_id}", json.dumps({"message": "first"})),
            a.log_events,
            lambda e: e.get("message") == "first",
        )

        await a.unsubscribe_logs()

        flag = await read_state(raw, f"{a.instance_id}.logging")
        assert flag is not None and flag["val"] is False

        await raw.publish(f"log.{a.instance_id}", json.dumps({"message": "gone"}))
        # A state, not another log line: the marker has to travel a route the test did not just
        # tear down, or its absence would prove nothing.
        await write_state(raw, "marker.0.done", wire_state(1))

        await expect_only_marker(
            a.state_events,
            marker=lambda e: e[0] == "marker.0.done",
            forbidden=lambda e: False,
        )
        assert a.log_events.empty(), "a record arrived after unsubscribing"

    async def test_a_broken_payload_does_not_kill_the_pump(self, db, run_adapter, raw) -> None:
        # A log line is written by somebody else; malformed JSON must cost that line and nothing
        # more, or one bad producer takes the collecting adapter down with it.
        #
        # Only against real Redis, and not because the SDK differs: the built-in server parses the
        # payload of a PUBLISH itself, unguarded, and drops the client connection when that throws
        # (statesInMemServerRedis.ts, the `publish` handler). The line never reaches this SDK there,
        # so there is nothing here to assert -- the robustness that is missing is js-controller's.
        only_real_redis(db, "the built-in server drops the connection on a malformed PUBLISH")
        a, _task = await run_adapter()
        await a.subscribe_logs()

        await raw.publish(f"log.{a.instance_id}", "{not json")

        entry = await drive(
            lambda: raw.publish(f"log.{a.instance_id}", json.dumps({"message": "after"})),
            a.log_events,
            lambda e: e.get("message") == "after",
        )

        assert entry["message"] == "after"


class TestObjectSubscriptions:
    async def test_changes_and_deletions_arrive(self, run_adapter, raw_objects) -> None:
        a, _task = await run_adapter()
        await a.subscribe_objects("*")

        obj_id = f"{a.namespace}.cfg"
        _id, obj = await drive(
            lambda: write_object(
                raw_objects,
                obj_id,
                {"_id": obj_id, "type": "state", "common": {"name": "x"}, "native": {}},
            ),
            a.object_events,
            lambda e: e[0] == obj_id and e[1] is not None,
        )
        assert obj["common"]["name"] == "x"

        _id, gone = await drive(
            lambda: delete_object(raw_objects, obj_id),
            a.object_events,
            lambda e: e[0] == obj_id and e[1] is None,
        )
        assert gone is None


class TestMessaging:
    async def test_send_to_reaches_the_other_instance(self, run_adapter) -> None:
        alpha, _t1 = await run_adapter("pytestalpha")
        beta, _t2 = await run_adapter("pytestbeta")

        msg = await drive(
            lambda: alpha.send_to("pytestbeta.0", "ping", {"x": 1}),
            beta.messages,
            lambda m: m.command == "ping",
        )

        assert msg.message == {"x": 1}
        assert msg.from_ == "system.adapter.pytestalpha.0"
        assert msg.wants_reply is False

    async def test_reply_travels_back_to_the_sender(self, run_adapter, raw) -> None:
        alpha, _t1 = await run_adapter("pytestalpha")
        beta, _t2 = await run_adapter("pytestbeta")

        # A request as sendTo with a callback writes it -- js-controller shape.
        request = json.dumps(
            {
                "command": "add",
                "message": {"a": 1, "b": 2},
                "from": "system.adapter.pytestalpha.0",
                "callback": {"message": {"a": 1, "b": 2}, "id": 7, "ack": False, "time": now_ms()},
            }
        )
        msg = await drive(
            lambda: raw.publish("messagebox.system.adapter.pytestbeta.0", request),
            beta.messages,
            lambda m: m.command == "add",
        )
        assert msg.wants_reply is True

        await beta.reply(msg, {"sum": 3})

        answer = await expect_event(alpha.messages, lambda m: m.command == "add")
        assert answer.message == {"sum": 3}
        assert answer.from_ == "system.adapter.pytestbeta.0"
        assert answer.callback["ack"] is True
        # An answered callback must not look like a request again.
        assert answer.wants_reply is False


class TestStopProtocol:
    async def test_sigkill_minus_one_shuts_the_adapter_down(self, run_adapter, raw) -> None:
        a, task = await run_adapter()

        # The controller writes -1 (ack false) and expects a graceful exit.
        for _ in range(5):
            await write_state(raw, f"{a.instance_id}.sigKill", wire_state(-1))
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=2)
                break
            except asyncio.TimeoutError:
                continue

        assert task.done(), "adapter did not stop on sigKill -1"
        assert task.exception() is None
        assert a.unloaded is True
        # The world must be able to see that the instance is down.
        assert (await read_state(raw, f"{a.instance_id}.alive"))["val"] is False

    async def test_the_own_pid_keeps_the_adapter_running(self, run_adapter, raw) -> None:
        # This is what the controller writes (ack true) right after the spawn --
        # reacting to it would kill every adapter at startup.
        a, task = await run_adapter()

        await write_state(raw, f"{a.instance_id}.sigKill", wire_state(os.getpid(), ack=True))
        await asyncio.sleep(0.5)

        assert not task.done()
        assert a.unloaded is False

    async def test_a_foreign_pid_means_another_supervisor(self, run_adapter, raw) -> None:
        # The instance was started twice; the stale process -- us -- must go.
        a, task = await run_adapter()

        for _ in range(5):
            await write_state(raw, f"{a.instance_id}.sigKill", wire_state(os.getpid() + 1, ack=True))
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=2)
                break
            except asyncio.TimeoutError:
                continue

        assert task.done(), "adapter did not stop on a foreign supervisor PID"
        assert task.exception() is None
        assert a.unloaded is True


class TestEncryptedConfiguration:
    SECRET = "Zgfr56gFe87jJOM"  # not 48 hex chars -> the legacy XOR branch, which is symmetric

    async def test_declared_entries_arrive_decrypted(self, run_adapter, raw_objects) -> None:
        cipher = decrypt(self.SECRET, "hunter2")
        token_cipher = decrypt(self.SECRET, "tok-123")
        await write_object(
            raw_objects,
            "system.config",
            {"_id": "system.config", "type": "config", "common": {}, "native": {"secret": self.SECRET}},
        )
        await write_object(
            raw_objects,
            "system.adapter.pytestcrypt.0",
            {
                "_id": "system.adapter.pytestcrypt.0",
                "type": "instance",
                "common": {"encryptedNative": ["password"]},
                "native": {"host": "device.local", "password": cipher, "token": token_cipher},
            },
        )

        a, _task = await run_adapter("pytestcrypt")

        assert a.config["password"] == "hunter2"
        assert a.config["host"] == "device.local"
        # Not declared in encryptedNative: stays as stored, decrypted only on demand.
        assert a.config["token"] == token_cipher
        assert await a.get_encrypted_config("token") == "tok-123"


COLLECTOR = "system.adapter.pytestcollector.0"


async def raise_log_flag(client, adapter, collector: str = COLLECTOR) -> None:
    """Pretend to be a log transporter, and wait until the adapter has noticed.

    What admin does when a user opens the log tab: it sets its own `.logging` state, and every
    adapter in the system starts pushing its records to that instance's channel. Waiting for the
    adapter to see it is the point -- the flag travels through a subscription, so writing it and
    logging in the next line is a race the test would lose about half the time.
    """
    await write_state(client, f"{collector}.logging", wire_state(True, ack=True))

    for _ in range(100):
        if collector in adapter._log_targets:
            return
        await asyncio.sleep(0.05)

    raise AssertionError(f"the adapter never noticed {collector}.logging")


class TestLogChannel:
    """Getting an adapter's own lines in front of a user.

    Every record goes to two places. stdout, which the controller captures and re-logs under the
    host -- that is what survives a crash and what a user watching the console sees. And the
    channel of every instance that asked for the log, which is what puts the line in admin's log
    tab attributed to *this* instance. The second one is what a Python adapter used to miss
    entirely: it published on a channel named after itself, which nobody listens on, so its lines
    reached admin only as host output and no filter for the instance ever found them.
    """

    async def test_a_record_reaches_the_instance_that_asked_for_it(self, run_adapter, raw) -> None:
        a, _task = await run_adapter("pytestlog")
        ps = raw.pubsub()
        await ps.psubscribe(f"log.{COLLECTOR}")
        try:
            await raise_log_flag(raw, a)
            marker = "iobroker-python integration marker"

            async def emit() -> None:
                a.log.warn(marker)

            msg = await expect_pmessage(ps, emit, lambda m: marker in m["data"])

            payload = json.loads(msg["data"])
            assert payload["severity"] == "warn"
            # The namespace, not the instance id: `pytestlog.0`, which is the "from" admin shows
            # and what a Node adapter's records carry.
            assert payload["from"] == a.namespace
            assert isinstance(payload["ts"], int)
            assert isinstance(payload["_id"], int)
        finally:
            await ps.aclose()

    async def test_an_instance_that_never_asked_receives_nothing(self, run_adapter, raw) -> None:
        """The normal state of an installation: no log tab open, no collector running.

        Asserted against a flag that exists and says `false`, not against a missing one -- that is
        the state admin leaves behind when a user closes the log tab, and the one an adapter is
        most likely to get wrong by treating "I have seen this instance" as "it wants the log".
        """
        await write_state(raw, f"{COLLECTOR}.logging", wire_state(False, ack=True))

        a, _task = await run_adapter("pytestquiet")
        ps = raw.pubsub()
        await ps.psubscribe(f"log.{COLLECTOR}")
        try:
            assert COLLECTOR not in a._log_targets

            a.log.warn("into the void")
            await asyncio.sleep(0.5)

            while msg := await ps.get_message(ignore_subscribe_messages=True, timeout=0.1):
                assert "into the void" not in msg["data"], "sent to an instance that never asked"
        finally:
            await ps.aclose()

    async def test_a_collector_that_stops_asking_stops_receiving(self, run_adapter, raw) -> None:
        a, _task = await run_adapter("pytestlog")
        await raise_log_flag(raw, a)

        await write_state(raw, f"{COLLECTOR}.logging", wire_state(False, ack=True))

        for _ in range(100):
            if COLLECTOR not in a._log_targets:
                break
            await asyncio.sleep(0.05)

        assert COLLECTOR not in a._log_targets, "the flag was lowered and the adapter kept sending"

    async def test_an_adapter_never_sends_to_itself(self, run_adapter, raw) -> None:
        # A collector that also writes would otherwise log what it receives, receive what it
        # logged, and keep going as fast as the database allows.
        a, _task = await run_adapter("pytestlog")

        await write_state(raw, f"{a.instance_id}.logging", wire_state(True, ack=True))
        await asyncio.sleep(0.5)

        assert a.instance_id not in a._log_targets

    async def test_a_flag_already_raised_at_startup_is_found(self, run_adapter, raw) -> None:
        # A collector started before this adapter has its flag set long before the adapter exists,
        # and a subscription only reports changes. Without the read at startup an adapter would
        # reach admin's log tab only after the user closed and reopened it.
        await write_state(raw, f"{COLLECTOR}.logging", wire_state(True, ack=True))

        a, _task = await run_adapter("pytestearly")

        assert COLLECTOR in a._log_targets

    async def test_the_loglevel_threshold_holds(self, run_adapter, raw) -> None:
        # Default level is info: debug must stay off the channel, error must pass.
        a, _task = await run_adapter("pytestlog")
        ps = raw.pubsub()
        await ps.psubscribe(f"log.{COLLECTOR}")
        try:
            await raise_log_flag(raw, a)
            seen: list[str] = []

            async def emit() -> None:
                a.log.debug("must not appear")
                a.log.error("second marker")

            def pred(m: dict) -> bool:
                seen.append(m["data"])
                return json.loads(m["data"]).get("message") == "second marker"

            msg = await expect_pmessage(ps, emit, pred)

            assert json.loads(msg["data"])["severity"] == "error"
            assert not any("must not appear" in data for data in seen)
        finally:
            await ps.aclose()


class TestStatusReport:
    """The states admin draws its instance graphs from.

    Written by this SDK for the same reason adapter-core writes them in the Node world: nothing
    else knows them. The controller sees a process, not what that process is doing with its memory.
    Before these existed, a Python instance showed `alive` and `uptime` in admin and nothing else --
    every memory and CPU row sat at `(null)`.
    """

    async def test_writes_what_admin_graphs(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()

        await a._report_status([4.0, 6.0])

        for name in ("cpu", "cputime", "memRss", "uptime", "inputCount", "outputCount"):
            state = await read_state(raw, f"{a.instance_id}.{name}")
            assert state is not None, f"{name} was not written"
            assert isinstance(state["val"], (int, float)), f"{name} is not a number"
            assert state["val"] >= 0, f"{name} is negative"
            # Acknowledged and attributed, or admin shows them as somebody else's command.
            assert state["ack"] is True and state["from"] == a.instance_id

    async def test_the_lag_is_the_average_rounded_up(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()

        await a._report_status([4.0, 6.0, 5.2])

        # ceil(15.2 / 3) = 6. Rounded up rather than to nearest, as js-controller does it: a lag
        # under half a millisecond should read as "a little", not as "none".
        assert (await read_state(raw, f"{a.instance_id}.eventLoopLag"))["val"] == 6

    async def test_a_period_without_measurements_leaves_the_lag_alone(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()

        await a._report_status([])

        # Not zero: nothing was measured, and writing 0 would claim a healthy loop on the strength
        # of no evidence -- the one reading a user would take at face value.
        assert await read_state(raw, f"{a.instance_id}.eventLoopLag") is None

    async def test_compact_mode_is_answered_at_startup(self, run_adapter, raw) -> None:
        # False, not empty. A Python adapter can never run in compact mode, and an empty indicator
        # in admin reads as "unknown" rather than as "no".
        a, _task = await run_adapter()

        state = await read_state(raw, f"{a.instance_id}.compactMode")

        assert state is not None and state["val"] is False

    async def test_the_counters_count_and_then_start_over(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_states("*")

        a.input_count = 0
        a.output_count = 0
        await a.set_state("counted", 1, ack=True)
        written = a.output_count

        await drive(
            lambda: write_state(raw, f"{a.namespace}.arrived", wire_state(2)),
            a.state_events,
            lambda e: e[0] == f"{a.namespace}.arrived",
        )

        assert written == 1, "a state written was not counted"
        assert a.input_count >= 1, "a state received was not counted"

        await a._report_status([])

        # Per period, not since startup: a report leaves both at zero, so the next value is the
        # traffic of the next fifteen seconds rather than a number that only grows.
        assert a.input_count == 0 and a.output_count == 0
