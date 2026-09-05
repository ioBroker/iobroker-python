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
    """What ioBroker calls a log transporter: an adapter that collects the log."""

    async def test_another_adapters_line_arrives(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_logs("*")

        entry = await drive(
            lambda: raw.publish(
                "log.system.adapter.hue.0",
                json.dumps({"severity": "warn", "message": "lamp unreachable"}),
            ),
            a.log_events,
            lambda e: e.get("message") == "lamp unreachable",
        )

        assert entry["severity"] == "warn"

    async def test_unsubscribing_stops_it(self, run_adapter, raw) -> None:
        a, _task = await run_adapter()
        await a.subscribe_logs("system.adapter.hue.*")
        await a.subscribe_logs("system.adapter.marker.*")

        await drive(
            lambda: raw.publish("log.system.adapter.hue.0", json.dumps({"message": "first"})),
            a.log_events,
            lambda e: e.get("message") == "first",
        )

        await a.unsubscribe_logs("system.adapter.hue.*")

        await raw.publish("log.system.adapter.hue.0", json.dumps({"message": "gone"}))
        await raw.publish("log.system.adapter.marker.0", json.dumps({"message": "marker"}))

        await expect_only_marker(
            a.log_events,
            marker=lambda e: e.get("message") == "marker",
            forbidden=lambda e: e.get("message") == "gone",
        )

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
        await a.subscribe_logs("*")

        await raw.publish("log.system.adapter.hue.0", "{not json")

        entry = await drive(
            lambda: raw.publish("log.system.adapter.hue.0", json.dumps({"message": "after"})),
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


class TestLogChannel:
    async def test_log_lines_are_published_for_the_transporters(self, run_adapter, raw) -> None:
        a, _task = await run_adapter("pytestlog")
        ps = raw.pubsub()
        await ps.psubscribe(f"log.{a.instance_id}")
        try:
            marker = "iobroker-python integration marker"

            async def emit() -> None:
                a.log.warn(marker)

            msg = await expect_pmessage(ps, emit, lambda m: marker in m["data"])

            payload = json.loads(msg["data"])
            assert payload["severity"] == "warn"
            assert payload["from"] == a.instance_id
            assert isinstance(payload["ts"], int)
        finally:
            await ps.aclose()

    async def test_the_loglevel_threshold_holds(self, run_adapter, raw) -> None:
        # Default level is info: debug must stay off the channel, error must pass.
        a, _task = await run_adapter("pytestlog")
        ps = raw.pubsub()
        await ps.psubscribe(f"log.{a.instance_id}")
        try:
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
