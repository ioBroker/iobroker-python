"""Tests for the connection layer.

The command lowercasing is asserted at the packing level, because a real Redis
accepts both cases and would hide a regression; the built-in js-controller
server accepts only lowercase, so a wrong packer means nothing works at all.
Configuration loading and the protocol check are the first things a user hits
when wiring an adapter up, and their failure modes must be errors, not silence.
"""

from __future__ import annotations

import json

import pytest

import iobroker.connection as connection
from iobroker.connection import (
    AsyncIoBrokerConnection,
    DbConfig,
    IoBrokerConnection,
    check_protocol,
    connect_async,
    find_config,
    load_db_config,
    _lower_cmd,
)
from support import only_real_redis, wire_state


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Config tests must not inherit IOB_* settings from the shell."""
    for section in ("STATES", "OBJECTS"):
        for suffix in ("HOST", "PORT", "DB", "PASS", "TYPE"):
            monkeypatch.delenv(f"IOB_{section}_{suffix}", raising=False)
    monkeypatch.delenv("IOB_CONFIG", raising=False)


class TestLowercaseCommands:
    def test_lowercases_only_the_command_name(self) -> None:
        # Key and value case must survive: "MyKey" and "mykey" are different keys.
        assert _lower_cmd(("GET", "MyKey")) == ("get", "MyKey")

    def test_multi_word_commands(self) -> None:
        # redis-py passes "CONFIG SET" as one string and splits it while packing,
        # so lowercasing must happen before the split.
        assert _lower_cmd(("CONFIG SET", "maxmemory", "0")) == ("config set", "maxmemory", "0")

    def test_bytes_command_name(self) -> None:
        assert _lower_cmd((b"GET", "k")) == ("get", "k")

    def test_empty_args(self) -> None:
        assert _lower_cmd(()) == ()

    def test_sync_connection_packs_lowercase(self) -> None:
        conn = IoBrokerConnection(host="127.0.0.1", port=1)
        wire = b"".join(conn._command_packer.pack("GET", "MyKey"))

        assert b"get" in wire
        assert b"GET" not in wire
        assert b"MyKey" in wire

    def test_sync_connection_packs_pipelines_lowercase(self) -> None:
        # setState goes through a MULTI, which the connection packs by feeding
        # every command through the packer's pack() -- the same hook as above.
        conn = IoBrokerConnection(host="127.0.0.1", port=1)
        wire = b"".join(conn.pack_commands([("SET", "K", "V"), ("PUBLISH", "K", "V")]))

        assert b"set" in wire and b"publish" in wire
        assert b"SET" not in wire and b"PUBLISH" not in wire

    def test_async_connection_packs_lowercase(self) -> None:
        conn = AsyncIoBrokerConnection(host="127.0.0.1", port=1)
        wire = b"".join(conn.pack_command("GET", "MyKey"))

        assert b"get" in wire
        assert b"GET" not in wire
        assert b"MyKey" in wire

    def test_async_connection_packs_pipelines_lowercase(self) -> None:
        conn = AsyncIoBrokerConnection(host="127.0.0.1", port=1)
        wire = b"".join(conn.pack_commands([("SET", "K", "V"), ("PUBLISH", "K", "V")]))

        assert b"set" in wire and b"publish" in wire
        assert b"SET" not in wire and b"PUBLISH" not in wire


class TestLoadDbConfigFromEnv:
    def test_reads_every_field(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_STATES_HOST", "10.0.0.2")
        monkeypatch.setenv("IOB_STATES_PORT", "6380")
        monkeypatch.setenv("IOB_STATES_DB", "3")
        monkeypatch.setenv("IOB_STATES_PASS", "secret")
        monkeypatch.setenv("IOB_STATES_TYPE", "redis")

        cfg = load_db_config("states")

        assert cfg == DbConfig(host="10.0.0.2", port=6380, db=3, password="secret", kind="redis")

    def test_defaults_when_only_the_port_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_OBJECTS_PORT", "9001")

        cfg = load_db_config("objects")

        assert cfg == DbConfig(host="127.0.0.1", port=9001, db=0, password=None, kind="jsonl")

    def test_empty_password_means_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The controller only sets IOB_*_PASS when a password is configured, but an
        # empty string from a hand-written service file must not become "".
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_STATES_PORT", "6379")
        monkeypatch.setenv("IOB_STATES_PASS", "")

        assert load_db_config("states").password is None


class TestLoadDbConfigFromFile:
    CONFIG = {
        "states": {"type": "redis", "host": "10.0.0.9", "port": 6380, "options": {"auth_pass": "pw", "db": 2}},
        "objects": {"type": "jsonl", "host": "127.0.0.1"},
    }

    @pytest.fixture
    def config_file(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
        _clear_env(monkeypatch)
        path = tmp_path / "iobroker.json"
        path.write_text(json.dumps(self.CONFIG), encoding="utf-8")
        return str(path)

    def test_reads_the_states_section(self, config_file: str) -> None:
        cfg = load_db_config("states", path=config_file)

        assert cfg == DbConfig(host="10.0.0.9", port=6380, db=2, password="pw", kind="redis")

    def test_missing_port_falls_back_per_section(self, config_file: str) -> None:
        # 9000 for states, 9001 for objects -- the ports js-controller listens on.
        assert load_db_config("objects", path=config_file).port == 9001

    def test_env_config_variable_is_used(self, config_file: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("IOB_CONFIG", config_file)

        assert load_db_config("states").host == "10.0.0.9"

    def test_rejects_an_unknown_section(self, config_file: str) -> None:
        with pytest.raises(ValueError, match="section"):
            load_db_config("files", path=config_file)


class TestFindConfig:
    def test_explicit_path_wins(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        explicit = tmp_path / "a.json"
        explicit.write_text("{}", encoding="utf-8")
        other = tmp_path / "b.json"
        other.write_text("{}", encoding="utf-8")
        monkeypatch.setenv("IOB_CONFIG", str(other))

        assert find_config(str(explicit)) == str(explicit)

    def test_explicit_path_must_exist(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            find_config(str(tmp_path / "missing.json"))

    def test_nothing_found_is_an_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The candidate list is emptied because a developer machine may genuinely
        # have C:/ioBroker or /opt/iobroker, which would turn this into a flake.
        monkeypatch.delenv("IOB_CONFIG", raising=False)
        monkeypatch.setattr(connection, "_CONFIG_CANDIDATES", ())

        with pytest.raises(FileNotFoundError, match="IOB_CONFIG"):
            find_config()


class TestIsBuiltin:
    @pytest.mark.parametrize("kind,builtin", [("redis", False), ("jsonl", True), ("file", True)])
    def test_only_real_redis_is_not_builtin(self, kind: str, builtin: bool) -> None:
        cfg = DbConfig(host="h", port=1, db=0, password=None, kind=kind)

        assert cfg.is_builtin is builtin


class TestSyncClient:
    """The synchronous client against a real database.

    The adapter itself runs async; the sync client exists for tools and small
    scripts, and nothing else on this page would notice if it broke.
    """

    def test_roundtrip_and_pipeline(self, db) -> None:
        from iobroker.connection import connect

        client = connect(db.states)
        try:
            client.set("io.pytest.0.sync", wire_state(1))
            assert json.loads(client.get("io.pytest.0.sync"))["val"] == 1

            pipe = client.pipeline(transaction=True)
            pipe.set("io.pytest.0.sync2", wire_state(2))
            pipe.publish("io.pytest.0.sync2", wire_state(2))
            pipe.execute()

            assert json.loads(client.get("io.pytest.0.sync2"))["val"] == 2
        finally:
            client.close()


class TestCheckProtocol:
    """Against a real database -- this is the connection test every adapter runs first."""

    async def test_passes_on_the_supported_version(self, db) -> None:
        for cfg, section in ((db.states, "states"), (db.objects, "objects")):
            client = connect_async(cfg)
            try:
                assert await check_protocol(client, section) == "4"
            finally:
                await client.aclose()

    async def test_missing_version_reads_as_no_iobroker(self, db) -> None:
        only_real_redis(db, "deleting a meta key")
        db.states_sync.delete("meta.states.protocolVersion")
        client = connect_async(db.states)
        try:
            with pytest.raises(ConnectionError, match="is ioBroker running"):
                await check_protocol(client, "states")
        finally:
            await client.aclose()

    async def test_version_mismatch_aborts(self, db) -> None:
        db.objects_sync.set("meta.objects.protocolVersion", "3")
        client = connect_async(db.objects)
        try:
            with pytest.raises(ConnectionError, match="protocol version"):
                await check_protocol(client, "objects")
        finally:
            await client.aclose()
