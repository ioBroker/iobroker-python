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
import redis.sentinel

from redis.asyncio.sentinel import SentinelConnectionPool, SentinelManagedConnection

import iobroker.connection as connection
from iobroker.connection import (
    AsyncIoBrokerConnection,
    DbConfig,
    IoBrokerConnection,
    check_protocol,
    connect,
    connect_async,
    find_config,
    load_db_config,
    _lower_cmd,
    _parse_sentinels,
)
from support import only_real_redis, wire_state


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Config tests must not inherit IOB_* settings from the shell."""
    for section in ("STATES", "OBJECTS"):
        for suffix in ("HOST", "PORT", "DB", "PASS", "TYPE", "SENTINELS", "SENTINEL_NAME"):
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


class TestParseSentinels:
    """The wire format between js-controller and this SDK: ``host:port,host:port``."""

    def test_a_list_of_addresses(self) -> None:
        assert _parse_sentinels("10.0.0.1:26379,10.0.0.2:26380") == (
            ("10.0.0.1", 26379),
            ("10.0.0.2", 26380),
        )

    def test_whitespace_and_empty_entries_are_ignored(self) -> None:
        # A hand-written service file is allowed to be untidy.
        assert _parse_sentinels(" 10.0.0.1:26379 , , 10.0.0.2:26380 ,") == (
            ("10.0.0.1", 26379),
            ("10.0.0.2", 26380),
        )

    def test_a_missing_port_means_the_sentinel_default(self) -> None:
        assert _parse_sentinels("sentinel-a,sentinel-b:26380") == (
            ("sentinel-a", 26379),
            ("sentinel-b", 26380),
        )

    def test_ipv6_literals_are_bracketed(self) -> None:
        # Without the brackets the address's own colons cannot be told from the separator, so
        # both sides agree to write them -- see buildPythonEnv in js-controller.
        assert _parse_sentinels("[::1]:26379,[fd00::2]") == (("::1", 26379), ("fd00::2", 26379))

    def test_nothing_configured(self) -> None:
        assert _parse_sentinels("") == ()

    def test_a_port_that_is_not_a_number_is_an_error(self) -> None:
        # Better here than as a connection that never comes up.
        with pytest.raises(ValueError):
            _parse_sentinels("10.0.0.1:no-port")


class TestLoadSentinelConfig:
    """Both routes into a sentinel configuration: the environment and iobroker.json."""

    def test_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_STATES_SENTINELS", "10.0.0.1:26379,10.0.0.2:26380")
        monkeypatch.setenv("IOB_STATES_SENTINEL_NAME", "iob")
        monkeypatch.setenv("IOB_STATES_TYPE", "redis")
        monkeypatch.setenv("IOB_STATES_DB", "2")
        monkeypatch.setenv("IOB_STATES_PASS", "secret")

        cfg = load_db_config("states")

        assert cfg.sentinels == (("10.0.0.1", 26379), ("10.0.0.2", 26380))
        assert cfg.sentinel_name == "iob"
        assert cfg.db == 2 and cfg.password == "secret" and cfg.kind == "redis"
        # No address of its own: the master is whatever the sentinels name at connect time, and a
        # leftover host here would be one somebody eventually connects to.
        assert cfg.host == "" and cfg.port == 0
        assert cfg.uses_sentinel

    def test_the_sentinels_alone_are_enough(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        # A sentinel configuration has no port, so the "is this from the environment" test cannot
        # be the port. If it were, an adapter started by the controller would fall through to
        # iobroker.json -- which on a multihost slave is not even the right file.
        _clear_env(monkeypatch)
        monkeypatch.setattr(connection, "_CONFIG_CANDIDATES", ())
        monkeypatch.setenv("IOB_STATES_SENTINELS", "10.0.0.1:26379")

        assert load_db_config("states").sentinels == (("10.0.0.1", 26379),)

    def test_the_master_group_defaults_to_mymaster(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # js-controller's own default when sentinelName is not configured.
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_OBJECTS_SENTINELS", "10.0.0.1:26379")

        assert load_db_config("objects").sentinel_name == "mymaster"

    def test_from_the_config_file(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        # A list of hosts is how ioBroker records a sentinel setup; there is no separate flag.
        _clear_env(monkeypatch)
        path = tmp_path / "iobroker.json"
        path.write_text(
            json.dumps(
                {
                    "states": {
                        "type": "redis",
                        "host": ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
                        "port": [26379, 26380, 26381],
                        "sentinelName": "iob",
                        "options": {"auth_pass": "pw", "db": 1},
                    }
                }
            ),
            encoding="utf-8",
        )

        cfg = load_db_config("states", path=str(path))

        assert cfg.sentinels == (("10.0.0.1", 26379), ("10.0.0.2", 26380), ("10.0.0.3", 26381))
        assert cfg.sentinel_name == "iob"
        assert cfg.password == "pw" and cfg.db == 1
        assert cfg.host == "" and cfg.port == 0

    def test_one_port_for_every_sentinel(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        # js-controller reads the same two fields this way round: a scalar port applies to all
        # hosts. Reading it differently here would connect to the wrong ports.
        _clear_env(monkeypatch)
        path = tmp_path / "iobroker.json"
        path.write_text(
            json.dumps({"objects": {"type": "redis", "host": ["a", "b"], "port": 26379}}),
            encoding="utf-8",
        )

        assert load_db_config("objects", path=str(path)).sentinels == (
            ("a", 26379),
            ("b", 26379),
        )

    def test_an_ordinary_configuration_has_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_env(monkeypatch)
        monkeypatch.setenv("IOB_STATES_PORT", "9000")

        cfg = load_db_config("states")

        assert cfg.sentinels == () and not cfg.uses_sentinel
        assert cfg.location == "127.0.0.1:9000"


class TestSentinelClient:
    """What the two connect functions build for a sentinel configuration.

    Asserted on the constructed client rather than against a server, because what has to be right
    is which *kind* of pool it is: a plain pool would connect once to a fixed address and stay
    there, which is precisely what a sentinel setup exists to avoid. The behaviour against a
    running sentinel is covered in test_sentinel.py.
    """

    CFG = DbConfig(
        host="",
        port=0,
        db=2,
        password="pw",
        kind="redis",
        sentinels=(("10.0.0.1", 26379), ("10.0.0.2", 26380)),
        sentinel_name="iob",
    )

    def _addresses(self, pool: SentinelConnectionPool) -> list[tuple[str, int]]:
        return [
            (s.connection_pool.connection_kwargs["host"], s.connection_pool.connection_kwargs["port"])
            for s in pool.sentinel_manager.sentinels
        ]

    def test_the_async_client_asks_the_sentinels(self) -> None:
        pool = connect_async(self.CFG).connection_pool

        assert isinstance(pool, SentinelConnectionPool)
        assert pool.service_name == "iob"
        assert pool.is_master, "reads must go to the master too -- a replica can be behind"
        assert self._addresses(pool) == [("10.0.0.1", 26379), ("10.0.0.2", 26380)]

    def test_the_connections_are_the_ones_that_re_resolve(self) -> None:
        # SentinelManagedConnection is what re-reads the master's address on every connect, and
        # so what turns a failover into a reconnect. Substituting the SDK's own connection class
        # here would switch that off; it is not needed, because sentinel means real Redis and
        # real Redis does not care about the case of a command name.
        pool = connect_async(self.CFG).connection_pool

        assert pool.connection_class is SentinelManagedConnection

    def test_the_database_settings_reach_the_master(self) -> None:
        pool = connect_async(self.CFG).connection_pool

        assert pool.connection_kwargs["db"] == 2
        assert pool.connection_kwargs["password"] == "pw"
        assert pool.connection_kwargs["decode_responses"] is True

    def test_the_password_does_not_reach_the_sentinels(self) -> None:
        # redis-py forwards only the socket_* options to the sentinel connections. Same as
        # js-controller, which gives ioredis a password and no sentinelPassword -- a sentinel
        # with its own authentication is out of reach on both sides, and this pins that so the
        # limitation is a documented one rather than a surprise.
        manager = connect_async(self.CFG).connection_pool.sentinel_manager

        assert "password" not in manager.sentinel_kwargs
        assert manager.sentinel_kwargs["socket_timeout"] == 10

    def test_the_sync_client_too(self) -> None:
        pool = connect(self.CFG).connection_pool

        assert isinstance(pool, redis.sentinel.SentinelConnectionPool)
        assert pool.service_name == "iob"

    def test_decode_can_be_turned_off(self) -> None:
        # The file store opens a second connection this way; a PNG decoded as UTF-8 is destroyed.
        pool = connect_async(self.CFG, decode=False).connection_pool

        assert pool.connection_kwargs["decode_responses"] is False


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
