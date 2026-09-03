"""The startup contract: how an adapter learns which instance it is, at which log level, and
against which databases.

The controller passes all of this through the environment rather than the command line, because
``/proc/<pid>/cmdline`` is world-readable while a process's environment is not (see
``doc/PYTHON.md``, "How an instance is started"). The database fields themselves are covered in
``test_connection.py``; what is pinned here is the part that decides the adapter's *identity* and
the precedence between environment, command line and ``iobroker.json``.

Pure unit tests -- no database, so they run everywhere and only once.
"""

from __future__ import annotations

import json

import pytest

from iobroker.adapter import Adapter, _read_instance, _read_loglevel
from iobroker.connection import load_db_config


def _clear_iob(monkeypatch: pytest.MonkeyPatch) -> None:
    """No IOB_* setting may leak in from the shell running the suite."""
    for section in ("STATES", "OBJECTS"):
        for suffix in ("HOST", "PORT", "DB", "PASS", "TYPE"):
            monkeypatch.delenv(f"IOB_{section}_{suffix}", raising=False)
    for var in ("IOB_CONFIG", "IOB_INSTANCE", "IOB_LOGLEVEL"):
        monkeypatch.delenv(var, raising=False)


class TestInstanceNumber:
    def test_iob_instance_is_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])
        monkeypatch.setenv("IOB_INSTANCE", "3")

        assert _read_instance() == 3

    def test_falls_back_to_the_command_line(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The controller passes --instance as well; it is the readable half of the contract.
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter", "--instance", "5"])

        assert _read_instance() == 5

    def test_the_environment_wins_over_the_command_line(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter", "--instance", "9"])
        monkeypatch.setenv("IOB_INSTANCE", "2")

        assert _read_instance() == 2

    def test_defaults_to_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])

        assert _read_instance() == 0

    def test_unknown_arguments_do_not_upset_the_parser(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The adapter's own arguments must not make the controller's --instance unreadable.
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter", "--custom", "x", "--instance", "7"])

        assert _read_instance() == 7


class TestLogLevel:
    def test_iob_loglevel_is_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])
        monkeypatch.setenv("IOB_LOGLEVEL", "debug")

        assert Adapter("demo", instance=0)._loglevel == "debug"

    def test_falls_back_to_the_command_line(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter", "--loglevel", "warn"])

        assert Adapter("demo", instance=0)._loglevel == "warn"
        assert _read_loglevel() == "warn"

    def test_defaults_to_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The controller only sets IOB_LOGLEVEL when one is configured.
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])

        assert Adapter("demo", instance=0)._loglevel == "info"


class TestAdapterIdentity:
    def test_identity_is_derived_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])
        monkeypatch.setenv("IOB_INSTANCE", "4")

        a = Adapter("myad")

        assert a.instance == 4
        assert a.namespace == "myad.4"
        # Everything the adapter writes about itself hangs off this id.
        assert a.instance_id == "system.adapter.myad.4"

    def test_an_explicit_instance_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Tests and embedded uses construct the adapter directly; that must not be overridden by
        # whatever happens to be in the environment.
        _clear_iob(monkeypatch)
        monkeypatch.setattr("sys.argv", ["adapter"])
        monkeypatch.setenv("IOB_INSTANCE", "4")

        assert Adapter("myad", instance=1).instance == 1


class TestEnvironmentBeatsTheConfigFile:
    """``iobroker.json`` is only consulted when the controller passed nothing."""

    @pytest.fixture
    def config_file(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
        _clear_iob(monkeypatch)
        path = tmp_path / "iobroker.json"
        path.write_text(
            json.dumps(
                {
                    "states": {"type": "jsonl", "host": "9.9.9.9", "port": 9000},
                    "objects": {"type": "jsonl", "host": "9.9.9.9", "port": 9001},
                }
            ),
            encoding="utf-8",
        )
        return str(path)

    def test_the_environment_is_preferred(
        self, config_file: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("IOB_STATES_HOST", "1.2.3.4")
        monkeypatch.setenv("IOB_STATES_PORT", "6390")
        monkeypatch.setenv("IOB_STATES_TYPE", "redis")

        cfg = load_db_config("states", path=config_file)

        assert (cfg.host, cfg.port, cfg.kind) == ("1.2.3.4", 6390, "redis")

    def test_the_file_is_not_even_opened(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The environment short-circuits before the file is located, so a missing or unreadable
        # iobroker.json cannot break an instance the controller started correctly.
        _clear_iob(monkeypatch)
        monkeypatch.setenv("IOB_OBJECTS_PORT", "9001")

        cfg = load_db_config("objects", path="/definitely/not/here/iobroker.json")

        assert cfg.port == 9001

    def test_the_file_is_used_when_the_environment_is_empty(self, config_file: str) -> None:
        cfg = load_db_config("states", path=config_file)

        assert (cfg.host, cfg.port) == ("9.9.9.9", 9000)
