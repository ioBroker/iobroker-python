"""Cross-language conformance: a state or object written by the **real js-controller client** is
read identically by the Python SDK, and vice versa.

The two-backend suite proves the SDK works against the real database *servers*. This proves the
harder half of "as good as the JS classes": that the wire *envelope* the SDK writes and parses is
the one a JavaScript adapter writes and parses -- so a Python adapter and a Node.js adapter sharing
a state or object actually agree on it, field for field.

The JS side is driven through ``tests/builtin/interop.mjs``, which runs ``@iobroker/db-states-redis``
and ``@iobroker/db-objects-redis`` -- the very clients a Node.js adapter uses. These tests run on
the **built-in backend only**: the envelope is produced by the JS client and is identical whatever
server stores it, the built-in backend is the exact js-controller server code, and it sidesteps
pointing a JS client at a shared Redis (db numbers, a live installation's keys). Skipped, not
failed, on the Redis backend -- like the other backend-specific tests.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path

from iobroker.types import State
from support import only_builtin

BUILTIN_DIR = Path(__file__).resolve().parent / "builtin"


async def run_js(db, cmd: str, id: str, arg: dict | None = None) -> dict:
    """Drive the real JS client for one operation and return its parsed RESULT."""
    node = shutil.which("node")
    assert node, "node must be on PATH for the interop tests"

    argv = [
        node,
        "interop.mjs",
        f"--states-port={db.states.port}",
        f"--objects-port={db.objects.port}",
        cmd,
        id,
    ]
    if arg is not None:
        argv.append(json.dumps(arg))

    proc = await asyncio.to_thread(
        subprocess.run,
        argv,
        cwd=BUILTIN_DIR,
        capture_output=True,
        text=True,
        timeout=40,
    )
    line = next(
        (ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")), None
    )
    if line is None:
        raise AssertionError(
            f"interop.mjs produced no RESULT (rc={proc.returncode})\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr[-2000:]}"
        )
    return json.loads(line[len("RESULT ") :])


A_STATE_OBJECT = {
    "type": "state",
    "common": {"name": "x", "role": "value", "type": "number", "read": True, "write": True},
    "native": {},
}


class TestJsWritesPythonReads:
    async def test_python_reads_a_js_written_state(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        await run_js(
            db,
            "set-state",
            "jsside.0.temp",
            {"val": 42, "ack": True, "from": "system.adapter.jsside.0"},
        )

        st = await adapter.get_foreign_state("jsside.0.temp")
        assert st is not None
        assert st.val == 42
        assert st.ack is True
        assert st.from_ == "system.adapter.jsside.0"
        assert st.q == 0
        assert isinstance(st.ts, int) and st.ts > 0
        assert st.lc == st.ts  # a fresh write: last-change equals timestamp

    async def test_python_reads_a_js_written_object(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        await run_js(db, "set-object", "jsside.0.dp", A_STATE_OBJECT)

        obj = await adapter.get_foreign_object("jsside.0.dp")
        assert obj is not None
        assert obj["_id"] == "jsside.0.dp"
        assert obj["type"] == "state"
        assert obj["common"]["name"] == "x"
        assert obj["common"]["role"] == "value"
        assert obj["native"] == {}


class TestPythonWritesJsReads:
    async def test_js_reads_a_python_written_state(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        await adapter.set_foreign_state(
            "pyside.0.temp", State(val=7, ack=True, from_="system.adapter.pyside.0")
        )

        st = (await run_js(db, "get-state", "pyside.0.temp"))["state"]
        assert st is not None
        assert st["val"] == 7
        assert st["ack"] is True
        assert st["from"] == "system.adapter.pyside.0"
        assert st["q"] == 0
        assert isinstance(st["ts"], int)
        assert isinstance(st["lc"], int)

    async def test_js_reads_a_python_written_object(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        await adapter.set_foreign_object(
            "pyside.0.dp",
            {
                "type": "state",
                "common": {"name": "y", "role": "value", "type": "number", "read": True, "write": True},
                "native": {"k": 1},
            },
        )

        obj = (await run_js(db, "get-object", "pyside.0.dp"))["object"]
        assert obj is not None
        assert obj["_id"] == "pyside.0.dp"
        assert obj["type"] == "state"
        assert obj["common"]["name"] == "y"
        assert obj["native"] == {"k": 1}
        # The SDK stamps `from` the way adapter-core does; the JS client must read it back intact.
        assert obj["from"] == "system.adapter.pytest.0"


class TestLastChangeConformance:
    """The ``lc`` rule is the JS client's, measured against the JS client itself."""

    async def test_both_sides_keep_lc_on_an_unchanged_write(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        # The reference: the JS client writing the same value twice does not move lc.
        await run_js(db, "set-state", "jsside.0.lc", {"val": 5, "ack": True})
        js_first = (await run_js(db, "get-state", "jsside.0.lc"))["state"]
        await run_js(db, "set-state", "jsside.0.lc", {"val": 5, "ack": True})
        js_second = (await run_js(db, "get-state", "jsside.0.lc"))["state"]

        assert js_second["lc"] == js_first["lc"]
        assert js_second["ts"] > js_first["ts"]  # ts still moves

        # The SDK must behave identically.
        await adapter.set_foreign_state("pyside.0.lc", State(val=5, ack=True))
        py_first = await adapter.get_foreign_state("pyside.0.lc")
        await asyncio.sleep(0.01)
        await adapter.set_foreign_state("pyside.0.lc", State(val=5, ack=True))
        py_second = await adapter.get_foreign_state("pyside.0.lc")

        assert py_second.lc == py_first.lc
        assert py_second.ts > py_first.ts

    async def test_a_python_write_continues_a_js_history(self, adapter, db) -> None:
        # The sharpest form: the SDK writing the same value on top of a JS-written state must keep
        # the lc the JS client established, not restart it.
        only_builtin(db, "drives the real JS client against the built-in server")

        await run_js(db, "set-state", "shared.0.lc", {"val": 9, "ack": True})
        js_state = (await run_js(db, "get-state", "shared.0.lc"))["state"]
        await asyncio.sleep(0.01)

        await adapter.set_foreign_state("shared.0.lc", State(val=9, ack=True))

        after = await adapter.get_foreign_state("shared.0.lc")
        assert after.lc == js_state["lc"], "the SDK restarted a last-change the JS client owned"


class TestValueFidelity:
    """Values of every JSON kind survive the crossing in both directions unchanged."""

    VALUES = [3.14, True, False, "text", {"a": 1, "b": [1, 2, 3]}]

    async def test_js_to_python(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        for i, value in enumerate(self.VALUES):
            id = f"jsside.0.v{i}"
            await run_js(db, "set-state", id, {"val": value, "ack": True})
            st = await adapter.get_foreign_state(id)
            assert st is not None and st.val == value, f"{value!r} did not survive JS->Python"

    async def test_python_to_js(self, adapter, db) -> None:
        only_builtin(db, "drives the real JS client against the built-in server")

        for i, value in enumerate(self.VALUES):
            id = f"pyside.0.v{i}"
            await adapter.set_foreign_state(id, State(val=value, ack=True))
            st = (await run_js(db, "get-state", id))["state"]
            assert st is not None and st["val"] == value, f"{value!r} did not survive Python->JS"
