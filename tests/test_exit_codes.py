"""The process/exit-code contract, exercised by running the adapter as a real subprocess.

The in-process fixtures never reach ``run()`` -- they drive ``_main()`` directly, so the exit code
the controller keys its restart behaviour off is never observed there. These tests start the
adapter the way js-controller does -- ``python <module>`` with the connection in the environment --
and assert the code the process ends with (see ``doc/PYTHON.md``, "Stopping, exit codes,
restarts").

Runs on whichever backend the fixture provides; the exit code is decided by ``run()`` and does not
depend on the backend, but booting the whole adapter against each is a fair end-to-end smoke test.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

from iobroker.exit_codes import ExitCode

ADAPTER = Path(__file__).resolve().parent / "exit_adapter.py"
SRC = Path(__file__).resolve().parent.parent / "src"


async def run_exit_adapter(db, behavior: str) -> int:
    """Start the helper adapter as a subprocess against ``db`` and return its exit code."""
    env = dict(os.environ)
    # The suite imports iobroker via pytest's pythonpath (src/), not from site-packages; a
    # subprocess does not inherit that, so put src/ on PYTHONPATH explicitly.
    env["PYTHONPATH"] = os.pathsep.join([str(SRC), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    for section, cfg in (("STATES", db.states), ("OBJECTS", db.objects)):
        env[f"IOB_{section}_HOST"] = cfg.host
        env[f"IOB_{section}_PORT"] = str(cfg.port)
        env[f"IOB_{section}_DB"] = str(cfg.db)
        env[f"IOB_{section}_TYPE"] = cfg.kind
        env.pop(f"IOB_{section}_PASS", None)
    for var in ("IOB_CONFIG", "IOB_INSTANCE", "IOB_LOGLEVEL"):
        env.pop(var, None)
    env["IOB_TEST_BEHAVIOR"] = behavior

    proc = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(ADAPTER)],
        capture_output=True,
        text=True,
        timeout=40,
        env=env,
    )
    return proc.returncode


class TestExitCodes:
    async def test_a_clean_stop_exits_zero(self, db) -> None:
        assert await run_exit_adapter(db, "stop") == int(ExitCode.NO_ERROR)

    async def test_terminate_exits_eleven(self, db) -> None:
        # A planned stop the controller must not restart.
        assert await run_exit_adapter(db, "terminate") == int(
            ExitCode.ADAPTER_REQUESTED_TERMINATION
        )

    async def test_an_unhandled_exception_exits_six(self, db) -> None:
        # UNCAUGHT_EXCEPTION: restarted and counted towards restart-loop detection.
        assert await run_exit_adapter(db, "raise") == int(ExitCode.UNCAUGHT_EXCEPTION)

    async def test_terminate_with_an_explicit_code_is_honoured(self, db) -> None:
        # terminate() must pass an explicit code straight through (here: restart-after-1s).
        assert await run_exit_adapter(db, "terminate156") == int(
            ExitCode.START_IMMEDIATELY_AFTER_STOP
        )
