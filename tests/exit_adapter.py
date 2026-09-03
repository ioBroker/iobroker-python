"""A helper adapter for the exit-code tests -- meant to be run as a subprocess, not collected by
pytest (its name does not match ``test_*``).

The behaviour is chosen through ``IOB_TEST_BEHAVIOR`` so the parent can assert the process exit
code for each case of the contract in ``doc/PYTHON.md`` ("Stopping, exit codes, restarts").
"""

from __future__ import annotations

import os

from iobroker import Adapter, ExitCode


class ExitAdapter(Adapter):
    async def on_ready(self) -> None:
        behavior = os.environ.get("IOB_TEST_BEHAVIOR", "stop")

        if behavior == "raise":
            # An exception the adapter never catches -> the controller sees code 6.
            raise RuntimeError("boom from on_ready")
        if behavior == "terminate":
            self.terminate("work done")
        elif behavior == "terminate156":
            self.terminate("restart me", exit_code=int(ExitCode.START_IMMEDIATELY_AFTER_STOP))
        else:  # "stop": an orderly shutdown, code 0
            self.stop()


if __name__ == "__main__":
    ExitAdapter("pytestexit", instance=0).run()
