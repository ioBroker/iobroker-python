"""Smallest complete Python adapter.

Creates two objects, writes a reading every three seconds and accepts switch
commands. That covers every building block a real adapter needs: objects,
states, ack semantics, messagebox, logging, shutdown.

    python examples/minimal_adapter.py --instance 0

Then look under "pyexample.0" in the admin UI.
"""

from __future__ import annotations

import asyncio
import math
import sys
import time

sys.path.insert(0, "src")

from iobroker import Adapter, Message, State  # noqa: E402


class ExampleAdapter(Adapter):
    async def on_ready(self) -> None:
        await self.set_object_not_exists(
            "temperature",
            {
                "type": "state",
                "common": {
                    "name": "Temperature",
                    "type": "number",
                    "role": "value.temperature",
                    "unit": "°C",
                    "read": True,
                    "write": False,
                },
            },
        )
        await self.set_object_not_exists(
            "switch",
            {
                "type": "state",
                "common": {
                    "name": "Switch",
                    "type": "boolean",
                    "role": "switch",
                    "read": True,
                    "write": True,
                },
            },
        )
        await self.set_object_not_exists(
            "info.connection",
            {
                "type": "state",
                "common": {
                    "name": "Connected",
                    "type": "boolean",
                    "role": "indicator.connected",
                    "read": True,
                    "write": False,
                },
            },
        )

        # Subscribe to our own states only -- anything else would be noise.
        await self.subscribe_states("*")
        await self.set_state("info.connection", True, ack=True)

        self._worker = asyncio.create_task(self._measure())

    async def _measure(self) -> None:
        """Stands in for a real measurement source."""
        try:
            while True:
                value = round(20 + 2 * math.sin(time.time() / 10), 2)
                # ack=True: a confirmed reading, not a command.
                await self.set_state("temperature", value, ack=True)
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise

    async def on_state_change(self, id: str, state: State | None) -> None:
        if state is None:
            self.log.debug(f"{id} deleted or expired")
            return
        # ack=False means somebody wants something switched.
        if not state.ack and id.endswith(".switch"):
            self.log.info(f"Switch command: {state.val}")
            # Drive the device here ... and confirm afterwards.
            await self.set_state("switch", state.val, ack=True)

    async def on_message(self, msg: Message) -> None:
        self.log.info(f"Message '{msg.command}' from {msg.from_}")
        if msg.command == "ping":
            await self.reply(msg, {"pong": True})

    async def on_unload(self) -> None:
        worker = getattr(self, "_worker", None)
        if worker:
            worker.cancel()
        self.log.info("Cleaned up")


if __name__ == "__main__":
    ExampleAdapter("pyexample").run()
