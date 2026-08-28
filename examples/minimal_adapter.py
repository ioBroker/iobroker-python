"""Kleinster vollstaendiger Python-Adapter.

Legt zwei Objekte an, schreibt alle drei Sekunden einen Messwert und nimmt
Schaltbefehle entgegen. Zeigt damit alle Bausteine, die ein echter Adapter
braucht: Objekte, States, ack-Semantik, Messagebox, Logging, Shutdown.

    python examples/minimal_adapter.py --instance 0

Danach im Admin unter "pyexample.0" nachsehen.
Aufraeumen: python examples/minimal_adapter.py --cleanup
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
                    "name": "Temperatur",
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
                    "name": "Schalter",
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
                    "name": "Verbunden",
                    "type": "boolean",
                    "role": "indicator.connected",
                    "read": True,
                    "write": False,
                },
            },
        )

        # Nur die eigenen States abonnieren -- alles andere waere Laerm.
        await self.subscribe_states("*")
        await self.set_state("info.connection", True, ack=True)

        self._worker = asyncio.create_task(self._measure())

    async def _measure(self) -> None:
        """Simuliert eine Messquelle."""
        try:
            while True:
                value = round(20 + 2 * math.sin(time.time() / 10), 2)
                # ack=True: bestaetigter Messwert, kein Befehl.
                await self.set_state("temperature", value, ack=True)
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise

    async def on_state_change(self, id: str, state: State | None) -> None:
        if state is None:
            self.log.debug(f"{id} geloescht oder abgelaufen")
            return
        # ack=False heisst: jemand will etwas schalten.
        if not state.ack and id.endswith(".switch"):
            self.log.info(f"Schaltbefehl: {state.val}")
            # Geraet schalten ... und danach bestaetigen.
            await self.set_state("switch", state.val, ack=True)

    async def on_message(self, msg: Message) -> None:
        self.log.info(f"Nachricht '{msg.command}' von {msg.from_}")
        if msg.command == "ping":
            await self.reply(msg, {"pong": True})

    async def on_unload(self) -> None:
        worker = getattr(self, "_worker", None)
        if worker:
            worker.cancel()
        self.log.info("Aufgeraeumt")


if __name__ == "__main__":
    ExampleAdapter("pyexample").run()
