"""Data types of the ioBroker object model."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

__all__ = ["State", "Message", "now_ms"]


def now_ms() -> int:
    """Timestamp in milliseconds -- the unit ioBroker uses throughout."""
    return int(time.time() * 1000)


@dataclass
class State:
    """An ioBroker state.

    ``ack`` carries the meaning: ``False`` is a command towards the device,
    ``True`` a confirmed reading. Confusing the two builds feedback loops.
    """

    val: Any
    ack: bool = False
    ts: int = field(default_factory=now_ms)
    lc: int | None = None
    q: int = 0
    from_: str = ""
    user: str | None = None
    expire: int | None = None
    c: str | None = None

    def to_wire(self) -> dict[str, Any]:
        """Serialize into the shape the JS client writes as well."""
        out: dict[str, Any] = {
            "val": self.val,
            "ack": self.ack,
            "ts": self.ts,
            "lc": self.lc if self.lc is not None else self.ts,
            "q": self.q,
            "from": self.from_,
        }
        if self.user is not None:
            out["user"] = self.user
        if self.c is not None:
            out["c"] = self.c
        return out

    @classmethod
    def from_wire(cls, raw: dict[str, Any]) -> "State":
        return cls(
            val=raw.get("val"),
            ack=bool(raw.get("ack", False)),
            ts=int(raw.get("ts") or now_ms()),
            lc=raw.get("lc"),
            q=int(raw.get("q") or 0),
            from_=raw.get("from", ""),
            user=raw.get("user"),
            c=raw.get("c"),
        )


@dataclass
class Message:
    """A messagebox message.

    ``callback`` is set when the sender expects a reply.
    """

    command: str
    message: Any
    from_: str
    callback: dict[str, Any] | None = None
    _id: int = 0

    @property
    def wants_reply(self) -> bool:
        return bool(self.callback and self.callback.get("ack") is not True)
