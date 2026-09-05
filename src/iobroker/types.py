"""Data types of the ioBroker object model."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

__all__ = ["State", "Message", "now_ms"]


def now_ms() -> int:
    """Timestamp in milliseconds -- the unit ioBroker uses throughout.

    Every timestamp that travels between adapters is in this unit: ``ts`` and ``lc`` on a state,
    ``ts`` on an object, ``createdAt`` on a file. Python's ``time.time()`` returns seconds as a
    float, so a value handed over without this conversion lands in 1970 and is very hard to see.
    """
    return int(time.time() * 1000)


@dataclass
class State:
    """An ioBroker state: a value plus everything the system records about it.

    ``ack`` carries the meaning: ``False`` is a command towards the device,
    ``True`` a confirmed reading. Confusing the two builds feedback loops.

    Constructing one directly is only needed when a field other than the value matters --
    :meth:`~iobroker.Adapter.set_state` takes a bare value and builds the rest. Reporting the last
    known reading of a device that has since gone away is the usual reason::

        await self.set_state("lamp.level", State(val=80, ack=True, q=0x02))
    """

    #: The value. Anything JSON can carry: a number, a string, a bool, a list, a dict.
    val: Any
    #: ``True`` for a confirmed reading, ``False`` for a command still to be carried out. The
    #: single most important field after the value itself, and the one adapters get wrong.
    ack: bool = False
    #: When this value was written, in milliseconds. Set automatically.
    ts: int = field(default_factory=now_ms)
    #: "Last change" -- when the value last actually *changed*, as opposed to when it was last
    #: written. Left at ``None`` so :meth:`~iobroker.Adapter.set_foreign_state` can carry the
    #: previous one forward for an unchanged write; that is what lets a sensor polled every 30
    #: seconds not look as though it changes every 30 seconds.
    lc: int | None = None
    #: Quality. ``0`` is good; the other values say why a reading is not to be trusted (device
    #: unreachable, substitute value, and so on), as listed in the ioBroker documentation.
    q: int = 0
    #: Who wrote it, as ``system.adapter.<namespace>``. Filled in automatically. Trailing
    #: underscore because ``from`` is a Python keyword; it goes over the wire as ``from``.
    from_: str = ""
    #: The user on whose behalf it was written, where anything tracks that (``system.user.admin``).
    user: str | None = None
    #: Not serialized, and not read by this SDK: the expiry that actually applies is the ``expire``
    #: argument of :meth:`~iobroker.Adapter.set_state`. Kept because the field exists in the JS
    #: state shape and code ported from there refers to it.
    expire: int | None = None
    #: A free comment on the write. Rarely used.
    c: str | None = None

    def to_wire(self) -> dict[str, Any]:
        """Serialize into the shape the JS client writes as well.

        ``lc`` falls back to ``ts`` rather than being omitted -- a state without ``lc`` would make
        every consumer that reads it fall over, and "changed when it was written" is the truth for
        a value nothing has compared yet.

        ``user`` and ``c`` are left out when unset instead of being written as ``null``, which is
        again what the JS client does: a reader tells "absent" from "explicitly nothing".
        """
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
        """Build a state from what the database returned.

        Forgiving on purpose: states in a long-lived installation were written by many versions of
        many adapters, and a missing ``ts`` or a ``q`` that arrives as a string must not turn a
        state change into an exception in the middle of a pump. Anything absent falls back to the
        same default a fresh state would have.

        :param raw: the decoded JSON as it was stored
        """
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

    #: What the message asks for -- what a receiver switches on. Free-form; the pair of adapters
    #: agree on it between themselves.
    command: str
    #: The payload. Anything JSON can carry, and often ``None`` for a command that needs no
    #: argument.
    message: Any
    #: The sender, as ``system.adapter.<namespace>``. Trailing underscore because ``from`` is a
    #: Python keyword. :meth:`~iobroker.Adapter.reply` answers here.
    from_: str
    #: Present when the sender is waiting for an answer, and carried back unchanged in the reply so
    #: the sender can match it to the call it made. ``None`` for fire-and-forget.
    callback: dict[str, Any] | None = None
    #: The sender's own sequence number. Not needed for replying -- the callback carries what
    #: matters -- and kept only because it is part of the message on the wire.
    _id: int = 0

    @property
    def wants_reply(self) -> bool:
        """Whether the sender is waiting for an answer.

        Both halves are needed. No callback at all means fire-and-forget; a callback whose ``ack``
        is already ``True`` means this *is* the answer to somebody else's question, and replying to
        it would send an answer to an answer.
        """
        return bool(self.callback and self.callback.get("ack") is not True)
