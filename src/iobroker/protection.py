"""Withholding another adapter's protected settings.

An instance object may list entries in ``common.protectedNative``. Those must not reach any adapter
other than the one they belong to — they are the settings an adapter keeps to itself even from
other adapters, on top of whatever encryption applies.

Like the permission check, ioBroker enforces this **in the client**, not in the database. A Python
adapter talks to the database directly, so nothing strips these fields on the way: without doing it
here, reading a foreign instance object would hand over exactly what the flag exists to withhold.
The rule below therefore mirrors ``@iobroker/adapter-core`` rather than inventing its own.
"""

from __future__ import annotations

from typing import Any

__all__ = ["NO_PROTECT_ADAPTERS", "strip_protected"]

#: Adapters exempt from the rule, because their job is to show or forward these settings.
#: Taken from js-controller's constants; diverging would either break them or leak elsewhere.
NO_PROTECT_ADAPTERS = ("admin", "iot", "cloud", "discovery")

_INSTANCE_PREFIX = "system.adapter."


def strip_protected(reader: str, id: str, obj: dict[str, Any] | None) -> dict[str, Any] | None:
    """Remove another adapter's protected settings from an object.

    Applies only to instance objects, since that is the only place the flag has a meaning.

    :param reader: name of the adapter doing the reading, without instance number
    :param id: id of the object that was read
    :param obj: the object; not modified
    :returns: the object, with the protected entries removed where the rule applies
    """
    if not obj or not id.startswith(_INSTANCE_PREFIX):
        return obj

    protected = obj.get("protectedNative")

    if not isinstance(protected, list) or not protected:
        return obj

    if reader in NO_PROTECT_ADAPTERS:
        return obj

    # "system.adapter.hue.0" -> "hue". Reading one's own settings is the normal case and must stay
    # untouched; an adapter has to be able to see its own configuration.
    owner = id.split(".")[2] if id.count(".") >= 2 else ""

    if reader == owner:
        return obj

    native = obj.get("native")

    if not isinstance(native, dict):
        return obj

    # Copied rather than mutated: the caller may hold the object for other purposes, and quietly
    # emptying fields in a shared dict is the kind of thing that is debugged for an afternoon.
    cleaned = dict(obj)
    cleaned["native"] = {k: v for k, v in native.items() if k not in protected}

    return cleaned
