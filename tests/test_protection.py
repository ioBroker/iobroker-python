"""Tests for withholding another adapter's protected settings.

ioBroker enforces this in the client, not in the database, and this SDK talks to the database
directly -- so these rules are the only thing standing between a foreign adapter and settings the
flag exists to withhold.
"""

from __future__ import annotations

import pytest

from iobroker.protection import strip_protected


def instance(name: str = "hue") -> dict:
    return {
        "_id": f"system.adapter.{name}.0",
        "type": "instance",
        "protectedNative": ["password", "token"],
        "native": {"host": "10.0.0.5", "password": "secret", "token": "abc"},
    }


class TestStripProtected:
    def test_withholds_them_from_another_adapter(self) -> None:
        out = strip_protected("weather", "system.adapter.hue.0", instance())

        assert out["native"] == {"host": "10.0.0.5"}

    def test_keeps_them_for_the_owning_adapter(self) -> None:
        # An adapter has to be able to read its own configuration.
        out = strip_protected("hue", "system.adapter.hue.0", instance())

        assert out["native"]["password"] == "secret"

    @pytest.mark.parametrize("reader", ["admin", "iot", "cloud", "discovery"])
    def test_keeps_them_for_the_exempt_adapters(self, reader: str) -> None:
        # Their job is to show or forward these settings; the list matches js-controller.
        out = strip_protected(reader, "system.adapter.hue.0", instance())

        assert out["native"]["token"] == "abc"

    def test_leaves_the_input_alone(self) -> None:
        # The caller may hold the object for other purposes; quietly emptying fields in a shared
        # dict is the kind of thing that gets debugged for an afternoon.
        obj = instance()
        strip_protected("weather", "system.adapter.hue.0", obj)

        assert obj["native"]["password"] == "secret"

    def test_only_applies_to_instance_objects(self) -> None:
        # The flag has no meaning elsewhere, and stripping there would silently drop data.
        obj = {"_id": "hue.0.lamp", "protectedNative": ["password"], "native": {"password": "x"}}
        out = strip_protected("weather", "hue.0.lamp", obj)

        assert out["native"]["password"] == "x"

    @pytest.mark.parametrize("obj", [None, {"_id": "system.adapter.hue.0", "native": {"a": 1}}])
    def test_passes_through_what_has_no_flag(self, obj) -> None:
        assert strip_protected("weather", "system.adapter.hue.0", obj) == obj

    def test_survives_a_missing_native_section(self) -> None:
        obj = {"_id": "system.adapter.hue.0", "protectedNative": ["password"]}

        assert strip_protected("weather", "system.adapter.hue.0", obj) == obj
