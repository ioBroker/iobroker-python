"""States against a real database -- the Python counterpart of js-controller's
``test/lib/testStates.ts``.

Everything here goes over the wire: what ``set_state`` writes is read back raw
to assert on the stored shape, because the JavaScript clients and the admin UI
read these keys too and only the wire format is the contract.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from iobroker.types import State, now_ms
from support import expect_pmessage, only_real_redis, read_state


class TestSetAndGet:
    async def test_roundtrip_in_the_own_namespace(self, adapter) -> None:
        await adapter.set_state("temperature", 21.5)

        state = await adapter.get_foreign_state("pytest.0.temperature")

        assert state is not None
        assert state.val == 21.5
        assert state.ack is False
        assert state.from_ == "system.adapter.pytest.0"
        # ts is stamped by the SDK in ms; a wildly wrong unit (seconds) would land far away.
        assert abs(state.ts - now_ms()) < 5000

    async def test_relative_and_absolute_reads_are_the_same_state(self, adapter) -> None:
        await adapter.set_state("temperature", 7)

        assert (await adapter.get_state("temperature")).val == 7
        assert (await adapter.get_state("pytest.0.temperature")).val == 7

    async def test_ack_is_preserved(self, adapter) -> None:
        await adapter.set_state("reading", 42, ack=True)

        assert (await adapter.get_state("reading")).ack is True

    @pytest.mark.parametrize(
        "val", [0, 1, -3, 21.5, True, False, "text", "übergroß ✓", "", None]
    )
    async def test_value_types_survive_the_wire(self, adapter, val) -> None:
        await adapter.set_state("value", val)

        state = await adapter.get_state("value")

        # val None comes back as a state whose value is None -- distinct from a
        # missing state, which is None itself.
        assert state is not None
        assert state.val == val

    async def test_a_state_object_passes_through(self, adapter) -> None:
        await adapter.set_foreign_state(
            "pytest.0.rich",
            State(val=5, ack=True, q=0x42, c="test comment", user="system.user.admin"),
        )

        stored = await read_state(adapter._states, "pytest.0.rich")

        assert stored["val"] == 5
        assert stored["ack"] is True
        assert stored["q"] == 0x42
        assert stored["c"] == "test comment"
        assert stored["user"] == "system.user.admin"
        # An empty from is filled with the writing instance -- who wrote a state
        # is what makes feedback loops debuggable.
        assert stored["from"] == "system.adapter.pytest.0"

    async def test_missing_state_is_none(self, adapter) -> None:
        assert await adapter.get_state("never.written") is None

    async def test_system_ids_are_not_prefixed(self, adapter, raw) -> None:
        await adapter.set_state("system.adapter.pytest.0.custom", 1)

        assert await raw.get("io.system.adapter.pytest.0.custom") is not None
        assert await raw.get("io.pytest.0.system.adapter.pytest.0.custom") is None


class TestAwkwardIdsAndPayloads:
    """ioBroker ids and values are UTF-8 and unbounded in practice; neither the lowercasing
    command packer nor the built-in server may mangle them."""

    async def test_unicode_in_an_id(self, adapter) -> None:
        await adapter.set_state("küche.temperatur.温度", 21)

        assert (await adapter.get_state("küche.temperatur.温度")).val == 21

    async def test_unicode_in_a_value(self, adapter) -> None:
        await adapter.set_state("greeting", "Grüße 温度 🌡")

        assert (await adapter.get_state("greeting")).val == "Grüße 温度 🌡"

    async def test_a_large_payload_survives(self, adapter) -> None:
        # Big enough to cross the socket buffer, which is where a length-handling bug shows up.
        payload = "x" * 100_000

        await adapter.set_state("big", payload)

        assert (await adapter.get_state("big")).val == payload

    async def test_a_deeply_nested_id(self, adapter) -> None:
        deep = ".".join(f"level{i}" for i in range(12))

        await adapter.set_state(deep, "bottom")

        assert (await adapter.get_state(deep)).val == "bottom"


class TestLastChange:
    """``lc`` is what separates "the reading is new" from "the reading was refreshed".

    The JS client only moves it when the value actually changed; a sensor polled every 30 seconds
    with a steady reading must not look like it changes every 30 seconds.
    """

    async def test_the_first_write_sets_lc_to_ts(self, adapter, raw) -> None:
        await adapter.set_state("lc.fresh", 5, ack=True)

        stored = await read_state(raw, "pytest.0.lc.fresh")
        assert stored["lc"] == stored["ts"]

    async def test_an_unchanged_value_keeps_lc(self, adapter, raw) -> None:
        await adapter.set_state("lc.steady", 5, ack=True)
        first = await read_state(raw, "pytest.0.lc.steady")
        await asyncio.sleep(0.01)

        await adapter.set_state("lc.steady", 5, ack=True)

        second = await read_state(raw, "pytest.0.lc.steady")
        assert second["lc"] == first["lc"], "lc moved although the value did not change"
        assert second["ts"] > first["ts"], "ts must still move on every write"

    async def test_a_changed_value_moves_lc(self, adapter, raw) -> None:
        await adapter.set_state("lc.moving", 5, ack=True)
        first = await read_state(raw, "pytest.0.lc.moving")
        await asyncio.sleep(0.01)

        await adapter.set_state("lc.moving", 6, ack=True)

        second = await read_state(raw, "pytest.0.lc.moving")
        assert second["lc"] > first["lc"]
        assert second["lc"] == second["ts"]

    async def test_an_explicit_lc_is_kept(self, adapter, raw) -> None:
        # A caller that knows when the value changed (replaying history, say) must win.
        await adapter.set_foreign_state("pytest.0.lc.pinned", State(val=1, ack=True, lc=12345))

        assert (await read_state(raw, "pytest.0.lc.pinned"))["lc"] == 12345

    async def test_true_and_one_count_as_a_change(self, adapter, raw) -> None:
        # Python's == treats True and 1 as equal, the JS client's isDeepStrictEqual does not.
        # A switch flipping between them must not silently stop moving lc.
        await adapter.set_state("lc.switch", 1, ack=True)
        first = await read_state(raw, "pytest.0.lc.switch")
        await asyncio.sleep(0.01)

        await adapter.set_state("lc.switch", True, ack=True)

        second = await read_state(raw, "pytest.0.lc.switch")
        assert second["lc"] > first["lc"]

    async def test_a_reused_state_object_is_not_pinned(self, adapter, raw) -> None:
        # The computed lc must never be written back onto the caller's State: reusing one object
        # for repeated writes would otherwise carry the first lc forward for good.
        reused = State(val=7, ack=True)
        await adapter.set_foreign_state("pytest.0.lc.reused", reused)
        await asyncio.sleep(0.01)
        await adapter.set_foreign_state("pytest.0.lc.reused", reused)
        await asyncio.sleep(0.01)

        assert reused.lc is None
        reused.val = 8
        await adapter.set_foreign_state("pytest.0.lc.reused", reused)

        stored = await read_state(raw, "pytest.0.lc.reused")
        assert stored["val"] == 8
        assert stored["lc"] == stored["ts"], "a real change must move lc even on a reused object"


class TestPublish:
    async def test_set_state_publishes_what_it_stores(self, adapter, raw) -> None:
        """Write and publish happen in one MULTI; subscribers must see the exact payload.

        This also proves the MULTI path (pack_commands) against a real server.
        The channel is matched loosely on purpose: real Redis delivers it with
        the "io." prefix, the built-in server without.
        """
        ps = raw.pubsub()
        await ps.psubscribe("io.pytest.0.*")
        try:
            msg = await expect_pmessage(
                ps,
                lambda: adapter.set_state("published", 99, ack=True),
                lambda m: m["channel"].endswith("pytest.0.published"),
            )

            published = json.loads(msg["data"])
            stored = json.loads(await raw.get("io.pytest.0.published"))
            assert published["val"] == stored["val"] == 99
            assert published["ack"] is stored["ack"] is True
        finally:
            await ps.aclose()

    async def test_delete_publishes_null(self, adapter, raw) -> None:
        # "null" on the channel is how subscribers learn a state is gone.
        await adapter.set_state("doomed", 1)
        ps = raw.pubsub()
        await ps.psubscribe("io.pytest.0.doomed")
        try:
            msg = await expect_pmessage(
                ps,
                lambda: adapter.delete_state("doomed"),
                lambda m: m["data"] == "null",
            )
            assert msg["channel"].endswith("pytest.0.doomed")
        finally:
            await ps.aclose()


class TestExpiry:
    async def test_expire_sets_a_ttl(self, db, adapter, raw) -> None:
        only_real_redis(db, "the ttl command")
        await adapter.set_state("volatile", 7, expire=5)

        assert 0 < await raw.ttl("io.pytest.0.volatile") <= 5
        assert (await adapter.get_state("volatile")).val == 7

    async def test_expired_state_is_gone(self, adapter) -> None:
        await adapter.set_state("shortlived", 7, expire=1)
        assert (await adapter.get_state("shortlived")).val == 7

        await asyncio.sleep(1.3)

        assert await adapter.get_state("shortlived") is None


class TestDelete:
    async def test_deleted_state_is_gone(self, adapter, raw) -> None:
        await adapter.set_state("gone", 1)
        await adapter.delete_state("gone")

        assert await adapter.get_state("gone") is None
        assert await raw.get("io.pytest.0.gone") is None

    async def test_deleting_a_missing_state_does_not_raise(self, adapter) -> None:
        await adapter.delete_foreign_state("pytest.0.never.there")
