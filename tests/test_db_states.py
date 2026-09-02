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
