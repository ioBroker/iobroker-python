"""Tests for the object helpers that do not need a database.

The parts that do talk to one are exercised against a running installation instead; unit tests
here cover the logic that is easy to get subtly wrong and would then fail somewhere far away.
"""

from __future__ import annotations

import pytest

from iobroker.adapter import Adapter


@pytest.fixture
def adapter() -> Adapter:
    return Adapter("demo", instance=0)


class TestAbsoluteIds:
    def test_prefixes_a_relative_id(self, adapter: Adapter) -> None:
        assert adapter._abs("temperature") == "demo.0.temperature"

    def test_leaves_our_own_namespace_alone(self, adapter: Adapter) -> None:
        # Prefixing twice would produce demo.0.demo.0.temperature, which silently writes to an
        # object nobody is watching.
        assert adapter._abs("demo.0.temperature") == "demo.0.temperature"

    def test_leaves_system_ids_alone(self, adapter: Adapter) -> None:
        assert adapter._abs("system.adapter.demo.0") == "system.adapter.demo.0"


class TestObjectView:
    @pytest.mark.asyncio
    async def test_rejects_designs_it_cannot_serve(self, adapter: Adapter) -> None:
        # Only the "system" design is implemented. Accepting another one and returning nothing
        # would look like "no objects exist" rather than "not supported".
        with pytest.raises(ValueError, match="system"):
            await adapter.get_object_view("custom", "state")
