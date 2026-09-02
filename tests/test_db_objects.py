"""Objects against a real database -- the Python counterpart of js-controller's
``test/lib/testObjects.ts`` and ``testObjectsFunctions.ts``.

The type index (``cfg.s.object.type.*``) gets its own attention here: an object
missing from it silently disappears from every ``getObjectView`` in the system,
including the ones admin uses to render the object tree.
"""

from __future__ import annotations

from iobroker.types import now_ms
from support import only_real_redis

STATE_OBJ = {"type": "state", "common": {"name": "dp", "role": "value"}, "native": {}}


class TestSetAndGet:
    async def test_roundtrip(self, adapter) -> None:
        await adapter.set_object("dp", dict(STATE_OBJ))

        obj = await adapter.get_object("dp")

        assert obj["_id"] == "pytest.0.dp"
        assert obj["type"] == "state"
        assert obj["common"] == {"name": "dp", "role": "value"}
        assert obj["from"] == "system.adapter.pytest.0"
        assert abs(obj["ts"] - now_ms()) < 5000

    async def test_a_missing_native_section_is_added(self, adapter) -> None:
        await adapter.set_object("bare", {"type": "state", "common": {"name": "x"}})

        assert (await adapter.get_object("bare"))["native"] == {}

    async def test_missing_object_is_none(self, adapter) -> None:
        assert await adapter.get_object("never.written") is None

    async def test_set_object_not_exists_keeps_the_existing_object(self, adapter) -> None:
        # This is what protects user edits to common from being reset on every
        # adapter start.
        first = await adapter.set_object_not_exists(
            "guarded", {"type": "state", "common": {"name": "original"}, "native": {}}
        )
        second = await adapter.set_object_not_exists(
            "guarded", {"type": "state", "common": {"name": "overwrite"}, "native": {}}
        )

        assert first is True
        assert second is False
        assert (await adapter.get_object("guarded"))["common"]["name"] == "original"


class TestExtend:
    async def test_merges_common_and_native_per_key(self, adapter) -> None:
        await adapter.set_object(
            "merged",
            {"type": "state", "common": {"name": "a", "role": "value"}, "native": {"a": 1}},
        )

        await adapter.extend_object("merged", {"common": {"name": "b"}, "native": {"b": 2}})

        obj = await adapter.get_object("merged")
        assert obj["common"] == {"name": "b", "role": "value"}
        assert obj["native"] == {"a": 1, "b": 2}

    async def test_replaces_everything_else(self, adapter) -> None:
        await adapter.set_object("retyped", dict(STATE_OBJ))

        await adapter.extend_object("retyped", {"type": "channel"})

        assert (await adapter.get_object("retyped"))["type"] == "channel"

    async def test_extending_a_missing_object_creates_it(self, adapter) -> None:
        await adapter.extend_object("fresh", {"type": "state", "common": {"name": "n"}})

        obj = await adapter.get_object("fresh")
        assert obj["common"] == {"name": "n"}
        assert obj["_id"] == "pytest.0.fresh"


class TestDelete:
    async def test_deleted_object_is_gone(self, adapter, raw_objects) -> None:
        await adapter.set_object("doomed", dict(STATE_OBJ))
        await adapter.delete_object("doomed")

        assert await adapter.get_object("doomed") is None
        assert await raw_objects.get("cfg.o.pytest.0.doomed") is None


class TestTypeIndex:
    """Only against real Redis: the index is read back with smembers/exists,
    which the built-in servers do not implement -- there the SDK's view path
    falls back to scanning anyway (covered in TestViews on both backends)."""

    async def test_objects_are_added_to_their_type_set(self, adapter, db) -> None:
        only_real_redis(db, "smembers to inspect the index")
        db.objects_sync.set("meta.objects.features.useSets", "1")

        await adapter.set_object("indexed", dict(STATE_OBJ))

        assert "cfg.o.pytest.0.indexed" in db.objects_sync.smembers("cfg.s.object.type.state")

    async def test_deleting_removes_from_the_type_set(self, adapter, db) -> None:
        only_real_redis(db, "smembers to inspect the index")
        db.objects_sync.set("meta.objects.features.useSets", "1")
        await adapter.set_object("indexed", dict(STATE_OBJ))

        await adapter.delete_object("indexed")

        # A stale entry would make the object show up in views pointing at nothing.
        assert "cfg.o.pytest.0.indexed" not in db.objects_sync.smembers("cfg.s.object.type.state")

    async def test_without_the_feature_no_set_is_written(self, adapter, db) -> None:
        only_real_redis(db, "exists to inspect the index")
        await adapter.set_object("unindexed", dict(STATE_OBJ))

        assert db.objects_sync.exists("cfg.s.object.type.state") == 0


class TestViews:
    """Runs on both backends. With the feature flag on, real Redis serves views
    from the type sets; the built-in servers know no smembers, so the SDK falls
    back to scanning there -- the answers must be the same either way."""

    async def _create_zoo(self, adapter) -> None:
        await adapter.set_object("aa", dict(STATE_OBJ))
        await adapter.set_object("bb", dict(STATE_OBJ))
        await adapter.set_object("ch", {"type": "channel", "common": {"name": "c"}, "native": {}})
        await adapter.set_foreign_object(
            "pytestother.0.x", {"type": "state", "common": {"name": "x"}, "native": {}}
        )

    async def test_view_over_the_type_index(self, adapter, db) -> None:
        db.objects_sync.set("meta.objects.features.useSets", "1")
        await self._create_zoo(adapter)

        states = await adapter.get_object_view("system", "state")
        channels = await adapter.get_object_view("system", "channel")

        assert [o["_id"] for o in states] == ["pytest.0.aa", "pytest.0.bb", "pytestother.0.x"]
        assert [o["_id"] for o in channels] == ["pytest.0.ch"]

    async def test_view_respects_the_key_range(self, adapter, db) -> None:
        db.objects_sync.set("meta.objects.features.useSets", "1")
        await self._create_zoo(adapter)

        # The JS convention: namespace as startkey, namespace + U+9999 as endkey.
        own = await adapter.get_object_view("system", "state", "pytest.0.", "pytest.0.香")

        assert [o["_id"] for o in own] == ["pytest.0.aa", "pytest.0.bb"]

    async def test_view_falls_back_to_scanning_without_the_index(self, adapter) -> None:
        # Installations with the sets switched off must still get answers.
        await self._create_zoo(adapter)

        states = await adapter.get_object_view("system", "state")

        assert [o["_id"] for o in states] == ["pytest.0.aa", "pytest.0.bb", "pytestother.0.x"]

    async def test_view_falls_back_when_the_set_is_empty(self, adapter, db) -> None:
        # Objects written while the feature was off are not in the sets; an empty
        # set must not read as "no objects exist".
        await self._create_zoo(adapter)
        db.objects_sync.set("meta.objects.features.useSets", "1")

        states = await adapter.get_object_view("system", "state")

        assert len(states) == 3


class TestProtectedNative:
    HUE = {
        "type": "instance",
        "common": {"name": "hue"},
        "protectedNative": ["password"],
        "native": {"host": "10.0.0.5", "password": "secret"},
    }

    async def test_foreign_instance_objects_are_stripped_on_read(self, adapter) -> None:
        await adapter.set_foreign_object("system.adapter.pytesthue.0", dict(self.HUE))

        obj = await adapter.get_foreign_object("system.adapter.pytesthue.0")

        assert obj["native"] == {"host": "10.0.0.5"}

    async def test_views_are_stripped_too(self, adapter, db) -> None:
        # Reading through a view must not be the loophole around protectedNative.
        db.objects_sync.set("meta.objects.features.useSets", "1")
        await adapter.set_foreign_object("system.adapter.pytesthue.0", dict(self.HUE))

        instances = await adapter.get_object_view("system", "instance")

        assert instances[0]["native"] == {"host": "10.0.0.5"}


class TestAdapterObjects:
    async def test_returns_only_the_own_namespace(self, adapter) -> None:
        await adapter.set_object("mine.a", dict(STATE_OBJ))
        await adapter.set_object("mine.b", dict(STATE_OBJ))
        await adapter.set_foreign_object("pytestother.0.x", dict(STATE_OBJ))

        objects = await adapter.get_adapter_objects()

        assert sorted(objects) == ["pytest.0.mine.a", "pytest.0.mine.b"]
        assert objects["pytest.0.mine.a"]["type"] == "state"
