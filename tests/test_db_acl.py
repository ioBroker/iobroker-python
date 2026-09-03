"""Default-ACL stamping against a real database -- the Python counterpart of
js-controller's ``test/lib/testObjectsACL.ts``.

ioBroker access control is enforced cooperatively in the client, not by the database. The JS
objects client stamps every newly created object that carries no ``acl`` of its own with
``system.config.common.defaultNewAcl``; an SDK that talks to the database directly has the same
obligation (see ``doc/PYTHON.md``, "ACL -- what the SDK must implement"). Without it, admin would
show and treat SDK-created objects differently from every other object in the system.

These run over **both backends**: the loading and stamping paths are plain GET/SET and behave the
same on real Redis and on the built-in jsonl servers.
"""

from __future__ import annotations

import json

from support import write_object

# Distinct sentinel numbers so an assertion pins down exactly which field landed where. The shape
# matches a real system.config: owner/ownerGroup plus per-kind permission bitmasks.
DEFAULT_ACL = {
    "owner": "system.user.tester",
    "ownerGroup": "system.group.testers",
    "object": 1636,
    "state": 1637,
    "file": 1632,
}


class TestStamping:
    """A new object without an acl inherits the installation's default."""

    async def test_a_new_state_gets_owner_group_object_and_state(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)

        await adapter.set_object("dp", {"type": "state", "common": {"name": "x"}, "native": {}})

        acl = (await adapter.get_object("dp"))["acl"]
        assert acl == {
            "owner": "system.user.tester",
            "ownerGroup": "system.group.testers",
            "object": 1636,
            "state": 1637,
        }
        # The file default is the default for files, never part of an object's own acl.
        assert "file" not in acl

    async def test_a_non_state_omits_the_state_bits(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)

        await adapter.set_object("ch", {"type": "channel", "common": {"name": "c"}, "native": {}})

        acl = (await adapter.get_object("ch"))["acl"]
        assert acl == {
            "owner": "system.user.tester",
            "ownerGroup": "system.group.testers",
            "object": 1636,
        }
        assert "state" not in acl and "file" not in acl

    async def test_an_explicit_acl_is_left_untouched(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)
        mine = {"owner": "system.user.someone", "ownerGroup": "system.group.x", "object": 1024}

        await adapter.set_object(
            "owned", {"type": "state", "common": {"name": "x"}, "native": {}, "acl": dict(mine)}
        )

        assert (await adapter.get_object("owned"))["acl"] == mine

    async def test_overwriting_preserves_the_stored_acl(self, adapter) -> None:
        # A plain setObject that omits the acl must not reset rights a user may have changed:
        # the acl already on the stored object wins over the default.
        adapter._default_new_acl = dict(DEFAULT_ACL)
        kept = {"owner": "system.user.someone", "ownerGroup": "system.group.x", "object": 1024}
        await adapter.set_object(
            "keep", {"type": "state", "common": {"name": "x"}, "native": {}, "acl": dict(kept)}
        )

        await adapter.set_object("keep", {"type": "state", "common": {"name": "y"}, "native": {}})

        obj = await adapter.get_object("keep")
        assert obj["acl"] == kept  # rights survived
        assert obj["common"]["name"] == "y"  # the rest was overwritten

    async def test_a_supplied_acl_wins_over_the_stored_one(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)
        await adapter.set_object(
            "swap",
            {"type": "state", "common": {"name": "x"}, "native": {}, "acl": {"object": 1000}},
        )

        await adapter.set_object(
            "swap",
            {"type": "state", "common": {"name": "x"}, "native": {}, "acl": {"object": 2000}},
        )

        assert (await adapter.get_object("swap"))["acl"] == {"object": 2000}

    async def test_without_a_default_no_acl_is_invented(self, adapter) -> None:
        # Installations that never configured defaultNewAcl (older ones) must keep behaving as
        # before: the SDK adds nothing, exactly like the JS client on such installs.
        assert adapter._default_new_acl is None

        await adapter.set_object("plain", {"type": "state", "common": {"name": "x"}, "native": {}})

        assert "acl" not in (await adapter.get_object("plain"))

    async def test_set_object_not_exists_stamps_the_default(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)

        await adapter.set_object_not_exists(
            "fresh", {"type": "state", "common": {"name": "x"}, "native": {}}
        )

        assert (await adapter.get_object("fresh"))["acl"]["object"] == 1636

    async def test_extend_stamps_on_create_and_keeps_it_on_patch(self, adapter) -> None:
        adapter._default_new_acl = dict(DEFAULT_ACL)

        # extendObject that creates the object -> the default is stamped.
        await adapter.extend_object("grown", {"type": "state", "common": {"name": "x"}})
        stamped = (await adapter.get_object("grown"))["acl"]
        assert stamped["object"] == 1636 and stamped["state"] == 1637

        # A later extend only patches; the acl must stay as it was, not be re-derived.
        await adapter.extend_object("grown", {"common": {"role": "value"}})
        assert (await adapter.get_object("grown"))["acl"] == stamped


class TestLoading:
    """Where the default comes from: ``system.config.common.defaultNewAcl``."""

    async def test_reads_the_default_from_system_config(self, adapter, raw_objects) -> None:
        await write_object(
            raw_objects,
            "system.config",
            {"type": "config", "common": {"defaultNewAcl": dict(DEFAULT_ACL)}, "native": {}},
        )

        await adapter._load_default_acl()

        assert adapter._default_new_acl == DEFAULT_ACL

    async def test_an_installation_without_a_default_stays_none(self, adapter, raw_objects) -> None:
        await write_object(
            raw_objects, "system.config", {"type": "config", "common": {}, "native": {}}
        )

        await adapter._load_default_acl()

        assert adapter._default_new_acl is None


class TestTracking:
    """A change made in admin reaches new objects without a restart."""

    async def test_a_system_config_change_updates_the_cached_default(self, adapter) -> None:
        assert adapter._default_new_acl is None
        payload = json.dumps(
            {"type": "config", "common": {"defaultNewAcl": dict(DEFAULT_ACL)}, "native": {}}
        )

        # The object-change dispatch is what the subscription on system.config feeds into.
        await adapter._dispatch("cfg.o.system.config", payload)

        assert adapter._default_new_acl == DEFAULT_ACL


class TestFullStartup:
    """The whole path a running adapter takes: startup loads the default, objects created
    afterwards carry it -- proving _load_default_acl is wired into _main."""

    async def test_a_started_adapter_stamps_from_system_config(self, run_adapter, raw_objects) -> None:
        await write_object(
            raw_objects,
            "system.config",
            {"type": "config", "common": {"defaultNewAcl": dict(DEFAULT_ACL)}, "native": {}},
        )

        a, _ = await run_adapter()
        await a.set_object("dp", {"type": "state", "common": {"name": "x"}, "native": {}})

        acl = (await a.get_object("dp"))["acl"]
        assert acl["owner"] == "system.user.tester"
        assert acl["object"] == 1636 and acl["state"] == 1637
