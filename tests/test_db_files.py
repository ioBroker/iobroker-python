"""The file store against a real database -- the Python counterpart of
js-controller's ``test/lib/testFiles.ts``.

The one thing that must never regress here is that content is carried as raw
bytes: the rest of the SDK decodes replies as text, and a PNG pulled through a
UTF-8 decoder is gone for good. The binary roundtrip below uses all 256 byte
values for exactly that reason.

Every test writes under its own file id: the built-in servers have no flushdb,
and unique ids make leftovers from one test invisible to the next on every
backend.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from support import only_builtin, only_real_redis


@pytest.fixture
def file_id() -> str:
    return f"pytest.0.files-{uuid4().hex[:8]}"


class TestWriteAndRead:
    async def test_text_roundtrip(self, adapter, file_id) -> None:
        await adapter.write_file(file_id, "notes.txt", "hello wörld")

        data = await adapter.read_file(file_id, "notes.txt")
        meta = await adapter.read_file_meta(file_id, "notes.txt")

        assert data == "hello wörld".encode()
        assert meta.size == len(data)
        assert meta.mime_type == "text/plain"
        assert meta.binary is False
        assert meta.created_at is not None and meta.modified_at is not None

    async def test_binary_survives_every_byte_value(self, adapter, file_id) -> None:
        payload = bytes(range(256))

        await adapter.write_file(file_id, "blob", payload)

        assert await adapter.read_file(file_id, "blob") == payload
        meta = await adapter.read_file_meta(file_id, "blob")
        assert meta.mime_type == "application/octet-stream"
        assert meta.binary is True

    async def test_extension_decides_the_type(self, adapter, file_id) -> None:
        # An SVG is an image yet text -- the exact case the admin UI serves wrong
        # when the flag is off.
        await adapter.write_file(file_id, "icon.svg", b"<svg/>")

        meta = await adapter.read_file_meta(file_id, "icon.svg")

        assert meta.mime_type == "image/svg+xml"
        assert meta.binary is False

    async def test_explicit_mime_type_wins(self, adapter, file_id) -> None:
        await adapter.write_file(file_id, "data.bin", b"x", mime_type="application/x-custom")

        assert (await adapter.read_file_meta(file_id, "data.bin")).mime_type == (
            "application/x-custom"
        )

    async def test_missing_file_is_none(self, adapter, file_id) -> None:
        assert await adapter.read_file(file_id, "no/such.file") is None
        assert await adapter.read_file_meta(file_id, "no/such.file") is None


class TestOverwrite:
    async def test_keeps_created_at_and_bumps_modified_at(self, adapter, file_id) -> None:
        await adapter.write_file(file_id, "log.txt", "v1")
        first = await adapter.read_file_meta(file_id, "log.txt")
        await asyncio.sleep(0.05)

        await adapter.write_file(file_id, "log.txt", "version two")
        second = await adapter.read_file_meta(file_id, "log.txt")

        assert second.created_at == first.created_at
        assert second.modified_at > first.modified_at
        assert second.size == len(b"version two")
        assert await adapter.read_file(file_id, "log.txt") == b"version two"


class TestUnlink:
    async def test_removes_content_and_meta(self, adapter, raw_objects, file_id) -> None:
        await adapter.write_file(file_id, "gone.txt", "x")

        await adapter.unlink(file_id, "gone.txt")

        assert await adapter.read_file(file_id, "gone.txt") is None
        assert await adapter.read_file_meta(file_id, "gone.txt") is None
        # The built-in server keeps a synthetic _data.json marker for the
        # directory itself; what must be gone is every key of the file.
        leftovers = [
            key for key in await raw_objects.keys(f"cfg.f.{file_id}$%$*") if "_data.json" not in key
        ]
        assert leftovers == []

    async def test_unlinking_a_missing_file_does_not_raise(self, adapter, file_id) -> None:
        await adapter.unlink(file_id, "never/existed.txt")


class TestReadDir:
    async def _populate(self, adapter, file_id) -> None:
        await adapter.write_file(file_id, "top.txt", "t")
        await adapter.write_file(file_id, "icons/lamp.png", b"\x89PNG")
        # Written with backslashes, as Windows callers do -- must land under icons/.
        await adapter.write_file(file_id, "icons\\deep.png", b"\x89PNG")

    async def test_lists_a_directory(self, adapter, file_id) -> None:
        await self._populate(adapter, file_id)

        entries = await adapter.read_dir(file_id, "icons")

        assert {e["file"] for e in entries} == {"lamp.png", "deep.png"}
        assert all(e["is_dir"] is False for e in entries)

    async def test_top_level_against_real_redis_returns_the_flat_tree(
        self, db, adapter, file_id
    ) -> None:
        # Real Redis globs the whole subtree, so nested files appear with their
        # full path. This pins that shape.
        only_real_redis(db, "the built-in server answers one level instead")
        await self._populate(adapter, file_id)

        entries = await adapter.read_dir(file_id)

        assert {e["file"] for e in entries} == {"top.txt", "icons/lamp.png", "icons/deep.png"}

    async def test_top_level_against_the_builtin_server_returns_one_level(
        self, db, adapter, file_id
    ) -> None:
        # The built-in server treats the pattern as a directory and answers one
        # level, with subdirectories as synthetic entries the SDK turns into
        # is_dir -- the shape the JS clients see on a default installation.
        only_builtin(db, "real Redis globs the whole subtree instead")
        await self._populate(adapter, file_id)

        entries = {e["file"]: e["is_dir"] for e in await adapter.read_dir(file_id)}

        assert entries == {"top.txt": False, "icons": True}

    async def test_leading_slash_reads_the_same_directory(self, adapter, file_id) -> None:
        await self._populate(adapter, file_id)

        assert await adapter.read_dir(file_id, "/icons") == await adapter.read_dir(
            file_id, "icons"
        )

    async def test_empty_directory_is_an_empty_list(self, adapter, file_id) -> None:
        assert await adapter.read_dir(file_id, "void") == []
