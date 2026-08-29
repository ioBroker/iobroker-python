"""Tests for the file store helpers.

The parts that talk to a database are exercised against a running installation. What is unit
tested here is the key and type handling — the places where a mistake produces an empty result or
a corrupted file instead of an error.
"""

from __future__ import annotations

import pytest

from iobroker.files import (
    FILE_SEPARATOR,
    FileMeta,
    file_key,
    guess_mime_type,
    normalize_name,
    split_file_key,
)

PREFIX = "cfg.f."


class TestNormalizeName:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("icons/lamp.png", "icons/lamp.png"),
            # Written on Windows, read on Linux -- the same file either way.
            ("icons\\lamp.png", "icons/lamp.png"),
            # "/a.png" and "a.png" mean the same file but would be two keys.
            ("/icons/lamp.png", "icons/lamp.png"),
            ("a.png", "a.png"),
        ],
    )
    def test_forms_that_mean_the_same_file(self, given: str, expected: str) -> None:
        assert normalize_name(given) == expected


class TestKeys:
    def test_builds_the_shape_the_database_uses(self) -> None:
        key = file_key(PREFIX, "myadapter.0", "icons/lamp.png", "data")

        assert key == f"cfg.f.myadapter.0{FILE_SEPARATOR}icons/lamp.png{FILE_SEPARATOR}data"

    def test_normalises_on_the_way_in(self) -> None:
        # Otherwise a file written with backslashes could never be read back with slashes.
        assert file_key(PREFIX, "a.0", "\\x\\y.txt", "meta") == file_key(PREFIX, "a.0", "x/y.txt", "meta")

    def test_round_trips(self) -> None:
        key = file_key(PREFIX, "myadapter.0", "icons/lamp.png", "meta")

        assert split_file_key(PREFIX, key) == ("myadapter.0", "icons/lamp.png", "meta")

    @pytest.mark.parametrize(
        "key",
        [
            "cfg.o.myadapter.0.state",  # an object, not a file
            "cfg.f.myadapter.0",  # no separator at all
            f"cfg.f.a.0{FILE_SEPARATOR}only-two-parts",
        ],
    )
    def test_rejects_what_is_not_a_file_key(self, key: str) -> None:
        assert split_file_key(PREFIX, key) is None


class TestMimeType:
    @pytest.mark.parametrize(
        "name,is_text,mime,binary",
        [
            ("lamp.png", False, "image/png", True),
            ("notes.txt", True, "text/plain", False),
            # An SVG is an image but is text, and the admin UI serves it as such.
            ("icon.svg", False, "image/svg+xml", False),
            ("data.json", True, "application/json", False),
            # No extension: the type of what was passed in decides.
            ("README", True, "text/plain", False),
            ("blob", False, "application/octet-stream", True),
            # Case in the extension must not matter.
            ("PHOTO.JPG", False, "image/jpeg", True),
        ],
    )
    def test_derives_type_and_binary_flag(self, name: str, is_text: bool, mime: str, binary: bool) -> None:
        assert guess_mime_type(name, is_text) == (mime, binary)


class TestFileMeta:
    def test_round_trips_through_the_wire_shape(self) -> None:
        meta = FileMeta(size=42, mime_type="image/png", binary=True, created_at=1, modified_at=2)
        back = FileMeta.from_wire(meta.to_wire())

        assert (back.size, back.mime_type, back.binary) == (42, "image/png", True)
        assert (back.created_at, back.modified_at) == (1, 2)

    def test_size_lives_under_stats(self) -> None:
        # The JavaScript client reads meta.stats.size; putting it at the top level would make a
        # file look zero-sized to the admin UI.
        assert FileMeta(size=7).to_wire()["stats"]["size"] == 7

    def test_supplies_an_acl_when_there_is_none(self) -> None:
        # Without one the admin UI cannot decide who may read the file.
        acl = FileMeta().to_wire()["acl"]

        assert acl["owner"] and acl["ownerGroup"] and acl["permissions"]
