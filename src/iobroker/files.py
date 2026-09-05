"""The file store.

ioBroker keeps files in the objects database rather than on disk, which is what makes them survive
a backup and reach every host in a multihost setup. Each file occupies two keys::

    cfg.f.<id>$%$<name>$%$data    the bytes, stored raw
    cfg.f.<id>$%$<name>$%$meta    JSON describing them

``<id>`` is usually the adapter namespace, ``<name>`` a path inside it such as ``icons/lamp.png``.

The one thing that has to be right here is that data is **bytes**. The rest of the SDK runs its
connections with ``decode_responses=True`` because objects and states are JSON, but decoding a PNG
as UTF-8 destroys it. Files therefore use a separate connection in byte mode, opened only when
files are actually used.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Any

__all__ = ["FileMeta", "FILE_SEPARATOR", "guess_mime_type", "file_key", "split_file_key"]

#: Separator ioBroker puts between id, file name and part. Not a character that occurs in paths.
FILE_SEPARATOR = "$%$"

#: Extensions treated as text, everything else counts as binary.
_TEXT_TYPES = {
    ".css": "text/css",
    ".csv": "text/csv",
    ".htm": "text/html",
    ".html": "text/html",
    ".js": "application/javascript",
    ".json": "application/json",
    ".md": "text/markdown",
    ".svg": "image/svg+xml",
    ".txt": "text/plain",
    ".xml": "text/xml",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
}

_BINARY_TYPES = {
    ".bmp": "image/bmp",
    ".gif": "image/gif",
    ".ico": "image/x-icon",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".mp3": "audio/mpeg",
    ".mp4": "video/mp4",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".wav": "audio/wav",
    ".webp": "image/webp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".zip": "application/zip",
}


@dataclass
class FileMeta:
    """What the object database records about a stored file.

    The ``$%$meta`` half of a file. It is what :meth:`~iobroker.Adapter.read_file_meta` returns,
    and reading it is cheap where reading the file itself is not -- which makes it the right way to
    ask how big a file is or whether it has moved.

    The field names are snake_case here and camelCase on the wire; :meth:`to_wire` and
    :meth:`from_wire` are where the two meet.
    """

    #: Length of the content in bytes. Stored nested as ``stats.size``, which is where the JS
    #: client puts it.
    size: int = 0
    #: The media type admin serves the file with. Derived from the extension when it was written.
    mime_type: str = "application/octet-stream"
    #: Whether the content is binary. Follows from the media type and decides whether anything
    #: reading the file may treat it as text.
    binary: bool = True
    #: When the file was first written, in milliseconds. Preserved across overwrites: a rewritten
    #: file is the same file.
    created_at: int | None = None
    #: When it was last written, in milliseconds.
    modified_at: int | None = None
    #: Owner, group and permissions. ``None`` means the default in :meth:`to_wire` applies.
    acl: dict[str, Any] | None = None

    def to_wire(self) -> dict[str, Any]:
        """Serialize into the shape the JavaScript client writes."""
        out: dict[str, Any] = {
            "stats": {"size": self.size},
            "mimeType": self.mime_type,
            "binary": self.binary,
            "acl": self.acl
            or {
                "owner": "system.user.admin",
                "ownerGroup": "system.group.administrator",
                # 0x644 is what the JavaScript client writes: owner read/write, everyone read.
                "permissions": 0x644,
            },
        }

        if self.created_at is not None:
            out["createdAt"] = self.created_at
        if self.modified_at is not None:
            out["modifiedAt"] = self.modified_at

        return out

    @classmethod
    def from_wire(cls, raw: dict[str, Any]) -> "FileMeta":
        """Read the metadata back, tolerating what older writers left out.

        Every field has a fallback, because these records were written by many versions over many
        years and a file whose meta is missing ``binary`` still has to be listable.

        :param raw: the decoded JSON from the ``$%$meta`` key
        """
        return cls(
            size=int((raw.get("stats") or {}).get("size") or 0),
            mime_type=raw.get("mimeType") or "application/octet-stream",
            binary=bool(raw.get("binary", True)),
            created_at=raw.get("createdAt"),
            modified_at=raw.get("modifiedAt"),
            acl=raw.get("acl"),
        )


def guess_mime_type(name: str, is_text: bool) -> tuple[str, bool]:
    """Derive the media type and whether the content counts as binary.

    Follows the JavaScript client: a known extension decides, and without one the type of the data
    passed in does. Getting this wrong matters because the admin UI serves files with this header.

    :param name: file name, used for its extension
    :param is_text: whether the caller passed a string rather than bytes
    :returns: the media type and the binary flag
    """
    _, ext = posixpath.splitext(name.lower())

    if ext in _TEXT_TYPES:
        return _TEXT_TYPES[ext], False
    if ext in _BINARY_TYPES:
        return _BINARY_TYPES[ext], True

    return ("text/plain", False) if is_text else ("application/octet-stream", True)


def file_key(prefix: str, id: str, name: str, part: str) -> str:
    """Build the database key of one part of a file.

    :param prefix: the file namespace, normally ``cfg.f.``
    :param id: owning id, usually the adapter namespace
    :param name: path of the file within that id
    :param part: ``data`` or ``meta``
    """
    return f"{prefix}{id}{FILE_SEPARATOR}{normalize_name(name)}{FILE_SEPARATOR}{part}"


def split_file_key(prefix: str, key: str) -> tuple[str, str, str] | None:
    """Take a database key apart again.

    :param prefix: the file namespace the key should carry
    :param key: the key as it came from the database
    :returns: id, file name and part, or ``None`` if the key is not a file key
    """
    if not key.startswith(prefix):
        return None

    parts = key[len(prefix) :].split(FILE_SEPARATOR)

    if len(parts) != 3:
        return None

    return parts[0], parts[1], parts[2]


def normalize_name(name: str) -> str:
    """Bring a file name into the form the database uses.

    Backslashes become slashes and a leading slash is dropped, because ``/icons/a.png`` and
    ``icons/a.png`` are the same file but would otherwise be two different keys.

    :param name: the name as the caller wrote it
    """
    return name.replace("\\", "/").lstrip("/")
