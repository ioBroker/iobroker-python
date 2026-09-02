"""Redis wire connection to the ioBroker databases.

This module encapsulates everything the Redis server built into js-controller
does differently from real Redis. Every deviation documented here was measured
against a running installation, not assumed -- see ``tools/probe.py``.

The four pitfalls:

1. **Commands must be lowercase.** The server dispatches in
   ``db-base/redisHandler.js`` without ``toLowerCase()``, yet registers its
   handlers in lowercase only. ioredis happens to send lowercase, redis-py
   sends uppercase. Measured on the wire::

       GET meta.states.protocolVersion  ->  -Error GET NOT SUPPORTED
       get meta.states.protocolVersion  ->  $1  4

2. **No HELLO, no CLIENT SETINFO.** redis-py negotiates RESP3 on connect and
   reports its client name. The server knows neither, so the connection fails
   with ``HELLO NOT SUPPORTED``.

3. **No PING on the states database.** The connection test goes through
   ``get meta.states.protocolVersion`` -- that key has to be checked anyway.

4. **No SCAN on the states database.** Only the objects database supports
   ``scan``/``sscan``. States are left with ``keys``, which blocks against real
   Redis but is harmless against the built-in server.
"""

from __future__ import annotations

import json
import os
import warnings
from dataclasses import dataclass
from typing import Any, Iterable

import redis
import redis.asyncio as aioredis

warnings.filterwarnings("ignore", category=DeprecationWarning, module="redis")

__all__ = [
    "DbConfig",
    "PROTOCOL_VERSION",
    "STATES_PREFIX",
    "OBJECTS_PREFIX",
    "FILES_PREFIX",
    "SETS_PREFIX",
    "MESSAGE_PREFIX",
    "LOG_PREFIX",
    "META_PREFIX",
    "find_config",
    "load_db_config",
    "connect",
    "connect_async",
    "check_protocol",
]

#: Protocol version of both databases supported by this SDK.
PROTOCOL_VERSION = "4"

STATES_PREFIX = "io."
OBJECTS_PREFIX = "cfg.o."
FILES_PREFIX = "cfg.f."
SETS_PREFIX = "cfg.s."
MESSAGE_PREFIX = "messagebox."
LOG_PREFIX = "log."
META_PREFIX = "meta."

_CONFIG_CANDIDATES = (
    "/opt/iobroker/iobroker-data/iobroker.json",
    "C:/ioBroker/iobroker-data/iobroker.json",
    "./iobroker-data/iobroker.json",
    "../iobroker-data/iobroker.json",
    "../../iobroker-data/iobroker.json",
)


# --------------------------------------------------------------------------
# Lowercasing command names
# --------------------------------------------------------------------------

def _lower_cmd(args: tuple) -> tuple:
    """Lowercase the command name -- args[0].

    redis-py passes multi-word commands such as ``"CONFIG SET"`` as a single
    string and splits them only while packing. Lowercasing before the split
    therefore covers both forms.
    """
    if args and isinstance(args[0], (str, bytes)):
        head = args[0].decode() if isinstance(args[0], bytes) else args[0]
        return (head.lower(),) + tuple(args[1:])
    return tuple(args)


class _LowercasePacker:
    """Wraps the command packer of the synchronous redis-py connection.

    ``pack`` is the only hook needed: pipelines go through the connection's
    ``pack_commands``, which feeds every command through ``pack`` -- the packer
    itself has no ``pack_commands``.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def pack(self, *args: Any):
        return self._inner.pack(*_lower_cmd(args))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class IoBrokerConnection(redis.connection.Connection):
    """Synchronous connection.

    Here redis-py packs through ``self._command_packer``; overriding
    ``pack_command`` alone has no effect because ``send_command`` bypasses it.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._command_packer = _LowercasePacker(self._command_packer)


class AsyncIoBrokerConnection(aioredis.connection.Connection):
    """Asynchronous connection.

    The other way round here: ``send_command`` calls ``self.pack_command``
    directly, so that is the correct hook.
    """

    def pack_command(self, *args: Any):
        return super().pack_command(*_lower_cmd(args))

    def pack_commands(self, commands: Iterable[tuple]):
        return super().pack_commands([_lower_cmd(c) for c in commands])


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class DbConfig:
    """Connection settings for one of the two databases."""

    host: str
    port: int
    db: int
    password: str | None
    kind: str  # "jsonl", "file" or "redis"

    @property
    def is_builtin(self) -> bool:
        """True when the built-in server answers instead of real Redis.

        Determines whether ``scan`` may be used and how expiry events arrive.
        """
        return self.kind != "redis"


def find_config(explicit: str | None = None) -> str:
    """Locate ``iobroker.json``.

    Order: argument, ``IOB_CONFIG`` environment variable, common paths.
    """
    if explicit:
        if not os.path.isfile(explicit):
            raise FileNotFoundError(explicit)
        return explicit
    env = os.environ.get("IOB_CONFIG")
    if env and os.path.isfile(env):
        return env
    for candidate in _CONFIG_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(
        "iobroker.json not found -- pass a path as argument or set IOB_CONFIG."
    )


def load_db_config(section: str, path: str | None = None) -> DbConfig:
    """Read the ``states`` or ``objects`` section of ``iobroker.json``.

    The py-controller will later pass these values through environment
    variables; the file is then not read at all.
    """
    if section not in ("states", "objects"):
        raise ValueError(f"Unknown section: {section}")

    prefix = f"IOB_{section.upper()}_"
    if os.environ.get(prefix + "PORT"):
        return DbConfig(
            host=os.environ.get(prefix + "HOST", "127.0.0.1"),
            port=int(os.environ[prefix + "PORT"]),
            db=int(os.environ.get(prefix + "DB", "0")),
            password=os.environ.get(prefix + "PASS") or None,
            kind=os.environ.get(prefix + "TYPE", "jsonl"),
        )

    with open(find_config(path), encoding="utf-8") as handle:
        cfg = json.load(handle)
    part = cfg[section]
    opts = part.get("options") or {}
    return DbConfig(
        host=part.get("host", "127.0.0.1"),
        port=int(part.get("port", 9000 if section == "states" else 9001)),
        db=int(opts.get("db") or 0),
        password=opts.get("auth_pass") or None,
        kind=part.get("type", "jsonl"),
    )


# --------------------------------------------------------------------------
# Connecting
# --------------------------------------------------------------------------

_POOL_KWARGS = dict(
    decode_responses=True,
    socket_timeout=10,
    socket_connect_timeout=10,
    # Force RESP2: the built-in server does not know HELLO.
    protocol=2,
    # Suppresses CLIENT SETINFO, which the server does not know either.
    lib_name=None,
    lib_version=None,
)


def connect(cfg: DbConfig) -> redis.Redis:
    """Open a synchronous connection."""
    pool = redis.ConnectionPool(
        connection_class=IoBrokerConnection,
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        password=cfg.password,
        **_POOL_KWARGS,
    )
    return redis.Redis(connection_pool=pool)


def connect_async(cfg: DbConfig, decode: bool = True) -> aioredis.Redis:
    """Open an asynchronous connection.

    :param cfg: which database to connect to
    :param decode: whether replies are decoded as text. Objects and states are JSON and want that;
        file content does not -- decoding a PNG as UTF-8 destroys it, so the file store opens a
        second connection with this off.
    """
    kwargs = dict(_POOL_KWARGS)
    kwargs["decode_responses"] = decode

    pool = aioredis.ConnectionPool(
        connection_class=AsyncIoBrokerConnection,
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        password=cfg.password,
        **kwargs,
    )
    return aioredis.Redis(connection_pool=pool)


async def check_protocol(client: aioredis.Redis, section: str) -> str:
    """Verify the protocol version; doubles as the connection test.

    Aborts on mismatch rather than pressing on: a protocol change means the key
    formats can no longer be relied upon.
    """
    version = await client.get(f"{META_PREFIX}{section}.protocolVersion")
    if version is None:
        raise ConnectionError(
            f"{section}: no protocol version found -- is ioBroker running?"
        )
    if version != PROTOCOL_VERSION:
        raise ConnectionError(
            f"{section}: protocol version {version!r}, this SDK speaks "
            f"{PROTOCOL_VERSION!r}."
        )
    return version
