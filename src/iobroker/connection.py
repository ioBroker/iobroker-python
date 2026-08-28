"""Redis-Wire-Anbindung an die ioBroker-Datenbanken.

Dieses Modul kapselt alles, was der eingebaute Redis-Server von js-controller
anders macht als echtes Redis. Jede Abweichung hier ist am laufenden System
nachgewiesen, nicht vermutet -- siehe ``tools/probe.py``.

Die vier Fallstricke:

1. **Kommandos muessen kleingeschrieben sein.** Der Server dispatcht in
   ``db-base/redisHandler.js`` ohne ``toLowerCase()``, registriert seine Handler
   aber ausschliesslich klein. ioredis sendet zufaellig klein, redis-py sendet
   gross. Am Draht nachgewiesen::

       GET meta.states.protocolVersion  ->  -Error GET NOT SUPPORTED
       get meta.states.protocolVersion  ->  $1  4

2. **Kein HELLO, kein CLIENT SETINFO.** redis-py verhandelt beim Verbinden
   RESP3 und meldet den Client-Namen. Beides kennt der Server nicht, der
   Verbindungsaufbau scheitert mit ``HELLO NOT SUPPORTED``.

3. **Kein PING auf der States-DB.** Der Verbindungstest laeuft ueber
   ``get meta.states.protocolVersion`` -- der Schluessel muss ohnehin geprueft
   werden.

4. **Kein SCAN auf der States-DB.** Nur die Objects-DB kann ``scan``/``sscan``.
   Fuer States bleibt ``keys`` -- gegen echtes Redis blockierend, gegen den
   eingebauten Server unkritisch.
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

#: Von diesem SDK unterstuetzte Protokollversion beider Datenbanken.
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
    "C:/pWork/iobroker-data/iobroker.json",
    "./iobroker-data/iobroker.json",
    "../iobroker-data/iobroker.json",
    "../../iobroker-data/iobroker.json",
)


# --------------------------------------------------------------------------
# Kommandonamen kleinschreiben
# --------------------------------------------------------------------------

def _lower_cmd(args: tuple) -> tuple:
    """Setzt den Kommandonamen -- args[0] -- auf Kleinschreibung.

    Mehrwortige Kommandos wie ``"CONFIG SET"`` gibt redis-py als einen String
    weiter und zerlegt sie erst beim Packen. Kleinschreibung vor dem Zerlegen
    trifft daher beide Formen.
    """
    if args and isinstance(args[0], (str, bytes)):
        head = args[0].decode() if isinstance(args[0], bytes) else args[0]
        return (head.lower(),) + tuple(args[1:])
    return tuple(args)


class _LowercasePacker:
    """Haengt sich vor den Command-Packer der synchronen redis-py-Verbindung."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def pack(self, *args: Any):
        return self._inner.pack(*_lower_cmd(args))

    def pack_commands(self, commands: Iterable[tuple]):
        return self._inner.pack_commands([_lower_cmd(c) for c in commands])

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class IoBrokerConnection(redis.connection.Connection):
    """Synchrone Verbindung.

    redis-py packt hier ueber ``self._command_packer``; ``pack_command`` allein
    zu ueberschreiben greift nicht, weil ``send_command`` daran vorbeigeht.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._command_packer = _LowercasePacker(self._command_packer)


class AsyncIoBrokerConnection(aioredis.connection.Connection):
    """Asynchrone Verbindung.

    Hier ist es umgekehrt: ``send_command`` ruft ``self.pack_command`` direkt
    auf, also ist genau das der richtige Haken.
    """

    def pack_command(self, *args: Any):
        return super().pack_command(*_lower_cmd(args))

    def pack_commands(self, commands: Iterable[tuple]):
        return super().pack_commands([_lower_cmd(c) for c in commands])


# --------------------------------------------------------------------------
# Konfiguration
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class DbConfig:
    """Verbindungsdaten einer der beiden Datenbanken."""

    host: str
    port: int
    db: int
    password: str | None
    kind: str  # "jsonl", "file" oder "redis"

    @property
    def is_builtin(self) -> bool:
        """True, wenn der eingebaute Server antwortet statt echtem Redis.

        Steuert, ob ``scan`` benutzt werden darf und wie Ablauf-Ereignisse
        ankommen.
        """
        return self.kind != "redis"


def find_config(explicit: str | None = None) -> str:
    """Sucht die ``iobroker.json``.

    Reihenfolge: Argument, Umgebungsvariable ``IOB_CONFIG``, uebliche Pfade.
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
        "iobroker.json nicht gefunden -- Pfad per Argument oder IOB_CONFIG angeben."
    )


def load_db_config(section: str, path: str | None = None) -> DbConfig:
    """Liest den ``states``- oder ``objects``-Abschnitt der ``iobroker.json``.

    Der py-controller reicht diese Werte spaeter ueber Umgebungsvariablen
    durch; dann wird die Datei gar nicht erst gelesen.
    """
    if section not in ("states", "objects"):
        raise ValueError(f"Unbekannter Abschnitt: {section}")

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
# Verbinden
# --------------------------------------------------------------------------

_POOL_KWARGS = dict(
    decode_responses=True,
    socket_timeout=10,
    socket_connect_timeout=10,
    # RESP2 erzwingen: der eingebaute Server kennt kein HELLO.
    protocol=2,
    # Unterdrueckt CLIENT SETINFO, das der Server ebenfalls nicht kennt.
    lib_name=None,
    lib_version=None,
)


def connect(cfg: DbConfig) -> redis.Redis:
    """Baut eine synchrone Verbindung."""
    pool = redis.ConnectionPool(
        connection_class=IoBrokerConnection,
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        password=cfg.password,
        **_POOL_KWARGS,
    )
    return redis.Redis(connection_pool=pool)


def connect_async(cfg: DbConfig) -> aioredis.Redis:
    """Baut eine asynchrone Verbindung."""
    pool = aioredis.ConnectionPool(
        connection_class=AsyncIoBrokerConnection,
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        password=cfg.password,
        **_POOL_KWARGS,
    )
    return aioredis.Redis(connection_pool=pool)


async def check_protocol(client: aioredis.Redis, section: str) -> str:
    """Prueft die Protokollversion und dient zugleich als Verbindungstest.

    Bricht bei Abweichung ab, statt auf gut Glueck weiterzumachen -- ein
    Protokollwechsel bedeutet, dass die Schluesselformate nicht mehr stimmen.
    """
    version = await client.get(f"{META_PREFIX}{section}.protocolVersion")
    if version is None:
        raise ConnectionError(
            f"{section}: keine Protokollversion gefunden -- laeuft ioBroker?"
        )
    if version != PROTOCOL_VERSION:
        raise ConnectionError(
            f"{section}: Protokollversion {version!r}, dieses SDK spricht "
            f"{PROTOCOL_VERSION!r}."
        )
    return version
