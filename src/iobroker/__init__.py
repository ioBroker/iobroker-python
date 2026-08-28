"""iobroker -- Python-SDK fuer ioBroker-Adapter.

Redet direkt das Redis-Wire-Protokoll der States- und Objects-Datenbank und
macht Python-Prozesse damit zu gleichrangigen Adaptern neben Node.
"""

from .adapter import Adapter
from .connection import (
    DbConfig,
    PROTOCOL_VERSION,
    connect,
    connect_async,
    load_db_config,
)
from .types import Message, State, now_ms

#: Einzige Quelle der Versionsnummer -- pyproject.toml liest sie hier aus.
__version__ = "0.1.1"

__all__ = [
    "Adapter",
    "State",
    "Message",
    "DbConfig",
    "PROTOCOL_VERSION",
    "connect",
    "connect_async",
    "load_db_config",
    "now_ms",
    "__version__",
]
