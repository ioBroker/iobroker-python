"""iobroker -- Python SDK for ioBroker adapters.

Speaks the Redis wire protocol of the states and objects databases directly,
making Python processes first-class adapters alongside Node.
"""

from .adapter import Adapter
from .connection import (
    DbConfig,
    PROTOCOL_VERSION,
    connect,
    connect_async,
    load_db_config,
)
from .crypto import decrypt
from .types import Message, State, now_ms

#: Single source of the version number -- pyproject.toml reads it from here.
__version__ = "0.2.0"

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
    "decrypt",
    "__version__",
]
