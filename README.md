# iobroker-python

Python SDK for ioBroker adapters. Speaks the Redis wire protocol of the states
and objects databases directly — a Python process becomes a first-class adapter
alongside any Node adapter, with no bridge in between.

> **Status: early draft.** The wire layer is verified against a running
> installation (js-controller 7.2.3, `jsonl` databases). The API may still
> change.

## Installation

```bash
pip install iobroker
```

## An adapter in thirty lines

```python
from iobroker import Adapter, State

class MyAdapter(Adapter):
    async def on_ready(self):
        await self.set_object_not_exists("temperature", {
            "type": "state",
            "common": {
                "name": "Temperature", "type": "number",
                "role": "value.temperature", "unit": "°C",
                "read": True, "write": False,
            },
        })
        await self.subscribe_states("*")
        await self.set_state("info.connection", True, ack=True)

    async def on_state_change(self, id: str, state: State | None):
        # ack=False means somebody wants something switched.
        if state and not state.ack:
            self.log.info(f"Command on {id}: {state.val}")

    async def on_message(self, msg):
        if msg.command == "ping":
            await self.reply(msg, {"pong": True})

MyAdapter("myadapter").run()
```

A runnable example lives in
[`examples/minimal_adapter.py`](https://github.com/ioBroker/iobroker-python/blob/main/examples/minimal_adapter.py).

## Connection settings

The adapter resolves them in this order:

1. Environment variables `IOB_STATES_HOST/PORT/DB/PASS/TYPE` and `IOB_OBJECTS_*`
   — this is how `py-controller` will pass them in later.
2. `IOB_CONFIG` holding the path to `iobroker.json`.
3. The usual installation paths.

Instance number and log level come from `--instance` / `--loglevel` or from
`IOB_INSTANCE` / `IOB_LOGLEVEL` — the same arguments js-controller already
passes to Node adapters today.

## How the built-in server differs from Redis

In a default setup you are not talking to real Redis but to the Redis protocol
server built into js-controller (ports 9000 and 9001). It deviates in several
places. Every point below was measured on the wire against a running
installation rather than inferred from documentation — `tools/probe.py` verifies
them for your own installation.

| Deviation | Consequence | How the SDK handles it |
|---|---|---|
| **Commands must be lowercase.** The server dispatches without `toLowerCase()` (`db-base/redisHandler.js`) but registers its handlers in lowercase only. ioredis happens to send lowercase, redis-py sends uppercase. | `GET …` → `-Error GET NOT SUPPORTED`, `get …` → `4`. Without handling, the very first command fails. | `connection.py` wraps redis-py's command packer. Synchronously via `_command_packer`, asynchronously via `pack_command` — redis-py takes a different route in each mode. |
| **No `HELLO`.** redis-py negotiates RESP3 on connect. | The connection fails with `HELLO NOT SUPPORTED`. | `protocol=2`, plus `lib_name=None` against `CLIENT SETINFO`. |
| **No `PING`** on the states database. | Common connection checks fail. | Connection test via `get meta.states.protocolVersion` — the version has to be checked anyway. |
| **No `SCAN`** on the states database. The objects database does support `scan`, `sscan`, `sadd`, `eval`. | Keys have to be found with `keys`. | `DbConfig.is_builtin` tells the two apart; against real Redis `keys` blocks and must be avoided. |
| **Pub/sub delivers the channel without the `io.` prefix.** Real Redis delivers it with. | Blindly stripping the prefix mangles ids. | The SDK tolerates both — exactly like the JS client. |
| **Expired states report differently.** No `__keyevent@0__:expired`; instead `null` arrives on the state channel itself. | Against real Redis an extra subscription would be needed. | `null` is reported to `on_state_change` as "state is gone". |

One more property that is not a bug but matters: **permission checks live in the
JS client, not in the database server.** Anything holding a Redis connection
effectively has admin rights. That is equally true for Node adapters — the
difference is that with Python, third-party code from PyPI shares the process.

## Capability probe

```bash
python tools/probe.py
```

(from the [repository](https://github.com/ioBroker/iobroker-python), not shipped
in the wheel)

Reports what both databases support in your installation, then performs a full
round trip: create an object, write a state, receive the change. Cleans up again
with `--cleanup`.

## Encrypted settings

ioBroker stores password fields in an instance's `native` section encrypted with the system secret.
Everything an adapter lists in `common.encryptedNative` arrives already decrypted in `self.config`,
so a password is read the same way as a hostname:

```python
async def on_ready(self):
    session = login(self.config["host"], self.config["password"])
```

Both storage formats are supported — AES-192-CBC as produced by current installations, and the
older XOR form still found in installations set up years ago. Which one applies is decided exactly
the way js-controller decides it.

AES needs the `cryptography` package, which is an optional dependency:

```toml
dependencies = ["iobroker[crypto]>=0.2.0"]
```

It is optional because it is compiled, and on a Raspberry Pi that is the difference between a fast
install and a long build — an adapter without encrypted settings should not pay for it. If a value
turns out to need it and it is missing, the error says so.

## Lifecycle

`alive`, `connected`, `uptime` and `memRss` are written by the adapter itself
through the states database — exactly like a Node adapter. Stopping goes through
the `sigKill` state: when the controller sets it to `-1`, the adapter shuts down
in an orderly fashion. That makes stopping work on Windows too, where there is
no `SIGTERM`.

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Windows
pip install -e ".[dev]"
python examples/minimal_adapter.py --instance 0
```

The version number lives in `src/iobroker/__init__.py` only; `pyproject.toml`
reads it from there through hatch, and the release workflow checks the git tag
against it.

## License

MIT
