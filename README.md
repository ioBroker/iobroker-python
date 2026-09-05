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

The same applies to `common.protectedNative`: those entries are withheld by the
client, so reading a foreign instance object straight from the database would
hand over exactly what the flag exists to keep back. `get_foreign_object` and
`get_object_view` therefore strip them, following the rule
`@iobroker/adapter-core` uses — an adapter still sees its own settings, and
`admin`, `iot`, `cloud` and `discovery` stay exempt.

The other side of that coin is **stamping new objects with the default ACL**.
`system.config.common.defaultNewAcl` defines owner, group and permission bits
for freshly created objects; the JS objects client applies it, so an object
created without an `acl` still carries one. The SDK does the same: a new object
with no `acl` of its own inherits the default (kept current through a
subscription on `system.config`, the way the JS client tracks it), while an
`acl` you supply — or one already on the object being overwritten — is left
untouched. Installations that never configured a default get no invented `acl`,
matching the JS client on such installs.

`lc` ("last change") is computed the same way, and for the same reason — the
database stores a blob, the client decides what goes in it. `set_state` moves
`lc` only when the value actually changed and otherwise carries the previous one
forward, so a sensor polled every 30 seconds with a steady reading does not look
like it changes every 30 seconds. `True` and `1` count as different values, as
they do for the JS client's `isDeepStrictEqual`. An `lc` you pass yourself always
wins. Like the JS client, this costs one read before the write.

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

## Objects

Beyond `get_object` / `set_object` / `set_object_not_exists`:

| | |
|---|---|
| `extend_object(id, patch)` | merges into an existing object; `common` and `native` key by key, so one field can be changed without overwriting what a user edited |
| `delete_object(id)` | removes it and keeps the type index in step |
| `get_adapter_objects()` | every object in the adapter's own namespace |
| `get_object_view("system", type)` | objects of one type within an id range |
| `subscribe_objects(pattern)` | notices configuration changes made while the adapter runs, delivered to `on_object_change` |
| `unsubscribe_objects(pattern)` | takes that back; also `unsubscribe_states`, and the `_foreign_` form of both |

`get_object_view` reads the type index sets rather than running Lua the way the JavaScript client
does — the states database cannot run Lua at all and the objects database only sometimes can. Where
the sets are switched off it falls back to scanning.

## Subscriptions

`subscribe_states(pattern)` and `subscribe_foreign_states(pattern)` deliver changes to
`on_state_change`; the object pair does the same for `on_object_change`. Each has an
`unsubscribe_` counterpart.

A pattern is removed by its exact text, the way Redis itself works: unsubscribing `hue.0.*` leaves
`hue.0.lamp.level` subscribed. Unsubscribing something that was never subscribed is not an error --
a script engine tearing a script down should not have to know whether a neighbour still holds the
same pattern.

Two things happen on every unsubscribe, and both matter. The server is told, which stops the
traffic now, and the pattern is dropped from the set this SDK keeps for reconnects. A pattern left
in that set would come back the next time the database blinks.

The adapter's own patterns -- its messagebox, the controller's `sigKill`, `system.config` -- are
refused with a warning. An adapter that unsubscribed its `sigKill` would simply stop responding to
`iobroker stop`, and nothing about that symptom points at the call that caused it.

## Files

ioBroker keeps files in the objects database rather than on disk, which is what makes them survive
a backup and reach every host in a multihost setup.

```python
await self.write_file(self.namespace, "icons/lamp.png", png_bytes)
data = await self.read_file(self.namespace, "icons/lamp.png")   # always bytes
await self.read_dir(self.namespace, "icons")                    # [{'file': 'lamp.png', 'is_dir': False}]
await self.unlink(self.namespace, "icons/lamp.png")
```

`read_file` returns bytes, never text: the caller knows whether it stored an image or a JSON
document, this layer does not, and guessing would corrupt one of the two. For the same reason files
use their own connection with decoding switched off — the connections carrying objects and states
decode replies as text, which would destroy a PNG.

Two things about the built-in server are worth knowing, both measured rather than assumed:

- **File keys are not globbed.** `keys("cfg.f.<id>$%$*")` returns one directory level, and
  `keys("cfg.f.<id>$%$icons")` returns nothing at all — the working form for a subdirectory is
  `icons/*`. Getting it wrong yields an empty list, not an error.
- **Subdirectories appear as a synthetic `<dir>/_data.json` entry**, which is how `read_dir` tells
  them apart from files.

## Lifecycle

`alive`, `connected`, `uptime` and `memRss` are written by the adapter itself
through the states database — exactly like a Node adapter. Stopping goes through
the `sigKill` state: when the controller sets it to `-1`, the adapter shuts down
in an orderly fashion. That makes stopping work on Windows too, where there is
no `SIGTERM`. Any other value is the PID the controller believes it supervises:
when that is not the adapter's own PID, another supervisor has taken over and
the stale process shuts down as well — the behaviour of `adapter-core`.

A database that goes away does **not** end the adapter: both pumps notice the
dead connection, back off (1 s, doubling to at most 30 s) and reopen their
subscriptions with the patterns registered before the outage, so events resume
by themselves once the database is back. `test_reconnect.py` proves it by
running the adapter through a TCP relay it cuts and mends again.

The process ends with the exit code the controller keys its restart behaviour
off (the `ExitCode` enum, mirroring `@iobroker/js-controller-common-db`).
`run()` returns `0` on a clean stop, exits `6` (`UNCAUGHT_EXCEPTION`) on an
exception the adapter never handled — restarted and counted towards restart-loop
detection — and exits with whatever `terminate()` was given. Call
`adapter.terminate("done")` for a planned stop the controller does **not**
restart (`11`); a `once` or `schedule` adapter uses it when its work is finished.

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Windows
pip install -e ".[dev]"
python examples/minimal_adapter.py --instance 0
```

The version number lives in `src/iobroker/__init__.py` only; `pyproject.toml`
reads it from there through hatch, and the release workflow checks the git tag
against it.

### Tests

```bash
pytest
```

The unit tests run everywhere. The database tests run against **two backends**
and are parametrized over both, because both are what a real installation runs:

- **`redis`** — a real Redis, as a large installation runs. Defaults to
  `127.0.0.1:6379`, database `15`; override with `IOB_TEST_REDIS_HOST` /
  `IOB_TEST_REDIS_PORT` / `IOB_TEST_REDIS_DB`. That database is flushed between
  tests, so it must be dedicated to the suite; if it contains anything the suite
  did not write itself, the database tests refuse to run rather than risk an
  installation. Data in other databases of the same Redis is never touched, but
  since Redis pub/sub is server-wide a live ioBroker on the same server may
  briefly see events for the `pytest*` namespaces while the suite runs.
- **`builtin`** — the databases built into js-controller (the jsonl flavour a
  default installation uses), started as a private Node.js process on fresh
  ports with a temp data dir. Needs Node.js and a one-time install:

  ```bash
  cd tests/builtin && npm ci
  ```

A backend that is not available skips its half of the tests. CI sets
`IOB_TEST_REQUIRE_REDIS=1` and `IOB_TEST_REQUIRE_BUILTIN=1`, which turn those
skips into failures so a broken setup cannot pass as green. A handful of tests
are gated to one backend — where an assertion needs a Redis command the built-in
servers do not implement (`ttl`, `smembers`), or where the two deliberately
differ (real Redis globs the whole file subtree, the built-in server answers one
directory level).

`test_interop.py` goes one step further and proves **cross-language
conformance**: a state or object written by the real js-controller client
(`@iobroker/db-states-redis` / `@iobroker/db-objects-redis`, driven through
`tests/builtin/interop.mjs`) is read field-for-field identically by the SDK, and
the reverse. The two-backend tests show the SDK works against the real database
*servers*; this shows the wire *envelope* the SDK writes and parses is the one a
Node.js adapter writes and parses — so a Python and a Node.js adapter sharing a
state actually agree on it. It runs on the built-in backend only (the envelope
comes from the JS client and is the same whatever server stores it).

## License

MIT
