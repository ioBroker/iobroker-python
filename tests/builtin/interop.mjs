// A bridge to the REAL js-controller database clients, so the Python suite can prove its wire
// envelope is the one a JavaScript adapter produces and consumes -- not merely one the database
// servers happen to accept.
//
// It drives @iobroker/db-states-redis and @iobroker/db-objects-redis (the very clients a Node.js
// adapter runs) against an already-running built-in server, does exactly one operation, prints its
// result as a single `RESULT <json>` line on stdout, and exits. Everything else (client/server
// chatter) goes to stderr, so the RESULT line is the only thing stdout carries.
//
// Usage:
//   node interop.mjs --states-port=N --objects-port=N [--host=127.0.0.1] <cmd> <id> [jsonArg]
//   cmd: set-state | get-state | set-object | get-object

import { Client as StatesClient } from '@iobroker/db-states-redis';
import { Client as ObjectsClient } from '@iobroker/db-objects-redis';

const args = {};
const positional = [];
for (const a of process.argv.slice(2)) {
    if (a.startsWith('--')) {
        const [k, v] = a.split('=');
        args[k.slice(2)] = v;
    } else {
        positional.push(a);
    }
}

const [cmd, id, jsonArg] = positional;
const host = args.host || '127.0.0.1';
const statesPort = Number(args['states-port']);
const objectsPort = Number(args['objects-port']);

// Client and server noise belongs on stderr; stdout is reserved for the one RESULT line.
const logger = {
    silly() {},
    debug() {},
    info() {},
    warn: msg => console.error(msg),
    error: msg => console.error(msg),
};

function connect(Cls, port) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`client did not connect on ${port}`)), 15000);
        const client = new Cls({
            namespace: 'jsside.0',
            hostname: 'jsside',
            connection: { type: 'redis', host, port, options: { auth_pass: null } },
            logger,
            connected: () => {
                clearTimeout(timer);
                resolve(client);
            },
            change: () => {},
        });
    });
}

let client;
let result;
try {
    if (cmd === 'set-state') {
        client = await connect(StatesClient, statesPort);
        const returnedId = await client.setState(id, JSON.parse(jsonArg));
        result = { id: returnedId };
    } else if (cmd === 'get-state') {
        client = await connect(StatesClient, statesPort);
        result = { state: await client.getState(id) };
    } else if (cmd === 'set-object') {
        client = await connect(ObjectsClient, objectsPort);
        await client.setObjectAsync(id, JSON.parse(jsonArg));
        result = { ok: true };
    } else if (cmd === 'get-object') {
        client = await connect(ObjectsClient, objectsPort);
        result = { object: await client.getObjectAsync(id) };
    } else {
        throw new Error(`unknown command: ${cmd}`);
    }

    console.log(`RESULT ${JSON.stringify(result)}`);
} catch (err) {
    console.error(`interop.mjs failed: ${(err && err.stack) || err}`);
    process.exitCode = 1;
} finally {
    if (client && typeof client.destroy === 'function') {
        try {
            await client.destroy();
        } catch {
            // the process is exiting anyway
        }
    }
    // The redis client keeps a reconnect timer alive; make sure the process actually ends.
    setTimeout(() => process.exit(process.exitCode || 0), 200);
}
