// Boots the databases built into js-controller -- the jsonl flavour, which is
// what a default installation runs -- so the Python test suite can talk to
// them over the wire exactly like a Python adapter on such an installation.
//
// Usage:  node server.mjs --states-port=19000 --objects-port=19001 --data-dir=/tmp/x
//
// Prints READY on stdout once both servers listen. Exits when stdin closes,
// so a crashing test runner can never leave a server behind.

import fs from 'node:fs';
import { Server as ObjectsServer } from '@iobroker/db-objects-jsonl';
import { Server as StatesServer } from '@iobroker/db-states-jsonl';

const args = Object.fromEntries(
    process.argv.slice(2).map(arg => {
        const [key, value] = arg.split('=');
        return [key, value];
    }),
);

const statesPort = Number(args['--states-port']);
const objectsPort = Number(args['--objects-port']);
const dataDir = args['--data-dir'];

if (!statesPort || !objectsPort || !dataDir) {
    console.error('Usage: node server.mjs --states-port=<n> --objects-port=<n> --data-dir=<dir>');
    process.exit(2);
}

// The suite's own output matters, the servers' does not -- but their errors do.
const logger = {
    silly() {},
    debug() {},
    info() {},
    warn: msg => console.error(msg),
    error: msg => console.error(msg),
};

function start(Cls, port, dir) {
    fs.mkdirSync(dir, { recursive: true });
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`server on port ${port} did not start`)), 15000);
        const server = new Cls({
            connection: {
                type: 'jsonl',
                host: '127.0.0.1',
                port,
                dataDir: dir,
                options: { auth_pass: null, retry_max_delay: 15000 },
            },
            logger,
            connected: () => {
                clearTimeout(timer);
                resolve(server);
            },
            change: () => {},
        });
    });
}

const objects = await start(ObjectsServer, objectsPort, `${dataDir}/objects`);
const states = await start(StatesServer, statesPort, `${dataDir}/states`);

console.log('READY');

async function shutdown() {
    try {
        await objects.destroy();
        await states.destroy();
    } catch {
        // nothing left to save -- the data dir is a temp dir
    }
    process.exit(0);
}

process.stdin.resume();
process.stdin.on('end', shutdown);
process.stdin.on('close', shutdown);
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
