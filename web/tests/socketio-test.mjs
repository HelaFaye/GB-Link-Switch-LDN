// Tests for the Celio socket's requests on slow lines; no hardware, no server, no
// dependencies: node web/tests/socketio-test.mjs
//
// A stand-in WebSocket delays every message by a set time each way, and a stand-in server
// acknowledges every request. CelioLink's own data pipeline (64 packets in flight, each
// sent again until acknowledged) runs over it. A fixed one-second wait for each
// acknowledgement stalled the link for good on a line slower than that: each copy timed
// out before its acknowledgement came back, and the late acknowledgement was dropped.

let failures = 0;
function check(ok, what) {
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}`);
    if (!ok) failures++;
}

// A line: oneWay ms each way, and a server that answers each request (or does not).
const line = { oneWay: 0, answer: true, copies: 0, got: new Set(), socket: null };
class FakeWebSocket {
    constructor() {
        line.socket = this;
        setTimeout(() => this.onmessage?.({ data: '0{"sid":"test"}' }), 1);
    }
    send(text) {
        setTimeout(() => {
            if (text.startsWith('40')) return this.back('40{"sid":"test"}');
            const m = /^42(\d+)(\[.*)$/.exec(text);
            if (!m) return;
            const [event, arg] = JSON.parse(m[2]);
            line.copies++;
            if (event === 'deviceData') for (const packet of arg) line.got.add(packet.sequence);
            if (line.answer) this.back(`43${m[1]}[true]`);
        }, line.oneWay);
    }
    back(text) { setTimeout(() => this.onmessage?.({ data: text }), line.oneWay); }
    close() { setTimeout(() => this.onclose?.(), 0); }
}
globalThis.WebSocket = FakeWebSocket;

const { SocketIo } = await import('../js/cable/socketio.js');
const { CelioLink } = await import('../js/cable/celio.js');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function connected(oneWay) {
    Object.assign(line, { oneWay, answer: true, copies: 0, got: new Set() });
    const socket = new SocketIo('ws://test', {});
    await socket.connect(5 * oneWay + 1000);
    return socket;
}

// ---- CelioLink's data, n packets, over a line of oneWay ms each way
async function pipeline(oneWay, n, limitMs) {
    const socket = await connected(oneWay);
    const link = new CelioLink(socket, { onRole: () => ({}) });
    link.running = true;
    let acknowledged = 0;
    const request = socket.requestWithRetry.bind(socket);
    socket.requestWithRetry = (...args) => request(...args).then((value) => { acknowledged++; return value; });
    const start = Date.now();
    for (let sequence = 0; sequence < n; sequence++) link.sendData({ sequence, data: [] });
    while (acknowledged < n && Date.now() - start < limitMs) await sleep(50);
    link.running = false;
    socket.close();
    return { acknowledged, got: line.got.size, copies: line.copies, ms: Date.now() - start };
}

{
    const r = await pipeline(40, 150, 5000);
    check(r.acknowledged === 150 && r.copies === 150,
        `80 ms round trip: all 150 packets acknowledged, each sent once (${r.copies} copies, ${r.ms} ms)`);
}
for (const oneWay of [600, 1000]) {
    const r = await pipeline(oneWay, 150, 20000);
    check(r.acknowledged === 150 && r.got === 150,
        `${2 * oneWay} ms round trip: all 150 packets acknowledged (${r.acknowledged}), the server has ${r.got}` +
        ` (${r.copies} copies, ${r.ms} ms)`);
    check(r.copies <= 150 + 64 * 2, `... the copies stay bounded: at most two more per packet of the first window (${r.copies})`);
}

// ---- one request: a late acknowledgement counts
{
    const socket = await connected(700);
    const start = Date.now();
    const result = await socket.requestWithRetry('deviceStatus', { uuid: 'a', linkStatus: 0xff02 }).catch((e) => e);
    check(Array.isArray(result) && result[0] === true,
        `a 1.4 s round trip: the request is acknowledged (${Date.now() - start} ms, ${line.copies} copies)`);
    check(line.copies <= 2, `... by the first copy's late acknowledgement, not after five failed copies (${line.copies})`);
    check(socket.srtt > 1300 && socket.srtt < 1600, `... and the round trip is learned (${Math.round(socket.srtt)} ms)`);
    // the next request's first wait follows the line: its first copy is enough. (The
    // first request's second copy, sent before its late acknowledgement came, is still on
    // the line: let it land before counting.)
    await sleep(2 * line.oneWay);
    line.copies = 0;
    await socket.requestWithRetry('deviceStatus', { uuid: 'b', linkStatus: 0xff03 });
    check(line.copies === 1, `the next request waits twice the round trip: sent once (${line.copies})`);
    socket.close();
}

// ---- a server that never answers: the retries run out, as before, and say so
{
    const socket = await connected(5);
    line.answer = false;
    const start = Date.now();
    const result = await socket.requestWithRetry('deviceStatus', { uuid: 'c' }, { retries: 2, timeoutMs: 100 }).catch((e) => e);
    check(result instanceof Error && result.message === 'The Server did not answer',
        `no answer: the request fails (${result.message ?? result})`);
    check(line.copies === 3, `... after one copy and two more (${line.copies})`);
    check(Date.now() - start >= 100 + 200 + 400, `... each waiting twice as long as the last (${Date.now() - start} ms)`);
    check(socket.acks.size === 0, `... and leaves no acknowledgement waiting (${socket.acks.size})`);
    socket.close();
}

// ---- stop() ends a request that would retry forever
{
    const socket = await connected(5);
    line.answer = false;
    let stopped = false;
    const pending = socket.requestWithRetry('deviceData', [{ sequence: 0, data: [] }],
        { retries: Infinity, timeoutMs: 50, stop: () => stopped });
    await sleep(120);
    stopped = true;
    const result = await pending.catch((e) => e);
    check(result instanceof Error, `stop() ends a request with endless retries (${result.message ?? result})`);
    socket.close();
}

// ---- a disconnect fails what is waiting, at once
{
    const socket = await connected(5);
    line.answer = false;
    const pending = socket.requestWithRetry('deviceStatus', { uuid: 'd' }, { timeoutMs: 5000 });
    await sleep(20);
    const start = Date.now();
    socket.drop();
    const result = await pending.catch((e) => e);
    check(result instanceof Error && Date.now() - start < 100, `a disconnect fails the request at once (${result.message ?? result})`);
}

console.log(failures ? `${failures} FAILED` : 'all passed');
process.exit(failures ? 1 : 0);
