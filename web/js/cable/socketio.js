// A socket.io v4 client over one WebSocket (Engine.IO 4, WebSocket transport only), enough
// for the Celio server: events with and without acknowledgements, both ways, JSON only.
//
// Wire: "0{...}" open, "2"/"3" ping/pong, "40{auth}" connect, "41" disconnect,
// "42[id][event,...args]" event, "43id[...args]" acknowledgement.

// The longest a copy of a request waits for its acknowledgement before the next goes.
const MAX_WAIT_MS = 8000;

export class SocketIo extends EventTarget {
    constructor(url, auth) {
        super();
        this.url = url;
        this.auth = auth;
        this.ws = null;
        this.connected = false;
        this.nextAck = 0;
        this.acks = new Map();
        this.handlers = new Map();
        this.closing = false;
        this.srtt = null;             // the smoothed round trip, ms; null until an acknowledgement
    }

    on(event, handler) { this.handlers.set(event, handler); }

    connect(timeoutMs = 5000) {
        this.closing = false;
        return new Promise((resolve, reject) => {
            const base = this.url.replace(/\/+$/, '');
            const ws = this.ws = new WebSocket(`${base}/socket.io/?EIO=4&transport=websocket`);
            const timer = setTimeout(() => { ws.close(); reject(new Error('Could not connect to Server')); }, timeoutMs);
            ws.onmessage = (message) => {
                const text = String(message.data);
                if (text[0] === '0') { ws.send(`40${JSON.stringify(this.auth)}`); return; }
                if (text === '2') { ws.send('3'); return; }
                if (text.startsWith('40')) {
                    clearTimeout(timer);
                    this.connected = true;
                    resolve();
                    return;
                }
                if (text.startsWith('44')) { clearTimeout(timer); reject(new Error(connectError(text))); return; }
                if (text.startsWith('41')) { this.drop(); return; }
                if (text.startsWith('42')) this.onEvent(text.slice(2));
                else if (text.startsWith('43')) this.onAck(text.slice(2));
            };
            ws.onerror = () => {};
            ws.onclose = () => {
                clearTimeout(timer);
                if (!this.connected) { reject(new Error('Could not connect to Server')); return; }
                this.drop();
            };
        });
    }

    drop() {
        if (!this.connected) return;
        this.connected = false;
        for (const [, pending] of this.acks) pending.reject(new Error('Disconnected'));
        this.acks.clear();
        if (!this.closing) this.dispatchEvent(new Event('disconnect'));
    }

    close() {
        this.closing = true;
        try { if (this.connected) this.ws.send('41'); } catch {}
        try { this.ws?.close(); } catch {}
        this.drop();
    }

    emit(event, ...args) {
        if (!this.connected) return;
        this.ws.send(`42${JSON.stringify([event, ...args])}`);
    }

    // Resolves with the acknowledgement's arguments.
    request(event, arg, timeoutMs = 5000) {
        if (!this.connected) return Promise.reject(new Error('Not connected to the Server'));
        const id = this.nextAck++;
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => { this.acks.delete(id); reject(new Error('The Server did not answer')); }, timeoutMs);
            this.acks.set(id, {
                resolve: (value) => { clearTimeout(timer); resolve(value); },
                reject: (error) => { clearTimeout(timer); reject(error); },
            });
            this.ws.send(`42${id}${JSON.stringify([event, arg ?? null])}`);
        });
    }

    // request(), sent again while the server does not answer (as Celio-Client's
    // emitWithRetry): the server drops what it already has by uuid or sequence.
    //
    // Each copy waits twice as long as the one before (1, 2, 4, 8, 8 ... s), and every
    // copy's acknowledgement is listened for until the last copy's wait is over, so an
    // acknowledgement that comes late still counts. With a fixed wait shorter than the
    // round trip, every copy timed out before its acknowledgement could arrive: on a line
    // slower than a second each way the link never moved. The first wait is twice the
    // smoothed round trip once one is known, never under 1 s or over 8 s; timeoutMs fixes
    // it instead.
    requestWithRetry(event, arg, { retries = 5, timeoutMs = null, backoffMs = 100, stop = null } = {}) {
        const first = timeoutMs ?? Math.min(Math.max(2 * (this.srtt ?? 0), 1000), MAX_WAIT_MS);
        return new Promise((resolve, reject) => {
            const ids = [];
            let done = false, attempt = 0, timer = null;
            const finish = (settle, value) => {
                if (done) return;
                done = true;
                clearTimeout(timer);
                for (const id of ids) this.acks.delete(id);
                settle(value);
            };
            const send = () => {
                if (done) return;
                if (!this.connected) { finish(reject, new Error('Not connected to the Server')); return; }
                attempt++;
                const id = this.nextAck++, sentAt = performance.now();
                ids.push(id);
                this.acks.set(id, {
                    // An acknowledgement names its copy, so each one is a clean sample.
                    resolve: (value) => { this.sampleRoundTrip(performance.now() - sentAt); finish(resolve, value); },
                    reject: (error) => finish(reject, error),
                });
                this.ws.send(`42${id}${JSON.stringify([event, arg ?? null])}`);
                timer = setTimeout(() => {
                    if (attempt > retries || stop?.() || !this.connected) {
                        finish(reject, new Error(this.connected ? 'The Server did not answer' : 'Disconnected'));
                        return;
                    }
                    timer = setTimeout(send, Math.min(backoffMs * attempt, 1000));
                }, Math.min(first * 2 ** (attempt - 1), MAX_WAIT_MS));
            };
            send();
        });
    }

    // The smoothed round trip, as TCP keeps it: 7/8 the old, 1/8 the new.
    sampleRoundTrip(ms) {
        this.srtt = this.srtt === null ? ms : this.srtt * 7 / 8 + ms / 8;
    }

    onEvent(text) {
        const bracket = text.indexOf('[');
        const id = bracket > 0 ? Number(text.slice(0, bracket)) : null;
        let packet;
        try { packet = JSON.parse(text.slice(bracket)); } catch { return; }
        const [event, ...args] = packet;
        // The server sends an event again until it is acknowledged.
        if (id !== null) this.ws.send(`43${id}${JSON.stringify([true])}`);
        this.handlers.get(event)?.(...args);
    }

    onAck(text) {
        const bracket = text.indexOf('[');
        const id = Number(text.slice(0, bracket));
        const pending = this.acks.get(id);
        if (!pending) return;
        this.acks.delete(id);
        try { pending.resolve(JSON.parse(text.slice(bracket))); } catch (error) { pending.reject(error); }
    }
}

function connectError(text) {
    try { return JSON.parse(text.slice(2)).message ?? 'Could not connect to Server'; } catch { return 'Could not connect to Server'; }
}
