// server.js — Gemini Live WebSocket proxy
//
// -----------------------------------------------------------------------------
// Why this exists
// -----------------------------------------------------------------------------
// The browser can't open a direct WebSocket to
// wss://generativelanguage.googleapis.com/... from inside networks that block
// direct access to Google (this is what "opening handshake timed out" meant
// in testing). The fix: host this small proxy somewhere with unrestricted
// network access (Railway, Render, Fly.io, a VPS, etc). The browser connects
// to THIS server instead, and this server opens the real connection to
// Gemini on the browser's behalf, relaying messages both ways.
//
//   Browser (Iran) --wss--> This proxy (Railway, outside IP) --wss--> Gemini
//
// Because the real GEMINI_API_KEY now only ever lives on this server (never
// in the browser), we skip the ephemeral-token dance entirely and connect to
// Gemini directly with the real key. Simpler, and no more 1-minute token
// expiry to race against.
// -----------------------------------------------------------------------------

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const GEMINI_HOST = 'generativelanguage.googleapis.com';

// Comma-separated list, same convention as the main app's api/chat.js and
// api/live-token.js, so you can reuse the same env var value.
function getGeminiKeys() {
    const raw = process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '';
    return raw.split(',').map(k => k.trim()).filter(Boolean);
}

// Optional: restrict which origins may open a proxy connection, so randoms
// can't rack up Gemini usage on your key. Leave ALLOWED_ORIGIN unset during
// initial testing, set it once you know your app's real origin.
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '';

const server = http.createServer((req, res) => {
    // Respond 200 on both `/` and `/health` so Railway's default healthcheck
    // (which often just hits `/`) doesn't see a 404 and kill the container.
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('ok');
        return;
    }
    res.writeHead(404);
    res.end();
});

// Some platforms send SIGTERM on redeploys/scaling events; log it instead of
// dying silently so it's obvious in the logs what happened.
process.on('SIGTERM', () => {
    console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'sigterm_received' }));
    server.close(() => process.exit(0));
});

// Shared connection handler for both proxy paths below. `path` is only used
// for logging so it's obvious in Render's log stream which leg (the
// conversational call or the transcription-only feed) a given line belongs
// to - the relay logic itself is identical either way: whatever the client
// sends goes upstream verbatim, whatever comes back from Gemini goes to the
// client verbatim. Which model/config actually gets used is entirely up to
// the `setup` message the CLIENT sends first; this proxy never inspects or
// rewrites message contents.
//
// FIX (یه کلید API خراب/quota-تمام‌شده کل قابلیت ویس رو برای همه از کار
// می‌انداخت): قبلاً همیشه فقط keys[0] رو امتحان می‌کرد، دقیقاً برخلاف
// api/chat.js و api/live-token.js که هر دو بین چند کلید (GEMINI_API_KEYS)
// می‌چرخند تا یه کلید خراب کل فیچر رو نخوابونه. الان اینجا هم همون الگو
// پیاده شده: کلیدها رو به ترتیب امتحان می‌کند، فقط تا وقتی که upstream
// هنوز واقعاً باز نشده (یعنی هیچ داده‌ی واقعی رد و بدل نشده) - بعد از باز
// شدن موفق یه اتصال، دیگر به کلید بعدی سوییچ نمی‌کند (چون در آن نقطه
// دیگر مشکل از کلید نیست، relay معمولی جریان دارد).
function connectUpstreamWithKeyRotation(path, keys, keyIndex, onOpen, onMessage, onCloseOrError) {
    if (keyIndex >= keys.length) {
        onCloseOrError(new Error('همه‌ی کلیدهای Gemini برای این اتصال fail شدند'));
        return;
    }
    const apiKey = keys[keyIndex];
    const upstreamUrl =
        `wss://${GEMINI_HOST}/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent` +
        `?key=${encodeURIComponent(apiKey)}`;
    const upstreamWs = new WebSocket(upstreamUrl);
    let openedSuccessfully = false;
    let settled = false; // true once we've either opened successfully or moved to the next key

    upstreamWs.on('open', () => {
        openedSuccessfully = true;
        settled = true;
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_open', keyIndex }));
        onOpen(upstreamWs);
    });
    upstreamWs.on('message', (data) => onMessage(data));
    const tryNextOrFail = (reason, extra) => {
        if (settled && openedSuccessfully) {
            // Already relaying real traffic on this key - this is a normal
            // end-of-session close, not a key problem. Report upward as-is.
            onCloseOrError(null, extra);
            return;
        }
        if (settled) return; // already moved on once, ignore duplicate close/error
        settled = true;
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_key_failed', keyIndex, reason }));
        connectUpstreamWithKeyRotation(path, keys, keyIndex + 1, onOpen, onMessage, onCloseOrError);
    };
    upstreamWs.on('close', (code, reason) => tryNextOrFail('close:' + code, { code, reason }));
    upstreamWs.on('error', (err) => tryNextOrFail('error:' + err.message, { err }));
}

function handleProxyConnection(path, clientWs, req) {
    const origin = req.headers.origin || '';
    console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_connected', origin }));

    // Realtime audio is a stream of small, frequent frames (one every
    // ~256ms per the client's buffer size). Nagle's algorithm (on by
    // default) can hold small TCP packets briefly hoping to coalesce them,
    // which adds latency that's pointless here - we want every frame out
    // immediately. Disabling it on the raw socket costs nothing since we're
    // not sending enough data to need the coalescing.
    if (clientWs._socket && clientWs._socket.setNoDelay) {
        clientWs._socket.setNoDelay(true);
    }

    if (ALLOWED_ORIGIN && origin !== ALLOWED_ORIGIN) {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'origin_rejected', origin, allowed: ALLOWED_ORIGIN }));
        clientWs.close(4403, 'origin not allowed');
        return;
    }

    const keys = getGeminiKeys();
    if (keys.length === 0) {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'no_api_key_configured' }));
        clientWs.close(4500, 'server has no GEMINI_API_KEY configured');
        return;
    }
    console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'connecting_upstream', totalKeys: keys.length }));

    let upstreamOpen = false;
    let upstreamWsRef = null;
    const pending = [];

    connectUpstreamWithKeyRotation(
        path,
        keys,
        0,
        (upstreamWs) => {
            // onOpen: this key worked - wire up the real relay from here on.
            upstreamWsRef = upstreamWs;
            upstreamOpen = true;
            if (upstreamWs._socket && upstreamWs._socket.setNoDelay) {
                upstreamWs._socket.setNoDelay(true);
            }
            console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_ready', bufferedMessages: pending.length }));
            for (const msg of pending) upstreamWs.send(msg);
            pending.length = 0;
        },
        (data) => {
            // onMessage: relay Gemini -> browser, same as before (see the
            // matching comment on the client->upstream side for why the
            // per-frame log line was removed).
            if (clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(data);
            }
        },
        (err, extra) => {
            // onCloseOrError: either every key failed (err set, before any
            // successful open), or the successfully-opened upstream closed
            // normally/abnormally afterward (err null, extra has code/reason).
            if (err) {
                console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'all_keys_failed', message: err.message }));
                if (clientWs.readyState === WebSocket.OPEN || clientWs.readyState === WebSocket.CONNECTING) {
                    clientWs.close(4500, 'upstream unavailable (all keys failed)');
                }
                return;
            }
            if (extra && extra.err) {
                console.error(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_error', message: extra.err.message }));
            } else if (extra) {
                console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_closed', code: extra.code, reason: extra.reason?.toString() }));
            }
            if (clientWs.readyState === WebSocket.OPEN || clientWs.readyState === WebSocket.CONNECTING) {
                clientWs.close(1011, 'upstream closed');
            }
        }
    );

    // --- Relay: browser -> Gemini ---
    // FIX (لاگ‌های حجیم): قبلاً هر فریم صوتی (هر ~۲۵۶ میلی‌ثانیه، برای کل
    // مدت هر تماس) جداگانه لاگ می‌شد - روی هاست رایگان (Render) این خیلی
    // سریع فضای لاگ رو پر می‌کرد و I/O همزمانِ console.log هم اضافه‌بار
    // بی‌مورد به هر فریم می‌داد. الان فقط لحظه‌ی شروع/پایان و رویدادهای
    // واقعاً مهم (تغییر کلید، خطا) لاگ می‌شوند، نه هر فریم تکی.
    clientWs.on('message', (data) => {
        if (upstreamOpen && upstreamWsRef && upstreamWsRef.readyState === WebSocket.OPEN) {
            upstreamWsRef.send(data);
        } else {
            pending.push(data);
        }
    });

    clientWs.on('close', (code, reason) => {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_closed', code, reason: reason?.toString() }));
        if (upstreamWsRef && (upstreamWsRef.readyState === WebSocket.OPEN || upstreamWsRef.readyState === WebSocket.CONNECTING)) {
            upstreamWsRef.close();
        }
    });
    clientWs.on('error', (err) => {
        console.error(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_error', message: err.message }));
        if (upstreamWsRef && (upstreamWsRef.readyState === WebSocket.OPEN || upstreamWsRef.readyState === WebSocket.CONNECTING)) {
            upstreamWsRef.close();
        }
    });
}

// Conversational voice call - model/config chosen by the client's own
// `setup` message (currently gemini-3.1-flash-live-preview, see index.html).
const wssLive = new WebSocket.Server({ noServer: true });
wssLive.on('connection', (clientWs, req) => handleProxyConnection('/live', clientWs, req));

// Transcription-only feed - a second, independent connection the client
// opens in parallel, pointed at gemini-3.5-transcribe-live with an explicit
// languageCodes so the on-screen Persian transcript stops being guessed as
// Dari/other scripts. Same relay logic as /live; this route exists only so
// two simultaneous upstream connections (with two different `setup`
// messages) can coexist without one clobbering the other on this server.
const wssTranscribe = new WebSocket.Server({ noServer: true });
wssTranscribe.on('connection', (clientWs, req) => handleProxyConnection('/transcribe', clientWs, req));

// FIX: two separate `new WebSocket.Server({ server, path })` instances
// attached to the SAME http.Server do not reliably co-exist - this is a
// long-standing, still-open limitation of the `ws` library itself (see
// https://github.com/websockets/ws/issues/1044 and
// https://github.com/websockets/ws/issues/1189), not something specific to
// this proxy. In testing here it surfaced as every /live connection being
// torn down immediately (code 1006) the instant /transcribe was added,
// even though /transcribe was never actually opened yet - so it wasn't a
// bug in the transcribe path itself, it was the mere presence of a second
// `{ server, path }` instance breaking the first one.
//
// The documented fix is `noServer: true` on both instances (above) plus
// manually handling the server's single `upgrade` event ourselves and
// routing by `req.url` to whichever WebSocketServer matches - this is the
// pattern the ws README itself recommends for "multiple servers sharing a
// single https server".
server.on('upgrade', (req, socket, head) => {
    const { pathname } = new URL(req.url, `http://${req.headers.host}`);
    if (pathname === '/live') {
        wssLive.handleUpgrade(req, socket, head, (ws) => wssLive.emit('connection', ws, req));
    } else if (pathname === '/transcribe') {
        wssTranscribe.handleUpgrade(req, socket, head, (ws) => wssTranscribe.emit('connection', ws, req));
    } else {
        socket.destroy();
    }
});

server.listen(PORT, () => {
    console.log(`Gemini Live proxy listening on :${PORT} (paths: /live, /transcribe)`);
});
