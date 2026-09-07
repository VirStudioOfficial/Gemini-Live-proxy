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
    console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'connecting_upstream' }));

    const apiKey = keys[0];

    const upstreamUrl =
        `wss://${GEMINI_HOST}/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent` +
        `?key=${encodeURIComponent(apiKey)}`;

    const upstreamWs = new WebSocket(upstreamUrl);

    let upstreamOpen = false;
    const pending = [];

    upstreamWs.on('open', () => {
        upstreamOpen = true;
        if (upstreamWs._socket && upstreamWs._socket.setNoDelay) {
            upstreamWs._socket.setNoDelay(true);
        }
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_open', bufferedMessages: pending.length }));
        for (const msg of pending) upstreamWs.send(msg);
        pending.length = 0;
    });

    // --- Relay: browser -> Gemini ---
    clientWs.on('message', (data) => {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_to_upstream', bytes: data.length, upstreamOpen }));
        if (upstreamOpen && upstreamWs.readyState === WebSocket.OPEN) {
            upstreamWs.send(data);
        } else {
            pending.push(data);
        }
    });

    // --- Relay: Gemini -> browser ---
    upstreamWs.on('message', (data) => {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_to_client', bytes: data.length }));
        if (clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(data);
        }
    });

    // --- Teardown: closing either side closes the other ---
    const closeBoth = (code, reason) => {
        if (clientWs.readyState === WebSocket.OPEN || clientWs.readyState === WebSocket.CONNECTING) {
            clientWs.close(code, reason);
        }
        if (upstreamWs.readyState === WebSocket.OPEN || upstreamWs.readyState === WebSocket.CONNECTING) {
            upstreamWs.close();
        }
    };

    upstreamWs.on('close', (code, reason) => {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_closed', code, reason: reason?.toString() }));
        closeBoth(1011, 'upstream closed');
    });
    upstreamWs.on('error', (err) => {
        console.error(JSON.stringify({ ts: new Date().toISOString(), path, event: 'upstream_error', message: err.message }));
        closeBoth(1011, 'upstream error');
    });

    clientWs.on('close', (code, reason) => {
        console.log(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_closed', code, reason: reason?.toString() }));
        if (upstreamWs.readyState === WebSocket.OPEN || upstreamWs.readyState === WebSocket.CONNECTING) {
            upstreamWs.close();
        }
    });
    clientWs.on('error', (err) => {
        console.error(JSON.stringify({ ts: new Date().toISOString(), path, event: 'client_error', message: err.message }));
        if (upstreamWs.readyState === WebSocket.OPEN || upstreamWs.readyState === WebSocket.CONNECTING) {
            upstreamWs.close();
        }
    });
}

// Conversational voice call - model/config chosen by the client's own
// `setup` message (currently gemini-3.1-flash-live-preview, see index.html).
const wssLive = new WebSocket.Server({ server, path: '/live' });
wssLive.on('connection', (clientWs, req) => handleProxyConnection('/live', clientWs, req));

// Transcription-only feed - a second, independent connection the client
// opens in parallel, pointed at gemini-3.5-transcribe-live with an explicit
// languageCodes so the on-screen Persian transcript stops being guessed as
// Dari/other scripts. Same relay logic as /live; this route exists only so
// two simultaneous upstream connections (with two different `setup`
// messages) can coexist without one clobbering the other on this server.
const wssTranscribe = new WebSocket.Server({ server, path: '/transcribe' });
wssTranscribe.on('connection', (clientWs, req) => handleProxyConnection('/transcribe', clientWs, req));

server.listen(PORT, () => {
    console.log(`Gemini Live proxy listening on :${PORT} (paths: /live, /transcribe)`);
});
