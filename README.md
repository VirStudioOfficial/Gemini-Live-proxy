# Gemini Live Proxy

Fixes "opening handshake timed out" when browsers in restricted networks try
to connect directly to `wss://generativelanguage.googleapis.com`. This proxy
sits somewhere with normal internet access and relays traffic:

```
Browser (restricted network) --wss--> This proxy (Railway etc.) --wss--> Gemini Live
```

## 1. Deploy this folder to Railway

1. Push this folder to its own GitHub repo (or a subfolder of your existing
   repo — Railway lets you pick a root directory per service).
2. In Railway: **New Project → Deploy from GitHub repo** → pick this repo.
3. Set the **Start Command** if Railway doesn't auto-detect it: `npm start`.
4. Add environment variables (Railway dashboard → Variables):
   - `GEMINI_API_KEY` = your real key (same one used in the main app's
     `api/chat.js`)
   - `ALLOWED_ORIGIN` = your deployed frontend URL, e.g.
     `https://your-app.vercel.app` (recommended once things work, to stop
     randoms from burning your quota)
5. Railway will give you a public URL like `your-proxy.up.railway.app`.
   WebSocket works automatically over the same HTTPS domain (`wss://` instead
   of `https://`).
6. Sanity check: visit `https://your-proxy.up.railway.app/health` — should
   return `ok`.

## 2. Point the frontend at the proxy instead of Google directly

In `index.html`, the `startLiveCall()` function currently does:

```js
const wsUrl = `wss://${LIVE_WS_HOST}/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContentConstrained?access_token=${encodeURIComponent(tokenData.token)}`;
const ws = new WebSocket(wsUrl);
```

Change it to connect to the proxy instead, and drop the ephemeral-token
fetch entirely (the proxy holds the real key now, so `/api/live-token` is no
longer needed for this flow — you can leave that endpoint in place unused,
or remove it later):

```js
const ws = new WebSocket('wss://your-proxy.up.railway.app/live');
```

Everything else in `startLiveCall()` — the `setup` message, audio streaming,
`onmessage` handling — stays the same, since the proxy is a transparent
relay of the exact same Gemini protocol messages.

One difference: this proxy connects upstream using the plain
`BidiGenerateContent` method with `?key=`, not the token-constrained
`BidiGenerateContentConstrained` variant, so the initial `setup` message
needs the full config (the old code sent `{ setup: {} }}` because the model
and generationConfig were already baked into the ephemeral token):

```js
ws.onopen = () => {
    ws.send(JSON.stringify({
        setup: {
            model: 'models/gemini-2.5-flash-native-audio-preview-09-2025',
            generationConfig: { responseModalities: ['AUDIO'] }
        }
    }));
};
```

## 3. Test

Open the app, tap the mic button. Network tab → WS filter should show a
connection to `your-proxy.up.railway.app/live` with status `101`, and audio
should flow both ways.

## Notes

- No ephemeral tokens are used anymore since the real key never leaves this
  server — simpler, and no more racing a 1-minute token expiry.
- `ALLOWED_ORIGIN` is a soft check (browsers send `Origin` faithfully, but a
  non-browser client could spoof it). For real usage caps, consider adding a
  short-lived signed token issued by your main backend that this proxy
  verifies before opening the upstream connection — happy to add that if you
  want tighter control.
- If Railway's free tier sleeps/cold-starts the service, the first connection
  after idle time may be slow to open. Render and Fly.io have similar
  free-tier tradeoffs; pick whichever you're most comfortable with.
