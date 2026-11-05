# toss·it

A tiny self-hosted shared clipboard for your homelab. Open it in any browser on your
network, see the other browsers that have it open, and toss them text or files.

No accounts, no pairing, no install, no app. One small container, one global room.

```
┌──────────┐      ws / https      ┌──────────┐      ws / https      ┌──────────┐
│ Browser  │ ──────────────────▶  │ toss·it  │ ──────────────────▶  │ Browser  │
│  (any)   │  text + file upload  │  relay   │  text + download URL │  (any)   │
└──────────┘                      └──────────┘                      └──────────┘
```

## What it does

- **Discovery.** Every browser with the page open appears in everyone else's device list,
  updating live as devices come and go. Each one is named for what it is —
  `Mac Chrome 3f9a`, `iPhone Safari b21c` — from the device, the browser, and a short
  id off the connection, so two tabs of the same browser are still distinct. Nothing is
  remembered between connections; you can rename a device inline for the session.
- **Text.** Pick a device, paste or type, send. It lands in that device's inbox with a
  Copy button (which works over plain HTTP too, not just HTTPS).
- **Files.** Pick a device, then choose files or drag them anywhere onto the page. The
  sender sees upload progress; the recipient gets a download link.
- **Nothing sticks around.** Files live in a temp directory and are deleted once their TTL
  lapses. Nothing survives a restart — no database, no volume needed.

## Run it

With Docker:

```sh
docker run -d --name toss-it -p 3000:3000 --restart unless-stopped \
  ghcr.io/you/toss-it   # or build locally: docker build -t toss-it .
```

With Docker Compose:

```sh
docker compose up -d
```

Without Docker (Node 20+):

```sh
npm install
npm start
```

Then open `http://<your-host>:3000` on two devices.

## Configuration

All optional, all set as environment variables.

| Variable            | Default              | What it does                                                     |
| ------------------- | -------------------- | ---------------------------------------------------------------- |
| `PORT`              | `3000`               | HTTP port the server listens on.                                   |
| `MAX_FILE_MB`       | `512`                | Largest single file that may be relayed. Bigger uploads get a 413. |
| `FILE_TTL_MINUTES`  | `60`                 | How long a relayed file stays downloadable before it is deleted.   |
| `DATA_DIR`          | `<system temp>/toss-it` | Where in-flight files are stored. Emptied on every start.       |

A file is **not** deleted when it is downloaded — it stays until its TTL lapses, so a
failed download can be retried.

## Behind a reverse proxy

The app speaks plain HTTP and knows nothing about TLS; terminate it in front. Serving it
over HTTPS is worth doing: in a secure context the Copy button can use the real clipboard
API. The client picks `wss://` or `ws://` automatically from the page's own URL, so no
configuration is needed on this side.

A minimal Caddyfile:

```caddyfile
toss.example.com {
	reverse_proxy localhost:3000
}
```

Caddy proxies WebSockets and streams large uploads without extra configuration. If you use
something else: nginx needs `proxy_set_header Upgrade`/`Connection` for the WebSocket and a
raised `client_max_body_size` for uploads. Either way, watch the proxy's read/write timeout
if you send very large files.

## Security

**toss·it is completely unauthenticated.** Anyone who can reach the port can see every
connected device and send to any of them. It is built for a trusted LAN or VPN — do not
expose it to the public internet, and putting it behind HTTPS does not change that.

Download URLs are unguessable random UUIDs, which is the only thing protecting a file in
transit. There is no encryption at rest and no end-to-end encryption.

## How it works

Four files, one runtime dependency (`ws`), no build step.

```
server.js            HTTP + WebSocket relay, upload/download, TTL janitor
public/index.html    the whole frontend — markup, styles, vanilla JS
Dockerfile           node:22-alpine, non-root, production deps only
docker-compose.yml
```

Text goes over the WebSocket. Files are POSTed to `/upload`, streamed straight to disk
(never buffered in memory), and the recipient is pushed a `/download/<uuid>` link over its
socket. A janitor sweeps expired files once a minute.
