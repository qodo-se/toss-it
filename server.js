import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

// ---------------------------------------------------------------- config

/** A positive number from the environment, or the default if it is missing or nonsense. */
function numberFromEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    console.warn(`toss-it: ignoring invalid ${name}="${raw}", using ${fallback}`);
    return fallback;
  }
  return value;
}

const PORT = numberFromEnv('PORT', 3000);
const MAX_FILE_MB = numberFromEnv('MAX_FILE_MB', 512);
const FILE_TTL_MINUTES = numberFromEnv('FILE_TTL_MINUTES', 60);
const DATA_DIR = process.env.DATA_DIR || path.join(os.tmpdir(), 'toss-it');

const MAX_FILE_BYTES = Math.round(MAX_FILE_MB * 1024 * 1024);
const FILE_TTL_MS = Math.round(FILE_TTL_MINUTES * 60 * 1000);
const MAX_TEXT_CHARS = 100000;
const MAX_NAME_CHARS = 32;

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'public');

// Nothing is meant to survive a restart: start from an empty data directory.
await fsp.rm(DATA_DIR, { recursive: true, force: true });
await fsp.mkdir(DATA_DIR, { recursive: true });

// ---------------------------------------------------------------- peers

/** @type {Map<string, {id: string, name: string, ws: import('ws').WebSocket}>} */
const peers = new Map();

/**
 * A per-connection label: what the device is, which browser, and a short id.
 * Browsers do not expose the machine's hostname, so the User-Agent sent with
 * the upgrade is the most we can know without asking the page.
 */
function deviceName(userAgent, id) {
  const ua = String(userAgent || '');

  const device =
    /iPhone/i.test(ua) ? 'iPhone'
    : /iPad/i.test(ua) ? 'iPad'
    : /Android/i.test(ua) ? 'Android'
    : /CrOS/i.test(ua) ? 'Chromebook'
    : /Macintosh|Mac OS X/i.test(ua) ? 'Mac'
    : /Windows/i.test(ua) ? 'Windows'
    : /Linux|X11|BSD/i.test(ua) ? 'Linux'
    : 'Device';

  // Order matters: Edge and Opera also claim Chrome, and Chrome also claims Safari.
  const browser =
    /\bEdg(iOS|A)?\//i.test(ua) ? 'Edge'
    : /\bOPR\/|OPiOS|Opera/i.test(ua) ? 'Opera'
    : /FxiOS|Firefox\//i.test(ua) ? 'Firefox'
    : /CriOS|Chrome\//i.test(ua) ? 'Chrome'
    : /Safari\//i.test(ua) ? 'Safari'
    : '';

  return [device, browser, id.slice(0, 4)].filter(Boolean).join(' ');
}

function cleanName(value) {
  if (typeof value !== 'string') return null;
  const name = value.replace(/\s+/g, ' ').trim().slice(0, MAX_NAME_CHARS);
  return name || null;
}

function send(ws, message) {
  if (ws.readyState !== ws.OPEN) return false;
  ws.send(JSON.stringify(message));
  return true;
}

/** Peers other than `exceptId`, in the shape the client renders. */
function peerList(exceptId) {
  return [...peers.values()]
    .filter((peer) => peer.id !== exceptId)
    .map((peer) => ({ id: peer.id, name: peer.name }));
}

function broadcastPeers() {
  for (const peer of peers.values()) {
    send(peer.ws, { type: 'peers', peers: peerList(peer.id) });
  }
}

function deliver(peer, item) {
  return send(peer.ws, { type: 'inbox', item });
}

// ---------------------------------------------------------------- http

const CONTENT_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

function endWith(res, status, body, headers = {}) {
  res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8', ...headers });
  res.end(body);
}

function endJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

/** Pipe a readable file into the response, releasing the fd however it ends. */
function streamFile(filePath, res, headers) {
  const stream = fs.createReadStream(filePath);
  // Without this, a client that aborts mid-download leaks the fd for the life
  // of the process — which also keeps the inode alive after the janitor unlinks it.
  res.on('close', () => stream.destroy());
  stream.on('error', () => {
    if (res.headersSent) return res.destroy();
    endWith(res, 404, 'Not found');
  });
  stream.once('open', () => {
    res.writeHead(200, headers);
    stream.pipe(res);
  });
}

function serveStatic(urlPath, res) {
  const relative = urlPath === '/' ? '/index.html' : urlPath;
  const filePath = path.resolve(PUBLIC_DIR, `.${relative}`);

  // Never serve anything that resolves outside public/.
  if (!filePath.startsWith(PUBLIC_DIR + path.sep)) {
    return endWith(res, 404, 'Not found');
  }

  // Resolve first: a directory would open fine and then fail mid-response.
  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) return endWith(res, 404, 'Not found');
    streamFile(filePath, res, {
      'Content-Type': CONTENT_TYPES[path.extname(filePath)] || 'application/octet-stream',
      'Content-Length': stat.size,
      'Cache-Control': 'no-cache',
    });
  });
}

const server = http.createServer((req, res) => {
  let urlPath;
  let query;
  try {
    const url = new URL(req.url, 'http://localhost');
    urlPath = decodeURIComponent(url.pathname);
    query = url.searchParams;
  } catch {
    return endWith(res, 400, 'Bad request');
  }
  // A NUL byte would make every fs call throw, taking the process down with it.
  if (urlPath.includes('\0')) return endWith(res, 400, 'Bad request');

  if (urlPath === '/healthz') return endWith(res, 200, 'ok');
  if (req.method === 'POST' && urlPath === '/upload') return handleUpload(req, res, query);
  if ((req.method === 'GET' || req.method === 'HEAD') && urlPath.startsWith('/download/')) {
    return handleDownload(req, res, urlPath.slice('/download/'.length));
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') return endWith(res, 405, 'Method not allowed');

  return serveStatic(urlPath, res);
});

// ---------------------------------------------------------------- file relay

/** @type {Map<string, {id: string, name: string, size: number, mime: string, toId: string, expiresAt: number}>} */
const files = new Map();

function cleanFileName(value) {
  const raw = typeof value === 'string' ? value : '';
  // Only ever used for display and Content-Disposition — never to build a path.
  const name = path.basename(raw).replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 255);
  return name || 'file';
}

function cleanMime(value) {
  const mime = String(value || '').split(';')[0].trim();
  return /^[\w.+-]+\/[\w.+-]+$/.test(mime) ? mime : 'application/octet-stream';
}

function contentDisposition(name) {
  const ascii = name.replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`;
}

async function discard(id) {
  files.delete(id);
  await fsp.rm(path.join(DATA_DIR, id), { force: true }).catch(() => {});
}

function handleUpload(req, res, query) {
  const target = peers.get(query.get('to') || '');
  if (!target) return endJson(res, 404, { error: 'That device is no longer connected.' });
  const sender = peers.get(query.get('from') || '');

  const declared = Number(req.headers['content-length']);
  if (Number.isFinite(declared) && declared > MAX_FILE_BYTES) {
    return endJson(res, 413, { error: `File is larger than the ${MAX_FILE_MB}MB limit.` });
  }

  const id = crypto.randomUUID();
  const name = cleanFileName(query.get('name'));
  const mime = cleanMime(req.headers['content-type']);
  const filePath = path.join(DATA_DIR, id);
  const out = fs.createWriteStream(filePath);

  let received = 0;
  let failed = false;

  const fail = (status, error) => {
    if (failed) return;
    failed = true;
    req.unpipe(out);
    out.destroy();
    discard(id);
    if (!res.headersSent) endJson(res, status, { error });
    // Stop the upload, but only once the error response has made it out.
    const closeRequest = () => { if (!req.readableEnded) req.destroy(); };
    if (res.writableFinished) closeRequest();
    else res.on('finish', closeRequest);
  };

  // Content-Length is advisory; this counter is the real enforcement.
  req.on('data', (chunk) => {
    received += chunk.length;
    if (received > MAX_FILE_BYTES) fail(413, `File is larger than the ${MAX_FILE_MB}MB limit.`);
  });
  req.on('aborted', () => fail(400, 'Upload was interrupted.'));
  out.on('error', () => fail(500, 'Could not store the file.'));

  out.on('finish', () => {
    if (failed) return;

    const stillThere = peers.get(target.id);
    if (!stillThere) {
      discard(id);
      return endJson(res, 410, { error: 'That device left before the upload finished.' });
    }

    const entry = { id, name, size: received, mime, toId: target.id, expiresAt: Date.now() + FILE_TTL_MS };
    files.set(id, entry);

    const delivered = deliver(stillThere, {
      kind: 'file',
      id,
      at: Date.now(),
      from: sender ? { id: sender.id, name: sender.name } : { id: '', name: 'Unknown device' },
      name,
      size: received,
      mime,
      url: `/download/${id}`,
      expiresAt: entry.expiresAt,
    });

    // Don't claim success — and don't park the file on disk for a full TTL —
    // if the recipient's socket was already closing.
    if (!delivered) {
      discard(id);
      return endJson(res, 410, { error: 'That device left before the upload finished.' });
    }

    endJson(res, 200, { id, size: received });
  });

  req.pipe(out);
}

function handleDownload(req, res, rawId) {
  // The id is looked up in the registry first; it is never joined onto a path
  // unless it matched an entry we created ourselves.
  const entry = files.get(rawId);
  if (!entry || entry.expiresAt <= Date.now()) {
    return endWith(res, 404, 'This file has expired or does not exist.');
  }

  const headers = {
    'Content-Type': entry.mime,
    'Content-Length': entry.size,
    'Content-Disposition': contentDisposition(entry.name),
    'Cache-Control': 'no-store',
  };

  // The client checks liveness with HEAD before starting a download.
  if (req.method === 'HEAD') {
    res.writeHead(200, headers);
    return res.end();
  }

  streamFile(path.join(DATA_DIR, entry.id), res, headers);
}

const janitor = setInterval(() => {
  const now = Date.now();
  for (const entry of files.values()) {
    if (entry.expiresAt <= now) discard(entry.id);
  }
}, 60000);
janitor.unref();

// ---------------------------------------------------------------- websocket hub

// The largest legal message is a MAX_TEXT_CHARS text frame; the 100MB default
// would let one client force a huge allocation before any length check runs.
const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 });

/**
 * WebSockets are exempt from the same-origin policy, so without this any web
 * page a user visits could open a socket to this relay and read the device
 * list. Non-browser clients send no Origin and are left alone.
 */
function sameOrigin(req) {
  const origin = req.headers.origin;
  if (!origin) return true;
  try {
    return new URL(origin).host === req.headers.host;
  } catch {
    return false;
  }
}

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    ({ pathname } = new URL(req.url, 'http://localhost'));
  } catch {
    return socket.destroy();
  }
  if (pathname !== '/ws') return socket.destroy();
  if (!sameOrigin(req)) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws, req) => {
  const id = crypto.randomUUID();
  const peer = { id, name: deviceName(req.headers['user-agent'], id), ws };
  peers.set(peer.id, peer);
  ws.isAlive = true;

  send(ws, {
    type: 'welcome',
    you: { id: peer.id, name: peer.name },
    peers: peerList(peer.id),
    limits: { maxFileBytes: MAX_FILE_BYTES, fileTtlMinutes: FILE_TTL_MINUTES },
  });
  broadcastPeers();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: 'error', message: 'Malformed message.' });
    }

    if (message?.type === 'rename') {
      const name = cleanName(message.name);
      if (!name) return send(ws, { type: 'error', message: 'Name cannot be empty.' });
      peer.name = name;
      send(ws, { type: 'renamed', name });
      return broadcastPeers();
    }

    if (message?.type === 'text') {
      const target = peers.get(message.to);
      if (!target || target.id === peer.id) {
        return send(ws, { type: 'error', message: 'That device is no longer connected.' });
      }
      const text = typeof message.text === 'string' ? message.text : '';
      if (!text.trim()) return send(ws, { type: 'error', message: 'Nothing to send.' });
      if (text.length > MAX_TEXT_CHARS) {
        return send(ws, { type: 'error', message: `Text is too long (max ${MAX_TEXT_CHARS} characters).` });
      }
      const delivered = deliver(target, {
        kind: 'text',
        id: crypto.randomUUID(),
        at: Date.now(),
        from: { id: peer.id, name: peer.name },
        text,
      });
      if (!delivered) {
        return send(ws, { type: 'error', message: 'That device is no longer connected.' });
      }
      return send(ws, { type: 'sent', to: target.id, kind: 'text' });
    }

    return send(ws, { type: 'error', message: 'Unknown message type.' });
  });

  ws.on('close', () => {
    peers.delete(peer.id);
    broadcastPeers();
  });

  ws.on('error', () => ws.terminate());
});

// Drop sockets that stopped answering (closed laptop, dropped wifi) within ~60s.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
heartbeat.unref();

server.listen(PORT, () => {
  console.log(`toss-it listening on http://localhost:${PORT}`);
  console.log(`  max file size ${MAX_FILE_MB}MB | file TTL ${FILE_TTL_MINUTES}min | data dir ${DATA_DIR}`);
});
