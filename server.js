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

const ADJECTIVES = [
  'Amber', 'Brave', 'Calm', 'Clever', 'Copper', 'Crimson', 'Dusty', 'Eager',
  'Fuzzy', 'Golden', 'Happy', 'Indigo', 'Jolly', 'Lucky', 'Mellow', 'Nimble',
  'Olive', 'Proud', 'Purple', 'Quiet', 'Rapid', 'Silver', 'Sunny', 'Teal',
  'Velvet', 'Witty', 'Zesty',
];

const ANIMALS = [
  'Otter', 'Falcon', 'Badger', 'Panda', 'Heron', 'Lynx', 'Marmot', 'Narwhal',
  'Ocelot', 'Puffin', 'Quokka', 'Raven', 'Stoat', 'Tapir', 'Urchin', 'Viper',
  'Walrus', 'Yak', 'Zebra', 'Gecko', 'Ibex', 'Koala',
];

function randomName() {
  const pick = (list) => list[crypto.randomInt(list.length)];
  return `${pick(ADJECTIVES)} ${pick(ANIMALS)}`;
}

function cleanName(value) {
  if (typeof value !== 'string') return null;
  const name = value.replace(/\s+/g, ' ').trim().slice(0, MAX_NAME_CHARS);
  return name || null;
}

function send(ws, message) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(message));
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
  send(peer.ws, { type: 'inbox', item });
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

function serveStatic(urlPath, res) {
  const relative = urlPath === '/' ? '/index.html' : urlPath;
  const filePath = path.resolve(PUBLIC_DIR, `.${relative}`);

  // Never serve anything that resolves outside public/.
  if (filePath !== PUBLIC_DIR && !filePath.startsWith(PUBLIC_DIR + path.sep)) {
    return endWith(res, 404, 'Not found');
  }

  const stream = fs.createReadStream(filePath);
  stream.on('error', () => endWith(res, 404, 'Not found'));
  stream.once('open', () => {
    res.writeHead(200, {
      'Content-Type': CONTENT_TYPES[path.extname(filePath)] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    stream.pipe(res);
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

    deliver(stillThere, {
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

  const stream = fs.createReadStream(path.join(DATA_DIR, entry.id));
  stream.on('error', () => {
    if (!res.headersSent) endWith(res, 404, 'This file has expired or does not exist.');
    else res.destroy();
  });
  stream.once('open', () => {
    res.writeHead(200, headers);
    stream.pipe(res);
  });
}

const janitor = setInterval(() => {
  const now = Date.now();
  for (const entry of files.values()) {
    if (entry.expiresAt <= now) discard(entry.id);
  }
}, 60000);
janitor.unref();

// ---------------------------------------------------------------- websocket hub

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    ({ pathname } = new URL(req.url, 'http://localhost'));
  } catch {
    return socket.destroy();
  }
  if (pathname !== '/ws') return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  const peer = { id: crypto.randomUUID(), name: randomName(), ws };
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
      deliver(target, {
        kind: 'text',
        id: crypto.randomUUID(),
        at: Date.now(),
        from: { id: peer.id, name: peer.name },
        text,
      });
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
