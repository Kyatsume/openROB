#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

APP_DIR="$HOME/sacredforge"
SESSION="sacredforge"

echo "========================================"
echo " SacredForge WebSocket/TCP Bridge Setup"
echo "========================================"
echo

if [ -z "${PREFIX:-}" ] || [ ! -d "$PREFIX" ]; then
  echo "ERROR: This installer must be run inside Termux."
  exit 1
fi

echo "[1/5] Updating Termux packages..."
pkg update -y
pkg upgrade -y

echo
echo "[2/5] Installing Node.js and tmux..."
pkg install -y nodejs-lts tmux

echo
echo "[3/5] Creating bridge files..."
mkdir -p "$APP_DIR"
cd "$APP_DIR"

cat > bridge.js <<'JAVASCRIPT'
'use strict';

const net = require('net');
const WebSocket = require('ws');

const WS_BASE = 'wss://global.sacredforge.online/ws';
const TARGET_HOST = 'sacredforge';
const LOGIN_PORT = 7800;

const CLIENT_ORIGIN = 'https://play.sacredforge.online';
const CLIENT_UA =
  'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
const CLIENT_COOKIE = '';

const activeBridges = new Set();

const WAN_META = JSON.stringify({
  wanmeta: 1,
  os: 'iOS',
  osVersion: '18.5',
  browser: 'Brave',
  browserVersion: '155.0.0.0',
  client: 'roBrowser 20250604',
  cores: '8',
  device: 'iPhone',
  gpu: 'Brave',
  memory: '4',
  platform: 'mobile',
  ua: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
});

function buildHeaders() {
  const h = {
    Origin: CLIENT_ORIGIN,
    Referer: CLIENT_ORIGIN + '/',
    'User-Agent': CLIENT_UA,
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    Pragma: 'no-cache',
  };
  if (CLIENT_COOKIE) h.Cookie = CLIENT_COOKIE;
  return h;
}

function rewriteServerRedirects(buf) {
  if (buf.length < 4) return buf;
  const packetId = buf.readUInt16LE(0);

  if ((packetId === 0x0ac4 || packetId === 0x0069) && buf.length >= 47) {
    const packetLen = buf.readUInt16LE(2) || buf.length;
    const entryStart = packetId === 0x0ac4 ? 64 : 47;
    const entrySize = packetId === 0x0ac4 ? 160 : 32;
    for (
      let offset = entryStart;
      offset + 6 <= Math.min(buf.length, packetLen || buf.length);
      offset += entrySize
    ) {
      const origIp = `${buf[offset]}.${buf[offset + 1]}.${buf[offset + 2]}.${buf[offset + 3]}`;
      const port = buf.readUInt16LE(offset + 4);
      if (port > 0 && port < 65535) {
        console.log(`[Bridge] Detected Char-Server ${origIp}:${port} -> 127.0.0.1:${port}`);
        startBridge(port);
        buf[offset] = 127;
        buf[offset + 1] = 0;
        buf[offset + 2] = 0;
        buf[offset + 3] = 1;
      }
    }
  }

  if (packetId === 0x0ac5 && buf.length >= 28) {
    const ipOffset = 22;
    const portOffset = 26;

    const origIp = `${buf[ipOffset]}.${buf[ipOffset + 1]}.${buf[ipOffset + 2]}.${buf[ipOffset + 3]}`;
    const port = buf.readUInt16LE(portOffset);

    console.log(`[Bridge] 0AC5 Map handoff: ${origIp}:${port}`);

    if (port > 0 && port < 65535) {
      console.log(`[Bridge] Redirecting Map-Server ${origIp}:${port} -> 127.0.0.1:${port}`);
      startBridge(port);
      buf[ipOffset] = 127;
      buf[ipOffset + 1] = 0;
      buf[ipOffset + 2] = 0;
      buf[ipOffset + 3] = 1;
    } else {
      console.error('[Bridge] Invalid Map-Server port:', port);
    }
  }

  if (packetId === 0x0071 && buf.length >= 28) {
    const origIp = `${buf[22]}.${buf[23]}.${buf[24]}.${buf[25]}`;
    const port = buf.readUInt16LE(26);
    if (port > 0 && port < 65535) {
      console.log(`[Bridge] Detected Map-Server (0071) ${origIp}:${port} -> 127.0.0.1:${port}`);
      startBridge(port);
      buf[22] = 127;
      buf[23] = 0;
      buf[24] = 0;
      buf[25] = 1;
    }
  }

  return buf;
}

function startBridge(port) {
  if (activeBridges.has(port)) return;
  activeBridges.add(port);

  const server = net.createServer((tcpSocket) => {
    const wsUrl = `${WS_BASE}/${TARGET_HOST}:${port}`;
    console.log(`[Bridge:${port}] OpenKore connected -> opening ${wsUrl}`);

    let pendingPackets = [];
    let wsReady = false;
    let reconnectAttempts = 0;
    const MAX_RECONNECT_ATTEMPTS = 5;
    let currentWs = null;
    let closed = false;

    const connectWebSocket = () => {
      const ws = new WebSocket(wsUrl, {
        headers: buildHeaders(),
        perMessageDeflate: false,
        handshakeTimeout: 15000,
      });
      currentWs = ws;
      ws.binaryType = 'nodebuffer';

      ws.on('open', () => {
        console.log(`[Bridge:${port}] 101 received. Sending wanmeta FIRST.`);
        wsReady = true;
        reconnectAttempts = 0;

        // wanmeta is always the first frame sent after the WebSocket opens.
        ws.send(WAN_META);

        // Flush packets OpenKore queued before the WebSocket handshake completed.
        for (const pkt of pendingPackets) {
          const id =
            pkt.length >= 2 ? pkt.readUInt16LE(0).toString(16).padStart(4, '0') : '????';
          console.log(`[Bridge:${port}] >>> BIN ${pkt.length}B id=0x${id} (queued)`);
          ws.send(pkt, { binary: true });
        }
        pendingPackets = [];
      });

      ws.on('message', (data, isBinary) => {
        if (!isBinary) {
          console.log(`[Bridge:${port}] <<< TEXT:`, data.toString());
          return;
        }
        const buf = Buffer.from(data);
        const id =
          buf.length >= 2 ? buf.readUInt16LE(0).toString(16).padStart(4, '0') : '????';
        console.log(`[Bridge:${port}] <<< BIN ${buf.length}B id=0x${id}`);
        if (!tcpSocket.destroyed) {
          tcpSocket.write(rewriteServerRedirects(buf));
        }
      });

      ws.on('close', (code, reason) => {
        const r = reason ? reason.toString() : '';
        console.log(`[Bridge:${port}] WebSocket closed (${code}): ${JSON.stringify(r)}`);
        wsReady = false;
        if (currentWs === ws) currentWs = null;
        if (!closed && !tcpSocket.destroyed) tcpSocket.destroy();
      });

      ws.on('unexpected-response', (req, res) => {
        console.error(`[Bridge:${port}] Unexpected HTTP ${res.statusCode} on handshake`);
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          if (body) console.error(`[Bridge:${port}] Body:`, body.slice(0, 400));
        });
      });

      ws.on('error', (err) => {
        console.error(`[Bridge:${port}] WebSocket error:`, err.message);
        wsReady = false;
        if (currentWs === ws) currentWs = null;

        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS && !tcpSocket.destroyed && !closed) {
          reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
          console.log(`[Bridge:${port}] Retrying in ${delay}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
          setTimeout(() => {
            if (!tcpSocket.destroyed && !closed) connectWebSocket();
          }, delay);
        } else {
          tcpSocket.destroy();
        }
      });

      return ws;
    };

    connectWebSocket();

    tcpSocket.on('data', (data) => {
      const id =
        data.length >= 2 ? data.readUInt16LE(0).toString(16).padStart(4, '0') : '????';
      console.log(`[Bridge:${port}] >>> BIN ${data.length}B id=0x${id}`);

      if (wsReady && currentWs && currentWs.readyState === WebSocket.OPEN) {
        currentWs.send(data, { binary: true });
      } else {
        pendingPackets.push(data);
      }
    });

    tcpSocket.on('close', () => {
      console.log(`[Bridge:${port}] TCP connection closed.`);
      closed = true;
      if (currentWs && currentWs.readyState === WebSocket.OPEN) currentWs.close();
    });

    tcpSocket.on('error', (err) => {
      console.error(`[Bridge:${port}] TCP error:`, err.message);
      closed = true;
      if (currentWs && currentWs.readyState === WebSocket.OPEN) currentWs.close();
      tcpSocket.destroy();
    });
  });

  server.listen(port, '127.0.0.1', () => {
    console.log(`[Bridge:${port}] Listening on 127.0.0.1:${port}`);
  });

  server.on('error', (err) => {
    console.error(`[Bridge:${port}] Server error:`, err.message);
    activeBridges.delete(port);
  });
}

startBridge(LOGIN_PORT);
JAVASCRIPT

echo
echo "[4/5] Installing the WebSocket dependency..."
npm init -y >/dev/null
npm install --no-audit --no-fund ws

echo
echo "[5/5] Starting the bridge..."
node --check bridge.js

# Replace an older session so rerunning this installer restarts the bridge.
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" "cd '$APP_DIR' && node bridge.js"

# Keep the phone awake while the bridge session is running, when supported.
if command -v termux-wake-lock >/dev/null 2>&1; then
  termux-wake-lock || true
fi

sleep 2

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "ERROR: The bridge did not stay running. Check the output above."
  exit 1
fi

echo
echo "========================================"
echo " Bridge started successfully."
echo "========================================"
echo "Folder: $APP_DIR"
echo "Session: $SESSION"
echo
echo "Attaching to the live bridge log now."
echo "Detach without stopping it: Ctrl+B, then D"
echo "Return later: tmux attach -t $SESSION"
echo "Stop it:       tmux kill-session -t $SESSION"
echo "View it:       tmux attach -t $SESSION"
echo

tmux attach-session -t "$SESSION"
