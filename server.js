'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 8080);

/*
 * ============================================================
 * ACURA DX-1000 SDR BRIDGE
 * Browser <-> Railway <-> VibeSDR / UberSDR
 * ============================================================
 *
 * Browser WebSocket:
 *   /sdr
 *
 * Browser sends frequency in MHz or Hz.
 * Bridge normalizes it to Hz before sending upstream.
 *
 * Upstream audio:
 *   VibeSDR / UberSDR V2
 *   Opus packets with V2 header
 * ============================================================
 */

const UPSTREAM =
  process.env.UBERSDR_URL ||
  process.env.VIBESDR_URL ||
  process.env.UPSTREAM_URL ||
  '';

const UPSTREAM_PASSWORD =
  process.env.UBERSDR_PASSWORD ||
  process.env.VIBESDR_PASSWORD ||
  process.env.UPSTREAM_PASSWORD ||
  '';

const UPSTREAM_UUID =
  process.env.UBERSDR_UUID ||
  process.env.VIBESDR_UUID ||
  process.env.UPSTREAM_UUID ||
  'acura-dx1000';

const DEFAULT_FREQ = 7255000;
const DEFAULT_MODE = 'lsb';

const browserClients = new Set();

let upstream = null;
let upstreamReady = false;
let upstreamConnecting = false;
let reconnectTimer = null;
let reconnectDelay = 1000;

let currentFreq = DEFAULT_FREQ;
let currentMode = DEFAULT_MODE;

let audioPackets = 0;
let audioBytes = 0;
let lastAudioAt = 0;
let lastUpstreamMessageAt = 0;


/* ============================================================
 * LOGGING
 * ============================================================
 */

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}


/* ============================================================
 * HELPERS
 * ============================================================
 */

function normalizeFrequency(value) {
  let f = Number(value);

  if (!Number.isFinite(f) || f <= 0) {
    return currentFreq;
  }

  /*
   * ACURA display normally sends MHz:
   * 7.255 -> 7,255,000 Hz
   *
   * But if browser already sends Hz, leave it alone.
   */
  if (f < 1000) {
    f *= 1000000;
  }

  return Math.round(f);
}


function normalizeMode(mode) {
  const m = String(mode || '').trim().toLowerCase();

  switch (m) {
    case 'usb':
    case 'lsb':
    case 'am':
    case 'fm':
    case 'cw':
    case 'cwu':
    case 'cwl':
      return m;

    default:
      return currentMode || DEFAULT_MODE;
  }
}


function safeSend(ws, data, options) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }

  try {
    ws.send(data, options);
    return true;
  } catch (err) {
    log('WebSocket send error:', err.message);
    return false;
  }
}


function broadcastText(obj) {
  const text =
    typeof obj === 'string'
      ? obj
      : JSON.stringify(obj);

  for (const ws of browserClients) {
    safeSend(ws, text);
  }
}


function broadcastBinary(buffer) {
  for (const ws of browserClients) {
    safeSend(ws, buffer, { binary: true });
  }
}


/* ============================================================
 * UPSTREAM URL
 * ============================================================
 */

function makeUpstreamUrl() {
  if (!UPSTREAM) {
    return '';
  }

  let url = UPSTREAM.trim();

  /*
   * Allow Railway variable to contain:
   *
   * https://host
   * http://host
   * wss://host
   * ws://host
   */

  url = url
    .replace(/^https:\/\//i, 'wss://')
    .replace(/^http:\/\//i, 'ws://');

  /*
   * VibeSDR / UberSDR local-audio protocol uses /ws/audio.
   *
   * If user already supplied /ws/audio, don't add it twice.
   */

  if (!/\/ws\/audio(?:\?|$)/i.test(url)) {
    url = url.replace(/\/+$/, '');
    url += '/ws/audio';
  }

  return url;
}


/* ============================================================
 * AUTH / INITIAL HANDSHAKE
 * ============================================================
 */

function sendUpstreamJSON(obj) {
  if (
    !upstream ||
    upstream.readyState !== WebSocket.OPEN
  ) {
    return false;
  }

  const text = JSON.stringify(obj);

  try {
    upstream.send(text);
    return true;
  } catch (err) {
    log('Upstream command send failed:', err.message);
    return false;
  }
}


function sendHandshake() {
  if (
    !upstream ||
    upstream.readyState !== WebSocket.OPEN
  ) {
    return;
  }

  /*
   * VibeSDR / UberSDR V2 hello.
   *
   * Keep fields explicit so servers that ignore unknown
   * fields can still consume the values they understand.
   */

  const hello = {
    type: 'hello',
    protocol: 2,
    version: 2,
    client: 'ACURA-DX1000',
    uuid: UPSTREAM_UUID
  };

  if (UPSTREAM_PASSWORD) {
    hello.password = UPSTREAM_PASSWORD;
    hello.auth = UPSTREAM_PASSWORD;
  }

  sendUpstreamJSON(hello);

  /*
   * Some VibeServer builds accept auth separately.
   */

  if (UPSTREAM_PASSWORD) {
    sendUpstreamJSON({
      type: 'auth',
      password: UPSTREAM_PASSWORD,
      auth: UPSTREAM_PASSWORD,
      uuid: UPSTREAM_UUID
    });
  }

  /*
   * Request audio stream.
   */

  sendUpstreamJSON({
    type: 'audio',
    action: 'start',
    protocol: 2,
    codec: 'opus'
  });

  /*
   * Send current receiver settings immediately.
   */

  setTimeout(() => {
    sendTune(currentFreq, currentMode);
  }, 100);
}


/* ============================================================
 * TUNING
 * ============================================================
 */

function sendTune(freq, mode) {
  currentFreq = normalizeFrequency(freq);
  currentMode = normalizeMode(mode);

  log(
    'UPSTREAM TUNE ->',
    currentFreq,
    'Hz',
    currentMode
  );

  if (
    !upstream ||
    upstream.readyState !== WebSocket.OPEN
  ) {
    log('Tune requested while upstream not ready');
    return false;
  }

  /*
   * VibeSDR/UberSDR control message.
   *
   * Include both "frequency" and "freq" for compatibility
   * between VibeServer builds.
   */

  const command = {
    type: 'tune',
    frequency: currentFreq,
    freq: currentFreq,
    mode: currentMode
  };

  return sendUpstreamJSON(command);
}


/* ============================================================
 * UPSTREAM CONNECTION
 * ============================================================
 */

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  const delay = reconnectDelay;

  reconnectDelay = Math.min(
    reconnectDelay * 2,
    15000
  );

  log(`Upstream reconnect scheduled in ${delay} ms`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectUpstream();
  }, delay);
}


function connectUpstream() {
  if (upstreamConnecting) {
    return;
  }

  if (
    upstream &&
    (
      upstream.readyState === WebSocket.OPEN ||
      upstream.readyState === WebSocket.CONNECTING
    )
  ) {
    return;
  }

  const url = makeUpstreamUrl();

  if (!url) {
    log('ERROR: No UberSDR/VibeSDR upstream URL configured.');
    log(
      'Set UBERSDR_URL, VIBESDR_URL, or UPSTREAM_URL in Railway Variables.'
    );

    broadcastText({
      type: 'status',
      upstream: false,
      error: 'UPSTREAM_NOT_CONFIGURED'
    });

    return;
  }

  upstreamConnecting = true;
  upstreamReady = false;

  log('Connecting to VibeSDR/UberSDR upstream...');
  log('Upstream audio socket:', url);

  const headers = {
    'User-Agent': 'ACURA-DX1000/2.0'
  };

  if (UPSTREAM_PASSWORD) {
    headers['Authorization'] =
      `Bearer ${UPSTREAM_PASSWORD}`;
  }

  let ws;

  try {
    ws = new WebSocket(url, {
      headers,
      handshakeTimeout: 10000,
      perMessageDeflate: false
    });
  } catch (err) {
    upstreamConnecting = false;

    log(
      'Unable to create upstream WebSocket:',
      err.message
    );

    scheduleReconnect();
    return;
  }

  upstream = ws;

  ws.binaryType = 'nodebuffer';


  ws.on('open', () => {
    upstreamConnecting = false;
    upstreamReady = true;
    reconnectDelay = 1000;

    log('================================');
    log('VibeSDR / UberSDR UPSTREAM OPEN');
    log('================================');

    broadcastText({
      type: 'status',
      upstream: true
    });

    sendHandshake();
  });


  ws.on('message', (data, isBinary) => {
    lastUpstreamMessageAt = Date.now();

    /*
     * ========================================================
     * BINARY = AUDIO
     * ========================================================
     */

    if (isBinary) {
      const packet = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data);

      audioPackets++;
      audioBytes += packet.length;
      lastAudioAt = Date.now();

      /*
       * IMPORTANT:
       *
       * Do NOT alter the packet.
       *
       * VibeSDR/UberSDR V2 audio contains the V2 header
       * followed by the Opus payload.
       *
       * The ACURA browser receives exactly the same packet.
       */

      broadcastBinary(packet);

      if (
        audioPackets === 1 ||
        audioPackets % 250 === 0
      ) {
        log(
          `AUDIO: ${audioPackets} packets / ${audioBytes} bytes`
        );
      }

      return;
    }


    /*
     * ========================================================
     * TEXT / CONTROL
     * ========================================================
     */

    const text = data.toString();

    log(
      'UPSTREAM TEXT:',
      text.substring(0, 500)
    );

    let msg;

    try {
      msg = JSON.parse(text);
    } catch (_) {
      /*
       * Pass non-JSON status messages to browser.
       */
      broadcastText({
        type: 'upstream',
        data: text
      });

      return;
    }


    /*
     * If server explicitly reports ready/authenticated,
     * resend tune to guarantee receiver synchronization.
     */

    const type = String(
      msg.type ||
      msg.event ||
      msg.status ||
      ''
    ).toLowerCase();

    if (
      type.includes('ready') ||
      type.includes('authenticated') ||
      type.includes('connected') ||
      type === 'hello'
    ) {
      upstreamReady = true;

      sendTune(
        currentFreq,
        currentMode
      );
    }


    /*
     * Signal meter / RSSI forwarding.
     */

    const rssi =
      msg.rssi ??
      msg.signal ??
      msg.dbm ??
      msg.smeter ??
      msg.sMeter;

    if (
      rssi !== undefined &&
      rssi !== null &&
      Number.isFinite(Number(rssi))
    ) {
      broadcastText({
        type: 'meter',
        rssi: Number(rssi),
        dbm: Number(rssi)
      });
    }


    /*
     * Forward control message to browser.
     */

    broadcastText(msg);
  });


  ws.on('ping', data => {
    try {
      ws.pong(data);
    } catch (_) {}
  });


  ws.on('close', (code, reason) => {
    const reasonText = reason
      ? reason.toString()
      : '';

    log(
      'VibeSDR/UberSDR upstream closed:',
      code,
      reasonText
    );

    upstreamReady = false;
    upstreamConnecting = false;

    if (upstream === ws) {
      upstream = null;
    }

    broadcastText({
      type: 'status',
      upstream: false
    });

    scheduleReconnect();
  });


  ws.on('error', err => {
    log(
      'VibeSDR/UberSDR upstream ERROR:',
      err.message
    );
  });


  ws.on('unexpected-response', (request, response) => {
    log(
      'UPSTREAM HTTP REJECTION:',
      response.statusCode,
      response.statusMessage || ''
    );

    let body = '';

    response.on('data', chunk => {
      body += chunk.toString();
    });

    response.on('end', () => {
      if (body) {
        log(
          'UPSTREAM RESPONSE:',
          body.substring(0, 1000)
        );
      }
    });
  });
}


/* ============================================================
 * BROWSER WEBSOCKET
 * ============================================================
 */

const browserWSS = new WebSocket.Server({
  noServer: true,
  perMessageDeflate: false
});


browserWSS.on('connection', ws => {
  browserClients.add(ws);

  log('ACURA SDR visitor connected');

  safeSend(
    ws,
    JSON.stringify({
      type: 'status',
      connected: true,
      upstream: upstreamReady,
      frequency: currentFreq,
      mode: currentMode,
      protocol: 'VibeSDR / UberSDR V2',
      audio: 'Opus + V2 header'
    })
  );

  /*
   * Make sure upstream exists whenever somebody opens radio.
   */

  connectUpstream();


  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      return;
    }

    const text = data.toString().trim();

    if (!text) {
      return;
    }

    let msg;

    try {
      msg = JSON.parse(text);
    } catch (_) {

      /*
       * Allow simple:
       *
       * 7.255
       * 7255000
       */

      const f = Number(text);

      if (Number.isFinite(f)) {
        const hz = normalizeFrequency(f);

        log(
          'ACURA TUNE REQUEST ->',
          hz,
          'Hz',
          currentMode
        );

        sendTune(hz, currentMode);
      }

      return;
    }


    const type = String(
      msg.type ||
      msg.action ||
      msg.command ||
      ''
    ).toLowerCase();


    /*
     * ========================================================
     * TUNE
     * ========================================================
     */

    if (
      type === 'tune' ||
      type === 'frequency' ||
      msg.frequency !== undefined ||
      msg.freq !== undefined ||
      msg.mhz !== undefined
    ) {
      const rawFreq =
        msg.frequency ??
        msg.freq ??
        msg.mhz;

      const mode =
        msg.mode ??
        currentMode;

      const hz =
        normalizeFrequency(rawFreq);

      const normalizedMode =
        normalizeMode(mode);

      log(
        'ACURA TUNE REQUEST ->',
        hz,
        'Hz',
        normalizedMode
      );

      sendTune(
        hz,
        normalizedMode
      );

      return;
    }


    /*
     * ========================================================
     * MODE
     * ========================================================
     */

    if (
      type === 'mode' &&
      msg.mode
    ) {
      currentMode =
        normalizeMode(msg.mode);

      log(
        'ACURA MODE REQUEST ->',
        currentMode
      );

      sendTune(
        currentFreq,
        currentMode
      );

      return;
    }


    /*
     * ========================================================
     * START / POWER ON
     * ========================================================
     */

    if (
      type === 'start' ||
      type === 'power' ||
      type === 'on' ||
      msg.power === true
    ) {
      log('ACURA RADIO POWER ON');

      connectUpstream();

      if (
        upstream &&
        upstream.readyState === WebSocket.OPEN
      ) {
        sendHandshake();
      }

      return;
    }


    /*
     * ========================================================
     * PING
     * ========================================================
     */

    if (type === 'ping') {
      safeSend(
        ws,
        JSON.stringify({
          type: 'pong',
          time: Date.now()
        })
      );

      return;
    }


    /*
     * Forward unknown JSON control message upstream.
     */

    if (
      upstream &&
      upstream.readyState === WebSocket.OPEN
    ) {
      safeSend(
        upstream,
        JSON.stringify(msg)
      );
    }
  });


  ws.on('close', () => {
    browserClients.delete(ws);

    log('ACURA SDR visitor disconnected');
  });


  ws.on('error', err => {
    browserClients.delete(ws);

    log(
      'ACURA browser socket error:',
      err.message
    );
  });
});


/* ============================================================
 * HTTP -> WEBSOCKET UPGRADE
 * ============================================================
 */

server.on('upgrade', (request, socket, head) => {
  let pathname;

  try {
    const url = new URL(
      request.url,
      `http://${request.headers.host || 'localhost'}`
    );

    pathname = url.pathname;
  } catch (_) {
    socket.destroy();
    return;
  }

  if (pathname !== '/sdr') {
    socket.destroy();
    return;
  }

  browserWSS.handleUpgrade(
    request,
    socket,
    head,
    ws => {
      browserWSS.emit(
        'connection',
        ws,
        request
      );
    }
  );
});


/* ============================================================
 * HEALTH / DEBUG
 * ============================================================
 */

app.get('/', (req, res) => {
  res.json({
    service: 'ACURA DX-1000 SDR BRIDGE',
    status: 'online',

    browserSocket: '/sdr',

    protocol:
      'VibeSDR / UberSDR V2',

    audio:
      'Opus + V2 header',

    upstreamConfigured:
      Boolean(UPSTREAM),

    upstreamConnected:
      Boolean(
        upstream &&
        upstream.readyState === WebSocket.OPEN
      ),

    upstreamReady,

    browserClients:
      browserClients.size,

    frequencyHz:
      currentFreq,

    mode:
      currentMode,

    audioPackets,

    audioBytes,

    lastAudioMsAgo:
      lastAudioAt
        ? Date.now() - lastAudioAt
        : null,

    lastUpstreamMessageMsAgo:
      lastUpstreamMessageAt
        ? Date.now() - lastUpstreamMessageAt
        : null
  });
});


app.get('/health', (req, res) => {
  res.json({
    ok: true,
    upstreamConfigured:
      Boolean(UPSTREAM),
    upstreamReady,
    audioPackets,
    browserClients:
      browserClients.size
  });
});


/* ============================================================
 * KEEPALIVE
 * ============================================================
 */

setInterval(() => {

  /*
   * Browser keepalive
   */

  for (const ws of browserClients) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.ping();
      } catch (_) {}
    }
  }


  /*
   * Upstream keepalive / reconnect
   */

  if (
    upstream &&
    upstream.readyState === WebSocket.OPEN
  ) {
    try {
      upstream.ping();
    } catch (_) {}
  } else if (
    browserClients.size > 0
  ) {
    connectUpstream();
  }

}, 15000);


/* ============================================================
 * START
 * ============================================================
 */

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('================================');
  console.log('ACURA DX-1000 SDR BRIDGE');
  console.log('================================');
  console.log(`Port: ${PORT}`);
  console.log('Browser socket: /sdr');
  console.log('Protocol: VibeSDR / UberSDR V2');
  console.log('Audio: Opus + V2 header');
  console.log(
    'ACURA MHz -> UberSDR Hz conversion: ENABLED'
  );
  console.log(
    `Upstream: ${UPSTREAM ? 'CONFIGURED' : 'NOT CONFIGURED'}`
  );
  console.log('================================');
  console.log('');

  if (UPSTREAM) {
    connectUpstream();
  }
});
