"use strict";
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
const OpusScript = require("opusscript");

// Safety net: a single bad connection or unexpected error should never take
// the whole service down. Log it and keep running instead of crashing.
process.on("unhandledRejection", reason => {
  console.error(new Date().toISOString(), "UNHANDLED REJECTION:", reason);
});
process.on("uncaughtException", err => {
  console.error(new Date().toISOString(), "UNCAUGHT EXCEPTION:", err && err.stack ? err.stack : err);
});

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 8080);
const UBERSDR_BASE =
  process.env.UBERSDR_URL ||
  process.env.UPSTREAM_URL ||
  "https://ubersdr.k3fef.com";
const PASSWORD =
  process.env.UPSTREAM_PASSWORD ||
  process.env.UBERSDR_PASSWORD ||
  "";
const CLIENT = "ACURA-DX1000/2.2";

// Defaults used only until the browser sends its first real "tune".
const DEFAULT_FREQUENCY = 7255000; // Hz
const DEFAULT_MODE = "lsb";

// The #acura-dx1000-new front-end hardcodes this and does not read a
// rate from the packet, so every packet we send it MUST already be at
// this rate or audio will play back at the wrong pitch/speed.
const OUTPUT_RATE = 12000;

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function uuid() {
  return crypto.randomUUID
    ? crypto.randomUUID()
    : crypto.randomBytes(16).toString("hex");
}

function httpBase() {
  return UBERSDR_BASE.replace(/^wss:/i, "https:")
    .replace(/^ws:/i, "http:")
    .replace(/\/+$/, "");
}

function wsBase() {
  return httpBase().replace(/^https:/i, "wss:").replace(/^http:/i, "ws:");
}

/* ============================================================
   UBERSDR SESSION REGISTRATION
   ============================================================ */
async function registerSession(sessionId) {
  const url =
    `${httpBase()}/connection?user_session_id=` + encodeURIComponent(sessionId);
  const body = { user_session_id: sessionId };
  if (PASSWORD) body.password = PASSWORD;

  log("POST /connection");
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": CLIENT,
      "X-Requested-With": "VibeSDR"
    },
    body: JSON.stringify(body)
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Registration HTTP ${response.status}: ${text}`);
  }
  let result;
  try {
    result = JSON.parse(text);
  } catch {
    result = { allowed: true };
  }
  log("Registration:", result);
  if (result.allowed === false) {
    throw new Error(result.reason || "Receiver refused connection");
  }
}

/* ============================================================
   UBERSDR AUDIO URL
   ============================================================ */
function audioUrl(sessionId, frequency, mode) {
  const params = new URLSearchParams();
  params.set("user_session_id", sessionId);
  params.set("frequency", String(frequency));
  params.set("mode", mode);
  params.set("format", "opus");
  params.set("version", "2");
  params.set("client", CLIENT);
  if (PASSWORD) params.set("password", PASSWORD);
  return `${wsBase()}/ws?${params.toString()}`;
}

/* ============================================================
   Simple linear-interpolation resampler.
   opusscript decodes at whatever rate the upstream Opus frame
   declares (8000/12000/16000/24000/48000); the DX-1000 front-end
   assumes every packet is 12kHz, so anything else must be
   converted before it goes out.
   ============================================================ */
function resampleInt16(samples, srcRate, dstRate) {
  if (srcRate === dstRate || samples.length === 0) return samples;
  const ratio = dstRate / srcRate;
  const outLen = Math.max(1, Math.round(samples.length * ratio));
  const out = new Int16Array(outLen);
  for (let i = 0; i < outLen; i++) {
    const srcPos = i / ratio;
    const i0 = Math.floor(srcPos);
    const i1 = Math.min(samples.length - 1, i0 + 1);
    const frac = srcPos - i0;
    out[i] = Math.round(
      samples[i0] * (1 - frac) + samples[i1] * frac
    );
  }
  return out;
}

/* ============================================================
   RSSI APPROXIMATION  -  READ BEFORE TRUSTING THE S-METER
   ---------------------------------------------------------
   UberSDR's V2 packet includes an 8-byte "signal metadata" field
   (offset 13-20) but its encoding isn't documented anywhere I have
   access to, so decoding a real dBm value out of it would be a
   guess dressed up as fact. Instead this estimates relative signal
   presence from the decoded audio's RMS level, which will make the
   S-meter move with real received audio but is NOT a calibrated
   RF measurement. Once this is live, compare the displayed reading
   against a known signal and we can tune the mapping below (or,
   better, figure out the real metadata layout and decode it
   properly)  -  flagging this now so it isn't mistaken for accurate.
   ============================================================ */
function estimateApproxDbm(int16Samples) {
  if (!int16Samples.length) return -127;
  let sumSq = 0;
  for (let i = 0; i < int16Samples.length; i++) {
    const s = int16Samples[i];
    sumSq += s * s;
  }
  const rms = Math.sqrt(sumSq / int16Samples.length) / 32768;
  const dbfs = rms > 0 ? 20 * Math.log10(rms) : -100;
  // Calibrated so typical received speech moves through the middle
  // and upper part of the scale instead of instantly slamming into
  // the ceiling and sticking there no matter what (the old "+55"
  // offset made almost any real speech clamp to the maximum on
  // basically every packet, which is why the S-meter looked frozen
  // whether or not anyone was talking). Still an approximation of
  // real signal strength, not a decoded RF measurement -- see the
  // note above this function.
  const approx = dbfs * 1.8 - 8;
  return Math.max(-127, Math.min(0, approx));
}

function encodeRssiRaw(dbm) {
  // Inverse of the front-end's: currentRSSI = raw*0.1 - 127
  const raw = Math.round((dbm + 127) / 0.1);
  return Math.max(0, Math.min(65535, raw));
}

/* ============================================================
   STATIC PAGES (unchanged behavior)
   ============================================================ */
app.get("/", (req, res) => {
  res.type("text/plain").send(
`ACURA SDR BRIDGE
STATUS: ONLINE

ENDPOINTS:
/sdr         - live tunable WebSocket (DX-1000 front-end)
/test-audio  - fixed-frequency test WebSocket
/test        - simple browser test page

DEFAULT FREQUENCY:
${(DEFAULT_FREQUENCY / 1e6).toFixed(3)} MHz ${DEFAULT_MODE.toUpperCase()}
`
  );
});

app.get("/test", (req, res) => {
  res.type("html").send(`<!doctype html>
<html><head><meta charset="utf-8"><title>ACURA SDR Audio Test</title></head>
<body style="background:#070b0e;color:#eee;font-family:Arial;text-align:center;padding:60px">
<h1>ACURA SDR  -  AUDIO ONLY TEST</h1>
<p>Use /sdr for the live tunable DX-1000 front-end.</p>
</body></html>`);
});

/* ============================================================
   WEBSOCKET UPGRADE ROUTING
   ---------------------------------------------------------
   ONE handler for the whole server, routing by path. (Previously
   this was two separate server.on("upgrade") listeners; the first
   one destroyed the socket for any non-/sdr path, including
   /test-audio, and the second listener then tried to use that
   already-destroyed socket  -  which throws and crashes the process.)
   ============================================================ */
const sdrWss = new WebSocket.Server({ noServer: true, perMessageDeflate: false });
const testWss = new WebSocket.Server({ noServer: true, perMessageDeflate: false });

server.on("upgrade", (req, socket, head) => {
  let path;
  try {
    path = new URL(req.url, "http://localhost").pathname;
  } catch {
    socket.destroy();
    return;
  }

  if (path === "/sdr") {
    sdrWss.handleUpgrade(req, socket, head, ws => {
      sdrWss.emit("connection", ws);
    });
    return;
  }

  if (path === "/test-audio") {
    testWss.handleUpgrade(req, socket, head, ws => {
      testWss.emit("connection", ws);
    });
    return;
  }

  socket.destroy();
});

/* ============================================================
   ONE BROWSER LISTENER = ONE UBERSDR SESSION
   Frequency/mode now come from the browser, not a constant.
   ============================================================ */
sdrWss.on("connection", browser => {
  log("DX-1000 BROWSER CONNECTED");

  const sessionId = uuid();
  let upstream = null;
  let decoder = null;
  let decoderRate = 0;
  let decoderChannels = 0;
  let connecting = false;
  let currentFrequency = DEFAULT_FREQUENCY;
  let currentMode = DEFAULT_MODE;
  let closed = false;

  function sendJSON(obj) {
    if (browser.readyState === WebSocket.OPEN) {
      browser.send(JSON.stringify(obj));
    }
  }

  function destroy() {
    closed = true;
    if (upstream) {
      try { upstream.close(); } catch {}
      upstream = null;
    }
    if (decoder) {
      try { decoder.delete(); } catch {}
      decoder = null;
    }
  }

  async function openUpstream(frequency, mode) {
    if (connecting || upstream) return;
    connecting = true;
    try {
      await registerSession(sessionId);
    } catch (err) {
      log("REGISTRATION FAILED:", err.message);
      sendJSON({ type: "error", message: "Registration failed: " + err.message });
      connecting = false;
      return;
    }
    if (closed || browser.readyState !== WebSocket.OPEN) {
      connecting = false;
      return;
    }

    let ws;
    try {
      const url = audioUrl(sessionId, frequency, mode);
      log("OPENING UBERSDR AUDIO @", frequency, mode);
      ws = new WebSocket(url, {
        handshakeTimeout: 15000,
        perMessageDeflate: false,
        headers: { "User-Agent": CLIENT }
      });
    } catch (err) {
      log("UPSTREAM WEBSOCKET CREATE FAILED:", err.message);
      sendJSON({ type: "error", message: "Could not open receiver connection: " + err.message });
      connecting = false;
      return;
    }

    upstream = ws;
    upstream.binaryType = "nodebuffer";
    connecting = false;

    upstream.on("open", () => {
      log("UBERSDR AUDIO OPEN");
      sendJSON({ type: "tuned", frequency: currentFrequency, mode: currentMode });
    });

    upstream.on("message", (data, isBinary) => {
      if (!isBinary) return;
      const packet = Buffer.isBuffer(data) ? data : Buffer.from(data);

      // UberSDR V2: 0-7 timestamp, 8-11 rate, 12 channels,
      // 13-20 signal metadata, 21+ Opus payload.
      if (packet.length <= 21) return;

      const rate = packet.readUInt32LE(8);
      const channels = packet.readUInt8(12);
      if (![8000, 12000, 16000, 24000, 48000].includes(rate)) return;
      if (channels !== 1 && channels !== 2) return;

      if (!decoder || decoderRate !== rate || decoderChannels !== channels) {
        if (decoder) { try { decoder.delete(); } catch {} }
        try {
          decoder = new OpusScript(rate, channels, OpusScript.Application.AUDIO);
        } catch (err) {
          log("OPUS DECODER CREATE FAILED:", err.message);
          decoder = null;
          return;
        }
        decoderRate = rate;
        decoderChannels = channels;
        log("OPUS DECODER:", rate, "Hz", channels, "ch");
      }

      const opus = packet.subarray(21);
      let pcm;
      try {
        pcm = Buffer.from(decoder.decode(opus));
      } catch (err) {
        log("OPUS DECODE ERROR:", err.message);
        return;
      }
      if (!pcm.length) return;

      // Downmix to mono int16 LE -> Int16Array
      const frameCount = channels === 1 ? pcm.length / 2 : pcm.length / 4;
      const mono = new Int16Array(frameCount);
      if (channels === 1) {
        for (let i = 0; i < frameCount; i++) {
          mono[i] = pcm.readInt16LE(i * 2);
        }
      } else {
        for (let i = 0; i < frameCount; i++) {
          const left = pcm.readInt16LE(i * 4);
          const right = pcm.readInt16LE(i * 4 + 2);
          mono[i] = Math.max(-32768, Math.min(32767, Math.round((left + right) / 2)));
        }
      }

      // Force everything to the fixed rate the front-end assumes.
      const resampled = resampleInt16(mono, rate, OUTPUT_RATE);

      const approxDbm = estimateApproxDbm(resampled);
      const rssiRaw = encodeRssiRaw(approxDbm);

      // Front-end packet: "SND" + 5 reserved + RSSI(u16 BE) + PCM16 BE mono
      const out = Buffer.allocUnsafe(10 + resampled.length * 2);
      out.write("SND", 0, "ascii");
      out.writeUInt8(0, 3);
      out.writeUInt8(0, 4);
      out.writeUInt8(0, 5);
      out.writeUInt8(0, 6);
      out.writeUInt8(0, 7);
      out.writeUInt16BE(rssiRaw, 8);
      for (let i = 0; i < resampled.length; i++) {
        out.writeInt16BE(resampled[i], 10 + i * 2);
      }

      if (browser.readyState === WebSocket.OPEN) {
        browser.send(out, { binary: true });
      }
    });

    upstream.on("error", err => {
      log("UBERSDR ERROR:", err.message);
      sendJSON({ type: "error", message: err.message });
    });

    upstream.on("close", (code, reason) => {
      log("UBERSDR CLOSED:", code, reason ? reason.toString() : "");
      upstream = null;
    });
  }

  function safeOpenUpstream(frequency, mode) {
    openUpstream(frequency, mode).catch(err => {
      log("openUpstream failed:", err && err.message ? err.message : err);
      sendJSON({ type: "error", message: "Connection failed: " + (err && err.message ? err.message : String(err)) });
      connecting = false;
    });
  }

  browser.on("message", data => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (msg.type === "tune") {
      const frequency = Number(msg.frequency);
      const mode = String(msg.mode || currentMode);
      if (!Number.isFinite(frequency) || frequency <= 0) return;
      currentFrequency = Math.round(frequency);
      currentMode = mode;

      if (!upstream) {
        // First tune from the browser opens the upstream session
        // at exactly the requested frequency/mode.
        safeOpenUpstream(currentFrequency, currentMode);
      } else if (upstream.readyState === WebSocket.OPEN) {
        // Already connected  -  retune the live session in place.
        try {
          upstream.send(JSON.stringify({
            type: "tune",
            frequency: currentFrequency,
            mode: currentMode
          }));
        } catch (err) {
          log("RETUNE SEND FAILED:", err.message);
        }
        sendJSON({ type: "tuned", frequency: currentFrequency, mode: currentMode });
      }
      return;
    }

    if (msg.type === "rf_gain") {
      // Forwarded best-effort  -  not confirmed the upstream honors this.
      if (upstream && upstream.readyState === WebSocket.OPEN) {
        try {
          upstream.send(JSON.stringify({ type: "rf_gain", value: msg.value }));
        } catch (err) {
          log("RF_GAIN SEND FAILED:", err.message);
        }
      }
      return;
    }
  });

  browser.on("close", () => {
    log("DX-1000 BROWSER DISCONNECTED");
    destroy();
  });

  browser.on("error", err => {
    log("BROWSER SOCKET ERROR:", err.message);
  });

  // If the browser never sends a tune (shouldn't happen with the
  // current front-end, which sends one immediately on open), fall
  // back to the default so audio still starts.
  setTimeout(() => {
    if (!closed && !upstream && !connecting) {
      safeOpenUpstream(currentFrequency, currentMode);
    }
  }, 1500);
});

/* ============================================================
   LEGACY FIXED-FREQUENCY TEST ENDPOINT (unchanged behavior, kept
   for /test  -  now correctly reachable via the single upgrade router)
   ============================================================ */
testWss.on("connection", async browser => {
  const sessionId = uuid();
  let upstream = null;
  let decoder = null;
  let decoderRate = 0;
  let decoderChannels = 0;

  function destroy() {
    if (upstream) { try { upstream.close(); } catch {} upstream = null; }
    if (decoder) { try { decoder.delete(); } catch {} decoder = null; }
  }

  try {
    await registerSession(sessionId);
  } catch (err) {
    if (browser.readyState === WebSocket.OPEN) {
      browser.send(JSON.stringify({ type: "error", message: err.message }));
    }
    return;
  }
  if (browser.readyState !== WebSocket.OPEN) return;

  try {
    upstream = new WebSocket(audioUrl(sessionId, DEFAULT_FREQUENCY, DEFAULT_MODE), {
      handshakeTimeout: 15000,
      perMessageDeflate: false,
      headers: { "User-Agent": CLIENT }
    });
  } catch (err) {
    log("TEST UPSTREAM CREATE FAILED:", err.message);
    return;
  }
  upstream.binaryType = "nodebuffer";

  upstream.on("message", (data, isBinary) => {
    if (!isBinary) return;
    const packet = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (packet.length <= 21) return;
    const rate = packet.readUInt32LE(8);
    const channels = packet.readUInt8(12);
    if (![8000, 12000, 16000, 24000, 48000].includes(rate)) return;
    if (channels !== 1 && channels !== 2) return;

    if (!decoder || decoderRate !== rate || decoderChannels !== channels) {
      if (decoder) { try { decoder.delete(); } catch {} }
      try {
        decoder = new OpusScript(rate, channels, OpusScript.Application.AUDIO);
      } catch {
        decoder = null;
        return;
      }
      decoderRate = rate;
      decoderChannels = channels;
    }
    const opus = packet.subarray(21);
    let pcm;
    try { pcm = Buffer.from(decoder.decode(opus)); } catch { return; }
    if (!pcm.length) return;

    let mono;
    if (channels === 1) {
      mono = pcm;
    } else {
      const frames = Math.floor(pcm.length / 4);
      mono = Buffer.allocUnsafe(frames * 2);
      for (let i = 0; i < frames; i++) {
        const left = pcm.readInt16LE(i * 4);
        const right = pcm.readInt16LE(i * 4 + 2);
        const mixed = Math.max(-32768, Math.min(32767, Math.round((left + right) / 2)));
        mono.writeInt16LE(mixed, i * 2);
      }
    }
    const out = Buffer.allocUnsafe(8 + mono.length);
    out.write("PCM1", 0, "ascii");
    out.writeUInt32LE(rate, 4);
    mono.copy(out, 8);
    if (browser.readyState === WebSocket.OPEN) {
      browser.send(out, { binary: true });
    }
  });

  browser.on("close", destroy);
  upstream.on("error", err => log("TEST UPSTREAM ERROR:", err.message));
  upstream.on("close", () => { upstream = null; });
});

/* ============================================================
   START
   ============================================================ */
server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("==============================");
  console.log("ACURA SDR BRIDGE  -  v2.2 (/sdr live tuning, crash-hardened)");
  console.log("==============================");
  console.log("Default:", (DEFAULT_FREQUENCY / 1e6).toFixed(3), "MHz", DEFAULT_MODE.toUpperCase());
  console.log("Live endpoint: /sdr");
  console.log("==============================");
  console.log("");
});
