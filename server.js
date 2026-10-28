"use strict";
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
// v2.5: we load opusscript's WASM libopus core directly instead of using
// its OpusScript JS wrapper. See SafeOpusDecoder below for why.
const createOpusNative = require("opusscript/build/opusscript_native_wasm.js");

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
const CLIENT = "ACURA-DX1000/2.4";

// Defaults used only until the browser sends its first real "tune".
const DEFAULT_FREQUENCY = 7255000; // Hz
const DEFAULT_MODE = "lsb";

// The #acura-dx1000-new front-end hardcodes this and does not read a
// rate from the packet, so every packet we send it MUST already be at
// this rate or audio will play back at the wrong pitch/speed.
const OUTPUT_RATE = 12000;

// Opus is natively 48 kHz. Building the decoder at the rate in the packet
// header (which describes the STREAM, not the frames) sizes libopus's
// output buffer too small and every frame overflows it -- that was the
// v2.2 bug, "memory access out of bounds" on every packet. Always 48 kHz.
const OPUS_NATIVE_RATE = 48000;

// Don't let a decode failure flood the logs the way v2.2 did.
const DECODE_ERROR_LOG_LIMIT = 5;

/* ============================================================
   REAL SIGNAL METADATA  (new in v2.4)
   ---------------------------------------------------------
   v2.2/v2.3 estimated the S-meter from decoded audio loudness,
   because the layout of UberSDR's 8-byte metadata field wasn't
   known. It is now -- read straight out of UberSDR's own client:

       s = view.getFloat32(13, true);   // signal power
       l = view.getFloat32(17, true);   // noise density
       window.currentBasebandPower = s;
       window.currentNoiseDensity  = l;

   Two little-endian Float32s. And their S-meter maps that value
   on the standard HF scale:

       t < -121        -> S0
       t < -73         -> 1 + (t + 121) / 6      (6 dB per S-unit)
       otherwise       -> 9 + (t + 73)  / 10     (dB over S9)

   S9 = -73, which is the same scale the DX-1000 front-end uses,
   so the value drops in directly as dBm with no fudge factor.

   -999 is UberSDR's "not available" sentinel; if we ever see that
   (or a malformed packet) we fall back to the old audio-loudness
   estimate so the meter still does something sensible.
   ============================================================ */
const SIGNAL_UNAVAILABLE = -900; // anything below this is a sentinel

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
   The decoder always runs at 48 kHz; the DX-1000 front-end
   assumes every packet is 12 kHz.
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
   FALLBACK ONLY -- used if the packet carries no real signal
   metadata. This is the old v2.2/v2.3 approximation.
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
  const approx = dbfs * 2.4 - 37;
  return Math.max(-127, Math.min(0, approx));
}

function encodeRssiRaw(dbm) {
  // Inverse of the front-end's: currentRSSI = raw*0.1 - 127
  const raw = Math.round((dbm + 127) / 0.1);
  return Math.max(0, Math.min(65535, raw));
}

// Purely for the log line, so the value is readable at a glance.
function formatSUnits(dbm) {
  const s =
    dbm < -121 ? 0 :
    dbm < -73  ? 1 + (dbm + 121) / 6 :
                 9 + (dbm + 73) / 10;
  if (s < 1) return "S0";
  if (s <= 9) return "S" + Math.round(s);
  return "S9+" + Math.round(dbm + 73) + "dB";
}

/* ============================================================
   SAFE OPUS DECODER  (new in v2.5)
   ---------------------------------------------------------
   The "memory access out of bounds" error on every packet was
   NOT bad data from UberSDR -- the stream is valid Opus. It is a
   bug in opusscript 0.1.1's JS wrapper: it builds its PCM output
   view with HEAPU16.subarray(bytePointer, ...), treating a BYTE
   address as a 16-bit INDEX, and then hands libopus that view's
   byteOffset -- i.e. DOUBLE the address it actually allocated.
   While the bridge is freshly started, that doubled address still
   happens to land inside WASM memory, so audio works. Every
   listener connection allocates more memory, so after the bridge
   has run for a while the doubled address falls past the end of
   WASM memory and EVERY decode traps. Restarting only hides it.

   This class talks to the same compiled libopus directly with the
   correct addresses, and re-reads the heap views on every call so
   it also survives WASM memory growth.
   ============================================================ */
const OPUS_MAX_PACKET_BYTES = 1276 * 3; // same limits as libopus build
const OPUS_MAX_FRAME_SAMPLES = 960 * 6; // 120 ms @ 48 kHz
const OPUS_APPLICATION_AUDIO = 2049;

let opusNative = null;
function getOpusNative() {
  if (!opusNative) opusNative = createOpusNative();
  return opusNative;
}

class SafeOpusDecoder {
  constructor(sampleRate, channels) {
    const m = getOpusNative();
    this.m = m;
    this.channels = channels;
    this.handler = new m.OpusScriptHandler(sampleRate, channels, OPUS_APPLICATION_AUDIO);
    this.inPtr = m._malloc(OPUS_MAX_PACKET_BYTES);
    // The native _decode writes every sample as TWO 16-bit slots
    // (low byte, high byte), so reserve 2 x 16-bit per sample.
    this.outPtr = m._malloc(OPUS_MAX_FRAME_SAMPLES * channels * 2 * 2);
    if (!this.inPtr || !this.outPtr) {
      this.delete();
      throw new Error("Opus buffer allocation failed");
    }
  }

  // Returns interleaved Int16Array PCM (length = frames * channels).
  decode(opusBytes) {
    if (!this.handler) throw new Error("decoder already deleted");
    if (!opusBytes.length || opusBytes.length > OPUS_MAX_PACKET_BYTES) {
      throw new Error("bad Opus packet size " + opusBytes.length);
    }
    const m = this.m;
    m.HEAPU8.set(opusBytes, this.inPtr);
    const frames = this.handler._decode(this.inPtr, opusBytes.length, this.outPtr);
    if (frames < 0) throw new Error("Opus decode error " + frames);

    const n = frames * this.channels;
    const heap = m.HEAPU16;          // re-read: may change after memory growth
    const base = this.outPtr >> 1;   // byte address -> 16-bit index
    const out = new Int16Array(n);
    for (let i = 0; i < n; i++) {
      out[i] = (heap[base + 2 * i] & 0xff) | ((heap[base + 2 * i + 1] & 0xff) << 8);
    }
    return out;
  }

  delete() {
    const m = this.m;
    if (this.handler) {
      try { m.OpusScriptHandler.destroy_handler(this.handler); } catch {}
      this.handler = null;
    }
    if (this.inPtr) { try { m._free(this.inPtr); } catch {} this.inPtr = 0; }
    if (this.outPtr) { try { m._free(this.outPtr); } catch {} this.outPtr = 0; }
  }
}

/* ============================================================
   SHARED OPUS -> FRONT-END PACKET PIPELINE
   ============================================================ */
function makeAudioPipeline(label) {
  let decoder = null;
  let decoderChannels = 0;
  let decodeErrors = 0;
  let firstAudioLogged = false;
  let firstSignalLogged = false;
  let usingFallbackSignal = false;
  let destroyed = false;

  function destroy() {
    destroyed = true;
    if (decoder) {
      try { decoder.delete(); } catch {}
      decoder = null;
    }
    decoderChannels = 0;
  }

  function handlePacket(packet) {
    // Packets can still trickle in for a moment after the listener
    // leaves; don't build a new decoder that would never be freed.
    if (destroyed) return null;
    // UberSDR V2: 0-7 timestamp, 8-11 rate, 12 channels,
    // 13-16 signal Float32 LE, 17-20 noise Float32 LE, 21+ Opus.
    if (packet.length <= 21) return null;

    const headerRate = packet.readUInt32LE(8);
    const channels = packet.readUInt8(12);
    if (![8000, 12000, 16000, 24000, 48000].includes(headerRate)) return null;
    if (channels !== 1 && channels !== 2) return null;

    // --- real signal metadata ---
    let signalDbm = null;
    let noiseDbm = null;
    try {
      const s = packet.readFloatLE(13);
      const n = packet.readFloatLE(17);
      if (Number.isFinite(s) && s > SIGNAL_UNAVAILABLE) signalDbm = s;
      if (Number.isFinite(n) && n > SIGNAL_UNAVAILABLE) noiseDbm = n;
    } catch {
      signalDbm = null;
      noiseDbm = null;
    }

    if (!decoder || decoderChannels !== channels) {
      if (decoder) { try { decoder.delete(); } catch {} }
      try {
        decoder = new SafeOpusDecoder(OPUS_NATIVE_RATE, channels);
      } catch (err) {
        log(label, "OPUS DECODER CREATE FAILED:", err.message);
        decoder = null;
        return null;
      }
      decoderChannels = channels;
      log(label, "OPUS DECODER:", OPUS_NATIVE_RATE, "Hz", channels, "ch",
        "(stream header said", headerRate, "Hz)");
    }

    const opus = packet.subarray(21);

    let pcm;
    try {
      pcm = decoder.decode(opus); // interleaved Int16Array
    } catch (err) {
      decodeErrors++;
      if (decodeErrors <= DECODE_ERROR_LOG_LIMIT) {
        log(label, "OPUS DECODE ERROR:", err.message,
          decodeErrors === DECODE_ERROR_LOG_LIMIT ? "(further decode errors suppressed)" : "");
      }
      return null;
    }
    if (!pcm.length) return null;

    // Downmix to mono Int16Array
    let mono;
    if (channels === 1) {
      mono = pcm;
    } else {
      const frameCount = pcm.length / 2;
      mono = new Int16Array(frameCount);
      for (let i = 0; i < frameCount; i++) {
        mono[i] = Math.round((pcm[i * 2] + pcm[i * 2 + 1]) / 2);
      }
    }

    const resampled = resampleInt16(mono, OPUS_NATIVE_RATE, OUTPUT_RATE);

    // Real value if the receiver gave us one, estimate only if it didn't.
    let dbm;
    if (signalDbm !== null) {
      dbm = Math.max(-127, Math.min(0, signalDbm));
      if (!firstSignalLogged) {
        firstSignalLogged = true;
        log(label, "SIGNAL METADATA OK -", signalDbm.toFixed(1), "dBm",
          "(" + formatSUnits(signalDbm) + ")",
          noiseDbm !== null
            ? "noise " + noiseDbm.toFixed(1) + " dBm, SNR " + Math.max(0, signalDbm - noiseDbm).toFixed(1) + " dB"
            : "noise unavailable");
      }
    } else {
      dbm = estimateApproxDbm(resampled);
      if (!usingFallbackSignal) {
        usingFallbackSignal = true;
        log(label, "NO SIGNAL METADATA IN PACKET - falling back to audio-level estimate");
      }
    }

    const rssiRaw = encodeRssiRaw(dbm);

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

    if (!firstAudioLogged) {
      firstAudioLogged = true;
      log(label, "FIRST AUDIO DECODED AND SENT -", resampled.length,
        "samples @", OUTPUT_RATE, "Hz");
    }

    return out;
  }

  return { handlePacket, destroy };
}

/* ============================================================
   STATIC PAGES
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
   ============================================================ */
function attachBridge(browser, opts) {
  const label = opts.label;
  const fixed = opts.fixed === true;

  log(label, "BROWSER CONNECTED");

  const sessionId = uuid();
  const pipeline = makeAudioPipeline(label);
  let upstream = null;
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
    pipeline.destroy();
  }

  async function openUpstream(frequency, mode) {
    if (connecting || upstream) return;
    connecting = true;
    try {
      await registerSession(sessionId);
    } catch (err) {
      log(label, "REGISTRATION FAILED:", err.message);
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
      log(label, "OPENING UBERSDR AUDIO @", frequency, mode);
      ws = new WebSocket(url, {
        handshakeTimeout: 15000,
        perMessageDeflate: false,
        headers: { "User-Agent": CLIENT }
      });
    } catch (err) {
      log(label, "UPSTREAM WEBSOCKET CREATE FAILED:", err.message);
      sendJSON({ type: "error", message: "Could not open receiver connection: " + err.message });
      connecting = false;
      return;
    }

    upstream = ws;
    upstream.binaryType = "nodebuffer";
    connecting = false;

    upstream.on("open", () => {
      log(label, "UBERSDR AUDIO OPEN");
      sendJSON({ type: "tuned", frequency: currentFrequency, mode: currentMode });
    });

    upstream.on("message", (data, isBinary) => {
      if (!isBinary || closed) return;
      const packet = Buffer.isBuffer(data) ? data : Buffer.from(data);

      const out = pipeline.handlePacket(packet);
      if (!out) return;

      if (browser.readyState === WebSocket.OPEN) {
        browser.send(out, { binary: true });
      }
    });

    upstream.on("error", err => {
      log(label, "UBERSDR ERROR:", err.message);
      sendJSON({ type: "error", message: err.message });
    });

    upstream.on("close", (code, reason) => {
      log(label, "UBERSDR CLOSED:", code, reason ? reason.toString() : "");
      upstream = null;
    });
  }

  function safeOpenUpstream(frequency, mode) {
    openUpstream(frequency, mode).catch(err => {
      log(label, "openUpstream failed:", err && err.message ? err.message : err);
      sendJSON({ type: "error", message: "Connection failed: " + (err && err.message ? err.message : String(err)) });
      connecting = false;
    });
  }

  browser.on("message", data => {
    if (fixed) return;

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
        safeOpenUpstream(currentFrequency, currentMode);
      } else if (upstream.readyState === WebSocket.OPEN) {
        try {
          upstream.send(JSON.stringify({
            type: "tune",
            frequency: currentFrequency,
            mode: currentMode
          }));
        } catch (err) {
          log(label, "RETUNE SEND FAILED:", err.message);
        }
        sendJSON({ type: "tuned", frequency: currentFrequency, mode: currentMode });
      }
      return;
    }

    if (msg.type === "rf_gain") {
      if (upstream && upstream.readyState === WebSocket.OPEN) {
        try {
          upstream.send(JSON.stringify({ type: "rf_gain", value: msg.value }));
        } catch (err) {
          log(label, "RF_GAIN SEND FAILED:", err.message);
        }
      }
      return;
    }
  });

  browser.on("close", () => {
    log(label, "BROWSER DISCONNECTED");
    destroy();
  });

  browser.on("error", err => {
    log(label, "BROWSER SOCKET ERROR:", err.message);
  });

  if (fixed) {
    safeOpenUpstream(currentFrequency, currentMode);
  } else {
    setTimeout(() => {
      if (!closed && !upstream && !connecting) {
        safeOpenUpstream(currentFrequency, currentMode);
      }
    }, 1500);
  }
}

sdrWss.on("connection", browser => {
  attachBridge(browser, { label: "DX-1000", fixed: false });
});

testWss.on("connection", browser => {
  attachBridge(browser, { label: "TEST", fixed: true });
});

/* ============================================================
   START
   ============================================================ */
server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("==============================");
  console.log("ACURA SDR BRIDGE  -  v2.5 (fixed Opus decoder + real S-meter metadata)");
  console.log("==============================");
  console.log("Default:", (DEFAULT_FREQUENCY / 1e6).toFixed(3), "MHz", DEFAULT_MODE.toUpperCase());
  console.log("Live endpoint: /sdr");
  console.log("Opus decoder rate:", OPUS_NATIVE_RATE, "Hz -> output", OUTPUT_RATE, "Hz");
  console.log("S-meter: real signal metadata (Float32 LE @ offset 13)");
  console.log("==============================");
  console.log("");
});
