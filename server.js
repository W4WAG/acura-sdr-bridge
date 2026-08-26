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
const CLIENT = "ACURA-DX1000/2.3";

// Defaults used only until the browser sends its first real "tune".
const DEFAULT_FREQUENCY = 7255000; // Hz
const DEFAULT_MODE = "lsb";

// The #acura-dx1000-new front-end hardcodes this and does not read a
// rate from the packet, so every packet we send it MUST already be at
// this rate or audio will play back at the wrong pitch/speed.
const OUTPUT_RATE = 12000;

/* ============================================================
   THE v2.2 BUG, AND WHY THIS FILE EXISTS
   ---------------------------------------------------------
   v2.2 built its Opus decoder using the sample rate read out of
   the UberSDR packet header (offset 8). That rate describes the
   STREAM, not the rate of the Opus frames themselves - Opus is
   natively 48 kHz. Constructing the decoder at, say, 12000 Hz
   sizes libopus's output buffer for 12 kHz worth of samples, and
   then a real 48 kHz frame decodes into four times that much
   audio. libopus writes past the end of the buffer and the WASM
   build aborts with:

       OPUS DECODE ERROR: memory access out of bounds

   ...on every single packet, forever, which is exactly what the
   production logs showed. Audio was arriving fine; none of it
   ever survived decoding.

   The fix: always build the decoder at OPUS_NATIVE_RATE (48000),
   which is both correct and the largest buffer Opus can need, so
   no frame can overflow it. The existing resampler then converts
   48000 -> OUTPUT_RATE for the front-end, which it was already
   doing for every other rate anyway.
   ============================================================ */
const OPUS_NATIVE_RATE = 48000;

// Don't let a decode failure flood the logs the way v2.2 did.
const DECODE_ERROR_LOG_LIMIT = 5;

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
   The decoder now always runs at 48 kHz; the DX-1000 front-end
   assumes every packet is 12 kHz, so everything is converted
   before it goes out.
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
  // Recalibrated from real on-air feedback: plain band static (no one
  // transmitting) should sit near the bottom of the scale (~S1), a
  // moderate/average received signal should land around the middle
  // (~S5), and a solid, clearly-copyable signal should read around
  // S9 -- with genuinely loud/strong audio climbing into S9+ above
  // that. Still an approximation of real signal strength, not a
  // decoded RF measurement -- see the note above this function.
  const approx = dbfs * 2.4 - 37;
  return Math.max(-127, Math.min(0, approx));
}

function encodeRssiRaw(dbm) {
  // Inverse of the front-end's: currentRSSI = raw*0.1 - 127
  const raw = Math.round((dbm + 127) / 0.1);
  return Math.max(0, Math.min(65535, raw));
}

/* ============================================================
   SHARED OPUS -> FRONT-END PACKET PIPELINE
   ---------------------------------------------------------
   Both /sdr and /test-audio used to carry their own copy of this
   logic, which is how the same decoder bug ended up duplicated in
   two places. There is now exactly one copy.
   ============================================================ */
function makeAudioPipeline(label) {
  let decoder = null;
  let decoderChannels = 0;
  let decodeErrors = 0;
  let firstAudioLogged = false;

  function destroy() {
    if (decoder) {
      try { decoder.delete(); } catch {}
      decoder = null;
    }
    decoderChannels = 0;
  }

  /**
   * Takes one raw UberSDR V2 binary packet.
   * Returns a Buffer ready to send to the front-end, or null.
   */
  function handlePacket(packet) {
    // UberSDR V2: 0-7 timestamp, 8-11 rate, 12 channels,
    // 13-20 signal metadata, 21+ Opus payload.
    if (packet.length <= 21) return null;

    const headerRate = packet.readUInt32LE(8);
    const channels = packet.readUInt8(12);
    if (![8000, 12000, 16000, 24000, 48000].includes(headerRate)) return null;
    if (channels !== 1 && channels !== 2) return null;

    // NOTE: headerRate is deliberately NOT used to build the decoder.
    // See the comment at the top of this file. Opus frames are 48 kHz.
    if (!decoder || decoderChannels !== channels) {
      if (decoder) { try { decoder.delete(); } catch {} }
      try {
        decoder = new OpusScript(OPUS_NATIVE_RATE, channels, OpusScript.Application.AUDIO);
      } catch (err) {
        log(label, "OPUS DECODER CREATE FAILED:", err.message);
        decoder = null;
        return null;
      }
      decoderChannels = channels;
      log(label, "OPUS DECODER:", OPUS_NATIVE_RATE, "Hz", channels, "ch",
        "(stream header said", headerRate, "Hz)");
    }

    // Copy the payload out of the parent buffer. subarray() returns a
    // view with a non-zero byteOffset, and handing that straight to a
    // WASM-backed decoder is a known source of trouble; a standalone
    // Buffer costs one memcpy and removes the whole class of problem.
    const opus = Buffer.from(packet.subarray(21));

    let pcm;
    try {
      pcm = Buffer.from(decoder.decode(opus));
    } catch (err) {
      decodeErrors++;
      if (decodeErrors <= DECODE_ERROR_LOG_LIMIT) {
        log(label, "OPUS DECODE ERROR:", err.message,
          decodeErrors === DECODE_ERROR_LOG_LIMIT ? "(further decode errors suppressed)" : "");
      }
      return null;
    }
    if (!pcm.length) return null;

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
    const resampled = resampleInt16(mono, OPUS_NATIVE_RATE, OUTPUT_RATE);

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
      if (!isBinary) return;
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
          log(label, "RETUNE SEND FAILED:", err.message);
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
    // /test-audio never tunes - open the default straight away.
    safeOpenUpstream(currentFrequency, currentMode);
  } else {
    // If the browser never sends a tune (shouldn't happen with the
    // current front-end, which sends one immediately on open), fall
    // back to the default so audio still starts.
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
  console.log("ACURA SDR BRIDGE  -  v2.3 (Opus decode fix)");
  console.log("==============================");
  console.log("Default:", (DEFAULT_FREQUENCY / 1e6).toFixed(3), "MHz", DEFAULT_MODE.toUpperCase());
  console.log("Live endpoint: /sdr");
  console.log("Opus decoder rate:", OPUS_NATIVE_RATE, "Hz -> output", OUTPUT_RATE, "Hz");
  console.log("==============================");
  console.log("");
});
