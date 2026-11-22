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
const PASSWORD =
  process.env.UPSTREAM_PASSWORD ||
  process.env.UBERSDR_PASSWORD ||
  "";
const CLIENT = "ACURA-DX1000/3.1";

/* ============================================================
   RECEIVERS  (new in v3.0)
   ---------------------------------------------------------
   Tried in this order. The first one is the club's main
   receiver; the rest are public UberSDR receivers used only
   when the ones above them turn us away (for example K3FEF's
   "2 users per IP" limit). All are listed in the public
   UberSDR directory (instances.ubersdr.org).
   The optional password only applies to the first receiver.
   To change the list without editing code, set the RECEIVERS
   variable in Railway to JSON like:
   [{"callsign":"K3FEF","location":"Milford, PA","url":"https://ubersdr.k3fef.com"}]
   ============================================================ */
const DEFAULT_RECEIVERS = [
  { callsign: "K3FEF", location: "Milford, PA", url: process.env.UBERSDR_URL || process.env.UPSTREAM_URL || "https://ubersdr.k3fef.com" },
  { callsign: "K3GMQ", location: "Buckingham, PA", url: "https://ubersdr.k3gmq.com" },
  { callsign: "N3NYN", location: "Clarksville, MD", url: "https://n3nyn.tunnel.ubersdr.org" },
  { callsign: "KH2R", location: "Kerhonkson, NY", url: "https://kh2r.tunnel.ubersdr.org" },
  { callsign: "K1RA", location: "Warrenton, VA", url: "https://ubersdr.k1ra.us:9443" }
];

function loadReceivers() {
  if (process.env.RECEIVERS) {
    try {
      const list = JSON.parse(process.env.RECEIVERS);
      if (Array.isArray(list) && list.length && list.every(r => r && r.url)) return list;
      console.error("RECEIVERS variable ignored: must be a JSON list with a url on each item");
    } catch (err) {
      console.error("RECEIVERS variable ignored:", err.message);
    }
  }
  return DEFAULT_RECEIVERS;
}
const RECEIVERS = loadReceivers();

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

function httpBase(receiver) {
  return String(receiver.url).replace(/^wss:/i, "https:")
    .replace(/^ws:/i, "http:")
    .replace(/\/+$/, "");
}

function wsBase(receiver) {
  return httpBase(receiver).replace(/^https:/i, "wss:").replace(/^http:/i, "ws:");
}

function receiverPassword(receiver) {
  if (receiver.password) return receiver.password;
  return receiver === RECEIVERS[0] ? PASSWORD : "";
}

/* ============================================================
   UBERSDR SESSION REGISTRATION
   ============================================================ */
async function registerSession(receiver, sessionId) {
  const url =
    `${httpBase(receiver)}/connection?user_session_id=` + encodeURIComponent(sessionId);
  const body = { user_session_id: sessionId };
  const password = receiverPassword(receiver);
  if (password) body.password = password;

  log("POST /connection", receiver.callsign || receiver.url);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  let response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": CLIENT,
        "X-Requested-With": "VibeSDR"
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
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
function audioUrl(receiver, sessionId, frequency, mode) {
  const params = new URLSearchParams();
  params.set("user_session_id", sessionId);
  params.set("frequency", String(frequency));
  params.set("mode", mode);
  params.set("format", "opus");
  params.set("version", "2");
  params.set("client", CLIENT);
  const password = receiverPassword(receiver);
  if (password) params.set("password", password);
  return `${wsBase(receiver)}/ws?${params.toString()}`;
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
   SHARED CHANNELS  (new in v3.0)
   ---------------------------------------------------------
   v2.x opened one receiver session per browser listener, so
   K3FEF's "2 users per IP" limit meant only 2 listeners for
   the whole website. Now everyone tuned to the same frequency
   and mode shares ONE receiver session (a "channel"): the
   audio is decoded once and copied to every listener on it.

   - A listener who is alone on a channel and retunes just
     retunes that session (the DX-1000 dial sends many tunes).
   - A listener who retunes away from a shared channel joins
     an existing channel on the new frequency if there is one,
     otherwise gets a new channel of their own.
   - A new channel tries each receiver in RECEIVERS order and
     uses the first one that accepts it.
   - If a receiver drops a channel that still has listeners,
     it reconnects automatically (up to MAX_RECONNECTS times).
   ============================================================ */
const MAX_RECONNECTS = 3;
const SLOW_CLIENT_BYTES = 1024 * 1024; // skip audio for a listener this far behind

const channels = new Set();
let channelCounter = 0;

function receiverInfo(receiver) {
  return receiver
    ? { callsign: receiver.callsign || "", location: receiver.location || "" }
    : null;
}

class Channel {
  constructor(frequency, mode) {
    this.id = "CH" + (++channelCounter);
    this.frequency = frequency;
    this.mode = mode;
    this.listeners = new Set();
    this.receiver = null;
    this.sessionId = null;
    this.upstream = null;
    this.upstreamOpen = false;
    this.openedFrequency = null;
    this.openedMode = null;
    this.pipeline = null;
    this.closed = false;
    this.starting = false;
    this.reconnects = 0;
    this.gotAudio = false;
    channels.add(this);
    log(this.id, "CREATED @", frequency, mode);
  }

  tunedMessage() {
    return {
      type: "tuned",
      frequency: this.frequency,
      mode: this.mode,
      receiver: receiverInfo(this.receiver),
      listeners: this.listeners.size
    };
  }

  broadcastJSON(obj) {
    for (const l of this.listeners) l.sendJSON(obj);
  }

  add(listener) {
    this.listeners.add(listener);
    listener.channel = this;
    if (this.upstreamOpen) listener.sendJSON(this.tunedMessage());
    log(this.id, "LISTENER JOINED -", this.listeners.size, "on this channel");
  }

  remove(listener) {
    this.listeners.delete(listener);
    if (listener.channel === this) listener.channel = null;
    log(this.id, "LISTENER LEFT -", this.listeners.size, "on this channel");
    if (this.listeners.size === 0) this.close("no listeners");
  }

  // Only used when this channel has exactly one listener.
  retune(frequency, mode) {
    this.frequency = frequency;
    this.mode = mode;
    if (this.upstreamOpen && this.upstream && this.upstream.readyState === WebSocket.OPEN) {
      try {
        this.upstream.send(JSON.stringify({ type: "tune", frequency, mode }));
        this.openedFrequency = frequency;
        this.openedMode = mode;
      } catch (err) {
        log(this.id, "RETUNE SEND FAILED:", err.message);
      }
      this.broadcastJSON(this.tunedMessage());
    }
    // If still connecting, the open handler sends the latest tune.
  }

  setGain(value) {
    if (this.listeners.size !== 1) return; // never let one listener change a shared channel
    if (this.upstream && this.upstream.readyState === WebSocket.OPEN) {
      try {
        this.upstream.send(JSON.stringify({ type: "rf_gain", value }));
      } catch (err) {
        log(this.id, "RF_GAIN SEND FAILED:", err.message);
      }
    }
  }

  close(why) {
    if (this.closed) return;
    this.closed = true;
    channels.delete(this);
    log(this.id, "CLOSED -", why);
    if (this.upstream) {
      try { this.upstream.close(); } catch {}
      this.upstream = null;
    }
    this.upstreamOpen = false;
    if (this.pipeline) {
      this.pipeline.destroy();
      this.pipeline = null;
    }
    for (const l of this.listeners) {
      if (l.channel === this) l.channel = null;
    }
    this.listeners.clear();
  }

  start() {
    if (this.starting || this.closed) return;
    this.starting = true;
    this.tryReceivers(0, []).catch(err => {
      log(this.id, "START FAILED:", err && err.message ? err.message : err);
      this.failAll("Connection failed: " + (err && err.message ? err.message : String(err)));
    });
  }

  failAll(message) {
    this.broadcastJSON({ type: "error", message });
    this.close("failed");
  }

  async tryReceivers(index, reasons) {
    if (this.closed) return;
    if (index >= RECEIVERS.length) {
      this.starting = false;
      this.failAll(
        "Registration failed: all receivers are busy right now (" +
        reasons.join("; ") + ")"
      );
      return;
    }
    const receiver = RECEIVERS[index];
    const sessionId = uuid();
    try {
      await registerSession(receiver, sessionId);
    } catch (err) {
      const why = (receiver.callsign || receiver.url) + ": " + err.message;
      log(this.id, "RECEIVER REFUSED -", why);
      reasons.push(why);
      return this.tryReceivers(index + 1, reasons);
    }
    if (this.closed) return;
    this.openUpstream(receiver, sessionId, index, reasons);
  }

  openUpstream(receiver, sessionId, index, reasons) {
    let ws;
    try {
      log(this.id, "OPENING", receiver.callsign || receiver.url, "AUDIO @", this.frequency, this.mode);
      ws = new WebSocket(audioUrl(receiver, sessionId, this.frequency, this.mode), {
        handshakeTimeout: 15000,
        perMessageDeflate: false,
        headers: { "User-Agent": CLIENT }
      });
    } catch (err) {
      reasons.push((receiver.callsign || receiver.url) + ": " + err.message);
      this.tryReceivers(index + 1, reasons).catch(() => {});
      return;
    }

    this.receiver = receiver;
    this.sessionId = sessionId;
    this.upstream = ws;
    this.openedFrequency = this.frequency;
    this.openedMode = this.mode;
    if (this.pipeline) this.pipeline.destroy();
    this.pipeline = makeAudioPipeline(this.id);
    ws.binaryType = "nodebuffer";

    ws.on("open", () => {
      if (this.closed || this.upstream !== ws) return;
      this.upstreamOpen = true;
      this.starting = false;
      log(this.id, "AUDIO OPEN via", receiver.callsign || receiver.url);
      // The listener may have retuned while we were connecting.
      if (this.openedFrequency !== this.frequency || this.openedMode !== this.mode) {
        try {
          ws.send(JSON.stringify({ type: "tune", frequency: this.frequency, mode: this.mode }));
          this.openedFrequency = this.frequency;
          this.openedMode = this.mode;
        } catch {}
      }
      this.broadcastJSON(this.tunedMessage());
    });

    ws.on("message", (data, isBinary) => {
      if (!isBinary || this.closed || this.upstream !== ws || !this.pipeline) return;
      const packet = Buffer.isBuffer(data) ? data : Buffer.from(data);
      const out = this.pipeline.handlePacket(packet);
      if (!out) return;
      if (!this.gotAudio) {
        this.gotAudio = true;
        this.reconnects = 0;
      }
      for (const l of this.listeners) l.sendAudio(out);
    });

    ws.on("error", err => {
      log(this.id, "UBERSDR ERROR (" + (receiver.callsign || receiver.url) + "):", err.message);
    });

    ws.on("close", (code, reason) => {
      if (this.upstream !== ws) return;
      const wasOpen = this.upstreamOpen;
      log(this.id, "UBERSDR CLOSED (" + (receiver.callsign || receiver.url) + "):", code, reason ? reason.toString() : "");
      this.upstream = null;
      this.upstreamOpen = false;
      if (this.closed) return;

      if (!wasOpen) {
        // Never got going on this receiver: try the next one.
        reasons.push((receiver.callsign || receiver.url) + ": audio connection closed (" + code + ")");
        this.tryReceivers(index + 1, reasons).catch(() => {});
        return;
      }
      // It was working and dropped (session time limit, network blip...).
      if (this.listeners.size === 0) { this.close("dropped, no listeners"); return; }
      if (this.reconnects >= MAX_RECONNECTS) {
        this.failAll("The receiver connection dropped. Please press play again.");
        return;
      }
      this.reconnects++;
      this.gotAudio = false;
      this.starting = true;
      log(this.id, "RECONNECTING, attempt", this.reconnects);
      setTimeout(() => {
        if (this.closed) return;
        this.tryReceivers(0, []).catch(err => this.failAll("Connection failed: " + err.message));
      }, 2000);
    });
  }
}

/* ============================================================
   VOICE WATCH  (new in v3.1)
   ---------------------------------------------------------
   Lights up "Talking now" on the Ocean Flight Tracker's VOICE
   boxes. While at least one web page is watching (/voice-watch),
   the relay keeps a quiet receiver session on each oceanic
   pilot-voice frequency the page asks about and watches the
   receiver's own signal-strength reading. No audio is decoded
   or sent anywhere; it only reports "someone is talking" when
   the signal rises clearly above that frequency's background.

   - Uses the BACKUP receivers only (never K3FEF), so the
     club's main receiver keeps its slots for real listeners.
   - Only frequencies on the VOICE_WATCH_ALLOWED list are
     accepted, and at most VOICE_WATCH_MAX at a time.
   - Sessions stop VOICE_WATCH_LINGER_MS after the last page
     stops watching, so nobody's receiver is tied up 24/7.
   ============================================================ */
const VOICE_WATCH_ALLOWED_KHZ = [2872, 2899, 2962, 2971, 3016, 3476, 4675, 5598, 5616, 5649, 6622,
  8825, 8831, 8864, 8879, 8891, 8906, 11279, 11309, 11336, 13291, 13306, 17946];
const VOICE_WATCH_MAX = 6;
const VOICE_WATCH_LINGER_MS = 60000;
const VOICE_ON_DB = 8;        // this far above background = talking
const VOICE_OFF_DB = 5;       // drop below this to start the "stopped" timer
const VOICE_ON_MS = 700;      // must stay up this long to count
const VOICE_OFF_MS = 3000;    // keep "talking" lit this long after it fades

const voiceWatchers = new Set();
const voiceMonitors = new Map(); // frequency -> monitor
let voiceStopTimer = null;

function voiceAllowed(frequency) {
  const khz = frequency / 1000;
  return VOICE_WATCH_ALLOWED_KHZ.some(k => Math.abs(k - khz) <= 1);
}

function voiceReceivers() {
  const backups = RECEIVERS.slice(1);
  return backups.length ? backups : RECEIVERS;
}

function voiceBroadcast(obj) {
  const text = JSON.stringify(obj);
  for (const w of voiceWatchers) {
    if (w.readyState === WebSocket.OPEN) {
      try { w.send(text); } catch {}
    }
  }
}

function voiceSnapshot() {
  return {
    type: "voice_status",
    frequencies: [...voiceMonitors.values()].map(m => ({
      frequency: m.frequency,
      active: m.active,
      receiver: receiverInfo(m.receiver),
      online: m.online
    }))
  };
}

class VoiceMonitor {
  constructor(frequency) {
    this.frequency = frequency;
    this.mode = "usb";
    this.ws = null;
    this.receiver = null;
    this.online = false;
    this.stopped = false;
    this.active = false;
    this.baseline = null;
    this.warm = [];
    this.aboveSince = 0;
    this.belowSince = 0;
    this.retryTimer = null;
    this.failures = 0;
    this.label = "VOICE " + (frequency / 1000) + "kHz";
    this.connect(0);
  }

  setActive(active) {
    if (this.active === active) return;
    this.active = active;
    log(this.label, active ? "TALKING" : "quiet");
    voiceBroadcast({ type: "voice", frequency: this.frequency, active });
  }

  onSignal(dbm) {
    const now = Date.now();
    if (this.baseline === null) {
      this.warm.push(dbm);
      if (this.warm.length >= 100) { // ~2 s of readings
        const sorted = this.warm.slice().sort((a, b) => a - b);
        this.baseline = sorted[Math.floor(sorted.length * 0.3)];
        this.warm = [];
      }
      return;
    }
    const over = dbm - this.baseline;
    // Background follows the quiet level; barely moves while someone talks.
    this.baseline += (over < VOICE_OFF_DB ? 0.01 : 0.0005) * over;

    if (over >= VOICE_ON_DB) {
      if (!this.aboveSince) this.aboveSince = now;
      this.belowSince = 0;
      if (!this.active && now - this.aboveSince >= VOICE_ON_MS) this.setActive(true);
    } else {
      this.aboveSince = 0;
      if (over < VOICE_OFF_DB) {
        if (!this.belowSince) this.belowSince = now;
        if (this.active && now - this.belowSince >= VOICE_OFF_MS) this.setActive(false);
      }
    }
  }

  async connect(index) {
    if (this.stopped) return;
    const list = voiceReceivers();
    if (index >= list.length) {
      this.failures++;
      const wait = Math.min(60000, 5000 * this.failures);
      log(this.label, "no receiver accepted, retrying in", wait / 1000, "s");
      this.retryTimer = setTimeout(() => this.connect(0), wait);
      return;
    }
    const receiver = list[index];
    const sessionId = uuid();
    try {
      await registerSession(receiver, sessionId);
    } catch (err) {
      log(this.label, "receiver refused -", (receiver.callsign || receiver.url) + ":", err.message);
      return this.connect(index + 1);
    }
    if (this.stopped) return;
    let ws;
    try {
      ws = new WebSocket(audioUrl(receiver, sessionId, this.frequency, this.mode), {
        handshakeTimeout: 15000,
        perMessageDeflate: false,
        headers: { "User-Agent": CLIENT }
      });
    } catch (err) {
      return this.connect(index + 1);
    }
    this.ws = ws;
    this.receiver = receiver;
    ws.binaryType = "nodebuffer";
    let opened = false;
    ws.on("open", () => {
      if (this.ws !== ws || this.stopped) return;
      opened = true;
      this.online = true;
      this.failures = 0;
      log(this.label, "watching via", receiver.callsign || receiver.url);
      voiceBroadcast(voiceSnapshot());
    });
    ws.on("message", (data, isBinary) => {
      if (!isBinary || this.ws !== ws) return;
      const packet = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (packet.length <= 21) return;
      let s;
      try { s = packet.readFloatLE(13); } catch { return; }
      if (Number.isFinite(s) && s > SIGNAL_UNAVAILABLE) this.onSignal(s);
    });
    ws.on("error", err => log(this.label, "error:", err.message));
    ws.on("close", code => {
      if (this.ws !== ws) return;
      this.ws = null;
      this.online = false;
      this.setActive(false);
      this.baseline = null;
      if (this.stopped) return;
      log(this.label, "receiver closed", code, "- reconnecting");
      voiceBroadcast(voiceSnapshot());
      this.retryTimer = setTimeout(() => this.connect(opened ? 0 : index + 1), opened ? 3000 : 500);
    });
  }

  stop() {
    this.stopped = true;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.ws) { try { this.ws.close(); } catch {} this.ws = null; }
    this.online = false;
    log(this.label, "stopped");
  }
}

function voiceWant(frequencies) {
  if (voiceStopTimer) { clearTimeout(voiceStopTimer); voiceStopTimer = null; }
  for (const f of frequencies) {
    if (voiceMonitors.size >= VOICE_WATCH_MAX) break;
    if (!voiceMonitors.has(f)) voiceMonitors.set(f, new VoiceMonitor(f));
  }
}

function voiceMaybeStop() {
  if (voiceWatchers.size > 0 || voiceStopTimer) return;
  voiceStopTimer = setTimeout(() => {
    voiceStopTimer = null;
    if (voiceWatchers.size > 0) return;
    for (const m of voiceMonitors.values()) m.stop();
    voiceMonitors.clear();
    log("VOICE WATCH: no pages watching, all monitors stopped");
  }, VOICE_WATCH_LINGER_MS);
}

function attachVoiceWatcher(browser) {
  voiceWatchers.add(browser);
  log("VOICE WATCH: page connected -", voiceWatchers.size, "watching");
  browser.on("message", data => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (msg.type !== "watch" || !Array.isArray(msg.frequencies)) return;
    const wanted = [...new Set(
      msg.frequencies.map(Number)
        .filter(f => Number.isFinite(f) && f > 0)
        .map(f => Math.round(f))
        .filter(voiceAllowed)
    )].slice(0, VOICE_WATCH_MAX);
    voiceWant(wanted);
    try { browser.send(JSON.stringify(voiceSnapshot())); } catch {}
  });
  browser.on("close", () => {
    voiceWatchers.delete(browser);
    log("VOICE WATCH: page left -", voiceWatchers.size, "watching");
    voiceMaybeStop();
  });
  browser.on("error", () => {});
}

function findChannel(frequency, mode, except) {
  for (const ch of channels) {
    if (ch !== except && !ch.closed && ch.frequency === frequency && ch.mode === mode) return ch;
  }
  return null;
}

/* ============================================================
   STATIC PAGES
   ============================================================ */
app.get("/", (req, res) => {
  let listeners = 0;
  for (const ch of channels) listeners += ch.listeners.size;
  res.type("text/plain").send(
`ACURA SDR BRIDGE v3.1
STATUS: ONLINE

ENDPOINTS:
/sdr         - live tunable WebSocket (DX-1000 front-end)
/test-audio  - fixed-frequency test WebSocket
/test        - simple browser test page
/status      - what is playing right now (JSON)
/voice-watch - "talking now" alerts for oceanic voice frequencies

NOW: ${listeners} listener(s) on ${channels.size} receiver session(s)
VOICE WATCH: ${voiceWatchers.size} page(s), ${voiceMonitors.size} frequency monitor(s)

RECEIVERS (tried in this order):
${RECEIVERS.map((r, i) => `${i + 1}. ${r.callsign || ""} ${r.location || ""}`.trim()).join("\n")}

DEFAULT FREQUENCY:
${(DEFAULT_FREQUENCY / 1e6).toFixed(3)} MHz ${DEFAULT_MODE.toUpperCase()}
`
  );
});

app.get("/status", (req, res) => {
  res.json({
    version: "3.1",
    voiceWatch: voiceSnapshot().frequencies,
    channels: [...channels].map(ch => ({
      id: ch.id,
      frequency: ch.frequency,
      mode: ch.mode,
      listeners: ch.listeners.size,
      receiver: receiverInfo(ch.receiver),
      open: ch.upstreamOpen
    }))
  });
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
const voiceWss = new WebSocket.Server({ noServer: true, perMessageDeflate: false });

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

  if (path === "/voice-watch") {
    voiceWss.handleUpgrade(req, socket, head, ws => {
      voiceWss.emit("connection", ws);
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
   ONE BROWSER LISTENER  ->  JOINS A SHARED CHANNEL
   ============================================================ */
function attachBridge(browser, opts) {
  const label = opts.label;
  const fixed = opts.fixed === true;
  let closed = false;

  log(label, "BROWSER CONNECTED");

  const listener = {
    channel: null,
    sendJSON(obj) {
      if (browser.readyState === WebSocket.OPEN) {
        try { browser.send(JSON.stringify(obj)); } catch {}
      }
    },
    sendAudio(buf) {
      if (browser.readyState !== WebSocket.OPEN) return;
      if (browser.bufferedAmount > SLOW_CLIENT_BYTES) return; // slow connection: drop, don't pile up
      try { browser.send(buf, { binary: true }); } catch {}
    }
  };

  function tuneTo(frequency, mode) {
    if (closed) return;
    const current = listener.channel;

    // Already there.
    if (current && !current.closed && current.frequency === frequency && current.mode === mode) {
      if (current.upstreamOpen) listener.sendJSON(current.tunedMessage());
      return;
    }

    // Someone is already listening there: share it.
    const existing = findChannel(frequency, mode, current);
    if (existing) {
      if (current) current.remove(listener);
      existing.add(listener);
      return;
    }

    // Alone on our channel: just retune it (no new receiver session).
    if (current && !current.closed && current.listeners.size === 1) {
      current.retune(frequency, mode);
      return;
    }

    // Leave the shared channel and start our own.
    if (current) current.remove(listener);
    const ch = new Channel(frequency, mode);
    ch.add(listener);
    ch.start();
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
      if (!Number.isFinite(frequency) || frequency <= 0) return;
      const mode = String(msg.mode || (listener.channel ? listener.channel.mode : DEFAULT_MODE)).toLowerCase();
      tuneTo(Math.round(frequency), mode);
      return;
    }

    if (msg.type === "rf_gain") {
      if (listener.channel) listener.channel.setGain(msg.value);
      return;
    }
  });

  browser.on("close", () => {
    closed = true;
    log(label, "BROWSER DISCONNECTED");
    if (listener.channel) listener.channel.remove(listener);
  });

  browser.on("error", err => {
    log(label, "BROWSER SOCKET ERROR:", err.message);
  });

  if (fixed) {
    tuneTo(DEFAULT_FREQUENCY, DEFAULT_MODE);
  } else {
    setTimeout(() => {
      if (!closed && !listener.channel) tuneTo(DEFAULT_FREQUENCY, DEFAULT_MODE);
    }, 1500);
  }
}

sdrWss.on("connection", browser => {
  attachBridge(browser, { label: "DX-1000", fixed: false });
});

testWss.on("connection", browser => {
  attachBridge(browser, { label: "TEST", fixed: true });
});

voiceWss.on("connection", browser => {
  attachVoiceWatcher(browser);
});

/* ============================================================
   START
   ============================================================ */
server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("==============================");
  console.log("ACURA SDR BRIDGE  -  v3.1 (shared channels + backup receivers + voice watch)");
  console.log("==============================");
  console.log("Default:", (DEFAULT_FREQUENCY / 1e6).toFixed(3), "MHz", DEFAULT_MODE.toUpperCase());
  console.log("Live endpoint: /sdr   Status: /status");
  console.log("Receivers:", RECEIVERS.map(r => r.callsign || r.url).join(" -> "));
  console.log("Opus decoder rate:", OPUS_NATIVE_RATE, "Hz -> output", OUTPUT_RATE, "Hz");
  console.log("==============================");
  console.log("");
});
