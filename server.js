"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
const OpusScript = require("opusscript");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 8080);

const UPSTREAM_BASE =
  process.env.UPSTREAM_BASE ||
  process.env.UBERSDR_BASE ||
  process.env.UPSTREAM_URL ||
  process.env.UBERSDR_URL ||
  "https://ubersdr.k3fef.com";

const UPSTREAM_PASSWORD =
  process.env.UPSTREAM_PASSWORD ||
  process.env.UBERSDR_PASSWORD ||
  "";

const ADMIN_SUFFIX =
  process.env.ADMIN_SUFFIX ||
  process.env.UBERSDR_ADMIN_SUFFIX ||
  "";

const CLIENT_NAME = "VibeSDR/10 (+https://vibesdr.net)";

const DEFAULT_FREQ = 7255000;
const DEFAULT_MODE = "lsb";

/*
 * ACURA'S EXISTING WEBPAGE EXPECTS:
 *
 * bytes 0..7  = miscellaneous/header
 * bytes 8..9  = RSSI, unsigned BE, interpreted:
 *
 *               rssi = raw * 0.1 - 127
 *
 * bytes 10+   = SIGNED PCM16 BIG-ENDIAN
 *
 * Therefore this bridge converts:
 *
 * UberSDR V2
 * 21-byte header + OPUS
 *
 *              ↓
 *
 * decoded PCM16
 *
 *              ↓
 *
 * ACURA's existing 10-byte packet format
 *
 * NO WordPress audio decoder change required.
 */


/* ============================================================
   LOGGING
   ============================================================ */

function log(...args) {
  console.log(
    new Date().toISOString(),
    ...args
  );
}


/* ============================================================
   HELPERS
   ============================================================ */

function makeUuid() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return crypto
    .randomBytes(16)
    .toString("hex");
}


function normalizeFrequency(value) {
  let f = Number(value);

  if (!Number.isFinite(f) || f <= 0) {
    return DEFAULT_FREQ;
  }

  if (f < 1000) {
    f *= 1000000;
  } else if (f < 1000000) {
    f *= 1000;
  }

  return Math.round(f);
}


function normalizeMode(value) {
  let mode =
    String(value || DEFAULT_MODE)
      .toLowerCase();

  if (mode === "cw") {
    mode = "cwu";
  }

  const allowed = new Set([
    "usb",
    "lsb",
    "am",
    "sam",
    "fm",
    "nfm",
    "cwu",
    "cwl"
  ]);

  return allowed.has(mode)
    ? mode
    : DEFAULT_MODE;
}


function httpBase() {
  let base =
    String(UPSTREAM_BASE || "")
      .trim();

  base =
    base.replace(/^ws:/i, "http:");

  base =
    base.replace(/^wss:/i, "https:");

  return base.replace(/\/+$/, "");
}


function wsBase() {
  const base = httpBase();

  if (base.startsWith("https://")) {
    return "wss://" + base.slice(8);
  }

  if (base.startsWith("http://")) {
    return "ws://" + base.slice(7);
  }

  return "wss://" + base;
}


function adminQuery() {
  if (!ADMIN_SUFFIX) {
    return "";
  }

  return ADMIN_SUFFIX.startsWith("&")
    ? ADMIN_SUFFIX
    : "&" + ADMIN_SUFFIX;
}


/* ============================================================
   SESSION REGISTRATION
   ============================================================ */

async function registerSession(uuid) {
  const url =
    `${httpBase()}/connection?user_session_id=` +
    encodeURIComponent(uuid) +
    adminQuery();

  log("REGISTERING UBERSDR SESSION");
  log("POST /connection uuid=" + uuid);

  const body = {
    user_session_id: uuid
  };

  if (UPSTREAM_PASSWORD) {
    body.password = UPSTREAM_PASSWORD;
  }

  const response =
    await fetch(url, {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
        "User-Agent": CLIENT_NAME,
        "X-Requested-With": "VibeSDR"
      },

      body: JSON.stringify(body)
    });

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `POST /connection HTTP ${response.status}: ` +
      text.slice(0, 300)
    );
  }

  let result = {};

  try {
    result = JSON.parse(text);
  } catch {
    result = { allowed: true };
  }

  log(
    "CONNECTION RESPONSE:",
    JSON.stringify(result).slice(0, 800)
  );

  if (result.allowed === false) {
    throw new Error(
      "UberSDR refused session: " +
      (result.reason || "unknown reason")
    );
  }

  return result;
}


/* ============================================================
   UBERSDR AUDIO URL
   ============================================================ */

function buildAudioUrl(uuid, frequency, mode) {
  const params =
    new URLSearchParams();

  params.set(
    "user_session_id",
    uuid
  );

  params.set(
    "frequency",
    String(frequency)
  );

  params.set(
    "mode",
    mode
  );

  params.set(
    "format",
    "opus"
  );

  params.set(
    "version",
    "2"
  );

  params.set(
    "client",
    CLIENT_NAME
  );

  if (UPSTREAM_PASSWORD) {
    params.set(
      "password",
      UPSTREAM_PASSWORD
    );
  }

  let url =
    `${wsBase()}/ws?${params.toString()}`;

  if (ADMIN_SUFFIX) {
    url += adminQuery();
  }

  return url;
}


/* ============================================================
   SIGNAL CONVERSION
   ============================================================ */

function powerToDbm(basebandPower) {
  let p = Number(basebandPower);

  /*
   * UberSDR's baseband power may already resemble
   * a negative dB value.
   */

  if (
    Number.isFinite(p) &&
    p <= 0 &&
    p >= -160
  ) {
    return p;
  }

  /*
   * If supplied as linear power, convert to dB.
   */

  if (
    Number.isFinite(p) &&
    p > 0
  ) {
    const db =
      10 * Math.log10(p);

    if (Number.isFinite(db)) {
      return Math.max(
        -127,
        Math.min(-10, db)
      );
    }
  }

  /*
   * Safe receiver-floor fallback.
   */

  return -100;
}


function rssiToRaw(rssi) {
  /*
   * Existing WordPress equation:
   *
   * currentRSSI =
   *     raw * 0.1 - 127
   *
   * Reverse it here.
   */

  let raw =
    Math.round(
      (rssi + 127) * 10
    );

  raw =
    Math.max(
      0,
      Math.min(65535, raw)
    );

  return raw;
}


/* ============================================================
   PCM CONVERSION FOR EXISTING ACURA WEBPAGE
   ============================================================ */

function makeAcuraPacket(
  pcmLE,
  rssi
) {
  /*
   * Existing webpage expects:
   *
   * 10-byte header
   * followed by PCM16 BIG-ENDIAN.
   */

  const samples =
    Math.floor(
      pcmLE.length / 2
    );

  const packet =
    Buffer.allocUnsafe(
      10 + samples * 2
    );

  /*
   * Header.
   */

  packet.fill(
    0,
    0,
    10
  );

  /*
   * Put RSSI where current WordPress code
   * already expects it: bytes 8-9.
   */

  const raw =
    rssiToRaw(rssi);

  packet.writeUInt16BE(
    raw,
    8
  );

  /*
   * opusscript produces PCM16 LE.
   *
   * Current ACURA browser decoder reads
   * PCM16 BIG-ENDIAN, therefore swap bytes.
   */

  for (
    let i = 0;
    i < samples;
    i++
  ) {
    const sample =
      pcmLE.readInt16LE(
        i * 2
      );

    packet.writeInt16BE(
      sample,
      10 + i * 2
    );
  }

  return packet;
}


/* ============================================================
   BROWSER WEBSOCKET
   ============================================================ */

const browserWss =
  new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false
  });


server.on(
  "upgrade",
  (req, socket, head) => {

    let pathname;

    try {
      pathname =
        new URL(
          req.url,
          `http://${req.headers.host || "localhost"}`
        ).pathname;
    }

    catch {
      socket.destroy();
      return;
    }


    if (pathname !== "/sdr") {
      socket.destroy();
      return;
    }


    browserWss.handleUpgrade(
      req,
      socket,
      head,
      ws => {

        browserWss.emit(
          "connection",
          ws,
          req
        );

      }
    );

  }
);


/* ============================================================
   ACURA CLIENT
   ============================================================ */

browserWss.on(
  "connection",
  browser => {

    log(
      "ACURA SDR visitor connected"
    );


    let upstream = null;

    let closed = false;

    let connecting = false;

    let reconnectTimer = null;

    let frequency =
      DEFAULT_FREQ;

    let mode =
      DEFAULT_MODE;

    const uuid =
      makeUuid();


    /*
     * Decoder gets created after first packet because
     * V2 header tells us the actual sample rate/channels.
     */

    let opusDecoder = null;

    let decoderRate = 0;

    let decoderChannels = 0;

    let packetCount = 0;

    let lastRssi = -100;


    function sendBrowser(obj) {
      if (
        browser.readyState !==
        WebSocket.OPEN
      ) {
        return;
      }

      try {
        browser.send(
          JSON.stringify(obj)
        );
      }

      catch {}
    }


    /* ========================================================
       OPUS DECODER
       ======================================================== */

    function ensureDecoder(
      sampleRate,
      channels
    ) {

      if (
        opusDecoder &&
        decoderRate === sampleRate &&
        decoderChannels === channels
      ) {
        return;
      }


      if (opusDecoder) {
        try {
          opusDecoder.delete();
        } catch {}

        opusDecoder = null;
      }


      log(
        "CREATING OPUS DECODER:",
        sampleRate,
        "Hz",
        channels,
        "channel(s)"
      );


      opusDecoder =
        new OpusScript(
          sampleRate,
          channels,
          OpusScript.Application.AUDIO
        );


      decoderRate =
        sampleRate;

      decoderChannels =
        channels;
    }


    /* ========================================================
       LIVE TUNING
       ======================================================== */

    function sendTune() {
      if (
        !upstream ||
        upstream.readyState !==
        WebSocket.OPEN
      ) {

        log(
          "Tune queued until upstream is ready"
        );

        connectUpstream();

        return;
      }


      const command = {
        type: "tune",
        frequency,
        mode
      };


      try {
        upstream.send(
          JSON.stringify(command)
        );

        log(
          "LIVE TUNE SENT ->",
          frequency,
          "Hz",
          mode
        );
      }

      catch (err) {

        log(
          "TUNE SEND ERROR:",
          err.message
        );

      }
    }


    /* ========================================================
       RECONNECT
       ======================================================== */

    function scheduleReconnect() {
      if (
        closed ||
        reconnectTimer
      ) {
        return;
      }


      reconnectTimer =
        setTimeout(
          () => {

            reconnectTimer =
              null;

            if (!closed) {
              connectUpstream();
            }

          },
          2000
        );
    }


    /* ========================================================
       CONNECT UBERSDR
       ======================================================== */

    async function connectUpstream() {

      if (
        closed ||
        connecting
      ) {
        return;
      }


      if (
        upstream &&
        (
          upstream.readyState ===
            WebSocket.OPEN ||

          upstream.readyState ===
            WebSocket.CONNECTING
        )
      ) {
        return;
      }


      connecting = true;


      log(
        "================================"
      );

      log(
        "STARTING UBERSDR CONNECTION"
      );

      log(
        "Frequency:",
        frequency,
        "Hz"
      );

      log(
        "Mode:",
        mode
      );

      log(
        "UUID:",
        uuid
      );

      log(
        "================================"
      );


      /*
       * STEP 1:
       * REGISTER SESSION.
       */

      try {

        await registerSession(
          uuid
        );


        log(
          "UBERSDR SESSION REGISTERED"
        );

      }

      catch (err) {

        connecting = false;


        log(
          "SESSION REGISTRATION FAILED:",
          err.message
        );


        sendBrowser({
          type: "error",
          stage: "registration",
          message: err.message
        });


        scheduleReconnect();

        return;
      }


      if (closed) {
        connecting = false;
        return;
      }


      /*
       * STEP 2:
       * OPEN OPUS AUDIO SOCKET.
       */

      const audioUrl =
        buildAudioUrl(
          uuid,
          frequency,
          mode
        );


      log(
        "OPENING UBERSDR OPUS AUDIO SOCKET"
      );


      let ws;


      try {

        ws =
          new WebSocket(
            audioUrl,
            {
              handshakeTimeout:
                15000,

              perMessageDeflate:
                false,

              headers: {
                "User-Agent":
                  CLIENT_NAME
              }
            }
          );

      }

      catch (err) {

        connecting = false;


        log(
          "WS CREATE ERROR:",
          err.message
        );


        scheduleReconnect();

        return;
      }


      upstream = ws;

      ws.binaryType =
        "nodebuffer";


      /* ------------------------------------------------------
         OPEN
         ------------------------------------------------------ */

      ws.on(
        "open",
        () => {

          if (
            closed ||
            upstream !== ws
          ) {
            return;
          }


          connecting = false;


          log(
            "================================"
          );

          log(
            "UBERSDR AUDIO SOCKET OPEN"
          );

          log(
            "================================"
          );


          sendBrowser({
            type: "upstream",
            status: "connected",
            frequency,
            mode
          });

        }
      );


      /* ------------------------------------------------------
         RECEIVE UBERSDR DATA
         ------------------------------------------------------ */

      ws.on(
        "message",
        (data, isBinary) => {

          if (
            closed ||
            upstream !== ws
          ) {
            return;
          }


          /*
           * TEXT MESSAGE
           */

          if (!isBinary) {

            const text =
              data.toString();


            log(
              "UBERSDR TEXT:",
              text.slice(0, 500)
            );


            return;
          }


          /*
           * V2 audio packet must contain:
           *
           * 21-byte header
           * + Opus payload
           */

          const packet =
            Buffer.isBuffer(data)
              ? data
              : Buffer.from(data);


          if (
            packet.length <= 21
          ) {
            return;
          }


          let sampleRate;

          let channels;

          let basebandPower;

          let noiseDensity;


          try {

            sampleRate =
              packet.readUInt32LE(8);

            channels =
              packet.readUInt8(12);

            basebandPower =
              packet.readFloatLE(13);

            noiseDensity =
              packet.readFloatLE(17);

          }

          catch (err) {

            log(
              "V2 HEADER READ ERROR:",
              err.message
            );

            return;
          }


          /*
           * Validate audio format.
           */

          if (
            ![
              8000,
              12000,
              16000,
              24000,
              48000
            ].includes(sampleRate)
          ) {

            log(
              "UNSUPPORTED SAMPLE RATE:",
              sampleRate
            );

            return;
          }


          if (
            channels !== 1 &&
            channels !== 2
          ) {

            log(
              "UNSUPPORTED CHANNEL COUNT:",
              channels
            );

            return;
          }


          /*
           * OPUS PAYLOAD STARTS AT BYTE 21.
           */

          const opus =
            packet.subarray(21);


          try {

            ensureDecoder(
              sampleRate,
              channels
            );


            const pcm =
              Buffer.from(
                opusDecoder.decode(
                  opus
                )
              );


            if (!pcm.length) {
              return;
            }


            /*
             * Calculate S-meter signal.
             */

            lastRssi =
              powerToDbm(
                basebandPower
              );


            /*
             * Convert to the exact packet format
             * that the CURRENT ACURA webpage
             * already knows how to play.
             */

            const acuraPacket =
              makeAcuraPacket(
                pcm,
                lastRssi
              );


            /*
             * SEND PCM TO THE WEBPAGE.
             */

            if (
              browser.readyState ===
              WebSocket.OPEN
            ) {

              browser.send(
                acuraPacket,
                {
                  binary: true
                }
              );

            }


            packetCount++;


            if (
              packetCount === 1
            ) {

              log(
                "================================"
              );

              log(
                "FIRST OPUS PACKET DECODED TO PCM"
              );

              log(
                "Input:",
                sampleRate,
                "Hz",
                channels,
                "channel(s)"
              );

              log(
                "Opus bytes:",
                opus.length
              );

              log(
                "PCM bytes:",
                pcm.length
              );

              log(
                "ACURA packet:",
                acuraPacket.length
              );

              log(
                "================================"
              );


              /*
               * Reassert VFO once actual
               * receiver audio is flowing.
               */

              sendTune();

            }


            if (
              packetCount <= 3 ||
              packetCount % 500 === 0
            ) {

              log(
                `PCM AUDIO PACKET #${packetCount}`,
                `${pcm.length} bytes`
              );

            }

          }

          catch (err) {

            log(
              "OPUS DECODE ERROR:",
              err.message
            );

          }

        }
      );


      /* ------------------------------------------------------
         ERROR
         ------------------------------------------------------ */

      ws.on(
        "error",
        err => {

          log(
            "UBERSDR WS ERROR:",
            err.message
          );

        }
      );


      /* ------------------------------------------------------
         CLOSE
         ------------------------------------------------------ */

      ws.on(
        "close",
        (code, reason) => {

          if (
            upstream !== ws
          ) {
            return;
          }


          connecting = false;

          upstream = null;


          log(
            "UBERSDR WS CLOSED:",
            code,
            reason
              ? reason.toString()
              : ""
          );


          sendBrowser({
            type: "upstream",
            status: "disconnected",
            code
          });


          if (!closed) {
            scheduleReconnect();
          }

        }
      );


      ws.on(
        "ping",
        data => {

          try {
            ws.pong(data);
          }

          catch {}

        }
      );

    }


    /* ========================================================
       COMMANDS FROM ACURA
       ======================================================== */

    browser.on(
      "message",
      (raw, isBinary) => {

        if (isBinary) {
          return;
        }


        let msg;


        try {

          msg =
            JSON.parse(
              raw.toString()
            );

        }

        catch {
          return;
        }


        /*
         * POWER
         */

        if (
          msg.type === "connect" ||
          msg.type === "start" ||
          msg.type === "power"
        ) {

          if (
            msg.frequency !== undefined
          ) {

            frequency =
              normalizeFrequency(
                msg.frequency
              );

          }


          if (msg.mode) {

            mode =
              normalizeMode(
                msg.mode
              );

          }


          connectUpstream();

          return;
        }


        /*
         * TUNE
         */

        if (
          msg.type === "tune" ||
          msg.type === "frequency" ||
          msg.type === "setFrequency"
        ) {

          const rawFreq =
            msg.frequency ??
            msg.freq ??
            msg.value;


          if (
            rawFreq !== undefined
          ) {

            frequency =
              normalizeFrequency(
                rawFreq
              );

          }


          if (msg.mode) {

            mode =
              normalizeMode(
                msg.mode
              );

          }


          log(
            "ACURA TUNE REQUEST ->",
            frequency,
            "Hz",
            mode
          );


          sendTune();

          return;
        }


        /*
         * MODE
         */

        if (
          msg.type === "mode"
        ) {

          mode =
            normalizeMode(
              msg.mode
            );


          sendTune();

          return;
        }


        /*
         * OTHER UBERSDR CONTROL MESSAGES
         */

        if (
          upstream &&
          upstream.readyState ===
            WebSocket.OPEN
        ) {

          try {

            upstream.send(
              JSON.stringify(msg)
            );

          }

          catch {}

        }

      }
    );


    /* ========================================================
       DISCONNECT
       ======================================================== */

    browser.on(
      "close",
      () => {

        closed = true;


        if (reconnectTimer) {
          clearTimeout(
            reconnectTimer
          );
        }


        if (upstream) {

          try {
            upstream.close();
          }

          catch {}


          upstream = null;
        }


        if (opusDecoder) {

          try {
            opusDecoder.delete();
          }

          catch {}


          opusDecoder = null;
        }


        log(
          "ACURA SDR visitor disconnected"
        );

      }
    );


    sendBrowser({
      type: "bridge",
      status: "ready",
      protocol:
        "UberSDR-V2-to-ACURA-PCM"
    });


    /*
     * Start immediately.
     */

    connectUpstream();

  }
);


/* ============================================================
   STATUS
   ============================================================ */

app.get(
  "/",
  (req, res) => {

    res
      .type("text/plain")
      .send(
`ACURA DX-1000 SDR BRIDGE
STATUS: ONLINE

UPSTREAM:
UberSDR V2 OPUS

DOWNSTREAM:
ACURA PCM16

SESSION:
POST /connection

AUDIO SOCKET:
/ws

BROWSER SOCKET:
/sdr

OPUS DECODING:
SERVER SIDE ENABLED
`
      );

  }
);


app.get(
  "/health",
  (req, res) => {

    res.json({
      ok: true,

      bridge:
        "ACURA DX-1000",

      upstream:
        "UberSDR V2 Opus",

      decoder:
        "opusscript",

      output:
        "PCM16-BE",

      browser:
        "/sdr"
    });

  }
);


/* ============================================================
   START
   ============================================================ */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log("");

    console.log(
      "================================"
    );

    console.log(
      "ACURA DX-1000 SDR BRIDGE"
    );

    console.log(
      "================================"
    );

    console.log(
      `Port: ${PORT}`
    );

    console.log(
      "Browser socket: /sdr"
    );

    console.log(
      "Upstream: UberSDR V2 OPUS"
    );

    console.log(
      "Server OPUS decoder: ENABLED"
    );

    console.log(
      "Browser output: PCM16"
    );

    console.log(
      "Existing ACURA webpage decoder: COMPATIBLE"
    );

    console.log(
      "================================"
    );

    console.log("");

  }
);
