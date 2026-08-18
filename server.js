"use strict";

/*
 * ACURA SDR BRIDGE
 * VibeSDR / UberSDR native audio-WebSocket protocol
 *
 * Extracted from VibePowerModule.swift.
 *
 * Browser:
 *      wss://YOUR-RAILWAY-HOST/sdr
 *
 * Upstream audio:
 *      /ws
 *      ?user_session_id=...
 *      &frequency=...
 *      &mode=...
 *      &format=opus
 *      &version=2
 *
 * Tune:
 *      {"type":"tune","frequency":14250000,"mode":"usb"}
 *
 * Binary audio packet:
 *
 *   0..7     uint64 LE   timestamp
 *   8..11    uint32 LE   sample rate
 *   12       uint8       channels
 *   13..16   float32 LE  baseband power
 *   17..20   float32 LE  noise density
 *   21..     Opus payload
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8080);

/*
 * Set this in Railway to the actual UberSDR/VibeSDR server.
 *
 * Examples:
 *
 * https://example.com
 * http://example.com:8073
 *
 * DO NOT put /ws on the end.
 */
const UPSTREAM_URL =
  process.env.UPSTREAM_URL ||
  process.env.UBERSDR_URL ||
  "";

/*
 * Optional upstream credentials.
 */
const UPSTREAM_PASSWORD =
  process.env.UPSTREAM_PASSWORD || "";

const VS_ADMIN_TICKET =
  process.env.VS_ADMIN_TICKET || "";

const VS_ADMIN_NONCE =
  process.env.VS_ADMIN_NONCE || "";

const VS_ADMIN_AUTH =
  process.env.VS_ADMIN_AUTH || "";


/* ============================================================
   EXPRESS
   ============================================================ */

const app = express();

app.get("/", (req, res) => {
  res.type("text/plain").send(
`ACURA DX-1000 SDR BRIDGE
STATUS: ONLINE
PROTOCOL: VibeSDR / UberSDR V2
AUDIO: OPUS
WEBSOCKET: /sdr
`
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    bridge: "ACURA DX-1000",
    protocol: "VibeSDR-v2",
    audio: "opus",
    upstreamConfigured: Boolean(UPSTREAM_URL)
  });
});

const server = http.createServer(app);


/* ============================================================
   BROWSER WEBSOCKET
   ============================================================ */

const browserWss = new WebSocket.Server({
  noServer: true,
  perMessageDeflate: false
});

server.on("upgrade", (req, socket, head) => {

  let pathname;

  try {
    pathname = new URL(
      req.url,
      `http://${req.headers.host || "localhost"}`
    ).pathname;
  } catch {
    socket.destroy();
    return;
  }

  if (pathname !== "/sdr") {
    socket.destroy();
    return;
  }

  browserWss.handleUpgrade(req, socket, head, ws => {
    browserWss.emit("connection", ws, req);
  });
});


/* ============================================================
   HELPERS
   ============================================================ */

function normalizeMode(mode) {

  mode = String(mode || "usb").toLowerCase();

  const allowed = new Set([
    "usb",
    "lsb",
    "am",
    "fm",
    "nfm",
    "wfm",
    "cw",
    "cwu",
    "cwl",
    "sam"
  ]);

  return allowed.has(mode) ? mode : "usb";
}


function normalizeFrequency(freq) {

  const n = Number(freq);

  if (!Number.isFinite(n))
    return 14250000;

  return Math.max(1, Math.round(n));
}


function makeSessionId() {

  if (typeof crypto.randomUUID === "function")
    return crypto.randomUUID();

  return crypto.randomBytes(16).toString("hex");
}


function websocketBase(base) {

  let s = String(base || "").trim();

  if (!s)
    return "";

  s = s.replace(/\/+$/, "");

  if (s.startsWith("https://"))
    return "wss://" + s.slice(8);

  if (s.startsWith("http://"))
    return "ws://" + s.slice(7);

  if (s.startsWith("wss://") ||
      s.startsWith("ws://"))
    return s;

  return "wss://" + s;
}


/*
 * This reproduces VibePowerModule.audioWsURL().
 */
function buildAudioUrl({
  frequency,
  mode,
  uuid
}) {

  const base = websocketBase(UPSTREAM_URL);

  if (!base)
    return null;

  const u = new URL(base + "/ws");

  u.searchParams.set(
    "user_session_id",
    uuid
  );

  u.searchParams.set(
    "frequency",
    String(frequency)
  );

  u.searchParams.set(
    "mode",
    mode
  );

  u.searchParams.set(
    "format",
    "opus"
  );

  u.searchParams.set(
    "version",
    "2"
  );

  /*
   * VibePowerModule identifies itself through
   * the client query parameter.
   */
  u.searchParams.set(
    "client",
    "ACURA-SDR-Bridge/1.0"
  );

  /*
   * Bypass password.
   */
  if (UPSTREAM_PASSWORD) {
    u.searchParams.set(
      "password",
      UPSTREAM_PASSWORD
    );
  }

  /*
   * Owner/admin authentication.
   *
   * VibePowerModule supports either:
   *
   * vs_admin_ticket
   *
   * OR
   *
   * vs_admin_nonce + vs_admin_auth
   */

  if (VS_ADMIN_TICKET) {

    u.searchParams.set(
      "vs_admin_ticket",
      VS_ADMIN_TICKET
    );

  } else {

    if (VS_ADMIN_NONCE) {
      u.searchParams.set(
        "vs_admin_nonce",
        VS_ADMIN_NONCE
      );
    }

    if (VS_ADMIN_AUTH) {
      u.searchParams.set(
        "vs_admin_auth",
        VS_ADMIN_AUTH
      );
    }
  }

  return u.toString();
}


/* ============================================================
   PACKET VALIDATION

   VibePowerModule V2:

   byte 0-7:
       timestamp uint64 LE

   byte 8-11:
       sample rate uint32 LE

   byte 12:
       channels

   byte 13-16:
       baseband power float32 LE

   byte 17-20:
       noise density float32 LE

   byte 21+:
       Opus
   ============================================================ */

function inspectAudioPacket(buffer) {

  if (!Buffer.isBuffer(buffer))
    buffer = Buffer.from(buffer);

  if (buffer.length <= 21)
    return null;

  const sampleRate =
    buffer.readUInt32LE(8);

  const channels =
    buffer.readUInt8(12);

  const basebandPower =
    buffer.readFloatLE(13);

  const noiseDensity =
    buffer.readFloatLE(17);

  if (
    sampleRate < 8000 ||
    sampleRate > 96000
  ) {
    return null;
  }

  if (
    channels !== 1 &&
    channels !== 2
  ) {
    return null;
  }

  if (buffer.length - 21 < 3)
    return null;

  return {
    sampleRate,
    channels,
    basebandPower,
    noiseDensity,
    opusBytes: buffer.length - 21
  };
}


/* ============================================================
   CLIENT SESSION
   ============================================================ */

browserWss.on(
  "connection",
  (browser, req) => {

    console.log(
      "ACURA SDR visitor connected"
    );

    let upstream = null;

    let closed = false;

    let frequency = 14250000;

    let mode = "usb";

    let sessionId =
      makeSessionId();

    let reconnectTimer = null;

    let firstAudioPacket = true;

    let packetCount = 0;

    let lastSampleRate = 0;


    function sendBrowser(obj) {

      if (
        browser.readyState !==
        WebSocket.OPEN
      )
        return;

      browser.send(
        JSON.stringify(obj)
      );
    }


    function sendTune() {

      if (
        !upstream ||
        upstream.readyState !==
        WebSocket.OPEN
      )
        return;

      const tune = {
        type: "tune",
        frequency,
        mode
      };

      upstream.send(
        JSON.stringify(tune)
      );

      console.log(
        `Tune -> ${frequency} ${mode}`
      );
    }


    function disconnectUpstream() {

      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      if (upstream) {

        upstream.removeAllListeners();

        try {
          upstream.close();
        } catch {}

        try {
          upstream.terminate();
        } catch {}

        upstream = null;
      }
    }


    function scheduleReconnect() {

      if (closed)
        return;

      if (reconnectTimer)
        return;

      reconnectTimer =
        setTimeout(() => {

          reconnectTimer = null;

          if (!closed)
            connectUpstream();

        }, 2000);
    }


    function connectUpstream() {

      if (closed)
        return;

      disconnectUpstream();

      if (!UPSTREAM_URL) {

        console.error(
          "UPSTREAM_URL is not configured"
        );

        sendBrowser({
          type: "error",
          message:
            "Railway UPSTREAM_URL is not configured"
        });

        return;
      }

      const audioUrl =
        buildAudioUrl({
          frequency,
          mode,
          uuid: sessionId
        });

      if (!audioUrl) {

        sendBrowser({
          type: "error",
          message:
            "Could not build upstream WebSocket URL"
        });

        return;
      }

      /*
       * Do not print passwords/admin credentials.
       */
      console.log(
        "Opening VibeSDR audio WebSocket"
      );

      console.log(
        `Frequency: ${frequency}`
      );

      console.log(
        `Mode: ${mode}`
      );

      console.log(
        `Session: ${sessionId}`
      );

      firstAudioPacket = true;

      packetCount = 0;

      lastSampleRate = 0;


      upstream =
        new WebSocket(audioUrl, {
          perMessageDeflate: false,
          handshakeTimeout: 10000
        });


      upstream.binaryType =
        "nodebuffer";


      upstream.on(
        "open",
        () => {

          console.log(
            "VibeSDR audio socket connected"
          );

          sendBrowser({
            type: "upstream",
            status: "connected",
            frequency,
            mode
          });

          /*
           * IMPORTANT:
           *
           * VibePowerModule does NOT send a
           * separate authentication handshake.
           *
           * Authentication and initial tuning
           * are already in the WS URL.
           *
           * The tune command is reasserted
           * after audio begins.
           */
        }
      );


      upstream.on(
        "message",
        (data, isBinary) => {

          if (closed)
            return;


          /* -------------------------------
             TEXT MESSAGE
             ------------------------------- */

          if (!isBinary) {

            const text =
              data.toString();

            console.log(
              "VibeSDR:",
              text.slice(0, 500)
            );

            /*
             * Pass server status/DSP text
             * directly to our browser.
             */

            if (
              browser.readyState ===
              WebSocket.OPEN
            ) {
              browser.send(text);
            }

            return;
          }


          /* -------------------------------
             AUDIO PACKET
             ------------------------------- */

          const packet =
            Buffer.isBuffer(data)
              ? data
              : Buffer.from(data);

          const info =
            inspectAudioPacket(packet);

          if (!info) {

            console.log(
              "Rejected invalid V2 packet:",
              packet.length
            );

            return;
          }

          packetCount++;


          /*
           * VibePowerModule reasserts the
           * requested tune after receiving
           * the FIRST audio packet.
           */

          if (firstAudioPacket) {

            firstAudioPacket = false;

            console.log(
              "First valid V2 audio packet received"
            );

            console.log(
              `Audio: ${info.sampleRate} Hz / ${info.channels} channel(s)`
            );

            sendTune();
          }


          /*
           * VibePowerModule detects a sample
           * rate change because the upstream
           * Opus encoder may have been created
           * for the old mode/rate.
           */

          if (
            lastSampleRate &&
            lastSampleRate !==
              info.sampleRate
          ) {

            console.log(
              `Audio sample rate changed: ${lastSampleRate} -> ${info.sampleRate}`
            );
          }

          lastSampleRate =
            info.sampleRate;


          /*
           * MOST IMPORTANT PART:
           *
           * DO NOT strip the 21-byte header.
           *
           * The client expects the complete
           * VibeSDR V2 binary packet.
           */

          if (
            browser.readyState ===
            WebSocket.OPEN
          ) {

            browser.send(packet, {
              binary: true
            });
          }


          if (
            packetCount <= 3 ||
            packetCount % 500 === 0
          ) {

            console.log(
              `Audio packet #${packetCount} ` +
              `${packet.length} bytes ` +
              `${info.sampleRate}Hz ` +
              `${info.channels}ch ` +
              `Opus=${info.opusBytes}`
            );
          }
        }
      );


      upstream.on(
        "ping",
        data => {

          try {
            upstream.pong(data);
          } catch {}
        }
      );


      upstream.on(
        "error",
        err => {

          console.error(
            "VibeSDR WebSocket error:",
            err.message
          );
        }
      );


      upstream.on(
        "close",
        (code, reason) => {

          if (closed)
            return;

          console.log(
            `VibeSDR disconnected: ${code} ${reason || ""}`
          );

          sendBrowser({
            type: "upstream",
            status: "disconnected"
          });

          upstream = null;

          scheduleReconnect();
        }
      );
    }


    /* ========================================================
       COMMANDS FROM ACURA WEB PAGE
       ======================================================== */

    browser.on(
      "message",
      (raw, isBinary) => {

        if (isBinary)
          return;

        let msg;

        try {

          msg =
            JSON.parse(
              raw.toString()
            );

        } catch {

          console.log(
            "Ignored non-JSON browser command"
          );

          return;
        }


        /* -------------------------------
           CONNECT / POWER ON
           ------------------------------- */

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

          /*
           * New radio power-on =
           * new session.
           */

          sessionId =
            makeSessionId();

          connectUpstream();

          return;
        }


        /* -------------------------------
           TUNE
           ------------------------------- */

        if (
          msg.type === "tune"
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

          sendTune();

          return;
        }


        /* -------------------------------
           BANDWIDTH / DSP / ETC.

           VibePowerModule forwards these
           as JSON text over the SAME audio
           WebSocket.
           ------------------------------- */

        if (
          upstream &&
          upstream.readyState ===
            WebSocket.OPEN
        ) {

          upstream.send(
            JSON.stringify(msg)
          );
        }
      }
    );


    browser.on(
      "close",
      () => {

        closed = true;

        console.log(
          "ACURA SDR visitor disconnected"
        );

        disconnectUpstream();
      }
    );


    browser.on(
      "error",
      err => {

        console.error(
          "Browser WebSocket error:",
          err.message
        );
      }
    );


    /*
     * Tell the ACURA page the bridge itself
     * is alive.
     */

    sendBrowser({
      type: "bridge",
      status: "ready",
      protocol: "VibeSDR-v2"
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
      "Protocol: VibeSDR / UberSDR V2"
    );

    console.log(
      "Audio: Opus + 21-byte V2 header"
    );

    console.log(
      `Upstream: ${
        UPSTREAM_URL
          ? "CONFIGURED"
          : "NOT CONFIGURED"
      }`
    );

    console.log(
      "================================"
    );
  }
);
