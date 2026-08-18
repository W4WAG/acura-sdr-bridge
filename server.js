"use strict";

/*
 * ============================================================
 * ACURA DX-1000 SDR BRIDGE
 * Native UberSDR / VibeSDR V2 Audio WebSocket
 * ============================================================
 *
 * Browser:
 *      wss://YOUR-RAILWAY-HOST/sdr
 *
 * Upstream:
 *      UberSDR native /ws
 *
 * Audio:
 *      Opus in VibeSDR V2 binary packets
 *
 * IMPORTANT:
 * The ACURA webpage displays/sends frequencies in MHz:
 *
 *      7.255
 *      14.250
 *      21.300
 *
 * UberSDR expects Hz:
 *
 *      7255000
 *      14250000
 *      21300000
 *
 * This bridge performs that conversion.
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8080);


/* ============================================================
   UPSTREAM CONFIGURATION
   ============================================================ */

const UPSTREAM_URL =
  process.env.UPSTREAM_URL ||
  process.env.UBERSDR_URL ||
  "";

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

});


/* ============================================================
   MODE NORMALIZATION
   ============================================================ */

function normalizeMode(mode) {

  mode =
    String(mode || "usb")
      .toLowerCase();

  const allowed = new Set([
    "usb",
    "lsb",
    "am",
    "sam",
    "fm",
    "nfm",
    "wfm",
    "cw",
    "cwu",
    "cwl"
  ]);


  /*
   * ACURA currently uses "cw".
   * UberSDR normally distinguishes CWU/CWL.
   */

  if (mode === "cw")
    return "cwu";


  return allowed.has(mode)
    ? mode
    : "usb";

}


/* ============================================================
   FREQUENCY NORMALIZATION

   ACURA webpage normally sends MHz.

   Examples:

       7.255  -> 7,255,000 Hz
      14.250  -> 14,250,000 Hz
      21.300  -> 21,300,000 Hz

   But we also accept kHz and Hz so the bridge is tolerant of
   either representation.
   ============================================================ */

function normalizeFrequency(value) {

  let f = Number(value);


  if (!Number.isFinite(f)) {

    return 14250000;

  }


  /*
   * MHz
   *
   * 7.255
   * 14.250
   * 28.400
   */

  if (f < 1000) {

    f *= 1000000;

  }


  /*
   * kHz
   *
   * 7255
   * 14250
   * 28400
   */

  else if (f < 100000) {

    f *= 1000;

  }


  /*
   * Otherwise assume the value
   * is already Hz.
   */


  return Math.max(
    1,
    Math.round(f)
  );

}


/* ============================================================
   SESSION ID
   ============================================================ */

function makeSessionId() {

  if (
    typeof crypto.randomUUID ===
    "function"
  ) {

    return crypto.randomUUID();

  }


  return crypto
    .randomBytes(16)
    .toString("hex");

}


/* ============================================================
   CONVERT HTTP URL TO WEBSOCKET URL
   ============================================================ */

function websocketBase(base) {

  let s =
    String(base || "")
      .trim()
      .replace(/\/+$/, "");


  if (!s)
    return "";


  if (s.startsWith("https://")) {

    return (
      "wss://" +
      s.slice(8)
    );

  }


  if (s.startsWith("http://")) {

    return (
      "ws://" +
      s.slice(7)
    );

  }


  if (
    s.startsWith("wss://") ||
    s.startsWith("ws://")
  ) {

    return s;

  }


  return "wss://" + s;

}


/* ============================================================
   BUILD UBERSDR AUDIO WEBSOCKET URL
   ============================================================ */

function buildAudioUrl({
  frequency,
  mode,
  uuid
}) {

  const base =
    websocketBase(
      UPSTREAM_URL
    );


  if (!base)
    return null;


  const u =
    new URL(
      base + "/ws"
    );


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


  u.searchParams.set(
    "client",
    "ACURA-SDR-Bridge/1.1"
  );


  /* ------------------------
     OPTIONAL PASSWORD
     ------------------------ */

  if (UPSTREAM_PASSWORD) {

    u.searchParams.set(
      "password",
      UPSTREAM_PASSWORD
    );

  }


  /* ------------------------
     OPTIONAL ADMIN AUTH
     ------------------------ */

  if (VS_ADMIN_TICKET) {

    u.searchParams.set(
      "vs_admin_ticket",
      VS_ADMIN_TICKET
    );

  }

  else {

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
   VIBESDR V2 AUDIO PACKET

   HEADER:

   0..7
       timestamp uint64 LE

   8..11
       sample rate uint32 LE

   12
       channels uint8

   13..16
       baseband power float32 LE

   17..20
       noise density float32 LE

   21+
       OPUS AUDIO
   ============================================================ */

function inspectAudioPacket(data) {

  const buffer =
    Buffer.isBuffer(data)
      ? data
      : Buffer.from(data);


  if (buffer.length <= 21)
    return null;


  let sampleRate;
  let channels;
  let basebandPower;
  let noiseDensity;


  try {

    sampleRate =
      buffer.readUInt32LE(8);

    channels =
      buffer.readUInt8(12);

    basebandPower =
      buffer.readFloatLE(13);

    noiseDensity =
      buffer.readFloatLE(17);

  }

  catch {

    return null;

  }


  if (
    sampleRate < 8000 ||
    sampleRate > 192000
  ) {

    return null;

  }


  if (
    channels !== 1 &&
    channels !== 2
  ) {

    return null;

  }


  const opusBytes =
    buffer.length - 21;


  if (opusBytes < 3)
    return null;


  return {
    sampleRate,
    channels,
    basebandPower,
    noiseDensity,
    opusBytes
  };

}


/* ============================================================
   ACURA CLIENT CONNECTION
   ============================================================ */

browserWss.on(
  "connection",
  (browser, req) => {

    console.log(
      "ACURA SDR visitor connected"
    );


    let upstream = null;

    let closed = false;


    /*
     * ACURA DEFAULT
     *
     * 14.250 MHz USB
     */

    let frequency =
      14250000;

    let mode =
      "usb";


    let sessionId =
      makeSessionId();


    let reconnectTimer =
      null;


    let firstAudioPacket =
      true;


    let packetCount =
      0;


    let lastSampleRate =
      0;


    /* ========================================================
       SEND JSON TO ACURA
       ======================================================== */

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
       SEND REAL TUNE COMMAND TO UBERSDR
       ======================================================== */

    function sendTune() {

      if (
        !upstream ||
        upstream.readyState !==
        WebSocket.OPEN
      ) {

        console.log(
          "Tune requested while upstream not ready"
        );

        return;

      }


      const tune = {

        type: "tune",

        frequency:
          Math.round(frequency),

        mode:
          mode

      };


      try {

        upstream.send(
          JSON.stringify(tune)
        );


        console.log(
          `REAL SDR TUNE -> ${frequency} Hz ${mode}`
        );


        sendBrowser({

          type: "receiver",

          status: "tuned",

          frequency,

          mode

        });

      }

      catch (err) {

        console.error(
          "Tune send failed:",
          err.message
        );

      }

    }


    /* ========================================================
       DISCONNECT UPSTREAM
       ======================================================== */

    function disconnectUpstream() {

      if (reconnectTimer) {

        clearTimeout(
          reconnectTimer
        );

        reconnectTimer =
          null;

      }


      if (!upstream)
        return;


      const old =
        upstream;


      upstream =
        null;


      try {

        old.removeAllListeners();

      }

      catch {}


      try {

        old.close();

      }

      catch {}


      try {

        old.terminate();

      }

      catch {}

    }


    /* ========================================================
       RECONNECT
       ======================================================== */

    function scheduleReconnect() {

      if (closed)
        return;


      if (reconnectTimer)
        return;


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
       CONNECT TO UBERSDR
       ======================================================== */

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

          uuid:
            sessionId

        });


      if (!audioUrl) {

        sendBrowser({

          type: "error",

          message:
            "Unable to build UberSDR WebSocket URL"

        });


        return;

      }


      console.log(
        "Opening UberSDR audio WebSocket"
      );


      console.log(
        `Frequency: ${frequency} Hz`
      );


      console.log(
        `Mode: ${mode}`
      );


      console.log(
        `Session: ${sessionId}`
      );


      firstAudioPacket =
        true;


      packetCount =
        0;


      lastSampleRate =
        0;


      const ws =
        new WebSocket(
          audioUrl,
          {
            perMessageDeflate: false,
            handshakeTimeout: 10000
          }
        );


      upstream =
        ws;


      ws.binaryType =
        "nodebuffer";


      /* -------------------------------
         CONNECTED
         ------------------------------- */

      ws.on(
        "open",
        () => {

          if (
            closed ||
            upstream !== ws
          ) {

            try {
              ws.close();
            }
            catch {}

            return;

          }


          console.log(
            "UberSDR audio socket connected"
          );


          sendBrowser({

            type: "upstream",

            status: "connected",

            frequency,

            mode

          });

        }
      );


      /* -------------------------------
         RECEIVE
         ------------------------------- */

      ws.on(
        "message",
        (data, isBinary) => {

          if (
            closed ||
            upstream !== ws
          ) {

            return;

          }


          /* ===========================
             TEXT
             =========================== */

          if (!isBinary) {

            const text =
              data.toString();


            console.log(
              "UberSDR:",
              text.slice(0, 500)
            );


            /*
             * Forward server status
             * messages to ACURA.
             */

            if (
              browser.readyState ===
              WebSocket.OPEN
            ) {

              browser.send(text);

            }


            return;

          }


          /* ===========================
             BINARY AUDIO
             =========================== */

          const packet =
            Buffer.isBuffer(data)
              ? data
              : Buffer.from(data);


          const info =
            inspectAudioPacket(
              packet
            );


          if (!info) {

            console.log(
              `Invalid V2 audio packet: ${packet.length} bytes`
            );

            return;

          }


          packetCount++;


          /*
           * UberSDR may initially land a
           * session on the owner's default
           * receiver frequency.
           *
           * Reassert OUR requested frequency
           * as soon as real audio arrives.
           */

          if (firstAudioPacket) {

            firstAudioPacket =
              false;


            console.log(
              "First valid UberSDR audio packet"
            );


            console.log(
              `Audio: ${info.sampleRate} Hz / ${info.channels} channel(s)`
            );


            console.log(
              `Reasserting ACURA VFO: ${frequency} Hz ${mode}`
            );


            sendTune();

          }


          /* ---------------------------
             SAMPLE RATE CHANGE
             --------------------------- */

          if (
            lastSampleRate &&
            lastSampleRate !==
              info.sampleRate
          ) {

            console.log(
              `Audio sample rate changed: ` +
              `${lastSampleRate} -> ` +
              `${info.sampleRate}`
            );

          }


          lastSampleRate =
            info.sampleRate;


          /*
           * Forward COMPLETE packet.
           *
           * Do NOT strip the V2 header.
           */

          if (
            browser.readyState ===
            WebSocket.OPEN
          ) {

            browser.send(
              packet,
              {
                binary: true
              }
            );

          }


          /*
           * Also provide real signal
           * information to the ACURA UI.
           *
           * This gives us the data needed
           * for the S-meter.
           */

          if (
            packetCount === 1 ||
            packetCount % 10 === 0
          ) {

            sendBrowser({

              type:
                "signal",

              basebandPower:
                info.basebandPower,

              noiseDensity:
                info.noiseDensity,

              frequency:
                frequency,

              mode:
                mode

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


      /* -------------------------------
         PING
         ------------------------------- */

      ws.on(
        "ping",
        data => {

          try {

            ws.pong(data);

          }

          catch {}

        }
      );


      /* -------------------------------
         ERROR
         ------------------------------- */

      ws.on(
        "error",
        err => {

          console.error(
            "UberSDR WebSocket error:",
            err.message
          );

        }
      );


      /* -------------------------------
         CLOSE
         ------------------------------- */

      ws.on(
        "close",
        (code, reason) => {

          if (
            closed ||
            upstream !== ws
          ) {

            return;

          }


          console.log(
            `UberSDR disconnected: ${code} ${reason || ""}`
          );


          upstream =
            null;


          sendBrowser({

            type:
              "upstream",

            status:
              "disconnected"

          });


          scheduleReconnect();

        }
      );

    }


    /* ========================================================
       COMMANDS FROM ACURA WEBPAGE
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

        }

        catch {

          console.log(
            "Ignored non-JSON ACURA command"
          );

          return;

        }


        /* ====================================================
           CONNECT / POWER
           ==================================================== */

        if (
          msg.type === "connect" ||
          msg.type === "start" ||
          msg.type === "power"
        ) {


          if (
            msg.frequency !==
            undefined
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


          sessionId =
            makeSessionId();


          console.log(
            `ACURA POWER -> ${frequency} Hz ${mode}`
          );


          connectUpstream();


          return;

        }


        /* ====================================================
           TUNE
           ==================================================== */

        if (
          msg.type === "tune"
        ) {


          if (
            msg.frequency !==
            undefined
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


          console.log(
            `ACURA TUNE REQUEST -> ${frequency} Hz ${mode}`
          );


          /*
           * This changes the REAL receiver.
           */

          sendTune();


          return;

        }


        /* ====================================================
           MODE
           ==================================================== */

        if (
          msg.type === "mode"
        ) {

          if (msg.mode) {

            mode =
              normalizeMode(
                msg.mode
              );

          }


          console.log(
            `ACURA MODE -> ${mode}`
          );


          sendTune();


          return;

        }


        /* ====================================================
           OTHER DSP COMMANDS
           ==================================================== */

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
       BROWSER CLOSED
       ======================================================== */

    browser.on(
      "close",
      () => {

        closed =
          true;


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
          "ACURA browser WebSocket error:",
          err.message
        );

      }
    );


    /* ========================================================
       BRIDGE READY
       ======================================================== */

    sendBrowser({

      type:
        "bridge",

      status:
        "ready",

      protocol:
        "VibeSDR-v2"

    });

  }
);


/* ============================================================
   START SERVER
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
      "Audio: Opus + V2 header"
    );

    console.log(
      "ACURA MHz -> UberSDR Hz conversion: ENABLED"
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
