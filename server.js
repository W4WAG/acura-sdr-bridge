"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

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

const CLIENT_NAME = "ACURA-DX-1000/1.0";

const DEFAULT_FREQ = 7255000;
const DEFAULT_MODE = "lsb";

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function makeUuid() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return crypto.randomBytes(16).toString("hex");
}

function normalizeFrequency(value) {
  let f = Number(value);

  if (!Number.isFinite(f) || f <= 0) {
    return DEFAULT_FREQ;
  }

  // MHz -> Hz
  if (f < 1000) {
    f *= 1000000;
  }

  // kHz -> Hz
  else if (f < 1000000) {
    f *= 1000;
  }

  return Math.round(f);
}

function normalizeMode(value) {
  let mode = String(value || DEFAULT_MODE).toLowerCase();

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

  return allowed.has(mode) ? mode : DEFAULT_MODE;
}

function httpBase() {
  let base = String(UPSTREAM_BASE || "").trim();

  base = base.replace(/^ws:/i, "http:");
  base = base.replace(/^wss:/i, "https:");

  return base.replace(/\/+$/, "");
}

function wsBase() {
  let base = httpBase();

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
   REQUIRED UBERSDR SESSION REGISTRATION

   This is the step the previous bridge was missing.

   UberSDRClient performs:

   POST /connection?user_session_id=<same UUID>

   JSON:
   {
      user_session_id: UUID,
      password: ...       // only if configured
   }

   The audio WS then opens using THAT SAME UUID.
   ============================================================ */

async function registerSession(uuid) {
  const url =
    `${httpBase()}/connection?user_session_id=` +
    encodeURIComponent(uuid) +
    adminQuery();

  log("Registering UberSDR session...");
  log("POST /connection uuid=" + uuid);

  const body = {
    user_session_id: uuid
  };

  if (UPSTREAM_PASSWORD) {
    body.password = UPSTREAM_PASSWORD;
  }

  let response;

  try {
    response = await fetch(url, {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
        "User-Agent": CLIENT_NAME,
        "X-Requested-With": "VibeSDR"
      },

      body: JSON.stringify(body)
    });
  }

  catch (err) {
    throw new Error(
      "POST /connection failed: " +
      err.message
    );
  }

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `POST /connection HTTP ${response.status}: ` +
      text.slice(0, 300)
    );
  }

  let result = {};

  try {
    result = JSON.parse(text);
  }

  catch {
    result = {
      allowed: true
    };
  }

  log(
    "POST /connection response:",
    JSON.stringify(result).slice(0, 500)
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
   EXACT UBERSDR AUDIO URL

   /ws
      ?user_session_id=...
      &frequency=...
      &mode=...
      &format=opus
      &version=2
   ============================================================ */

function buildAudioUrl(uuid, frequency, mode) {
  const params = new URLSearchParams();

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
   CLIENT SESSION
   ============================================================ */

browserWss.on(
  "connection",
  (browser) => {

    log("ACURA SDR visitor connected");

    let upstream = null;

    let closed = false;

    let connecting = false;

    let registered = false;

    let reconnectTimer = null;

    let frequency =
      DEFAULT_FREQ;

    let mode =
      DEFAULT_MODE;

    /*
     * IMPORTANT:
     *
     * One UUID for the entire ACURA session.
     *
     * DO NOT create a different UUID between
     * /connection and /ws.
     */

    const uuid =
      makeUuid();

    let firstPacket = true;

    let packetCount = 0;


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


    function sendTune() {
      if (
        !upstream ||
        upstream.readyState !==
        WebSocket.OPEN
      ) {
        log(
          "Tune queued until upstream is ready"
        );

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
          "Tune send error:",
          err.message
        );
      }
    }


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

            reconnectTimer = null;

            if (!closed) {
              connectUpstream();
            }

          },
          2000
        );
    }


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

      /*
       * STEP 1:
       *
       * REGISTER SESSION FIRST.
       */

      try {
        await registerSession(uuid);

        registered = true;

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
          stage: "connection",
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
       *
       * OPEN AUDIO SOCKET USING SAME UUID.
       */

      const audioUrl =
        buildAudioUrl(
          uuid,
          frequency,
          mode
        );

      log(
        "Opening UberSDR audio WebSocket..."
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

      let ws;

      try {
        ws =
          new WebSocket(
            audioUrl,
            {
              handshakeTimeout: 15000,

              perMessageDeflate: false,

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
          "WebSocket creation failed:",
          err.message
        );

        scheduleReconnect();

        return;
      }

      upstream = ws;

      ws.binaryType =
        "nodebuffer";


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

          /*
           * Do NOT invent a hello/auth message.
           *
           * UberSDR uses:
           *
           * POST /connection
           *
           * then the WS query itself.
           */
        }
      );


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
           * TEXT
           */

          if (!isBinary) {
            const text =
              data.toString();

            log(
              "UBERSDR TEXT:",
              text.slice(0, 500)
            );

            if (
              browser.readyState ===
              WebSocket.OPEN
            ) {
              browser.send(text);
            }

            return;
          }


          /*
           * BINARY AUDIO
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

          packetCount++;


          /*
           * VibeSDR V2 HEADER
           */

          let sampleRate = 0;
          let channels = 0;
          let basebandPower = 0;
          let noiseDensity = 0;

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

          catch {
            return;
          }


          /*
           * FIRST PACKET:
           *
           * EXACTLY LIKE VibePowerModule.swift,
           * reassert the tune AFTER audio starts.
           */

          if (firstPacket) {
            firstPacket = false;

            log(
              "================================"
            );

            log(
              "FIRST LIVE AUDIO PACKET RECEIVED"
            );

            log(
              "Sample rate:",
              sampleRate
            );

            log(
              "Channels:",
              channels
            );

            log(
              "================================"
            );

            sendTune();
          }


          /*
           * COMPLETE V2 PACKET GOES TO ACURA.
           *
           * DO NOT REMOVE THE 21-BYTE HEADER.
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
           * REAL SIGNAL DATA
           */

          if (
            packetCount === 1 ||
            packetCount % 10 === 0
          ) {
            sendBrowser({
              type: "signal",

              basebandPower,

              noiseDensity,

              sampleRate,

              channels,

              frequency,

              mode
            });
          }


          if (
            packetCount <= 3 ||
            packetCount % 500 === 0
          ) {
            log(
              `AUDIO PACKET #${packetCount}`,
              `${packet.length} bytes`
            );
          }
        }
      );


      ws.on(
        "unexpected-response",
        (request, response) => {

          connecting = false;

          log(
            "UPSTREAM HTTP REJECTION:",
            response.statusCode,
            response.statusMessage || ""
          );

          let body = "";

          response.on(
            "data",
            chunk => {
              body +=
                chunk.toString();
            }
          );

          response.on(
            "end",
            () => {

              if (body) {
                log(
                  "UPSTREAM RESPONSE:",
                  body.slice(0, 1000)
                );
              }

            }
          );
        }
      );


      ws.on(
        "error",
        err => {

          log(
            "UBERSDR WS ERROR:",
            err.message
          );

        }
      );


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
       COMMANDS FROM ACURA RADIO
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
         * POWER / CONNECT
         */

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

          log(
            "ACURA POWER ->",
            frequency,
            "Hz",
            mode
          );

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
            rawFreq !==
            undefined
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

          log(
            "ACURA MODE ->",
            mode
          );

          sendTune();

          return;
        }


        /*
         * BANDWIDTH
         */

        if (
          msg.type === "bandwidth" ||
          msg.bandwidthLow !==
            undefined ||
          msg.bandwidthHigh !==
            undefined
        ) {

          if (
            upstream &&
            upstream.readyState ===
              WebSocket.OPEN
          ) {

            upstream.send(
              JSON.stringify({
                type: "tune",

                bandwidthLow:
                  Number(
                    msg.bandwidthLow ??
                    msg.low ??
                    -2700
                  ),

                bandwidthHigh:
                  Number(
                    msg.bandwidthHigh ??
                    msg.high ??
                    2700
                  )
              })
            );
          }

          return;
        }


        /*
         * OTHER UBERSDR COMMANDS
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

        log(
          "ACURA SDR visitor disconnected"
        );
      }
    );


    browser.on(
      "error",
      err => {

        log(
          "ACURA BROWSER ERROR:",
          err.message
        );

      }
    );


    sendBrowser({
      type: "bridge",
      status: "ready",
      protocol:
        "UberSDR V2"
    });
  }
);


/* ============================================================
   STATUS
   ============================================================ */

app.get(
  "/",
  (req, res) => {

    res.type(
      "text/plain"
    ).send(
`ACURA DX-1000 SDR BRIDGE
STATUS: ONLINE
UPSTREAM: ${httpBase()}
PROTOCOL: UberSDR V2
SESSION PREFLIGHT: POST /connection
AUDIO SOCKET: /ws
AUDIO: OPUS
BROWSER SOCKET: /sdr
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
        httpBase(),

      protocol:
        "UberSDR V2",

      sessionRegistration:
        "/connection",

      audioSocket:
        "/ws",

      browserSocket:
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
      "SESSION: POST /connection FIRST"
    );
    console.log(
      "Audio socket: /ws"
    );
    console.log(
      "Protocol: UberSDR V2"
    );
    console.log(
      "Audio: Opus + 21-byte V2 header"
    );
    console.log(
      `Upstream: ${httpBase()}`
    );
    console.log(
      "================================"
    );
    console.log("");

  }
);
