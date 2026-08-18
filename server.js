"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const browserWss = new WebSocket.Server({ noServer: true });

const PORT = process.env.PORT || 8080;

/*
 * IMPORTANT:
 *
 * VibePowerModule.swift confirms UberSDR/VibeSDR audio uses:
 *
 *   /ws
 *
 * NOT:
 *
 *   /ws/audio
 *
 * Connection format:
 *
 * /ws?user_session_id=UUID
 *    &frequency=Hz
 *    &mode=usb
 *    &format=opus
 *    &version=2
 *    &client=...
 *
 * Binary packet format:
 *
 * bytes  0-7   uint64 LE timestamp
 * bytes  8-11  uint32 LE sample rate
 * byte   12    channels
 * bytes 13-16  float32 LE baseband power
 * bytes 17-20  float32 LE noise density
 * bytes 21+    Opus payload
 *
 * Tuning command:
 *
 * {"type":"tune","frequency":14250000,"mode":"usb"}
 */

const UPSTREAM_BASE =
  process.env.UPSTREAM_BASE ||
  process.env.UBERSDR_BASE ||
  "https://ubersdr.k3fef.com";

const UPSTREAM_PASSWORD =
  process.env.UPSTREAM_PASSWORD ||
  process.env.UBERSDR_PASSWORD ||
  "";

const ADMIN_SUFFIX =
  process.env.ADMIN_SUFFIX ||
  process.env.UBERSDR_ADMIN_SUFFIX ||
  "";

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function wsBaseFromHttp(base) {
  let s = String(base || "").trim().replace(/\/+$/, "");

  if (s.startsWith("https://")) {
    s = "wss://" + s.slice(8);
  } else if (s.startsWith("http://")) {
    s = "ws://" + s.slice(7);
  } else if (!/^wss?:\/\//i.test(s)) {
    s = "wss://" + s;
  }

  return s;
}

function normalizeMode(mode) {
  const m = String(mode || "usb").toLowerCase();

  const allowed = new Set([
    "usb",
    "lsb",
    "am",
    "fm",
    "nfm",
    "cw",
    "cwu",
    "cwl"
  ]);

  return allowed.has(m) ? m : "usb";
}

function normalizeFrequency(value) {
  let n = Number(value);

  if (!Number.isFinite(n)) {
    return 14250000;
  }

  /*
   * ACURA display may send MHz, kHz or Hz.
   *
   * 14.250  -> 14250000
   * 14250   -> 14250000
   * 14250000 stays unchanged
   */

  if (n < 1000) {
    n *= 1_000_000;
  } else if (n < 1_000_000) {
    n *= 1000;
  }

  return Math.round(n);
}

function makeUuid() {
  if (crypto.randomUUID) {
    return crypto.randomUUID();
  }

  return crypto.randomBytes(16).toString("hex");
}

function buildUpstreamUrl(freq, mode, uuid) {
  const base = wsBaseFromHttp(UPSTREAM_BASE);

  const params = new URLSearchParams();

  params.set("user_session_id", uuid);
  params.set("frequency", String(freq));
  params.set("mode", mode);
  params.set("format", "opus");
  params.set("version", "2");

  /*
   * Match VibePowerModule.swift client identification.
   */
  params.set(
    "client",
    "ACURA-DX-1000/1 (+https://acurahamradio.org)"
  );

  if (UPSTREAM_PASSWORD) {
    params.set("password", UPSTREAM_PASSWORD);
  }

  let url = `${base}/ws?${params.toString()}`;

  /*
   * VibePowerModule.swift appends the owner's admin
   * authentication suffix directly to the query.
   *
   * Expected forms include:
   *
   * &vs_admin_ticket=...
   *
   * or
   *
   * &vs_admin_nonce=...&vs_admin_auth=...
   */

  if (ADMIN_SUFFIX) {
    url += ADMIN_SUFFIX.startsWith("&")
      ? ADMIN_SUFFIX
      : "&" + ADMIN_SUFFIX;
  }

  return url;
}

app.use(express.json());

app.get("/", (req, res) => {
  res.type("text/plain").send(
    [
      "ACURA DX-1000 SDR BRIDGE",
      "STATUS: ONLINE",
      "Protocol: VibeSDR / UberSDR V2",
      "Browser WebSocket: /sdr",
      "Upstream WebSocket: /ws",
      "Audio: Opus",
      "Packet Header: 21 bytes",
      "Tuning: LIVE"
    ].join("\n")
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "ACURA DX-1000 SDR Bridge",
    upstream: UPSTREAM_BASE,
    protocol: "VibeSDR/UberSDR V2",
    upstreamPath: "/ws",
    browserPath: "/sdr"
  });
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
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  browserWss.handleUpgrade(req, socket, head, ws => {
    browserWss.emit("connection", ws, req);
  });
});

browserWss.on("connection", (client, req) => {
  log("ACURA SDR visitor connected");

  const requestUrl = new URL(
    req.url,
    `http://${req.headers.host || "localhost"}`
  );

  let frequency = normalizeFrequency(
    requestUrl.searchParams.get("frequency") ||
    requestUrl.searchParams.get("freq") ||
    14250000
  );

  let mode = normalizeMode(
    requestUrl.searchParams.get("mode") || "usb"
  );

  const uuid =
    requestUrl.searchParams.get("uuid") ||
    requestUrl.searchParams.get("user_session_id") ||
    makeUuid();

  let upstream = null;
  let closed = false;
  let reconnectTimer = null;
  let lastTune = {
    frequency,
    mode
  };

  function sendClient(obj) {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(JSON.stringify(obj));
      } catch {}
    }
  }

  function connectUpstream() {
    if (closed) return;

    if (upstream) {
      try {
        upstream.removeAllListeners();
        upstream.terminate();
      } catch {}

      upstream = null;
    }

    const upstreamUrl = buildUpstreamUrl(
      frequency,
      mode,
      uuid
    );

    log("Connecting to VibeSDR/UberSDR upstream...");
    log("Upstream:", upstreamUrl.replace(/password=[^&]+/g, "password=***"));

    upstream = new WebSocket(upstreamUrl, {
      perMessageDeflate: false,
      handshakeTimeout: 15000,
      headers: {
        "User-Agent":
          "ACURA-DX-1000/1 (+https://acurahamradio.org)"
      }
    });

    upstream.binaryType = "nodebuffer";

    upstream.on("open", () => {
      log("UPSTREAM AUDIO CONNECTED");

      sendClient({
        type: "status",
        status: "connected",
        frequency,
        mode
      });

      /*
       * VibePowerModule.swift reasserts the tune after
       * the first received packet because commands sent
       * during the handshake can be lost.
       *
       * We do the same below when the first binary
       * packet arrives.
       */
    });

    let firstPacket = true;

    upstream.on("message", (data, isBinary) => {
      if (closed) return;

      if (isBinary) {
        const packet = Buffer.isBuffer(data)
          ? data
          : Buffer.from(data);

        if (packet.length < 21) {
          return;
        }

        /*
         * VibeSDR V2 header
         */

        const sampleRate = packet.readUInt32LE(8);
        const channels = packet.readUInt8(12);
        const basebandPower = packet.readFloatLE(13);
        const noiseDensity = packet.readFloatLE(17);

        if (firstPacket) {
          firstPacket = false;

          log(
            "FIRST AUDIO PACKET",
            "bytes=" + packet.length,
            "rate=" + sampleRate,
            "channels=" + channels
          );

          /*
           * Exact tune command used by VibePowerModule.swift
           */

          if (upstream.readyState === WebSocket.OPEN) {
            upstream.send(
              JSON.stringify({
                type: "tune",
                frequency: lastTune.frequency,
                mode: lastTune.mode
              })
            );

            log(
              "TUNE ASSERT ->",
              lastTune.frequency,
              "Hz",
              lastTune.mode
            );
          }
        }

        /*
         * Send the COMPLETE V2 packet unchanged.
         *
         * This preserves the 21-byte header + Opus payload
         * expected by the ACURA browser decoder.
         */

        if (client.readyState === WebSocket.OPEN) {
          client.send(packet, {
            binary: true
          });
        }

        /*
         * Signal data for the S-meter.
         *
         * basebandPower and noiseDensity come directly
         * from the VibeSDR V2 packet header.
         */

        sendClient({
          type: "signal",
          basebandPower,
          noiseDensity,
          sampleRate,
          channels
        });

        return;
      }

      /*
       * Preserve upstream JSON/text messages.
       */

      const text = data.toString();

      if (client.readyState === WebSocket.OPEN) {
        client.send(text);
      }
    });

    upstream.on("unexpected-response", (request, response) => {
      log(
        "UPSTREAM HTTP REJECTION:",
        response.statusCode,
        response.statusMessage
      );

      let body = "";

      response.on("data", chunk => {
        body += chunk.toString();
      });

      response.on("end", () => {
        if (body) {
          log("UPSTREAM RESPONSE:", body.slice(0, 1000));
        }

        sendClient({
          type: "status",
          status: "upstream-http-error",
          code: response.statusCode
        });
      });
    });

    upstream.on("error", err => {
      log("UPSTREAM ERROR:", err.message);

      sendClient({
        type: "status",
        status: "error",
        message: err.message
      });
    });

    upstream.on("close", (code, reason) => {
      log(
        "UPSTREAM CLOSED:",
        code,
        reason ? reason.toString() : ""
      );

      upstream = null;

      if (!closed) {
        clearTimeout(reconnectTimer);

        reconnectTimer = setTimeout(() => {
          log("Reconnecting upstream...");
          connectUpstream();
        }, 2000);
      }
    });
  }

  client.on("message", (data, isBinary) => {
    if (isBinary) return;

    let msg;

    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    /*
     * Accept the ACURA page's different tune command
     * shapes without changing the webpage.
     */

    if (
      msg.type === "tune" ||
      msg.type === "frequency" ||
      msg.type === "setFrequency" ||
      msg.cmd === "tune"
    ) {
      const requested =
        msg.frequency ??
        msg.freq ??
        msg.hz ??
        msg.value;

      if (requested !== undefined) {
        frequency = normalizeFrequency(requested);
      }

      if (msg.mode) {
        mode = normalizeMode(msg.mode);
      }

      lastTune = {
        frequency,
        mode
      };

      log(
        "ACURA TUNE REQUEST ->",
        frequency,
        "Hz",
        mode
      );

      if (
        upstream &&
        upstream.readyState === WebSocket.OPEN
      ) {
        /*
         * EXACT command used by VibePowerModule.swift.
         */

        upstream.send(
          JSON.stringify({
            type: "tune",
            frequency,
            mode
          })
        );

        log(
          "LIVE TUNE SENT ->",
          frequency,
          "Hz",
          mode
        );
      } else {
        log("Tune queued until upstream connects");
      }

      return;
    }

    /*
     * Bandwidth command.
     */

    if (
      msg.type === "bandwidth" ||
      msg.bandwidthLow !== undefined ||
      msg.bandwidthHigh !== undefined
    ) {
      if (
        upstream &&
        upstream.readyState === WebSocket.OPEN
      ) {
        upstream.send(
          JSON.stringify({
            type: "tune",
            bandwidthLow:
              Number(msg.bandwidthLow ?? msg.low ?? -2700),
            bandwidthHigh:
              Number(msg.bandwidthHigh ?? msg.high ?? 2700)
          })
        );
      }

      return;
    }

    /*
     * Other VibeSDR commands pass straight through.
     */

    if (
      upstream &&
      upstream.readyState === WebSocket.OPEN
    ) {
      upstream.send(data.toString());
    }
  });

  client.on("close", () => {
    closed = true;

    clearTimeout(reconnectTimer);

    if (upstream) {
      try {
        upstream.close();
      } catch {}

      upstream = null;
    }

    log("ACURA SDR visitor disconnected");
  });

  client.on("error", err => {
    log("BROWSER SOCKET ERROR:", err.message);
  });

  connectUpstream();
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("==============================");
  console.log("ACURA DX-1000 SDR BRIDGE");
  console.log("==============================");
  console.log("Port:", PORT);
  console.log("Browser socket: /sdr");
  console.log("Protocol: VibeSDR / UberSDR V2");
  console.log("Upstream endpoint: /ws");
  console.log("Audio: Opus + 21-byte V2 header");
  console.log("Live tuning: ENABLED");
  console.log("S-meter data: ENABLED");
  console.log("Upstream:", UPSTREAM_BASE);
  console.log("==============================");
  console.log("");
});
