const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;

/*
 * ACURA DX-1000 SDR BRIDGE
 * ------------------------
 * Railway-hosted bridge between:
 *
 *   acurahamradio.org
 *          ↓
 *   ACURA SDR Bridge
 *          ↓
 *   Public KiwiSDR receiver
 *
 * The Kiwi receiver address will be supplied through the
 * KIWI_URL Railway environment variable.
 */

app.use(express.json());

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization"
  );
  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, OPTIONS"
  );

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

/* -------------------------------------------------------
   BASIC STATUS PAGE
------------------------------------------------------- */

app.get("/", (req, res) => {
  res.type("html").send(`
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>ACURA SDR Bridge</title>
      <style>
        body {
          margin: 0;
          background: #020b11;
          color: #55eaff;
          font-family: Arial, sans-serif;
          text-align: center;
          padding: 70px 20px;
        }

        h1 {
          letter-spacing: 4px;
          text-shadow: 0 0 18px #00d9ff;
        }

        .status {
          max-width: 650px;
          margin: 35px auto;
          padding: 25px;
          border: 1px solid #0d6274;
          border-radius: 14px;
          background: #06141d;
        }

        .green {
          color: #65ff91;
        }
      </style>
    </head>

    <body>

      <h1>ACURA DX-1000</h1>

      <h2>HF SDR BRIDGE</h2>

      <div class="status">
        <h3 class="green">● SERVER ONLINE</h3>

        <p>
          Atlantic Coast United Radio Association
        </p>

        <p>
          Live KiwiSDR gateway
        </p>
      </div>

    </body>
    </html>
  `);
});

/* -------------------------------------------------------
   HEALTH CHECK
------------------------------------------------------- */

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "ACURA DX-1000 SDR Bridge",
    kiwiConfigured: Boolean(process.env.KIWI_URL),
    time: new Date().toISOString()
  });
});

/* -------------------------------------------------------
   SDR INFORMATION
------------------------------------------------------- */

app.get("/api/status", (req, res) => {
  res.json({
    online: true,
    receiverConfigured: Boolean(process.env.KIWI_URL),
    receiver: process.env.KIWI_NAME || "ACURA Remote KiwiSDR",
    system: "ACURA DX-1000",
    mode: "RX ONLY"
  });
});

/* -------------------------------------------------------
   WEBSOCKET SERVER

   The WordPress SDR will connect here.

   This first bridge layer accepts frequency/mode commands
   from the ACURA radio. The Kiwi-specific session layer
   will be attached next after Railway deployment is verified.
------------------------------------------------------- */

const wss = new WebSocket.Server({
  server,
  path: "/sdr"
});

wss.on("connection", (socket) => {

  console.log("ACURA SDR client connected");

  socket.send(JSON.stringify({
    type: "status",
    connected: true,
    bridge: "ACURA DX-1000",
    receiverConfigured: Boolean(process.env.KIWI_URL)
  }));

  socket.on("message", (data) => {

    try {

      const message = JSON.parse(data.toString());

      console.log("SDR COMMAND:", message);

      /*
       * Expected commands from our ACURA front panel:
       *
       * {
       *   type: "tune",
       *   frequency: 14250000,
       *   mode: "usb"
       * }
       *
       * or
       *
       * {
       *   type: "mode",
       *   mode: "lsb"
       * }
       */

      if (message.type === "tune") {

        const frequency = Number(message.frequency);

        if (
          !Number.isFinite(frequency) ||
          frequency < 10000 ||
          frequency > 30000000
        ) {
          socket.send(JSON.stringify({
            type: "error",
            message: "Frequency outside KiwiSDR HF range"
          }));

          return;
        }

        socket.send(JSON.stringify({
          type: "tuned",
          frequency,
          mode: message.mode || "usb"
        }));
      }

    } catch (error) {

      console.error("Invalid SDR message:", error);

      socket.send(JSON.stringify({
        type: "error",
        message: "Invalid SDR command"
      }));
    }

  });

  socket.on("close", () => {
    console.log("ACURA SDR client disconnected");
  });

});

/* -------------------------------------------------------
   START SERVER
------------------------------------------------------- */

server.listen(PORT, "0.0.0.0", () => {

  console.log("");
  console.log("-----------------------------------------");
  console.log(" ACURA DX-1000 SDR BRIDGE");
  console.log("-----------------------------------------");
  console.log(` Server listening on port ${PORT}`);

  if (process.env.KIWI_URL) {
    console.log(` Kiwi receiver: ${process.env.KIWI_URL}`);
  } else {
    console.log(" Kiwi receiver: NOT CONFIGURED YET");
  }

  console.log("-----------------------------------------");
  console.log("");

});
