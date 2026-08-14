const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const KIWI_URL = process.env.KIWI_URL || "";

app.use(express.json());

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});


/* =========================================================
   ACURA DX-1000 BRIDGE HOME
========================================================= */

app.get("/", (req, res) => {
  res.type("html").send(`
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>ACURA DX-1000 SDR Bridge</title>
<style>
body{
  margin:0;
  background:#020b11;
  color:#55eaff;
  font-family:Arial,sans-serif;
  text-align:center;
  padding:70px 20px;
}
h1{
  letter-spacing:4px;
  text-shadow:0 0 18px #00d9ff;
}
.box{
  max-width:650px;
  margin:35px auto;
  padding:28px;
  border:1px solid #0d6274;
  border-radius:14px;
  background:#06141d;
}
.ok{color:#65ff91;}
</style>
</head>
<body>
<h1>ACURA DX-1000</h1>
<h2>LIVE HF SDR BRIDGE</h2>

<div class="box">
<h3 class="ok">● SERVER ONLINE</h3>
<p>Atlantic Coast United Radio Association</p>
<p>Live KiwiSDR Gateway</p>
</div>
</body>
</html>
  `);
});


/* =========================================================
   HEALTH
========================================================= */

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "ACURA DX-1000 SDR Bridge",
    kiwiConfigured: Boolean(KIWI_URL),
    time: new Date().toISOString()
  });
});


/* =========================================================
   STATUS
========================================================= */

app.get("/api/status", (req, res) => {
  res.json({
    online: true,
    kiwiConfigured: Boolean(KIWI_URL),
    kiwiUrl: KIWI_URL || null,
    system: "ACURA DX-1000",
    mode: "RX ONLY"
  });
});


/* =========================================================
   BASIC KIWI HTTP TEST
========================================================= */

app.get("/api/kiwi-test", async (req, res) => {

  if (!KIWI_URL) {
    return res.status(500).json({
      ok: false,
      error: "KIWI_URL not configured"
    });
  }

  try {

    const response = await fetch(KIWI_URL, {
      signal: AbortSignal.timeout(10000)
    });

    res.json({
      ok: true,
      reachable: response.ok,
      status: response.status,
      statusText: response.statusText
    });

  } catch (error) {

    res.status(502).json({
      ok: false,
      reachable: false,
      error: error.message
    });

  }
});


/* =========================================================
   KIWI WEBSOCKET TEST
========================================================= */

app.get("/api/kiwi-ws-test", (req, res) => {

  if (!KIWI_URL) {
    return res.status(500).json({
      ok: false,
      error: "KIWI_URL not configured"
    });
  }

  const parsed = new URL(KIWI_URL);

  const wsProtocol =
    parsed.protocol === "https:" ? "wss:" : "ws:";

  const stamp = Math.floor(Date.now() / 1000);

  const wsUrl =
    `${wsProtocol}//${parsed.host}/${stamp}/SND`;

  let finished = false;

  const kiwi = new WebSocket(wsUrl, {
    handshakeTimeout: 10000
  });

  const finish = (code, data) => {

    if (finished) return;

    finished = true;

    try {
      kiwi.close();
    } catch (_) {}

    res.status(code).json(data);
  };

  const timer = setTimeout(() => {

    finish(504, {
      ok: false,
      error: "Kiwi WebSocket timeout"
    });

  }, 12000);

  kiwi.on("open", () => {
    kiwi.send("SET auth t=kiwi p=");
  });

  kiwi.on("message", (data) => {

    clearTimeout(timer);

    const packet = Buffer.from(data);

    finish(200, {
      ok: true,
      websocketConnected: true,
      firstMessageTag:
        packet.subarray(0, 3).toString("ascii"),
      messageBytes: packet.length
    });

  });

  kiwi.on("error", (error) => {

    clearTimeout(timer);

    finish(502, {
      ok: false,
      websocketConnected: false,
      error: error.message
    });

  });

});


/* =========================================================
   RECEIVER HELPERS
========================================================= */

const filters = {

  usb: [300, 2700],
  lsb: [-2700, -300],
  am: [-4900, 4900],
  cw: [300, 700]

};


function normalizeMode(mode) {

  mode =
    String(mode || "usb").toLowerCase();

  return filters[mode] ? mode : "usb";
}


function normalizeFrequency(value) {

  let f = Number(value);

  if (!Number.isFinite(f)) {
    return 14250;
  }

  /* Hz -> kHz */
  if (f > 100000) {
    f /= 1000;
  }

  return Math.min(
    30000,
    Math.max(10, f)
  );
}


function makeTuneCommand(frequency, mode) {

  const m = normalizeMode(mode);
  const f = normalizeFrequency(frequency);
  const [low, high] = filters[m];

  return (
    `SET mod=${m}` +
    ` low_cut=${low}` +
    ` high_cut=${high}` +
    ` freq=${f.toFixed(3)}`
  );
}


/* =========================================================
   LIVE ACURA SDR SOCKET

   WordPress will connect to:

   wss://acura-sdr-bridge-production.up.railway.app/sdr
========================================================= */

const wss = new WebSocket.Server({
  server,
  path: "/sdr"
});


wss.on("connection", (browser) => {

  console.log("ACURA SDR visitor connected");

  if (!KIWI_URL) {

    browser.send(JSON.stringify({
      type: "error",
      message: "KIWI_URL not configured"
    }));

    return browser.close();
  }


  const parsed = new URL(KIWI_URL);

  const wsProtocol =
    parsed.protocol === "https:" ? "wss:" : "ws:";

  const stamp = Math.floor(Date.now() / 1000);

  const kiwiWsUrl =
    `${wsProtocol}//${parsed.host}/${stamp}/SND`;


  let frequency = 14250;
  let mode = "usb";
  let configured = false;
  let keepalive = null;


  browser.send(JSON.stringify({
    type: "bridge",
    status: "connecting",
    receiver: parsed.host
  }));


  const kiwi = new WebSocket(kiwiWsUrl, {
    handshakeTimeout: 10000
  });


  function sendKiwi(command) {

    if (kiwi.readyState === WebSocket.OPEN) {
      kiwi.send(command);
    }
  }


  function configureKiwi() {

    if (configured) return;

    configured = true;
sendKiwi("SET AR OK in=12000 out=44100");
    sendKiwi("SET ident_user=ACURA-DX");

    sendKiwi(
      makeTuneCommand(frequency, mode)
    );

    sendKiwi(
      "SET agc=1 hang=0 thresh=-100 slope=6 decay=1000 manGain=50"
    );

    sendKiwi("SET compression=0");

    sendKiwi("SET squelch=0 max=0");

    sendKiwi("SET keepalive");

    keepalive = setInterval(() => {
      sendKiwi("SET keepalive");
    }, 5000);


    browser.send(JSON.stringify({
      type: "receiver",
      status: "live",
      frequency,
      mode
    }));
  }


  kiwi.on("open", () => {

    console.log("Kiwi SND socket connected");

    sendKiwi("SET auth t=kiwi p=");

    browser.send(JSON.stringify({
      type: "kiwi",
      status: "connected"
    }));
  });


  kiwi.on("message", (data) => {

    const packet = Buffer.from(data);

    if (packet.length < 3) return;

    const tag =
      packet.subarray(0, 3).toString("ascii");


    /* Kiwi setup messages */

    if (tag === "MSG") {

      const text =
        packet.subarray(4).toString("utf8");


      if (
        text.includes("sample_rate=") ||
        text.includes("audio_rate=")
      ) {

        configureKiwi();
      }


      if (browser.readyState === WebSocket.OPEN) {

        browser.send(JSON.stringify({
          type: "kiwi-msg",
          value: text
        }));
      }

      return;
    }


    /* Real Kiwi received audio packet */

    if (tag === "SND") {

      if (browser.readyState === WebSocket.OPEN) {

        browser.send(packet, {
          binary: true
        });
      }

      return;
    }

  });


  /* Commands from ACURA radio */

  browser.on("message", (data, isBinary) => {

    if (isBinary) return;

    let message;

    try {
      message =
        JSON.parse(data.toString());
    } catch (_) {
      return;
    }


    if (message.type === "tune") {

      frequency =
        normalizeFrequency(message.frequency);

      mode =
        normalizeMode(message.mode || mode);

      sendKiwi(
        makeTuneCommand(frequency, mode)
      );


      browser.send(JSON.stringify({
        type: "tuned",
        frequency,
        mode
      }));
    }


    if (message.type === "mode") {

      mode =
        normalizeMode(message.mode);

      sendKiwi(
        makeTuneCommand(frequency, mode)
      );
    }

  });


  kiwi.on("error", (error) => {

    console.error(
      "Kiwi error:",
      error.message
    );

    if (browser.readyState === WebSocket.OPEN) {

      browser.send(JSON.stringify({
        type: "error",
        message: error.message
      }));
    }

  });


  kiwi.on("close", () => {

    console.log("Kiwi disconnected");

    if (keepalive) {
      clearInterval(keepalive);
    }

    if (browser.readyState === WebSocket.OPEN) {

      browser.send(JSON.stringify({
        type: "kiwi",
        status: "disconnected"
      }));
    }

  });


  browser.on("close", () => {

    console.log("ACURA visitor disconnected");

    if (keepalive) {
      clearInterval(keepalive);
    }

    try {
      kiwi.close();
    } catch (_) {}

  });

});


/* =========================================================
   START
========================================================= */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log("");
    console.log("===============================");
    console.log(" ACURA DX-1000 SDR BRIDGE");
    console.log("===============================");
    console.log(`Port: ${PORT}`);
    console.log(`Kiwi: ${KIWI_URL || "NOT SET"}`);
    console.log("Live browser socket: /sdr");
    console.log("===============================");
    console.log("");

  }
);
