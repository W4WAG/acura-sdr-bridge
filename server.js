"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
const OpusScript = require("opusscript");

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

const CLIENT = "ACURA-AUDIO-TEST/1.0";

const FREQUENCY = 7255000;
const MODE = "lsb";

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function uuid() {
  return crypto.randomUUID
    ? crypto.randomUUID()
    : crypto.randomBytes(16).toString("hex");
}

function httpBase() {
  return UBERSDR_BASE
    .replace(/^wss:/i, "https:")
    .replace(/^ws:/i, "http:")
    .replace(/\/+$/, "");
}

function wsBase() {
  return httpBase()
    .replace(/^https:/i, "wss:")
    .replace(/^http:/i, "ws:");
}


/* ============================================================
   UBERSDR SESSION REGISTRATION
   ============================================================ */

async function registerSession(sessionId) {

  const url =
    `${httpBase()}/connection?user_session_id=` +
    encodeURIComponent(sessionId);

  const body = {
    user_session_id: sessionId
  };

  if (PASSWORD) {
    body.password = PASSWORD;
  }

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
    throw new Error(
      `Registration HTTP ${response.status}: ${text}`
    );
  }

  let result;

  try {
    result = JSON.parse(text);
  } catch {
    result = { allowed: true };
  }

  log("Registration:", result);

  if (result.allowed === false) {
    throw new Error(
      result.reason || "Receiver refused connection"
    );
  }
}


/* ============================================================
   UBERSDR AUDIO URL
   ============================================================ */

function audioUrl(sessionId) {

  const params = new URLSearchParams();

  params.set("user_session_id", sessionId);
  params.set("frequency", String(FREQUENCY));
  params.set("mode", MODE);
  params.set("format", "opus");
  params.set("version", "2");
  params.set("client", CLIENT);

  if (PASSWORD) {
    params.set("password", PASSWORD);
  }

  return `${wsBase()}/ws?${params.toString()}`;
}


/* ============================================================
   BARE TEST PAGE
   ============================================================ */

app.get("/test", (req, res) => {

  res.type("html").send(`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ACURA SDR Audio Test</title>

<style>
body {
  background:#070b0e;
  color:#eee;
  font-family:Arial,sans-serif;
  text-align:center;
  padding:60px 20px;
}

h1 {
  font-size:28px;
}

#status {
  margin:25px;
  font:700 18px monospace;
}

button {
  padding:18px 34px;
  font-size:20px;
  font-weight:700;
  cursor:pointer;
}

.good { color:#46e878; }
.bad  { color:#ff6262; }
</style>
</head>

<body>

<h1>ACURA SDR — AUDIO ONLY TEST</h1>

<p>
K3FEF • 7.255 MHz • LSB
</p>

<button id="play">▶ PLAY LIVE AUDIO</button>

<div id="status">READY</div>

<script>
(() => {

  const button = document.getElementById("play");
  const status = document.getElementById("status");

  let ws = null;
  let ctx = null;
  let gain = null;
  let playAt = 0;

  async function startAudio() {

    if (!ctx) {

      ctx = new (
        window.AudioContext ||
        window.webkitAudioContext
      )();

      gain = ctx.createGain();

      gain.gain.value = 1.0;

      gain.connect(
        ctx.destination
      );
    }

    if (ctx.state === "suspended") {
      await ctx.resume();
    }

    playAt =
      ctx.currentTime + 0.15;
  }


  function playPCM(arrayBuffer) {

    const bytes =
      new Uint8Array(arrayBuffer);

    /*
     * Test bridge packet:
     *
     * 0-3  = PCM1
     * 4-7  = sample rate UInt32 LE
     * 8+   = PCM16 LE mono
     */

    if (bytes.length < 10) return;

    if (
      bytes[0] !== 80 ||
      bytes[1] !== 67 ||
      bytes[2] !== 77 ||
      bytes[3] !== 49
    ) return;

    const view =
      new DataView(arrayBuffer);

    const sampleRate =
      view.getUint32(4, true);

    const sampleCount =
      Math.floor(
        (bytes.length - 8) / 2
      );

    if (!sampleCount) return;

    const audioBuffer =
      ctx.createBuffer(
        1,
        sampleCount,
        sampleRate
      );

    const channel =
      audioBuffer.getChannelData(0);

    let offset = 8;

    for (
      let i = 0;
      i < sampleCount;
      i++, offset += 2
    ) {

      channel[i] =
        view.getInt16(
          offset,
          true
        ) / 32768;
    }

    const source =
      ctx.createBufferSource();

    source.buffer =
      audioBuffer;

    source.connect(gain);

    const now =
      ctx.currentTime;

    if (
      playAt <
      now + 0.05
    ) {
      playAt =
        now + 0.05;
    }

    source.start(playAt);

    playAt +=
      sampleCount /
      sampleRate;
  }


  button.onclick =
    async () => {

      if (
        ws &&
        ws.readyState === WebSocket.OPEN
      ) {

        ws.close();

        return;
      }

      await startAudio();

      status.textContent =
        "CONNECTING...";

      status.className = "";

      button.textContent =
        "CONNECTING...";


      const protocol =
        location.protocol === "https:"
          ? "wss:"
          : "ws:";

      ws =
        new WebSocket(
          protocol +
          "//" +
          location.host +
          "/test-audio"
        );

      ws.binaryType =
        "arraybuffer";


      ws.onopen = () => {

        status.textContent =
          "CONNECTED — WAITING FOR AUDIO";

        status.className =
          "good";

        button.textContent =
          "■ STOP LIVE AUDIO";
      };


      ws.onmessage =
        event => {

          if (
            typeof event.data === "string"
          ) {

            try {

              const msg =
                JSON.parse(event.data);

              if (msg.type === "audio") {

                status.textContent =
                  "LIVE AUDIO • " +
                  msg.rate +
                  " Hz";

              }

              if (msg.type === "error") {

                status.textContent =
                  msg.message;

                status.className =
                  "bad";
              }

            } catch {}

            return;
          }

          playPCM(
            event.data
          );
        };


      ws.onerror = () => {

        status.textContent =
          "WEBSOCKET ERROR";

        status.className =
          "bad";
      };


      ws.onclose = () => {

        status.textContent =
          "DISCONNECTED";

        status.className = "";

        button.textContent =
          "▶ PLAY LIVE AUDIO";

        ws = null;
      };

    };

})();
</script>

</body>
</html>`);
});


app.get("/", (req, res) => {

  res.type("text/plain").send(
`ACURA SDR AUDIO TEST
STATUS: ONLINE

TEST PAGE:
/test

FREQUENCY:
7.255 MHz LSB
`
  );

});


/* ============================================================
   TEST AUDIO WEBSOCKET
   ============================================================ */

const testWss =
  new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false
  });


server.on(
  "upgrade",
  (req, socket, head) => {

    let path;

    try {

      path =
        new URL(
          req.url,
          "http://localhost"
        ).pathname;

    } catch {

      socket.destroy();

      return;
    }


    if (path !== "/test-audio") {

      socket.destroy();

      return;
    }


    testWss.handleUpgrade(
      req,
      socket,
      head,
      ws => {

        testWss.emit(
          "connection",
          ws
        );

      }
    );

  }
);


/* ============================================================
   ONE LISTENER = ONE UBERSDR SESSION
   ============================================================ */

testWss.on(
  "connection",
  async browser => {

    log(
      "TEST BROWSER CONNECTED"
    );

    const sessionId =
      uuid();

    let upstream = null;

    let decoder = null;

    let decoderRate = 0;

    let decoderChannels = 0;

    let firstAudio = true;


    function sendJSON(obj) {

      if (
        browser.readyState ===
        WebSocket.OPEN
      ) {

        browser.send(
          JSON.stringify(obj)
        );
      }

    }


    function destroy() {

      if (upstream) {

        try {
          upstream.close();
        } catch {}

        upstream = null;
      }


      if (decoder) {

        try {
          decoder.delete();
        } catch {}

        decoder = null;
      }
    }


    try {

      await registerSession(
        sessionId
      );

    } catch (err) {

      log(
        "REGISTRATION FAILED:",
        err.message
      );

      sendJSON({
        type: "error",
        message:
          "Registration failed: " +
          err.message
      });

      return;
    }


    if (
      browser.readyState !==
      WebSocket.OPEN
    ) {
      return;
    }


    const url =
      audioUrl(sessionId);


    log(
      "OPENING UBERSDR AUDIO"
    );


    upstream =
      new WebSocket(
        url,
        {
          handshakeTimeout: 15000,
          perMessageDeflate: false,

          headers: {
            "User-Agent":
              CLIENT
          }
        }
      );


    upstream.binaryType =
      "nodebuffer";


    upstream.on(
      "open",
      () => {

        log(
          "UBERSDR AUDIO OPEN"
        );

      }
    );


    upstream.on(
      "message",
      (data, isBinary) => {

        if (!isBinary) {
          return;
        }


        const packet =
          Buffer.isBuffer(data)
            ? data
            : Buffer.from(data);


        /*
         * UberSDR V2:
         *
         * 0-7   timestamp
         * 8-11  rate
         * 12    channels
         * 13-20 signal data
         * 21+   Opus
         */

        if (
          packet.length <= 21
        ) {
          return;
        }


        const rate =
          packet.readUInt32LE(8);

        const channels =
          packet.readUInt8(12);


        if (
          ![
            8000,
            12000,
            16000,
            24000,
            48000
          ].includes(rate)
        ) {
          return;
        }


        if (
          channels !== 1 &&
          channels !== 2
        ) {
          return;
        }


        if (
          !decoder ||
          decoderRate !== rate ||
          decoderChannels !== channels
        ) {

          if (decoder) {

            try {
              decoder.delete();
            } catch {}
          }


          decoder =
            new OpusScript(
              rate,
              channels,
              OpusScript.Application.AUDIO
            );


          decoderRate =
            rate;

          decoderChannels =
            channels;


          log(
            "OPUS DECODER:",
            rate,
            "Hz",
            channels,
            "ch"
          );
        }


        const opus =
          packet.subarray(21);


        let pcm;


        try {

          pcm =
            Buffer.from(
              decoder.decode(opus)
            );

        } catch (err) {

          log(
            "OPUS DECODE ERROR:",
            err.message
          );

          return;
        }


        if (!pcm.length) {
          return;
        }


        /*
         * Downmix stereo to mono if needed.
         */

        let mono;


        if (channels === 1) {

          mono = pcm;

        } else {

          const frames =
            Math.floor(
              pcm.length / 4
            );

          mono =
            Buffer.allocUnsafe(
              frames * 2
            );


          for (
            let i = 0;
            i < frames;
            i++
          ) {

            const left =
              pcm.readInt16LE(
                i * 4
              );

            const right =
              pcm.readInt16LE(
                i * 4 + 2
              );

            const mixed =
              Math.max(
                -32768,
                Math.min(
                  32767,
                  Math.round(
                    (left + right) / 2
                  )
                )
              );


            mono.writeInt16LE(
              mixed,
              i * 2
            );
          }
        }


        /*
         * Browser packet:
         *
         * PCM1
         * rate uint32 LE
         * PCM16 LE mono
         */

        const out =
          Buffer.allocUnsafe(
            8 + mono.length
          );


        out.write(
          "PCM1",
          0,
          "ascii"
        );


        out.writeUInt32LE(
          rate,
          4
        );


        mono.copy(
          out,
          8
        );


        if (
          browser.readyState ===
          WebSocket.OPEN
        ) {

          browser.send(
            out,
            {
              binary: true
            }
          );
        }


        if (firstAudio) {

          firstAudio = false;


          log(
            "FIRST PCM AUDIO SENT TO TEST PAGE"
          );


          sendJSON({
            type: "audio",
            rate
          });


          /*
           * Reassert 7.255 LSB.
           */

          if (
            upstream.readyState ===
            WebSocket.OPEN
          ) {

            upstream.send(
              JSON.stringify({
                type: "tune",
                frequency:
                  FREQUENCY,
                mode:
                  MODE
              })
            );
          }
        }

      }
    );


    upstream.on(
      "error",
      err => {

        log(
          "UBERSDR ERROR:",
          err.message
        );


        sendJSON({
          type: "error",
          message:
            err.message
        });

      }
    );


    upstream.on(
      "close",
      (code, reason) => {

        log(
          "UBERSDR CLOSED:",
          code,
          reason
            ? reason.toString()
            : ""
        );

      }
    );


    browser.on(
      "close",
      () => {

        log(
          "TEST BROWSER DISCONNECTED"
        );

        destroy();

      }
    );

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
      "=============================="
    );
    console.log(
      "ACURA SDR AUDIO-ONLY TEST"
    );
    console.log(
      "=============================="
    );
    console.log(
      "Frequency: 7.255 MHz LSB"
    );
    console.log(
      "Test page: /test"
    );
    console.log(
      "No ACURA radio code"
    );
    console.log(
      "=============================="
    );
    console.log("");

  }
);
