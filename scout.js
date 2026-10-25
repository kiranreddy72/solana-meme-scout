
const https = require("https");
const { io } = require("socket.io-client");

const token = process.env.TELEGRAM_BOT_TOKEN;
const chatId = process.env.TELEGRAM_CHAT_ID;

if (!token || !chatId) {
  throw new Error("Telegram secrets are missing.");
}

function sendTelegram(message) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      chat_id: chatId,
      text: message,
      disable_web_page_preview: true
    });

    const req = https.request({
      hostname: "api.telegram.org",
      path: `/bot${token}/sendMessage`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data)
      }
    }, (res) => {
      let body = "";
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        if (res.statusCode === 200) resolve();
        else reject(new Error(`Telegram error: ${res.statusCode} ${body}`));
      });
    });

    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

const socket = io("https://sol.shrine.trade", {
  reconnection: true,
  timeout: 15000
});

let stopping = false;
let count = 0;
const seen = new Set();

socket.on("connect", () => {
  console.log("Connected to new-token feed.");
  socket.emit("subscribe_new_tokens", {
    protocols: ["PUMPFUN"]
  });
});

socket.on("new_token", async (coin) => {
  if (stopping || coin.protocol !== "PUMPFUN") return;
  if (!coin.mint || seen.has(coin.mint)) return;

  seen.add(coin.mint);
  count++;

  const name = coin.name || "Unknown";
  const symbol = coin.symbol || "Unknown";
  const mint = coin.mint;

  const message =
    `🆕 NEW PUMPFUN TOKEN\n\n` +
    `Name: ${name}\n` +
    `Symbol: $${symbol}\n` +
    `Mint: ${mint}\n\n` +
    `🔎 Verify carefully before trading.\n` +
    `https://pump.fun/coin/${mint}`;

  try {
    await sendTelegram(message);
    console.log(`Alert sent: ${symbol} (${mint})`);
  } catch (error) {
    console.error("Telegram alert failed:", error.message);
  }
});

socket.on("connect_error", (error) => {
  console.error("Feed connection error:", error.message);
});

socket.on("error", (error) => {
  console.error("Feed error:", error.message || error);
});

setTimeout(() => {
  stopping = true;
  socket.disconnect();
  console.log(`Finished. New tokens seen: ${count}`);
  process.exit(0);
}, 4 * 60 * 1000);
