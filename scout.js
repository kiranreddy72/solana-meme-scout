
const https = require("https");
const { io } = require("socket.io-client");

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const MIN_LIQUIDITY_USD = 5000;
const MIN_VOLUME_24H_USD = 1000;
const MIN_BUYS_24H = 10;
const RUN_MS = 4 * 60 * 1000;

if (!BOT_TOKEN || !CHAT_ID) {
  throw new Error("Telegram secrets are missing.");
}

function getJson(url, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { "User-Agent": "SolanaMemeScout/1.0" },
      timeout
    }, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
        try {
          resolve(JSON.parse(body));
        } catch {
          reject(new Error("Invalid JSON response"));
        }
      });
    });
    req.on("timeout", () => req.destroy(new Error("Request timeout")));
    req.on("error", reject);
  });
}

function sendTelegram(message) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      chat_id: CHAT_ID,
      text: message,
      disable_web_page_preview: true
    });

    const req = https.request({
      hostname: "api.telegram.org",
      path: `/bot${BOT_TOKEN}/sendMessage`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data)
      }
    }, res => {
      let body = "";
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        try {
          const result = JSON.parse(body);
          if (res.statusCode === 200 && result.ok) resolve();
          else reject(new Error(`Telegram error: ${res.statusCode}`));
        } catch {
          reject(new Error(`Telegram HTTP ${res.statusCode}`));
        }
      });
    });

    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getMetadata(uri) {
  if (!uri || !uri.startsWith("https://")) return {};
  try {
    return await getJson(uri, 7000);
  } catch {
    return {};
  }
}

function findSocials(meta) {
  const links = Array.isArray(meta?.extensions)
    ? meta.extensions
    : [];

  const candidates = [
    meta?.twitter,
    meta?.x,
    meta?.website,
    meta?.telegram,
    ...links.map(x => x?.url)
  ].filter(x => typeof x === "string");

  const xLink = candidates.find(x =>
    /(^|\/\/)(www\.)?(x\.com|twitter\.com)\//i.test(x)
  );

  return xLink || null;
}

async function getMarketData(mint) {
  const url =
    `https://api.dexscreener.com/token-pairs/v1/solana/${encodeURIComponent(mint)}`;

  // New launches may not have a visible pool immediately.
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const pairs = await getJson(url);
      if (Array.isArray(pairs) && pairs.length) {
        const valid = pairs.filter(p => p.chainId === "solana");
        valid.sort((a, b) =>
          (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
        );
        if (valid.length) return valid[0];
      }
    } catch (error) {
      console.log("Market lookup:", error.message);
    }
    if (attempt < 2) await sleep(8000);
  }
  return null;
}

function money(n) {
  if (!Number.isFinite(n)) return "unknown";
  return "$" + Math.round(n).toLocaleString("en-US");
}

const socket = io("https://sol.shrine.trade", {
  reconnection: true,
  timeout: 15000
});

const seen = new Set();
let stopping = false;
let handled = 0;

socket.on("connect", () => {
  console.log("Connected to launch feed.");
  socket.emit("subscribe_new_tokens", {
    protocols: ["PUMPFUN"]
  });
});

socket.on("new_token", async coin => {
  if (stopping || coin?.protocol !== "PUMPFUN" || !coin.mint) return;
  if (seen.has(coin.mint)) return;

  seen.add(coin.mint);
  handled++;

  // Keep concurrent event processing limited.
  if (handled > 60) return;

  const mint = coin.mint;
  const name = String(coin.name || "Unknown").slice(0, 80);
  const symbol = String(coin.symbol || "Unknown").slice(0, 30);

  console.log(`Checking ${symbol}: ${mint}`);

  const [pair, metadata] = await Promise.all([
    getMarketData(mint),
    getMetadata(coin.uri)
  ]);

  const liquidity = pair?.liquidity?.usd;
  const volume = pair?.volume?.h24;
  const buys = pair?.txns?.h24?.buys;
  const sells = pair?.txns?.h24?.sells;
  const marketCap = pair?.marketCap ?? pair?.fdv;
  const xLink = findSocials(metadata);

  const hasMarket = Number.isFinite(liquidity) &&
    Number.isFinite(volume) &&
    Number.isFinite(buys);

  const marketPass = hasMarket &&
    liquidity >= MIN_LIQUIDITY_USD &&
    volume >= MIN_VOLUME_24H_USD &&
    buys >= MIN_BUYS_24H;

  // Strict cluster policy: no "approved" status without an actual review.
  const status = !hasMarket
    ? "⚪ INSUFFICIENT MARKET DATA"
    : !marketPass
      ? "🔴 BELOW MARKET FILTERS"
      : "🟡 CANDIDATE — MANUAL RISK CHECK REQUIRED";

  const message =
    `🧭 SOLANA MEME SCOUT\n\n` +
    `Name: ${name}\n` +
    `Ticker: $${symbol}\n` +
    `Mint: ${mint}\n\n` +
    `${status}\n\n` +
    `Liquidity: ${money(liquidity)}\n` +
    `24h volume: ${money(volume)}\n` +
    `24h buys/sells: ${buys ?? "unknown"}/${sells ?? "unknown"}\n` +
    `Market cap/FDV: ${money(marketCap)}\n\n` +
    `X link in metadata: ${xLink || "Not found"}\n` +
    `X search: https://x.com/search?q=${encodeURIComponent(symbol === "Unknown" ? mint : symbol)}&src=typed_query&f=live\n` +
    `Bubblemaps: https://app.bubblemaps.io/sol/token/${mint}\n` +
    `DEX Screener: https://dexscreener.com/solana/${mint}\n` +
    `Pump.fun: https://pump.fun/coin/${mint}\n\n` +
    `⚠️ X trend growth is NOT verified automatically.\n` +
    `⚠️ Holder clusters are NOT verified automatically.\n` +
    `Check creator/holder links, LP conditions and sellability before considering a trade.`;

  try {
    await sendTelegram(message);
    console.log(`Alert sent for ${symbol}`);
  } catch (error) {
    console.error("Telegram send failed:", error.message);
  }
});

socket.on("connect_error", error =>
  console.error("Feed connection error:", error.message)
);

socket.on("error", error =>
  console.error("Feed error:", error.message || error)
);

setTimeout(() => {
  stopping = true;
  socket.disconnect();
  console.log(`Finished. Tokens checked: ${handled}`);
  process.exit(0);
}, RUN_MS);
