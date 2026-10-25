
const https = require("https");
const { io } = require("socket.io-client");

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const MIN_LIQUIDITY_USD = 5000;
const MIN_VOLUME_24H_USD = 1000;
const MIN_BUYS_24H = 10;

const RUN_MS = 4 * 60 * 1000;
const RETRY_MS = 20000;
const MAX_MARKET_ATTEMPTS = 6;
const MAX_TOKENS_PER_RUN = 20;

if (!BOT_TOKEN || !CHAT_ID) {
  throw new Error("Telegram secrets are missing.");
}

function getJson(url, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { "User-Agent": "SolanaMemeScout/1.0" },
      timeout
    }, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch {
          reject(new Error("Invalid JSON"));
        }
      });
    });

    req.on("timeout", () => req.destroy(new Error("Timeout")));
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
      res.setEncoding("utf8");
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        try {
          const result = JSON.parse(body);
          if (res.statusCode === 200 && result.ok) {
            resolve();
          } else {
            reject(new Error(`Telegram HTTP ${res.statusCode}`));
          }
        } catch {
          reject(new Error("Invalid Telegram response"));
        }
      });
    });

    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function money(value) {
  return Number.isFinite(value)
    ? "$" + Math.round(value).toLocaleString("en-US")
    : "Not available yet";
}

async function getMetadata(uri) {
  if (!uri || !uri.startsWith("https://")) return {};

  try {
    return await getJson(uri, 7000);
  } catch {
    return {};
  }
}

function findXLink(meta) {
  const candidates = [
    meta?.twitter,
    meta?.x,
    ...(Array.isArray(meta?.extensions)
      ? meta.extensions.map(item => item?.url)
      : [])
  ].filter(value => typeof value === "string");

  return candidates.find(value =>
    /https?:\/\/(www\.)?(x\.com|twitter\.com)\//i.test(value)
  ) || null;
}

async function getMarketData(mint) {
  const url =
    `https://api.dexscreener.com/token-pairs/v1/solana/${encodeURIComponent(mint)}`;

  try {
    const pairs = await getJson(url);

    if (!Array.isArray(pairs)) return null;

    const validPairs = pairs.filter(pair =>
      pair.chainId === "solana" &&
      pair.baseToken?.address?.toLowerCase() === mint.toLowerCase()
    );

    validPairs.sort((a, b) =>
      (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
    );

    return validPairs[0] || null;
  } catch (error) {
    console.log(`Market lookup for ${mint}: ${error.message}`);
    return null;
  }
}

function marketFields(pair) {
  if (!pair) return null;

  const liquidity = pair.liquidity?.usd;
  const volume = pair.volume?.h24;
  const buys = pair.txns?.h24?.buys;
  const sells = pair.txns?.h24?.sells;
  const marketCap = pair.marketCap ?? pair.fdv;

  return {
    liquidity,
    volume,
    buys,
    sells,
    marketCap,
    price: Number(pair.priceUsd),
    url: pair.url
  };
}

function hasUsableMarketData(data) {
  return data &&
    Number.isFinite(data.liquidity) &&
    Number.isFinite(data.volume) &&
    Number.isFinite(data.buys);
}

function marketStatus(data) {
  if (!hasUsableMarketData(data)) {
    return "⚪ INSUFFICIENT MARKET DATA";
  }

  if (
    data.liquidity < MIN_LIQUIDITY_USD ||
    data.volume < MIN_VOLUME_24H_USD ||
    data.buys < MIN_BUYS_24H
  ) {
    return "🔴 BELOW MARKET FILTERS";
  }

  return "🟡 CANDIDATE — RISK CHECK NOT COMPLETE";
}

function marketText(data) {
  if (!data) {
    return [
     `Liquidity: Not verified\n` +
`24h volume: Not provided by this update\n\n` +
`X search: https://x.com/search?q=${encodeURIComponent(info.symbol || mint)}&src=typed_query&f=live\n` +
`Bubblemaps: https://app.bubblemaps.io/sol/token/${mint}\n` +
`DEX Screener: https://dexscreener.com/solana/${mint}\n` +
`Pump.fun: https://pump.fun/coin/${mint}\n`]
  }

  return [
    `Liquidity: ${money(data.liquidity)}`,
    `24h volume: ${money(data.volume)}`,
    `24h buys/sells: ${data.buys ?? "Unknown"}/${data.sells ?? "Unknown"}`,
    `Market cap/FDV: ${money(data.marketCap)}`,
    `Price: ${money(data.price)}`
  ].join("\n");
}

const socket = io("https://sol.shrine.trade", {
  reconnection: true,
  timeout: 15000
});

const seen = new Set();
const tokenInfo = new Map();
const poolToMint = new Map();
const liveAlertSent = new Set();
const tradeStats = new Map();
let stopping = false;
let handled = 0;

socket.on("connect", () => {
  console.log("Connected to Shrine launch feed.");

  socket.emit("subscribe_new_tokens", {
    protocols: ["PUMPFUN"]
  });
});

// Live stream updates can provide price and market-cap estimates.
// These are separate from DexScreener's volume and transaction data.
socket.on("token_update", async update => {
  if (!update || stopping) return;

  const pool = String(update.pool || "").replace(/^pool:/, "");
  const mint = poolToMint.get(pool);

  if (!mint) {
    console.log("Live update pool not mapped:", pool);
    return;
  }

  const info = tokenInfo.get(mint) || {};
  const price = Number(update.priceUSD);
  const marketCap = Number(update.mcapUSD);

  console.log(
    "Mapped live update:",
    info.symbol,
    "price USD:", price,
    "market cap USD:", marketCap
  );

  if (liveAlertSent.has(mint)) return;
  if (!Number.isFinite(price) || !Number.isFinite(marketCap) || marketCap <= 0) return;

  liveAlertSent.add(mint);

  try {
    await sendTelegram(
      `📈 LIVE TOKEN UPDATE\n\n` +
      `Name: ${info.name || "Unknown"}\n` +
      `Ticker: $${info.symbol || "Unknown"}\n` +
      `Mint: ${mint}\n` +
      `Live price: $${price.toFixed(10)}\n` +
      `Live market cap estimate: $${Math.round(marketCap).toLocaleString("en-US")}\n\n` +
      `Liquidity: Not verified\n` +
      `24h volume: Not provided by this update\n\n` +
      `https://pump.fun/coin/${mint}\n\n` +
      `⚠️ Market cap does not prove liquidity or safety.`
    );
  } catch (error) {
    console.error("Live update Telegram failed:", error.message);
    liveAlertSent.delete(mint);
  }
});
socket.on("new_token", async coin => {
  if (stopping || coin?.protocol !== "PUMPFUN" || !coin.mint) return;
  if (seen.has(coin.mint)) return;
  if (handled >= MAX_TOKENS_PER_RUN) return;

  seen.add(coin.mint);
  handled++;

  const mint = coin.mint;
  const name = String(coin.name || "Unknown").slice(0, 80);
  const symbol = String(coin.symbol || "Unknown").slice(0, 30);

  tokenInfo.set(mint, { name, symbol });

  console.log(`New token: ${symbol} (${mint})`);

  // Ask Shrine for live updates for this token.
  
socket.emit("subscribe", { mint }, ack => {
  console.log("Token subscription response:", mint, ack || "no response");

  if (ack?.ok) {
    const pool = String(
      ack.pool || (ack.room || "").replace(/^pool:/, "")
    );

    if (pool) {
      poolToMint.set(pool, mint);
      console.log("Mapped pool to token:", pool, mint);
    }
  }
});
  const metadata = await getMetadata(coin.uri);
  const xLink = findXLink(metadata);

  const links =
    `X search: https://x.com/search?q=${encodeURIComponent(symbol === "Unknown" ? mint : symbol)}&src=typed_query&f=live\n` +
    `Bubblemaps: https://app.bubblemaps.io/sol/token/${mint}\n` +
    `DEX Screener: https://dexscreener.com/solana/${mint}\n` +
    `Pump.fun: https://pump.fun/coin/${mint}`;

  // Send the launch alert immediately instead of waiting for market data.
  try {
    await sendTelegram(
      `🆕 NEW PUMPFUN TOKEN\n\n` +
      `Name: ${name}\n` +
      `Ticker: $${symbol}\n` +
      `Mint: ${mint}\n\n` +
      `⚪ Market data pending\n\n` +
      `X link in metadata: ${xLink || "Not found"}\n` +
      `${links}\n\n` +
      `X trend and holder clusters are not verified automatically.`
    );
  } catch (error) {
    console.error("Launch alert failed:", error.message);
  }

  // Retry market data for about two minutes.
  // If data becomes available, send a follow-up alert.
  for (let attempt = 1; attempt <= MAX_MARKET_ATTEMPTS; attempt++) {
    if (stopping) return;

    if (attempt > 1) await sleep(RETRY_MS);
    if (stopping) return;

    const pair = await getMarketData(mint);
    const data = marketFields(pair);

    console.log(
      `Market attempt ${attempt}/${MAX_MARKET_ATTEMPTS} for ${symbol}:`,
      data ? JSON.stringify(data) : "no indexed pair"
    );

    if (!hasUsableMarketData(data)) continue;

    const status = marketStatus(data);

    try {
      await sendTelegram(
        `📊 MARKET DATA UPDATE\n\n` +
        `Name: ${name}\n` +
        `Ticker: $${symbol}\n` +
        `Mint: ${mint}\n\n` +
        `${status}\n\n` +
        `${marketText(data)}\n\n` +
        `Pair: ${data.url || "Not provided"}\n` +
        `${links}\n\n` +
        `⚠️ This is not a safety approval.\n` +
        `⚠️ X trend growth and Bubblemaps clusters remain unverified.`
      );

      console.log(`Market update sent for ${symbol}`);
    } catch (error) {
      console.error("Market update alert failed:", error.message);
    }

    return;
  }

  console.log(`No usable market data during retry window: ${symbol}`);
});

socket.on("connect_error", error => {
  console.error("Feed connection error:", error.message);
});

socket.on("error", error => {
  console.error("Feed error:", error.message || error);
});

setTimeout(() => {
  stopping = true;
  socket.disconnect();
  console.log(`Finished. Tokens seen: ${handled}`);
  process.exit(0);
}, RUN_MS);
