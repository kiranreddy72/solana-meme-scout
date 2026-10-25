const https = require("https");

function sendTelegram(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    throw new Error("Telegram secrets are missing.");
  }

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
  }, res => {
    let body = "";
    res.on("data", chunk => body += chunk);
    res.on("end", () => {
      if (res.statusCode !== 200) {
        console.error("Telegram error:", body);
        process.exitCode = 1;
      } else {
        console.log("Telegram message sent.");
      }
    });
  });

  req.on("error", err => {
    console.error("Telegram request failed:", err.message);
    process.exitCode = 1;
  });

  req.write(data);
  req.end();
}

sendTelegram("✅ Solana Meme Scout: GitHub cloud test successful!");
