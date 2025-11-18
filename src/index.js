/*
  ==========================================================
   DaSystem Log Service (v2 / API Format)
  ==========================================================
   Endpoint: POST /api
   Description:
     Receive structured logs (new request format)
     Store to D1 → Forward to Telegram (optional)
*/


export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    if (url.pathname === "/api") {
      return await handleApi(request, env);
    } else if (url.pathname === "/ably") {
      return await handleAblyWebhook(request, env);
    } else {
      return new Response("Not Found", { status: 404 });
    }
  },
};

/////////////////////////   Main Handler   /////////////////////////
async function handleApiRequest(requestId, payload) {
  const { service, instance, level, message } = payload;

  // Validate
  if (!service || !instance || level === undefined || !message) {
    return nack(requestId, "MissingField", "Missing fields in payload: service, instance, level, message");
  }

  // Save to DB
  await G_DB.prepare(
    `INSERT INTO log1 (c1, c2, i1, t1) VALUES (?, ?, ?, ?)`
  ).bind(service, instance, level, message).run();

  // Optional: forward notification (only for high levels)
  if (level >= 3) {
    await messageTelegram(payload);
  }
  return ack(requestId);
}
async function handleApi(request, env) {
  // Auth check
  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) {
    return nack("unknown", "UNAUTHORIZED", "Missing or invalid Authorization header");
  }

  const token = auth.split(" ")[1];
  if (token !== env.DA_WRITETOKEN) {
    return nack("unknown", "INVALID_TOKEN", "Token authentication failed");
  }

  // Parse body
  let body;
  try {
    body = await request.json();
  } catch {
    return nack("unknown", "INVALID_JSON", "Malformed JSON body");
  }

  const requestId = body.request_id || "unknown";
  if (!body.payload || !body.payload.message) {
    return nack(requestId, "INVALID_FIELD", "Missing required field: payload.message");
  }

  G_ENV = env;
  G_DB = env.DB;

  try {
    return await handleApiRequest(requestId, body.payload);
  } catch (err) {
    await errDelegate(`handleApiRequest failed: ${err.message}`);
    return nack(requestId, "DB_ERROR", err.message);
  }
}
/////////////////////////   Ably Webhook Handler   /////////////////////////
async function handleAblyWebhook(request, env) {

  try {
    const headerSecret = request.headers.get("X-Ably-Auth");
    if (!headerSecret || headerSecret !== env.ABLY_WEBHOOK_SECRET) {
      console.error("❌ Invalid Ably webhook secret");
      return new Response("Unauthorized", { status: 401 });
    }

    const text = await request.text();

    let body;
    try {
      body = JSON.parse(text);
    } catch (err) {
      await errDelegate(`❌ JSON parse error:${err.message}`);
      return new Response("Invalid JSON", { status: 400 });
    }

    const messages = body.items || body.messages;
    if (!messages || !Array.isArray(messages)) {
      await errDelegate("❌ No messages in webhook payload");
      return new Response("Invalid webhook format", { status: 400 });
    }

    G_ENV = env;
    G_DB = env.DB;

    for (const msg of messages) {
      if (!msg.data) {
        continue;
      }

      let msgpayload;
      try {
        msgpayload = JSON.parse(msg.data);
      } catch (err) {
        await errDelegate(`❌ Could not parse message.data JSON: ${err.message}`);
        continue;
      }

      // ✅ CHECK payload exists, not null, and is object
      const payload = msgpayload?.payload;
      if (!payload || typeof payload !== "object") {
        continue; // ❗ don't call handleApiRequest
      }

      try {
        await handleApiRequest("unknow", payload);
      } catch (err) {
        await errDelegate(`💥 handleApiRequest failed: ${err.message}`);
        await errDelegate(`Ably webhook failed: ${err.message}`);
      }
    }

    return new Response("OK", { status: 200 });

  } catch (err) {
    await errDelegate(`Webhook error: ${err.message}`);
    return new Response("Internal Server Error", { status: 500 });
  }
}





/////////////////////////   Utility   /////////////////////////
function ack(requestId) {
  return jsonResponse({ type: "ack", request_id: requestId });
}

function nack(requestId, code, message) {
  return jsonResponse(
    {
      type: "nack",
      request_id: requestId,
      payload: { status: "error", code, message },
    },
    400
  );
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function errDelegate(msg) {
  console.error(msg);
  await notifyTextTelegram(`❌ *Error*\n${msg}`);
}

async function messageTelegram(logData) {
  try {
    const msg = `${logData.message}\n\n🧩 ${logData.service}/${logData.instance}\n🔢 Level: ${logData.level}`;
    await notifyTextTelegram(msg);
  } catch (err) {
    console.error(`notifyTelegramByLevel error: ${err.message}`);
  }
}

async function notifyTextTelegram(logData) {
  try {
    if (!G_ENV) return;

    const botToken = G_ENV.LOG_TELEGRAM_BOT_TOKEN
    const chatId = G_ENV.LOG_TELEGRAM_CHAT_ID
    if (!botToken || !chatId) return;

    const apiUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    const msg = `${logData}`;

    await fetch(apiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: chatId, text: msg, parse_mode: "Markdown" }),
    });
  } catch (err) {
    console.error(`notifyTelegramByLevel error: ${err.message}`);
  }
}

/////////////////////////   Globals   /////////////////////////
let G_DB = null;
let G_ENV = null;
