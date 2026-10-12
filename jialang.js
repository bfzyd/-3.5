// ============================================================
// Fish Audio 中转 Worker（TTS + ASR + KV 版）
// 环境变量：
//   FISH_API_KEYS (secret, 必填)  多个密钥，逗号分隔
//   API_KEY       (secret, 可选)  你自己设的访问密钥
// KV 绑定：
//   FISH_KV       (KV Namespace, 可选) 存失效密钥
// 路由：
//   POST /        TTS（JSON 请求体）
//   POST /asr     ASR（multipart/form-data）
// 音色锁死为嘉岚3.5，TTS 模型锁死免费版
// ============================================================

const FISH_TTS_API = "https://api.fish.audio/v1/tts";
const FISH_ASR_API = "https://api.fish.audio/v1/asr";
const FISH_MODEL = "s2.1-pro-free";
const DEFAULT_VOICE = "f729a143b9a34005bdae0b21697fa41a";

const KEY_COOLDOWN_MS = 60 * 60 * 1000; // 密钥冷却 1 小时
const UPSTREAM_TIMEOUT_MS = 30000;       // 上游请求超时 30s
const RETRY_BACKOFF_MS = 1000;           // 429 退避基数

const KV_PREFIX = "dead:";

// 模块级：失效密钥表 { key: 失效时间戳 }
const deadKeys = new Map();

// KV 是否已加载过（每个 isolate 只加载一次）
let kvLoaded = false;
let kvLoadPromise = null;

// 密钥解析缓存
let cachedRaw = null;
let cachedKeys = [];

export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    const url = new URL(req.url);

    if (req.method === "GET" && (url.pathname === "/" || url.pathname === "")) {
      return new Response("Fish Audio Proxy is running.\n", {
        headers: { "Content-Type": "text/plain;charset=utf-8", ...corsHeaders() },
      });
    }

    if (req.method !== "POST") {
      return jsonError("Method Not Allowed", 405);
    }

    // ---------- 鉴权 ----------
    if (env.API_KEY) {
      const auth = req.headers.get("authorization") || "";
      const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
      if (!safeEqual(token, env.API_KEY)) {
        return jsonError("无效的 API 密钥", 401);
      }
    }

    // ---------- 路由分发 ----------
    const isASR =
      url.pathname === "/asr" ||
      req.headers.get("X-Service") === "asr";

    if (isASR) {
      return handleASR(req, env, ctx);
    }

    return handleTTS(req, env, ctx);
  },
};

// ============================================================
// TTS 处理
// ============================================================
async function handleTTS(req, env, ctx) {
  let body;
  try {
    body = await req.json();
  } catch {
    return jsonError("请求体不是合法 JSON", 400);
  }

  const text = String(body.input || body.text || "").trim();
  if (!text) return jsonError("缺少文本", 400);

  const traceparent = req.headers.get("traceparent");
  const fishBody = buildFishBody(body, text);

  const keys = parseKeys(env.FISH_API_KEYS);
  if (keys.length === 0) {
    return jsonError("未配置 FISH_API_KEYS", 500);
  }

  await ensureDeadKeysLoaded(env);

  const now = Date.now();
  gcDeadKeys(now);

  const available = keys.filter((k) => {
    const deadAt = deadKeys.get(k);
    if (deadAt && now - deadAt < KEY_COOLDOWN_MS) return false;
    if (deadAt) deadKeys.delete(k);
    return true;
  });

  if (available.length === 0) {
    return jsonError("所有 Fish 密钥均在冷却中，请稍后再试", 503);
  }

  const candidates = shuffle(available);

  let lastErr = null;
  let lastStatus = 502;
  let attempt = 0;

  for (const key of candidates) {
    const result = await trySynth(key, fishBody, env, traceparent);
    attempt++;

    if (result.ok) {
      if (deadKeys.delete(key) && env.FISH_KV) {
        ctx.waitUntil(env.FISH_KV.delete(KV_PREFIX + key).catch(() => {}));
      }
      return result.response;
    }

    lastErr = result.error;
    lastStatus = result.status || 502;

    if (result.status === 429) {
      await sleep(RETRY_BACKOFF_MS * (2 ** (attempt - 1)));
      continue;
    }

    if (result.dead) {
      markDead(key, env, ctx);
      continue;
    }

    if (result.retriable) {
      continue;
    }

    return new Response(
      result.rawBody || JSON.stringify({ error: { message: result.error } }),
      {
        status: result.status || 502,
        headers: {
          "Content-Type": result.contentType || "application/json",
          ...corsHeaders(),
        },
      }
    );
  }

  return jsonError("所有 Fish 密钥尝试失败: " + (lastErr || "未知错误"), lastStatus);
}

// ============================================================
// ASR 处理
// ============================================================
async function handleASR(req, env, ctx) {
  const keys = parseKeys(env.FISH_API_KEYS);
  if (keys.length === 0) {
    return jsonError("未配置 FISH_API_KEYS", 500);
  }

  await ensureDeadKeysLoaded(env);

  const now = Date.now();
  gcDeadKeys(now);

  const available = keys.filter((k) => {
    const deadAt = deadKeys.get(k);
    if (deadAt && now - deadAt < KEY_COOLDOWN_MS) return false;
    if (deadAt) deadKeys.delete(k);
    return true;
  });

  if (available.length === 0) {
    return jsonError("所有 Fish 密钥均在冷却中，请稍后再试", 503);
  }

  // 解析 multipart/form-data
  let formData;
  try {
    formData = await req.formData();
  } catch {
    return jsonError("请求体不是合法的 multipart/form-data", 400);
  }

  const audioFile = formData.get("audio");
  if (!audioFile || !(audioFile instanceof File)) {
    return jsonError("缺少 audio 文件", 400);
  }

  const language = formData.get("language") || undefined;
  const ignoreTimestamps = formData.get("ignore_timestamps") !== "false";

  // 模型通过请求头指定，默认 transcribe-1
  const asrModel = req.headers.get("model") || "transcribe-1";

  const traceparent = req.headers.get("traceparent");

  const candidates = shuffle(available);

  let lastErr = null;
  let lastStatus = 502;
  let attempt = 0;

  for (const key of candidates) {
    const result = await tryASR(
      key,
      audioFile,
      language,
      ignoreTimestamps,
      asrModel,
      traceparent
    );
    attempt++;

    if (result.ok) {
      if (deadKeys.delete(key) && env.FISH_KV) {
        ctx.waitUntil(env.FISH_KV.delete(KV_PREFIX + key).catch(() => {}));
      }
      return new Response(JSON.stringify(result.data), {
        status: 200,
        headers: { "Content-Type": "application/json", ...corsHeaders() },
      });
    }

    lastErr = result.error;
    lastStatus = result.status || 502;

    if (result.status === 429) {
      await sleep(RETRY_BACKOFF_MS * (2 ** (attempt - 1)));
      continue;
    }

    if (result.dead) {
      markDead(key, env, ctx);
      continue;
    }

    if (result.retriable) {
      continue;
    }

    return new Response(
      result.rawBody || JSON.stringify({ error: { message: result.error } }),
      {
        status: result.status || 502,
        headers: {
          "Content-Type": result.contentType || "application/json",
          ...corsHeaders(),
        },
      }
    );
  }

  return jsonError("所有 Fish 密钥尝试失败: " + (lastErr || "未知错误"), lastStatus);
}

// ============================================================
// KV 相关
// ============================================================
async function ensureDeadKeysLoaded(env) {
  if (kvLoaded || !env.FISH_KV) return;
  if (!kvLoadPromise) {
    kvLoadPromise = loadDeadKeys(env).finally(() => {
      kvLoaded = true;
    });
  }
  return kvLoadPromise;
}

async function loadDeadKeys(env) {
  try {
    const list = await env.FISH_KV.list({ prefix: KV_PREFIX });
    if (!list || !list.keys || list.keys.length === 0) return;

    await Promise.all(
      list.keys.map(async ({ name }) => {
        try {
          const ts = await env.FISH_KV.get(name);
          if (!ts) return;
          const key = name.slice(KV_PREFIX.length);
          const t = Number(ts);
          if (Number.isFinite(t)) deadKeys.set(key, t);
        } catch { /* 单条失败忽略 */ }
      })
    );
  } catch { /* KV 不可用，退化为纯内存 */ }
}

function markDead(key, env, ctx) {
  const ts = Date.now();
  deadKeys.set(key, ts);

  if (env.FISH_KV && ctx) {
    ctx.waitUntil(
      env.FISH_KV.put(KV_PREFIX + key, String(ts), {
        expirationTtl: Math.ceil(KEY_COOLDOWN_MS / 1000) + 60,
      }).catch(() => {})
    );
  }
}

// ============================================================
// 构造 TTS 请求体
// ============================================================
function buildFishBody(body, text) {
  const prosody = body.prosody || {};

  return {
    text,
    reference_id: DEFAULT_VOICE,
    format: body.format || "mp3",
    sample_rate: body.sample_rate || 44100,
    mp3_bitrate: body.mp3_bitrate || 128,

    temperature: body.temperature ?? 0.7,
    top_p: body.top_p ?? 0.7,

    prosody: {
      speed: clamp(Number(prosody.speed ?? body.speed) || 1.0, 0.5, 2.0),
      volume: clamp(Number(prosody.volume ?? body.volume) || 0, -20, 20),
      normalize_loudness: prosody.normalize_loudness ?? body.normalize_loudness ?? true,
    },

    chunk_length: body.chunk_length || 300,
    normalize: body.normalize ?? true,
    latency: body.latency || "normal",
    max_new_tokens: body.max_new_tokens || 1024,
    repetition_penalty: body.repetition_penalty ?? 1.2,
    min_chunk_length: body.min_chunk_length || 50,
    condition_on_previous_chunks: body.condition_on_previous_chunks ?? true,
    early_stop_threshold: body.early_stop_threshold ?? 1,
  };
}

// ============================================================
// 尝试 TTS
// ============================================================
async function trySynth(key, fishBody, env, traceparent) {
  let resp = null;
  let netErr = null;

  try {
    resp = await fetch(FISH_TTS_API, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${key}`,
        "Content-Type": "application/json",
        "model": FISH_MODEL,
        ...(traceparent ? { "traceparent": traceparent } : {}),
      },
      body: JSON.stringify(fishBody),
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch (e) {
    netErr = e;
  }

  if (!resp) {
    const msg = netErr?.name === "TimeoutError"
      ? `上游请求超时 (${UPSTREAM_TIMEOUT_MS}ms)`
      : `网络错误: ${netErr?.message || "未知"}`;
    return { ok: false, error: msg, dead: false, retriable: true };
  }

  if (resp.ok) {
    const headers = {
      "Content-Type": resp.headers.get("content-type") || "audio/mpeg",
      "Cache-Control": "no-store",
      ...corsHeaders(),
    };
    const cl = resp.headers.get("content-length");
    if (cl) headers["Content-Length"] = cl;

    return {
      ok: true,
      response: new Response(resp.body, { headers }),
    };
  }

  let errText = "";
  try { errText = await resp.text(); } catch {}

  let errMsg = errText;
  try {
    const errJson = JSON.parse(errText);
    if (errJson.message) {
      errMsg = errJson.message;
    } else if (Array.isArray(errJson) && errJson[0]?.msg) {
      errMsg = errJson.map((e) => e.msg).join("; ");
    }
  } catch { /* 保留原文 */ }

  if (resp.status >= 500) {
    return {
      ok: false,
      error: `Fish Audio ${resp.status}: ${errMsg}`,
      rawBody: errText,
      contentType: resp.headers.get("content-type"),
      status: resp.status,
      dead: false,
      retriable: true,
    };
  }

  const dead = resp.status === 401;

  return {
    ok: false,
    error: `Fish Audio ${resp.status}: ${errMsg}`,
    rawBody: errText,
    contentType: resp.headers.get("content-type"),
    status: resp.status,
    dead,
    retriable: false,
  };
}

// ============================================================
// 尝试 ASR
// ============================================================
async function tryASR(key, audioFile, language, ignoreTimestamps, asrModel, traceparent) {
  const form = new FormData();
  form.append("audio", audioFile);
  if (language) form.append("language", language);
  form.append("ignore_timestamps", String(ignoreTimestamps));

  let resp = null;
  let netErr = null;

  try {
    resp = await fetch(FISH_ASR_API, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${key}`,
        "model": asrModel,
        ...(traceparent ? { "traceparent": traceparent } : {}),
      },
      body: form,
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch (e) {
    netErr = e;
  }

  if (!resp) {
    const msg = netErr?.name === "TimeoutError"
      ? `上游请求超时 (${UPSTREAM_TIMEOUT_MS}ms)`
      : `网络错误: ${netErr?.message || "未知"}`;
    return { ok: false, error: msg, dead: false, retriable: true };
  }

  if (resp.ok) {
    let data;
    try {
      data = await resp.json();
    } catch {
      return {
        ok: false,
        error: "上游返回非法 JSON",
        status: 502,
        dead: false,
        retriable: true,
      };
    }
    return { ok: true, data };
  }

  let errText = "";
  try { errText = await resp.text(); } catch {}

  let errMsg = errText;
  try {
    const errJson = JSON.parse(errText);
    if (errJson.message) {
      errMsg = errJson.message;
    } else if (Array.isArray(errJson) && errJson[0]?.msg) {
      errMsg = errJson.map((e) => e.msg).join("; ");
    }
  } catch { /* 保留原文 */ }

  if (resp.status >= 500) {
    return {
      ok: false,
      error: `Fish Audio ${resp.status}: ${errMsg}`,
      rawBody: errText,
      contentType: resp.headers.get("content-type"),
      status: resp.status,
      dead: false,
      retriable: true,
    };
  }

  const dead = resp.status === 401;

  return {
    ok: false,
    error: `Fish Audio ${resp.status}: ${errMsg}`,
    rawBody: errText,
    contentType: resp.headers.get("content-type"),
    status: resp.status,
    dead,
    retriable: false,
  };
}

// ============================================================
// 工具
// ============================================================
function parseKeys(str) {
  const s = String(str || "");
  if (s === cachedRaw) return cachedKeys;
  cachedRaw = s;
  cachedKeys = s.split(",").map((k) => k.trim()).filter(Boolean);
  return cachedKeys;
}

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function gcDeadKeys(now) {
  for (const [k, t] of deadKeys) {
    if (now - t >= KEY_COOLDOWN_MS) deadKeys.delete(k);
  }
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function safeEqual(a, b) {
  if (a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jsonError(message, status) {
  return new Response(JSON.stringify({ error: { message } }), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };
}