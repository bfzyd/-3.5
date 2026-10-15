// ============================================================
// Fish Audio 中转 Worker（TTS + ASR + KV 版）
// 环境变量：
//   FISH_API_KEYS (secret, 必填)  多个密钥，逗号分隔
//   API_KEY       (secret, 可选)  你自己设的访问密钥
// KV 绑定：
//   FISH_KV       (KV Namespace, 可选) 存失效密钥
// 路由：
//   POST /        TTS（JSON 或 MessagePack 请求体）
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
  const ct = (req.headers.get("content-type") || "").toLowerCase();
  const useMsgpack =
    ct.includes("application/msgpack") || ct.includes("application/x-msgpack");

  let body;
  try {
    if (useMsgpack) {
      const buf = await req.arrayBuffer();
      body = msgpackDecode(new Uint8Array(buf));
    } else {
      body = await req.json();
    }
  } catch {
    return jsonError(
      useMsgpack ? "请求体不是合法 msgpack" : "请求体不是合法 JSON",
      400
    );
  }

  if (!body || typeof body !== "object") {
    return jsonError("请求体必须是对象", 400);
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
  let retryCount = 0;

  for (const key of candidates) {
    const result = await trySynth(key, fishBody, env, traceparent, useMsgpack);

    if (result.ok) {
      if (deadKeys.delete(key) && env.FISH_KV) {
        ctx.waitUntil(env.FISH_KV.delete(KV_PREFIX + key).catch(() => {}));
      }
      return result.response;
    }

    lastErr = result.error;
    lastStatus = result.status || 502;

    if (result.status === 429) {
      await sleep(RETRY_BACKOFF_MS * (2 ** retryCount));
      retryCount++;
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

  const finalStatus = lastStatus === 401 ? 503 : lastStatus;
  return jsonError("所有 Fish 密钥尝试失败: " + (lastErr || "未知错误"), finalStatus);
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
  // 原样透传：客户端不传就不带，上游默认 ignore_timestamps=true
  const ignoreTimestamps = formData.get("ignore_timestamps"); // string | null
  // 客户端传了 model 头才带上，否则上游用默认 transcribe-1
  const asrModel = req.headers.get("model") || null;

  const traceparent = req.headers.get("traceparent");

  const candidates = shuffle(available);

  let lastErr = null;
  let lastStatus = 502;
  let retryCount = 0;

  for (const key of candidates) {
    const result = await tryASR(
      key,
      audioFile,
      language,
      ignoreTimestamps,
      asrModel,
      traceparent
    );

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
      await sleep(RETRY_BACKOFF_MS * (2 ** retryCount));
      retryCount++;
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

  const finalStatus = lastStatus === 401 ? 503 : lastStatus;
  return jsonError("所有 Fish 密钥尝试失败: " + (lastErr || "未知错误"), finalStatus);
}

// ============================================================
// KV 相关
// ============================================================
async function ensureDeadKeysLoaded(env) {
  if (kvLoaded || !env.FISH_KV) return;
  if (!kvLoadPromise) {
    kvLoadPromise = loadDeadKeys(env).then((ok) => {
      if (ok) {
        kvLoaded = true;
      } else {
        // KV 暂时不可用，允许下次请求重试
        kvLoadPromise = null;
      }
    });
  }
  return kvLoadPromise;
}

// 返回 true 表示加载流程成功（KV 可用），false 表示 KV 不可用可重试
async function loadDeadKeys(env) {
  let list;
  try {
    list = await env.FISH_KV.list({ prefix: KV_PREFIX });
  } catch {
    return false;
  }

  if (!list || !list.keys || list.keys.length === 0) return true;

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

  return true;
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
async function trySynth(key, fishBody, env, traceparent, useMsgpack) {
  const contentType = useMsgpack ? "application/msgpack" : "application/json";
  const bodyBytes = useMsgpack
    ? msgpackEncode(fishBody)
    : JSON.stringify(fishBody);

  let resp = null;
  let netErr = null;

  try {
    resp = await fetch(FISH_TTS_API, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${key}`,
        "Content-Type": contentType,
        "model": FISH_MODEL,
        ...(traceparent ? { "traceparent": traceparent } : {}),
      },
      body: bodyBytes,
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
  if (ignoreTimestamps !== null && ignoreTimestamps !== undefined) {
    form.append("ignore_timestamps", ignoreTimestamps);
  }

  const headers = {
    "Authorization": `Bearer ${key}`,
    ...(traceparent ? { "traceparent": traceparent } : {}),
  };
  if (asrModel) headers["model"] = asrModel;

  let resp = null;
  let netErr = null;

  try {
    resp = await fetch(FISH_ASR_API, {
      method: "POST",
      headers,
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
// 精简 MessagePack 编解码（只覆盖 TTS 请求体所需类型）
// ============================================================
function msgpackEncode(value) {
  const chunks = [];
  encodeValue(value, chunks);
  const total = chunks.reduce((s, c) => s + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.length; }
  return out;
}

function encodeValue(v, chunks) {
  if (v === null || v === undefined) {
    chunks.push(new Uint8Array([0xc0]));
  } else if (v === true) {
    chunks.push(new Uint8Array([0xc3]));
  } else if (v === false) {
    chunks.push(new Uint8Array([0xc2]));
  } else if (typeof v === "number") {
    encodeNumber(v, chunks);
  } else if (typeof v === "string") {
    encodeString(v, chunks);
  } else if (Array.isArray(v)) {
    encodeArray(v, chunks);
  } else if (typeof v === "object") {
    encodeMap(v, chunks);
  } else {
    chunks.push(new Uint8Array([0xc0]));
  }
}

function encodeNumber(n, chunks) {
  if (Number.isInteger(n)) {
    if (n >= 0 && n <= 0x7f) {
      chunks.push(new Uint8Array([n]));
    } else if (n < 0 && n >= -32) {
      chunks.push(new Uint8Array([0xe0 | (n + 32)]));
    } else if (n >= -128 && n <= 127) {
      const b = new Uint8Array(2); b[0] = 0xd0; b[1] = n & 0xff; chunks.push(b);
    } else if (n >= -32768 && n <= 32767) {
      const b = new Uint8Array(3); b[0] = 0xd1;
      new DataView(b.buffer).setInt16(1, n); chunks.push(b);
    } else if (n >= -2147483648 && n <= 2147483647) {
      const b = new Uint8Array(5); b[0] = 0xd2;
      new DataView(b.buffer).setInt32(1, n); chunks.push(b);
    } else {
      const b = new Uint8Array(9); b[0] = 0xcb;
      new DataView(b.buffer).setFloat64(1, n); chunks.push(b);
    }
  } else {
    const b = new Uint8Array(9); b[0] = 0xcb;
    new DataView(b.buffer).setFloat64(1, n); chunks.push(b);
  }
}

function encodeString(s, chunks) {
  const bytes = new TextEncoder().encode(s);
  const len = bytes.length;
  let header;
  if (len <= 31) {
    header = new Uint8Array([0xa0 | len]);
  } else if (len <= 0xff) {
    header = new Uint8Array([0xd9, len]);
  } else if (len <= 0xffff) {
    header = new Uint8Array(3); header[0] = 0xda;
    new DataView(header.buffer).setUint16(1, len);
  } else {
    header = new Uint8Array(5); header[0] = 0xdb;
    new DataView(header.buffer).setUint32(1, len);
  }
  chunks.push(header, bytes);
}

function encodeArray(arr, chunks) {
  const len = arr.length;
  if (len <= 15) {
    chunks.push(new Uint8Array([0x90 | len]));
  } else if (len <= 0xffff) {
    const h = new Uint8Array(3); h[0] = 0xdc;
    new DataView(h.buffer).setUint16(1, len); chunks.push(h);
  } else {
    const h = new Uint8Array(5); h[0] = 0xdd;
    new DataView(h.buffer).setUint32(1, len); chunks.push(h);
  }
  for (const v of arr) encodeValue(v, chunks);
}

function encodeMap(obj, chunks) {
  const keys = Object.keys(obj).filter((k) => obj[k] !== undefined);
  const len = keys.length;
  if (len <= 15) {
    chunks.push(new Uint8Array([0x80 | len]));
  } else if (len <= 0xffff) {
    const h = new Uint8Array(3); h[0] = 0xde;
    new DataView(h.buffer).setUint16(1, len); chunks.push(h);
  } else {
    const h = new Uint8Array(5); h[0] = 0xdf;
    new DataView(h.buffer).setUint32(1, len); chunks.push(h);
  }
  for (const k of keys) {
    encodeString(k, chunks);
    encodeValue(obj[k], chunks);
  }
}

function msgpackDecode(buf) {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const state = { view, offset: 0 };
  return decodeValue(state);
}

function decodeValue(state) {
  const b = state.view.getUint8(state.offset++);
  if (b <= 0x7f) return b;
  if (b >= 0xe0) return b - 256;
  if (b >= 0xa0 && b <= 0xbf) return decodeString(state, b & 0x1f);
  if (b >= 0x90 && b <= 0x9f) return decodeArray(state, b & 0x0f);
  if (b >= 0x80 && b <= 0x8f) return decodeMap(state, b & 0x0f);

  switch (b) {
    case 0xc0: return null;
    case 0xc2: return false;
    case 0xc3: return true;
    case 0xcc: { const v = state.view.getUint8(state.offset); state.offset += 1; return v; }
    case 0xcd: { const v = state.view.getUint16(state.offset); state.offset += 2; return v; }
    case 0xce: { const v = state.view.getUint32(state.offset); state.offset += 4; return v; }
    case 0xcf: { const v = state.view.getBigUint64(state.offset); state.offset += 8; return Number(v); }
    case 0xd0: { const v = state.view.getInt8(state.offset); state.offset += 1; return v; }
    case 0xd1: { const v = state.view.getInt16(state.offset); state.offset += 2; return v; }
    case 0xd2: { const v = state.view.getInt32(state.offset); state.offset += 4; return v; }
    case 0xd3: { const v = state.view.getBigInt64(state.offset); state.offset += 8; return Number(v); }
    case 0xca: { const v = state.view.getFloat32(state.offset); state.offset += 4; return v; }
    case 0xcb: { const v = state.view.getFloat64(state.offset); state.offset += 8; return v; }
    case 0xd9: { const len = state.view.getUint8(state.offset); state.offset += 1; return decodeString(state, len); }
    case 0xda: { const len = state.view.getUint16(state.offset); state.offset += 2; return decodeString(state, len); }
    case 0xdb: { const len = state.view.getUint32(state.offset); state.offset += 4; return decodeString(state, len); }
    case 0xc4: { const len = state.view.getUint8(state.offset); state.offset += 1; return decodeBin(state, len); }
    case 0xc5: { const len = state.view.getUint16(state.offset); state.offset += 2; return decodeBin(state, len); }
    case 0xc6: { const len = state.view.getUint32(state.offset); state.offset += 4; return decodeBin(state, len); }
    case 0xdc: { const len = state.view.getUint16(state.offset); state.offset += 2; return decodeArray(state, len); }
    case 0xdd: { const len = state.view.getUint32(state.offset); state.offset += 4; return decodeArray(state, len); }
    case 0xde: { const len = state.view.getUint16(state.offset); state.offset += 2; return decodeMap(state, len); }
    case 0xdf: { const len = state.view.getUint32(state.offset); state.offset += 4; return decodeMap(state, len); }
    default:
      throw new Error("不支持的 msgpack 类型: 0x" + b.toString(16));
  }
}

function decodeString(state, len) {
  const bytes = new Uint8Array(
    state.view.buffer,
    state.view.byteOffset + state.offset,
    len
  );
  state.offset += len;
  return new TextDecoder().decode(bytes);
}

function decodeBin(state, len) {
  const bytes = new Uint8Array(
    state.view.buffer,
    state.view.byteOffset + state.offset,
    len
  );
  state.offset += len;
  return bytes;
}

function decodeArray(state, len) {
  const arr = new Array(len);
  for (let i = 0; i < len; i++) arr[i] = decodeValue(state);
  return arr;
}

function decodeMap(state, len) {
  const obj = {};
  for (let i = 0; i < len; i++) {
    const k = decodeValue(state);
    const v = decodeValue(state);
    obj[k] = v;
  }
  return obj;
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
  const max = Math.max(a.length, b.length);
  let r = a.length ^ b.length;
  for (let i = 0; i < max; i++) {
    r |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }
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
    "Access-Control-Allow-Headers":
      "Content-Type, Authorization, Accept, X-Service, model, traceparent",
    "Access-Control-Max-Age": "86400",
  };
}