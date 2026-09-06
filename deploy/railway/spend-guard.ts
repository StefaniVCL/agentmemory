import { DatabaseSync } from "node:sqlite";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { existsSync, mkdirSync, readFileSync, realpathSync, statSync, chmodSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { randomBytes, timingSafeEqual } from "node:crypto";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export const LIMIT_MICRO_USD = 30_000_000;
export const WINDOW_MS = 35 * 24 * 60 * 60 * 1000;
export const PRICE_REVIEW_UNTIL = Date.parse("2026-10-01T00:00:00Z");
const LEDGER = "/data/spend-guard/reservations.sqlite";
const MAX_BODY_BYTES = 100_000;
const MAX_RESPONSE_BYTES = 8_000_000;
const UPSTREAM = "https://api.openai.com";

export class GuardError extends Error {
  code: string;
  status: number;
  constructor(code: string, status = 503) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

export class ReservationLedger {
  path: string;
  db: DatabaseSync;
  identity: { dev: number; ino: number };
  clock: () => number;
  constructor(path: string, clock: () => number = Date.now) {
    if (!existsSync(path)) throw new GuardError("spend_ledger_not_initialized");
    this.path = realpathSync(path);
    this.identity = statSync(path);
    this.clock = clock;
    this.db = new DatabaseSync(path, { open: true });
    try {
      this.db.exec("PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;");
      const check = this.db.prepare("PRAGMA quick_check").get();
      if (check?.quick_check !== "ok") throw new GuardError("spend_ledger_corrupt");
      const meta = this.db.prepare("SELECT version FROM control WHERE id=1").get();
      if (meta?.version !== 1) throw new GuardError("spend_ledger_schema_mismatch");
    } catch (error) { this.db.close(); throw error; }
  }
  static initialize(path: string, now = Date.now()) {
    if (existsSync(path) || existsSync(dirname(path))) {
      throw new GuardError("refusing_to_reset_spend_ledger");
    }
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const db = new DatabaseSync(path);
    try {
      db.exec(`PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;
        BEGIN IMMEDIATE;
        CREATE TABLE control(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
          last_time INTEGER NOT NULL, frozen INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE reservations(id INTEGER PRIMARY KEY, created_at INTEGER NOT NULL,
          micro_usd INTEGER NOT NULL CHECK(micro_usd>0), kind TEXT NOT NULL);
        CREATE INDEX reservations_time ON reservations(created_at);`);
      db.prepare("INSERT INTO control(id,version,last_time) VALUES(1,1,?)").run(now);
      db.exec("COMMIT");
    } finally { db.close(); }
    chmodSync(path, 0o600);
  }
  assertIdentity() {
    const current = statSync(this.path);
    if (current.dev !== this.identity.dev || current.ino !== this.identity.ino || current.size === 0) {
      throw new GuardError("spend_ledger_replaced");
    }
  }
  reserve(kind: "chat" | "embedding") {
    this.assertIdentity();
    const cost = kind === "chat" ? 30_000 : 10_000;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const now = this.clock();
      const state = this.db.prepare("SELECT last_time, frozen FROM control WHERE id=1").get()!;
      if (!Number.isSafeInteger(now) || now < Number(state.last_time)) throw new GuardError("spend_clock_rollback");
      if (now >= PRICE_REVIEW_UNTIL) throw new GuardError("spend_price_review_required");
      if (state.frozen) throw new GuardError("spend_guard_frozen");
      const sum = this.db.prepare("SELECT COALESCE(SUM(micro_usd),0) AS total FROM reservations WHERE created_at>=?").get(now - WINDOW_MS)!;
      if (Number(sum.total) + cost > LIMIT_MICRO_USD) throw new GuardError("spend_limit_reached", 429);
      this.db.prepare("INSERT INTO reservations(created_at,micro_usd,kind) VALUES(?,?,?)").run(now, cost, kind);
      this.db.prepare("UPDATE control SET last_time=? WHERE id=1").run(now);
      this.db.exec("COMMIT");
    } catch (error) { this.db.exec("ROLLBACK"); throw error; }
  }
  freeze() { this.db.exec("UPDATE control SET frozen=1 WHERE id=1"); }
  close() { this.db.close(); }
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new GuardError("unsupported_request", 400);
  return value as Record<string, unknown>;
}
function onlyKeys(value: Record<string, unknown>, keys: string[]) {
  if (Object.keys(value).some(key => !keys.includes(key))) throw new GuardError("unsupported_request_field", 400);
}
export function validateRequest(path: string, raw: Buffer) {
  if (raw.byteLength > MAX_BODY_BYTES) throw new GuardError("request_too_large", 413);
  let parsed: unknown;
  try { parsed = JSON.parse(raw.toString("utf8")); } catch { throw new GuardError("invalid_json", 400); }
  const input = object(parsed);
  if (path === "/v1/chat/completions") {
    onlyKeys(input, ["model", "messages", "max_tokens", "n", "stream"]);
    if (input.model !== "gpt-4o-mini" || (input.n !== undefined && input.n !== 1) ||
        (input.stream !== undefined && input.stream !== false) ||
        !Number.isInteger(input.max_tokens) || Number(input.max_tokens) < 1 || Number(input.max_tokens) > 4096 ||
        !Array.isArray(input.messages) || input.messages.length < 1 || input.messages.length > 32) {
      throw new GuardError("unsupported_chat_request", 400);
    }
    const messages = input.messages.map(message => {
      const item = object(message);
      onlyKeys(item, ["role", "content"]);
      if (!["system", "user", "assistant"].includes(String(item.role)) || typeof item.content !== "string") {
        throw new GuardError("text_messages_only", 400);
      }
      return { role: item.role, content: item.content };
    });
    return { kind: "chat" as const, body: { model: "gpt-4o-mini", messages, max_tokens: input.max_tokens, n: 1, stream: false, service_tier: "default" } };
  }
  if (path === "/v1/embeddings") {
    onlyKeys(input, ["model", "input"]);
    const texts = typeof input.input === "string" ? [input.input] : input.input;
    if (input.model !== "text-embedding-3-small" || !Array.isArray(texts) || !texts.length || texts.length > 128 ||
        texts.some(text => typeof text !== "string" || !text.length || Buffer.byteLength(text, "utf8") > 8000)) {
      throw new GuardError("unsupported_embedding_request", 400);
    }
    return { kind: "embedding" as const, body: { model: "text-embedding-3-small", input: texts } };
  }
  throw new GuardError("endpoint_not_allowed", 404);
}

function send(res: ServerResponse, status: number, body: unknown) {
  if (!res.destroyed) {
    res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify(body));
  }
}
async function readBounded(stream: AsyncIterable<Uint8Array>, limit: number) {
  const parts: Buffer[] = [];
  let total = 0;
  for await (const part of stream) {
    total += part.byteLength;
    if (total > limit) throw new GuardError("body_too_large", 413);
    parts.push(Buffer.from(part));
  }
  return Buffer.concat(parts);
}
function authorized(req: IncomingMessage, token: string) {
  const actual = Buffer.from(req.headers.authorization || "");
  const expected = Buffer.from(`Bearer ${token}`);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export function createGateway(options: {
  ledger: ReservationLedger;
  upstreamKey: string;
  localToken: string;
  upstream?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}) {
  let inFlight = 0;
  return createServer(async (req, res) => {
    let admitted = false;
    try {
      if (!authorized(req, options.localToken)) throw new GuardError("unauthorized", 401);
      if (req.method !== "POST" || !["/v1/chat/completions", "/v1/embeddings"].includes(req.url || "")) {
        throw new GuardError("endpoint_not_allowed", 404);
      }
      if (req.headers["content-type"]?.split(";")[0] !== "application/json" || req.headers["content-encoding"]) {
        throw new GuardError("json_only", 400);
      }
      const request = validateRequest(req.url!, await readBounded(req, MAX_BODY_BYTES));
      if (!options.upstreamKey) throw new GuardError("upstream_key_not_configured");
      if (inFlight >= 4) throw new GuardError("spend_guard_busy", 429);
      options.ledger.reserve(request.kind);
      inFlight++; admitted = true;
      const response = await (options.fetchImpl || fetch)(`${options.upstream || UPSTREAM}${req.url}`, {
        method: "POST", redirect: "error", signal: AbortSignal.timeout(options.timeoutMs || 60_000),
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${options.upstreamKey}` },
        body: JSON.stringify(request.body),
      });
      if (!response.ok) {
        send(res, response.status >= 400 && response.status <= 599 ? response.status : 502,
          { error: { code: "upstream_request_failed", message: "Upstream request failed; reservation retained." } });
        return;
      }
      if (!response.body) throw new GuardError("upstream_empty_response", 502);
      const result = JSON.parse((await readBounded(response.body, MAX_RESPONSE_BYTES)).toString("utf8"));
      const usage = result.usage;
      if (!usage || !Number.isSafeInteger(usage.prompt_tokens) || usage.prompt_tokens < 0 || usage.prompt_tokens > 128_000 ||
          (request.kind === "chat" && (!Number.isSafeInteger(usage.completion_tokens) || usage.completion_tokens < 0 || usage.completion_tokens > 4096))) {
        options.ledger.freeze();
        throw new GuardError("unexpected_upstream_usage", 502);
      }
      send(res, 200, result);
    } catch (error) {
      const known = error instanceof GuardError;
      send(res, known ? error.status : 503, { error: { code: known ? error.code : "spend_guard_unavailable", message: "Request blocked or failed; any reservation is retained." } });
    } finally { if (admitted) inFlight--; }
  });
}

export function childEnvironment(source: NodeJS.ProcessEnv, localToken: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of ["PATH", "LANG", "NODE_ENV", "TINI_SUBREAPER", "AGENTMEMORY_SECRET", "AGENTMEMORY_DATA_DIR", "AGENTMEMORY_III_VERSION"]) {
    if (source[name] !== undefined) env[name] = source[name];
  }
  return { ...env, HOME: "/home/node", USER: "node", PORT: "3111",
    OPENAI_API_KEY: localToken, OPENAI_BASE_URL: "http://127.0.0.1:3114",
    OPENAI_EMBEDDING_BASE_URL: "http://127.0.0.1:3114", OPENAI_MODEL: "gpt-4o-mini",
    OPENAI_EMBEDDING_MODEL: "text-embedding-3-small", EMBEDDING_PROVIDER: "openai",
    MAX_TOKENS: "4096", AGENTMEMORY_AUTO_COMPRESS: "false", AGENTMEMORY_ALLOW_AGENT_SDK: "false",
    AGENTMEMORY_IMAGE_EMBEDDINGS: "false", CONSOLIDATION_ENABLED: source.CONSOLIDATION_ENABLED === "false" ? "false" : "true",
  };
}
export function assertDataMount(mountInfo: string) {
  if (!mountInfo.split("\n").some(line => line.split(" ")[4] === "/data")) throw new GuardError("persistent_data_mount_required");
}

async function main() {
  assertDataMount(readFileSync("/proc/self/mountinfo", "utf8"));
  if (process.argv[2] === "--init-ledger") {
    ReservationLedger.initialize(LEDGER);
    console.log("Spending ledger initialized. No upstream request was made.");
    return;
  }
  if (process.env.SPEND_GUARD_SETUP_ONLY === "true") {
    if (process.env.OPENAI_API_KEY) throw new GuardError("setup_requires_no_upstream_key");
    if (!existsSync(LEDGER)) ReservationLedger.initialize(LEDGER);
    const initialized = new ReservationLedger(LEDGER);
    initialized.close();
    createServer((req, res) => send(res, req.url === "/agentmemory/livez" ? 200 : 503,
      { status: "setup_required", inference_enabled: false })).listen(3111, "0.0.0.0");
    console.log("Spending ledger ready. Setup-only mode; AgentMemory and paid inference are not running.");
    return;
  }
  const ledger = new ReservationLedger(LEDGER);
  const upstreamKey = process.env.OPENAI_API_KEY || "";
  delete process.env.OPENAI_API_KEY;
  const localToken = randomBytes(32).toString("hex");
  const server = createGateway({ ledger, upstreamKey, localToken });
  server.requestTimeout = 65_000;
  server.headersTimeout = 10_000;
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(3114, "127.0.0.1", resolveListen);
  });
  const child = spawn("gosu", ["node:node", "agentmemory", ...process.argv.slice(2)], {
    env: childEnvironment(process.env, localToken), stdio: "inherit",
  });
  let stopping = false;
  const shutdown = (code: number) => {
    if (stopping) return;
    stopping = true;
    child.kill("SIGTERM"); server.close();
    setTimeout(() => process.exit(code), 1500).unref();
  };
  child.once("error", () => shutdown(1));
  child.once("exit", code => shutdown(code === 0 ? 0 : 1));
  process.once("SIGTERM", () => shutdown(0));
  process.once("SIGINT", () => shutdown(0));
  console.log("Spending guard active: USD30 conservative reservations / rolling35days; price review required by 2026-10-01 UTC.");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { console.error("Spending guard stopped safely; verify persistent ledger and configuration."); process.exitCode = 1; });
}
