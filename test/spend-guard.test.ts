import { afterEach, describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname, resolve, basename } from "node:path";
import { createServer, type Server } from "node:http";
import { DatabaseSync } from "node:sqlite";
import { ReservationLedger, createGateway, validateRequest, childEnvironment, assertDataMount,
  LIMIT_MICRO_USD, WINDOW_MS, PRICE_REVIEW_UNTIL } from "../deploy/railway/spend-guard.ts";

const NOW = Date.parse("2026-09-06T00:00:00Z");
const folders: string[] = [];
const ledgers: ReservationLedger[] = [];
const servers: Server[] = [];
function ledger(now = NOW) {
  const folder = mkdtempSync(join(tmpdir(), "spend-guard-")); folders.push(folder);
  const path = join(folder, "private", "ledger.sqlite");
  ReservationLedger.initialize(path, now);
  const instance = new ReservationLedger(path, () => now); ledgers.push(instance);
  return instance;
}
function charged(instance: ReservationLedger) {
  return Number(instance.db.prepare("SELECT COALESCE(SUM(micro_usd),0) AS total FROM reservations").get()!.total);
}
function fill(instance: ReservationLedger, amount: number, when = NOW) {
  instance.db.prepare("INSERT INTO reservations(created_at,micro_usd,kind) VALUES(?,?,?)").run(when, amount, "fixture");
}
const chat = { model: "gpt-4o-mini", messages: [{ role: "user", content: "test" }], max_tokens: 4096, stream: false };
const embed = { model: "text-embedding-3-small", input: ["test"] };
function validate(path: string, body: unknown) { return validateRequest(path, Buffer.from(JSON.stringify(body))); }
async function listen(server: Server) {
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
}
async function post(base: string, path = "/v1/chat/completions", body: unknown = chat, token = "local-only-token") {
  return fetch(`${base}${path}`, { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` }, body: JSON.stringify(body) });
}
function gateway(instance: ReservationLedger, fetchImpl: typeof fetch) {
  return createGateway({ ledger: instance, upstreamKey: "fake-upstream-key", localToken: "local-only-token", fetchImpl });
}
afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
  for (const item of ledgers.splice(0)) item.close();
  for (const folder of folders.splice(0)) {
    if (dirname(resolve(folder)) !== resolve(tmpdir()) || !basename(folder).startsWith("spend-guard-")) throw new Error("Unsafe cleanup target");
    rmSync(folder, { recursive: true, force: true });
  }
});

describe("persistent spending reservations", () => {
  it("reserves the entire worst-case charge before forwarding", () => {
    const item = ledger(); item.reserve("chat"); item.reserve("embedding");
    expect(charged(item)).toBe(40_000);
  });
  it("stops exactly at the conservative cap and survives reopening", () => {
    const item = ledger(); fill(item, LIMIT_MICRO_USD - 30_000); item.reserve("chat");
    expect(() => item.reserve("embedding")).toThrow("spend_limit_reached");
    const reopened = new ReservationLedger(item.path, () => NOW); ledgers.push(reopened);
    expect(() => reopened.reserve("chat")).toThrow("spend_limit_reached");
  });
  it("uses one durable cap across independent connections", () => {
    const first = ledger(); fill(first, LIMIT_MICRO_USD - 30_000);
    const second = new ReservationLedger(first.path, () => NOW); ledgers.push(second);
    first.reserve("chat"); expect(() => second.reserve("chat")).toThrow("spend_limit_reached");
  });
  it("retains previous-month reservations and only expires after 35 days", () => {
    const item = ledger(); fill(item, LIMIT_MICRO_USD, NOW - 7 * 86400_000);
    expect(() => item.reserve("embedding")).toThrow("spend_limit_reached");
    item.db.prepare("UPDATE reservations SET created_at=?").run(NOW - WINDOW_MS - 1);
    item.reserve("embedding");
    expect(charged(item)).toBe(LIMIT_MICRO_USD + 10_000);
  });
  it("rejects clock rollback and expired price approval", () => {
    const item = ledger(); item.clock = () => NOW - 1;
    expect(() => item.reserve("chat")).toThrow("spend_clock_rollback");
    item.clock = () => PRICE_REVIEW_UNTIL;
    expect(() => item.reserve("chat")).toThrow("spend_price_review_required");
    expect(charged(item)).toBe(0);
  });
  it("will not reset an existing or damaged ledger", () => {
    const item = ledger();
    expect(() => ReservationLedger.initialize(item.path)).toThrow("refusing_to_reset_spend_ledger");
    expect(() => new ReservationLedger(`${item.path}-missing`)).toThrow("spend_ledger_not_initialized");
    const corrupt = join(dirnameFor(item.path), "bad.sqlite"); writeFileSync(corrupt, "invalid sqlite");
    expect(() => new ReservationLedger(corrupt)).toThrow();
  });
  it("fails when the database is replaced while a process is running", () => {
    const item = ledger(); const moved = `${item.path}.old`;
    if (process.platform === "win32") item.db.close();
    renameSync(item.path, moved); writeFileSync(item.path, readFileSync(moved));
    if (process.platform === "win32") item.db = new DatabaseSync(item.path);
    expect(() => item.reserve("chat")).toThrow("spend_ledger_replaced");
  });
});
function dirnameFor(path: string) { return join(path, ".."); }

describe("allowed paid requests", () => {
  it("rebuilds text-only requests and forces standard tier", () => {
    expect(validate("/v1/chat/completions", chat).body).toMatchObject({ n: 1, stream: false, service_tier: "default" });
    expect(validate("/v1/embeddings", embed).kind).toBe("embedding");
  });
  it.each([
    ["/v1/responses", chat],
    ["/v1/chat/completions?bypass=true", chat],
    ["/v1/chat/completions", { ...chat, model: "gpt-4o" }],
    ["/v1/chat/completions", { ...chat, n: 2 }],
    ["/v1/chat/completions", { ...chat, max_tokens: 4097 }],
    ["/v1/chat/completions", { ...chat, stream: true }],
    ["/v1/chat/completions", { ...chat, tools: [] }],
    ["/v1/chat/completions", { ...chat, service_tier: "priority" }],
    ["/v1/chat/completions", { ...chat, messages: [{ role: "user", content: [{ type: "image_url", image_url: { url: "x" } }] }] }],
    ["/v1/embeddings", { ...embed, model: "text-embedding-3-large" }],
    ["/v1/embeddings", { ...embed, input: [[1, 2]] }],
    ["/v1/embeddings", { ...embed, input: ["a".repeat(8001)] }],
  ])("rejects wider billable surface %s", (path, body) => expect(() => validate(path as string, body)).toThrow());
  it("rejects oversized and malformed bodies", () => {
    expect(() => validateRequest("/v1/chat/completions", Buffer.alloc(100_001))).toThrow("request_too_large");
    expect(() => validateRequest("/v1/chat/completions", Buffer.from("{"))).toThrow("invalid_json");
  });
  it("removes real keys, alternate providers and bypass URLs from the child", () => {
    const env = childEnvironment({ OPENAI_API_KEY: "real", ANTHROPIC_API_KEY: "alternate", AWS_SECRET_ACCESS_KEY: "aws",
      OPENAI_BASE_URL: "https://bypass", OPENAI_EMBEDDING_BASE_URL: "https://bypass", AGENTMEMORY_ALLOW_AGENT_SDK: "true",
      AGENTMEMORY_SECRET: "memory-token", PATH: "path" }, "local-token");
    expect(env.OPENAI_API_KEY).toBe("local-token"); expect(env.ANTHROPIC_API_KEY).toBeUndefined();
    expect(env.AWS_SECRET_ACCESS_KEY).toBeUndefined(); expect(env.OPENAI_BASE_URL).toBe("http://127.0.0.1:3114");
    expect(env.OPENAI_EMBEDDING_BASE_URL).toBe(env.OPENAI_BASE_URL);
    expect(env.AGENTMEMORY_ALLOW_AGENT_SDK).toBe("false"); expect(env.AGENTMEMORY_SECRET).toBe("memory-token");
    expect(Object.values(env)).not.toContain("real");
  });
  it("requires a real /data mount rather than a directory or environment variable", () => {
    expect(() => assertDataMount("1 2 0:1 / / rw - overlay overlay rw")).toThrow();
    expect(() => assertDataMount("1 2 0:1 / /data rw - ext4 /dev/x rw")).not.toThrow();
  });
});

describe("fake-upstream integration", () => {
  it("forwards no more requests than reserved under concurrent load", async () => {
    const item = ledger(); fill(item, LIMIT_MICRO_USD - 30_000); let calls = 0;
    const url = await listen(gateway(item, (async () => {
      calls++; await new Promise(resolve => setTimeout(resolve, 20));
      return Response.json({ usage: { prompt_tokens: 10, completion_tokens: 2 }, choices: [] });
    }) as typeof fetch));
    const results = await Promise.all(Array.from({ length: 16 }, () => post(url)));
    expect(results.filter(result => result.status === 200)).toHaveLength(1);
    expect(calls).toBe(1); expect(charged(item)).toBe(LIMIT_MICRO_USD);
  });
  it("charges retries and ambiguous failures without revealing the upstream error", async () => {
    const item = ledger(); let calls = 0;
    const url = await listen(gateway(item, (async () => {
      calls++; throw new Error("private-upstream-key-must-never-appear");
    }) as typeof fetch));
    for (let i = 0; i < 3; i++) {
      const response = await post(url);
      expect(response.status).toBe(503); expect(await response.text()).not.toContain("private-upstream-key");
    }
    expect(calls).toBe(3); expect(charged(item)).toBe(90_000);
  });
  it("does not forward unauthorized or unapproved requests", async () => {
    const item = ledger(); let calls = 0;
    const url = await listen(gateway(item, (async () => { calls++; return Response.json({}); }) as typeof fetch));
    expect((await post(url, "/v1/chat/completions", chat, "wrong")).status).toBe(401);
    expect((await post(url, "/v1/responses")).status).toBe(404);
    expect((await post(url, "/v1/chat/completions", { ...chat, n: 2 })).status).toBe(400);
    expect(calls).toBe(0); expect(charged(item)).toBe(0);
  });
  it("covers embedding requests and freezes on unexpected usage", async () => {
    const item = ledger(); let calls = 0;
    const url = await listen(gateway(item, (async () => {
      calls++; return Response.json({ usage: { prompt_tokens: 999999 }, data: [] });
    }) as typeof fetch));
    expect((await post(url, "/v1/embeddings", embed)).status).toBe(502);
    expect((await post(url, "/v1/embeddings", embed)).status).toBe(503);
    expect(calls).toBe(1); expect(charged(item)).toBe(10_000);
  });
  it("refuses redirects on a real local HTTP upstream and retains reservation", async () => {
    let destinationCalls = 0;
    const destination = await listen(createServer((_req, res) => { destinationCalls++; res.end("unexpected"); }));
    const upstream = await listen(createServer((_req, res) => { res.writeHead(307, { Location: destination }); res.end(); }));
    const item = ledger(); const url = await listen(createGateway({ ledger: item, upstreamKey: "fake", localToken: "local-only-token", upstream }));
    expect((await post(url)).status).toBe(503); expect(destinationCalls).toBe(0); expect(charged(item)).toBe(30_000);
  });
  it("exchanges a successful embedding through an actual fake HTTP provider", async () => {
    let seenKey = "";
    const upstream = await listen(createServer(async (req, res) => {
      seenKey = req.headers.authorization || "";
      const chunks: Buffer[] = []; for await (const part of req) chunks.push(Buffer.from(part));
      expect(JSON.parse(Buffer.concat(chunks).toString())).toEqual(embed);
      res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ usage: { prompt_tokens: 1, total_tokens: 1 }, data: [{ embedding: [0] }] }));
    }));
    const item = ledger(); const url = await listen(createGateway({ ledger: item, upstreamKey: "fake", localToken: "local-only-token", upstream }));
    const result = await post(url, "/v1/embeddings", embed);
    expect(result.status).toBe(200); expect(seenKey).toBe("Bearer fake"); expect(charged(item)).toBe(10_000);
  });
  it("keeps the reservation when an upstream times out", async () => {
    const upstream = await listen(createServer((_req, _res) => {}));
    const item = ledger();
    const url = await listen(createGateway({ ledger: item, upstreamKey: "fake", localToken: "local-only-token", upstream, timeoutMs: 20 }));
    expect((await post(url)).status).toBe(503); expect(charged(item)).toBe(30_000);
  });
});
