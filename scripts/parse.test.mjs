// Unit tests for the MCP response parser. No network, no Noema server needed.
// Runs against the built bundle, so `npm run build` must come first (CI does).
import { test } from "node:test";
import assert from "node:assert/strict";
import { statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

// dist/ is committed and bb builds it; fail loudly instead of testing a stale
// bundle when server.ts has moved on.
const here = dirname(fileURLToPath(import.meta.url));
const src = statSync(join(here, "..", "server.ts")).mtimeMs;
const bundle = statSync(join(here, "..", "dist", "server.js")).mtimeMs;
if (src > bundle + 1000) {
  throw new Error("dist/server.js is older than server.ts — run `npm run bundle` before testing.");
}

import { parseRpcBody } from "../dist/server.js";

test("parses a plain JSON body", () => {
  const msg = parseRpcBody("application/json", '{"jsonrpc":"2.0","id":7,"result":{"ok":true}}', 7);
  assert.deepEqual(msg?.result, { ok: true });
});

test("ignores a JSON body for another id", () => {
  assert.equal(parseRpcBody("application/json", '{"jsonrpc":"2.0","id":8,"result":{}}', 7), null);
});

test("parses SSE frames after a keepalive event with id/retry fields", () => {
  const body = 'id: 0\nretry: 3000\n\ndata: {"jsonrpc":"2.0","id":3,"result":{"ok":1}}\n\n';
  assert.deepEqual(parseRpcBody("text/event-stream", body, 3)?.result, { ok: 1 });
});

test("skips non-JSON comment frames and finds the matching event", () => {
  const body = [
    ": heartbeat",
    "",
    "data: not json",
    "",
    'data: {"jsonrpc":"2.0","id":4,"result":{"ok":2}}',
    "",
  ].join("\n");
  assert.deepEqual(parseRpcBody("text/event-stream", body, 4)?.result, { ok: 2 });
});

test("joins multi-line data payloads", () => {
  const body = 'data: {"jsonrpc":"2.0","id":5,\ndata: "result":{"ok":3}}\n\n';
  assert.deepEqual(parseRpcBody("text/event-stream", body, 5)?.result, { ok: 3 });
});

test("returns the error frame for the caller to surface", () => {
  const body = 'data: {"jsonrpc":"2.0","id":6,"error":{"message":"tool not found"}}\n\n';
  assert.equal(parseRpcBody("text/event-stream", body, 6)?.error?.message, "tool not found");
});

test("returns null for an empty body (notifications get 202)", () => {
  assert.equal(parseRpcBody("text/event-stream", "", 9), null);
});

test("accepts a JSON body mislabelled as SSE", () => {
  const body = '{"jsonrpc":"2.0","id":10,"result":{"ok":4}}';
  assert.deepEqual(parseRpcBody("text/event-stream", body, 10)?.result, { ok: 4 });
});
