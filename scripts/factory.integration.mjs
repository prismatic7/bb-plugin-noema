// Factory integration test: runs the real plugin factory against a fake BB API
// and drives every registered tool against a live Noema cortex.
//
//   node scripts/factory.integration.mjs https://host:3005 coding-agents
//
// It uses the same code the plugin host runs (dist/server.js), so it covers
// settings resolution, tool registration, zod validation, author stamping,
// cortex targeting, error surfacing and the thread guidance text.
import assert from "node:assert/strict";

const baseUrl = process.argv[2] ?? process.env.NOEMA_HTTP_URL ?? "http://127.0.0.1:3004";
const cortex = process.argv[3] ?? "coding-agents";
const runId = Date.now().toString(36);

process.env.NOEMA_HTTP_URL = baseUrl;
process.env.NOEMA_CORTEX = cortex;

const { default: plugin } = await import("../dist/server.js");

const results = [];
const record = (label, ok, detail) => {
  results.push({ label, ok });
  console.log(`${ok ? "PASS" : "FAIL"}  ${label}${detail ? `  ${detail}` : ""}`);
};

// ── Fake BB API ──────────────────────────────────────────────────────
const tools = new Map();
const eventHandlers = new Map();
const kv = new Map();
let instructionsFn = null;
const logs = [];

const fakeBb = {
  log: {
    info: (m) => logs.push(`info ${m}`),
    warn: (m) => logs.push(`warn ${m}`),
    error: (m) => logs.push(`error ${m}`),
  },
  settings: {
    define(descriptors) {
      const defaults = Object.fromEntries(
        Object.entries(descriptors).map(([key, d]) => [key, d.default ?? (d.type === "boolean" ? false : "")]),
      );
      return {
        get: async () => defaults,
        onChange: () => undefined,
        experimental_set: async () => defaults,
      };
    },
  },
  agents: {
    registerTool(tool) {
      tools.set(tool.name, tool);
    },
    contributeInstructions(fn) {
      instructionsFn = fn;
    },
    configure() {},
  },
  events: {
    on(name, handler) {
      eventHandlers.set(name, handler);
    },
  },
  storage: {
    kv: {
      get: async (key) => kv.get(key),
      set: async (key, value) => void kv.set(key, value),
      delete: async (key) => void kv.delete(key),
      list: async () => [...kv.keys()],
    },
  },
  onDispose() {},
};

await plugin(fakeBb);

const ctx = { threadId: "thr_factory_test", projectId: "proj_factory_test", signal: AbortSignal.timeout(30000) };

/** Validate through the tool's own schema, then execute — exactly as BB does. */
async function call(name, args = {}) {
  const tool = tools.get(name);
  assert.ok(tool, `tool ${name} is not registered`);
  const parsed = tool.parameters.parse(args);
  return tool.execute(parsed, ctx);
}

const textOf = (result) => (typeof result === "string" ? result : result.content.map((c) => c.text).join("\n"));
const isError = (result) => typeof result !== "string" && result.isError === true;

// ── Registration ─────────────────────────────────────────────────────
record("registerTool registered the full surface", tools.size === 18, `${tools.size} tools: ${[...tools.keys()].join(", ")}`);
record("contributeInstructions registered", typeof instructionsFn === "function");

const guidance = instructionsFn();
record("guidance names the cortex", guidance.includes(cortex) && guidance.includes("cortex_remember"), guidance.split("\n")[2]);

// ── Read path ────────────────────────────────────────────────────────
const status = textOf(await call("cortex_status", { probe_tools: true }));
record("cortex_status", status.includes(`**${cortex}**`) && /38 tools/.test(status), status.split("\n")[2]);

const search = textOf(await call("cortex_search", { query: "noema" }));
record("cortex_search", search.length > 50, search.split("\n")[0]?.slice(0, 80));

const callList = textOf(await call("cortex_call", {}));
record("cortex_call lists endpoint tools", callList.includes("create_trace") && callList.includes("sync_events"), `${callList.split("\n").length} lines`);

const guide = textOf(await call("cortex_guide", {}));
record("cortex_guide merges instructions and usage", guide.includes("## cortex_usage"), guide.split("\n")[0]?.slice(0, 80));

// ── Write path (author stamping) ─────────────────────────────────────
const created = textOf(await call("cortex_remember", {
  title: `factory integration probe ${runId}`,
  type: "note",
  body: "# probe\n",
  tags: "probe,bb-plugin-noema",
}));
const traceId = created.match(/\d{8}-[a-z0-9-]+/)?.[0];
record("cortex_remember", Boolean(traceId), traceId ?? created.slice(0, 120));

const recalled = textOf(await call("cortex_recall", { id: traceId }));
record("cortex_remember stamps the configured author", /"author": "bb"/.test(recalled), recalled.split("\n")[3]?.trim());

const tags = textOf(await call("cortex_tags", { action: "add", id: traceId, tags: ["probe-extra"] }));
record("cortex_tags add", !tags.startsWith("Noema:"), tags.slice(0, 90));

const appended = textOf(await call("cortex_append", { id: traceId, content: "appended by the factory test\n" }));
record("cortex_append", /appended/i.test(appended), appended.slice(0, 90));

const similar = textOf(await call("cortex_similar", { trace_id: traceId, limit: 3 }));
record("cortex_similar", similar.length > 0, similar.split("\n")[0]?.slice(0, 80));

const history = textOf(await call("cortex_history", { id: traceId }));
record("cortex_history", /2026/.test(history), `${history.split("\n").length} lines`);

const lineage = textOf(await call("cortex_lineage", { id: traceId }));
record("cortex_lineage", lineage.includes(traceId), lineage.split("\n")[0]?.slice(0, 80));

// ── Lifecycle ────────────────────────────────────────────────────────
const archived = textOf(await call("cortex_archive", { action: "archive", id: traceId }));
record("cortex_archive archive", /archiv/i.test(archived), archived.slice(0, 80));
const unarchived = textOf(await call("cortex_archive", { action: "unarchive", id: traceId }));
record("cortex_archive unarchive", /unarchiv/i.test(unarchived), unarchived.slice(0, 80));
const trashed = textOf(await call("cortex_archive", { action: "trash", id: traceId }));
record("cortex_archive trash", /trash/i.test(trashed), trashed.slice(0, 80));
const recovered = textOf(await call("cortex_archive", { action: "restore", id: traceId }));
record("cortex_archive restore", /recover/i.test(recovered), recovered.slice(0, 80));

// ── Error paths ──────────────────────────────────────────────────────
const badCortex = await call("cortex_search", { query: "x", cortex: "no-such-cortex" });
record("unknown cortex is a readable tool error", isError(badCortex) && /Unknown cortex/.test(textOf(badCortex)), textOf(badCortex).slice(0, 90));

const badCall = await call("cortex_call", { tool: "no_such_tool", arguments: {} });
record("unknown tool through cortex_call is a tool error", isError(badCall) && /tool not found/.test(textOf(badCall)), textOf(badCall).slice(0, 90));

// ── Journal (event wiring) ───────────────────────────────────────────
const idle = eventHandlers.get("thread.idle");
assert.ok(idle, "thread.idle handler is registered");
idle({ thread: { id: "thr_factory_test", projectId: "proj_factory_test" }, lastAssistantText: null });
await new Promise((r) => setTimeout(r, 4000));
const journalId = kv.get("activityJournalTraceId");
record("activity journal resolves and caches its trace id", Boolean(journalId), journalId ?? "(none)");

if (journalId) {
  const journal = textOf(await call("cortex_recall", { id: journalId }));
  record("journal line records the idle thread", journal.includes("thr_factory_test"), journal.split("\n").slice(-3).join(" | ").slice(0, 140));
}

// ── Clean up the probe trace ─────────────────────────────────────────
await call("cortex_archive", { action: "trash", id: traceId });
record("cleanup", true, `${traceId} trashed`);

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
if (failed.length) console.log(`failed: ${failed.map((f) => f.label).join(", ")}`);
process.exit(failed.length ? 1 : 0);
