// Integration probe: drives the plugin's real MCP client against a live Noema
// cortex. Run with: node scripts/probe-noema.mjs <baseUrl> [cortex-name]
import { NoemaSession, parseRpcBody } from "../dist/server.js";

const baseUrl = process.argv[2] ?? process.env.NOEMA_HTTP_URL ?? "http://127.0.0.1:3004";
const name = process.argv[3] ?? "coding-agents";
const target = { name, baseUrl };
const results = [];

const runId = Date.now().toString(36);
const signal = AbortSignal.timeout(30000);
const session = new NoemaSession(target, {
  profile: "full",
  accessKey: undefined,
  author: "probe",
  timeoutMs: 20000,
});

function record(label, ok, detail) {
  results.push({ label, ok, detail });
  console.log(`${ok ? "PASS" : "FAIL"}  ${label}  ${detail ?? ""}`);
}

// 1. SSE parsing of a plain JSON body and an SSE frame.
const plain = parseRpcBody("application/json", '{"jsonrpc":"2.0","id":7,"result":{"ok":true}}', 7);
record("parseRpcBody json", plain?.result?.ok === true);
const sse = parseRpcBody(
  "text/event-stream",
  'id: 0\nretry: 3000\n\ndata: {"jsonrpc":"2.0","id":8,"result":{"ok":true}}\n\n',
  8,
);
record("parseRpcBody sse keepalive frame", sse?.result?.ok === true, JSON.stringify(sse));

// 2. Handshake + identity.
const identity = await session.callTool("cortex_identity", {}, signal);
const identityJson = JSON.parse(identity.text);
record("initialize + cortex_identity", identityJson.name === name, `name=${identityJson.name} version=${identityJson.version}`);

// 3. Tool inventory on the full endpoint.
const tools = await session.listTools(signal);
record("tools/list", tools.length > 20, `${tools.length} tools`);

// 4. Search (real FTS).
const search = await session.callTool("search_traces", { query: "noema" }, signal);
record("search_traces", !search.isError && search.text.length > 0, `${search.text.split("\n").length} lines`);

// 5. Write → read → update → append → archive → restore, then clean up.
const created = await session.callTool(
  "create_trace",
  {
    title: `plugin integration probe ${runId}`,
    type: "note",
    body: "# probe\n",
    tags: "probe,bb-plugin-noema",
    author: "bb-probe",
  },
  signal,
);
const id = created.text.match(/\d{8}-[a-z0-9-]+/)?.[0];
record("create_trace", Boolean(id), id ?? created.text.slice(0, 120));

if (id) {
  const got = await session.callTool("get_trace", { id }, signal);
  record("get_trace", got.text.includes("probe"), got.text.split("\n")[0]?.slice(0, 90));

  const updated = await session.callTool("update_trace", { id, title: `plugin integration probe ${runId} (edited)` }, signal);
  record("update_trace", !updated.isError, updated.text.split("\n")[0]?.slice(0, 90));

  const appended = await session.callTool("append_trace", { id, content: "appended line\n" }, signal);
  record("append_trace", !appended.isError, appended.text.split("\n")[0]?.slice(0, 90));

  const added = await session.callTool("append_trace_tags", { id, tags: "probe-extra" }, signal);
  record("append_trace_tags", !added.isError, added.text.split("\n")[0]?.slice(0, 90));

  const lineage = await session.callTool("trace_lineage", { id }, signal);
  record("trace_lineage", !lineage.isError, lineage.text.split("\n")[0]?.slice(0, 90));

  const history = await session.callTool("trace_history", { id }, signal);
  record("trace_history", !history.isError, `${history.text.split("\n").length} lines`);

  const archived = await session.callTool("archive_trace", { id }, signal);
  record("archive_trace", !archived.isError, archived.text.split("\n")[0]?.slice(0, 90));

  const unarchived = await session.callTool("unarchive_trace", { id }, signal);
  record("unarchive_trace", !unarchived.isError, unarchived.text.split("\n")[0]?.slice(0, 90));

  const trashed = await session.callTool("delete_trace", { id }, signal);
  record("delete_trace", !trashed.isError, trashed.text.split("\n")[0]?.slice(0, 90));

  const recovered = await session.callTool("recover_trace", { id }, signal);
  record("recover_trace", !recovered.isError, recovered.text.split("\n")[0]?.slice(0, 90));

  // Remove the probe trace for good: trash then leave it in the 30-day bin is
  // acceptable, but a second trash keeps the cortex tidy and is recoverable.
  await session.callTool("delete_trace", { id }, signal);
  record("cleanup (probe trace trashed)", true, id);
}

// 6. Error surface: an unknown tool comes back as a JSON-RPC error and must
//    surface as a readable message, not an unhandled crash.
try {
  await session.callTool("this_tool_does_not_exist", {}, signal);
  record("unknown tool surfaces an error", false, "no error raised");
} catch (err) {
  record("unknown tool surfaces an error", /tool not found/i.test(err.message), err.message);
}

await session.close();

// 7. Session expiry recovery: new session must work after close.
const second = new NoemaSession(target, { profile: "full", author: "probe", timeoutMs: 20000 });
const again = await second.callTool("cortex_identity", {}, AbortSignal.timeout(20000));
record("re-handshake after close", JSON.parse(again.text).name === name);
await second.close();

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length ? 1 : 0);
