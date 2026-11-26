# bb-plugin-noema

Durable memory for BB coding agents, backed by **your own** Noema cortex over
Noema's Streamable HTTP MCP endpoints. Not a hosted service — you run the Noema
server yourself (`noema serve --cortex <name> --transport http`).

The plugin registers native agent tools for Noema's everyday memory surface, plus
`cortex_call`, a raw passthrough so every tool the cortex exposes — maintainer,
curator, federation and anything a future Noema release adds — stays reachable
without waiting for a plugin release.

Built against BB 0.50 and plugin SDK 0.6; targets Noema ≥ 0.24.

## Tools

Memory work:

- **`cortex_search`** — full-text / semantic / hybrid search over traces, ranked by relevance.
- **`cortex_context`** — resolve several task queries in one call, alongside the cortex's active preferences (`recall_context`).
- **`cortex_recall`** — read one trace by ID, full body and metadata.
- **`cortex_list`** — list traces, filtered by type, author, tag, origin or archive state.
- **`cortex_remember`** — create one trace (fact, decision, preference, context, skill,
  intent, observation, note).
- **`cortex_remember_many`** — create up to 16 traces atomically.
- **`cortex_update`** — change fields on an existing trace; search first rather than duplicating.
- **`cortex_append`** — append to a trace body without reading it first.
- **`cortex_tags`** — `set` / `add` / `stats` over the tag taxonomy.
- **`cortex_vote`** — tier-preference vote (`up` / `down`) on a trace.
- **`cortex_similar`** — traces related to a given trace.
- **`cortex_history`** — a trace's audit trail.
- **`cortex_lineage`** — a trace's derivation graph.
- **`cortex_resolve`** — resolve a concurrent-edit divergence.
- **`cortex_archive`** — lifecycle: `archive`, `unarchive`, `trash` (30-day recoverable), `restore`.

Cortex introspection and escape hatch:

- **`cortex_guide`** — the cortex's own `get_instructions` plus `cortex_usage`.
- **`cortex_status`** — every configured cortex: reachability, identity, Noema version,
  exposed tool count. Run this first when a call fails.
- **`cortex_call`** — call any exposed tool by name with a raw argument object; omit
  `tool` to list what the endpoint offers.

Every tool takes an optional `cortex` argument to target one of the configured
cortices other than the default.

Tools are named `cortex_*`, not `noema_*`: an agent running inside BB usually also
has Hermes' own Noema memory provider loaded, and identically named tools from two
surfaces make routing ambiguous. `cortex_*` is the BB plugin; `noema_*` is the other.

## How it works

The plugin implements an MCP client for Noema's Streamable HTTP transport
(protocol `2025-03-26`) with the full session lifecycle: `initialize` → the
`notifications/initialized` handshake Noema requires before tool calls →
`tools/call`, re-initialising once if the session expires (HTTP 404). Handshakes
are serialised so concurrent tool calls cannot race it, responses are parsed
whether Noema frames them as SSE or as plain JSON, and sessions are torn down
with `DELETE` on dispose.

Two side channels, both opt-out:

- **Thread guidance** — a short "Noema memory" section is appended to thread
  instructions so agents actually search before they write. Resolved per thread
  start; no reload needed after a settings change.
- **Activity journal** — one metadata line per idle or failed thread, appended to
  an `Activity journal` trace (found by search, id cached in plugin KV). Never
  stores thread content, assistant output, or prompts.

## Cortexes

Noema serves **one cortex per process**, so a cortex is chosen by URL. Point the
plugin at the cortex you want as the default, and add others as name → URL pairs:

```sh
bb plugin config noema set noemaHttpUrl  "https://host.ts.net:3005"
bb plugin config noema set noemaCortex   "coding-agents"
bb plugin config noema set noemaCortices '{"teaching":"https://host.ts.net:3006"}'
```

Agents then reach the others with `cortex: "teaching"` on any tool.

## Role endpoints

Noema ≥ 0.24 exposes the same cortex at role-scoped MCP paths. `noemaToolProfile`
selects one:

| Value | Path | Tools |
|---|---|---|
| `full` (default) | `/mcp` | everything, including maintainer, curator and federation tools |
| `agent` | `/mcp/agent` | everyday memory work only |
| `maintainer` | `/mcp/maintainer` | agent tools plus diagnosis and tag maintenance |
| `curator` | `/mcp/curator` | agent tools plus consolidation |

`cortex_call` can only reach tools the selected endpoint exposes, so `full` is the
default. A role URL is a tool-set view, not an authorisation boundary.

## Settings

| Setting | Default | Description |
|---|---|---|
| `noemaHttpUrl` | `NOEMA_HTTP_URL` or `http://127.0.0.1:3004` | Noema base URL; the MCP path is appended. |
| `noemaCortex` | `NOEMA_CORTEX` or `coding-agents` | Name of the cortex served at that URL. |
| `noemaCortices` | `NOEMA_CORTICES` or empty | JSON object of extra `name → base URL` pairs. |
| `noemaToolProfile` | `full` | Which role endpoint to connect to. |
| `noemaAuthor` | `bb` | Author stamped on traces created without one. |
| `noemaAccessKey` | `NOEMA_MCP_KEY` | Bearer key matching the server's `NOEMA_MCP_KEY`. Stored as a secret; never written to the database or sent to the frontend. |
| `noemaTimeoutMs` | `20000` | Per-call request timeout. |
| `noemaInstructions` | `true` | Append memory guidance to thread instructions. |
| `noemaActivityJournal` | `true` | Write the Activity journal trace. |

## Install

```sh
npm install
bb plugin install . --yes     # local path source: edits reload with `bb plugin reload noema`
bb plugin dev .               # or watch and reload on every change
```

Set the URL for your Noema host. Over a tailnet, expose the loopback server rather
than binding it publicly:

```sh
# on the Noema host
tailscale serve --bg --https=3005 http://127.0.0.1:3004
# then, on the BB host
bb plugin config noema set noemaHttpUrl "https://<host>.<tailnet>.ts.net:3005"
```

## Verify

```sh
npm run build     # typecheck
npm test          # MCP response parser unit tests
npm run probe -- https://<host>.<tailnet>.ts.net:3005 coding-agents
```

`npm run probe` drives the real client against a live cortex: handshake, tool
inventory, search, then create → read → update → append → tag → lineage →
history → archive → restore → trash → recover, an unknown-tool error, and a
fresh handshake after close. It trashes its own probe trace at the end.

Inside a thread, `cortex_status` reports reachability per cortex; `cortex_call`
lists each endpoint's tools.

## Troubleshooting

- **Connection refused** — the Noema process is not listening on that host/port.
  Noema binds `127.0.0.1` by default: expose it with `tailscale serve` or tunnel it
  with `ssh -N -L 3004:127.0.0.1:3004 <host>`.
- **HTTP 401/403** — the MCP access key is missing or does not match
  `NOEMA_MCP_KEY` on the server.
- **`tool not found`** — the selected `noemaToolProfile` endpoint does not carry
  that tool; switch to `full` or use `/mcp/maintainer` deliberately.
- **Unknown cortex** — cortex names come from the plugin settings, not from the
  server; add the pair to `noemaCortices`.

## Layout

```
server.ts               plugin backend: MCP client, tools, journal, guidance
scripts/probe-noema.mjs integration probe against a live cortex
scripts/parse.test.mjs  unit tests for the response parser
dist/                   build output from `bb plugin build`
```
