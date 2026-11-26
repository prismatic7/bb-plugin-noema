// bb-plugin-noema — federated agentic memory for BB.
//
// Bridges BB threads into **your own** Noema cortex over Noema's Streamable
// HTTP MCP endpoints. Not a hosted service — you run the Noema server yourself
// (`noema serve --cortex <name> --transport http`).
//
// Registers native agent tools for Noema's everyday surface (search, recall,
// context, capture, edit, tags, lifecycle, lineage), plus `cortex_call`, a raw
// passthrough so every tool the cortex exposes — maintainer, curator,
// federation, and anything added in a future Noema release — stays reachable
// without waiting for a plugin release.
//
// Targets Noema ≥ 0.24: role-scoped MCP endpoints (/mcp, /mcp/agent,
// /mcp/maintainer, /mcp/curator, /mcp/federation), Streamable HTTP with session
// lifecycle (initialize → notifications/initialized → tools/call, re-init on
// session expiry), and multiple named cortices.
import { type BbPluginApi, type PluginAgentToolResult } from "@get-bb/plugin-sdk";
import { z } from "zod";

// ─── Shared vocabulary ───────────────────────────────────────────────

const TRACE_TYPES = [
  "fact",
  "decision",
  "preference",
  "context",
  "skill",
  "intent",
  "observation",
  "note",
] as const;

const TOOL_PROFILES = ["agent", "maintainer", "curator", "full"] as const;
type ToolProfile = (typeof TOOL_PROFILES)[number];

/** Noema's tool inventory, by role endpoint. `full` (/mcp) is the union. */
const AGENT_TOOL_NAMES = [
  "get_instructions",
  "cortex_usage",
  "cortex_identity",
  "list_traces",
  "get_trace",
  "search_traces",
  "recall_context",
  "find_similar_traces",
  "create_trace",
  "create_traces",
  "update_trace",
  "append_trace",
  "set_trace_tags",
  "append_trace_tags",
  "tag_stats",
  "vote_trace",
  "search_activity",
  "archive_trace",
  "unarchive_trace",
  "delete_trace",
  "recover_trace",
  "trace_history",
  "trace_lineage",
  "resolve_divergence",
] as const;

interface Target {
  name: string;
  baseUrl: string;
}

interface ClientOptions {
  profile: ToolProfile;
  accessKey?: string;
  author: string;
  timeoutMs: number;
}

function endpointPath(profile: ToolProfile): string {
  return profile === "full" ? "/mcp" : `/mcp/${profile}`;
}

function trimBaseUrl(url: string): string {
  return url.trim().replace(/\/+$/, "");
}

function describeFetchFailure(err: unknown, target: Target): string {
  const cause = err instanceof Error ? (err.cause as { code?: string } | undefined) : undefined;
  const code = cause?.code ?? "";
  const detail = err instanceof Error ? err.message : String(err);
  if (code === "ECONNREFUSED" || /ECONNREFUSED|fetch failed/i.test(detail)) {
    return (
      `Noema is unreachable at ${target.baseUrl} (connection refused). ` +
      `Check that \`noema serve --cortex <name> --transport http\` is running there. ` +
      `If Noema runs on another machine, expose it over the tailnet ` +
      `(\`tailscale serve --bg --https=<port> http://127.0.0.1:<noema-port>\`) or tunnel it ` +
      `(\`ssh -N -L <local-port>:127.0.0.1:<noema-port> <host>\`) and point the plugin at that URL.`
    );
  }
  if (code === "ENOTFOUND" || /ENOTFOUND|getaddrinfo/i.test(detail)) {
    return `Noema host for ${target.baseUrl} did not resolve (DNS). Check the URL in the plugin settings.`;
  }
  if (/aborted|AbortError/i.test(detail)) {
    return `Noema request to ${target.baseUrl} timed out or was cancelled.`;
  }
  return `Noema request to ${target.baseUrl} failed: ${detail}`;
}

// ─── MCP client (Streamable HTTP) ────────────────────────────────────

class NoemaError extends Error {
  constructor(
    message: string,
    readonly status?: number,
    readonly retryable = false,
  ) {
    super(message);
    this.name = "NoemaError";
  }
}

interface RpcMessage {
  jsonrpc?: string;
  id?: number | string | null;
  result?: unknown;
  error?: { code?: number; message?: string; data?: unknown };
}

/** Parse a Streamable HTTP response body, which may be SSE-framed or plain JSON. */
function parseRpcBody(contentType: string, text: string, id: number): RpcMessage | null {
  const trimmed = text.trim();
  if (!trimmed) return null;

  const matches = (msg: RpcMessage) => msg && (msg.id === id || msg.id === String(id));

  // Plain JSON wins even when the server mislabels the content type.
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed) as RpcMessage | RpcMessage[];
      if (Array.isArray(parsed)) return parsed.find(matches) ?? null;
      return matches(parsed) ? parsed : null;
    } catch {
      // fall through to SSE parsing
    }
  }

  if (contentType.includes("text/event-stream") || trimmed.includes("data:")) {
    for (const event of trimmed.split(/\r?\n\r?\n/)) {
      const data = event
        .split(/\r?\n/)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trim())
        .join("\n");
      if (!data) continue;
      try {
        const msg = JSON.parse(data) as RpcMessage;
        if (matches(msg)) return msg;
      } catch {
        // keep scanning: comment/keepalive frames are not JSON
      }
    }
  }
  return null;
}

class NoemaSession {
  private sessionId: string | null = null;
  private initialising: Promise<void> | null = null;
  private nextId = 1;

  constructor(
    readonly target: Target,
    private opts: ClientOptions,
  ) {}

  get url(): string {
    return `${trimBaseUrl(this.target.baseUrl)}${endpointPath(this.opts.profile)}`;
  }

  private headers(sessionId?: string | null): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
    };
    if (this.opts.accessKey) headers.Authorization = `Bearer ${this.opts.accessKey}`;
    if (sessionId) headers["Mcp-Session-Id"] = sessionId;
    return headers;
  }

  private async post(body: unknown, signal: AbortSignal, sessionId?: string | null): Promise<Response> {
    try {
      return await fetch(this.url, {
        method: "POST",
        headers: this.headers(sessionId),
        body: JSON.stringify(body),
        signal,
      });
    } catch (err) {
      if (signal.aborted) throw err;
      throw new NoemaError(describeFetchFailure(err, this.target), undefined, true);
    }
  }

  private async readMessage(resp: Response, id: number): Promise<RpcMessage | null> {
    const text = await resp.text();
    return parseRpcBody(resp.headers.get("content-type") ?? "", text, id);
  }

  /** One initialize → notifications/initialized handshake, deduplicated. */
  initialize(signal: AbortSignal): Promise<void> {
    if (this.sessionId) return Promise.resolve();
    if (!this.initialising) {
      this.initialising = this.doInitialize(signal).finally(() => {
        this.initialising = null;
      });
    }
    return this.initialising;
  }

  private async doInitialize(signal: AbortSignal): Promise<void> {
    const id = this.nextId++;
    const resp = await this.post(
      {
        jsonrpc: "2.0",
        method: "initialize",
        params: {
          protocolVersion: "2025-03-26",
          capabilities: {},
          clientInfo: { name: "bb-plugin-noema", version: "0.4.0" },
        },
        id,
      },
      signal,
      null,
    );

    if (resp.status === 401 || resp.status === 403) {
      throw new NoemaError(
        `Noema rejected the credentials (HTTP ${resp.status}). Set the MCP access key in the plugin settings (it must match NOEMA_MCP_KEY on the server).`,
        resp.status,
      );
    }
    if (!resp.ok) {
      const body = (await resp.text()).slice(0, 400);
      throw new NoemaError(`Noema initialize failed: HTTP ${resp.status}${body ? ` — ${body}` : ""}`, resp.status);
    }

    const message = await this.readMessage(resp, id);
    if (message?.error) throw new NoemaError(`Noema initialize error: ${message.error.message ?? "unknown"}`);
    if (!message) throw new NoemaError(`Noema initialize: no response from ${this.url}`);

    const sessionId = resp.headers.get("mcp-session-id");
    if (!sessionId) throw new NoemaError(`Noema initialize: no Mcp-Session-Id header from ${this.url}`);

    // Noema ≥ 0.22 requires the initialized notification before tool calls.
    const ack = await this.post({ jsonrpc: "2.0", method: "notifications/initialized" }, signal, sessionId);
    // 202 Accepted is the expected answer; anything else is worth surfacing.
    if (ack.status >= 400 && ack.status !== 404) {
      throw new NoemaError(`Noema initialize handshake failed: HTTP ${ack.status}`, ack.status);
    }
    this.sessionId = sessionId;
  }

  /** Call a tool; returns the joined text content. Throws NoemaError on failure. */
  async callTool(
    name: string,
    args: Record<string, unknown>,
    signal: AbortSignal,
    attempt = 0,
  ): Promise<{ text: string; isError: boolean }> {
    await this.initialize(signal);

    const id = this.nextId++;
    const resp = await this.post(
      { jsonrpc: "2.0", method: "tools/call", params: { name, arguments: args }, id },
      signal,
      this.sessionId,
    );

    // Session expired (server restarted, idle TTL): re-handshake once.
    if (resp.status === 404 && attempt === 0) {
      this.sessionId = null;
      await this.initialize(signal);
      return this.callTool(name, args, signal, attempt + 1);
    }
    if (resp.status === 401 || resp.status === 403) {
      throw new NoemaError(
        `Noema rejected the credentials (HTTP ${resp.status}). Check the MCP access key in the plugin settings.`,
        resp.status,
      );
    }
    if (!resp.ok) {
      const body = (await resp.text()).slice(0, 400);
      throw new NoemaError(`Noema HTTP ${resp.status}${body ? ` — ${body}` : ""}`, resp.status);
    }

    const message = await this.readMessage(resp, id);
    if (!message) throw new NoemaError(`Noema returned no result for ${name} from ${this.url}`);
    if (message.error) throw new NoemaError(`Noema: ${message.error.message ?? "unknown error"}`);

    const result = message.result as
      | { content?: Array<{ type?: string; text?: string }>; isError?: boolean }
      | undefined;

    if (result && Array.isArray(result.content)) {
      const text = result.content
        .map((part) => (part?.type === "text" ? (part.text ?? "") : ""))
        .filter(Boolean)
        .join("\n");
      return { text: text || "(empty result)", isError: result.isError === true };
    }
    return { text: JSON.stringify(result ?? null, null, 2), isError: false };
  }

  /** List the tools this endpoint exposes. */
  async listTools(signal: AbortSignal): Promise<string[]> {
    await this.initialize(signal);
    const id = this.nextId++;
    const resp = await this.post({ jsonrpc: "2.0", method: "tools/list", params: {}, id }, signal, this.sessionId);
    if (!resp.ok) throw new NoemaError(`Noema tools/list failed: HTTP ${resp.status}`, resp.status);
    const message = await this.readMessage(resp, id);
    const tools = (message?.result as { tools?: Array<{ name: string }> } | undefined)?.tools;
    return (tools ?? []).map((tool) => tool.name).sort();
  }

  /** Best-effort session teardown. */
  async close(): Promise<void> {
    if (!this.sessionId) return;
    const sessionId = this.sessionId;
    this.sessionId = null;
    try {
      await fetch(this.url, {
        method: "DELETE",
        headers: this.headers(sessionId),
        signal: AbortSignal.timeout(2000),
      });
    } catch {
      // A dropped session on the server is harmless.
    }
  }
}

// ─── Target pool ─────────────────────────────────────────────────────

class CortexPool {
  private sessions = new Map<string, NoemaSession>();

  constructor(
    private targets: Map<string, Target>,
    private options: ClientOptions,
    private defaultName: string,
  ) {}

  names(): string[] {
    return [...this.targets.keys()];
  }

  defaultTarget(): Target {
    return this.targets.get(this.defaultName) ?? [...this.targets.values()][0]!;
  }

  resolve(name?: string): NoemaSession {
    const target = name ? this.targets.get(name) : undefined;
    if (name && !target) {
      throw new NoemaError(
        `Unknown cortex "${name}". Configured cortices: ${this.names().join(", ") || "(none)"}. ` +
          `Add more in the plugin settings (Noema cortices) or use cortex_status to list them.`,
      );
    }
    const chosen = target ?? this.defaultTarget();
    let session = this.sessions.get(chosen.name);
    if (!session) {
      session = new NoemaSession(chosen, this.options);
      this.sessions.set(chosen.name, session);
    }
    return session;
  }

  /** Drop cached sessions (settings changed). */
  async reset(): Promise<void> {
    const sessions = [...this.sessions.values()];
    this.sessions.clear();
    await Promise.all(sessions.map((session) => session.close()));
  }

  closeAll(): Promise<void> {
    return this.reset();
  }
}

function parseTargets(raw: string): Map<string, string> {
  const map = new Map<string, string>();
  if (!raw.trim()) return map;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      for (const [name, url] of Object.entries(parsed as Record<string, unknown>)) {
        if (typeof url === "string" && url.trim()) map.set(name.trim(), url.trim());
      }
    }
  } catch {
    // Malformed JSON: ignored, surfaced through cortex_status.
  }
  return map;
}

/** Timeout that also honours the caller's abort signal. */
function scopedSignal(parent: AbortSignal, ms: number): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("timeout")), ms);
  const onAbort = () => controller.abort(parent.reason);
  parent.addEventListener("abort", onAbort, { once: true });
  return {
    signal: controller.signal,
    dispose: () => {
      clearTimeout(timer);
      parent.removeEventListener("abort", onAbort);
    },
  };
}

// ─── Tool result helpers ─────────────────────────────────────────────

function toResult(outcome: { text: string; isError: boolean }): PluginAgentToolResult {
  if (!outcome.isError) return outcome.text;
  return { content: [{ type: "text", text: outcome.text }], isError: true };
}

// ─── Plugin entry ────────────────────────────────────────────────────

export default async function plugin(bb: BbPluginApi) {
  bb.log.info("bb-plugin-noema loading");

  const settings = bb.settings.define({
    noemaHttpUrl: {
      type: "string",
      label: "Noema base URL",
      default: process.env.NOEMA_HTTP_URL ?? "http://127.0.0.1:3004",
      description:
        "Base URL of your Noema server (the MCP path is appended). Tailnet example: https://noema.example.ts.net:3005",
    },
    noemaCortex: {
      type: "string",
      label: "Default cortex",
      default: process.env.NOEMA_CORTEX ?? "coding-agents",
      description: "Cortex name served at the base URL — Noema serves one cortex per process (`noema serve --cortex <name>`).",
    },
    noemaCortices: {
      type: "string",
      label: "Additional cortices (JSON)",
      experimental_multiline: true,
      default: process.env.NOEMA_CORTICES ?? "",
      description:
        'Extra cortex name → base URL pairs, e.g. {"teaching":"https://noema.example.ts.net:3006"}. Each becomes a value for the tools\' `cortex` argument.',
    },
    noemaToolProfile: {
      type: "select",
      label: "MCP tool profile",
      options: [...TOOL_PROFILES],
      default: "full",
      description:
        "Noema endpoint to connect to: full (/mcp, every tool), agent (/mcp/agent), maintainer (/mcp/maintainer), curator (/mcp/curator). cortex_call can only reach tools the endpoint exposes.",
    },
    noemaAuthor: {
      type: "string",
      label: "Author for new traces",
      default: "bb",
      description: "Stamped on traces created with cortex_remember / cortex_remember_many when no author is given.",
    },
    noemaAccessKey: {
      type: "string",
      label: "MCP access key",
      secret: true,
      default: process.env.NOEMA_MCP_KEY ?? "",
      description: "Bearer key, matching NOEMA_MCP_KEY on the Noema host. Leave empty when the server has no key configured.",
    },
    noemaTimeoutMs: {
      type: "number",
      label: "Request timeout (ms)",
      default: 20000,
      description: "Per-call timeout for Noema requests.",
    },
    noemaInstructions: {
      type: "boolean",
      label: "Inject memory guidance into threads",
      default: true,
      description: "Append a short Noema usage section to thread instructions so agents actually use their memory.",
    },
    noemaActivityJournal: {
      type: "boolean",
      label: "Activity journal",
      default: true,
      description:
        "Append one metadata line to an 'Activity journal' trace whenever a thread goes idle or fails. Never stores thread content.",
    },
  });

  const stored = await settings.get();

  let config = {
    baseUrl: stored.noemaHttpUrl,
    cortexName: stored.noemaCortex,
    cortices: stored.noemaCortices,
    profile: stored.noemaToolProfile as ToolProfile,
    author: stored.noemaAuthor,
    accessKey: stored.noemaAccessKey,
    timeoutMs: Math.max(1000, stored.noemaTimeoutMs),
    instructions: stored.noemaInstructions,
    journal: stored.noemaActivityJournal,
  };

  function buildPool(): CortexPool {
    const targets = new Map<string, Target>();
    const defaultName = config.cortexName.trim() || "default";
    targets.set(defaultName, { name: defaultName, baseUrl: config.baseUrl });
    const extras = parseTargets(config.cortices);
    for (const [name, baseUrl] of extras) {
      if (name === defaultName) continue;
      targets.set(name, { name, baseUrl });
    }
    return new CortexPool(
      targets,
      {
        profile: config.profile,
        accessKey: config.accessKey || undefined,
        author: config.author,
        timeoutMs: config.timeoutMs,
      },
      defaultName,
    );
  }

  let pool = buildPool();

  bb.log.info(
    `[noema] cortex "${[...pool.names()].join(", ")}" via ${config.baseUrl} (${endpointPath(config.profile)})`,
  );

  settings.onChange(async (next) => {
    config = {
      baseUrl: next.noemaHttpUrl,
      cortexName: next.noemaCortex,
      cortices: next.noemaCortices,
      profile: next.noemaToolProfile as ToolProfile,
      author: next.noemaAuthor,
      accessKey: next.noemaAccessKey,
      timeoutMs: Math.max(1000, next.noemaTimeoutMs),
      instructions: next.noemaInstructions,
      journal: next.noemaActivityJournal,
    };
    await pool.closeAll();
    pool = buildPool();
    bb.log.info(`[noema] settings changed → ${config.baseUrl}${endpointPath(config.profile)}`);
  });

  // ── Per-call plumbing ────────────────────────────────────────────

  async function run(
    cortex: string | undefined,
    signal: AbortSignal,
    fn: (session: NoemaSession, signal: AbortSignal) => Promise<{ text: string; isError: boolean }>,
  ): Promise<PluginAgentToolResult> {
    const scope = scopedSignal(signal, config.timeoutMs);
    try {
      const session = pool.resolve(cortex);
      return toResult(await fn(session, scope.signal));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      bb.log.warn(`[noema] call failed: ${message}`);
      return { content: [{ type: "text", text: message }], isError: true };
    } finally {
      scope.dispose();
    }
  }

  const cortexParam = z
    .string()
    .optional()
    .describe("Cortex to target by name (see cortex_status). Omit for the configured default.");

  const tagsParam = z
    .union([z.string(), z.array(z.string())])
    .describe("Tags as a comma/semicolon-separated string or an array.");

  // ── Read tools ───────────────────────────────────────────────────

  bb.agents.registerTool({
    name: "cortex_search",
    description:
      "Search your Noema memory for traces by full-text (or semantic/hybrid) relevance. Returns ranked hits with ids, titles, dates, tags and a snippet. Start here when you need prior context.",
    presentation: { label: { pending: "Searching memory", completed: "Searched memory" }, icon: { glyph: "Zap" } },
    parameters: z.object({
      query: z.string().min(1).describe("Search query — keywords or a natural phrase."),
      all: z.boolean().optional().describe("Include archived traces."),
      mode: z.string().optional().describe("'lexical' (FTS5, default), 'semantic' or 'hybrid'."),
      cortex: cortexParam,
    }),
    async execute({ query, all, mode, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) =>
        session.callTool("search_traces", dropUndefined({ query, all, mode }), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_context",
    description:
      "Resolve several task queries against memory in one call, alongside the cortex's active preferences. Use it at the start of a task instead of many cortex_search calls.",
    presentation: { label: { pending: "Loading memory context", completed: "Loaded memory context" } },
    parameters: z.object({
      queries: z.array(z.string()).max(8).describe("Task-specific queries (maximum 8)."),
      include_preferences: z.boolean().optional().describe("Include traces tagged user-preference (default true)."),
      known_preferences_revision: z
        .string()
        .optional()
        .describe("Revision of preference bodies still in your context; omit after compaction."),
      limit_per_query: z.number().int().min(0).max(5).optional().describe("Matches per query (default 3, max 5)."),
      max_body_chars: z.number().int().min(0).max(12000).optional().describe("Body characters per trace (default 4000)."),
      all: z.boolean().optional().describe("Include archived traces."),
      mode: z.string().optional().describe("'lexical' (default), 'semantic' or 'hybrid'."),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      return run(cortex, signal, (session, signal) =>
        session.callTool("recall_context", dropUndefined(params), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_recall",
    description: "Read one memory trace by ID, including its full body, metadata and tags.",
    presentation: { label: { pending: "Reading trace", completed: "Read trace" } },
    parameters: z.object({
      id: z.string().min(1).describe("Trace ID (bare, or noema:<id>)."),
      record_usage: z.boolean().optional().describe("Count this read in the trace's usage stats."),
      cortex: cortexParam,
    }),
    async execute({ id, record_usage, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) =>
        session.callTool("get_trace", dropUndefined({ id, record_usage }), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_list",
    description: "List memory traces, optionally filtered by type, author, tag, origin or archive state.",
    presentation: { label: { pending: "Listing traces", completed: "Listed traces" } },
    parameters: z.object({
      type: z.enum(TRACE_TYPES).optional().describe("Filter by trace type."),
      author: z.string().optional().describe("Filter by author."),
      tag: z.string().optional().describe("Filter by exact tag."),
      origin: z.string().optional().describe("Filter by origin cortex."),
      archived: z.boolean().optional().describe("Only archived traces."),
      all: z.boolean().optional().describe("Include archived traces alongside active ones."),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      return run(cortex, signal, (session, signal) =>
        session.callTool("list_traces", dropUndefined(params), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_lineage",
    description: "Show a trace's derivation graph: what it was derived from and what derives from it.",
    presentation: { label: { pending: "Reading lineage", completed: "Read lineage" } },
    parameters: z.object({ id: z.string().min(1).describe("Trace ID."), cortex: cortexParam }),
    async execute({ id, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) => session.callTool("trace_lineage", { id }, signal));
    },
  });

  // ── Write tools ──────────────────────────────────────────────────

  bb.agents.registerTool({
    name: "cortex_remember",
    description:
      "Create one durable memory trace. Pick a type that reflects the intent — fact, decision, preference, context, skill, intent, observation or note. One trace per idea; do not store secrets or transcripts.",
    instructions:
      "When you learn something durable — a project convention, a decision and its reason, a user preference, a hard-won fix — save it with cortex_remember. Search first so you update rather than duplicate.",
    presentation: { label: { pending: "Saving memory", completed: "Saved memory" }, icon: { glyph: "Zap" } },
    parameters: z.object({
      title: z.string().min(1).max(100).describe("Short descriptive title (no date — the ID prepends one)."),
      type: z.enum(TRACE_TYPES).describe("Memory type."),
      body: z.string().min(1).describe("Full content (markdown)."),
      tags: z.union([z.string(), z.array(z.string())]).optional().describe("Comma-separated tags or an array."),
      derived_from: z
        .union([z.string(), z.array(z.string())])
        .optional()
        .describe("Trace IDs this memory was derived from."),
      origin: z.string().optional().describe("Origin cortex or source label."),
      author: z.string().optional().describe("Author; defaults to the plugin's configured author."),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      const payload = dropUndefined(params);
      payload.author ??= config.author;
      return run(cortex, signal, (session, signal) => session.callTool("create_trace", payload, signal));
    },
  });

  bb.agents.registerTool({
    name: "cortex_remember_many",
    description: "Create up to 16 independent traces in one call. The whole request is validated before any write.",
    presentation: { label: { pending: "Saving memories", completed: "Saved memories" } },
    parameters: z.object({
      traces: z
        .array(
          z.object({
            title: z.string().min(1).max(100),
            type: z.enum(TRACE_TYPES),
            body: z.string().min(1),
            tags: z.union([z.string(), z.array(z.string())]).optional(),
            derived_from: z.union([z.string(), z.array(z.string())]).optional(),
            origin: z.string().optional(),
            author: z.string().optional(),
          }),
        )
        .min(1)
        .max(16)
        .describe("Traces to create."),
      cortex: cortexParam,
    }),
    async execute({ traces, cortex }, { signal }) {
      const payload = traces.map((trace) => ({ ...dropUndefined(trace), author: trace.author ?? config.author }));
      return run(cortex, signal, (session, signal) => session.callTool("create_traces", { traces: payload }, signal));
    },
  });

  bb.agents.registerTool({
    name: "cortex_update",
    description:
      "Update fields of an existing trace. Only the fields you pass change; order the remaining ones explicitly when you supply a new body. Prefer this over creating a near-duplicate.",
    presentation: { label: { pending: "Updating trace", completed: "Updated trace" } },
    parameters: z.object({
      id: z.string().min(1).describe("Trace ID to update."),
      title: z.string().max(100).optional(),
      type: z.enum(TRACE_TYPES).optional(),
      body: z.string().optional(),
      tags: z.union([z.string(), z.array(z.string())]).optional().describe("Replaces existing tags."),
      derived_from: z.union([z.string(), z.array(z.string())]).optional(),
      author: z.string().optional(),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      return run(cortex, signal, (session, signal) =>
        session.callTool("update_trace", dropUndefined(params), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_append",
    description:
      "Append content to the end of an existing trace's body without reading it first. Use it to journal into a long-running trace.",
    presentation: { label: { pending: "Appending to trace", completed: "Appended to trace" } },
    parameters: z.object({
      id: z.string().min(1).describe("Trace ID."),
      content: z.string().min(1).describe("Text to append."),
      cortex: cortexParam,
    }),
    async execute({ id, content, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) => session.callTool("append_trace", { id, content }, signal));
    },
  });

  // ── Tags, votes, history ─────────────────────────────────────────

  bb.agents.registerTool({
    name: "cortex_tags",
    description:
      "Manage tags: action 'set' replaces a trace's tags, 'add' appends idempotently, 'stats' reports the cortex's tag taxonomy. Use tags for retrieval, not as a substitute for votes.",
    presentation: { label: { pending: "Updating tags", completed: "Updated tags" } },
    parameters: z.object({
      action: z.enum(["set", "add", "stats"]).describe("Operation to perform."),
      id: z.string().optional().describe("Trace ID (required for set/add)."),
      tags: tagsParam.optional(),
      limit: z.number().int().min(1).optional().describe("For 'stats': how many tags to list."),
      cortex: cortexParam,
    }),
    async execute({ action, id, tags, limit, cortex }, { signal }) {
      if (action === "stats") {
        return run(cortex, signal, (session, signal) =>
          session.callTool("tag_stats", dropUndefined({ limit }), signal),
        );
      }
      const missing = !id ? "id" : !tags ? "tags" : null;
      if (missing) {
        return {
          content: [{ type: "text", text: `cortex_tags: '${missing}' is required for action '${action}'.` }],
          isError: true,
        };
      }
      const tool = action === "set" ? "set_trace_tags" : "append_trace_tags";
      return run(cortex, signal, (session, signal) => session.callTool(tool, { id: id!, tags }, signal));
    },
  });

  bb.agents.registerTool({
    name: "cortex_vote",
    description:
      "Cast a tier-preference vote on a trace: 'up' asks for it to be promoted, 'down' for demotion. Use sparingly, only when the user has clearly indicated it.",
    presentation: { label: { pending: "Voting on trace", completed: "Voted on trace" } },
    parameters: z.object({
      id: z.string().min(1).describe("Trace ID."),
      direction: z.enum(["up", "down"]).describe("Vote direction."),
      cortex: cortexParam,
    }),
    async execute({ id, direction, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) => session.callTool("vote_trace", { id, direction }, signal));
    },
  });

  bb.agents.registerTool({
    name: "cortex_history",
    description: "Show a trace's audit trail: every mutation in chronological order.",
    presentation: { label: { pending: "Reading trace history", completed: "Read trace history" } },
    parameters: z.object({ id: z.string().min(1).describe("Trace ID."), cortex: cortexParam }),
    async execute({ id, cortex }, { signal }) {
      return run(cortex, signal, (session, signal) => session.callTool("trace_history", { id }, signal));
    },
  });

  bb.agents.registerTool({
    name: "cortex_similar",
    description:
      "Find traces related to a given trace. Useful before writing a new memory, to catch an existing one on the same subject.",
    presentation: { label: { pending: "Finding similar traces", completed: "Found similar traces" } },
    parameters: z.object({
      trace_id: z.string().min(1).describe("Source trace ID."),
      limit: z.number().int().min(0).optional().describe("Maximum matches (default 10)."),
      include_archived: z.boolean().optional(),
      mode: z.string().optional().describe("'lexical' (default), 'semantic' or 'hybrid'."),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      return run(cortex, signal, (session, signal) =>
        session.callTool("find_similar_traces", dropUndefined(params), signal),
      );
    },
  });

  bb.agents.registerTool({
    name: "cortex_resolve",
    description:
      "Resolve a divergence (concurrent edit conflict) on a trace: accept one of the versions by origin, or supply a merged body.",
    presentation: { label: { pending: "Resolving divergence", completed: "Resolved divergence" } },
    parameters: z.object({
      id: z.string().min(1).describe("Trace ID."),
      accept: z.string().optional().describe("Origin ID of the version to accept."),
      body: z.string().optional().describe("Merged body, instead of accepting a version."),
      cortex: cortexParam,
    }),
    async execute(args, { signal }) {
      const { cortex, ...params } = args;
      return run(cortex, signal, (session, signal) =>
        session.callTool("resolve_divergence", dropUndefined(params), signal),
      );
    },
  });

  // ── Lifecycle ────────────────────────────────────────────────────

  bb.agents.registerTool({
    name: "cortex_archive",
    description:
      "Change a trace's lifecycle: 'archive' and 'unarchive' keep it recoverable but out of the way; 'trash' soft-deletes it (recoverable for 30 days) and 'restore' brings it back.",
    presentation: { label: { pending: "Changing trace state", completed: "Changed trace state" } },
    parameters: z.object({
      action: z.enum(["archive", "unarchive", "trash", "restore"]).describe("Lifecycle operation."),
      id: z.string().min(1).describe("Trace ID."),
      cortex: cortexParam,
    }),
    async execute({ action, id, cortex }, { signal }) {
      const tool =
        action === "archive"
          ? "archive_trace"
          : action === "unarchive"
            ? "unarchive_trace"
            : action === "trash"
              ? "delete_trace"
              : "recover_trace";
      return run(cortex, signal, (session, signal) => session.callTool(tool, { id }, signal));
    },
  });

  // ── Discovery and diagnostics ────────────────────────────────────

  bb.agents.registerTool({
    name: "cortex_guide",
    description:
      "Read the cortex's own agent guidance (get_instructions) plus its structured usage contract (cortex_usage). Call it once when you need to know what this cortex expects of you.",
    presentation: { label: { pending: "Reading cortex guide", completed: "Read cortex guide" } },
    parameters: z.object({ cortex: cortexParam }),
    async execute({ cortex }, { signal }) {
      return run(cortex, signal, async (session, signal) => {
        const [guide, usage] = await Promise.all([
          session.callTool("get_instructions", {}, signal),
          session.callTool("cortex_usage", {}, signal),
        ]);
        return {
          text: `${guide.text}\n\n## cortex_usage\n\n${usage.text}`,
          isError: guide.isError || usage.isError,
        };
      });
    },
  });

  bb.agents.registerTool({
    name: "cortex_status",
    description:
      "Report every configured cortex: reachability, identity, Noema version and exposed tool count. Use it when a cortex_* call fails, or to find a cortex name for the `cortex` argument.",
    presentation: { label: { pending: "Checking Noema", completed: "Checked Noema" } },
    parameters: z.object({
      probe_tools: z.boolean().optional().describe("Also list the number of exposed tools (one extra request per cortex)."),
    }),
    async execute({ probe_tools }, { signal }) {
      const scope = scopedSignal(signal, Math.max(config.timeoutMs, 10000));
      const lines: string[] = [];
      try {
        for (const name of pool.names()) {
          const session = pool.resolve(name);
          const isDefault = name === pool.defaultTarget().name;
          try {
            const identity = await session.callTool("cortex_identity", {}, scope.signal);
            let tools = "";
            if (probe_tools) {
              const names = await session.listTools(scope.signal);
              tools = ` · ${names.length} tools`;
            }
            lines.push(
              `- **${name}**${isDefault ? " (default)" : ""} — ok · ${session.url}${tools}\n  ${identity.text.replace(/\n/g, "\n  ")}`,
            );
          } catch (err) {
            lines.push(`- **${name}**${isDefault ? " (default)" : ""} — FAILED · ${session.url}\n  ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      } finally {
        scope.dispose();
      }
      return lines.length
        ? `## Noema cortices\n\n${lines.join("\n")}`
        : "No cortices configured. Set the Noema base URL and cortex name in the plugin settings.";
    },
  });

  bb.agents.registerTool({
    name: "cortex_call",
    description:
      "Call any tool the connected Noema endpoint exposes, by name, with a raw argument object — maintainer, curator, federation and future tools included. Omit `tool` to list what the endpoint offers.",
    instructions:
      "Prefer the dedicated cortex_* tools. Use cortex_call for tools without one (tag_doctor, rename_tag, delete_tag, metrics_summary, consolidation_health, federation_status, sync_events, …) after listing them here.",
    presentation: { label: { pending: "Calling Noema", completed: "Called Noema" } },
    parameters: z.object({
      tool: z.string().optional().describe("Exact Noema tool name. Omit to list the endpoint's tools."),
      arguments: z
        .record(z.string(), z.unknown())
        .optional()
        .describe("Tool arguments, matching that tool's schema (see the listing)."),
      cortex: cortexParam,
    }),
    async execute({ tool, arguments: args, cortex }, { signal }) {
      const scope = scopedSignal(signal, config.timeoutMs);
      try {
        const session = pool.resolve(cortex);
        if (!tool) {
          const names = await session.listTools(scope.signal);
          const native = new Set(AGENT_TOOL_NAMES as readonly string[]);
          const extra = names.filter((name) => !native.has(name));
          return [
            `## Tools on ${session.url}`,
            "",
            names.map((name) => `- ${name}`).join("\n"),
            "",
            extra.length ? `Reachable only through cortex_call: ${extra.join(", ")}` : "",
          ].join("\n");
        }
        return toResult(await session.callTool(tool, args ?? {}, scope.signal));
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { content: [{ type: "text", text: message }], isError: true };
      } finally {
        scope.dispose();
      }
    },
  });

  // ── Thread guidance ──────────────────────────────────────────────
  // Tells the agent that durable memory exists and how to use it. Resolved
  // per thread start, so a settings change needs no plugin reload.

  bb.agents.contributeInstructions(() => {
    if (!config.instructions) return null;
    const names = pool.names();
    const others = names.filter((name) => name !== pool.defaultTarget().name);
    return [
      "## Noema memory",
      "",
      `You have durable memory in a Noema cortex ("${pool.defaultTarget().name}", served at ${config.baseUrl}).`,
      "- `cortex_search` for prior context before non-trivial work; `cortex_context` resolves several queries at once.",
      "- `cortex_remember` for anything durable you learn — a convention, a decision and its reason, a user preference, a fix.",
      "- Prefer `cortex_update` / `cortex_append` over creating a near-duplicate; search first.",
      "- Do not store secrets, credentials, or thread transcripts in memory.",
      "- `cortex_status` diagnoses a failing call; `cortex_call` reaches any tool without a dedicated wrapper.",
      others.length
        ? `- Target ${others.join(", ")} with the \`cortex\` argument when a task belongs to another cortex.`
        : "",
    ]
      .filter(Boolean)
      .join("\n");
  });

  // ── Activity journal ─────────────────────────────────────────────
  // Observe-only: one metadata line per idle/failed transition in a dedicated
  // "Activity journal" trace. Never stores thread content or assistant output.

  const JOURNAL_KEY = "activityJournalTraceId";
  const journalIdPattern = /\d{8}-activity-journal/;

  async function journalTraceId(signal: AbortSignal): Promise<string | null> {
    const cached = await bb.storage.kv.get<string>(JOURNAL_KEY);
    if (cached) return cached;
    const session = pool.resolve(undefined);
    const found = await session.callTool("search_traces", { query: "Activity journal" }, signal);
    const existing = found.text.match(journalIdPattern)?.[0];
    if (existing) {
      await bb.storage.kv.set(JOURNAL_KEY, existing);
      return existing;
    }
    const created = await session.callTool(
      "create_trace",
      {
        title: "Activity journal",
        type: "note",
        tags: "journal,bb",
        body: "# BB thread activity journal\n",
        author: config.author,
      },
      signal,
    );
    const id = created.text.match(journalIdPattern)?.[0] ?? null;
    if (id) await bb.storage.kv.set(JOURNAL_KEY, id);
    return id;
  }

  async function writeJournalLine(line: string): Promise<void> {
    if (!config.journal) return;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), config.timeoutMs);
    try {
      const session = pool.resolve(undefined);
      const id = await journalTraceId(controller.signal);
      if (!id) {
        bb.log.warn("[noema] could not resolve activity journal trace id");
        return;
      }
      await session.callTool("append_trace", { id, content: line }, controller.signal);
    } catch (err) {
      bb.log.warn(`[noema] activity journal write failed: ${err instanceof Error ? err.message : String(err)}`);
      // A stale cached id fails once; drop it so the next run rediscovers.
      await bb.storage.kv.delete(JOURNAL_KEY).catch(() => undefined);
    } finally {
      clearTimeout(timer);
    }
  }

  const stamp = () => new Date().toISOString();

  bb.events.on("thread.idle", ({ thread }) => {
    void writeJournalLine(
      `- ${stamp()} ${pool.defaultTarget().name} thread ${thread.id} (project ${thread.projectId}) idle`,
    );
  });

  bb.events.on("thread.failed", ({ thread, error }) => {
    bb.log.warn(`[noema] thread ${thread.id} failed${error ? `: ${error}` : ""}`);
    void writeJournalLine(
      `- ${stamp()} ${pool.defaultTarget().name} FAILED thread ${thread.id} (project ${thread.projectId})${error ? `: ${error}` : ""}`,
    );
  });

  // ── Cleanup ──────────────────────────────────────────────────────

  bb.onDispose(() => {
    void pool.closeAll();
    bb.log.info("bb-plugin-noema disposed");
  });
}

/** Strip undefined values so Noema receives only what the caller set. */
function dropUndefined<T extends Record<string, unknown>>(obj: T): Record<string, unknown> {
  const clean: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) clean[key] = value;
  }
  return clean;
}

// Exported for integration tests: the MCP client against a live Noema server.
export { NoemaSession, CortexPool, parseRpcBody };
