const CMA_DEFAULT_BASE_URL = "https://api.anthropic.com";
const CMA_API_VERSION = "2023-06-01";
const CMA_BETA = "managed-agents-2026-04-01";
const CMA_REQUEST_TIMEOUT_MS = 60_000;

export type CmaAuthHeaders = () => Promise<Record<string, string>> | Record<string, string>;

export interface CmaClientOptions {
  auth: CmaAuthHeaders;
  baseUrl?: string;
  fetch?: typeof fetch;
}

export interface CmaCustomTool {
  type: "custom";
  name: string;
  description: string;
  input_schema: unknown;
}

export type CmaUserContent =
  { type: "text"; text: string } | { type: "image"; source: { type: "base64"; media_type: string; data: string } };

export type CmaOutboundEvent =
  | { type: "user.message"; content: CmaUserContent[] }
  | { type: "user.custom_tool_result"; custom_tool_use_id: string; content: Array<{ type: "text"; text: string }> }
  | { type: "user.tool_result"; tool_use_id: string; content: Array<{ type: "text"; text: string }> }
  | { type: "user.interrupt" };

interface CmaWorkItem {
  type: "work";
  id: string;
  state: string;
  data: { type: string; id: string };
  environment_id?: string;
}

interface CmaWorkHeartbeat {
  type: string;
  lease_extended?: boolean;
  state?: string;
  last_heartbeat?: string;
  ttl_seconds?: number;
}

interface CmaStopReason {
  type: string;
  event_ids?: string[];
}

export interface CmaEvent {
  type: string;
  id?: string;
  processed_at?: string;
  content?: Array<{ type: string; text?: string; thinking?: string }>;
  thinking?: string;
  name?: string;
  input?: unknown;
  custom_tool_use_id?: string;
  stop_reason?: CmaStopReason;
  error?: { type?: string; message?: string };
}

export type CmaStreamFrame =
  | { kind: "start"; eventType: string; eventId: string }
  | { kind: "delta"; eventId: string; text: string }
  | { kind: "event"; event: CmaEvent };

interface CmaSession {
  id: string;
  status: string;
}

interface CmaSessionCreateBody {
  agent: {
    type: "agent_with_overrides";
    id: string;
    system?: string;
    model?: { id: string };
    tools?: CmaCustomTool[];
  };
  environment_id: string;
  vault_ids?: string[];
  metadata?: Record<string, string>;
}

export class CmaApiError extends Error {
  readonly status: number;
  readonly errorType: string | undefined;

  constructor(status: number, message: string, errorType?: string) {
    super(message);
    this.status = status;
    this.errorType = errorType;
  }
}

export function isTerminalCmaStatus(status: number): boolean {
  return status >= 400 && status < 500 && status !== 408 && status !== 429;
}

async function* sseData(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let data: string[] = [];
  try {
    for (;;) {
      const { done, value } = await reader.read();
      buffer += done ? "" : decoder.decode(value, { stream: true });
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) break;
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (line === "") {
          if (data.length) yield data.join("\n");
          data = [];
        } else if (line.startsWith("data:")) {
          data.push(line.slice(5).replace(/^ /, ""));
        }
      }
      if (done) {
        if (data.length) yield data.join("\n");
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

function toStreamFrame(raw: unknown): CmaStreamFrame | null {
  const frame = raw as {
    type?: string;
    event?: { type?: string; id?: string };
    event_id?: string;
    delta?: { type?: string; content?: { type?: string; text?: string } };
  } | null;
  if (!frame || typeof frame.type !== "string") return null;
  if (frame.type === "event_start") {
    if (typeof frame.event?.type !== "string" || typeof frame.event.id !== "string") return null;
    return { kind: "start", eventType: frame.event.type, eventId: frame.event.id };
  }
  if (frame.type === "event_delta") {
    if (typeof frame.event_id !== "string" || typeof frame.delta?.content?.text !== "string") return null;
    return { kind: "delta", eventId: frame.event_id, text: frame.delta.content.text };
  }
  return { kind: "event", event: frame as CmaEvent };
}

export interface CmaClient {
  createSession(body: CmaSessionCreateBody): Promise<CmaSession>;
  getSession(sessionId: string): Promise<CmaSession>;
  updateSessionTools(sessionId: string, tools: CmaCustomTool[]): Promise<void>;
  deleteSession(sessionId: string): Promise<void>;
  sendEvents(sessionId: string, events: CmaOutboundEvent[]): Promise<void>;
  listEvents(
    sessionId: string,
    opts?: { limit?: number; page?: string },
  ): Promise<{ data: CmaEvent[]; nextPage: string | null }>;
  streamEvents(
    sessionId: string,
    opts: { signal: AbortSignal; deltas?: string[] },
  ): Promise<AsyncIterable<CmaStreamFrame>>;
  pollWork(
    environmentId: string,
    opts?: { blockMs?: number; reclaimOlderThanMs?: number; workerId?: string },
  ): Promise<CmaWorkItem | null>;
  ackWork(environmentId: string, workId: string): Promise<CmaWorkItem>;
  heartbeatWork(
    environmentId: string,
    workId: string,
    opts?: { desiredTtlSeconds?: number; expectedLastHeartbeat?: string },
  ): Promise<CmaWorkHeartbeat>;
  stopWork(environmentId: string, workId: string, force?: boolean): Promise<void>;
}

async function* frames(body: ReadableStream<Uint8Array>): AsyncGenerator<CmaStreamFrame> {
  for await (const data of sseData(body)) {
    const parsed = (() => {
      try {
        return JSON.parse(data) as unknown;
      } catch {
        return null;
      }
    })();
    const frame = toStreamFrame(parsed);
    if (frame) yield frame;
  }
}

export function createCmaClient(options: CmaClientOptions): CmaClient {
  const baseUrl = (options.baseUrl ?? CMA_DEFAULT_BASE_URL).replace(/\/$/, "");
  const doFetch = options.fetch ?? fetch;

  const headers = async (): Promise<Record<string, string>> => ({
    "anthropic-version": CMA_API_VERSION,
    "anthropic-beta": CMA_BETA,
    "content-type": "application/json",
    ...(await options.auth()),
  });

  const request = async (method: string, path: string, body?: unknown): Promise<unknown> => {
    const response = await doFetch(`${baseUrl}${path}`, {
      method,
      headers: await headers(),
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(CMA_REQUEST_TIMEOUT_MS),
    });
    const text = await response.text();
    if (!response.ok) {
      const parsed = (() => {
        try {
          return JSON.parse(text) as { error?: { type?: string; message?: string } };
        } catch {
          return null;
        }
      })();
      const message = parsed?.error?.message || text.slice(0, 500) || response.statusText;
      throw new CmaApiError(
        response.status,
        `CMA ${method} ${path} failed (${response.status}): ${message}`,
        parsed?.error?.type,
      );
    }
    if (!text) return null;
    return JSON.parse(text);
  };

  return {
    async createSession(body) {
      return (await request("POST", "/v1/sessions", body)) as CmaSession;
    },
    async getSession(sessionId) {
      return (await request("GET", `/v1/sessions/${encodeURIComponent(sessionId)}`)) as CmaSession;
    },
    async updateSessionTools(sessionId, tools) {
      await request("POST", `/v1/sessions/${encodeURIComponent(sessionId)}`, { agent: { tools } });
    },
    async deleteSession(sessionId) {
      await request("DELETE", `/v1/sessions/${encodeURIComponent(sessionId)}`);
    },
    async sendEvents(sessionId, events) {
      await request("POST", `/v1/sessions/${encodeURIComponent(sessionId)}/events`, { events });
    },
    async listEvents(sessionId, opts) {
      const query = new URLSearchParams();
      if (opts?.limit) query.set("limit", String(opts.limit));
      if (opts?.page) query.set("page", opts.page);
      const suffix = query.size ? `?${query}` : "";
      const listed = (await request("GET", `/v1/sessions/${encodeURIComponent(sessionId)}/events${suffix}`)) as {
        data?: CmaEvent[];
        next_page?: string | null;
      };
      return { data: listed.data ?? [], nextPage: listed.next_page ?? null };
    },
    async pollWork(environmentId, opts) {
      const query = new URLSearchParams();
      if (opts?.blockMs) query.set("block_ms", String(opts.blockMs));
      if (opts?.reclaimOlderThanMs) query.set("reclaim_older_than_ms", String(opts.reclaimOlderThanMs));
      const suffix = query.size ? `?${query}` : "";
      const response = await doFetch(
        `${baseUrl}/v1/environments/${encodeURIComponent(environmentId)}/work/poll${suffix}`,
        {
          method: "GET",
          headers: {
            ...(await headers()),
            ...(opts?.workerId ? { "anthropic-worker-id": opts.workerId } : {}),
          },
          signal: AbortSignal.timeout(CMA_REQUEST_TIMEOUT_MS),
        },
      );
      const text = await response.text();
      if (response.status === 404 || response.status === 204 || !text) return null;
      if (!response.ok) {
        throw new CmaApiError(response.status, `CMA work poll failed (${response.status}): ${text.slice(0, 500)}`);
      }
      const item = JSON.parse(text) as CmaWorkItem | { type?: string };
      return item && item.type === "work" ? (item as CmaWorkItem) : null;
    },
    async ackWork(environmentId, workId) {
      return (await request(
        "POST",
        `/v1/environments/${encodeURIComponent(environmentId)}/work/${encodeURIComponent(workId)}/ack`,
      )) as CmaWorkItem;
    },
    async heartbeatWork(environmentId, workId, opts) {
      const query = new URLSearchParams();
      if (opts?.desiredTtlSeconds) query.set("desired_ttl_seconds", String(opts.desiredTtlSeconds));
      if (opts?.expectedLastHeartbeat) query.set("expected_last_heartbeat", opts.expectedLastHeartbeat);
      const suffix = query.size ? `?${query}` : "";
      return (await request(
        "POST",
        `/v1/environments/${encodeURIComponent(environmentId)}/work/${encodeURIComponent(workId)}/heartbeat${suffix}`,
      )) as CmaWorkHeartbeat;
    },
    async stopWork(environmentId, workId, force = false) {
      await request(
        "POST",
        `/v1/environments/${encodeURIComponent(environmentId)}/work/${encodeURIComponent(workId)}/stop`,
        { force },
      );
    },
    async streamEvents(sessionId, opts) {
      const query = new URLSearchParams();
      for (const delta of opts.deltas ?? []) query.append("event_deltas[]", delta);
      const suffix = query.size ? `?${query}` : "";
      const response = await doFetch(`${baseUrl}/v1/sessions/${encodeURIComponent(sessionId)}/events/stream${suffix}`, {
        method: "GET",
        headers: { ...(await headers()), accept: "text/event-stream" },
        signal: opts.signal,
      });
      if (!response.ok || !response.body) {
        const text = response.body ? await response.text() : "";
        throw new CmaApiError(
          response.status,
          `CMA event stream failed (${response.status}): ${text.slice(0, 500) || response.statusText}`,
        );
      }
      return frames(response.body);
    },
  };
}
