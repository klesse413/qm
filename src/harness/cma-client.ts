const CMA_DEFAULT_BASE_URL = "https://api.anthropic.com";
const CMA_API_VERSION = "2023-06-01";
const CMA_BETA = "managed-agents-2026-04-01";

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
  | { type: "user.interrupt" };

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

  const request = async (method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<unknown> => {
    const response = await doFetch(`${baseUrl}${path}`, {
      method,
      headers: await headers(),
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      ...(signal ? { signal } : {}),
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
