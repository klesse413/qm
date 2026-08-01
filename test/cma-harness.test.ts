import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import {
  cmaContextKey,
  cmaCustomTools,
  cmaHarnessConfigOptions,
  cmaToolContext,
  createCmaHarness,
  type CmaSessionRecord,
} from "../src/harness/cma-harness.ts";
import type { HarnessTurnInput } from "../src/harness/harness.ts";
import { NonRetryableTurnError } from "../src/core/turn-error.ts";
import { createMemoryMap } from "../src/persistence/durable-map.ts";
import type { ScopeId, Session, SessionEntry } from "../src/types.ts";
import type { Config } from "../src/config.ts";

type FakeEvent = Record<string, unknown>;

interface FakeCmaState {
  url: string;
  createBodies: Array<Record<string, unknown>>;
  createHeaders: Array<Record<string, string | string[] | undefined>>;
  eventPosts: Array<{ sessionId: string; events: FakeEvent[] }>;
  toolUpdates: Array<{ sessionId: string; tools: unknown[] }>;
  deleted: string[];
  listedEvents: FakeEvent[];
  status: string;
  streams: number;
  createStatus: number;
  failEventsPosts: number;
}

function startFakeCma(
  onEvents: (events: FakeEvent[], state: FakeCmaState) => FakeEvent[],
): Promise<{ state: FakeCmaState; close: () => Promise<void> }> {
  const state: FakeCmaState = {
    url: "",
    createBodies: [],
    createHeaders: [],
    eventPosts: [],
    toolUpdates: [],
    deleted: [],
    listedEvents: [],
    status: "idle",
    streams: 0,
    createStatus: 200,
    failEventsPosts: 0,
  };
  let sessionCount = 0;
  let sse: ServerResponse | null = null;
  const body = (req: import("node:http").IncomingMessage): Promise<string> =>
    new Promise((resolve) => {
      let data = "";
      req.on("data", (chunk: Buffer) => (data += chunk.toString()));
      req.on("end", () => resolve(data));
    });
  const json = (res: ServerResponse, status: number, value: unknown) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(value));
  };
  const emit = (frames: FakeEvent[]) => {
    for (const frame of frames) {
      if (typeof frame.type === "string" && !frame.type.startsWith("event_")) state.listedEvents.push(frame);
      sse?.write(`data: ${JSON.stringify(frame)}\n\n`);
    }
  };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    const path = url.pathname;
    if (req.method === "POST" && path === "/v1/sessions") {
      const parsed = JSON.parse(await body(req)) as Record<string, unknown>;
      state.createBodies.push(parsed);
      state.createHeaders.push({ ...req.headers });
      if (state.createStatus !== 200) return json(res, state.createStatus, { error: { message: "bad key" } });
      sessionCount++;
      return json(res, 200, { type: "session", id: `sesn_${sessionCount}`, status: "idle" });
    }
    const sessionMatch = /^\/v1\/sessions\/([^/]+)(\/.*)?$/.exec(path);
    if (!sessionMatch) return json(res, 404, { error: { message: "not found" } });
    const sessionId = sessionMatch[1]!;
    const rest = sessionMatch[2] ?? "";
    if (req.method === "GET" && rest === "")
      return json(res, 200, { type: "session", id: sessionId, status: state.status });
    if (req.method === "POST" && rest === "") {
      const parsed = JSON.parse(await body(req)) as { agent?: { tools?: unknown[] } };
      state.toolUpdates.push({ sessionId, tools: parsed.agent?.tools ?? [] });
      return json(res, 200, { type: "session", id: sessionId, status: state.status });
    }
    if (req.method === "DELETE" && rest === "") {
      state.deleted.push(sessionId);
      return json(res, 200, {});
    }
    if (req.method === "POST" && rest === "/events") {
      if (state.failEventsPosts > 0) {
        state.failEventsPosts--;
        return json(res, 500, { error: { message: "transient" } });
      }
      const parsed = JSON.parse(await body(req)) as { events: FakeEvent[] };
      state.eventPosts.push({ sessionId, events: parsed.events });
      for (const event of parsed.events)
        state.listedEvents.push({ ...event, id: `sevt_in_${state.listedEvents.length}` });
      json(res, 200, {});
      emit(onEvents(parsed.events, state));
      return;
    }
    if (req.method === "GET" && rest === "/events") return json(res, 200, { data: state.listedEvents });
    if (req.method === "GET" && rest === "/events/stream") {
      state.streams++;
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("\n");
      sse = res;
      req.on("close", () => {
        if (sse === res) sse = null;
      });
      return;
    }
    return json(res, 404, { error: { message: "not found" } });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      state.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      resolve({
        state,
        close: () =>
          new Promise((done) => {
            sse?.end();
            server.close(() => done());
            server.closeAllConnections?.();
          }),
      });
    });
  });
}

const scope = { kind: "org", id: "test" } as unknown as ScopeId;

function stubTools(executed: string[]): HarnessTurnInput["tools"] {
  return {
    execute: async (command: string) => {
      executed.push(command);
      return { stdout: `ran ${command}`, stderr: "", code: 0 };
    },
  } as unknown as HarnessTurnInput["tools"];
}

function turnInput(
  overrides: Partial<HarnessTurnInput> & { entries?: SessionEntry[]; executed?: string[] },
): HarnessTurnInput {
  const entries = overrides.entries ?? [];
  const session = (overrides.session ?? { id: "session-1" }) as Session;
  return {
    session,
    input: "hi",
    systemPrompt: "be concise",
    history: [],
    tools: stubTools(overrides.executed ?? []),
    scopeLabel: scope,
    orgScopeId: scope,
    emit: async (entry) => {
      const saved = { ...entry, sessionId: session.id, seq: entries.length + 1, createdAt: Date.now() } as SessionEntry;
      entries.push(saved);
      return saved;
    },
    recordModelCall: () => {},
    ...overrides,
  } as HarnessTurnInput;
}

test("CMA drives a full turn: custom tool round-trip, streamed deltas, durable session record", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type === "user.message") {
      return [
        { type: "agent.custom_tool_use", id: "sevt_t1", name: "execute", input: { command: "echo hi" } },
        {
          type: "session.status_idle",
          id: "sevt_s1",
          stop_reason: { type: "requires_action", event_ids: ["sevt_t1"] },
        },
      ];
    }
    if (first?.type === "user.custom_tool_result") {
      return [
        { type: "event_start", event: { type: "agent.message", id: "sevt_m1" } },
        {
          type: "event_delta",
          event_id: "sevt_m1",
          delta: { type: "content_delta", index: 0, content: { type: "text", text: "Hel" } },
        },
        {
          type: "event_delta",
          event_id: "sevt_m1",
          delta: { type: "content_delta", index: 0, content: { type: "text", text: "lo" } },
        },
        { type: "agent.message", id: "sevt_m1", content: [{ type: "text", text: "Hello" }] },
        { type: "agent.thinking", id: "sevt_th1", thinking: "pondering" },
        { type: "session.status_idle", id: "sevt_s2", stop_reason: { type: "end_turn" } },
      ];
    }
    return [];
  });
  t.after(fake.close);
  const records = createMemoryMap<CmaSessionRecord>();
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
    sessions: records,
  });
  const entries: SessionEntry[] = [];
  const executed: string[] = [];
  const deltas: string[] = [];
  const result = await harness.turns.runTurn(turnInput({ entries, executed, onDelta: (delta) => deltas.push(delta) }));

  assert.equal(result.reply, "Hello");
  assert.deepEqual(deltas, ["Hel", "lo"]);
  assert.deepEqual(executed, ["echo hi"]);
  assert.deepEqual(
    entries.map((entry) => entry.type),
    ["user", "tool_call", "tool_result", "thinking", "assistant"],
  );
  assert.equal(fake.state.createBodies.length, 1);
  const created = fake.state.createBodies[0]!;
  const agent = created.agent as {
    type: string;
    id: string;
    system: string;
    model: { id: string };
    tools: Array<{ type: string; name: string }>;
  };
  assert.equal(agent.type, "agent_with_overrides");
  assert.equal(agent.id, "agent_1");
  assert.equal(agent.system, "be concise");
  assert.equal(created.environment_id, "env_1");
  assert.ok(agent.tools.some((tool) => tool.type === "custom" && tool.name === "execute"));
  assert.equal(fake.state.createHeaders[0]!["x-api-key"], "sk-test");
  assert.equal(fake.state.createHeaders[0]!["anthropic-beta"], "managed-agents-2026-04-01");
  const toolResultPost = fake.state.eventPosts[1]!.events[0] as {
    type: string;
    custom_tool_use_id: string;
    content: Array<{ text: string }>;
  };
  assert.equal(toolResultPost.type, "user.custom_tool_result");
  assert.equal(toolResultPost.custom_tool_use_id, "sevt_t1");
  assert.match(toolResultPost.content[0]!.text, /ran echo hi/);
  const record = await records.get("session-1");
  assert.equal(record?.cmaSessionId, "sesn_1");
  assert.equal(record?.lastSeq, entries.length);
});

test("CMA resumes the mapped session across turns and rotates it when the system prompt changes", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type !== "user.message") return [];
    const turn = fake.state.eventPosts.length;
    return [
      { type: "agent.message", id: `sevt_m${turn}`, content: [{ type: "text", text: `reply ${turn}` }] },
      { type: "session.status_idle", id: `sevt_s${turn}`, stop_reason: { type: "end_turn" } },
    ];
  });
  t.after(fake.close);
  const records = createMemoryMap<CmaSessionRecord>();
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
    sessions: records,
  });
  const entries: SessionEntry[] = [];
  const first = await harness.turns.runTurn(turnInput({ entries, input: "first question" }));
  assert.equal(first.reply, "reply 1");
  assert.equal(fake.state.createBodies.length, 1);

  const second = await harness.turns.runTurn(turnInput({ entries, history: [...entries], input: "second question" }));
  assert.equal(second.reply, "reply 2");
  assert.equal(fake.state.createBodies.length, 1, "an unchanged context reuses the CMA session");
  const secondMessage = fake.state.eventPosts.at(-1)!.events[0] as { content: Array<{ type: string; text?: string }> };
  assert.doesNotMatch(secondMessage.content[0]!.text!, /BEGIN TRANSCRIPT/);
  assert.match(secondMessage.content[0]!.text!, /second question/);

  const third = await harness.turns.runTurn(
    turnInput({ entries, history: [...entries], input: "third question", systemPrompt: "be thorough" }),
  );
  assert.equal(third.reply, "reply 3");
  assert.equal(fake.state.createBodies.length, 2, "a changed system prompt rotates to a fresh CMA session");
  const thirdMessage = fake.state.eventPosts.at(-1)!.events[0] as { content: Array<{ type: string; text?: string }> };
  assert.match(thirdMessage.content[0]!.text!, /BEGIN TRANSCRIPT/);
  assert.match(thirdMessage.content[0]!.text!, /first question/);
  assert.equal((await records.get("session-1"))?.cmaSessionId, "sesn_2");
});

test("CMA strict posture holds the tool call for approval and interrupts the session", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type === "user.message") {
      return [
        { type: "agent.custom_tool_use", id: "sevt_t1", name: "execute", input: { command: "rm -rf /" } },
        {
          type: "session.status_idle",
          id: "sevt_s1",
          stop_reason: { type: "requires_action", event_ids: ["sevt_t1"] },
        },
      ];
    }
    return [];
  });
  t.after(fake.close);
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
  });
  const entries: SessionEntry[] = [];
  const executed: string[] = [];
  const result = await harness.turns.runTurn(turnInput({ entries, executed, toolApprovalGate: () => false }));

  assert.equal(result.pausedOnApproval, true);
  assert.equal(result.reply, "");
  assert.equal(result.pendingApprovals?.[0]?.command, "execute");
  assert.deepEqual(executed, [], "the gated tool never reaches the sandbox");
  const posted = fake.state.eventPosts.map((post) => post.events.map((event) => event.type as string)).flat();
  assert.deepEqual(posted, ["user.message", "user.custom_tool_result", "user.interrupt"]);
  const blocked = fake.state.eventPosts[1]!.events[0] as { content: Array<{ text: string }> };
  assert.match(blocked.content[0]!.text, /needs human approval/);
});

test("CMA classifies terminal API errors as non-retryable and clears a dead session mapping", async (t) => {
  const fake = await startFakeCma(() => [{ type: "session.status_terminated", id: "sevt_dead" }]);
  t.after(fake.close);
  const records = createMemoryMap<CmaSessionRecord>();
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
    sessions: records,
  });
  await assert.rejects(
    harness.turns.runTurn(turnInput({})),
    (error: Error) => !(error instanceof NonRetryableTurnError) && /terminated/.test(error.message),
  );
  assert.equal(await records.get("session-1"), null);

  fake.state.createStatus = 401;
  await assert.rejects(harness.turns.runTurn(turnInput({})), NonRetryableTurnError);

  const unconfigured = createCmaHarness();
  await assert.rejects(unconfigured.turns.runTurn(turnInput({})), /CMA harness is not configured/);
});

test("CMA polling delivery completes a turn without an event stream", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type !== "user.message") return [];
    return [
      { type: "agent.message", id: "sevt_m1", content: [{ type: "text", text: "polled reply" }] },
      { type: "session.status_idle", id: "sevt_s1", stop_reason: { type: "end_turn" } },
    ];
  });
  t.after(fake.close);
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
    delivery: "poll",
    pollIntervalMs: 20,
  });
  const result = await harness.turns.runTurn(turnInput({}));
  assert.equal(result.reply, "polled reply");
  assert.equal(fake.state.streams, 0);
});

test("CMA one-shots run in throwaway sessions that are deleted afterwards", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type !== "user.message") return [];
    return [
      { type: "agent.message", id: "sevt_m1", content: [{ type: "text", text: "one-shot reply" }] },
      { type: "session.status_idle", id: "sevt_s1", stop_reason: { type: "end_turn" } },
    ];
  });
  t.after(fake.close);
  const records = createMemoryMap<CmaSessionRecord>();
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
    sessions: records,
  });
  assert.equal(await harness.models.oneShot?.("system", "question"), "one-shot reply");
  assert.deepEqual(fake.state.deleted, ["sesn_1"]);
  assert.deepEqual(await records.entries(), []);
  const created = fake.state.createBodies[0]!.agent as { tools: unknown[] };
  assert.deepEqual(created.tools, []);
});

test("CMA retries a transiently failing initial send instead of hanging", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type !== "user.message") return [];
    return [
      { type: "agent.message", id: "sevt_m1", content: [{ type: "text", text: "made it" }] },
      { type: "session.status_idle", id: "sevt_s1", stop_reason: { type: "end_turn" } },
    ];
  });
  t.after(fake.close);
  fake.state.failEventsPosts = 1;
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
  });
  const result = await harness.turns.runTurn(turnInput({}));
  assert.equal(result.reply, "made it");
  assert.equal(fake.state.eventPosts.length, 1);
});

test("CMA abort mid-generation keeps the text already streamed", async (t) => {
  const fake = await startFakeCma((events) => {
    const first = events[0] as { type?: string } | undefined;
    if (first?.type !== "user.message") return [];
    return [
      { type: "event_start", event: { type: "agent.message", id: "sevt_m1" } },
      {
        type: "event_delta",
        event_id: "sevt_m1",
        delta: { type: "content_delta", index: 0, content: { type: "text", text: "partial answer" } },
      },
    ];
  });
  t.after(fake.close);
  const harness = createCmaHarness({
    environmentId: "env_1",
    agentId: "agent_1",
    apiKey: "sk-test",
    baseUrl: fake.state.url,
    turnWallClockMs: 15_000,
  });
  const cancel = new AbortController();
  const deltas: string[] = [];
  const entries: SessionEntry[] = [];
  const turn = harness.turns.runTurn(
    turnInput({ entries, cancel: cancel.signal, onDelta: (delta) => deltas.push(delta) }),
  );
  const deadline = Date.now() + 5_000;
  while (!deltas.length && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  cancel.abort();
  const result = await turn;
  assert.equal(result.stopped, true);
  assert.equal(result.reply, "partial answer");
  assert.equal(entries.at(-1)?.type, "assistant");
  const posted = fake.state.eventPosts.flatMap((post) => post.events.map((event) => event.type as string));
  assert.deepEqual(posted, ["user.message", "user.interrupt"]);
});

test("CMA cancellation before any network call returns a quiet stop", async () => {
  const harness = createCmaHarness({ environmentId: "env_1", agentId: "agent_1", apiKey: "sk-test" });
  const cancel = new AbortController();
  cancel.abort();
  assert.deepEqual(await harness.turns.runTurn(turnInput({ cancel: cancel.signal })), { reply: "", stopped: true });
});

test("CMA forwards external-content screening into its tool bridge", () => {
  const screenExternalContent: NonNullable<HarnessTurnInput["screenExternalContent"]> = async () => ({
    decision: "auto",
  });
  const ref = cmaToolContext({ screenExternalContent } as HarnessTurnInput);
  assert.equal(ref.screenExternalContent, screenExternalContent);
});

test("CMA custom tool declarations carry each bridged tool's schema", () => {
  const tools = cmaCustomTools([
    { name: "execute", description: "run a command", parameters: { type: "object" }, execute: async () => ({}) },
  ]);
  assert.deepEqual(tools, [
    { type: "custom", name: "execute", description: "run a command", input_schema: { type: "object" } },
  ]);
  assert.equal(cmaContextKey("system", "claude-opus-5"), cmaContextKey("system", "claude-opus-5"));
  assert.notEqual(cmaContextKey("system", "claude-opus-5"), cmaContextKey("other", "claude-opus-5"));
});

test("CMA config options map every knob the adapter consumes", () => {
  const config = {
    cmaModel: "claude-sonnet-5",
    judgeModelId: "claude-haiku-4-5",
    cmaEnvironmentId: "env_9",
    cmaAgentId: "agent_9",
    cmaApiKey: "sk-cma",
    anthropicApiKey: "sk-ant",
    cmaBaseUrl: "https://cma.example",
    cmaDelivery: "poll",
    cmaVaultIds: ["vlt_1"],
    turnWallClockMs: 120_000,
    execTimeoutDefaultMs: 60_000,
    execTimeoutMaxMs: 600_000,
    backgroundJobTtlMs: 60_000,
    backgroundJobTtlMaxMs: 600_000,
    scratchExecEnabled: true,
    sharedOwnerAuthIsolation: false,
    reachExecEnabled: false,
    signingSecret: "secret",
    apiBaseUrl: "https://core.example",
  } as unknown as Config;
  const options = cmaHarnessConfigOptions(config);
  assert.equal(options.defaultModelId, "claude-sonnet-5");
  assert.equal(options.judgeModelId, "claude-haiku-4-5");
  assert.equal(options.environmentId, "env_9");
  assert.equal(options.agentId, "agent_9");
  assert.equal(options.apiKey, "sk-cma");
  assert.equal(options.baseUrl, "https://cma.example");
  assert.equal(options.delivery, "poll");
  assert.deepEqual(options.vaultIds, ["vlt_1"]);
  assert.equal(options.turnWallClockMs, 120_000);
  assert.equal(options.scratchExec, true);
  assert.equal(options.controlTools, true);
  const fallback = cmaHarnessConfigOptions({ ...(config as object), cmaApiKey: undefined } as unknown as Config);
  assert.equal(fallback.apiKey, "sk-ant");
});
