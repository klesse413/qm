import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { createCmaClient } from "../src/harness/cma-client.ts";
import { createCmaWorker, type CmaWorkerExecution } from "../src/harness/cma-worker.ts";

type J = Record<string, unknown>;

interface FakeQueueState {
  url: string;
  queue: Array<{ id: string; sessionId: string }>;
  acked: string[];
  stopped: string[];
  heartbeats: Array<{ workId: string; expected: string | null }>;
  toolResults: Array<{ sessionId: string; event: J }>;
  listedEvents: Map<string, J[]>;
}

function startFakeQueue(
  onToolResult: (sessionId: string, event: J, state: FakeQueueState) => J[],
): Promise<{ state: FakeQueueState; close: () => Promise<void> }> {
  const state: FakeQueueState = {
    url: "",
    queue: [],
    acked: [],
    stopped: [],
    heartbeats: [],
    toolResults: [],
    listedEvents: new Map(),
  };
  const sse = new Map<string, ServerResponse>();
  let heartbeatCount = 0;
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
  const emit = (sessionId: string, frames: J[]) => {
    for (const frame of frames) {
      state.listedEvents.get(sessionId)?.push(frame);
      sse.get(sessionId)?.write(`data: ${JSON.stringify(frame)}\n\n`);
    }
  };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    const path = url.pathname;
    const work = /^\/v1\/environments\/env_w\/work\/(?:poll$|([^/]+)\/(ack|heartbeat|stop)$)/.exec(path);
    if (work) {
      if (path.endsWith("/work/poll")) {
        const item = state.queue.shift();
        if (!item) return json(res, 204, null);
        return json(res, 200, {
          type: "work",
          id: item.id,
          state: "queued",
          data: { type: "session", id: item.sessionId },
        });
      }
      const workId = work[1]!;
      if (work[2] === "ack") {
        state.acked.push(workId);
        return json(res, 200, { type: "work", id: workId, state: "starting", data: { type: "session", id: "" } });
      }
      if (work[2] === "heartbeat") {
        state.heartbeats.push({ workId, expected: url.searchParams.get("expected_last_heartbeat") });
        return json(res, 200, {
          type: "work_heartbeat",
          lease_extended: true,
          last_heartbeat: `hb_${++heartbeatCount}`,
        });
      }
      state.stopped.push(workId);
      return json(res, 200, { type: "work", id: workId, state: "stopped", data: { type: "session", id: "" } });
    }
    const session = /^\/v1\/sessions\/([^/]+)(\/.*)?$/.exec(path);
    if (!session) return json(res, 404, { error: { message: "not found" } });
    const sessionId = session[1]!;
    const rest = session[2] ?? "";
    if (req.method === "POST" && rest === "/events") {
      const events = (JSON.parse(await body(req)) as { events: J[] }).events;
      for (const event of events) state.toolResults.push({ sessionId, event });
      json(res, 200, {});
      for (const event of events) emit(sessionId, onToolResult(sessionId, event, state));
      return;
    }
    if (req.method === "GET" && rest === "/events")
      return json(res, 200, { data: state.listedEvents.get(sessionId) ?? [] });
    if (req.method === "GET" && rest === "/events/stream") {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("\n");
      sse.set(sessionId, res);
      req.on("close", () => sse.delete(sessionId));
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
            for (const res of sse.values()) res.end();
            server.close(() => done());
            server.closeAllConnections?.();
          }),
      });
    });
  });
}

function worker(
  state: FakeQueueState,
  executions: Map<string, CmaWorkerExecution>,
  deny?: (toolName: string, input: unknown) => string | null,
) {
  const client = createCmaClient({ auth: () => ({ authorization: "Bearer env-key" }), baseUrl: state.url });
  return createCmaWorker({
    client,
    environmentId: "env_w",
    workerId: "qm-test-worker",
    resolveSession: async (id) => executions.get(id) ?? null,
    ...(deny ? { denyReason: (_execution, toolName, input) => deny(toolName, input) } : {}),
    heartbeatIntervalMs: 20,
    turnTimeoutMs: 10_000,
    onError: () => {},
  });
}

test("the worker claims a work item, executes bash on the scope volume, and stops the item on end_turn", async (t) => {
  const fake = await startFakeQueue((_sessionId, event) => {
    if (event.type !== "user.tool_result") return [];
    return [
      { type: "agent.message", id: "sevt_m1", content: [{ type: "text", text: "done" }] },
      { type: "session.status_idle", id: "sevt_s1", stop_reason: { type: "end_turn" } },
    ];
  });
  t.after(fake.close);
  fake.state.queue.push({ id: "work_1", sessionId: "sesn_w1" });
  fake.state.listedEvents.set("sesn_w1", [
    { type: "agent.tool_use", id: "sevt_t1", name: "bash", input: { command: "echo hi" } },
  ]);
  const commands: string[] = [];
  const executions = new Map<string, CmaWorkerExecution>([
    [
      "sesn_w1",
      {
        scopeId: "channel:C1",
        runBash: async (command) => {
          commands.push(command);
          return { output: "hi\n[exit 0]", isError: false };
        },
      },
    ],
  ]);
  const w = worker(fake.state, executions);
  const controller = new AbortController();
  const running = w.run(controller.signal);
  const deadline = Date.now() + 5_000;
  while (!fake.state.stopped.length && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  controller.abort();
  await running;

  assert.deepEqual(fake.state.acked, ["work_1"]);
  assert.deepEqual(commands, ["echo hi"]);
  const result = fake.state.toolResults[0]!;
  assert.equal(result.sessionId, "sesn_w1");
  assert.equal(result.event.type, "user.tool_result");
  assert.equal(result.event.tool_use_id, "sevt_t1");
  assert.deepEqual(fake.state.stopped, ["work_1"]);
  assert.equal(fake.state.heartbeats[0]?.expected, "NO_HEARTBEAT");
  assert.ok(fake.state.heartbeats.length >= 1);
});

test("the worker enforces a command-policy denial instead of executing", async (t) => {
  const fake = await startFakeQueue((_sessionId, event) => {
    if (event.type !== "user.tool_result") return [];
    return [{ type: "session.status_idle", id: "sevt_s1", stop_reason: { type: "end_turn" } }];
  });
  t.after(fake.close);
  fake.state.queue.push({ id: "work_2", sessionId: "sesn_w2" });
  fake.state.listedEvents.set("sesn_w2", [
    { type: "agent.tool_use", id: "sevt_t2", name: "bash", input: { command: "rm -rf /" } },
  ]);
  const commands: string[] = [];
  const executions = new Map<string, CmaWorkerExecution>([
    [
      "sesn_w2",
      {
        scopeId: "personal:U1",
        runBash: async (command) => {
          commands.push(command);
          return { output: "", isError: false };
        },
      },
    ],
  ]);
  const w = worker(fake.state, executions, (toolName, input) =>
    toolName === "bash" && /rm -rf \//.test(String((input as { command?: string }).command))
      ? "recursive delete of /"
      : null,
  );
  const controller = new AbortController();
  const running = w.run(controller.signal);
  const deadline = Date.now() + 5_000;
  while (!fake.state.stopped.length && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  controller.abort();
  await running;

  assert.deepEqual(commands, [], "the denied command never runs");
  const result = fake.state.toolResults[0]!.event as { content: Array<{ text: string }> };
  assert.match(result.content[0]!.text, /denied by command policy.*recursive delete/);
  assert.deepEqual(fake.state.stopped, ["work_2"]);
});

test("a work item for an unmapped session is stopped without posting anything", async (t) => {
  const fake = await startFakeQueue(() => []);
  t.after(fake.close);
  fake.state.queue.push({ id: "work_3", sessionId: "sesn_unknown" });
  const w = worker(fake.state, new Map());
  const controller = new AbortController();
  const running = w.run(controller.signal);
  const deadline = Date.now() + 5_000;
  while (!fake.state.stopped.length && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  controller.abort();
  await running;

  assert.deepEqual(fake.state.stopped, ["work_3"]);
  assert.deepEqual(fake.state.toolResults, []);
});
