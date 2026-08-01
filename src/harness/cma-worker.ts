import { sleep } from "../util/async.ts";
import { swallow, swallowAs } from "../util/errors.ts";
import type { CmaClient, CmaEvent, CmaStreamFrame } from "./cma-client.ts";

const WORKER_POLL_BLOCK_MS = 900;
const WORKER_IDLE_SLEEP_MS = 1_000;
const WORKER_HEARTBEAT_MS = 30_000;
const WORKER_TURN_TIMEOUT_MS = 30 * 60_000;

export interface CmaWorkerExecution {
  scopeId: string;
  runBash(command: string, timeoutSeconds?: number): Promise<{ output: string; isError: boolean }>;
}

export interface CmaWorkerOptions {
  client: CmaClient;
  environmentId: string;
  workerId?: string;
  resolveSession(cmaSessionId: string): Promise<CmaWorkerExecution | null>;
  denyReason(execution: CmaWorkerExecution, toolName: string, input: unknown): string | null;
  turnTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  onError?(context: string, error: unknown): void;
}

export interface CmaWorker {
  run(signal: AbortSignal): Promise<void>;
  handleWorkItem(workId: string, cmaSessionId: string, signal: AbortSignal): Promise<void>;
}

function toolInputCommand(input: unknown): string | null {
  const command = (input as { command?: unknown } | null)?.command;
  return typeof command === "string" ? command : null;
}

function toolInputTimeout(input: unknown): number | undefined {
  const timeout = (input as { timeout?: unknown } | null)?.timeout;
  return typeof timeout === "number" && timeout > 0 ? timeout : undefined;
}

export function createCmaWorker(opts: CmaWorkerOptions): CmaWorker {
  const heartbeatIntervalMs = opts.heartbeatIntervalMs ?? WORKER_HEARTBEAT_MS;
  const turnTimeoutMs = opts.turnTimeoutMs ?? WORKER_TURN_TIMEOUT_MS;
  const onError = opts.onError ?? ((context: string, error: unknown) => swallow(context, error));

  const startHeartbeats = (workId: string, signal: AbortSignal): (() => void) => {
    let expected = "NO_HEARTBEAT";
    let timer: NodeJS.Timeout | null = null;
    const beat = async () => {
      try {
        const result = await opts.client.heartbeatWork(opts.environmentId, workId, {
          expectedLastHeartbeat: expected,
        });
        if (result.last_heartbeat) expected = result.last_heartbeat;
      } catch (error) {
        onError("cma-worker: heartbeat", error);
      }
      if (!signal.aborted) timer = setTimeout(() => void beat(), heartbeatIntervalMs);
    };
    void beat();
    return () => {
      if (timer) clearTimeout(timer);
    };
  };

  const respond = async (cmaSessionId: string, toolUseId: string, text: string): Promise<void> => {
    await opts.client.sendEvents(cmaSessionId, [
      { type: "user.tool_result", tool_use_id: toolUseId, content: [{ type: "text", text }] },
    ]);
  };

  const executeToolUse = async (
    cmaSessionId: string,
    execution: CmaWorkerExecution,
    event: CmaEvent,
  ): Promise<void> => {
    if (!event.id || typeof event.name !== "string") return;
    const denial = opts.denyReason(execution, event.name, event.input);
    if (denial) {
      await respond(cmaSessionId, event.id, `[denied by command policy] ${denial}`);
      return;
    }
    if (event.name === "bash") {
      const command = toolInputCommand(event.input);
      if (!command) {
        await respond(cmaSessionId, event.id, "[error] bash tool call carried no command");
        return;
      }
      try {
        const result = await execution.runBash(command, toolInputTimeout(event.input));
        await respond(cmaSessionId, event.id, result.output);
      } catch (error) {
        await respond(cmaSessionId, event.id, `[error] ${error instanceof Error ? error.message : String(error)}`);
      }
      return;
    }
    await respond(cmaSessionId, event.id, `[tool not supported by this worker: ${event.name}]`);
  };

  const handleWorkItem = async (workId: string, cmaSessionId: string, signal: AbortSignal): Promise<void> => {
    await opts.client.ackWork(opts.environmentId, workId);
    const abort = new AbortController();
    const onAbort = () => abort.abort();
    signal.addEventListener("abort", onAbort, { once: true });
    const stopHeartbeats = startHeartbeats(workId, abort.signal);
    const timeout = setTimeout(() => abort.abort(), turnTimeoutMs);
    const seen = new Set<string>();
    const execution = await opts.resolveSession(cmaSessionId);
    try {
      if (!execution) {
        onError("cma-worker: unmapped session", new Error(`no scope mapping for ${cmaSessionId}`));
        return;
      }
      let done = false;
      const handleEvent = async (event: CmaEvent): Promise<void> => {
        if (event.id) {
          if (seen.has(event.id)) return;
          seen.add(event.id);
        }
        if (event.type === "agent.tool_use") {
          await executeToolUse(cmaSessionId, execution, event);
          return;
        }
        if (event.type === "session.status_idle") {
          const reason = event.stop_reason?.type;
          if (reason === "end_turn" || reason === "interrupted") done = true;
          return;
        }
        if (event.type === "session.status_terminated") done = true;
      };
      while (!done && !abort.signal.aborted) {
        try {
          const stream = await opts.client.streamEvents(cmaSessionId, { signal: abort.signal });
          const backfill = await opts.client.listEvents(cmaSessionId, { limit: 100 });
          for (const event of backfill.data) {
            await handleEvent(event);
            if (done) break;
          }
          if (done) break;
          for await (const frame of stream as AsyncIterable<CmaStreamFrame>) {
            if (frame.kind !== "event") continue;
            await handleEvent(frame.event);
            if (done) break;
          }
        } catch (error) {
          if (done || abort.signal.aborted) break;
          onError("cma-worker: event stream", error);
          await sleep(500);
        }
      }
    } finally {
      clearTimeout(timeout);
      stopHeartbeats();
      signal.removeEventListener("abort", onAbort);
      abort.abort();
      await opts.client.stopWork(opts.environmentId, workId).catch(swallowAs("cma-worker: stop", undefined));
    }
  };

  const run = async (signal: AbortSignal): Promise<void> => {
    while (!signal.aborted) {
      let item = null;
      try {
        item = await opts.client.pollWork(opts.environmentId, {
          blockMs: WORKER_POLL_BLOCK_MS,
          ...(opts.workerId ? { workerId: opts.workerId } : {}),
        });
      } catch (error) {
        onError("cma-worker: poll", error);
      }
      if (signal.aborted) return;
      if (!item || item.data.type !== "session") {
        await sleep(WORKER_IDLE_SLEEP_MS);
        continue;
      }
      try {
        await handleWorkItem(item.id, item.data.id, signal);
      } catch (error) {
        onError("cma-worker: work item", error);
      }
    }
  };

  return { run, handleWorkItem };
}
