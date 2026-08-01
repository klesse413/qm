import { createHash, randomBytes } from "node:crypto";
import { CONFIG_DEFAULTS, type Config } from "../config.ts";
import { NonRetryableTurnError } from "../core/turn-error.ts";
import { contextTokenBudgetForModel, DEFAULT_AGENT_MODEL_ID, modelSupportedByHarness } from "../model/pi-models.ts";
import { createMemoryMap, type DurableMap } from "../persistence/durable-map.ts";
import { startSignalPoll, type RunSignalStore } from "../runs/run-signal-store.ts";
import { parseSecurityScreenVerdict, SECURITY_SCREEN_SYSTEM_PROMPT } from "../security/security-posture.ts";
import type { ScopeId, SessionEntry } from "../types.ts";
import { sleep } from "../util/async.ts";
import { errMessage, swallow, swallowAs } from "../util/errors.ts";
import { countTokens } from "../util/tokens.ts";
import {
  CmaApiError,
  createCmaClient,
  isTerminalCmaStatus,
  type CmaAuthHeaders,
  type CmaClient,
  type CmaCustomTool,
  type CmaEvent,
  type CmaOutboundEvent,
  type CmaStreamFrame,
  type CmaUserContent,
} from "./cma-client.ts";
import { compactTranscript, deterministicCompactSummary } from "./context-compaction.ts";
import { defineHarness, type Harness, type HarnessTurnInput, type HarnessTurnResult } from "./harness.ts";
import {
  buildDetectionPrompt,
  CONTEXT_COMPACTION_PROMPT,
  parseDetectVerdict,
  renderDetectPrompt,
  sanitizeTitle,
  TITLE_GENERATION_PROMPT,
} from "./pi-harness.ts";
import { coreToolOptions, createPiTools, type PiToolsOptions, type ToolContextRef } from "./pi-tools.ts";
import { reconstructMessagesFromHistory, replayTranscript, seedPriorTurns } from "./replay.ts";

const CMA_POLL_INTERVAL_MS = 1_500;
const CMA_LIST_PAGE_LIMIT = 100;
const CMA_LIST_PAGE_CAP = 50;
const CMA_INTERRUPT_SETTLE_MS = 15_000;
const CMA_STREAM_RETRIES = 5;

export interface CmaSessionRecord {
  cmaSessionId: string;
  contextKey: string;
  toolsKey: string;
  lastSeq: number;
  updatedAt: number;
}

export interface CmaHarnessOptions {
  modelId?: string | ((scope?: ScopeId) => string | undefined);
  defaultModelId?: string;
  judgeModelId?: string;
  environmentId?: string;
  agentId?: string;
  apiKey?: string;
  authHeaders?: CmaAuthHeaders;
  baseUrl?: string;
  fetch?: typeof fetch;
  delivery?: "stream" | "poll";
  pollIntervalMs?: number;
  vaultIds?: string[];
  sessions?: DurableMap<CmaSessionRecord>;
  scratchExec?: boolean;
  ownerAuthExec?: boolean;
  reachExec?: boolean;
  controlTools?: boolean;
  turnWallClockMs?: number;
  execTimeoutMs?: number;
  execTimeoutCeilingMs?: number;
  backgroundJobTtlMs?: number;
  backgroundJobTtlMaxMs?: number;
  signals?: RunSignalStore;
}

export function cmaHarnessConfigOptions(config: Config): CmaHarnessOptions {
  const apiKey = config.cmaApiKey ?? config.anthropicApiKey;
  return {
    ...(config.cmaModel ? { defaultModelId: config.cmaModel } : {}),
    ...(config.judgeModelId && modelSupportedByHarness(config.judgeModelId, "cma")
      ? { judgeModelId: config.judgeModelId }
      : {}),
    ...(config.cmaEnvironmentId ? { environmentId: config.cmaEnvironmentId } : {}),
    ...(config.cmaAgentId ? { agentId: config.cmaAgentId } : {}),
    ...(apiKey ? { apiKey } : {}),
    ...(config.cmaBaseUrl ? { baseUrl: config.cmaBaseUrl } : {}),
    delivery: config.cmaDelivery,
    ...(config.cmaVaultIds?.length ? { vaultIds: config.cmaVaultIds } : {}),
    ...coreToolOptions(config),
    turnWallClockMs: config.turnWallClockMs,
  };
}

export function cmaToolContext(turn: HarnessTurnInput): ToolContextRef {
  return {
    current: turn.tools,
    pendingApprovals: [],
    pausedOnApproval: false,
    silentRequested: false,
    pollFire: Boolean(turn.pollFire),
    emit: turn.emit,
    scopeLabel: turn.scopeLabel,
    orgScopeId: turn.orgScopeId,
    screenExternalContent: turn.screenExternalContent,
    toolApprovalGate: turn.toolApprovalGate,
  };
}

type BridgedTool = {
  name: string;
  description: string;
  parameters: unknown;
  execute(
    callId: string,
    args: unknown,
  ): Promise<{ content?: Array<{ type?: string; text?: string }>; terminate?: boolean }>;
};

function toolOptions(opts: CmaHarnessOptions, turn?: HarnessTurnInput): PiToolsOptions {
  return {
    scratchExec: opts.scratchExec,
    ownerAuthExec: opts.ownerAuthExec,
    reachExec: opts.reachExec,
    controlTools: opts.controlTools,
    execTimeoutMs: opts.execTimeoutMs,
    execTimeoutCeilingMs: opts.execTimeoutCeilingMs,
    backgroundJobTtlMs: opts.backgroundJobTtlMs,
    backgroundJobTtlMaxMs: opts.backgroundJobTtlMaxMs,
    ...(turn
      ? { readOnly: turn.readOnly, surfaceTools: turn.surfaceTools, surfaceName: turn.surfaceName }
      : { surfaceTools: true, surfaceName: "slack" }),
  };
}

function asTools(ref: ToolContextRef, options: PiToolsOptions): BridgedTool[] {
  return createPiTools(ref, options) as unknown as BridgedTool[];
}

export function cmaCustomTools(bridged: readonly BridgedTool[]): CmaCustomTool[] {
  return bridged.map((tool) => ({
    type: "custom",
    name: tool.name,
    description: tool.description,
    input_schema: tool.parameters,
  }));
}

function toolText(result: Awaited<ReturnType<BridgedTool["execute"]>>): string {
  return (result.content ?? [])
    .filter((item): item is { type?: string; text: string } => typeof item.text === "string")
    .map((item) => item.text)
    .join("\n");
}

function sha(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function cmaContextKey(systemPrompt: string, model: string): string {
  return `${model}\n${sha(systemPrompt)}`;
}

function cmaToolsKey(tools: readonly CmaCustomTool[]): string {
  return sha(JSON.stringify(tools.map((tool) => [tool.name, tool.description, tool.input_schema])));
}

function turnPrompt(turn: HarnessTurnInput, replaySource: readonly SessionEntry[], seedTurns: boolean): string {
  const replay = replayTranscript(reconstructMessagesFromHistory(replaySource));
  const prior = seedTurns
    ? seedPriorTurns(turn.priorTurns ?? [])
        .map((message) => message.text)
        .join("\n")
    : "";
  return [replay, prior, turn.input, turn.environment].filter((value) => value?.trim()).join("\n\n");
}

function userMessage(text: string, images: HarnessTurnInput["images"] = []): CmaOutboundEvent {
  const content: CmaUserContent[] = [
    { type: "text", text },
    ...images.map((image) => ({
      type: "image" as const,
      source: { type: "base64" as const, media_type: image.mimeType, data: image.dataBase64 },
    })),
  ];
  return { type: "user.message", content };
}

function eventText(event: CmaEvent): string {
  return (event.content ?? [])
    .filter((block) => block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("");
}

function classifyTurnError(error: unknown): Error {
  if (error instanceof CmaApiError && isTerminalCmaStatus(error.status)) {
    return new NonRetryableTurnError(error.message);
  }
  return error instanceof Error ? error : new Error(String(error));
}

class CmaEventWindowExceeded extends Error {}

async function listAllEvents(client: CmaClient, sessionId: string): Promise<CmaEvent[]> {
  const events: CmaEvent[] = [];
  let page: string | undefined;
  for (let i = 0; i < CMA_LIST_PAGE_CAP; i++) {
    const listed = await client.listEvents(sessionId, {
      limit: CMA_LIST_PAGE_LIMIT,
      ...(page ? { page } : {}),
    });
    events.push(...listed.data);
    if (!listed.nextPage) return events.sort((a, b) => (a.processed_at ?? "").localeCompare(b.processed_at ?? ""));
    page = listed.nextPage;
  }
  throw new CmaEventWindowExceeded(
    `CMA session ${sessionId} has more than ${CMA_LIST_PAGE_CAP * CMA_LIST_PAGE_LIMIT} listable events; rotating to a fresh session`,
  );
}

export function createCmaHarness(opts: CmaHarnessOptions = {}): Harness {
  const configuredModel = opts.modelId;
  const judgeModelId = opts.judgeModelId ?? "claude-haiku-4-5";
  const resolveModelId = (scope?: ScopeId) =>
    [
      typeof configuredModel === "function" ? configuredModel(scope) : configuredModel,
      opts.defaultModelId,
      DEFAULT_AGENT_MODEL_ID,
    ].find((id): id is string => modelSupportedByHarness(id, "cma"))!;
  const defaultTurnWallClockMs = opts.turnWallClockMs ?? CONFIG_DEFAULTS.turnWallClockSec * 1000;
  const sessionRecords = opts.sessions ?? createMemoryMap<CmaSessionRecord>();
  const delivery = opts.delivery ?? "stream";
  const pollIntervalMs = opts.pollIntervalMs ?? CMA_POLL_INTERVAL_MS;
  const active = new Set<AbortController>();
  let client: CmaClient | null = null;

  const ensureClient = (): CmaClient => {
    if (client) return client;
    const auth = opts.authHeaders ?? (opts.apiKey ? () => ({ "x-api-key": opts.apiKey! }) : null);
    if (!auth || !opts.environmentId || !opts.agentId) {
      throw new NonRetryableTurnError(
        "The CMA harness is not configured — set CMA_ENVIRONMENT_ID, CMA_AGENT_ID, and ANTHROPIC_API_KEY (or CMA_API_KEY). See docs/harness-cma.md.",
      );
    }
    client = createCmaClient({
      auth,
      ...(opts.baseUrl ? { baseUrl: opts.baseUrl } : {}),
      ...(opts.fetch ? { fetch: opts.fetch } : {}),
    });
    return client;
  };

  const waitForIdle = async (api: CmaClient, sessionId: string, timeoutMs: number): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const session = await api.getSession(sessionId);
      if (session.status !== "running") return;
      await sleep(250);
    }
    throw new Error("CMA session did not settle after an interrupt");
  };

  const ensureSession = async (
    api: CmaClient,
    turn: HarnessTurnInput,
    model: string,
    tools: CmaCustomTool[],
    ephemeral: boolean,
  ): Promise<{ cmaSessionId: string; replaySource: readonly SessionEntry[]; fresh: boolean; priorLastSeq: number }> => {
    const contextKey = cmaContextKey(turn.systemPrompt, model);
    const toolsKey = cmaToolsKey(tools);
    const record = ephemeral ? null : await sessionRecords.get(turn.session.id);
    if (record && record.contextKey === contextKey) {
      const live = await api.getSession(record.cmaSessionId).catch((error: unknown) => {
        if (error instanceof CmaApiError && isTerminalCmaStatus(error.status)) return null;
        throw error;
      });
      if (live && live.status !== "terminated") {
        if (live.status === "running") {
          await api
            .sendEvents(record.cmaSessionId, [{ type: "user.interrupt" }])
            .catch(swallowAs("cma: stale interrupt", undefined));
          await waitForIdle(api, record.cmaSessionId, CMA_INTERRUPT_SETTLE_MS);
        }
        if (record.toolsKey !== toolsKey) {
          await api.updateSessionTools(record.cmaSessionId, tools);
          await sessionRecords.merge(turn.session.id, { toolsKey, updatedAt: Date.now() });
        }
        return {
          cmaSessionId: record.cmaSessionId,
          replaySource: turn.history.filter((entry) => entry.seq > record.lastSeq),
          fresh: false,
          priorLastSeq: record.lastSeq,
        };
      }
    }
    if (record)
      await api.deleteSession(record.cmaSessionId).catch(swallowAs("cma: rotated session cleanup", undefined));
    const created = await api.createSession({
      agent: {
        type: "agent_with_overrides",
        id: opts.agentId!,
        system: turn.systemPrompt,
        model: { id: model },
        tools,
      },
      environment_id: opts.environmentId!,
      ...(opts.vaultIds?.length ? { vault_ids: opts.vaultIds } : {}),
      metadata: { qm_session: turn.session.id, qm_scope: String(turn.scopeLabel) },
    });
    if (!ephemeral) {
      await sessionRecords.put(turn.session.id, {
        cmaSessionId: created.id,
        contextKey,
        toolsKey,
        lastSeq: 0,
        updatedAt: Date.now(),
      });
    }
    return { cmaSessionId: created.id, replaySource: turn.history, fresh: true, priorLastSeq: 0 };
  };

  const runPrompt = async (
    turn: HarnessTurnInput,
    mode: { toolsEnabled: boolean; ephemeral: boolean } = { toolsEnabled: true, ephemeral: false },
  ): Promise<HarnessTurnResult> => {
    if (turn.cancel?.aborted) return { reply: "", stopped: true };
    const api = ensureClient();
    const model = modelSupportedByHarness(turn.model, "cma") ? turn.model! : resolveModelId(turn.scopeLabel);
    let maxSeq = turn.history.at(-1)?.seq ?? 0;
    const emit: HarnessTurnInput["emit"] = async (entry) => {
      const saved = await turn.emit(entry);
      maxSeq = Math.max(maxSeq, saved.seq);
      return saved;
    };
    const ref = cmaToolContext(turn);
    ref.emit = emit;
    const controller = new AbortController();
    ref.abortSignal = controller.signal;
    active.add(controller);
    const bridged = mode.toolsEnabled ? asTools(ref, toolOptions(opts, turn)) : [];
    const toolsByName = new Map(bridged.map((tool) => [tool.name, tool]));
    const customTools = cmaCustomTools(bridged);
    const ensured = await ensureSession(api, turn, model, customTools, mode.ephemeral).catch((error: unknown) => {
      active.delete(controller);
      throw classifyTurnError(error);
    });
    const cmaSessionId = ensured.cmaSessionId;
    const userEntry = await emit({
      type: "user",
      payload: {
        text: turn.input,
        ...((turn.triggerTs ?? turn.entryTs) ? { ts: turn.triggerTs ?? turn.entryTs } : {}),
        ...(turn.attachments?.length ? { attachments: turn.attachments } : {}),
      },
      scopeLabel: turn.scopeLabel,
    });
    const promptText = turnPrompt(turn, ensured.replaySource, ensured.fresh && !turn.history.length);
    const initial = userMessage(promptText, turn.images);

    let stopped = false;
    let done = false;
    let messageSent = false;
    let tapeWriteFailed = false;
    const seenEventIds = new Set<string>();
    const pendingTools = new Map<string, { name: string; input: unknown }>();
    const resulted = new Set<string>();
    const texts = new Map<string, string>();
    const deltaTexts = new Map<string, string>();
    const steerQueue: string[] = [];
    const recordedPrompts: string[] = [promptText];
    let inputTokenEstimate = countTokens(turn.systemPrompt) + countTokens(promptText);
    let recordedSteps = 0;
    let modelCalls = 0;
    let sawAgentEvent = false;
    let sentSinceLastAgentEvent = true;

    const appendTape = async (payload: unknown, trigger = false) => {
      if (!turn.tape) return;
      try {
        await turn.tape({
          kind: "message",
          harness: "cma",
          payload,
          scopeLabel: turn.scopeLabel,
          ...(trigger
            ? {
                entrySeq: userEntry.seq,
                meta: {
                  bareText: turn.input,
                  ...((turn.triggerTs ?? turn.entryTs) ? { ts: (turn.triggerTs ?? turn.entryTs)! } : {}),
                },
              }
            : {}),
        });
      } catch (error) {
        tapeWriteFailed = true;
        swallow("cma: tape append", error);
      }
    };

    let interruptSend: Promise<void> | null = null;
    const interrupt = async (fromUser: boolean) => {
      stopped ||= fromUser;
      const wasAborted = controller.signal.aborted;
      controller.abort();
      if (!wasAborted) {
        interruptSend = api
          .sendEvents(cmaSessionId, [{ type: "user.interrupt" }])
          .catch(swallowAs("cma: interrupt", undefined));
      }
      await interruptSend;
    };
    const onCancel = () => {
      void interrupt(false);
    };
    if (turn.cancel) {
      if (turn.cancel.aborted) onCancel();
      else turn.cancel.addEventListener("abort", onCancel, { once: true });
    }
    const stopSignals =
      opts.signals && turn.runId
        ? startSignalPoll(
            opts.signals,
            turn.runId,
            {
              onAbort: async () => interrupt(true),
              onSteer: async (steer, ts) => {
                await emit({
                  type: "user",
                  payload: { text: steer, ...(ts ? { ts } : {}), steered: true },
                  scopeLabel: turn.scopeLabel,
                });
                steerQueue.push(steer);
              },
            },
            { onError: (error) => swallow("cma signal poll", error), drainOnStop: true },
          )
        : null;

    const recordStep = async () => {
      const step = recordedSteps++;
      try {
        await turn.recordLlmRequest?.({
          turnSeq: userEntry.seq,
          step,
          model,
          request:
            step === 0
              ? { system: turn.systemPrompt, prompt: promptText, tools: customTools.map((tool) => tool.name) }
              : { prompt: recordedPrompts[step] ?? "[steer]" },
          truncated: false,
          transport: { modelId: model },
        });
      } catch (error) {
        swallow("cma: llm request record", error);
      }
    };

    const runTool = async (toolUseId: string, call: { name: string; input: unknown }): Promise<CmaOutboundEvent> => {
      const tool = toolsByName.get(call.name);
      let text: string;
      let terminate = false;
      if (!tool) {
        text = `[tool unavailable: ${call.name}]`;
      } else {
        try {
          const result = await tool.execute(toolUseId, call.input ?? {});
          text = toolText(result);
          terminate = Boolean(result.terminate);
        } catch (error) {
          text = errMessage(error);
        }
      }
      resulted.add(toolUseId);
      inputTokenEstimate += countTokens(text);
      if (terminate || ref.pausedOnApproval || ref.silentRequested) done = true;
      return { type: "user.custom_tool_result", custom_tool_use_id: toolUseId, content: [{ type: "text", text }] };
    };

    const handleIdle = async (stopReason: CmaEvent["stop_reason"]): Promise<void> => {
      if (stopReason?.type === "requires_action") {
        const referenced = stopReason.event_ids ?? [...pendingTools.keys()];
        const ids = referenced.filter((id) => pendingTools.has(id) && !resulted.has(id));
        if (!ids.length) {
          if (referenced.every((id) => resulted.has(id))) return;
          throw new Error("CMA session requires an action this adapter cannot provide");
        }
        const results: CmaOutboundEvent[] = [];
        for (const id of ids) results.push(await runTool(id, pendingTools.get(id)!));
        for (const result of results) await appendTape(result);
        await api.sendEvents(cmaSessionId, results);
        sentSinceLastAgentEvent = true;
        if (done) await interrupt(false);
        return;
      }
      if (stopReason?.type === "end_turn" || stopReason === undefined) {
        await recordStep();
        if (steerQueue.length) {
          const steers = steerQueue.splice(0);
          for (const steer of steers) {
            recordedPrompts.push(steer);
            inputTokenEstimate += countTokens(steer);
            await appendTape(userMessage(steer));
          }
          await api.sendEvents(
            cmaSessionId,
            steers.map((steer) => userMessage(steer)),
          );
          sentSinceLastAgentEvent = true;
          return;
        }
        done = true;
        return;
      }
      done = true;
    };

    const handleEvent = async (event: CmaEvent): Promise<void> => {
      if (event.id) {
        if (seenEventIds.has(event.id)) return;
        seenEventIds.add(event.id);
      }
      if (event.type.startsWith("agent.")) sawAgentEvent = true;
      if (event.type.startsWith("agent.") || event.type.startsWith("session.")) sentSinceLastAgentEvent = false;
      if (event.type === "agent.message") {
        const text = eventText(event);
        const eventId = event.id ?? randomBytes(8).toString("hex");
        const sawStart = deltaTexts.has(eventId);
        const streamedSoFar = deltaTexts.get(eventId) ?? "";
        if (text.length > streamedSoFar.length && text.startsWith(streamedSoFar)) {
          if (!streamedSoFar && !sawStart) turn.onTextBlockStart?.();
          turn.onDelta?.(text.slice(streamedSoFar.length));
        }
        texts.set(eventId, text);
        modelCalls++;
        turn.recordModelCall({
          model,
          inputTokens: inputTokenEstimate,
          entryCount: turn.history.length,
        });
        await appendTape(event);
        return;
      }
      if (event.type === "agent.thinking") {
        const thinking = typeof event.thinking === "string" ? event.thinking.trim() : "";
        if (thinking) await emit({ type: "thinking", payload: { thinking }, scopeLabel: turn.scopeLabel });
        return;
      }
      if (event.type === "agent.custom_tool_use") {
        if (typeof event.name === "string" && event.id) {
          pendingTools.set(event.id, { name: event.name, input: event.input });
          await appendTape(event);
        }
        return;
      }
      if (event.type === "session.status_idle") {
        await handleIdle(event.stop_reason);
        return;
      }
      if (event.type === "session.status_terminated") {
        if (!mode.ephemeral) await sessionRecords.delete(turn.session.id);
        throw new Error("CMA session terminated mid-turn");
      }
      if (event.type === "session.error") {
        if (!mode.ephemeral) await sessionRecords.delete(turn.session.id);
        throw new Error(`CMA session error: ${event.error?.message ?? event.error?.type ?? "unknown"}`);
      }
    };

    const handleFrame = async (frame: CmaStreamFrame): Promise<void> => {
      if (frame.kind === "start") {
        if (frame.eventType === "agent.message") {
          deltaTexts.set(frame.eventId, "");
          turn.onTextBlockStart?.();
        }
        return;
      }
      if (frame.kind === "delta") {
        if (!deltaTexts.has(frame.eventId)) return;
        deltaTexts.set(frame.eventId, (deltaTexts.get(frame.eventId) ?? "") + frame.text);
        turn.onDelta?.(frame.text);
        return;
      }
      await handleEvent(frame.event);
    };

    const markExistingEventsSeen = async () => {
      if (ensured.fresh) return;
      for (const event of await listAllEvents(api, cmaSessionId)) {
        if (event.id) seenEventIds.add(event.id);
      }
    };

    let fatal: unknown = null;
    const handled = async (frameOrEvent: CmaStreamFrame | CmaEvent, isFrame: boolean): Promise<void> => {
      try {
        if (isFrame) await handleFrame(frameOrEvent as CmaStreamFrame);
        else await handleEvent(frameOrEvent as CmaEvent);
      } catch (error) {
        fatal = error;
        throw error;
      }
    };
    const retryOrThrow = async (error: unknown, attempts: number): Promise<void> => {
      if (fatal) throw fatal instanceof Error ? fatal : new Error(String(fatal));
      if (error instanceof CmaEventWindowExceeded) throw error;
      if (error instanceof CmaApiError && isTerminalCmaStatus(error.status)) throw error;
      if (attempts > CMA_STREAM_RETRIES) throw error;
      await sleep(Math.min(5_000, 250 * 2 ** attempts));
    };
    const sendInitial = async (): Promise<void> => {
      await api.sendEvents(cmaSessionId, [initial]);
      messageSent = true;
      await appendTape(initial, true);
    };

    const consumeStream = async (): Promise<void> => {
      let attempts = 0;
      await markExistingEventsSeen();
      while (!done && !controller.signal.aborted) {
        try {
          const stream = await api.streamEvents(cmaSessionId, {
            signal: controller.signal,
            deltas: ["agent.message"],
          });
          if (!messageSent) {
            await sendInitial();
          } else {
            for (const event of await listAllEvents(api, cmaSessionId)) {
              await handled(event, false);
              if (done) return;
            }
          }
          for await (const frame of stream) {
            attempts = 0;
            await handled(frame, true);
            if (done) return;
          }
          if (!done && !controller.signal.aborted) throw new Error("CMA event stream ended before the turn settled");
        } catch (error) {
          if (done || controller.signal.aborted) return;
          await retryOrThrow(error, ++attempts);
        }
      }
    };

    const consumePoll = async (): Promise<void> => {
      let attempts = 0;
      let quietPolls = 0;
      while (!done && !controller.signal.aborted) {
        try {
          if (!messageSent) {
            await markExistingEventsSeen();
            await sendInitial();
          }
          await sleep(pollIntervalMs);
          if (done || controller.signal.aborted) return;
          const before = seenEventIds.size;
          for (const event of await listAllEvents(api, cmaSessionId)) {
            await handled(event, false);
            if (done) return;
          }
          attempts = 0;
          if (seenEventIds.size > before) {
            quietPolls = 0;
            continue;
          }
          quietPolls++;
          if (quietPolls < 2) continue;
          const session = await api.getSession(cmaSessionId);
          if (session.status === "terminated") {
            await handled({ type: "session.status_terminated" }, false);
          } else if (session.status === "idle" && sawAgentEvent && !sentSinceLastAgentEvent) {
            const unresolved = [...pendingTools.keys()].filter((id) => !resulted.has(id));
            await handled(
              {
                type: "session.status_idle",
                stop_reason: unresolved.length
                  ? { type: "requires_action", event_ids: unresolved }
                  : { type: "end_turn" },
              },
              false,
            );
            quietPolls = 0;
          }
        } catch (error) {
          if (done || controller.signal.aborted) return;
          await retryOrThrow(error, ++attempts);
        }
      }
    };

    const wallMs = turn.turnWallClockMs ?? defaultTurnWallClockMs;
    let timer: NodeJS.Timeout | undefined;
    const assembleReply = (): string => {
      const byEvent = new Map<string, string>(deltaTexts);
      for (const [id, text] of texts) byEvent.set(id, text);
      return [...byEvent.values()]
        .filter((text) => text.trim())
        .join("\n\n")
        .trim();
    };
    const finishInterrupted = async (): Promise<HarnessTurnResult> => {
      const terminal = ref.silentRequested || ref.pausedOnApproval;
      const reply = terminal ? "" : assembleReply();
      if (reply)
        await emit({ type: "assistant", payload: { text: reply, stopped: true }, scopeLabel: turn.scopeLabel });
      return {
        reply,
        stopped: true,
        ...(ref.silentRequested ? { silent: true } : {}),
        ...(ref.pendingApprovals?.length ? { pendingApprovals: ref.pendingApprovals } : {}),
        ...(ref.pausedOnApproval ? { pausedOnApproval: true } : {}),
        modelCalls: Math.max(1, modelCalls),
        ...(tapeWriteFailed ? { tapeWriteFailed: true } : {}),
      };
    };
    try {
      const consume = delivery === "poll" ? consumePoll() : consumeStream();
      consume.catch(() => undefined);
      try {
        await (wallMs > 0
          ? Promise.race([
              consume,
              new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                  void interrupt(false);
                  reject(new NonRetryableTurnError(`CMA turn exceeded ${Math.round(wallMs / 1000)}s wall clock`));
                }, wallMs);
              }),
            ])
          : consume);
      } catch (error) {
        if (controller.signal.aborted && !(error instanceof NonRetryableTurnError)) return await finishInterrupted();
        if (error instanceof CmaEventWindowExceeded && !mode.ephemeral) {
          await sessionRecords.delete(turn.session.id).catch(swallowAs("cma: overflow rotation", undefined));
        }
        throw classifyTurnError(error);
      }
      if (!done && controller.signal.aborted) return await finishInterrupted();
      const terminal = ref.silentRequested || ref.pausedOnApproval;
      const reply = terminal ? "" : assembleReply();
      if (reply)
        await emit({
          type: "assistant",
          payload: { text: reply, ...(stopped ? { stopped: true } : {}) },
          scopeLabel: turn.scopeLabel,
        });
      return {
        reply,
        ...(stopped ? { stopped: true as const } : {}),
        ...(ref.silentRequested ? { silent: true } : {}),
        ...(ref.pendingApprovals?.length ? { pendingApprovals: ref.pendingApprovals } : {}),
        ...(ref.pausedOnApproval ? { pausedOnApproval: true } : {}),
        modelCalls: Math.max(1, modelCalls),
        ...(tapeWriteFailed ? { tapeWriteFailed: true } : {}),
      };
    } finally {
      if (timer) clearTimeout(timer);
      if (recordedSteps === 0) await recordStep();
      await stopSignals?.();
      await interruptSend;
      turn.cancel?.removeEventListener("abort", onCancel);
      controller.abort();
      active.delete(controller);
      if (mode.ephemeral) {
        await api.deleteSession(cmaSessionId).catch(swallowAs("cma: oneshot cleanup", undefined));
      } else if (messageSent) {
        await sessionRecords
          .merge(turn.session.id, { lastSeq: Math.max(maxSeq, ensured.priorLastSeq), updatedAt: Date.now() })
          .catch(swallowAs("cma: session record", undefined));
      }
    }
  };

  const single = async (
    systemPrompt: string,
    prompt: string,
    signal?: AbortSignal,
    observe?: Pick<HarnessTurnInput, "recordModelCall" | "recordLlmRequest">,
    modelOverride?: string,
  ): Promise<string | undefined> => {
    const session = { id: `oneshot-${randomBytes(8).toString("hex")}` } as HarnessTurnInput["session"];
    const scope = { kind: "org", id: "oneshot" } as unknown as ScopeId;
    const emitted: SessionEntry[] = [];
    const result = await runPrompt(
      {
        session,
        input: prompt,
        systemPrompt,
        history: [],
        tools: {} as HarnessTurnInput["tools"],
        scopeLabel: scope,
        orgScopeId: scope,
        ...(signal ? { cancel: signal } : {}),
        ...(modelOverride ? { model: modelOverride } : {}),
        readOnly: true,
        emit: async (entry) => {
          const saved = {
            ...entry,
            sessionId: session.id,
            seq: emitted.length + 1,
            createdAt: Date.now(),
          } as SessionEntry;
          emitted.push(saved);
          return saved;
        },
        recordModelCall: observe?.recordModelCall ?? (() => {}),
        ...(observe?.recordLlmRequest ? { recordLlmRequest: observe.recordLlmRequest } : {}),
      },
      { toolsEnabled: false, ephemeral: true },
    );
    return result.reply || undefined;
  };

  return defineHarness(
    {
      id: "cma",
      controlTransport: "api",
      toolTransport: "dynamic",
      transcriptFormat: "cma-events",
      capabilities: new Set(["abort", "steer", "images", "provider-sessions"]),
    },
    {
      runTurn: (turn) => runPrompt(turn),
      close: () => {
        for (const controller of active) controller.abort();
        active.clear();
      },
      resetSession: async (sessionId) => {
        await sessionRecords.delete(sessionId);
      },
      async shouldRespond(detect) {
        try {
          const out = await single(
            buildDetectionPrompt(detect.reactionGuidance),
            renderDetectPrompt(detect),
            undefined,
            { recordModelCall: detect.recordModelCall },
            judgeModelId,
          );
          return parseDetectVerdict((out ?? "").trim(), Boolean(detect.reactionGuidance?.trim()));
        } catch (error) {
          swallow("cma: detect", error);
          return { respond: false };
        }
      },
      async compactHistory(input) {
        try {
          const out = await single(CONTEXT_COMPACTION_PROMPT, compactTranscript(input.history), undefined, {
            recordModelCall: input.recordModelCall,
          });
          return out ?? deterministicCompactSummary(input.history);
        } catch (error) {
          swallow("cma: compact", error);
          return deterministicCompactSummary(input.history);
        }
      },
      contextTokenBudget(scopeLabel, model) {
        const id = modelSupportedByHarness(model, "cma") ? model! : resolveModelId(scopeLabel as ScopeId | undefined);
        return contextTokenBudgetForModel(id);
      },
      oneShot: (system, prompt) => single(system, prompt),
      judge: (system, prompt) => single(system, prompt, undefined, undefined, judgeModelId),
      screenSecurity: async ({ payload, signal, recordModelCall, recordLlmRequest }) =>
        parseSecurityScreenVerdict(
          await single(SECURITY_SCREEN_SYSTEM_PROMPT, payload, signal, {
            recordModelCall,
            ...(recordLlmRequest ? { recordLlmRequest } : {}),
          }),
        ),
      generateTitle: async (transcript) => sanitizeTitle(await single(TITLE_GENERATION_PROMPT, transcript)),
      summarizeApproval: async (command, reason, purpose) =>
        single(
          "Explain this command in one plain-English sentence for an approver.",
          [command, reason, purpose].filter(Boolean).join("\n"),
        ),
    },
  );
}
