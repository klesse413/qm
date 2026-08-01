# The Claude Managed Agents harness

`HARNESS=cma` runs the agent loop on [Claude Managed Agents](https://platform.claude.com/docs/en/managed-agents/overview)
(CMA) instead of a harness process inside the core. CMA hosts the model loop and the
conversation state. QM keeps everything else: identity, policy, approvals, durable
history, files, and each scope's sandbox. Every QM session maps to a persistent CMA
session, and that mapping lives in a Postgres-backed map (`cma_harness_sessions`), so
conversations resume across core restarts and blue-green deploys.

## How a turn works

1. The adapter looks up the CMA session for the QM session. If there is no mapping, or
   the resolved system prompt or model changed, or the CMA session is gone, it creates a
   fresh CMA session and seeds it with a replayed transcript of QM's durable history.
   CMA fixes `system` and `model` for a session's lifetime, so a change to either
   rotates the session. Nothing is lost in a rotation because QM's log is the system of
   record.
2. QM's tool surface (`execute`, `read`, `write`, `publish`, `memory`, `history`,
   `background`, and the rest) is declared to CMA as custom tools. When the model calls
   one, CMA pauses the session and hands the call back as an `agent.custom_tool_use`
   event. QM runs it through the same shared tool bridge every harness uses, so command
   policy, approvals, screening, and the scope's durable sandbox all apply, then posts a
   `user.custom_tool_result` event and the model continues.
3. Output streams back over the sessions event stream (SSE) with incremental text
   deltas. The turn ends when the session reports `session.status_idle` with
   `stop_reason: end_turn`.
4. After the turn, the mapping records how much QM history the CMA session has seen, so
   the next turn sends only the new message. Entries the CMA session has never seen,
   like overheard messages or turns run on another harness, are replayed as a transcript
   prefix.

## Setup

Create a CMA environment and agent once, then point the deployment at them:

```bash
curl https://api.anthropic.com/v1/environments \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "anthropic-beta: managed-agents-2026-04-01" \
  -d '{"name": "qm", "config": {"type": "cloud"}}'

curl https://api.anthropic.com/v1/agents \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "anthropic-beta: managed-agents-2026-04-01" \
  -d '{"name": "qm", "model": "claude-opus-5"}'
```

The agent is boilerplate. QM overrides its system prompt, model, and tools on every
session it creates, so the deployment only needs the two ids:

```bash
HARNESS=cma
CMA_ENVIRONMENT_ID=env_...
CMA_AGENT_ID=agent_...
ANTHROPIC_API_KEY=sk-ant-...
```

Set `CMA_API_KEY` instead of (or alongside) `ANTHROPIC_API_KEY` to bill CMA sessions to
a separate workspace key.

Optional knobs, all listed in [`.env.example`](../.env.example): `CMA_MODEL` (default
`claude-opus-5`), `CMA_DELIVERY` (`stream` or `poll`), `CMA_BASE_URL`, and
`CMA_VAULT_IDS`. Auth is an API key from the environment. The client takes a header
provider internally, so workload-identity auth can slot in later without restructuring.

## Where execution happens today

All command execution flows through QM's `execute` tool into the scope's own sandbox
(`SANDBOX_BACKEND`: local, Sprites, or AWS), with QM's command policy, egress policy,
and audit applied as usual. The adapter never enables CMA's built-in toolset, so the
`environment_id` the sessions API requires hosts no QM workload in this mode and a plain
`cloud` environment works fine.

This preserves QM's durable-computer contract: each person and each room keeps one
standing computer where installed tools and files persist indefinitely, across sessions
and across harness switches. CMA's own sandboxes are scoped to a session and reclaimed
after 30 days, which is a different contract.

## Next step: the self-hosted sandbox integration

The custom-tool design above is phase one. The planned second phase enables CMA's
native toolset and services it with a QM worker registered as a
[self-hosted sandbox](https://platform.claude.com/docs/en/managed-agents/self-hosted-sandboxes):
the worker claims per-turn work items from the environment's queue, routes each session
onto its scope's durable volume, enforces QM's command policy at execution time, and
returns results as `user.tool_result` events.

That shape keeps QM's durable computer and control plane while CMA owns the full
brain-to-hands protocol. It also gets three things the custom-tool path cannot: the
model works with the native tools it was trained on, tool traffic becomes visible to
CMA's console and permission controls, and tool results stop counting against the
public API's write rate limits. The multiplayer verbs (publish, memory, sharing, reach)
stay custom tools in that phase too, because they operate on QM's stores rather than on
a sandbox. The worker protocol is still in beta, so this lands as a separate change once
its remaining questions are settled; the current adapter is forward-compatible with it,
since native and custom tools can coexist on one agent.

## Security postures

- **Strict** is enforced at the QM layer, the same as on every harness: the shared tool
  gate holds each tool call for human approval before it executes. Under CMA the blocked
  call is answered with a `user.custom_tool_result` explaining the hold, the session is
  interrupted, and the approved re-run retries the call. The blocked attempt stays in
  the CMA transcript, and the approved turn re-sends the user message, so the model sees
  the retry in context. CMA has its own `permission_policy: always_ask` control, but it
  only gates tools CMA executes; since QM executes every tool in this phase, the QM
  gate is the one that matters.
- **Auto** works unchanged. Inbound screening and tool-result screening run inside QM's
  shared tool bridge before results are forwarded to CMA.
- **Dangerous** has no pauses, and the predeclared command policy still applies inside
  QM's tools.

## Connectivity

QM deployments often sit behind SSO or private networks and cannot receive inbound
webhook POSTs, so the adapter never registers webhooks. Both delivery modes use only
outbound HTTPS to `api.anthropic.com`:

- `CMA_DELIVERY=stream` (default) holds an SSE stream per active turn, with incremental
  text deltas. On disconnect the adapter reconnects and backfills from the events list,
  deduplicating by event id.
- `CMA_DELIVERY=poll` lists the session's events on an interval. There are no
  incremental deltas; replies arrive whole. Use this where long-lived streaming
  connections are unreliable (aggressive proxies, some egress gateways).

## Keychain and vaults

QM's keychain stays the credential path for QM tools. `execute` runs in the scope
sandbox, where the keychain injects credentials at egress, per scope, the same as on
every harness. CMA vaults inject credentials on CMA's side, into MCP servers or sandbox
egress for tools CMA executes. Since this phase executes tools in QM, vaults only matter
if the base agent is given MCP servers; `CMA_VAULT_IDS` passes a static vault list to
every session for that case. Per-scope vault granularity is not expressible this way.
The mapping is deployment-wide, and that is a limitation of the current integration.

## Known limitations

- The model and system prompt are fixed per CMA session, so changing either rotates to
  a fresh CMA session seeded with replayed history. Frequent model switching pays that
  re-seed each time.
- Effort and fast-mode controls are not offered. CMA applies `effort` on the agent
  rather than on per-session model overrides.
- Steering messages are queued and delivered between turns of the CMA session rather
  than injected mid-generation.
- Subagents are not offered on this harness yet.
- The sessions API events the adapter consumes carry no per-turn token usage or cost,
  so budget accounting estimates input tokens from the system prompt, the message, and
  tool results.
- The events list API documents no ordering or since-cursor, so polling mode and stream
  reconnects re-list the session's events and deduplicate. On long-lived sessions that
  costs I/O proportional to history length; prefer `stream`, which only pays it on
  reconnect. A session whose listable history outgrows the adapter's paging window is
  rotated to a fresh session automatically.
- Model utilities (message detection, judging, title generation, screening) each run as
  a throwaway CMA session that is created, driven, and deleted, which costs a few extra
  API round trips per call compared to a single model request.
- CMA retains session history on Anthropic's side. QM deletes nothing there
  automatically except those one-shot utility sessions; operators who need shorter
  retention should archive or delete sessions via the CMA API.
