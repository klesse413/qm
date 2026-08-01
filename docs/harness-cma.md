# The Claude Managed Agents harness

`HARNESS=cma` runs the agent loop on [Claude Managed Agents](https://platform.claude.com/docs/en/managed-agents/overview)
(CMA), Anthropic's server-hosted agent runtime, instead of a harness process inside the
core. Each QM session maps to a persistent CMA session that holds the conversation state
on Anthropic's side; QM stays the system of record for durable history, identity, policy,
files, and the per-scope sandbox.

## How a turn works

1. The adapter looks up the QM session's CMA session id in a durable Postgres-backed map
   (`cma_harness_sessions`). If there is no mapping, or the resolved system prompt or
   model changed, or the CMA session is gone, it creates a fresh CMA session — overriding
   the base agent's `system`, `model`, and `tools` per session — and seeds it with a
   replayed transcript of QM's durable history, so nothing is lost across rotations.
2. QM's fixed tool surface (`execute`, `read`, `write`, `publish`, `memory`, `history`,
   `background`, …) is declared to CMA as **custom tools**. The model runs on CMA; every
   tool call comes back as an `agent.custom_tool_use` event, QM executes it locally —
   `execute` runs in the scope's own durable sandbox, exactly as with every other
   harness — and returns a `user.custom_tool_result` event.
3. Output streams back over the sessions event stream (SSE) with incremental text
   deltas; the turn ends when the session reports `session.status_idle` with
   `stop_reason: end_turn`.
4. After the turn, the mapping records how much QM history the CMA session has seen, so
   the next turn sends only the new message. Entries the CMA session has not seen
   (overheard messages, turns run on another harness) are replayed as a transcript
   prefix. Because both sides are durable, a core restart or blue-green deploy resumes
   mid-conversation.

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

The agent is boilerplate: QM overrides its system prompt, model, and tools on every
session it creates, so the ids are all that matter.

```bash
HARNESS=cma
CMA_ENVIRONMENT_ID=env_...
CMA_AGENT_ID=agent_...
ANTHROPIC_API_KEY=sk-ant-...
```

Set `CMA_API_KEY` instead of (or alongside) `ANTHROPIC_API_KEY` to bill CMA sessions to
a separate workspace key.

Optional knobs, all in [`.env.example`](../.env.example): `CMA_MODEL` (default
`claude-opus-5`), `CMA_DELIVERY` (`stream` or `poll`), `CMA_BASE_URL`, and
`CMA_VAULT_IDS`. Auth is an API key from the environment; the client takes a header
provider internally, so workload-identity auth can slot in later without restructuring.

## Managed vs self-hosted sandbox

QM keeps its own durable-computer semantics under CMA by construction: the adapter never
enables CMA's built-in toolset, so all command execution flows through QM's `execute`
tool into the per-scope sandbox (`SANDBOX_BACKEND` — local, Sprites, or AWS), with QM's
command policy, egress policy, and audit applied as usual. The CMA `environment_id` is
required by the sessions API but hosts no QM workload in this mode, so a plain `cloud`
environment is fine.

CMA also offers [self-hosted sandboxes](https://platform.claude.com/docs/en/managed-agents/self-hosted-sandboxes),
where a worker in your infrastructure polls the environment's work queue and executes
CMA's native tools. That is the right mapping if you later want CMA's own `bash` tool:
create the environment with `{"type": "self_hosted"}` and run the worker yourself, so
execution stays in your infrastructure. Enabling CMA's native toolset changes semantics
either way — commands run in the CMA session's workspace (per-session, reclaimed after
30 days of session life), not in QM's per-scope durable computer, and QM's command
policy, approval flow, and egress audit do not apply to them. The adapter therefore does
not enable it; if that trade-off is ever wanted, it should be a deliberate, separate
change.

## Security posture mapping

QM postures map onto CMA as follows:

- **Strict** — enforced at the QM layer, same as every harness: the shared tool gate
  holds each tool call for human approval _before_ it executes. Under CMA the blocked
  call is answered with a `user.custom_tool_result` explaining the hold, the session is
  interrupted, and the approved re-run retries the call. The seam: the blocked attempt
  stays in the CMA transcript, and the approved turn re-sends the user message, so the
  model sees the retry in context. CMA's native `permission_policy: always_ask` control
  is the analogous server-side mechanism, but it only gates CMA-executed tools; since QM
  executes every tool here, the QM-layer gate is the effective control.
- **Auto** — inbound screening and tool-result screening run inside QM's shared tool
  bridge before results are forwarded to CMA, unchanged.
- **Dangerous** — no pauses; the predeclared command policy still applies inside QM's
  tools.

## Connectivity

QM deployments often sit behind SSO or private networks and cannot receive inbound
webhook POSTs, so the adapter never registers webhooks. Both delivery modes use only
outbound HTTPS to `api.anthropic.com`:

- `CMA_DELIVERY=stream` (default) — hold an SSE stream per active turn, with incremental
  text deltas. On disconnect the adapter reconnects and backfills from the events list,
  deduplicating by event id.
- `CMA_DELIVERY=poll` — list the session's events on an interval. No incremental deltas;
  replies arrive whole. Use this where long-lived streaming connections are unreliable
  (aggressive proxies, some egress gateways).

## Keychain and vaults

QM's keychain stays the credential path for QM tools: `execute` runs in the scope
sandbox where the keychain injects credentials at egress, per scope, as with every
harness. CMA **vaults** inject credentials on CMA's side — into MCP servers or
sandbox egress for CMA-executed tools. Since this adapter executes tools in QM, vaults
are only relevant if the base agent is given MCP servers; `CMA_VAULT_IDS` passes a
static vault list to every session for that case. Per-scope vault granularity is not
expressible this way — the mapping is deployment-wide, not per QM scope — which is an
honest limitation of the current integration, not of vaults themselves.

## Known limitations

- The model and system prompt are fixed per CMA session, so changing either rotates to
  a fresh CMA session (seeded with replayed history). Frequent model switching therefore
  costs a re-seed each time.
- Effort/thinking-level and fast-mode controls are not offered: CMA applies `effort` on
  the agent, not on per-session model overrides.
- Steering messages are queued and delivered between turns of the CMA session rather
  than injected mid-generation.
- Subagents (`Agent`-style child tasks) are not offered on this harness yet.
- Per-turn token usage and cost are not reported by the sessions API events the adapter
  consumes, so budget accounting uses estimated input tokens.
- The events list API documents no ordering or since-cursor, so polling mode and stream
  reconnects re-list the session's events and deduplicate by id. On long-lived sessions
  that costs I/O proportional to history length — prefer `stream`, which pays it only on
  reconnect.
- CMA retains session history on Anthropic's side. QM deletes nothing there
  automatically except one-shot utility sessions; operators who need shorter retention
  should archive or delete sessions via the CMA API.
