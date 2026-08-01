# Spike: CMA self-hosted sandbox worker on QM's durable volumes

This branch holds the phase 2 spike described in [`harness-cma.md`](./harness-cma.md).
The question it answers: can QM service CMA's native toolset from a worker in the
operator's infrastructure, routing each session's execution onto its scope's durable
volume, using only the publicly documented protocol?

Short answer: yes, with two gaps that need a live environment to close.

## What the prototype proves

`src/harness/cma-worker.ts` plus `test/cma-worker.test.ts` implement and exercise the
whole loop against a scripted work-queue server:

1. Poll `GET /v1/environments/{id}/work/poll`, claim a work item, ack it.
2. Keep the lease alive with heartbeats, chaining `expected_last_heartbeat` from
   `NO_HEARTBEAT` through each server-returned value.
3. Look up which QM scope the CMA session belongs to. The phase 1 adapter already
   stamps `qm_session` and `qm_scope` into session metadata and keeps a durable
   session map, so per-scope routing needs no new state. The stock SDK worker takes
   one static workdir; a custom worker is free to route per session, and that was the
   biggest open question going in.
4. Stream the session's events, execute `bash` calls through an injected per-scope
   executor (in the real integration this is the existing `Sandbox` interface, which
   already fronts local, Sprites, and AWS volumes), and post `user.tool_result` events.
5. Refuse a policy-violating command and return the denial as the tool result, so QM's
   command policy applies at execution time. The model sees the refusal and can adjust.
6. Stop the work item when the session goes idle.

One detail matters for deployment shape: the worker does not need to run where the
volumes are mounted. It calls the same `Sandbox` interface the core uses, so it can run
next to the core and reach local, Sprites, or AWS scope computers through the existing
routing. No new infrastructure tier.

## Open questions that need a live environment

- Native tool schemas. The docs name the tools (`bash`, `read`, `write`, `edit`,
  `glob`, `grep`, `web_fetch`, `web_search`) but publish no input or output schemas;
  those live in SDK source. The prototype handles `bash` and returns a clear
  unsupported-tool result for the rest. Before phase 2 ships, either verify the shapes
  against a live session or implement via the official SDK's worker extension points.
- Whether work items for a session's turn expect the worker to answer `agent.tool_use`
  via `user.tool_result` events (the documented event exists for exactly this) or via
  a channel the docs do not describe. The docs describe the flow in prose; the exact
  contract needs one live turn to confirm.
- Lease TTL defaults and reclaim behavior when a worker dies mid-turn.
- The docs pages disagree on the poll route (`POST /work/poller` on one page,
  `GET /work/poll` on the API reference). The prototype follows the API reference.

## What phase 2 changes when it lands

- Agent sessions declare the native toolset alongside QM's custom tools (the API
  supports mixing). Shell and file work moves to native tools; `publish`, `memory`,
  `history`, sharing, and the other multiplayer verbs stay custom tools through the
  core.
- Strict posture maps to CMA's `permission_policy: always_ask` with confirmations
  forwarded from QM's approval surfaces, replacing the phase 1 gate for native tools
  only.
- The worker runs under the core's supervisor with the environment key as its only new
  credential. Keychain injection at sandbox egress and the egress audit continue to
  apply because execution still happens inside QM's sandboxes.

## Status

Prototype and tests pass. Do not merge this branch as is: the native tool coverage is
`bash` only, nothing wires the worker into the core's supervisor yet, and the two
protocol gaps above are unverified against a live environment.
