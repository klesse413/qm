---
name: verify
description: Boot the QM core against in-memory stores and drive real turns through its HTTP API to observe a change end to end.
---

# Verify a core change by driving the HTTP API

The core's user-facing surface is `POST /v1/turns`. The fastest live handle mirrors
`scripts/api-livetest.ts`: build the app in-process, serve it unauthenticated, and post
turns.

```ts
import { loadConfig } from "./src/config.ts";
import { buildApp } from "./src/wiring.ts";
import { createInsecureTestServer } from "./src/api/server.ts";

const built = buildApp(loadConfig({ HARNESS: "mock", ORG_ID: "acme", DATA_DIR: "<tmpdir>", SEED_SKILLS: "0" }));
const server = createInsecureTestServer(built.app);
```

Run scripts with plain `node script.ts` (Node ≥ 24 type-strips). Post a DM turn:

```json
{
  "surface": "battery",
  "actor": { "externalId": "U1" },
  "conversation": { "kind": "dm", "threadRef": "t1" },
  "text": "hi"
}
```

The JSON response carries `status` (`ok`, `pending_approval`, `silent`, `refused`),
`reply`, and `pendingApprovals`. Resolve an approval by re-posting the same turn with
`approval: { "requestId": "...", "approved": true, "scope": "session" }`. Postures come
from `HARNESS_SECURITY_POSTURE` (`strict` pauses every harness tool call).

Gotchas learned the hard way:

- `SANDBOX_BACKEND=local` needs a Docker daemon; without one, `execute` returns
  "requires a running Docker daemon" as the tool result — the turn flow still exercises
  end to end, but commands don't actually run.
- One-shot model utilities (title generation, approval summaries, detect) also flow
  through the selected harness — expect extra provider calls beyond the turn itself.
- For a harness that talks to a remote API, point its base-URL env at a local scripted
  fake (see `test/cma-harness.test.ts` for the shape) and drive `/v1/turns` against it.
- `HARNESS=mock` answers turns with canned text and needs no provider credentials.
