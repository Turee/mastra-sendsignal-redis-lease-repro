# `sendSignal({ requireDelivery: true })` accepted as `deliver` to an ended run: REPRODUCED with Redis Streams pubsub

This is a standalone repro against the npm releases `@mastra/core@1.78.0` and `@mastra/redis-streams@0.6.3`. Storage
is `InMemoryStore`, the model is a hand-written `LanguageModelV2` stub, there is no FGA and no API key is needed.

**Result:** the message loss reproduces, at up to 16/30 iterations at the best timing. The same script with
Mastra's default in-process pubsub never shows it (0 in 330 iterations).

The cause is **not** `Session.sendSignal` reading "running" before its awaits. In every reproducing iteration, `Session.sendSignal` took its *idle*
branch, not the stale "running" branch. The idle branch passes `streamOptions` and calls `agent.sendSignal` with
`ifIdle: wake`. The runtime's wake path then loses the thread's **Redis lease** to the run that has just ended,
because that run still holds the lease. The runtime hands the signal off to that run as if it were a live owner in
another process, and returns `{ action: 'deliver', runId: <ended run> }`. Nothing drains it.

## Expected vs actual

| | Expected | Actual (Redis Streams pubsub, failing iterations) |
|---|---|---|
| `accepted` | `wake` with a new run, or a `deliver` that gets drained | `{ action: 'deliver', runId: <run 1> }`, with run 1 already finished |
| New run after run 1's `agent_end` | one, which answers the follow-up | none for 3 s (the script's `SETTLE_MS`) |
| Model sees the follow-up | yes | no |
| Follow-up persisted | yes | no |

The script uses a strict oracle. It exits 1 (`BUG REPRODUCED`) only if:

- `accepted.action === 'deliver'`;
- `accepted.runId === run1`;
- no `agent_start` follows run 1's `agent_end` within 3 s;
- the follow-up never reached the model;
- the follow-up was not persisted.

Every iteration then runs an **idle control** on the same session. It waits until the session is idle, then 1 s more
so the last lease is released, then sends `control-<i>` with `requireDelivery`. The control must settle `wake` and be
answered and persisted. If any control fails, the script exits 2. No control failed in the round-2 results below.

## Event order in a reproducing iteration

This is `results/trace-delay-15.log`, with `TRACE=1`, `STRATEGY=delay` and `DELAY_MS=15`. The `redis …` lines are an
extra topic subscriber, and the `runtime.sendSignal` lines come from `probe.ts`.

```
 122ms model.doStream run1 FINAL step
 130ms   redis run-registered run=fe466108          <- the session only learns about run 1 now (Redis echo)
 131ms session agent_start [stream.activeRunId=undefined]   <- run 1 already finished locally; the session is replaying it
 135ms session usage_update
 137ms SEND follow-up (isRunning=true runId=fe466108 activeRunId=undefined) -> idle branch
 138ms   runtime.sendSignal "follow-up-0" ifIdle.streamOptions=true getActiveThreadRunId=undefined
 145ms   runtime.sendSignal "follow-up-0" -> deliver fe466108   <- lease still held by run 1
 145ms session usage_update
 146ms   redis signal-enqueued run=fe466108 signal="follow-up-0"
 147ms session agent_end complete
 147ms accepted -> {"accepted":true,"runId":"fe466108-…","action":"deliver"}
 147ms   redis run-completed run=fe466108
4168ms model.doStream answer "control-0"                 <- the next model call is the control; follow-up never seen
```

This matches the order seen in production: `usage_update`, then the signal is sent, then `message_end`, then `agent_end`,
then `accepted` settles `deliver` for the ended run.

## Mechanism (1.78.0 dist)

Line numbers are in `node_modules/@mastra/core/dist/storage-7mYy2C-B.js` (`AgentThreadStreamRuntime`) unless
another file is named.

1. **The session lags behind the run.** With Redis, every broadcast stream part waits for its `XADD`
   (`#withBroadcastStream` → `#publishAndWait`, about L1802). Run 1's output therefore reaches status `success`
   several ms before the session sees its `message_end` / `agent_end`. In that window `#isThreadBlockingRun` (L1024)
   is already false, even before the completion watcher has run. The session's sample reads `isRunning=true`,
   `runId=run1` and `stream.activeRunId()=null`. That sample sends `Session.sendSignal` down its idle branch
   (`agent-controller-DRiP9C5v.js` L5331), not the stale branch (L5258).
2. **The completion watcher clears local state before it releases the lease.** `#watchThreadRunCompletion`
   (L2673–2720) removes run 1 from `threadRunsById` and `activeThreadRunIds` (around L2690–2700). Only after
   `broadcastFinished` does it check `#hasPendingThreadWork`. If no work is pending, it calls `#releaseThreadLease`
   (L2718, then L743–748), which fires the Redis `releaseLease` and does not wait for it.
3. **The wake path loses the lease to the ended run.** `sendSignal` (L4131) finds no active run and reserves a new
   `runId`. It then calls `leaseProvider.acquireLease` (L4359). On Redis that is `SET NX`, then an eval, then `GET`
   (`@mastra/redis-streams/dist/index.js` L607–635). Run 1 still owns the key, so the result is
   `{ acquired: false, owner: run1 }`. The runtime publishes `signal-enqueued` for run 1 and returns
   `{ action: 'deliver', runId: run1 }` (L4365–4377).
4. **Nobody drains it.** The `signal-enqueued` echo for run 1 reaches the thread's active subscription, but that
   subscription ignores it (L857–867): run 1 has already been removed from `threadKeysByRunId` and is not in
   `ownedRunIds`. The watcher logs no pending work and releases the lease. 6–8 s later all queues (pending, pre-run,
   pending-idle) are empty, so the signal is simply dropped. In the `lateFollowUpAfterControl=true` subset it was
   left queued instead and is picked up only by the next run on the thread. When the echo is admitted before the
   watcher's pending-work check, the watcher drains it and starts a follow-up run. Those are the
   `deliver(run1) newRun=true answered=true` rows below.
5. **A second lost-message variant.** When the lease is released between `SET NX` and `GET`, `owner` comes back
   undefined. `sendSignal` then returns `deliver` with the reserved `runId`, and that run never starts. These are the
   `deliver(new) newRun=false answered=false` rows. The message is lost the same way, but the strict oracle does not
   count it, because `runId` is not run 1.

With the in-process pubsub, the lease operations and pubsub delivery settle within microtasks, so the window does not
open. With Redis, every step above is a network round trip.

## Strategies and rates

These are round 2, Redis pubsub, `STEPS=2` (a tool step, then a final text step) and `STEP_MS=5`. Each strategy ran
30 iterations in a fresh process. `branch` is the path `Session.sendSignal` took from its own sample (`running` is
the stale-sample branch).

| Strategy | BUG (strict) | Other outcomes |
|---|---|---|
| (a) `agent_end`: send synchronously in the session's run-1 `agent_end` listener | 0/30 | 26 idle wake, answered; **4 `deliver(new)`, never answered** |
| `event:usage_update:2` (the final step's usage, the production order) | **6/30** | 22 `deliver(run1)` drained by a new run; 2 `deliver(new)` lost |
| `event:message_end:2` (the final assistant `message_end`) | 0/30 | 30 idle wake, answered |
| `event:agent_start:1` | 0/30 | 27 `running` branch, delivered into run 1 and answered; 3 drained |
| (b) delay 0 ms after the final step's `doStream` | 0/30 | 30 idle `deliver(new)`, answered |
| delay 2 | 0/30 | 28 idle `deliver(new)`, answered; 2 `running`, answered |
| delay 4 | 0/30 | 20 `running`, answered; 10 idle `deliver(new)`, answered |
| delay 6 | **1/30** | 26 drained; 3 other, answered |
| delay 8 | 0/30 | 30 drained |
| delay 10 | 0/30 | 29 drained; 1 answered |
| delay 12 | **2/30** | 28 drained |
| delay 15 | **16/30** | 8 drained; 6 `deliver(new)` lost |
| delay 18 | **9/30** | 11 wake; 10 `deliver(new)` lost |
| delay 20 | **1/30** | 23 wake; 6 `deliver(new)` lost |
| delay 25 | 0/30 | 29 wake; 1 `deliver(new)` lost |
| `STEPS=1`, `event:usage_update:1` | **6/30** | 24 drained |

Across all of these:

- Every BUG iteration was on the **idle** branch.
- The stale "running" branch was taken in 50 iterations and never produced the bug. In each of those, run 1 was
  still active and the signal was answered.
- The traced run (`TRACE=1`, delay 15) reproduced 5/10.

Of the 16 BUG iterations at delay 15, 4 show `lateFollowUpAfterControl=true`: the stranded follow-up was persisted
only later, inside the control's run. A thread with no later message never answers it.

The round-1 numbers are in `results-round1/`. They are similar, for example `usage_update:2` at 7/30 and delay 15 at
23/30. In that round the control was sent immediately after idle, and it sometimes hit the same lease race itself
(`deliver` instead of `wake`, though still answered). Those runs exited 2, which led to the 1 s wait before the
control.

**In-process pubsub comparison** (`PUBSUB=memory`, 30 each): `agent_end`, `usage_update:2`, and delays 0, 2, 4, 6,
8, 10, 12, 15 and 18 gave **0/330 BUG**. Every follow-up was answered.

Option (c), an FGA gate that permits the woken run, was not needed and was not tried.

## Run

```sh
docker run -d --name mastra-repro-redis -p 6390:6379 redis:7-alpine
npm install --registry=https://registry.npmjs.org/
STRATEGY=delay DELAY_MS=15 ITER=30 npx tsx repro.ts      # best window; exit 1 = BUG REPRODUCED
STRATEGY=event:usage_update:2 ITER=30 npx tsx repro.ts   # production event order
./sweep.sh            # all strategies above -> results/*.log, results/summary.txt
./sweep-memory.sh     # same with PUBSUB=memory (in-process pubsub) for comparison
TRACE=1 VERBOSE=1 STRATEGY=delay DELAY_MS=15 ITER=10 npx tsx repro.ts   # adds Redis topic + runtime probe lines
```

Exit codes: `1` means BUG REPRODUCED, `0` means NOT REPRODUCED, and `2` means a control failed.

These environment variables control the script:

| Variable | Values |
|---|---|
| `STRATEGY` | `agent_end`, `event:<session event type>:<nth>` or `delay` |
| `DELAY_MS` | for `delay` |
| `STEPS` | `1` or `2` (default `2`) |
| `STEP_MS` | final step length (default `5`) |
| `ITER` | default `30` |
| `SETTLE_MS` | default `3000` |
| `REDIS_URL` | default `redis://localhost:6390` |
| `PUBSUB` | `memory` for the comparison |
| `TRACE` | `1` adds diagnostics |
| `VERBOSE` | `1` |

`probe.ts` is used only with `TRACE=1`. It wraps the internal `AgentThreadStreamRuntime.sendSignal`, imported from
the published dist chunk, to log what the runtime saw. The verdict never uses it. Each iteration uses its own
`resourceId` and thread, and each process uses its own Redis key prefix.

## Environment

- `@mastra/core` 1.78.0, `@mastra/memory` 1.38.0, `@mastra/redis-streams` 0.6.3, `redis` 5.12.1, `zod` 4.6.7,
  `tsx` 4.23.15 (exact pins, from the public npm registry; no pg in this variant)
- Redis 7.4.11 (`redis:7-alpine`) in the docker container `mastra-repro-redis`, on host port 6390 (left running)
- Node v26.0.0, npm 11.12.1, macOS 26.7.1 (arm64)
