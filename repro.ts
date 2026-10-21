// Repro (Redis Streams pubsub + InMemoryStore): Session.sendSignal(..., { requireDelivery: true }) sent while a
// run is finishing can settle accepted as { action: 'deliver', runId: <run 1> } after run 1 has ended, and nothing
// drains the signal. The window is hit by timing alone (no FGA or other gate).
//
// env: STRATEGY = agent_end | event:<session event type>:<nth> (nth occurrence during run 1) | delay
//      (when to send the follow-up)
//      DELAY_MS (delay: ms after the final step's doStream starts), STEPS = 1 | 2, STEP_MS (final step length)
//      ITER (iterations), SETTLE_MS (how long to watch for a new run after a candidate 'deliver'), REDIS_URL
import { Agent } from '@mastra/core/agent';
import { AgentController } from '@mastra/core/agent-controller';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore } from '@mastra/core/storage';
import { createTool } from '@mastra/core/tools';
import { Memory } from '@mastra/memory';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import { z } from 'zod';

const STRATEGY = process.env.STRATEGY ?? 'agent_end';
const DELAY_MS = Number(process.env.DELAY_MS ?? 0);
const STEPS = Number(process.env.STEPS ?? 2);
const STEP_MS = Number(process.env.STEP_MS ?? 5);
const ITER = Number(process.env.ITER ?? 30);
const SETTLE_MS = Number(process.env.SETTLE_MS ?? 3000);
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6390';
const VERBOSE = process.env.VERBOSE === '1';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
let t0 = Date.now();
let events: string[] = [];
const log = (s: string) => {
  events.push(`${String(Date.now() - t0).padStart(4)}ms ${s}`);
  if (VERBOSE) console.log(events.at(-1));
};
process.on('unhandledRejection', e => log(`unhandledRejection: ${e}`));

// --- Hand-written LanguageModelV2 stub. Run 1: step 1 calls a tool (STEPS=2), the final step streams text for
// STEP_MS. Any other prompt (follow-up / control) gets a one-shot answer naming what it answered.
const prompts: string[] = [];
let onFinalStepStart: (() => void) | undefined;
const lastUserText = (prompt: any[]) => {
  const last = prompt.at(-1);
  if (last?.role === 'tool') return '<tool-result>';
  const users = prompt.filter(m => m.role === 'user');
  return users.at(-1)?.content?.map((p: any) => p.text ?? '').join('') ?? '';
};
const model: any = {
  specificationVersion: 'v2',
  provider: 'stub',
  modelId: 'stub-model',
  supportedUrls: {},
  async doGenerate() {
    throw new Error('unused');
  },
  async doStream(opts: any) {
    const text = lastUserText(opts.prompt);
    const all = JSON.stringify(opts.prompt);
    prompts.push(all);
    const isRun1First = text.startsWith('first') && !all.includes('"tool-result"');
    const toolStep = STEPS === 2 && isRun1First;
    const finalStep = (STEPS === 2 && text === '<tool-result>') || (STEPS === 1 && isRun1First);
    log(`model.doStream ${toolStep ? 'run1 tool step' : finalStep ? 'run1 FINAL step' : `answer "${text}"`}`);
    if (finalStep) onFinalStepStart?.();
    return {
      stream: new ReadableStream({
        async start(c) {
          c.enqueue({ type: 'stream-start', warnings: [] });
          c.enqueue({ type: 'response-metadata', id: 'r', modelId: 'stub-model', timestamp: new Date(0) });
          if (toolStep) {
            c.enqueue({ type: 'tool-call', toolCallId: 'tc1', toolName: 'noop', input: '{}' });
            c.enqueue({ type: 'finish', finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } });
            return c.close();
          }
          c.enqueue({ type: 'text-start', id: 't' });
          c.enqueue({ type: 'text-delta', id: 't', delta: finalStep ? 'final ' : `answer:${text}` });
          if (finalStep) {
            await sleep(STEP_MS);
            c.enqueue({ type: 'text-delta', id: 't', delta: 'answer' });
          }
          c.enqueue({ type: 'text-end', id: 't' });
          c.enqueue({ type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } });
          c.close();
        },
      }),
    };
  },
};

const noop = createTool({ id: 'noop', description: 'noop', inputSchema: z.object({}), execute: async () => ({ ok: true }) });
const storage = new InMemoryStore();
// PUBSUB=memory: comparison run with Mastra's default in-process pubsub (no Redis).
const redisPubsub = process.env.PUBSUB === 'memory' ? undefined : new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `repro-${process.pid}-${Date.now()}` });
const agent = new Agent({ id: 'a', name: 'a', instructions: 'test', model, tools: { noop } });
const controller = new AgentController({
  id: 'c',
  storage,
  memory: new Memory({ storage }),
  modes: [{ id: 'default', name: 'Default', default: true, agent }],
});
const mastra = new Mastra({ storage, ...(redisPubsub ? { pubsub: redisPubsub } : {}), logger: false, agentControllers: { c: controller } });
await controller.init();
const pubsub: any = redisPubsub ?? mastra.pubsub;

if (process.env.TRACE === '1') (await import('./probe.ts')).installProbe(s => log(s));
const settle = (p: Promise<any>) => p.then(a => a, e => ({ rejected: String(e) }));
const persisted = async (threadId: string) =>
  (await (await storage.getStore('memory'))!.listMessages({ threadId })).messages.map(
    (m: any) => `${m.role}:${(m.content.parts ?? []).map((p: any) => p.text ?? '').join('')}`,
  );
const waitFor = async (pred: () => boolean | Promise<boolean>, ms: number) => {
  const end = Date.now() + ms;
  while (!(await pred())) {
    if (Date.now() > end) return false;
    await sleep(2);
  }
  return true;
};

type Outcome = { verdict: string; bug: boolean; controlOk: boolean; controlInfo?: string; accepted: any; branch: string; order: string };

async function iteration(i: number): Promise<Outcome> {
  t0 = Date.now();
  events = [];
  const session = await controller.createSession({ id: `s${i}`, resourceId: `r${i}`, ownerId: 'o' });
  await session.state.set({ yolo: true } as any); // auto-approve the noop tool
  const threadId = session.thread.getId()!;
  const followUp = `follow-up-${i}`;
  let agentStarts = 0;
  let run1Ended = false;
  let startsAtRun1End = -1;
  let sent: { target: ReturnType<typeof session.sendSignal>; run1: string | null; branch: string } | undefined;
  const state = () =>
    `isRunning=${session.run.isRunning()} runId=${session.run.getRunId()?.slice(0, 8)} activeRunId=${session.stream.activeRunId()?.slice(0, 8)}`;
  const send = () => {
    if (sent) return;
    const running = Boolean(session.run.isRunning() && session.run.getRunId() && session.stream.activeRunId());
    const run1 = session.run.getRunId();
    log(`SEND follow-up (${state()}) -> ${running ? 'stale-running branch' : 'idle branch'}`);
    sent = {
      target: session.sendSignal({ content: followUp }, { requireDelivery: true }),
      run1,
      branch: running ? 'running' : 'idle',
    };
  };
  const unsub = session.subscribe((e: any) => {
    if (!['text_delta', 'message_update', 'part', 'display_state_changed'].includes(e.type))
      log(`session ${e.type}${e.reason ? ' ' + e.reason : ''} [stream.activeRunId=${session.stream.activeRunId()?.slice(0, 8)}]`);
    if (e.type === 'agent_start') agentStarts++;
    if (e.type === 'agent_end' && !run1Ended) {
      run1Ended = true;
      startsAtRun1End = agentStarts;
      if (STRATEGY === 'agent_end') send();
    }
    if (!run1Ended) {
      const n = (counts[e.type] = (counts[e.type] ?? 0) + 1);
      if (STRATEGY === `event:${e.type}:${n}`) send();
    }
  });
  const counts: Record<string, number> = {};
  onFinalStepStart = () => {
    if (STRATEGY === 'delay') setTimeout(send, DELAY_MS);
  };

  // TRACE=1: also log every event on the thread's pubsub topic as this process's reader receives it.
  const topic = `agent.thread-stream.${encodeURIComponent(`r${i}\0${threadId}`)}`;
  const trace = (ev: any, ack?: () => void) => {
    const d = ev.data ?? {};
    if (d.type !== 'stream-part' || ['finish', 'step-finish', 'start'].includes(d.part?.type))
      log(`  redis ${d.type}${d.part ? ':' + d.part.type : ''} run=${String(d.runId).slice(0, 8)}${d.signal ? ' signal=' + JSON.stringify(d.signal.contents ?? '').slice(0, 30) : ''}`);
    ack?.();
  };
  if (process.env.TRACE === '1') await pubsub.subscribe(topic, trace);
  session.sendSignal({ content: `first-${i}` });
  await waitFor(() => run1Ended && Boolean(sent), 5000);
  onFinalStepStart = undefined;
  if (!sent) return unsub(), { verdict: 'no-send', bug: false, controlOk: true, accepted: null, branch: 'none', order: events.join(' | ') };
  const accepted: any = await settle(sent.target.accepted);
  log(`accepted -> ${JSON.stringify(accepted)}`);
  const reached = () => prompts.some(p => p.includes(followUp));
  const isPersisted = async () => (await persisted(threadId)).some(m => m.includes(followUp));
  const candidate = accepted.action === 'deliver' && accepted.runId === sent.run1;
  // A candidate gets the full SETTLE_MS of quiet; anything else just needs the follow-up answered.
  if (candidate) await sleep(SETTLE_MS);
  else await waitFor(async () => reached() && (await isPersisted()) && !session.run.isRunning(), SETTLE_MS);
  const newRun = agentStarts > startsAtRun1End;
  const reachedModel = reached();
  const wasPersisted = await isPersisted();
  const bug = candidate && !newRun && !reachedModel && !wasPersisted;

  // Idle control on the same session: must wake and be answered and persisted.
  const idleBeforeControl = await waitFor(() => !session.run.isRunning() && !session.run.getRunId(), 3000);
  await sleep(1000); // let the last run's thread lease release in Redis, so the control is a genuinely idle send
  const control: any = await settle(session.sendSignal({ content: `control-${i}` }, { requireDelivery: true }).accepted);
  const answeredControl = async () => (await persisted(threadId)).some(m => m.startsWith('assistant:') && m.includes(`control-${i}`));
  await waitFor(answeredControl, 3000);
  const controlOk =
    control.action === 'wake' && (await persisted(threadId)).includes(`signal:control-${i}`) && (await answeredControl());
  const finalMsgs = await persisted(threadId);
  const controlInfo = `control accepted=${JSON.stringify(control)} idleBeforeControl=${idleBeforeControl} persisted=${JSON.stringify(finalMsgs)}`;
  unsub();
  if (process.env.TRACE === '1') await pubsub.unsubscribe(topic, trace);
  const verdict = bug
    ? 'BUG'
    : accepted.rejected
      ? 'rejected'
      : `${accepted.action}${accepted.runId === sent.run1 ? '(run1)' : '(new)'} newRun=${newRun} answered=${reachedModel && wasPersisted}`;
  // Did the lost follow-up turn up later (e.g. in the control's run)? Reported, not part of the oracle.
  const lateFollowUp = finalMsgs.some(m => m.includes(followUp));
  return { verdict: bug ? `BUG lateFollowUpAfterControl=${lateFollowUp}` : verdict, bug, controlOk, controlInfo, accepted, branch: sent.branch, order: events.join(' | ') };
}

console.log(`STRATEGY=${STRATEGY} DELAY_MS=${DELAY_MS} STEPS=${STEPS} STEP_MS=${STEP_MS} ITER=${ITER} pubsub=${agent.getPubSub()?.constructor.name}`);
const tally = new Map<string, number>();
let bugs = 0;
let controlFailures = 0;
let firstBugOrder: string | undefined;
let sampleOrder: string | undefined;
for (let i = 0; i < ITER; i++) {
  const o = await iteration(i);
  const k = `branch=${o.branch} ${o.verdict}`;
  tally.set(k, (tally.get(k) ?? 0) + 1);
  if (o.bug) {
    bugs++;
    firstBugOrder ??= o.order;
  }
  if (!o.controlOk) {
    controlFailures++;
    console.log(`#${i} CONTROL FAILED: ${o.controlInfo}\n  ${o.order.split(' | ').join('\n  ')}`);
  }
  if (o.branch === 'running') sampleOrder ??= o.order;
  if (VERBOSE) console.log(`#${i} ${k} control=${o.controlOk}`);
}
for (const [k, n] of tally) console.log(`  ${n}/${ITER}  ${k}`);
if (sampleOrder) console.log(`sample event order (stale-running branch):\n  ${sampleOrder.split(' | ').join('\n  ')}`);
if (firstBugOrder) console.log(`first BUG event order:\n  ${firstBugOrder.split(' | ').join('\n  ')}`);
await redisPubsub?.close().catch(() => {});
if (controlFailures) {
  console.log(`CONTROL FAILED in ${controlFailures}/${ITER} iterations`);
  process.exit(2);
}
if (bugs) {
  console.log(`BUG REPRODUCED ${bugs}/${ITER}: accepted deliver to ended run 1, no new run, never answered or persisted.`);
  process.exit(1);
}
console.log(`NOT REPRODUCED 0/${ITER}`);
process.exit(0);
