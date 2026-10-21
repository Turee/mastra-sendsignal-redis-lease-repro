// Diagnostic only (TRACE=1): wrap the internal AgentThreadStreamRuntime.sendSignal of @mastra/core 1.78.0 to log
// what the runtime sees when the follow-up reaches it. Not used by the repro's verdict.
// @ts-ignore internal chunk of the published dist (same module instance the package uses)
import { _ as runtime } from './node_modules/@mastra/core/dist/storage-7mYy2C-B.js';
export function installProbe(log: (s: string) => void) {
  const orig = runtime.sendSignal.bind(runtime);
  runtime.sendSignal = (agent: any, signal: any, target: any, pubsub: any) => {
    const active = runtime.getActiveThreadRunId(target, pubsub);
    const r = orig(agent, signal, target, pubsub);
    const text = JSON.stringify(signal.contents ?? '').slice(0, 24);
    log(`  runtime.sendSignal ${text} ifIdle.streamOptions=${Boolean(target.ifIdle?.streamOptions)} getActiveThreadRunId=${String(active).slice(0, 8)} hasThreadRun=${active ? runtime.hasThreadRun(active, pubsub) : '-'}`);
    r.accepted.then((a: any) => log(`  runtime.sendSignal ${text} -> ${a.action} ${String(a.runId).slice(0, 8)}`), () => {});
    return r;
  };
}
