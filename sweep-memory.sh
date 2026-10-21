#!/bin/sh
# Same strategies with Mastra's default in-process pubsub (PUBSUB=memory), for comparison.
cd "$(dirname "$0")"
ITER=${ITER:-30}
run() { name=$1; shift; env ITER=$ITER PUBSUB=memory "$@" npx tsx repro.ts > results/$name.log 2>&1; echo "$name exit=$?"; grep -E '^  [0-9]+/' results/$name.log; }
run m-agent_end STRATEGY=agent_end
run m-usage_update_2 STRATEGY=event:usage_update:2
for d in 0 2 4 6 8 10 12 15 18; do run m-delay-$d STRATEGY=delay DELAY_MS=$d; done
