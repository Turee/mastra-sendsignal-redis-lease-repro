#!/bin/sh
# Runs every strategy ITER times (sequentially, fresh process each) and writes results/<name>.log
cd "$(dirname "$0")"
ITER=${ITER:-30}
run() { name=$1; shift; env ITER=$ITER "$@" npx tsx repro.ts > results/$name.log 2>&1; echo "$name exit=$?"; grep -E '^  [0-9]+/' results/$name.log; }
run a-agent_end STRATEGY=agent_end
run b-usage_update_2 STRATEGY=event:usage_update:2
run b-message_end_final STRATEGY=event:message_end:2
run b-agent_start_1 STRATEGY=event:agent_start:1
for d in 0 2 4 6 8 10 12 15 18 20 25; do run c-delay-$d STRATEGY=delay DELAY_MS=$d; done
run d-steps1-usage_update_1 STRATEGY=event:usage_update:1 STEPS=1
