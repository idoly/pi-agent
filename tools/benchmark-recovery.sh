#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

iterations=${PI_BENCH_ITERATIONS:-5}
output=${PI_BENCH_OUTPUT:-target/recovery-benchmark.txt}
report=core/target/surefire-reports/io.github.idoly.pi.agent.session.JsonlRecoveryCheckpointTest.txt
mkdir -p "$(dirname "$output")"

run_once() {
  mvn --batch-mode --no-transfer-progress \
    -pl core -am \
    -Dtest=JsonlRecoveryCheckpointTest#boundedVerifierCoversManyGenerationsAcrossScanAndDetailPages \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -DforkCount=1 \
    -DreuseForks=false \
    test >/dev/null
  awk 'END {
    if (match($0, /Time elapsed: [0-9.]+/)) {
      print substr($0, RSTART + 14, RLENGTH - 14)
    } else {
      exit 1
    }
  }' "$report"
}

run_once >/dev/null
{
  printf 'java=%s\n' "$(java -version 2>&1 | head -1)"
  printf 'os=%s\n' "$(uname -a)"
  printf 'scenario=128 generations, 8 changed, scan batch 17, detail page 3\n'
  printf 'iterations=%s\n' "$iterations"
  for ((iteration = 1; iteration <= iterations; iteration++)); do
    printf 'iteration_%s_seconds=%s\n' "$iteration" "$(run_once)"
  done
} | tee "$output"
