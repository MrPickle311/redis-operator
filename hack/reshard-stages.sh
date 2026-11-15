#!/usr/bin/env bash
# Stages of docs/experiments/resharding-cost (see the plan): every run is one
# hack/reshard-cost.py move, there and back, with a check before each.
#
#   hack/reshard-stages.sh 1        # stage 1: no data, V0: P0 x4, P5 3->6 x3, P5 3->9 x3
#   hack/reshard-stages.sh 2        # stage 2: no data, V1 and V2 against V0
#   hack/reshard-stages.sh 3        # stage 3: data, layout L: key size, P1 1 GB, P2 5 GB, P3 8 GB, P6 3->6 with P3
#   hack/reshard-stages.sh 4a       # stage 4, first half: layout N, P3 x2, P6 x1 (data of P3 loaded)
#   hack/reshard-stages.sh 5        # stage 5: P3 (8 GB, 1 KB) layout L: V3 and V4 (batch 10 / 1000) against
#                                   # V0, then redis-cli rebalance with pipeline 100 and 10, operator stopped
#   hack/reshard-stages.sh 6        # extra on P3 (loaded): V1 with data 2x, then redis-cli r1 (R100, R10; operator stopped)
#   hack/reshard-stages.sh 4b       # stage 4, second half: P6-N r2, then P4 (same bytes, 10 KB values) N and L
#
# Points are interleaved (P0, P5-6, P5-9, P0, ...) so a drift of the cluster over
# time spreads over all of them. A failed check stops the stage: the cluster is not
# in a state to measure (look at it, then run the stage again from REP_FROM).
# Needs Prometheus on localhost:9090 and Loki on localhost:3100 (port-forwards).
set -euo pipefail
cd "$(dirname "$0")/.."
# One stage at a time: two runners would patch spec.shards and measure the same resharding twice.
exec 9>"${TMPDIR:-/tmp}/reshard-stages.lock"
flock -n 9 || { echo "another hack/reshard-stages.sh runs: stopping" >&2; exit 1; }
PAUSE=${PAUSE:-60}
REP_FROM=${REP_FROM:-1}

ready() {
  for _ in $(seq 1 30); do
    hack/reshard-cost.py check >"${TMPDIR:-/tmp}/reshard-check.$$" 2>&1 && return 0
    sleep 10
  done
  cat "${TMPDIR:-/tmp}/reshard-check.$$"
  echo "check failed for 5 min: stopping" >&2
  exit 1
}

# run STAGE POINT VARIANT REP TO: there (3 -> TO) and back (TO -> 3)
run() {
  ready; hack/reshard-cost.py move "$5" "$1" "$2" "$3" "$4"; sleep "$PAUSE"
  ready; hack/reshard-cost.py move 3 "$1" "$2" "$3" "$4"; sleep "$PAUSE"
}

# run with the layout of the masters set before each direction: there, then (with the new
# shards' masters placed too) back
run_layout() { # STAGE POINT VARIANT REP TO LAYOUT
  ready; hack/reshard-cost.py layout "$6"; ready
  hack/reshard-cost.py move "$5" "$1" "$2" "$3" "$4"; sleep "$PAUSE"
  ready; hack/reshard-cost.py layout "$6"; ready
  hack/reshard-cost.py move 3 "$1" "$2" "$3" "$4"; sleep "$PAUSE"
}

# keys for GB of data, from the bytes per key measured with prefill (KEY_BYTES)
keys_for() { python3 -c "print(int($1 * 1e9 / $KEY_BYTES))"; }

# variant V0|V1|V2|V3|V4: the knobs of the operator (restarts only the operator)
#   V3: 10 keys per MIGRATE (as redis-cli), V4: 1000 keys per MIGRATE (more in flight per call)
variant() {
  local d=(-n redis-operator-system deploy/redis-operator-controller-manager)
  case "$1" in
    V0) kubectl set env "${d[@]}" RESHARD_REFRESH- MIGRATE_BATCH- ;;
    V1) kubectl set env "${d[@]}" RESHARD_REFRESH=pair MIGRATE_BATCH- ;;
    V2) kubectl set env "${d[@]}" RESHARD_REFRESH=parallel MIGRATE_BATCH- ;;
    V3) kubectl set env "${d[@]}" RESHARD_REFRESH- MIGRATE_BATCH=10 ;;
    V4) kubectl set env "${d[@]}" RESHARD_REFRESH- MIGRATE_BATCH=1000 ;;
  esac
  kubectl rollout status "${d[@]}" --timeout=180s
  echo "variant $1: $(kubectl get "${d[@]}" -o jsonpath='{.spec.template.spec.containers[0].env[*].name}')"
  sleep 20 # the new operator takes the lease and reconciles once
}

case "${1:-}" in
  2)
    # V1 and V2 against V0 of stage 1; one V0 P0 per repetition checks the cluster did not drift
    trap 'variant V0' EXIT
    for rep in $(seq "$REP_FROM" 2); do
      variant V0; run 2 P0 V0 "$rep" 4
      for to in 4 6 9; do
        point=P0; [ "$to" != 4 ] && point=P5
        variant V1; run 2 "$point" V1 "$rep" "$to"
        variant V2; run 2 "$point" V2 "$rep" "$to"
      done
    done
    ;;
  3)
    variant V0
    ready
    hack/reshard-cost.py flush
    hack/reshard-cost.py prefill 100000 1000 | tee "${TMPDIR:-/tmp}/keysize.$$"
    KEY_BYTES=$(grep -o '[0-9]* bytes per key' "${TMPDIR:-/tmp}/keysize.$$" | cut -d' ' -f1)
    echo "key size: $KEY_BYTES bytes per key of 1000 B"
    hack/reshard-cost.py flush
    for gb in 1 5 8; do
      point=P1; [ "$gb" = 5 ] && point=P2; [ "$gb" = 8 ] && point=P3
      ready; hack/reshard-cost.py prefill "$(keys_for "$gb")" 1000
      for rep in $(seq "$REP_FROM" 2); do run_layout 3 "$point-L" V0 "$rep" 4 L; done
    done
    for rep in $(seq "$REP_FROM" 2); do run_layout 3 P6-L V0 "$rep" 6 L; done
    ;;
  5)
    OP=(-n redis-operator-system deploy/redis-operator-controller-manager)
    trap 'kubectl scale "${OP[@]}" --replicas=1; variant V0' EXIT
    variant V0
    if [ -z "${SKIP_PREP:-}" ]; then # SKIP_PREP=1 REP_FROM=2: go on after a stop, data of P3 loaded, 3 shards
      ready; hack/reshard-cost.py flush
      KEY_BYTES=1147
      ready; hack/reshard-cost.py prefill "$(keys_for 8)" 1000
      run_layout 5 P3-L V0 1 4 L
    fi
    for rep in $(seq "$REP_FROM" 2); do
      variant V3; run_layout 5 P3-L V3 "$rep" 4 L
      variant V4; run_layout 5 P3-L V4 "$rep" 4 L
    done
    variant V0
    # redis-cli needs 4 balanced masters to drain one: the operator makes them (a V0 control 3->4)
    ready; hack/reshard-cost.py layout L; ready
    hack/reshard-cost.py move 4 5 P3-L V0 2; sleep "$PAUSE"
    ready
    kubectl scale "${OP[@]}" --replicas=0
    kubectl -n redis-operator-system wait --for=delete pod -l control-plane=controller-manager --timeout=120s || true
    for rep in $(seq "$REP_FROM" 2); do
      for pipe in 100 10; do
        hack/reshard-cost.py rcli drain "$pipe" 5 P3-L "R$pipe" "$rep"; sleep "$PAUSE"
        hack/reshard-cost.py rcli fill "$pipe" 5 P3-L "R$pipe" "$rep"; sleep "$PAUSE"
      done
    done
    kubectl scale "${OP[@]}" --replicas=1
    kubectl rollout status "${OP[@]}" --timeout=180s
    sleep 30 # the operator evens out what redis-cli left within its 2 % threshold
    ready; hack/reshard-cost.py move 3 5 P3-L V0 2
    ;;
  6)
    OP=(-n redis-operator-system deploy/redis-operator-controller-manager)
    trap 'kubectl scale "${OP[@]}" --replicas=1; variant V0' EXIT
    for rep in 1 2; do variant V1; run_layout 6 P3-L V1 "$rep" 4 L; done
    variant V0
    ready; hack/reshard-cost.py layout L; ready
    hack/reshard-cost.py move 4 6 P3-L V0 1; sleep "$PAUSE"
    ready
    kubectl scale "${OP[@]}" --replicas=0
    kubectl -n redis-operator-system wait --for=delete pod -l control-plane=controller-manager --timeout=120s || true
    for pipe in 100 10; do
      hack/reshard-cost.py rcli drain "$pipe" 5 P3-L "R$pipe" 1; sleep "$PAUSE"
      hack/reshard-cost.py rcli fill "$pipe" 5 P3-L "R$pipe" 1; sleep "$PAUSE"
    done
    kubectl scale "${OP[@]}" --replicas=1
    kubectl rollout status "${OP[@]}" --timeout=180s
    sleep 30
    ready; hack/reshard-cost.py move 3 6 P3-L V0 1
    ;;
  4a)
    variant V0
    for rep in $(seq "$REP_FROM" 2); do run_layout 4 P3-N V0 "$rep" 4 N; done
    run_layout 4 P6-N V0 1 6 N
    ;;
  4b)
    variant V0
    run_layout 4 P6-N V0 2 6 N
    ready; hack/reshard-cost.py flush
    # P4: about the bytes of P3 (8 GB) in values of 10000 B: 10x fewer keys
    hack/reshard-cost.py prefill 100000 10000 | tee "${TMPDIR:-/tmp}/keysize.$$"
    KEY_BYTES=$(grep -o '[0-9]* bytes per key' "${TMPDIR:-/tmp}/keysize.$$" | cut -d' ' -f1)
    echo "key size: $KEY_BYTES bytes per key of 10000 B"
    hack/reshard-cost.py flush
    ready; hack/reshard-cost.py prefill "$(keys_for 8)" 10000
    for rep in $(seq "$REP_FROM" 2); do run_layout 4 P4-N V0 "$rep" 4 N; done
    for rep in $(seq "$REP_FROM" 2); do run_layout 4 P4-L V0 "$rep" 4 L; done
    ;;
  1)
    for rep in $(seq "$REP_FROM" 4); do
      run 1 P0 V0 "$rep" 4
      if [ "$rep" -le 3 ]; then
        run 1 P5 V0 "$rep" 6
        run 1 P5 V0 "$rep" 9
      fi
    done
    ;;
  *) sed -n '2,12p' "$0"; exit 2 ;;
esac
echo "stage $1 done"
