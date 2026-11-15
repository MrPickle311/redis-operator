#!/usr/bin/env bash
# Scaling curve of the RedisCluster store: ops/s against the number of shards.
#
# For every N in STEPS the script sets spec.shards, waits until the slots are balanced
# and the cluster is Ready (the time is the cost of that resharding), then runs PODS
# memtier Pods as fast as they can for DURATION s and prints one row:
#   shards  reshard-s  ops/s  p99-ms(max of Pods)  master-main-thread-busy(mean,max)  loader-cpu(max)
# "master main thread busy" near 100 % means Redis is the limit, far below it the load is.
#
# Needs: kubectl on the cluster, Prometheus on localhost:9090
#   kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090
# The cluster ends with spec.shards back at its first value.
#
# CPU_LIMIT=250m puts a CPU limit on the Redis container for the run (and takes it off
# after). Without a limit a master answers in microseconds and the clients (a closed
# loop, one request per client) are the limit, not Redis; with it the masters saturate
# and the curve shows how the cluster scales. See showcase.md.
#
#   STEPS="3 6 9 12" PODS=6 THREADS=3 CLIENTS=25 DURATION=90 CPU_LIMIT=250m hack/scaling-curve.sh
# Every row has a time (UTC); hack/prom-window.py START END shows node, network, disk and
# Redis metrics of that window.
set -euo pipefail

NS=${NS:-redis-play}
STEPS=${STEPS:-"3 6 9 12"}
PODS=${PODS:-4}
THREADS=${THREADS:-3}
CLIENTS=${CLIENTS:-25}
DURATION=${DURATION:-90}
CPU_LIMIT=${CPU_LIMIT:-}
RATIO=${RATIO:-1:4}       # sets:gets; 0:1 only reads, 1:0 only writes
PIPELINE=${PIPELINE:-1}   # requests a client keeps in flight
KEYS=${KEYS:-100000}
SIZE=${SIZE:-256}
PROM=${PROM:-http://localhost:9090}
IMAGE=${IMAGE:-registry.homelab/library/memtier_benchmark:2.1.4}
RC=redisclusters.redis.operator.com/store
# Load goes to the nodes with room, away from most of the Redis Pods.
LOAD_NODES=${LOAD_NODES:-talos-u0c-uk3,talos-sie-ons}

log() { echo "$(date -u +%T) $*" >&2; }
field() { kubectl -n "$NS" get $RC -o jsonpath="$1"; }
reason() { field "{.status.conditions[?(@.type==\"$1\")].reason}"; }
prom() { curl -s "$PROM/api/v1/query" --data-urlencode "query=$1" | python3 -c \
  'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "nan")'; }

# Until the slots are where spec.shards wants them and the cluster is Ready.
wait_ready() {
  sleep 5
  until want=$(field '{.spec.shards}'); [ "$(reason Migrating)" = Balanced ] && [ -z "$(field '{.status.slotMigration}')" ] &&
        [ "$(field '{.status.shards[*].name}' | wc -w)" = "$want" ] &&
        [ "$(field '{.status.shards[*].slots}' | wc -w)" = "$want" ] && [ "$(reason Ready)" = Ready ] &&
        [ "$(field '{.status.conditions[?(@.type=="Ready")].observedGeneration}')" = "$(field '{.metadata.generation}')" ]; do
    sleep 5
  done
}

# job NAME SECONDS PARALLELISM THREADS CLIENTS EXTRA-memtier-args
job() {
  kubectl -n "$NS" delete job "$1" --ignore-not-found --wait=true >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: $1, namespace: $NS, labels: {app: store-scale}}
spec:
  parallelism: $3
  completions: $3
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata: {labels: {app: store-scale}}
    spec:
      restartPolicy: Never
      securityContext: {runAsUser: 65534, runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions: [{key: kubernetes.io/hostname, operator: In, values: [${LOAD_NODES//,/, }]}]
      containers:
        - name: load
          image: $IMAGE
          command: [sh, -c]
          # memtier takes the password only as an argument; it stays inside this Pod.
          args:
            - |
              memtier_benchmark -s store.$NS.svc -p 6379 --cluster-mode --tls --cacert=/ca/ca.crt -a "\$username:\$password" --test-time=$2 -t $4 -c $5 --key-maximum=$KEYS -d $SIZE --key-prefix=scale: --hide-histogram $6 2>&1 | grep -E '^(Totals|ERROR|Error)' | tail -3
          envFrom: [{secretRef: {name: load-redis-user}}]
          resources: {requests: {cpu: "$4", memory: 128Mi}, limits: {memory: 1Gi}}
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts: [{name: ca, mountPath: /ca, readOnly: true}]
      volumes:
        - {name: ca, secret: {secretName: store-ca, items: [{key: ca.crt, path: ca.crt}]}}
EOF
}

# Totals line of every Pod of the job: ops/s p99.
totals() {
  kubectl -n "$NS" wait --for=condition=complete "job/$1" --timeout=$(( $2 + 180 ))s >/dev/null
  for pod in $(kubectl -n "$NS" get pods -l job-name="$1" -o name); do
    kubectl -n "$NS" logs "$pod" | grep '^Totals' | awk '{print $2, $9}'
  done
}

orig_cpu_limit=$(field '{.spec.resources.limits.cpu}')
restore_cpu_limit() {
  [ -n "$CPU_LIMIT" ] || return 0
  log "limits.cpu back to '${orig_cpu_limit:-none}'"
  if [ -n "$orig_cpu_limit" ]; then patch="{\"spec\":{\"resources\":{\"limits\":{\"cpu\":\"$orig_cpu_limit\"}}}}"
  else patch='{"spec":{"resources":{"limits":{"cpu":null}}}}'; fi
  kubectl -n "$NS" patch $RC --type merge -p "$patch" >/dev/null
  wait_ready
}
if [ -n "$CPU_LIMIT" ]; then
  trap restore_cpu_limit EXIT
  log "limits.cpu = $CPU_LIMIT"
  kubectl -n "$NS" patch $RC --type merge -p "{\"spec\":{\"resources\":{\"limits\":{\"cpu\":\"$CPU_LIMIT\"}}}}" >/dev/null
  wait_ready
fi

start_shards=$(field '{.spec.shards}')
log "start: $start_shards shards; load $PODS Pods x $THREADS threads x $CLIENTS clients, ${DURATION}s per step"

if [ -z "$(prom 'count(redis_up{namespace="'"$NS"'"})' | grep -v nan)" ]; then echo "Prometheus not reachable at $PROM" >&2; exit 1; fi

log "prefill: $KEYS keys of $SIZE bytes"
job scale-prefill 600 1 2 5 "--ratio=1:0 --key-pattern=P:P -n allkeys"
kubectl -n "$NS" wait --for=condition=complete job/scale-prefill --timeout=900s >/dev/null

printf '%-7s %-10s %-9s %-9s %-14s %-18s %s\n' shards reshard-s ops/s p99-ms master-busy loader-cpu 'load window (UTC)'
for n in $STEPS; do
  took=0
  if [ "$(field '{.spec.shards}')" != "$n" ]; then
    t0=$(date +%s)
    kubectl -n "$NS" patch $RC --type merge -p "{\"spec\":{\"shards\":$n}}" >/dev/null
    log "spec.shards = $n"
    wait_ready
    took=$(( $(date +%s) - t0 ))
    log "ready after ${took}s"
  fi
  sleep 20 # connections, caches and the exporter settle

  win_start=$(date -u +%T)
  job scale-load "$DURATION" "$PODS" "$THREADS" "$CLIENTS" "--ratio=$RATIO --pipeline=$PIPELINE --key-pattern=R:R"
  sleep $(( DURATION * 8 / 10 + 12 ))
  busy=$(prom 'avg(sum by (pod) (rate(redis_cpu_user_main_thread_seconds_total{namespace="'"$NS"'",redis_instance=~"store-.*",role="primary"}[40s]) + rate(redis_cpu_sys_main_thread_seconds_total{namespace="'"$NS"'",redis_instance=~"store-.*",role="primary"}[40s])))')
  busymax=$(prom 'max(sum by (pod) (rate(redis_cpu_user_main_thread_seconds_total{namespace="'"$NS"'",redis_instance=~"store-.*",role="primary"}[40s]) + rate(redis_cpu_sys_main_thread_seconds_total{namespace="'"$NS"'",redis_instance=~"store-.*",role="primary"}[40s])))')
  loadcpu=$(prom 'max(rate(container_cpu_usage_seconds_total{namespace="'"$NS"'",pod=~"scale-load-.*",container="load"}[75s]))')

  rows=$(totals scale-load "$DURATION")
  ops=$(echo "$rows" | awk '{s+=$1} END {printf "%.0f", s}')
  p99=$(echo "$rows" | awk 'BEGIN{m=0} {if ($2>m) m=$2} END {print m}')
  printf '%-7s %-10s %-9s %-9s %-14s %-18s %s\n' "$n" "$took" "$ops" "$p99" \
    "$(python3 -c "print('%.0f%% / %.0f%%' % (100*float('$busy'), 100*float('$busymax')))")" \
    "$(python3 -c "print('%.1f of $THREADS cores' % float('$loadcpu'))")" \
    "$win_start-$(date -u +%T)"
done

kubectl -n "$NS" delete job scale-load scale-prefill --ignore-not-found >/dev/null
log "deleting the keys scale:*"
for pod in $(kubectl -n "$NS" get redisinstances.redis.operator.com -o jsonpath='{range .items[*]}{.status.currentPrimary}{"\n"}{end}' | grep '^store-'); do
  kubectl -n "$NS" exec "$pod" -c redis -- sh -c 'export REDISCLI_AUTH=$(cat /internal-auth/operator-password)
    cli() { redis-cli --no-auth-warning -s /controller/redis.sock --user _operator "$@"; }
    cli --scan --pattern "scale:*" --count 1000 | sed "s/^/UNLINK /" | cli --pipe >/dev/null' || true
done

if [ "$(field '{.spec.shards}')" != "$start_shards" ]; then
  log "back to $start_shards shards"
  kubectl -n "$NS" patch $RC --type merge -p "{\"spec\":{\"shards\":$start_shards}}" >/dev/null
  wait_ready
fi
log done
