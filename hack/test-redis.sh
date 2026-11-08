#!/usr/bin/env bash
# Runs the tests of the Instance Manager that need a real Redis (*OnRedis),
# which go test skips without their REDIS_TEST_* variables: one plain Redis
# with a data directory, and two pairs of cluster nodes on one network.
set -euo pipefail

IMAGE=${REDIS_IMAGE:-redis:7.2}
NET=redis-test-$$
DATA=$(mktemp -d)
names=(plain rc-a rc-b mig-a mig-b)

cleanup() {
	docker rm -f "${names[@]/#/$NET-}" >/dev/null 2>&1 || true
	docker network rm "$NET" >/dev/null 2>&1 || true
	rm -rf "$DATA"
}
trap cleanup EXIT

docker network create "$NET" >/dev/null
docker run -d --name "$NET-plain" --network "$NET" -p 127.0.0.1:0:6379 --user "$(id -u)" -v "$DATA:/data" "$IMAGE" >/dev/null
for n in rc-a rc-b mig-a mig-b; do
	docker run -d --name "$NET-$n" --network "$NET" -p 127.0.0.1:0:6379 "$IMAGE" --cluster-enabled yes >/dev/null
done

addr() { docker port "$NET-$1" 6379/tcp | head -1; }
ip() { docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$NET-$1"; }
for n in "${names[@]}"; do
	until docker exec "$NET-$n" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done
done

REDIS_TEST_ADDR=$(addr plain) \
REDIS_TEST_DATA=$DATA \
REDIS_TEST_CLUSTER_ADDRS=$(addr rc-a),$(addr rc-b) \
REDIS_TEST_CLUSTER_HOSTS=$(ip rc-a),$(ip rc-b) \
REDIS_TEST_MIGRATE_ADDRS=$(addr mig-a),$(addr mig-b) \
REDIS_TEST_MIGRATE_HOSTS=$(ip mig-a),$(ip mig-b) \
	go test ./internal/instancemanager -run OnRedis -count=1 -v "$@" | grep -E '^(=== RUN|--- |ok|FAIL|PASS)|SKIP'
