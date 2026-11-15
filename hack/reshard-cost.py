#!/usr/bin/env python3
"""Measurements of docs/experiments/resharding-cost.md on the RedisCluster bulk.

  hack/reshard-cost.py check                 # before a measurement: Ready, Balanced, no evictions, replicas in sync
  hack/reshard-cost.py prefill KEYS SIZE     # memtier writes keys 1..KEYS of SIZE bytes; prints bytes per key
  hack/reshard-cost.py flush                 # FLUSHALL on the masters
  hack/reshard-cost.py rcli drain|fill PIPELINE STAGE POINT VARIANT REP
                                             # redis-cli --cluster rebalance with the operator stopped:
                                             # drain = shard 3 to weight 0 (4->3), fill = --cluster-use-empty-masters (3->4)
  hack/reshard-cost.py layout L|N            # masters where the layout wants them (switchover, a shard at a time)
                                             # L: every master on BIG_NODE; N: shards 0-2 off BIG_NODE, others on it
  hack/reshard-cost.py move N STAGE POINT VARIANT REP   # spec.shards = N, wait until Balanced, save one row
                                             # e.g. move 4 1 P0 V0 2; POINT with the layout: P3-L, P3-N

move takes the time of every slot from the "Moved slot" lines in Loki, so it needs
  kubectl -n monitoring port-forward svc/loki 3100:3100
Every run has an id, S<stage>-<point>-<variant>-r<rep>-<from>to<to>, e.g. S1-P0-V0-r2-3to4. Its row goes
to docs/experiments/resharding-cost/results.tsv and, with a Grafana link to its window, results.md;
two marker lines (START, END) go to Loki {job="reshard-cost"}: the dashboards show them as
annotations and list the runs in the table "Experiment runs".
Times in the row:
  total  = from the patch to Migrating=Balanced (lastTransitionTime, 1 s resolution)
  wait   = from the patch to the first moved slot: new Pods, volumes, MEET (not resharding)
  move   = from the first to the last moved slot
  c_slot = median gap between two moved slots; pauses = gaps over 500 ms (end of a reconcile)
"""
import datetime, json, os, statistics, subprocess, sys, time, urllib.parse, urllib.request

NS, NAME = os.environ.get("NS", "redis-play"), os.environ.get("CLUSTER", "bulk")
LOKI = os.environ.get("LOKI", "http://localhost:3100")
IMAGE = os.environ.get("IMAGE", "registry.homelab/library/memtier_benchmark:2.1.4")
RC = f"redisclusters.redis.operator.com/{NAME}"
OUT = os.path.join(os.path.dirname(__file__), "..", "docs/experiments/resharding-cost/results.tsv")
GRAFANA = os.environ.get("GRAFANA", "http://localhost:3000")
BIG_NODE = os.environ.get("BIG_NODE", "talos-u0c-uk3")


def marker(at, fields):
    """one logfmt line in Loki {job="reshard-cost"} at the time at (s)"""
    line = " ".join(f'{k}="{v}"' if " " in str(v) else f"{k}={v}" for k, v in fields.items())
    body = {"streams": [{"stream": {"job": "reshard-cost", "cluster": NAME, "namespace": NS},
                         "values": [[str(int(at * 1e9)), line]]}]}
    req = urllib.request.Request(f"{LOKI}/loki/api/v1/push", json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    urllib.request.urlopen(req)


def grafana_link(t0, t1):
    return (f"{GRAFANA}/d/redis-operator-cluster?var-namespace={NS}&var-cluster={NAME}"
            f"&from={int((t0 - 60) * 1000)}&to={int((t1 + 60) * 1000)}")


def sh(*args, input=None):
    """A command; reads (get) are tried again on a passing error of the API server, five times."""
    tries = 5 if len(args) > 3 and args[0] == "kubectl" and "get" in args[:5] else 1
    for i in range(tries):
        try:
            return subprocess.run(args, input=input, capture_output=True, text=True, check=True).stdout
        except subprocess.CalledProcessError as e:
            if i == tries - 1:
                raise
            print(f"retrying after: {' '.join(args[:6])}: {e.stderr.strip()[:200]}", file=sys.stderr, flush=True)
            time.sleep(3)


def cluster():
    return json.loads(sh("kubectl", "-n", NS, "get", RC, "-o", "json"))


def condition(c, kind):
    return next((x for x in c["status"].get("conditions", []) if x["type"] == kind), {})


def pods():
    return sh("kubectl", "-n", NS, "get", "pods", "-l", f"redis.operator.com/cluster={NAME}",
              "-o", "jsonpath={.items[*].metadata.name}").split()


def cli(pod, *cmd):
    """redis-cli as _operator on the socket; the password stays in the Pod."""
    script = ('export REDISCLI_AUTH=$(cat /internal-auth/operator-password); '
              'redis-cli --no-auth-warning -s /controller/redis.sock --user _operator "$@"')
    return sh("kubectl", "-n", NS, "exec", pod, "-c", "redis", "--", "sh", "-c", script, "cli", *cmd)


def info(pod, section):
    return dict(line.split(":", 1) for line in cli(pod, "info", section).replace("\r", "").splitlines()
                if ":" in line)


def masters():
    return [p for p in pods() if info(p, "replication").get("role") == "master"]


def layout():
    """{pod: (node, role)} of every Pod of the cluster; role from Redis itself"""
    out = {}
    for item in json.loads(sh("kubectl", "-n", NS, "get", "pods", "-l", f"redis.operator.com/cluster={NAME}",
                              "-o", "json"))["items"]:
        pod = item["metadata"]["name"]
        try:
            role = info(pod, "replication").get("role", "?")
        except subprocess.CalledProcessError:
            role = "down"
        out[pod] = (item["spec"].get("nodeName", "?"), role)
    return out


def show_layout(title, lay):
    print(f"{title}:")
    for pod, (node, role) in sorted(lay.items()):
        print(f"  {pod:<12} {role:<7} {node}")
    per_node = {}
    for pod, (node, role) in lay.items():
        if role == "master":
            per_node.setdefault(node, []).append(pod)
    print("  masters per node: " + ", ".join(f"{n}={len(p)}" for n, p in sorted(per_node.items())))


def masters_short(lay):
    """masters as pod@node, for the row"""
    return " ".join(f"{p}@{n.removeprefix('talos-')}" for p, (n, r) in sorted(lay.items()) if r == "master")


def data():
    """keys and used memory of all masters"""
    keys = mem = 0
    for p in masters():
        keys += int(cli(p, "dbsize"))
        mem += int(info(p, "memory")["used_memory"])
    return keys, mem


def check():
    c = cluster()
    ok = True
    for kind, want in [("Ready", "True"), ("Migrating", "False")]:
        cond = condition(c, kind)
        good = cond.get("status") == want and cond.get("observedGeneration") == c["metadata"]["generation"]
        print(f"{kind}: {cond.get('status')}/{cond.get('reason')} {'ok' if good else 'NOT OK'}")
        ok &= good
    ips = dict(line.split() for line in sh("kubectl", "-n", NS, "get", "pods", "-l", f"redis.operator.com/cluster={NAME}",
                                            "-o", "jsonpath={range .items[*]}{.metadata.name} {.status.podIP}{\"\\n\"}{end}").splitlines())
    for p in pods():
        r, s = info(p, "replication"), info(p, "stats")
        me = next(l.split()[1].split(":")[0] for l in cli(p, "cluster", "nodes").splitlines() if "myself" in l)
        line = f"{p}: {r['role']} evicted={s['evicted_keys']}"
        if me != ips.get(p):
            line += f" STALE own IP {me} (Pod {ips.get(p)}): CLUSTER MEET {ips.get(p)} 6379 from another node"
            ok = False
        if r["role"] == "slave":
            line += f" link={r['master_link_status']} sync={r['master_sync_in_progress']}"
            ok &= r["master_link_status"] == "up" and r["master_sync_in_progress"] == "0"
        ok &= s["evicted_keys"] == "0"
        print(line)
    keys, mem = data()
    print(f"masters: {keys} keys, {mem / 2**30:.2f} GiB used")
    print("ready to measure" if ok else "NOT ready")
    return ok


def prefill(keys, size):
    before = data()
    job = "bulk-prefill"
    sh("kubectl", "-n", NS, "delete", "job", job, "--ignore-not-found", "--wait=true")
    manifest = {
        "apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": job, "namespace": NS},
        "spec": {"backoffLimit": 0, "ttlSecondsAfterFinished": 3600, "template": {"spec": {
            "restartPolicy": "Never",
            "securityContext": {"runAsUser": 65534, "runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [{
                "name": "load", "image": IMAGE, "command": ["sh", "-c"],
                # memtier takes the password only as an argument; it stays inside this Pod.
                "args": [f'memtier_benchmark -s {NAME}.{NS}.svc -p 6379 --cluster-mode --tls --cacert=/ca/ca.crt '
                         f'-a "$username:$password" --ratio=1:0 --key-pattern=P:P --key-minimum=1 '
                         f'--key-maximum={keys} -n allkeys -d {size} -t 4 -c 8 --hide-histogram 2>&1 | tail -12'],
                "envFrom": [{"secretRef": {"name": f"{NAME}-load-redis-user"}}],
                "resources": {"requests": {"cpu": "4", "memory": "256Mi"}, "limits": {"memory": "1Gi"}},
                "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                "volumeMounts": [{"name": "ca", "mountPath": "/ca", "readOnly": True}]}],
            "volumes": [{"name": "ca", "secret": {"secretName": f"{NAME}-ca", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}],
        }}}}
    sh("kubectl", "apply", "-f", "-", input=json.dumps(manifest))
    sh("kubectl", "-n", NS, "wait", "--for=condition=complete", f"job/{job}", "--timeout=3h")
    print(sh("kubectl", "-n", NS, "logs", f"job/{job}"))
    after = data()
    added = after[0] - before[0]
    print(f"masters: {after[0]} keys (+{added}), {after[1] / 2**30:.2f} GiB used "
          f"(+{(after[1] - before[1]) / 2**30:.2f}); {(after[1] - before[1]) / max(added, 1):.0f} bytes per key")


def flush():
    for p in masters():
        print(p, cli(p, "flushall").strip())


def set_layout(mode):
    """Switch the master of every shard over to the Pod the layout wants, one shard at a time:
    L puts every master on BIG_NODE (MIGRATE stays in the machine), N the masters of shards 0-2
    on other nodes and the rest on BIG_NODE (MIGRATE crosses the network both ways)."""
    nodes = {p: n for p, (n, _) in layout().items()}
    for inst in sorted({p.rsplit("-", 1)[0] for p in nodes}, key=lambda i: int(i.rsplit("-", 1)[1])):
        shard = int(inst.rsplit("-", 1)[1])
        on_big = mode == "L" or shard >= 3
        want = [p for p in sorted(nodes) if p.rsplit("-", 1)[0] == inst and (nodes[p] == BIG_NODE) == on_big]
        ri = json.loads(sh("kubectl", "-n", NS, "get", "redisinstance", inst, "-o", "json"))
        primary = ri["status"].get("currentPrimary")
        if not want:
            print(f"{inst}: no Pod {'on' if on_big else 'off'} {BIG_NODE}, master stays {primary}@{nodes.get(primary)}")
            continue
        if primary in want:
            print(f"{inst}: master {primary}@{nodes[primary]} ok")
            continue
        print(f"{inst}: switchover {primary} -> {want[0]}@{nodes[want[0]]}", flush=True)
        sh("kubectl", "-n", NS, "annotate", "--overwrite", "redisinstance", inst, f"redis.operator.com/promote={want[0]}")
        for _ in range(60):
            time.sleep(5)
            st = json.loads(sh("kubectl", "-n", NS, "get", "redisinstance", inst, "-o", "json"))["status"]
            if st.get("currentPrimary") == want[0] and not st.get("targetPrimary") and \
                    info(want[0], "replication").get("role") == "master":
                break
        else:
            sys.exit(f"{inst}: switchover to {want[0]} did not finish in 5 min")
    show_layout(f"Layout {mode}", layout())


def slot_counts(pod):
    """{node id: slots it serves} of the masters, as pod sees the cluster"""
    out = {}
    for line in cli(pod, "cluster", "nodes").splitlines():
        f = line.split()
        if "master" in f[2] and "fail" not in f[2]:
            n = 0
            for r in f[8:]:
                if r.startswith("["):
                    continue
                a, _, b = r.partition("-")
                n += int(b or a) - int(a) + 1
            out[f[0]] = n
    return out


def rcli(direction, pipeline, stage, point, variant, rep):
    """One rebalance by redis-cli instead of the operator (stopped by the caller): the time of the
    whole command; c_slot is its mean per slot (redis-cli logs no time per slot)."""
    primary = json.loads(sh("kubectl", "-n", NS, "get", "redisinstance", f"{NAME}-0", "-o", "json"))["status"]["currentPrimary"]
    ip = sh("kubectl", "-n", NS, "get", "pod", primary, "-o", "jsonpath={.status.podIP}")
    before = slot_counts(primary)
    masters_now = len([n for n in before.values() if n > 0])
    if direction == "drain":
        shard3 = json.loads(sh("kubectl", "-n", NS, "get", "redisinstance", f"{NAME}-3", "-o", "json"))["status"]["currentPrimary"]
        node = next(l.split()[0] for l in cli(shard3, "cluster", "nodes").splitlines() if "myself" in l)
        args = f"--cluster-weight {node}=0"
        shards = f"{masters_now}->{masters_now - 1}"
    else:
        args = "--cluster-use-empty-masters"
        shards = f"{masters_now}->{len(before)}"
    keys, mem = data()
    lay = layout()
    show_layout("Pods before", lay)
    run = f"S{stage}-{point}-{variant}-r{rep}-{shards.replace('->', 'to')}"
    tags = {"run": run, "stage": stage, "point": point, "variant": variant, "rep": rep, "shards": shards}
    script = ("export REDISCLI_AUTH=$(cat /internal-auth/operator-password); "
              f"redis-cli --tls --cacert /tls/ca.crt --user _operator --no-auth-warning --cluster rebalance "
              f"{ip}:6379 {args} --cluster-pipeline {pipeline} 2>&1 | grep -v '^#*$' | tail -20")
    t0 = time.time()
    marker(t0, {"event": "START", **tags, "keys": keys})
    out = sh("kubectl", "-n", NS, "exec", primary, "-c", "redis", "--", "sh", "-c", script)
    t1 = time.time()
    print(out)
    after = slot_counts(primary)
    slots = sum(max(0, before.get(n, 0) - after.get(n, 0)) for n in before)
    move_s = t1 - t0
    row = {
        "run": run, "stage": stage, "point": point, "variant": variant, "rep": rep,
        "start": datetime.datetime.fromtimestamp(t0, datetime.timezone.utc).strftime("%H:%M:%S"),
        "shards": shards, "keys": keys, "GiB": f"{mem / 2**30:.2f}", "slots": slots, "local_slots": "-",
        "total_s": f"{move_s:.0f}", "wait_s": "0", "move_s": f"{move_s:.1f}",
        "c_slot_ms": f"{1000 * move_s / max(slots, 1):.1f}", "p90_ms": "-", "pauses": "-", "pauses_s": "-",
        "masters_before": masters_short(lay), "masters_after": masters_short(layout()),
        "grafana": grafana_link(t0, t1),
    }
    marker(t1, {"event": "END", **tags, "move_s": row["move_s"], "c_slot_ms": row["c_slot_ms"], "wait_s": 0,
                "local_slots": "-", "masters": row["masters_before"],
                "from_ms": int((t0 - 60) * 1000), "to_ms": int((t1 + 60) * 1000)})
    print("\t".join(f"{k}={v}" for k, v in row.items()))
    with open(OUT, "a") as f:
        f.write("\t".join(str(v) for v in row.values()) + "\n")
    with open(OUT.removesuffix(".tsv") + ".md", "a") as f:
        f.write(f"| {run} | {row['start']} | {slots} | {keys} | 0 | {row['move_s']} | {row['c_slot_ms']} (średnio) | - | - | "
                f"{row['masters_before']} | [okno]({row['grafana']}) |\n")


def moved_slots(start, end):
    """(timestamp s, from pod, to pod) of the "Moved slot" lines of the cluster, oldest first"""
    query = ('{namespace="redis-operator-system", container="manager"} |= `Moved slot` '
             f'|= `"RedisCluster": {{"name":"{NAME}","namespace":"{NS}"}}`')
    moves, since = [], int(start * 1e9)
    while True:
        url = f"{LOKI}/loki/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": query, "start": since, "end": int(end * 1e9), "limit": 5000, "direction": "forward"})
        batch = []
        for s in json.load(urllib.request.urlopen(url))["data"]["result"]:
            for t, line in s["values"]:
                fields = json.loads(line[line.index("{"):])
                batch.append((int(t), fields.get("from", "?"), fields.get("to", "?")))
        batch.sort()
        moves += batch
        if len(batch) < 5000:
            return [(t / 1e9, f, to) for t, f, to in moves]
        since = batch[-1][0] + 1


def move(n, stage, point, variant, rep):
    keys, mem = data()
    c = cluster()
    before = c["spec"]["shards"]
    run = f"S{stage}-{point}-{variant}-r{rep}-{before}to{n}"
    tags = {"run": run, "stage": stage, "point": point, "variant": variant, "rep": rep, "shards": f"{before}->{n}"}
    lay_before = layout()
    show_layout("Pods before", lay_before)
    t0 = time.time()
    marker(t0, {"event": "START", **tags, "keys": keys})
    sh("kubectl", "-n", NS, "patch", RC, "--type", "merge", "-p", json.dumps({"spec": {"shards": n}}))
    gen = int(json.loads(sh("kubectl", "-n", NS, "get", RC, "-o", "json"))["metadata"]["generation"])
    print(f"{before} -> {n} shards, {keys} keys, {mem / 2**30:.2f} GiB; waiting for Balanced", flush=True)
    while True:
        time.sleep(5)
        c = cluster()
        cond = condition(c, "Migrating")
        if cond.get("reason") == "Balanced" and cond.get("observedGeneration") == gen and \
                len(c["status"].get("shards", [])) == n:
            break
    t1 = datetime.datetime.fromisoformat(cond["lastTransitionTime"].replace("Z", "+00:00")).timestamp()
    time.sleep(10)  # Loki gets the last lines
    moves = moved_slots(t0, time.time())
    ts = [t for t, _, _ in moves]
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    pauses = [g for g in gaps if g > 0.5]
    lay_after = layout()
    show_layout("Pods after", lay_after)
    # Where the slots went: source node -> target node, counted. "same" = MIGRATE never left the machine.
    nodes = {p: n for p, (n, _) in {**lay_before, **lay_after}.items()}
    routes = {}
    for _, f, to in moves:
        key = f"{nodes.get(f, '?').removeprefix('talos-')}->{nodes.get(to, '?').removeprefix('talos-')}"
        routes[key] = routes.get(key, 0) + 1
    local = sum(v for k, v in routes.items() if k.split("->")[0] == k.split("->")[1])
    print("slots by node route: " + ", ".join(f"{k}={v}" for k, v in sorted(routes.items())))
    row = {
        "run": run, "stage": stage, "point": point, "variant": variant, "rep": rep, "start": datetime.datetime.fromtimestamp(t0, datetime.timezone.utc).strftime("%H:%M:%S"),
        "shards": f"{before}->{n}", "keys": keys, "GiB": f"{mem / 2**30:.2f}", "slots": len(ts),
        "local_slots": local,
        "total_s": f"{t1 - t0:.0f}", "wait_s": f"{ts[0] - t0:.1f}" if ts else "-",
        "move_s": f"{ts[-1] - ts[0]:.1f}" if ts else "-",
        "c_slot_ms": f"{1000 * statistics.median(gaps):.1f}" if gaps else "-",
        "p90_ms": f"{1000 * statistics.quantiles(gaps, n=10)[-1]:.1f}" if len(gaps) > 10 else "-",
        "pauses": len(pauses), "pauses_s": f"{sum(pauses):.1f}",
        "masters_before": masters_short(lay_before), "masters_after": masters_short(lay_after),
    }
    row["grafana"] = grafana_link(t0, t1)
    marker(t1, {"event": "END", **tags, "move_s": row["move_s"], "c_slot_ms": row["c_slot_ms"],
                "wait_s": row["wait_s"], "local_slots": local, "masters": row["masters_before"],
                "from_ms": int((t0 - 60) * 1000), "to_ms": int((t1 + 60) * 1000)})
    print("\t".join(f"{k}={v}" for k, v in row.items()))
    md = OUT.removesuffix(".tsv") + ".md"
    if not os.path.exists(md):
        with open(md, "w") as f:
            f.write("# Wyniki: koszt reshardingu\n\nJeden wiersz na przebieg; link otwiera dashboard "
                    "RedisCluster w oknie przebiegu (±1 min).\n\n| Przebieg | Start UTC | slots | keys | wait s | "
                    "move s | c_slot ms | p90 ms | lokalnie | mastery przed | Grafana |\n"
                    "|---|---|---|---|---|---|---|---|---|---|---|\n")
    with open(md, "a") as f:
        f.write(f"| {run} | {row['start']} | {row['slots']} | {keys} | {row['wait_s']} | {row['move_s']} | "
                f"{row['c_slot_ms']} | {row['p90_ms']} | {local} | {row['masters_before']} | "
                f"[okno]({row['grafana']}) |\n")
    new = not os.path.exists(OUT)
    with open(OUT, "a") as f:
        if new:
            f.write("\t".join(row) + "\n")
        f.write("\t".join(str(v) for v in row.values()) + "\n")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "check":
        sys.exit(0 if check() else 1)
    elif cmd == "prefill":
        prefill(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "rcli":
        rcli(*sys.argv[2:8])
    elif cmd == "layout":
        set_layout(sys.argv[2])
    elif cmd == "flush":
        flush()
    elif cmd == "move":
        if len(sys.argv) != 7:
            sys.exit("move N STAGE POINT VARIANT REP, e.g. move 4 1 P0 V0 2")
        move(int(sys.argv[2]), *sys.argv[3:7])
    else:
        print(__doc__)
        sys.exit(2)
