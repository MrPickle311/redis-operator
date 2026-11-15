#!/usr/bin/env python3
"""Calibration of the hardware for docs/experiments/resharding-cost: what the network, one CPU
core and the disk of every node can do, without the operator. The resharding results are then
read as shares of these numbers, so another cluster can redo the calibration and compare.

  hack/calibrate.py [IM_WINDOW_START IM_WINDOW_END]   # UTC HH:MM:SS of a resharding, for the IM calls

Runs Pods pinned to every node that takes Redis Pods, in the namespace redis-calib (deleted at
the end), and writes docs/experiments/resharding-cost/calibration.md. Needs Prometheus on
localhost:9090 for the Instance Manager calls (kubectl -n monitoring port-forward
svc/kube-prometheus-stack-prometheus 9090:9090).

  network  iperf3 TCP 10 s and ping x20 between every ordered pair of nodes, Pod to Pod (flannel
           VXLAN, as Redis); no TLS (iperf3 has none)
  CPU      redis-server and redis-benchmark in one Pod per node, without TLS: SET of 1000 B with
           pipeline 16 (what one core of Redis does), and redis-cli --intrinsic-latency
  disk     fio on a longhorn-nvme-local volume per node: 4 KiB writes each followed by fsync
           (as AOF appendfsync always; everysec does one per second) and 1 MiB sequential writes
  IM       mean latency of every Instance Manager path in the window, from Prometheus
"""
import datetime, json, os, subprocess, sys, time, urllib.parse, urllib.request

NS = "redis-calib"
TOOLS = "registry.homelab/library/benchtools:1"
REDIS = "registry.homelab/library/redis:7.2"
NODES = ["talos-sad-s24", "talos-u0c-uk3", "talos-wdp-fwu"]  # sie-ons: control-plane, takes no Redis Pod
PROM = os.environ.get("PROM", "http://localhost:9090")
OUT = os.path.join(os.path.dirname(__file__), "..", "docs/experiments/resharding-cost/calibration.md")


def sh(*args, input=None, check=True):
    return subprocess.run(args, input=input, capture_output=True, text=True, check=check).stdout


def apply(obj):
    sh("kubectl", "apply", "-f", "-", input=json.dumps(obj))


def pod(name, node, image, command, volume=None):
    spec = {"nodeName": node, "restartPolicy": "Never", "terminationGracePeriodSeconds": 0,
            "containers": [{"name": "c", "image": image, "command": ["sh", "-c", command],
                            "resources": {"requests": {"cpu": "1", "memory": "256Mi"}}}]}
    if volume:
        spec["containers"][0]["volumeMounts"] = [{"name": "v", "mountPath": "/v"}]
        spec["volumes"] = [{"name": "v", "persistentVolumeClaim": {"claimName": volume}}]
    apply({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": NS}, "spec": spec})


def wait_ready(names, timeout="300s"):
    for n in names:
        sh("kubectl", "-n", NS, "wait", "--for=condition=Ready", f"pod/{n}", f"--timeout={timeout}")


def ex(name, command):
    return sh("kubectl", "-n", NS, "exec", name, "--", "sh", "-c", command)


def short(node):
    return node.removeprefix("talos-")


def network():
    for n in NODES:
        pod(f"iperf-{short(n)}", n, TOOLS, "iperf3 -s")
    wait_ready([f"iperf-{short(n)}" for n in NODES])
    ip = {n: sh("kubectl", "-n", NS, "get", "pod", f"iperf-{short(n)}", "-o", "jsonpath={.status.podIP}") for n in NODES}
    rows = []
    for a in NODES:
        for b in NODES:
            if a == b:
                continue
            r = json.loads(ex(f"iperf-{short(a)}", f"iperf3 -c {ip[b]} -t 10 -J"))
            gbps = r["end"]["sum_received"]["bits_per_second"] / 1e9
            ping = ex(f"iperf-{short(a)}", f"ping -c 20 -i 0.2 -q {ip[b]}").strip().splitlines()[-1]
            mn, avg, mx, _ = ping.split("=")[1].split()[0].split("/")
            rows.append((short(a), short(b), gbps, float(avg), float(mx)))
            print(f"net {short(a)} -> {short(b)}: {gbps:.2f} Gbit/s, rtt avg {avg} ms", flush=True)
    return rows


def cpu():
    script = ("redis-server --save '' --appendonly no --daemonize yes >/dev/null && sleep 1 && "
              "redis-benchmark -q -t set -n 2000000 -d 1000 -P 16 -c 50 --csv && "
              "redis-benchmark -q -t set -n 300000 -d 1000 -P 1 -c 50 --csv && "
              "redis-cli --intrinsic-latency 10 | grep -o 'Max latency so far: [0-9]* microseconds' | tail -1")
    rows = []
    for n in NODES:
        name = f"cpu-{short(n)}"
        pod(name, n, REDIS, "sleep 3600")
        wait_ready([name])
        lines = ex(name, script).strip().splitlines()
        sets = [float(l.split(",")[1].strip('"')) for l in lines if l.startswith('"SET"')]
        intrinsic = lines[-1]
        rows.append((short(n), sets[0], sets[1], intrinsic))
        print(f"cpu {short(n)}: SET p16 {sets[0]:.0f}/s, p1 {sets[1]:.0f}/s; {intrinsic}", flush=True)
    return rows


def disk():
    rows = []
    for n in NODES:
        claim = f"fio-{short(n)}"
        apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": claim, "namespace": NS},
               "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "longhorn-nvme-local",
                        "resources": {"requests": {"storage": "5Gi"}}}})
        pod(claim, n, TOOLS, "sleep 3600", volume=claim)
        wait_ready([claim], "600s")
        # best-effort locality: wait until the one replica sits on this node
        volume = sh("kubectl", "-n", NS, "get", "pvc", claim, "-o", "jsonpath={.spec.volumeName}")
        for _ in range(60):
            where = sh("kubectl", "-n", "longhorn-system", "get", "replicas.longhorn.io", "-l",
                       f"longhornvolume={volume}", "-o",
                       "jsonpath={range .items[*]}{.spec.nodeID}:{.status.currentState} {end}").split()
            if where == [f"{n}:running"]:
                break
            time.sleep(5)
        sync = json.loads(ex(claim, "fio --name=s --filename=/v/f --rw=write --bs=4k --size=64m "
                                    "--fsync=1 --output-format=json"))["jobs"][0]["sync"]["lat_ns"]
        seq = json.loads(ex(claim, "fio --name=q --filename=/v/g --rw=write --bs=1m --size=2g "
                                   "--direct=1 --output-format=json"))["jobs"][0]["write"]
        p = sync["percentile"]
        rows.append((short(n), " ".join(where), sync["mean"] / 1e6, p["99.000000"] / 1e6, seq["bw_bytes"] / 1e6))
        print(f"disk {short(n)}: fsync mean {sync['mean'] / 1e6:.2f} ms p99 {p['99.000000'] / 1e6:.2f} ms, "
              f"seq {seq['bw_bytes'] / 1e6:.0f} MB/s, replica {where}", flush=True)
    return rows


def im_calls(start, end):
    day = datetime.date.today().isoformat()
    ts = [int(datetime.datetime.strptime(f"{day} {t} +0000", "%Y-%m-%d %H:%M:%S %z").timestamp()) for t in (start, end)]
    q = ("sum by (path) (increase(redis_operator_im_request_duration_seconds_sum[{w}s])) / "
         "sum by (path) (increase(redis_operator_im_request_duration_seconds_count[{w}s]))").format(w=ts[1] - ts[0])
    qc = "sum by (path) (increase(redis_operator_im_request_duration_seconds_count[{w}s]))".format(w=ts[1] - ts[0])
    get = lambda q: {r["metric"]["path"]: float(r["value"][1]) for r in json.load(urllib.request.urlopen(
        f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": ts[1]})))["data"]["result"]}
    mean, count = get(q), get(qc)
    return [(p, 1000 * mean[p], count.get(p, 0)) for p in sorted(mean) if mean[p] == mean[p]]


def main():
    sh("kubectl", "create", "namespace", NS, check=False)
    try:
        net, cores, disks = network(), cpu(), disk()
    finally:
        sh("kubectl", "delete", "namespace", NS, "--wait=false", check=False)
    im = im_calls(sys.argv[1], sys.argv[2]) if len(sys.argv) > 2 else []
    links = json.load(urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode(
        {"query": 'node_network_speed_bytes{device=~"en.*|eth.*"} * 8 / 1e6 and on (instance, device) node_network_up == 1'})))["data"]["result"]
    when = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    md = [f"# Kalibracja sprzętu ({when})", "",
          "Wyniki `hack/calibrate.py`; opis metody w nagłówku skryptu. Liczby z jednego przebiegu.", "",
          "## Sieć (Pod → Pod, flannel VXLAN, bez TLS)", "",
          "Karty sieciowe (node-exporter, łącze w górze):", "",
          "| Węzeł (instance) | Karta | Mbit/s |", "|---|---|---|"]
    md += [f"| {l['metric']['instance']} | {l['metric']['device']} | {float(l['value'][1]):.0f} |" for l in links]
    md += ["", "| Z | Do | iperf3 Gbit/s | MB/s | RTT śr. ms | RTT maks. ms |", "|---|---|---|---|---|---|"]
    md += [f"| {a} | {b} | {g:.2f} | {g * 125:.0f} | {avg:.3f} | {mx:.3f} |" for a, b, g, avg, mx in net]
    md += ["", "## Jeden rdzeń Redisa (redis-benchmark w tym samym Podzie, bez TLS, SET 1000 B, 50 klientów)", "",
           "| Węzeł | SET/s pipeline 16 | SET/s pipeline 1 | intrinsic latency |", "|---|---|---|---|"]
    md += [f"| {n} | {p16:.0f} | {p1:.0f} | {i} |" for n, p16, p1, i in cores]
    md += ["", "## Dysk (`longhorn-nvme-local`, replika na węźle Poda)", "",
           "| Węzeł | Replika | zapis 4 KiB + fsync śr. ms | p99 ms | zapis sekwencyjny 1 MiB MB/s |",
           "|---|---|---|---|---|"]
    md += [f"| {n} | {w} | {m:.2f} | {p:.2f} | {bw:.0f} |" for n, w, m, p, bw in disks]
    if im:
        md += ["", f"## Wywołania Instance Managera (okno {sys.argv[1]}–{sys.argv[2]} UTC)", "",
               "| Ścieżka | śr. ms | wywołań |", "|---|---|---|"]
        md += [f"| {p} | {m:.2f} | {c:.0f} |" for p, m, c in im]
    with open(OUT, "w") as f:
        f.write("\n".join(md) + "\n")
    print("written", OUT)


if __name__ == "__main__":
    main()
