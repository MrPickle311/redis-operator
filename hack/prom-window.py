#!/usr/bin/env python3
"""Where was the bottleneck of a load test? Redis metrics next to CPU, network and disk.

  hack/prom-window.py 19:39:50 19:40:55            # today, UTC, the window of a row
  hack/prom-window.py 19:39:50 19:40:55 redis-play 'store-.*'   # namespace, redis_instance regex

Needs Prometheus on localhost:9090:
  kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090
LINK_MBS=125 is the speed of the network card in MB/s (1 GbE).

Output:
  1. per node and per Redis primary: average and maximum of the window;
  2. a timeline, every 15 s one row: what Redis did (ops/s, main thread, time per command)
     and what the machine did (CPU throttling, network, disk, delayed AOF fsyncs);
  3. a verdict: for every resource how long it was saturated, how ops/s moves with it
     (Pearson r) and whether Redis was waiting for it.

Correlation is not cause. A resource is called the limit only when it is saturated while the
main thread of the masters is NOT; the proof is an intervention that moves one thing
(see showcase.md): the limit goes away, the resource's line drops, ops/s rises.
"""
import datetime, json, os, sys, urllib.parse, urllib.request

PROM = os.environ.get("PROM", "http://localhost:9090")
LINK_MBS = float(os.environ.get("LINK_MBS", "125"))
NIC = "en.*|eth.*|bond.*"


def epoch(hms):
    day = datetime.date.today().isoformat()
    return int(datetime.datetime.strptime(f"{day} {hms} +0000", "%Y-%m-%d %H:%M:%S %z").timestamp())


def get(path, **params):
    return json.load(urllib.request.urlopen(f"{PROM}{path}?" + urllib.parse.urlencode(params)))["data"]["result"]


def show(title, query, a, b, div=1, fmt="%.1f", top=6):
    rows = []
    for s in get("/api/v1/query_range", query=query, start=a, end=b, step=15):
        v = [float(x[1]) for x in s["values"] if x[1] != "NaN"]
        label = next((s["metric"][k] for k in ("instance", "pod", "node", "device") if k in s["metric"]), "")
        if v:
            rows.append((sum(v) / len(v) / div, max(v) / div, label))
    print(" ", title)
    for avg, mx, label in sorted(rows, reverse=True)[:top]:
        print("     " + fmt % avg + " avg  " + fmt % mx + " max  " + label)


def line(query, a, b):
    """One value per 15 s from a query that returns a single series: {timestamp: value}."""
    r = get("/api/v1/query_range", query=query, start=a, end=b, step=15)
    return {int(t): float(v) for t, v in r[0]["values"] if v != "NaN"} if r else {}


def pearson(x, y):
    n = len(x)
    if n < 3:
        return float("nan")
    mx, my = sum(x) / n, sum(y) / n
    sx = sum((p - mx) ** 2 for p in x) ** 0.5
    sy = sum((q - my) ** 2 for q in y) ** 0.5
    return sum((p - mx) * (q - my) for p, q in zip(x, y)) / (sx * sy) if sx and sy else float("nan")


start, end = epoch(sys.argv[1]), epoch(sys.argv[2])
ns = sys.argv[3] if len(sys.argv) > 3 else "redis-play"
inst = sys.argv[4] if len(sys.argv) > 4 else "store-.*"
sel = f'namespace="{ns}",role="primary",redis_instance=~"{inst}"'
pods = f'namespace="{ns}",container="redis",pod=~"{inst}"'


def main_thread(w):
    return (f'sum by (pod)(rate(redis_cpu_user_main_thread_seconds_total{{{sel}}}[{w}])'
            f'+rate(redis_cpu_sys_main_thread_seconds_total{{{sel}}}[{w}]))')


print("window", sys.argv[1], "-", sys.argv[2])
show("node CPU busy (cores)", 'sum by (instance)(rate(node_cpu_seconds_total{mode!="idle"}[1m]))', start, end)
show(f"node network in MB/s (the link is {LINK_MBS:.0f})",
     f'sum by (instance)(rate(node_network_receive_bytes_total{{device=~"{NIC}"}}[1m]))', start, end, 1e6)
show("node network out MB/s",
     f'sum by (instance)(rate(node_network_transmit_bytes_total{{device=~"{NIC}"}}[1m]))', start, end, 1e6)
show("node disk write MB/s", 'sum by (instance)(rate(node_disk_written_bytes_total[1m]))', start, end, 1e6)
show("node disk busy (1.0 = always busy)", 'max by (instance)(rate(node_disk_io_time_seconds_total[1m]))',
     start, end, 1, "%.2f")
show("load Pods CPU (cores)",
     f'rate(container_cpu_usage_seconds_total{{namespace="{ns}",pod=~"scale-load-.*",container="load"}}[1m])',
     start, end, 1, "%.2f")
show("Redis primary main thread (1.0 = a full core)", main_thread("30s"), start, end, 1, "%.2f", 3)
show("Redis primary connections", f'redis_connected_clients{{{sel}}}', start, end, 1, "%.0f", 3)
show("Redis primary traffic out MB/s", f'rate(redis_net_output_bytes_total{{{sel}}}[30s])', start, end, 1e6, "%.1f", 3)

# The timeline: Redis (what happened to the data) and the machine (why).
series = {
    "ops/s":      line(f'sum(rate(redis_commands_processed_total{{{sel}}}[30s]))', start, end),
    "main":       line(f'avg({main_thread("30s")})', start, end),
    "us/cmd":     line(f'1e6*sum(rate(redis_commands_duration_seconds_total{{{sel}}}[30s]))'
                       f'/sum(rate(redis_commands_total{{{sel}}}[30s]))', start, end),
    "throttle":   line(f'sum(rate(container_cpu_cfs_throttled_periods_total{{{pods}}}[1m]))'
                       f'/sum(rate(container_cpu_cfs_periods_total{{{pods}}}[1m]))', start, end),
    "net tx %":   line(f'100*max(sum by (instance)(rate(node_network_transmit_bytes_total{{device=~"{NIC}"}}[1m])))'
                       f'/({LINK_MBS}*1e6)', start, end),
    "net rx %":   line(f'100*max(sum by (instance)(rate(node_network_receive_bytes_total{{device=~"{NIC}"}}[1m])))'
                       f'/({LINK_MBS}*1e6)', start, end),
    "disk busy":  line('max(rate(node_disk_io_time_seconds_total[1m]))', start, end),
    "disk MB/s":  line('max(sum by (instance)(rate(node_disk_written_bytes_total[1m])))/1e6', start, end),
    "write ms":   line('1000*max(rate(node_disk_write_time_seconds_total[1m])'
                       '/rate(node_disk_writes_completed_total[1m]))', start, end),
    "fsync lag":  line(f'sum(increase(redis_aof_delayed_fsync{{namespace="{ns}"}}[30s]))', start, end),
}
# the CPU a master may use: its limit, a full core without one
lim = get("/api/v1/query", query=f'max(kube_pod_container_resource_limits{{{pods},resource="cpu"}})', time=end)
cpu_limit = float(lim[0]["value"][1]) if lim and lim[0]["value"][1] != "NaN" else 1.0
series["main/limit"] = {t: v / cpu_limit for t, v in series["main"].items()}
cols = [c for c in series if c != "main/limit"]
times = sorted(series["ops/s"])
# the verdict ignores the ramp up and down of the load: samples under half of the peak
steady = [t for t in times if series["ops/s"][t] >= 0.5 * max(series["ops/s"].values())]
print("\n  timeline (UTC; ops/s = all primaries; main = average main thread of a master, 1.0 = a full core;")
print("  us/cmd = time Redis spent per command; throttle = share of CPU periods cut by the cgroup limit;")
print("  net/disk = the busiest node; fsync lag = AOF fsyncs that made Redis wait, per 30 s)")
print("  time      " + "".join(f"{c:>10}" for c in cols))
for t in times:
    cells = []
    for c in cols:
        v = series[c].get(t)
        cells.append(f"{'-':>10}" if v is None else f"{v:>10.0f}" if c in ("ops/s", "us/cmd", "fsync lag") and v >= 10
                     else f"{v:>10.2f}")
    print("  " + datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%H:%M:%S") + "  " + "".join(cells))


def mean(xs):
    return sum(xs) / len(xs) if xs else float("nan")


def share(name, ok):
    vals = [series[name][t] for t in steady if t in series[name]]
    return 100 * sum(1 for v in vals if ok(v)) / len(vals) if vals else 0.0


def r_with_ops(name):
    common = [t for t in steady if t in series[name]]
    return pearson([series["ops/s"][t] for t in common], [series[name][t] for t in common])


def effect(name, ok):
    """What Redis did while the resource was saturated against what it did when it was not.
    Returns (text, hurt): hurt is True when ops/s is >= 10 % lower or a command >= 20 % slower."""
    sat = [t for t in steady if t in series[name] and ok(series[name][t])]
    idle = [t for t in steady if t in series[name] and not ok(series[name][t])]
    if not sat:
        return "", False
    if not idle:
        return "saturated all window, nothing to compare with", None
    ops = mean([series["ops/s"][t] for t in sat]) / mean([series["ops/s"][t] for t in idle]) - 1
    cost = mean([series["us/cmd"][t] for t in sat if t in series["us/cmd"]]) / \
        mean([series["us/cmd"][t] for t in idle if t in series["us/cmd"]]) - 1
    return f"while saturated: ops/s {100 * ops:+.0f} %, us/cmd {100 * cost:+.0f} %", ops <= -0.1 or cost >= 0.2


main_busy = share("main/limit", lambda v: v >= 0.9)
print("\n  verdict (saturated = share of the window; r = Pearson of the resource with ops/s;")
print("  effect = Redis while the resource was saturated against while it was not;")
print(f"  {len(steady)} of {len(times)} samples counted, the ramp of the load left out)")
checks = [
    ("CPU quota of a master (throttled >= 25 %)", "throttle", lambda v: v >= 0.25),
    ("network card of a node (out >= 80 %)", "net tx %", lambda v: v >= 80),
    ("network card of a node (in >= 80 %)", "net rx %", lambda v: v >= 80),
    ("disk of a node (busy >= 80 %)", "disk busy", lambda v: v >= 0.8),
    ("AOF fsync that made Redis wait", "fsync lag", lambda v: v > 0),
]
limits, unproven = [], []
for label, name, ok in checks:
    sat = share(name, ok)
    text, hurt = effect(name, ok)
    r = r_with_ops(name)
    r_text = "   n/a" if r != r else f"{r:+.2f}"
    print(f"    {label:<44} saturated {sat:>3.0f} %   r = {r_text}   {text}")
    if sat >= 30 and hurt:
        limits.append(label)
    elif sat >= 30:
        unproven.append(label)
print(f"    main thread of a master >= 90 % of its CPU ({cpu_limit:g} core) in {main_busy:.0f} % of the window")
if main_busy >= 50:
    print("  => the CPU of the masters is the limit: Redis is busy, ops/s is what one core (or its quota) does.")
elif limits:
    print(f"  => limit: {', '.join(limits)}. Confirm with the intervention in showcase.md: remove it, ops/s must rise.")
elif unproven:
    print(f"  => busy but no effect seen: {', '.join(unproven)}. These are loaded, not (yet) the limit.")
    print("     With the main thread idle, the clients are the usual limit: requests in flight / latency")
    print("     (Little's law). More clients or --pipeline; then look again.")
else:
    print("  => nothing is saturated and the main thread is not busy: the clients are the limit")
    print("     (requests in flight / latency, Little's law). More clients or --pipeline.")
print("  (a window of a few samples shows a hint, not a proof)")
