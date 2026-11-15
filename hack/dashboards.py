"""Generates config/monitoring/dashboards/*.json. Run: python3 hack/dashboards.py ."""
import json
import sys

DS = {"type": "prometheus", "uid": "${datasource}"}
SHARD_COLORS = ["blue", "green", "orange", "purple", "red", "yellow", "dark-blue", "dark-green", "dark-orange", "dark-purple"]


def target(expr, legend="", ref="A", instant=False):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "range": not instant}
    if instant:
        t["instant"] = True
    return t


class Grid:
    def __init__(self):
        self.y = 0
        self.x = 0
        self.row_h = 0
        self.panels = []
        self.id = 1

    def add(self, panel, w, h):
        if self.x + w > 24:
            self.y += self.row_h
            self.x, self.row_h = 0, 0
        panel["gridPos"] = {"x": self.x, "y": self.y, "w": w, "h": h}
        panel["id"] = self.id
        panel.setdefault("datasource", DS)
        self.id += 1
        self.x += w
        self.row_h = max(self.row_h, h)
        self.panels.append(panel)

    def row(self, title):
        self.y += self.row_h
        self.x, self.row_h = 0, 0
        self.panels.append({"type": "row", "title": title, "collapsed": False, "id": self.id,
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.id += 1
        self.y += 1


def mappings(values):
    """values: {value: (text, color)}"""
    return [{"type": "value", "options": {str(v): {"text": t, "color": c, "index": i}
                                          for i, (v, (t, c)) in enumerate(values.items())}}]


def stat(title, targets, unit="none", text_mode="value", maps=None, color_mode="value", desc=""):
    return {"type": "stat", "title": title, "description": desc, "targets": targets,
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "textMode": text_mode, "colorMode": color_mode, "graphMode": "none", "justifyMode": "center"},
            "fieldConfig": {"defaults": {"unit": unit, "mappings": maps or [],
                                         "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": [{"color": "text", "value": None}]}},
                            "overrides": []}}


def rows(metric, by):
    """max by (by) of the metric, and -1 for a series seen in the range that is
    gone: Grafana draws the last value of a series that stopped up to now."""
    return f"max by ({by}) ({metric}) or (max by ({by}) (max_over_time({metric}[$__range])) * 0 - 1)"


def gone(maps):
    maps = json.loads(json.dumps(maps))
    maps[0]["options"]["-1"] = {"text": " ", "color": "transparent", "index": 99}
    return maps


def timeline(title, targets, maps, desc="", merge=True, show_values="auto", overrides=None):
    # insertNulls: a series that stops (an old reason, a deleted Pod) ends its
    # bar there, instead of Grafana drawing its last value up to now.
    defaults = {"mappings": gone(maps), "custom": {"fillOpacity": 85, "lineWidth": 0, "insertNulls": 20000, "spanNulls": False},
                # Not thresholds: with merged values the state timeline turns the
                # numbers into threshold labels and drops the value mappings.
                "color": {"mode": "palette-classic"}}
    return {"type": "state-timeline", "title": title, "description": desc, "targets": targets, "interval": "5s",
            "options": {"mergeValues": merge, "showValue": show_values, "alignValue": "center", "rowHeight": 0.85,
                        "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []}}


def series(title, targets, unit="short", desc="", stack=False):
    custom = {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never", "spanNulls": False}
    if stack:
        custom["stacking"] = {"mode": "normal"}
    return {"type": "timeseries", "title": title, "description": desc, "targets": targets,
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
            "fieldConfig": {"defaults": {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}},
                            "overrides": []}}


def text(content):
    return {"type": "text", "title": "", "options": {"mode": "markdown", "content": content}, "transparent": True}


def bars(title, targets, desc=""):
    p = series(title, targets, desc=desc, stack=True)
    p["fieldConfig"]["defaults"]["custom"].update({"drawStyle": "bars", "fillOpacity": 80, "barAlignment": 0})
    p["fieldConfig"]["defaults"]["decimals"] = 0
    p["interval"] = "5s"
    return p


def variable(name, label, query, hide=0):
    return {"name": name, "label": label, "type": "query", "datasource": DS, "hide": hide,
            "query": {"query": query, "refId": name}, "definition": query, "refresh": 2, "sort": 1,
            "includeAll": False, "multi": False, "current": {}}


def dashboard(uid, title, description, variables, grid, annotations):
    return {
        "uid": uid, "title": title, "description": description, "tags": ["redis-operator"],
        "editable": True, "schemaVersion": 39, "version": 1, "refresh": "5s", "graphTooltip": 1,
        "time": {"from": "now-30m", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
        "templating": {"list": [{"name": "datasource", "label": "Prometheus", "type": "datasource",
                                 "query": "prometheus", "current": {}, "hide": 0},
                                {"name": "loki", "label": "Loki", "type": "datasource",
                                 "query": "loki", "current": {}, "hide": 0}] + variables},
        "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                  "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
                                  "name": "Annotations & Alerts", "type": "dashboard"}] + annotations},
        "panels": grid.panels,
    }


def annotation(name, expr, text, color):
    return {"name": name, "datasource": DS, "enable": True, "iconColor": color, "expr": expr,
            "step": "15s", "titleFormat": name, "textFormat": text, "useValueForTime": False}


def failed(pattern, text):
    """Shows False in red for the reasons matching pattern, e.g. Failed."""
    return [{"matcher": {"id": "byRegexp", "options": pattern},
             "properties": [{"id": "mappings", "value": gone(mappings({1: ("in progress", "orange"), 0: (text, "red")}))}]}]


OPERATION = mappings({1: ("in progress", "orange"), 0: ("idle", "#2c3235")})
ROLE = mappings({0: ("replica", "blue"), 1: ("primary", "green"), 2: ("becoming primary", "orange")})
TRUE_FALSE = mappings({1: ("True", "green"), 0: ("False", "red")})


def counted(metric, window):
    """Increments of a counter in the window, also of a series born in it:
    increase() misses the first one, as a counter appears with 1."""
    return f"({metric} - ({metric} offset {window} or {metric} * 0))"


REASON_COLORS = [
    ("^(Ready|Applied|Issued|Validated|VolumesReady|MemoryLimitSet|Covered)$", "green"),
    ("^(Completed|Balanced)$", "#5a6270"),
    ("^(InProgress|Moving|Waiting|Pending|RollingUpdate|Switchover|Failover|Resharding|InstancesNotReady|ShardsNotReady|CAExpiresSoon)$", "orange"),
    (".*", "red"),
]


def state_rows(title, expr, partition, display, field, colors, desc):
    """One row per value of the label partition, the text of its bar the
    label field over time, coloured by regexes of it: a table query,
    partitioned by value."""
    return {
        "type": "state-timeline", "title": title, "description": desc, "interval": "5s",
        "targets": [{"datasource": DS, "expr": expr, "format": "table", "range": True, "instant": False, "refId": "A"}],
        "transformations": [
            {"id": "partitionByValues", "options": {"fields": [partition], "keepFields": False, "naming": {"asLabels": True}}},
            {"id": "organize", "options": {"excludeByName": {"Value": True}}},
            {"id": "sortBy", "options": {"sort": [{"field": "Time"}]}},
        ],
        "options": {"mergeValues": True, "showValue": "auto", "alignValue": "center", "rowHeight": 0.85,
                    "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
        "fieldConfig": {"defaults": {
            "displayName": display,
            "custom": {"fillOpacity": 85, "lineWidth": 0, "insertNulls": 20000, "spanNulls": False},
            "color": {"mode": "fixed", "fixedColor": "red"},
            "mappings": [{"type": "regex", "options": {"pattern": p, "result": {"color": c, "index": i}}}
                         for i, (p, c) in enumerate(colors)],
        }, "overrides": []},
    }


def conditions(title, expr, desc):
    """One row per condition, the text of its bar the reason, coloured by what
    the reason means: a table query, partitioned by type."""
    return {
        "type": "state-timeline", "title": title, "description": desc, "interval": "5s",
        "targets": [{"datasource": DS, "expr": f"max by (type, reason) ({expr})", "format": "table",
                     "range": True, "instant": False, "refId": "A"}],
        "transformations": [
            {"id": "partitionByValues", "options": {"fields": ["type"], "keepFields": False, "naming": {"asLabels": True}}},
            {"id": "organize", "options": {"excludeByName": {"Value": True}}},
            {"id": "sortBy", "options": {"sort": [{"field": "Time"}]}},
        ],
        "options": {"mergeValues": True, "showValue": "auto", "alignValue": "center", "rowHeight": 0.85,
                    "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
        "fieldConfig": {"defaults": {
            "displayName": "${__field.labels.type}",
            "custom": {"fillOpacity": 85, "lineWidth": 0, "insertNulls": 20000, "spanNulls": False},
            "color": {"mode": "fixed", "fixedColor": "red"},
            "mappings": [{"type": "regex", "options": {"pattern": p, "result": {"color": c, "index": i}}}
                         for i, (p, c) in enumerate(REASON_COLORS)],
        }, "overrides": []},
    }


LOKI = {"type": "loki", "uid": "${loki}"}
# zap console lines: <time>\t<LEVEL>\t[events\t]<message>\t<json>
OPERATOR_LINE = ('| regexp `^\\S+\\t(?P<level>\\S+)\\t(?:events\\t)?(?P<msg>[^\\t]+)` '
                 '| regexp `"reason": "(?P<reason>[^"]+)"` | regexp `"error": "(?P<error>(?:[^"\\\\]|\\\\.)*)"` '
                 '| line_format `operator  {{.msg}} {{.reason}} {{.error}}`')


def operator_logs(objects):
    """LogQL of the log lines of the operator about the objects, a regexp of
    their {"name":...,"namespace":...}."""
    return ('{namespace="redis-operator-system", container="manager"} '
            f'|~ `\\{{"name":"{objects}","namespace":"$namespace"\\}}` != `Moved slot` ' + OPERATOR_LINE)


def logs(title, exprs, desc):
    return {"type": "logs", "title": title, "description": desc, "datasource": LOKI,
            "targets": [{"datasource": LOKI, "expr": e, "refId": chr(65 + i), "queryType": "range"}
                        for i, e in enumerate(exprs)],
            "options": {"showTime": True, "sortOrder": "Ascending", "wrapLogMessage": True, "enableLogDetails": True,
                        "dedupStrategy": "none", "prettifyLogMessage": False, "showLabels": False}}


def loki_annotation(name, objects, match, color):
    expr = operator_logs(objects).replace(" != `Moved slot` ", f" != `Moved slot` |~ `{match}` ")
    return {"name": name, "datasource": LOKI, "enable": True, "iconColor": color, "expr": expr,
            "target": {"expr": expr, "refId": "Anno"}}


def completed(objects, fields):
    """LogQL of the lines "Finished <kind>" (or "Completed <kind>" of older
    versions) about the objects, their JSON fields as labels: of the operator,
    and of the Instance Managers in the Pods, which log their self-fences the
    same way."""
    return ('{namespace=~"redis-operator-system|$namespace", container=~"manager|redis"} '
            f'|~ `\\{{"name":"{objects}","namespace":"$namespace"\\}}` |~ `(Completed|Finished) ({OPERATION_KINDS})` '
            f'| regexp `\\t(?P<fields>\\{{.*\\}})$` | line_format `{{{{.fields}}}}` | json {fields} | drop fields')


OPERATION_KINDS = "switchover|failover|rolling update|safe start|recreate|volume replaced|configuration|certificate|scale up|scale down|self-fence"
KIND_COLORS = [("switchover", "blue"), ("failover", "red"), ("rolling update", "purple"), ("safe start", "#56a9a5"),
               ("recreate", "orange"), ("volume replaced", "dark-orange"), ("configuration", "#8ab8ff"),
               ("certificate", "#73bf69"), ("scale up", "yellow"), ("scale down", "dark-yellow"), ("self-fence", "dark-red")]


def loki_stat(title, expr, unit="none", desc=""):
    p = stat(title, [{"datasource": LOKI, "expr": expr, "refId": "A", "queryType": "instant", "legendFormat": "{{kind}}"}],
             unit=unit, text_mode="value", desc=desc)
    p["datasource"] = LOKI
    p["options"]["reduceOptions"]["calcs"] = ["lastNotNull"]
    return p


def operations(g, objects, what):
    """Every switchover, failover, rolling update and safe start of the
    objects, from the line "Finished <kind>" the operator logs at the end of
    each (or "Completed <kind>" of older versions, without an outcome)."""
    sel = completed(objects, "kind, outcome, name, from, to, epoch, durationMs, downMs, reason, startMs, endMs")
    ok = '!~ `"outcome": "(rejected|failed|interrupted)"`'
    count = lambda match: f'sum(count_over_time({completed(objects, "kind")} {match} [$__range])) or vector(0)'
    slowest = lambda kind, field: (f'max(max_over_time({completed(objects, "kind, " + field)} |= `"kind": "{kind}"` {ok} '
                                   f'| unwrap {field} [$__range]))')
    g.row("Operations: everything the operator (and the Instance Managers) did to it")
    g.add(loki_stat("Switchovers", count(f'|= `"kind": "switchover"` {ok}'), desc=f"Completed planned moves of the primary of {what}"), 4, 4)
    g.add(loki_stat("Failovers", count('|= `"kind": "failover"`'), desc=f"Failovers of {what}"), 4, 4)
    g.add(loki_stat("Rejected or failed", count('|~ `"outcome": "(rejected|failed|interrupted)"`'),
                    desc="Switchovers and recreates the operator rejected (e.g. a target that is not Ready, the primary), "
                         "that failed (the target did not catch up in a minute) or that a failover interrupted"), 4, 4)
    g.add(loki_stat("Slowest switchover", slowest("switchover", "durationMs"), "ms",
                    "From the start of the switchover until the target is the primary"), 4, 4)
    g.add(loki_stat("Slowest failover", slowest("failover", "durationMs"), "ms",
                    "From the start of the failover until the target is the primary"), 4, 4)
    g.add(loki_stat("Longest primary outage", f'max(max_over_time({completed(objects, "kind, downMs")} | unwrap downMs [$__range]))', "ms",
                    "How long a primary was unavailable before a failover or a safe start ended"), 4, 4)
    links = completed(objects, "kind, outcome, name, from, to, epoch, durationMs, downMs, reason, startMs, endMs").replace(
        " | drop fields",
        ' | label_format linkFrom=`{{ if .startMs }}{{ sub .startMs 3000 }}{{ end }}`, '
        'linkTo=`{{ if .endMs }}{{ add .endMs 5000 }}{{ end }}`, outcome=`{{ or .outcome "completed" }}`, inst=`{{ .name }}` | drop fields')
    colors = lambda pairs: [{"type": "value", "options": {k: {"text": k, "color": c, "index": i} for i, (k, c) in enumerate(pairs)}}]
    table = {
        "type": "table", "title": "Every operation (click one for its logs)", "datasource": LOKI,
        "description": "One row per switchover, failover, rolling update, safe start, recreate (asked for), volume "
                       "replaced (a PVC of a replica deleted or lost), configuration (spec.redis applied or rejected by Redis), "
                       "certificate (issued or updated), scale up or down, and self-fence (the Instance Manager paused the "
                       "writes of the primary while it could not reach the API server), from the line logged when it ends, "
                       "with its outcome and why it started. Took: from its start to its end; for a volume, until the replica is Ready again on the new "
                       "one, after its full sync. Primary down: for a failover or a "
                       "safe start, since the primary became unavailable. Click the kind of a row: the dashboard opens on "
                       "the seconds of that operation, and *What happened, to the millisecond* shows every log line of it, "
                       "of the operator and of the Pods.",
        "targets": [{"datasource": LOKI, "expr": links, "refId": "A", "queryType": "range"}],
        "transformations": [
            {"id": "extractFields", "options": {"source": "labels"}},
            {"id": "organize", "options": {
                "excludeByName": {"labels": True, "Line": True, "tsNs": True, "id": True, "fields": True, "startMs": True, "endMs": True},
                "indexByName": {"Time": 0, "kind": 1, "outcome": 2, "name": 3, "from": 4, "to": 5, "epoch": 6, "durationMs": 7,
                                "downMs": 8, "reason": 9},
                "renameByName": {"kind": "What", "outcome": "Outcome", "name": "Of", "from": "From", "to": "To", "epoch": "Epoch",
                                 "durationMs": "Took", "downMs": "Primary down", "reason": "Why"}}},
            {"id": "convertFieldType", "options": {"conversions": [
                {"targetField": "Took", "destinationType": "number"},
                {"targetField": "Primary down", "destinationType": "number"}]}},
            {"id": "sortBy", "options": {"sort": [{"field": "Time", "desc": True}]}},
        ],
        "options": {"showHeader": True, "cellHeight": "md"},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": [
            {"matcher": {"id": "byName", "options": "What"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                {"id": "mappings", "value": colors(KIND_COLORS)},
                {"id": "links", "value": [{
                    "title": "The logs of this operation",
                    "url": "/d/redis-operator-instance/?var-datasource=${datasource}&var-loki=${loki}&var-namespace=${namespace}"
                           "&var-instance=${__data.fields.inst}&from=${__data.fields.linkFrom}&to=${__data.fields.linkTo}"}]}]},
            {"matcher": {"id": "byName", "options": "Outcome"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                {"id": "mappings", "value": colors([("completed", "green"), ("approved", "green"), ("applied", "green"),
                                                    ("issued", "green"), ("updated", "green"), ("lifted", "green"),
                                                    ("rejected", "orange"), ("failed", "red"), ("interrupted", "red"),
                                                    ("failover", "red"), ("demoted", "red")])}]},
            {"matcher": {"id": "byRegexp", "options": "Took|Primary down"}, "properties": [
                {"id": "unit", "value": "ms"},
                {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "gradient", "valueDisplayMode": "text"}},
                {"id": "color", "value": {"mode": "continuous-GrYlRd"}}]},
            {"matcher": {"id": "byRegexp", "options": "linkFrom|linkTo|inst"}, "properties": [{"id": "custom.hidden", "value": True}]},
        ]},
    }
    g.add(table, 24, 10)
    points = series("How long each one took",
                    [{"datasource": LOKI, "refId": "A", "queryType": "range", "legendFormat": "{{kind}}",
                      "expr": f'max by (kind) (max_over_time({completed(objects, "kind, durationMs")} {ok} | unwrap durationMs [$__interval]))'}],
                    "ms", desc="A point per operation at the moment it ended; the height is how long it took (log scale)")
    points["datasource"] = LOKI
    points["interval"] = "1s"
    points["fieldConfig"]["defaults"]["custom"].update({"drawStyle": "points", "pointSize": 12,
                                                         "scaleDistribution": {"type": "log", "log": 10}})
    points["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": n}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
        for n, c in KIND_COLORS]
    g.add(points, 24, 7)


EXPERIMENT = '{job="reshard-cost", namespace="$namespace", cluster="$cluster"}'


def experiment_annotation(sel=EXPERIMENT):
    """START and END of every run of hack/reshard-cost.py, with its id"""
    expr = sel + ' | logfmt | line_format `{{.event}} {{.run}} {{.note}}`'
    return {"name": "Experiment runs", "datasource": LOKI, "enable": True, "iconColor": "#ff9830", "expr": expr,
            "target": {"expr": expr, "refId": "Anno"}}


def experiment_runs(g):
    """Every run of hack/reshard-cost.py of the last 30 days, a click opens its window"""
    g.row("Experiment runs (hack/reshard-cost.py)")
    g.add({
        "type": "table", "title": "Every run (click its id to open its window, ±1 min)", "datasource": LOKI,
        "timeFrom": "30d",
        "description": "One row per run of hack/reshard-cost.py move: stage, point (P3-L: layout L), variant of the "
                       "operator, repetition, shards, and what it measured. The run id opens this dashboard on the "
                       "window of the run; the markers START and END are the annotation Experiment runs. "
                       "Event INVALID (red): a run that must be left out, and why (Note): a duplicate runner, an "
                       "interrupted run, a resharding that was not measured.",
        "targets": [{"datasource": LOKI, "refId": "A", "queryType": "range",
                     "expr": EXPERIMENT + ' |~ "event=(END|INVALID)" | logfmt'}],
        "transformations": [
            {"id": "extractFields", "options": {"source": "labels"}},
            {"id": "organize", "options": {
                "excludeByName": {"labels": True, "Line": True, "tsNs": True, "id": True, "job": True,
                                  "cluster": True, "namespace": True, "detected_level": True, "service_name": True},
                "indexByName": {"Time": 0, "event": 1, "run": 2, "stage": 3, "point": 4, "variant": 5, "rep": 6, "shards": 7,
                                "wait_s": 8, "move_s": 9, "c_slot_ms": 10, "local_slots": 11, "masters": 12, "note": 13},
                "renameByName": {"Time": "End", "run": "Run", "stage": "Stage", "point": "Point", "variant": "Variant",
                                 "rep": "Rep", "shards": "Shards", "wait_s": "Wait s", "move_s": "Move s",
                                 "c_slot_ms": "c_slot ms", "local_slots": "Slots local", "masters": "Masters before",
                                 "event": "Event", "note": "Note"}}},
            {"id": "sortBy", "options": {"sort": [{"field": "End", "desc": True}]}},
        ],
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": [
            {"matcher": {"id": "byName", "options": "Run"}, "properties": [{"id": "links", "value": [{
                "title": "Open the window of this run",
                "url": "/d/redis-operator-cluster/?var-datasource=${datasource}&var-loki=${loki}&var-namespace=${namespace}"
                       "&var-cluster=${cluster}&from=${__data.fields.from_ms}&to=${__data.fields.to_ms}"}]}]},
            {"matcher": {"id": "byRegexp", "options": "from_ms|to_ms"}, "properties": [{"id": "custom.hidden", "value": True}]},
            {"matcher": {"id": "byName", "options": "Event"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                {"id": "mappings", "value": mappings({"END": ("ok", "green"), "INVALID": ("INVALID", "red")})}]},
        ]},
    }, 24, 9)


def pods_of(namespace_sel, pod_regex):
    """kube_pod_info of the Pods, once per Pod: kube-state-metrics may run twice."""
    return f'max by (pod, pod_ip, node) (kube_pod_info{{{namespace_sel},pod=~"{pod_regex}"}})'


def replicas_by_ip(pods):
    """The Pods, by slave_ip as the primary reports its replicas, named replica."""
    return f'label_replace(label_replace({pods}, "replica", "$1", "pod", "(.*)"), "slave_ip", "$1", "pod_ip", "(.*)")'


def topology(sel, pods, title, desc):
    """A node graph: a node per Pod (green primary, blue replica, under it the
    machine, its commands per second), an edge per replica from its primary
    with the bytes per second it replicates."""
    # "and" the series of now: the rate looks back minutes, over a switchover
    # it would also give the old role of a Pod, and the old pairs below.
    ops = (f"sum by (pod, node, role) ((rate(redis_commands_processed_total{{{sel}}}[$__rate_interval]) "
           f"and redis_commands_processed_total{{{sel}}}) * on (pod) group_left (node) {pods})")
    for label, src in [("id", "pod"), ("title", "pod"), ("subtitle", "node")]:
        ops = f'label_replace({ops}, "{label}", "$1", "{src}", "(.*)")'
    ops = f'label_replace(label_replace({ops}, "color", "green", "role", "primary"), "color", "blue", "role", "replica")'
    nodes = f"sum by (id, title, subtitle, color) ({ops})"
    edges = (f"sum by (pod, slave_ip) (deriv(redis_connected_slave_offset_bytes{{{sel}}}[$__rate_interval]) "
             f"and redis_connected_slave_offset_bytes{{{sel}}}) "
             f"* on (slave_ip) group_left (replica) {replicas_by_ip(pods)}")
    edges = f'label_replace(label_replace({edges}, "source", "$1", "pod", "(.*)"), "target", "$1", "replica", "(.*)")'
    edges = f'sum by (id, source, target) (label_join({edges}, "id", "->", "source", "target"))'
    unit = lambda ref, u: {"matcher": {"id": "byFrameRefID", "options": ref}, "properties": [{"id": "unit", "value": u}]}
    return {"type": "nodeGraph", "title": title, "description": desc,
            "targets": [{"datasource": DS, "expr": nodes, "refId": "A", "format": "table", "instant": True, "range": False},
                        {"datasource": DS, "expr": edges, "refId": "B", "format": "table", "instant": True, "range": False}],
            # The node graph finds its fields by name, which a rename does not
            # change (it sets the display name); a calculated field is named
            # by its alias.
            "transformations": [
                # timeSeries false: per frame, not one frame joined by time, which
                # would merge the nodes and the edges.
                {"id": "calculateField", "options": {"mode": "reduceRow", "reduce": {"reducer": "sum"},
                                                     "alias": "mainstat", "replaceFields": False, "timeSeries": False}},
                {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True,
                                                                 "Value #A": True, "Value #B": True}}}],
            "options": {"nodes": {"mainStatUnit": "ops"}, "edges": {"mainStatUnit": "Bps"}},
            "fieldConfig": {"defaults": {}, "overrides": [unit("A", "ops"), unit("B", "Bps")]}}


def replication(g, sel, pods):
    # "and" the pairs of the moment: a window looks back over a switchover,
    # and would draw the old pairs next to the new ones for its length.
    joined = lambda metric: (f"sum by (pod, slave_ip) (({metric}) and on (pod, slave_ip) "
                             f"redis_connected_slave_offset_bytes{{{sel}}}) "
                             f"* on (slave_ip) group_left (replica) {replicas_by_ip(pods)}")
    g.add(series("Replication throughput per replica",
                 [target(joined(f"deriv(redis_connected_slave_offset_bytes{{{sel}}}[$__rate_interval])"), "{{pod}} → {{replica}}")],
                 "Bps", desc="Bytes per second each replica takes from its primary: the write traffic of the primary"), 8, 8)
    g.add(series("Lag of every replica",
                 [target(joined(f"max_over_time((max by (pod) (redis_master_repl_offset{{{sel}}}) - on (pod) group_right "
                                f"redis_connected_slave_offset_bytes{{{sel}}})[1m:])"), "{{pod}} → {{replica}}")],
                 "bytes", desc="The most bytes of writes the replica had not acknowledged in each minute, as its "
                               "primary sees it. A replica acknowledges once a second, so up to a second of writes "
                               "is normal; a line that keeps growing is a replica that falls behind."), 8, 8)
    g.add({"type": "bargauge", "title": "Resyncs served in the time range",
           "description": "Full: the primary sent a whole snapshot (a new or a lost replica). Partial: a replica "
                          "caught up from the backlog, e.g. after a switchover.",
           "targets": [target(f"sum by (pod) (increase(redis_replica_resyncs_full{{{sel}}}[$__range]))", "{{pod}} full", instant=True),
                       target(f"sum by (pod) (increase(redis_replica_partial_resync_accepted{{{sel}}}[$__range]))", "{{pod}} partial", "B", instant=True)],
           "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
           "fieldConfig": {"defaults": {"min": 0, "decimals": 0, "color": {"mode": "palette-classic"}}, "overrides": []}}, 8, 8)


def disk(g, namespace, pvc_regex, sel):
    pvc = f'namespace="{namespace}",persistentvolumeclaim=~"{pvc_regex}"'
    used = f"max by (persistentvolumeclaim) (kubelet_volume_stats_used_bytes{{{pvc}}})"
    cap = f"max by (persistentvolumeclaim) (kubelet_volume_stats_capacity_bytes{{{pvc}}})"
    g.row("Disk")
    g.add({"type": "bargauge", "title": "Volumes used",
           "description": "Used space of every PVC, from the kubelet; orange from 70 %, red from 85 %",
           "targets": [target(f"100 * {used} / {cap}", "{{persistentvolumeclaim}}", instant=True)],
           "options": {"orientation": "horizontal", "displayMode": "lcd", "showUnfilled": True,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
           "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100, "color": {"mode": "thresholds"},
                                        "thresholds": {"mode": "absolute", "steps": [
                                            {"color": "green", "value": None}, {"color": "orange", "value": 70},
                                            {"color": "red", "value": 85}]}}, "overrides": []}}, 8, 8)
    g.add(series("Volume used over time", [target(used, "{{persistentvolumeclaim}}"),
                                           target(f"max({cap})", "capacity", "B")], "bytes"), 8, 8)
    g.add(series("AOF size", [target(f"max by (pod) (redis_aof_current_size_bytes{{{sel}}})", "{{pod}}")], "bytes",
                 desc="The append-only file grows with every write and shrinks at a rewrite"), 4, 8)
    g.add(stat("Since the last RDB save",
               [target(f"time() - max by (pod) (redis_rdb_last_save_timestamp_seconds{{{sel}}})", "{{pod}}", instant=True)],
               unit="s", text_mode="value_and_name",
               desc="How old the snapshot on the volume is; with persistence aof the AOF has the writes since"), 4, 8)


DECISIONS = ('{namespace="redis-operator-system", container="manager"} != `Moved slot` != `Could not configure` '
             '|~ `Started|Completed|Finished|Approved|Promot|Creating|Replacing|Switching|Deleting|Ran cluster|Updated Pod|'
             'Event occurred|Fenc|Demot|Recreat|Scal|Forget|Reconciler error` '
             '| regexp `^\\S+\\t(?P<level>\\S+)\\t(?:(?P<logger>events)\\t)?(?P<msg>[^\\t]+)\\t(?P<fields>\\{.*\\})$` '
             '| line_format `{{.fields}}` | json '
             '| label_format object=`{{ or .RedisInstance_name .RedisCluster_name .RedisUser_name .object_name }}`, '
             'what=`{{ or .controllerKind .kind }}`, '
             'where=`{{ or .RedisInstance_namespace .RedisCluster_namespace .RedisUser_namespace .object_namespace }}` '
             '| label_format decision=`{{ if .logger }}{{ .reason }}{{ else }}{{ .msg }}{{ end }}`, '
             'detail=`{{ if .logger }}{{ .note }}{{ else }}{{ if .outcome }}{{ .outcome }} {{ end }}{{ if .from }}{{ .from }} → {{ .to }} {{ end }}'
             '{{ if .epoch }}epoch {{ .epoch }} {{ end }}{{ if .durationMs }}{{ .durationMs }} ms {{ end }}'
             '{{ if .downMs }}primary down {{ .downMs }} ms {{ end }}{{ if .pod_extracted }}{{ .pod_extracted }} {{ end }}'
             '{{ if .command }}{{ .command }} on {{ .node }} {{ end }}{{ if .role }}{{ .name }} is {{ .role }} {{ end }}'
             '{{ if .reason }}{{ .reason }} {{ end }}'
             '{{ if and (not .role) (ne .name .object) }}{{ .name }} {{ end }}{{ if .error }}{{ .error }}{{ end }}{{ end }}` '
             '| keep what, where, object, decision, detail, level')
# The JSON field pod is pod_extracted: pod is the label of the stream, the
# Pod of the operator.


def decisions_table(expr):
    return {
        "type": "table", "title": "Every decision", "datasource": LOKI,
        "description": "One row per decision of the operator, from its log: switchovers and failovers with where "
                       "from and to, Pods it created or replaced, approvals of a restarted primary, cluster commands, "
                       "Events and errors. Blue: something starts, green: done, orange: an Event, red: an error.",
        "targets": [{"datasource": LOKI, "expr": expr, "refId": "A", "queryType": "range"}],
        "transformations": [
            {"id": "extractFields", "options": {"source": "labels"}},
            {"id": "organize", "options": {
                "excludeByName": {"labels": True, "Line": True, "tsNs": True, "id": True, "level": True},
                "indexByName": {"Time": 0, "what": 1, "where": 2, "object": 3, "decision": 4, "detail": 5},
                "renameByName": {"what": "Kind", "where": "Namespace", "object": "Object",
                                 "decision": "Decision", "detail": "Details"}}},
            {"id": "sortBy", "options": {"sort": [{"field": "Time", "desc": True}]}},
        ],
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": [
            {"matcher": {"id": "byName", "options": "Decision"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                {"id": "custom.width", "value": 260},
                {"id": "mappings", "value": [
                    {"type": "regex", "options": {"pattern": p, "result": {"color": c, "index": i}}}
                    for i, (p, c) in enumerate([("^(Reconciler error|.*Failed|.*Rejected|.*Blocked|Failover.*)$", "red"),
                                                ("^(Started.*|Switching.*|Creating.*|Replacing.*|Deleting.*|Ran cluster.*)$", "blue"),
                                                ("^(Completed.*|Finished.*|Approved.*|Updated.*|.*Completed|.*Balanced)$", "green"),
                                                (".*", "orange")])]}]},
            {"matcher": {"id": "byName", "options": "Time"}, "properties": [{"id": "custom.width", "value": 190}]},
            {"matcher": {"id": "byName", "options": "Kind"}, "properties": [{"id": "custom.width", "value": 130}]},
        ]},
    }


EVENTS_HELP = ("Every Event the operator emitted, counted, so even a state that lasted less than a "
               "scrape shows: a bar per reason at the moment it happened.")


# Commands of the operator, the Instance Manager and the exporter themselves.
INTERNAL_CMDS = ("acl.*|auth|client.*|cluster.*|command.*|config.*|eval.*|hello|info|latency.*|memory.*|ping|"
                 "psync|randomkey|replconf|replicaof|select|slowlog.*|type")
LINK = mappings({1: ("up", "green"), 0: ("down", "red")})


def redis_inside(g, sel, cluster=False):
    """What Redis itself reports through the exporter: errors, links, cache, latency, memory, forks."""
    g.row("Inside Redis")
    g.add(bars("Errors Redis answered, by error",
               [target("sum by (err) " + counted(f"redis_errors_total{{{sel}}}", "$__rate_interval"), "{{err}}")],
               desc="What clients got back: READONLY right after a switchover, NOREPLICAS with minReplicasToWrite, "
                    "WRONGPASS or NOPERM for users, MOVED and ASK while slots move, CLUSTERDOWN, BUSYKEY. "
                    "Commands of the operator count too (e.g. an ERR of a refused parameter)."), 16, 8)
    g.add(timeline("Link of every replica to its master",
                   [target(rows(f"redis_master_link_up{{{sel}}}", "pod"), "{{pod}}")], LINK,
                   desc="Red: the replica has lost its master (master_link_status:down), e.g. after the master "
                        "restarted on a new IP. A replica can be Ready while its link is down."), 8, 8)
    g.add(series("Cache hit ratio",
                 [target(f"sum by (pod) (rate(redis_keyspace_hits_total{{{sel}}}[$__rate_interval])) / "
                         f"(sum by (pod) (rate(redis_keyspace_hits_total{{{sel}}}[$__rate_interval])) + "
                         f"sum by (pod) (rate(redis_keyspace_misses_total{{{sel}}}[$__rate_interval])))", "{{pod}}")],
                 "percentunit", desc="Reads that found their key; empty while nothing reads"), 8, 8)
    g.add(series("Evicted and expired keys per second",
                 [target(f"sum by (pod) (rate(redis_evicted_keys_total{{{sel}}}[$__rate_interval]))", "{{pod}} evicted"),
                  target(f"sum by (pod) (rate(redis_expired_keys_total{{{sel}}}[$__rate_interval]))", "{{pod}} expired", "B")],
                 desc="Evicted: removed to stay below maxmemory (maxmemoryPolicy); with noeviction writes fail instead. "
                      "Expired: their TTL ran out"), 8, 8)
    g.add(series("Latency p99, by command",
                 [target(f'histogram_quantile(0.99, sum by (le, cmd) (rate(redis_commands_latencies_usec_bucket'
                         f'{{{sel},cmd!~"{INTERNAL_CMDS}"}}[$__rate_interval])))', "{{cmd}}")],
                 "µs", desc="Time Redis spent on a command, without the network; the commands of the operator and "
                            "the exporter are left out"), 8, 8)
    g.add(series("Slow log: new entries and the last one",
                 [target(f"sum by (pod) (increase(redis_slowlog_last_id{{{sel}}}[$__rate_interval]))", "{{pod}} new entries"),
                  target(f"max by (pod) (redis_last_slow_execution_duration_seconds{{{sel}}}) * 1000",
                         "{{pod}} last took (ms)", "B")],
                 desc="Commands slower than slowlog-log-slower-than (10 ms): how many were added when, and how long "
                      "the newest one took. The command and its arguments: SLOWLOG GET"), 6, 8)
    g.add(series("Memory fragmentation",
                 [target(f"max by (pod) (redis_mem_fragmentation_ratio{{{sel}}})", "{{pod}}")],
                 desc="RSS / used memory: about 1 to 1.5 is fine; above, memory is lost to fragmentation; below 1, swapping"),
          6, 8)
    g.add(series("Last fork took",
                 [target(f"max by (pod) (redis_latest_fork_seconds{{{sel}}})", "{{pod}}")], "s",
                 desc="Redis stops while it forks for an RDB save, an AOF rewrite or a full resync; grows with the dataset"),
          6, 8)
    g.add(series("Saving in the background",
                 [target(f"max by (pod) (redis_rdb_bgsave_in_progress{{{sel}}})", "{{pod}} RDB"),
                  target(f"max by (pod) (redis_aof_rewrite_in_progress{{{sel}}})", "{{pod}} AOF rewrite", "B")],
                 stack=True, desc="1 while an RDB save or an AOF rewrite runs"), 6, 8)
    g.add(series("Rejected connections and blocked clients",
                 [target("sum by (pod) " + counted(f"redis_rejected_connections_total{{{sel}}}", "$__rate_interval"), "{{pod}} rejected"),
                  target(f"max by (pod) (redis_blocked_clients{{{sel}}})", "{{pod}} blocked", "B")],
                 desc="Rejected: over maxclients. Blocked: clients waiting in BLPOP, XREAD BLOCK, WAIT"), 12, 8)
    if cluster:
        g.add(timeline("cluster_state, seen by every node",
                       [target(rows(f"redis_cluster_state{{{sel}}}", "pod"), "{{pod}}")],
                       mappings({1: ("ok", "green"), 0: ("fail", "red")}),
                       desc="Every node judges the cluster on its own: one red row is a node cut off from the others"),
              12, 8)
        g.add(series("Slots in trouble and nodes known, seen by every node",
                      [target(f"max by (pod) (redis_cluster_slots_pfail{{{sel}}})", "{{pod}} slots pfail"),
                       target(f"max by (pod) (redis_cluster_slots_fail{{{sel}}})", "{{pod}} slots fail", "B"),
                       target(f"max by (pod) (redis_cluster_known_nodes{{{sel}}})", "{{pod}} known nodes", "C")],
                      desc="pfail: a node thinks the master of the slots is down; fail: the masters agree. Known nodes "
                           "above the nodes that run: an ID not forgotten yet"), 12, 8)
    else:
        g.add(series("Connected replicas of the primary",
                      [target(f"max by (namespace, pod) (redis_connected_slaves{{{sel}}}) and on (namespace, pod) "
                              f"(redis_operator_instance_pod_role{{{sel}}} == 1)", "{{pod}}")],
                      desc="Replicas the primary streams to; below spec.instances - 1 a replica is down or syncing"), 12, 8)


def key_types(g, sel):
    """The sample of keys.lua: share of keys by type and memory. The Pods of a
    shard hold the same keys, so they are averaged, and the shards added up."""
    sample = f"avg by (redis_instance, key) (redis_script_values{{{sel}}})"
    share = f"sum by (key) ({sample}) / scalar(sum({sample}))"
    by_type = (f'sum by (type) (label_replace({sample}, "type", "$1", "key", "(\\\\S+) .*")) / scalar(sum({sample}))')
    g.row("Keys by type and size")
    g.add({"type": "bargauge", "title": "Keys by type and memory, now", "datasource": DS,
           "description": "Every Pod samples 200 random keys at every scrape (RANDOMKEY, TYPE, MEMORY USAGE) "
                          "and counts them by type and memory: a share of the keys, not exact. A rare type or a few big "
                          "keys may not show; redis-cli --bigkeys finds those.",
           "targets": [target(share, "{{key}}", instant=True)],
           "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
           "fieldConfig": {"defaults": {"unit": "percentunit", "min": 0, "max": 1, "decimals": 1,
                                        "color": {"mode": "palette-classic"}}, "overrides": []}}, 10, 10)
    g.add(series("Keys by type over time", [target(by_type, "{{type}}")], "percentunit", stack=True,
                 desc="Share of the sampled keys by type; with Keys it tells how many of each"), 14, 10)



def pressure(g, sel, pods, roles, cluster=False):
    """Where a node runs out of something under load: its main thread, CPU,
    memory, network, buffers and backlog; and failovers it did not ask for.
    pods: a regex of the Pods; roles: redis_operator_instance_pod_role of them."""
    k8s = f'namespace="$namespace",pod=~"{pods}",container="redis"'
    masters = f"and on (namespace, pod) ({roles} == 1)"
    ri = "$__rate_interval"
    g.row("Load and pressure")
    g.add(series("Main thread busy",
                 [target(f"sum by (pod) (rate(redis_cpu_user_main_thread_seconds_total{{{sel}}}[{ri}]) + "
                         f"rate(redis_cpu_sys_main_thread_seconds_total{{{sel}}}[{ri}]))", "{{pod}}")],
                 "percentunit", desc="Redis runs commands on one thread: at 100 % a node can do no more, however many "
                                     "cores the machine has. Add shards, not CPUs."), 12, 8)
    g.add(series("CPU throttled by the limit",
                 [target(f"sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{{{k8s}}}[{ri}])) / "
                         f"sum by (pod) (rate(container_cpu_cfs_periods_total{{{k8s}}}[{ri}]))", "{{pod}}")],
                 "percentunit", desc="Share of the time the kernel held Redis back for its CPU limit: latency spikes "
                                     "even when the average looks fine. Empty without a CPU limit."), 12, 8)
    g.add(series("Commands per second, every master",
                 [target(f"sum by (redis_instance) (rate(redis_commands_processed_total{{{sel}}}[{ri}]) {masters})",
                         "{{redis_instance}}")], "ops",
                 desc="Lines far apart: a hot shard, with hot keys or more slots; Redis 7.2 has no numbers per slot"),
          18 if cluster else 12, 8)
    if cluster:
        g.add(series("Busiest master / average",
                     [target(f"max(sum by (redis_instance) (rate(redis_commands_processed_total{{{sel}}}[{ri}]) {masters})) / "
                             f"avg(sum by (redis_instance) (rate(redis_commands_processed_total{{{sel}}}[{ri}]) {masters}))",
                             "skew")],
                     desc="1: the load is even; 2: one shard does twice the average and reaches its ceiling first"), 6, 8)
    g.add(series("Memory of every master, of maxmemory",
                 [target(f"max by (namespace, pod) (redis_memory_used_bytes{{{sel}}}) / "
                         f"max by (namespace, pod) (redis_memory_max_bytes{{{sel}}}) {masters}", "{{pod}}")],
                 "percentunit", desc="At 100 %: with noeviction writes fail with OOM (see the errors), with an "
                                     "allkeys-* policy keys are evicted"), 12 if cluster else 12, 8)
    g.add(series("Container memory, of its limit",
                 [target(f"max by (pod) (container_memory_working_set_bytes{{{k8s}}}) / "
                         f"max by (pod) (kube_pod_container_resource_limits{{{k8s},resource=\"memory\"}})", "{{pod}}")],
                 "percentunit", desc="maxmemory plus the copy of a fork (BGSAVE, AOF rewrite, full resync), client and "
                                     "replica buffers: at 100 % the kernel OOMKills Redis"), 12, 8)
    g.add(series("Network of every node",
                 [target(f"sum by (pod) (rate(redis_net_input_bytes_total{{{sel}}}[{ri}]))", "{{pod}} in"),
                  target(f"sum by (pod) (rate(redis_net_output_bytes_total{{{sel}}}[{ri}]))", "{{pod}} out", "B")],
                 "Bps", desc="Clients, replication and MIGRATE; the cluster bus has its own port"), 12, 8)
    g.add(series("Output buffers",
                 [target(f"max by (pod) (redis_mem_clients_slaves{{{sel}}})", "{{pod}} to replicas"),
                  target(f"max by (pod) (redis_mem_clients_normal{{{sel}}})", "{{pod}} to clients", "B"),
                  target(f'max(redis_config_client_output_buffer_limit_bytes{{{sel},class="slave",limit="soft"}})',
                         "replica soft limit", "C")],
                 "bytes", desc="A replica over the soft limit for a minute, or over the hard one, is dropped and "
                               "comes back with a full resync, which under load may repeat. The hard limit (256 MB "
                               "by default) may be above the memory limit of the Pod."), 12, 8)
    g.add(series("Seconds of writes the backlog holds",
                 [target(f"max by (namespace, pod) (redis_config_repl_backlog_size{{{sel}}}) / "
                         f"(max by (namespace, pod) (deriv(redis_master_repl_offset{{{sel}}}[1m])) > 0) {masters}",
                         "{{pod}}")],
                 "s", desc="A replica cut off for longer than this needs a full resync instead of a partial one. "
                           "Seconds under heavy writes: raise repl-backlog-size in spec.redis.parameters."), 12, 8)
    g.add(bars("Changes of the master",
               [target(f"sum by (redis_instance) (changes(max by (redis_instance, pod) ({roles})[{ri}:5s])) / 2",
                       "{{redis_instance}}")],
               desc=("A bar without a red marker (Failover of a shard) is a failover Redis Cluster made on its own: a "
                     "master that did not answer for cluster-node-timeout, e.g. busy or forking under load."
                     if cluster else "Every switchover or failover; the markers say which")), 12, 8)
    stalls = series("Stalls, against cluster-node-timeout" if cluster else "Stalls",
                    [target(f"max by (pod) (redis_latest_fork_seconds{{{sel}}})", "{{pod}} last fork"),
                     target(f"max by (pod, event_name) (redis_latency_spike_duration_seconds{{{sel}}})",
                            "{{pod}} {{event_name}}", "B")]
                    + ([target("vector(15)", "cluster-node-timeout", "C")] if cluster else []),
                    "s", desc="How long Redis stopped: the last fork, and the latest spike of every kind from LATENCY "
                              "LATEST (set latency-monitor-threshold, e.g. 100 ms, in spec.redis.parameters). "
                              + ("A stall near cluster-node-timeout lets the other masters fail the node over." if cluster else ""))
    stalls["fieldConfig"]["defaults"]["custom"]["scaleDistribution"] = {"type": "log", "log": 10}
    g.add(stalls, 12, 8)
    g.add(stat("Restarts in the time range",
               [target(f"sum by (pod) {counted(f'kube_pod_container_status_restarts_total{{{k8s}}}', '$__range')}",
                       "{{pod}}", instant=True)],
               text_mode="value_and_name", desc="Of the redis container; kubectl describe pod tells OOMKilled"), 24, 4)


LOAD_TEST = "Load test"


def load_annotation():
    return annotation(LOAD_TEST, 'max(kube_job_status_active{namespace="$namespace",job_name=~".*load.*"}) > 0',
                      "A Job with load in its name runs", "yellow")


FIRING = mappings({1: ("firing", "red")})


def firing(filters):
    """ALERTS that fire, of any of the filters (label matchers), once per alert name."""
    return " or ".join(f'ALERTS{{alertstate="firing",{f}}}' for f in filters)


def alerts(g, filters, desc="The alerts of config/monitoring/prometheusrule.yaml that fire now or fired in the range"):
    f = firing(filters)
    # As rows(), with a subquery, as the alerts are an expression, not one series.
    expr = f"max by (alertname) ({f}) or (max by (alertname) (max_over_time(({f})[$__range:15s])) * 0 - 1)"
    g.add(timeline("Alerts", [target(expr, "{{alertname}}")], FIRING,
                   desc=desc + "; hover a bar for the labels. Nothing here: none fired."), 24, 4)


def alert_annotation(filters):
    return annotation("Alerts", f"max by (alertname, severity) ({firing(filters)})", "{{alertname}} ({{severity}})", "red")


def instance_dashboard():
    sel = 'namespace="$namespace",redis_instance="$instance"'
    obj = 'namespace="$namespace",kind="RedisInstance",name="$instance"'
    g = Grid()
    g.add(text(
        "**How to read it.** The top row is now. Below, every timeline is one row per Pod or per reason, "
        "coloured over time; select a range on any graph with the mouse to replay what happened in it "
        "(a switchover, a failover, a rolling update), and read it to the millisecond in *What happened*. "
        "Vertical markers on every graph come from the log of the operator: **blue** a switchover, **red** a "
        "failover, **purple** a rolling update, **orange** another Event; the switches at the top name them, "
        "hover a marker for its text."), 24, 3)
    g.add(stat("Primary", [target(f"max by (pod) (redis_operator_instance_pod_role{{{sel}}}) == 1", "{{pod}}", instant=True)],
               text_mode="name", color_mode="background",
               desc="The Pod in status.currentPrimary: the one -rw sends clients to"), 5, 4)
    g.add(stat("Primary epoch", [target(f"max(redis_operator_instance_primary_epoch{{{sel}}})", instant=True)],
               desc="Grows by one with every switchover and failover"), 3, 4)
    g.add(stat("Ready instances", [target(f"max(redis_operator_instance_ready_instances{{{sel}}})", "ready", instant=True),
                                   target(f"max(redis_operator_instance_instances{{{sel}}})", "wanted", "B", instant=True)],
               text_mode="value_and_name", desc="Ready Pods of status.readyInstances, and spec.instances"), 4, 4)
    g.add(stat("Ready", [target(f'max by (reason) (redis_operator_condition{{{obj},type="Ready"}})', "{{reason}}", instant=True)],
               text_mode="name", color_mode="background", maps=TRUE_FALSE,
               desc="The condition Ready: green with reason Ready, red with the reason why not"), 12, 4)

    alerts(g, ['namespace="$namespace",name="$instance"', 'namespace="$namespace",redis_instance="$instance"',
            'namespace="$namespace",persistentvolumeclaim=~"$instance-[0-9]+-data"'])
    operations(g, "$instance", "this instance")
    g.row("What happened")
    g.add(logs("What happened, to the millisecond",
               [operator_logs("$instance"),
                '{namespace="$namespace", pod=~"$instance-[0-9]+"} != `Could not configure` '
                '| line_format `{{.pod}} {{.container}}  {{__line__}}`'],
               "The log of the operator about this instance and the logs of its Pods (Redis, the Instance Manager, "
               "the exporter), in order. Select the time of a switchover or failover on a graph to read it step by "
               "step: the fence, the promotion, REPLICAOF, the partial resync."), 24, 12)
    g.add(timeline("Role of every Pod", [target(rows(f"redis_operator_instance_pod_role{{{sel}}}", "pod"), "{{pod}}")], ROLE,
                   desc="Green: the primary. Orange: the replica the operator is promoting (status.targetPrimary). "
                        "The green bar jumping rows is a switchover or a failover."), 24, 6)
    g.add(timeline("Pod Ready (kubelet)",
                   [target(rows(f'kube_pod_status_ready{{namespace="$namespace",pod=~"$instance-[0-9]+",condition="true"}}', "pod"), "{{pod}}")],
                   mappings({1: ("Ready", "green"), 0: ("not Ready", "red")}),
                   desc="Red while a Pod restarts, or a replica has not finished its first sync"), 24, 5)
    g.add(conditions("Conditions: one row each, the text is the reason",
                      f"redis_operator_condition{{{obj}}}",
                      "Every condition of the RedisInstance on one row; the bar shows its reason over time. "
                      "Green: fine (Ready, Applied, Issued). Grey: idle (Completed). Orange: something runs "
                      "(InProgress, RollingUpdate, Switchover). Red: a problem (PrimaryUnavailable, Failed, ...)."), 24, 10)
    g.row("Traffic and replication")
    ipods = pods_of('namespace="$namespace"', "$instance-[0-9]+")
    g.add(topology(sel, ipods, "Replication: who replicates from whom, and where it runs",
                   "Every Pod with the machine it runs on and its commands per second (green the primary, blue a "
                   "replica); every arrow a replica, with the bytes per second it replicates."), 24, 10)
    replication(g, sel, ipods)
    g.add(series("Commands per second",
                 [target(f"sum by (pod) (rate(redis_commands_processed_total{{{sel}}}[$__rate_interval]))", "{{pod}}")],
                 "ops", desc="A dip on the primary is the pause of a switchover or the gap of a failover"), 12, 8)
    g.add(series("Keys", [target(f'sum by (pod) (redis_db_keys{{{sel}}})', "{{pod}}")],
                 desc="Equal on every Pod once the replicas are in sync"), 8, 8)
    g.add(series("Connected clients", [target(f"max by (pod) (redis_connected_clients{{{sel}}})", "{{pod}}")]), 8, 8)
    g.add(series("Memory", [target(f"max by (pod) (redis_memory_used_bytes{{{sel}}})", "{{pod}} used"),
                            target(f'max(redis_memory_max_bytes{{{sel}}})', "maxmemory", "B")], "bytes"), 8, 8)

    pressure(g, sel, "$instance-[0-9]+", f"redis_operator_instance_pod_role{{{sel}}}")
    redis_inside(g, sel)
    key_types(g, sel)
    disk(g, "$namespace", "$instance-[0-9]+-data", sel)
    g.row("Volumes")
    g.add(state_rows("State of every volume", f"max by (ordinal, state) (redis_operator_instance_volume{{{sel}}})",
                     "ordinal", "volume ${__field.labels.ordinal}", "state",
                     [("^Ready$", "green"), ("^Detached$", "#5a6270"), ("^Terminating$", "orange"), (".*", "red")],
                     "status.volumes, a row per instance: Ready green; Terminating orange (the PVC is being deleted, the "
                     "operator moves the replica to a new one); Missing or Lost red; Detached grey (scaled down). A "
                     "state of a few seconds may fall between two scrapes: the table of operations has every volume "
                     "replaced."), 24, 5)
    variables = [
        variable("namespace", "Namespace", "label_values(redis_operator_instance_instances, namespace)"),
        variable("instance", "RedisInstance",
                 'label_values(redis_operator_instance_instances{namespace="$namespace"}, redis_instance)'),
    ]
    annotations = [
        loki_annotation("Switchover", "$instance", "Started switchover|reason\": \"Switchover", "blue"),
        loki_annotation("Failover", "$instance", "Started failover|reason\": \"Failover", "red"),
        loki_annotation("Rolling update", "$instance", "reason\": \"RollingUpdate", "purple"),
        alert_annotation(['namespace="$namespace",name="$instance"', 'namespace="$namespace",redis_instance="$instance"',
            'namespace="$namespace",persistentvolumeclaim=~"$instance-[0-9]+-data"']),
        load_annotation(),
        loki_annotation("Other events", "$instance",
                        "reason\": \"(Primary|Replica|Volume|Recreate|Scale|Memory|Foreign|Instance|Parameters|Invalid|CA)", "orange"),
    ]
    return dashboard("redis-operator-instance", "Redis Operator / RedisInstance",
                     "Roles, conditions, Events and replication of a RedisInstance", variables, g, annotations)


def cluster_dashboard():
    sel = 'namespace="$namespace",redis_cluster="$cluster"'
    cond = 'namespace="$namespace",kind="RedisCluster",name="$cluster"'
    events = 'namespace="$namespace",name=~"$cluster|$cluster-[0-9]+"'
    shards = mappings({i: (f"shard {i}", c) for i, c in enumerate(SHARD_COLORS)} | {-1: ("none", "transparent")})
    g = Grid()
    g.add(text(
        "**How to read it.** Bars and stats on top are now. The slot map has a row per 256 slots, coloured by the "
        "shard that serves them: during a resharding the colours change row by row. Select a range on any graph "
        "with the mouse to replay it. Markers: **purple** an Event of the resharding (start, wait, end), **red** a "
        "failover or switchover of a shard, **orange** shading while slots move; the switches at the top name them. "
        "*Slots moved* counts every slot, however fast it went."), 24, 3)
    g.add(stat("Ready", [target(f'max by (reason) (redis_operator_condition{{{cond},type="Ready"}})', "{{reason}}", instant=True)],
               text_mode="name", color_mode="background", maps=TRUE_FALSE), 6, 4)
    g.add(stat("Migrating", [target(f'max by (reason) (redis_operator_condition{{{cond},type="Migrating"}})', "{{reason}}", instant=True)],
               text_mode="name", color_mode="background",
               maps=mappings({1: ("", "orange"), 0: ("", "green")}),
               desc="Moving or Waiting (orange), Blocked or Balanced (green: nothing moves)"), 6, 4)
    g.add(stat("Slots to move", [target(f"max(redis_operator_cluster_slots_to_move{{{sel}}})", instant=True)],
               desc="status.slotMigration.slotsToMove: 0 once every shard serves its share"), 6, 4)
    g.add(stat("Shards", [target(f"count(max by (shard) (redis_operator_cluster_shard_slots{{{sel}}}))", instant=True)],
               desc="Shards that exist, also one on its way out"), 6, 4)

    alerts(g, ['namespace="$namespace",name=~"$cluster(-[0-9]+)?"', 'namespace="$namespace",redis_cluster="$cluster"',
           'namespace="$namespace",redis_instance=~"$cluster-[0-9]+"',
           'namespace="$namespace",persistentvolumeclaim=~"$cluster-[0-9]+-[0-9]+-data"'])
    experiment_runs(g)
    g.row("Resharding")
    g.add({"type": "bargauge", "title": "Slots per shard now", "datasource": DS,
           "description": "16384 slots split between the shards; equal when balanced",
           "targets": [target(f"max by (shard) (redis_operator_cluster_shard_slots{{{sel}}})", "shard {{shard}}", instant=True)],
           "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
           "fieldConfig": {"defaults": {"min": 0, "max": 8192, "color": {"mode": "palette-classic"}}, "overrides": []}},
          8, 8)
    g.add(series("Slots per shard over time",
                 [target(f"max by (shard) (redis_operator_cluster_shard_slots{{{sel}}})", "shard {{shard}}")],
                 desc="The lines meet at 16384 / shards when the resharding is done"), 16, 8)
    g.add(bars("Slots moved, from shard to shard",
               [target("sum by (from_shard, to_shard) " + counted(f"redis_operator_slots_moved_total{{{sel}}}", "$__rate_interval"),
                       "{{from_shard}} → {{to_shard}}")],
               desc="Every slot the operator moved, counted, so none is missed between two scrapes"), 16, 8)
    g.add(stat("Slots moved in the time range",
               [target("sum by (from_shard, to_shard) " + counted(f"redis_operator_slots_moved_total{{{sel}}}", "$__range"),
                       "{{from_shard}} → {{to_shard}}", instant=True)],
               text_mode="value_and_name"), 8, 8)
    moved = ('{namespace="redis-operator-system", container="manager"} |= `Moved slot` '
             '|= `"RedisCluster": {"name":"$cluster","namespace":"$namespace"}`')
    per_slot = series("Time per moved slot (from the log of the operator)",
                      [{"datasource": LOKI, "refId": "A", "queryType": "range", "legendFormat": "ms per slot",
                        "expr": f"1000 / sum(rate({moved} [1m]))"}], "ms",
                      desc="1 s divided by the \"Moved slot\" lines of the last minute: everything one slot costs "
                           "(MIGRATE of its keys, SETSLOT on both nodes, the status of every node read after it). "
                           "Compare with the keys and bytes per slot on the right.")
    per_slot["datasource"] = LOKI
    g.add(per_slot, 12, 8)
    g.add(series("Keys and bytes per slot (average of the cluster)",
                 [target(f'sum(redis_db_keys{{{sel},role="primary"}}) / 16384', "keys per slot"),
                  target(f'sum(redis_memory_used_dataset_bytes{{{sel},role="primary"}}) / 16384 / 1024', "KiB per slot", "B")],
                 desc="All keys (and dataset memory) of the masters over 16384: what an average slot carries. "
                      "A slot of its own may hold more or less."), 12, 8)
    g.add(logs("Every moved slot, to the millisecond",
               [moved + ' | regexp `"slot": (?P<slot>\\d+), "from": "(?P<from>[^"]+)", "to": "(?P<to>[^"]+)"` '
                        '| line_format `slot {{.slot}}  {{.from}} → {{.to}}`'],
               "One line per slot, the time it was handed over: the gap to the line before is what that slot "
               "cost. The node log below leaves these lines out."), 24, 10)
    migrate = f'{sel},cmd="migrate"'
    g.add(series("MIGRATE: time per call, on the sources",
                 [target(f"1000 * sum by (pod) (rate(redis_commands_duration_seconds_total{{{migrate}}}[$__rate_interval])) "
                         f"/ sum by (pod) (rate(redis_commands_total{{{migrate}}}[$__rate_interval]))", "{{pod}} mean"),
                  target(f"histogram_quantile(0.99, sum by (le, pod) (rate(redis_commands_latencies_usec_bucket"
                         f"{{{migrate}}}[$__rate_interval]))) / 1000", "{{pod}} p99", "B")],
                 "ms", desc="One MIGRATE moves up to 100 keys and blocks the main thread of the source meanwhile: "
                            "clients of that node wait as long. Mean from commandstats; p99 from Redis' latency "
                            "histogram (buckets are powers of 2, so the p99 steps). Every call counts, not only "
                            "those above slowlog-log-slower-than."), 12, 8)
    g.add(series("MIGRATE calls per second, on the sources",
                 [target(f"sum by (pod) (rate(redis_commands_total{{{migrate}}}[$__rate_interval]))", "{{pod}}")],
                 "ops", desc="Calls per second; keys per second are at most 100 times more"), 12, 8)
    g.add(conditions("Conditions: one row each, the text is the reason",
                      f"redis_operator_condition{{{cond}}}",
                      "Ready, SlotsCovered and Migrating, one row each; the bar shows the reason over time. "
                      "Green: fine. Grey: idle (Balanced). Orange: slots move or wait (Moving, Waiting, Resharding). "
                      "Red: a problem (Blocked, SlotsUncovered, ...)."), 24, 5)
    g.row("Slot map")
    g.add(timeline("Which shard serves the slots (a row per 256 slots)",
                   [target(f"max by (block) (redis_operator_cluster_slot_block_shard{{{sel}}})", "{{block}}")],
                   shards, show_values="never",
                   desc="Every row is a block of 256 slots, coloured by the shard that serves most of them"), 24, 24)

    operations(g, "$cluster-[0-9]+", "the masters of the shards (by the operator; Redis Cluster fails over on its own too)")
    g.row("Replication")
    cpods = pods_of('namespace="$namespace"', "$cluster-[0-9]+-[0-9]+")
    g.add(topology(sel, cpods, "Replication: masters, replicas and where they run",
                   "Every node with its machine and commands per second (green a master, blue a replica); every "
                   "arrow a replica, with the bytes per second it replicates."), 24, 12)
    replication(g, sel, cpods)
    g.add({"type": "bargauge", "title": "Masters per machine", "datasource": DS,
           "description": "More than one master on a machine: losing it takes them all at once, and with them a "
                          "majority of the cluster (alert RedisClusterMastersOnOneMachine). podAntiAffinityType keeps "
                          "the Pods of one shard apart, not the masters of different shards.",
           "targets": [target(f"count by (node) (max by (pod) (redis_operator_cluster_shard_primary{{{sel}}} == 1) "
                              f'* on (pod) group_left (node) max by (pod, node) (kube_pod_info{{namespace="$namespace"}}))',
                              "{{node}}", instant=True)],
           "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
           "fieldConfig": {"defaults": {"min": 0, "decimals": 0, "color": {"mode": "thresholds"},
                                        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                                                     {"color": "red", "value": 2}]}},
                           "overrides": []}}, 24, 5)
    pressure(g, sel, "$cluster-[0-9]+-[0-9]+",
             'redis_operator_instance_pod_role{namespace="$namespace",redis_instance=~"$cluster-[0-9]+"}', cluster=True)
    redis_inside(g, sel, cluster=True)
    key_types(g, sel)
    disk(g, "$namespace", "$cluster-[0-9]+-[0-9]+-data", sel)
    g.row("Nodes")
    g.add(logs("What happened, to the millisecond",
               [operator_logs("$cluster(-[0-9]+)?")],
               "The log of the operator about the cluster and its shards, without the line of every moved slot: "
               "resharding, failovers, nodes that join or are forgotten."), 24, 10)
    g.add(timeline("Role of every node",
                   [target(rows(f'redis_operator_instance_pod_role{{namespace="$namespace",redis_instance=~"$cluster-[0-9]+"}}', "pod"), "{{pod}}")],
                   ROLE, desc="Green: the master of its shard. A green bar jumping rows is a failover."), 24, 10)
    g.add(series("Commands per second per shard",
                 [target(f"sum by (redis_instance) (rate(redis_commands_processed_total{{{sel}}}[$__rate_interval]))",
                         "{{redis_instance}}")], "ops"), 12, 8)
    g.add(series("Keys per shard (masters)",
                 [target(f'sum by (redis_instance) (redis_db_keys{{{sel},role="primary"}})', "{{redis_instance}}")],
                 stack=True, desc="Keys follow their slots during a resharding"), 12, 8)
    variables = [
        variable("namespace", "Namespace", "label_values(redis_operator_cluster_slots_to_move, namespace)"),
        variable("cluster", "RedisCluster",
                 'label_values(redis_operator_cluster_slots_to_move{namespace="$namespace"}, redis_cluster)'),
    ]
    annotations = [
        loki_annotation("Resharding", "$cluster", "reason\": \"Resharding", "purple"),
        loki_annotation("Failover of a shard", "$cluster-[0-9]+", "Started (cluster )?failover|reason\": \"(Switchover|Failover)", "red"),
        alert_annotation(['namespace="$namespace",name=~"$cluster(-[0-9]+)?"', 'namespace="$namespace",redis_cluster="$cluster"',
           'namespace="$namespace",redis_instance=~"$cluster-[0-9]+"',
           'namespace="$namespace",persistentvolumeclaim=~"$cluster-[0-9]+-[0-9]+-data"']),
        load_annotation(),
        annotation("Slots move", f'max(redis_operator_condition{{{cond},type="Migrating",reason="Moving"}}) == 1',
                   "Slots move", "orange"),
        experiment_annotation(),
    ]
    return dashboard("redis-operator-cluster", "Redis Operator / RedisCluster",
                     "Slots, shards, masters and Events of a RedisCluster", variables, g, annotations)


def operator_dashboard():
    job = 'job=~"$job"'
    rate = lambda m, by, extra="": f"sum by ({by}) (rate({m}{{{job}{extra}}}[$__rate_interval]))"
    q = lambda e, legend, ref="A", instant=False: target(e, legend, ref, instant)
    g = Grid()
    g.add(text(
        "**How to read it.** The graph is the operator now: an arrow to every RedisInstance, RedisCluster and "
        "RedisUser with the calls to the Kubernetes API its reconciles made per minute, and an arrow to the "
        "Instance Managers of every instance with the HTTP calls per minute; a node shows the reconciles per "
        "minute. Below: the controllers of controller-runtime, their workqueues, every object, the calls to the "
        "API in time, the calls to the Instance Managers and the decisions."), 24, 3)
    g.add(stat("Leader", [q(f"max(leader_election_master_status{{{job}}})", "", instant=True)],
               maps=mappings({1: ("leading", "green"), 0: ("standby", "orange")}), color_mode="background",
               desc="This replica of the operator holds the leader lease and runs the controllers"), 4, 4)
    g.add(stat("Reconciles per second", [q(f"sum(rate(controller_runtime_reconcile_total{{{job}}}[$__rate_interval]))", "", instant=True)],
               unit="reqps"), 4, 4)
    g.add(stat("Errors per second", [q(f"sum(rate(controller_runtime_reconcile_errors_total{{{job}}}[$__rate_interval]))", "", instant=True)],
               unit="reqps", desc="Reconciles that returned an error and are retried with backoff"), 4, 4)
    g.add(stat("Queued", [q(f"sum(workqueue_depth{{{job}}})", "", instant=True)],
               desc="Objects waiting in the workqueues of all controllers"), 4, 4)
    g.add(stat("Memory", [q(f"sum(process_resident_memory_bytes{{{job}}})", "", instant=True)], unit="bytes"), 4, 4)
    g.add(stat("Goroutines", [q(f"sum(go_goroutines{{{job}}})", "", instant=True)]), 4, 4)

    # The graph: the operator, every object, the Instance Managers of every instance.
    def lbl(e, **labels):
        for k, v in labels.items():
            e = f'label_replace({e}, "{k}", "{v}", "", "")'
        return e
    per_min = lambda m: f"60 * sum by (kind, namespace, name) (rate({m}[$__rate_interval]))"
    operator = lbl(f"60 * sum(rate(controller_runtime_reconcile_total{{{job}}}[$__rate_interval]))",
                   id="operator", title="redis-operator", subtitle="operator", color="purple")
    objects = per_min('redis_operator_reconcile_total{kind!=""}')
    objects = f'label_join(label_join(label_replace({objects}, "title", "$1", "name", "(.*)"), "id", "/", "kind", "namespace", "name"), "subtitle", " · ", "kind", "namespace")'
    for kind, color in [("RedisInstance", "green"), ("RedisCluster", "orange"), ("RedisUser", "blue")]:
        objects = f'label_replace({objects}, "color", "{color}", "kind", "{kind}")'
    im = "60 * sum by (namespace, redis_instance) (rate(redis_operator_im_requests_total[$__rate_interval]))"
    im_nodes = (f'label_replace(label_replace(label_join(label_join({im}, "id", "/", "namespace", "redis_instance"), '
                f'"title", " ", "redis_instance"), "subtitle", "Instance Managers", "", ""), "color", "#8e8e8e", "", "")')
    im_nodes = f'label_replace({im_nodes}, "id", "im/$1", "id", "(.*)")'
    nodes = f"sum by (id, title, subtitle, color) ({operator} or {objects} or {im_nodes})"
    api_edges = per_min('redis_operator_api_requests_total{kind!=""}')
    api_edges = f'label_join({api_edges}, "target", "/", "kind", "namespace", "name")'
    im_edges = f'label_replace(label_join({im}, "target", "/", "namespace", "redis_instance"), "target", "im/$1", "target", "(.*)")'
    edges = f'label_replace(({api_edges}) or ({im_edges}), "source", "operator", "", "")'
    edges = f'sum by (id, source, target) (label_join({edges}, "id", "->", "source", "target"))'
    graph = topology_panel("How the operator talks to its objects", nodes, edges,
                           "Purple: the operator. Green: RedisInstances, orange: RedisClusters, blue: RedisUsers, with "
                           "their reconciles per minute. Grey: the Instance Managers of an instance, which the operator "
                           "calls over HTTP. Hover an arrow: calls per minute, to the Kubernetes API for an object, over "
                           "HTTP for Instance Managers.", "suffix: rec/min", "suffix: calls/min")
    alerts(g, ['alertname=~"Redis.*"'], "Every alert of config/monitoring/prometheusrule.yaml, of every object")
    g.add(graph, 24, 20)

    g.row("Controllers (controller-runtime)")
    g.add(series("Reconciles per second, by controller and result",
                 [q(rate("controller_runtime_reconcile_total", "controller, result"), "{{controller}} {{result}}")],
                 "reqps", stack=True,
                 desc="success: done; requeue_after: done, look again later (e.g. every 30 s, or 1 s while slots "
                      "move); requeue: again at once; error: failed, retried with backoff"), 12, 8)
    g.add(series("Reconcile duration, by controller",
                 [q(f"histogram_quantile(0.5, sum by (le, controller) (rate(controller_runtime_reconcile_time_seconds_bucket{{{job}}}[$__rate_interval])))", "{{controller}} p50"),
                  q(f"histogram_quantile(0.95, sum by (le, controller) (rate(controller_runtime_reconcile_time_seconds_bucket{{{job}}}[$__rate_interval])))", "{{controller}} p95", "B"),
                  q(f"histogram_quantile(0.99, sum by (le, controller) (rate(controller_runtime_reconcile_time_seconds_bucket{{{job}}}[$__rate_interval])))", "{{controller}} p99", "C")],
                 "s", desc="A reconcile of a RedisCluster moving slots runs up to 5 s on purpose"), 12, 8)
    g.add(series("Errors per second, by controller",
                 [q(rate("controller_runtime_reconcile_errors_total", "controller"), "{{controller}}")], "reqps",
                 desc="Most are conflicts of a status update with a newer version, retried at once"), 8, 7)
    g.add(series("Workers busy, of the maximum",
                 [q(f"sum by (controller) (controller_runtime_active_workers{{{job}}})", "{{controller}} busy"),
                  q(f"sum by (controller) (controller_runtime_max_concurrent_reconciles{{{job}}})", "{{controller}} max", "B")],
                 desc="Reconciles running at once; one worker per controller, so a long one makes the others wait"), 8, 7)
    g.add(series("Webhook requests per second",
                 [q(rate("controller_runtime_webhook_requests_total", "webhook, code"), "{{webhook}} {{code}}")], "reqps",
                 desc="The validating webhook of RedisUser, by HTTP status"), 8, 7)

    g.row("Workqueues")
    g.add(series("Depth", [q(f"sum by (name) (workqueue_depth{{{job}}})", "{{name}}")],
                 desc="Objects waiting for a worker"), 6, 7)
    g.add(series("Adds and retries per second", [q(rate("workqueue_adds_total", "name"), "{{name}} adds"),
                                                q(rate("workqueue_retries_total", "name"), "{{name}} retries", "B")],
                 "reqps", desc="Adds: an event or a requeue put an object in the queue. Retries: after an error, with backoff"), 6, 7)
    g.add(series("Time in the queue (p95)",
                 [q(f"histogram_quantile(0.95, sum by (le, name) (rate(workqueue_queue_duration_seconds_bucket{{{job}}}[$__rate_interval])))", "{{name}}")],
                 "s", desc="How long an object waited before a worker took it"), 6, 7)
    g.add(series("Longest running reconcile",
                 [q(f"max by (name) (workqueue_longest_running_processor_seconds{{{job}}})", "{{name}}"),
                  q(f"max by (name) (workqueue_unfinished_work_seconds{{{job}}})", "{{name}} unfinished", "B")],
                 "s", desc="A reconcile that keeps growing here is stuck"), 6, 7)

    g.row("Every object")
    obj = 'kind!=""'
    table = {"type": "table", "title": "Every object in the time range",
             "description": "Per RedisInstance, RedisCluster and RedisUser: reconciles and how they ended, the time "
                            "they took, and the calls they made to the Kubernetes API (reads from the cache, writes "
                            "to the API server).",
             "targets": [
                 q(f"sum by (kind, namespace, name) (round(increase(redis_operator_reconcile_total{{{obj}}}[$__range])))", "", "A", True),
                 q(f'sum by (kind, namespace, name) (round(increase(redis_operator_reconcile_total{{{obj},result="error"}}[$__range])))', "", "B", True),
                 q(f'sum by (kind, namespace, name) (round(increase(redis_operator_reconcile_total{{{obj},result="requeue_after"}}[$__range])))', "", "C", True),
                 q(f"sum by (kind, namespace, name) (increase(redis_operator_reconcile_seconds_total{{{obj}}}[$__range])) "
                   f"/ sum by (kind, namespace, name) (increase(redis_operator_reconcile_total{{{obj}}}[$__range]))", "", "D", True),
                 q(f'sum by (kind, namespace, name) (round(increase(redis_operator_api_requests_total{{{obj},source="cache"}}[$__range])))', "", "E", True),
                 q(f'sum by (kind, namespace, name) (round(increase(redis_operator_api_requests_total{{{obj},source="api"}}[$__range])))', "", "F", True)],
             "transformations": [
                 {"id": "merge", "options": {}},
                 {"id": "organize", "options": {
                     "excludeByName": {"Time": True},
                     "renameByName": {"kind": "Kind", "namespace": "Namespace", "name": "Name", "Value #A": "Reconciles",
                                      "Value #B": "Errors", "Value #C": "Look again later", "Value #D": "Average time",
                                      "Value #E": "Reads (cache)", "Value #F": "Writes (API)"}}},
                 {"id": "sortBy", "options": {"sort": [{"field": "Reconciles", "desc": True}]}}],
             "options": {"showHeader": True, "cellHeight": "sm"},
             "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": [
                 {"matcher": {"id": "byName", "options": "Average time"}, "properties": [{"id": "unit", "value": "s"}]},
                 {"matcher": {"id": "byRegexp", "options": "Reconciles|Writes \\(API\\)|Reads \\(cache\\)"}, "properties": [
                     {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "gradient", "valueDisplayMode": "text"}}]},
                 {"matcher": {"id": "byName", "options": "Errors"}, "properties": [
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                     {"id": "thresholds", "value": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}}]}]}}
    g.add(table, 14, 10)
    g.add(series("Reconciles per minute, by object",
                 [q(f"60 * sum by (kind, name) (rate(redis_operator_reconcile_total{{{obj}}}[$__rate_interval]))", "{{kind}} {{name}}")],
                 desc="Every event of a watched object (its Pods, Secrets, ...) and every requeue makes a reconcile"), 10, 10)

    g.row("Calls to the Kubernetes API")
    g.add(series("Calls per second, by verb",
                 [q('sum by (verb, source) (rate(redis_operator_api_requests_total[$__rate_interval]))', "{{verb}} ({{source}})")],
                 "reqps", stack=True,
                 desc="get and list are answered by the cache of the informers; create, update, patch, delete and the "
                      "status go to the API server"), 12, 8)
    g.add(series("Writes to the API server, by kind",
                 [q('sum by (resource, verb) (rate(redis_operator_api_requests_total{source="api"}[$__rate_interval]))', "{{verb}} {{resource}}")],
                 "reqps", stack=True), 12, 8)
    g.add(series("Calls per second, by object",
                 [q(f'sum by (kind, name) (rate(redis_operator_api_requests_total{{{obj}}}[$__rate_interval]))', "{{kind}} {{name}}")],
                 "reqps", stack=True, desc="The calls of the reconciles of every object, reads and writes"), 12, 8)
    g.add(series("Reads from the cache, by kind",
                 [q('sum by (resource, verb) (rate(redis_operator_api_requests_total{source="cache"}[$__rate_interval]))', "{{verb}} {{resource}}")],
                 "reqps", stack=True), 12, 8)
    g.add(series("Requests of client-go to the API server, by method and status",
                 [q(rate("rest_client_requests_total", "method, code"), "{{method}} {{code}}")], "reqps", stack=True,
                 desc="What really goes over the network: the writes, the watches of the informers, leader election"), 24, 8)
    g.row("Calls to the Instance Managers")
    g.add(series("Calls per second, by path",
                 [q('sum by (path) (rate(redis_operator_im_requests_total[$__rate_interval]))', "{{path}}")], "reqps", stack=True,
                 desc="/status every reconcile for every Pod; /config, /replicaof, /fence, /promote and /cluster/* when "
                      "something changes"), 12, 8)
    g.add(series("Calls per second, by instance",
                 [q('sum by (namespace, redis_instance) (rate(redis_operator_im_requests_total[$__rate_interval]))', "{{namespace}}/{{redis_instance}}")],
                 "reqps", stack=True), 12, 8)
    g.add(series("Failed calls per second",
                 [q('sum by (redis_instance, pod, path, code) (rate(redis_operator_im_requests_total{code!="200"}[$__rate_interval]))',
                    "{{pod}} {{path}} {{code}}")], "reqps",
                 desc="error: no answer (a Pod that restarts, a NetworkPolicy); 409: a stale epoch; 503: not ready"), 12, 8)
    g.add(series("Latency (p50, p95), by path",
                 [q('histogram_quantile(0.5, sum by (le, path) (rate(redis_operator_im_request_duration_seconds_bucket[$__rate_interval])))', "{{path}} p50"),
                  q('histogram_quantile(0.95, sum by (le, path) (rate(redis_operator_im_request_duration_seconds_bucket[$__rate_interval])))', "{{path}} p95", "B")],
                 "s", desc="/cluster/migrate runs up to 5 s on purpose"), 12, 8)

    g.row("Decisions")
    g.add(series("Events per minute, by reason",
                 [q("sum by (kind, reason) " + counted("redis_operator_events_total", "$__rate_interval"), "{{kind}} {{reason}}")],
                 desc="Every Event the operator emitted: a decision it took, e.g. a switchover, a failover, a rolling "
                      "update, a resharding"), 24, 8)
    g.add(decisions_table(DECISIONS), 24, 14)
    variables = [variable("job", "Operator", "label_values(controller_runtime_reconcile_total, job)")]
    return dashboard("redis-operator-operator", "Redis Operator / Operator",
                     "Reconciles, workqueues, API and Instance Manager calls, and decisions of the operator",
                     variables, g, [alert_annotation(['alertname=~"Redis.*"']),
                                    experiment_annotation('{job="reshard-cost"}')])


def topology_panel(title, nodes, edges, desc, node_unit, edge_unit):
    p = topology('', '', title, desc)
    p["targets"][0]["expr"], p["targets"][1]["expr"] = nodes, edges
    p["options"] = {"nodes": {"mainStatUnit": node_unit}, "edges": {"mainStatUnit": edge_unit}}
    p["fieldConfig"]["overrides"] = []
    p["fieldConfig"]["defaults"] = {"decimals": 1}
    p["description"] = desc
    return p


repo = sys.argv[1]
for name, d in [("redis-instance", instance_dashboard()), ("redis-cluster", cluster_dashboard()),
                ("redis-operator", operator_dashboard())]:
    with open(f"{repo}/config/monitoring/dashboards/{name}.json", "w") as f:
        json.dump(d, f, indent=1)
        f.write("\n")
