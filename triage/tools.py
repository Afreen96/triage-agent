"""Read-only investigation tools.

Design rules:
  * Every network call goes through _get(), which can only issue HTTP GET.
    The agent has no way to change the systems it is investigating.
  * Tools return small, pre-summarised JSON. Raw Jaeger/Prometheus payloads
    would burn the token budget and bury the signal.
  * All inputs are validated before they reach a URL.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

MAX_OUTPUT_CHARS = 6000
DEFAULT_SERVICES = ("gateway=http://localhost:8081,orders=http://localhost:8082,"
                    "payments=http://localhost:8083,ledger=http://localhost:8084")


def jaeger_url():
    return os.environ.get("JAEGER_URL", "http://localhost:16686").rstrip("/")


def prom_url():
    return os.environ.get("PROM_URL", "http://localhost:9090").rstrip("/")


def services():
    out = {}
    for pair in os.environ.get("TRIAGE_SERVICES", DEFAULT_SERVICES).split(","):
        name, _, url = pair.strip().partition("=")
        if name and url:
            out[name] = url.rstrip("/")
    return out


class ToolError(Exception):
    """Bad input or upstream failure; shown to the model as an error result."""


def _get(url, timeout=8):
    """The single choke point for network access: GET only."""
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:300]
        raise ToolError("upstream returned HTTP %d: %s" % (e.code, body))
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise ToolError("upstream unreachable: %s" % e)
    except ValueError:
        raise ToolError("upstream returned non-JSON response")


def _int(args, key, default, lo, hi):
    try:
        v = int(args.get(key, default))
    except (TypeError, ValueError):
        raise ToolError("%s must be an integer" % key)
    return max(lo, min(hi, v))


def _service(args, required=True):
    name = args.get("service")
    if name is None and not required:
        return None
    if name not in services():
        raise ToolError("unknown service %r; known services: %s" % (name, sorted(services())))
    return name


# ------------------------------------------------------------------ traces --
def _tags(span):
    return {t.get("key"): t.get("value") for t in span.get("tags", [])}


def _is_error(tags):
    if str(tags.get("error", "false")).lower() not in ("false", ""):
        return True
    if str(tags.get("otel.status_code", "")).upper() == "ERROR":
        return True
    try:
        return int(tags.get("http.status_code", 0)) >= 500
    except (TypeError, ValueError):
        return False


def _svc(trace, span):
    return trace.get("processes", {}).get(span.get("processID"), {}).get("serviceName", "?")


def _summarise(trace):
    spans = trace.get("spans", [])
    return {
        "trace_id": trace.get("traceID"),
        "start": datetime.fromtimestamp(min(s["startTime"] for s in spans) / 1e6, timezone.utc).strftime("%H:%M:%SZ"),
        "duration_ms": round(max(s["duration"] for s in spans) / 1000, 1),
        "span_count": len(spans),
        "services": sorted({_svc(trace, s) for s in spans}),
        "services_with_error_spans": sorted({_svc(trace, s) for s in spans if _is_error(_tags(s))}),
    }


def _pct(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(p * len(sorted_vals)))]


def search_traces(args):
    service = _service(args)
    lookback = _int(args, "lookback_seconds", 60, 5, 900)
    limit = _int(args, "limit", 8, 1, 20)
    end = int(time.time() * 1e6)
    params = {"service": service, "start": end - lookback * 1_000_000, "end": end, "limit": 300}
    if args.get("min_duration_ms"):
        params["minDuration"] = "%dms" % _int(args, "min_duration_ms", 0, 1, 600000)
    data = _get(jaeger_url() + "/api/traces?" + urlencode(params)).get("data") or []
    rows = [_summarise(t) for t in data if t.get("spans")]
    sampled = len(rows)
    with_errors = sum(1 for r in rows if r["services_with_error_spans"])
    durs = sorted(r["duration_ms"] for r in rows)
    if args.get("errors_only"):
        rows = [r for r in rows if r["services_with_error_spans"]]
    rows.sort(key=lambda r: r["duration_ms"], reverse=True)
    return {
        "service": service, "lookback_seconds": lookback,
        "traces_sampled": sampled, "traces_with_errors": with_errors,
        "duration_ms_p50": _pct(durs, 0.5) if durs else None,
        "duration_ms_p95": _pct(durs, 0.95) if durs else None,
        "traces": rows[:limit],
        "note": "Slowest first. Use get_trace on a trace_id to see which span is responsible.",
    }


_KEEP_TAGS = ("error.message", "http.status_code", "service.version", "db.system")


def get_trace(args):
    trace_id = str(args.get("trace_id", ""))
    if not re.fullmatch(r"[0-9a-f]{1,32}", trace_id):
        raise ToolError("trace_id must be a lowercase hex string")
    data = _get(jaeger_url() + "/api/traces/" + quote(trace_id)).get("data") or []
    if not data:
        raise ToolError("trace %s not found" % trace_id)
    trace = data[0]
    spans = sorted(trace.get("spans", []), key=lambda s: s["startTime"])
    by_id = {s["spanID"]: s for s in spans}
    child_time = {}
    parent_of = {}
    for s in spans:
        for ref in s.get("references", []):
            if ref.get("refType") == "CHILD_OF" and ref.get("spanID") in by_id:
                parent_of[s["spanID"]] = ref["spanID"]
                child_time[ref["spanID"]] = child_time.get(ref["spanID"], 0) + s["duration"]
    out = []
    for s in spans:
        tags = _tags(s)
        parent = by_id.get(parent_of.get(s["spanID"]))
        out.append({
            "service": _svc(trace, s),
            "operation": s.get("operationName"),
            "parent": "%s: %s" % (_svc(trace, parent), parent.get("operationName")) if parent else None,
            "duration_ms": round(s["duration"] / 1000, 1),
            # self time = time spent in this span itself, excluding its children
            "self_time_ms": round(max(0, s["duration"] - child_time.get(s["spanID"], 0)) / 1000, 1),
            "error": _is_error(tags),
            "tags": {k: tags[k] for k in _KEEP_TAGS if k in tags},
        })
    return {"trace_id": trace_id, "spans": out}


# ----------------------------------------------------------------- metrics --
def query_metrics(args):
    promql = str(args.get("promql", "")).strip()
    if not promql or len(promql) > 500:
        raise ToolError("promql is required and must be under 500 characters")
    resp = _get(prom_url() + "/api/v1/query?" + urlencode({"query": promql}))
    if resp.get("status") != "success":
        raise ToolError("prometheus error: %s" % resp.get("error"))
    data = resp.get("data", {})
    if data.get("resultType") != "vector":
        return {"promql": promql, "result_type": data.get("resultType"), "result": data.get("result")}
    series = []
    for r in data.get("result", [])[:40]:
        try:
            value = round(float(r["value"][1]), 4)
        except (ValueError, TypeError, IndexError):
            value = r.get("value")
        series.append({"labels": {k: v for k, v in r.get("metric", {}).items() if k not in ("job", "instance")},
                       "value": value})
    return {"promql": promql, "series": series,
            "note": "empty series means no data matched" if not series else None}


# ----------------------------------------------------------------- deploys --
def get_recent_deploys(args):
    only = _service(args, required=False)
    now = datetime.now(timezone.utc)
    rows, unreachable = [], []
    for name, url in services().items():
        if only and name != only:
            continue
        try:
            deploys = _get(url + "/deploys").get("deploys", [])
        except ToolError:
            unreachable.append(name)
            continue
        for d in deploys:
            try:
                at = datetime.strptime(d["deployed_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                age_min = round((now - at).total_seconds() / 60, 1)
            except (KeyError, ValueError):
                age_min = None
            rows.append({"service": name, "version": d.get("version"), "deployed_at": d.get("deployed_at"),
                         "minutes_ago": age_min, "author": d.get("author"), "change": d.get("change")})
    rows.sort(key=lambda r: r["minutes_ago"] if r["minutes_ago"] is not None else 1e12)
    return {"now": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "deploys": rows, "unreachable_services": unreachable}


# ---------------------------------------------------------------- registry --
def _schema(props, required=()):
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


TOOLS = [
    {
        "name": "search_traces",
        "description": ("Search recent distributed traces that pass through a service. Returns aggregate stats "
                        "(sample size, error count, p50/p95 duration) plus the slowest matching traces. "
                        "Known services: gateway, orders, payments, ledger."),
        "inputSchema": _schema({
            "service": {"type": "string", "description": "Service name"},
            "lookback_seconds": {"type": "integer", "description": "How far back to search (default 60, max 900)"},
            "min_duration_ms": {"type": "integer", "description": "Only traces with a span at least this slow"},
            "errors_only": {"type": "boolean", "description": "Only traces containing an error span"},
            "limit": {"type": "integer", "description": "Max traces to return (default 8, max 20)"},
        }, ["service"]),
        "handler": search_traces,
    },
    {
        "name": "get_trace",
        "description": ("Fetch one trace by id and list its spans in time order with service, parent, duration, "
                        "self time (time excluding child spans), error flag and error message."),
        "inputSchema": _schema({"trace_id": {"type": "string", "description": "Trace id from search_traces"}},
                               ["trace_id"]),
        "handler": get_trace,
    },
    {
        "name": "query_metrics",
        "description": (
            "Run an instant PromQL query against Prometheus (scrape interval 2s; use rate windows of 20s or more). "
            "Metrics: http_requests_total{service,status} counter; "
            "http_request_duration_seconds_bucket{service,le} histogram; "
            "db_pool_in_use{service} and db_pool_max{service} gauges; build_info{service,version}. "
            "Examples: sum by (service,status) (rate(http_requests_total[30s])) | "
            "histogram_quantile(0.95, sum by (service,le) (rate(http_request_duration_seconds_bucket[30s])))"),
        "inputSchema": _schema({"promql": {"type": "string", "description": "PromQL expression"}}, ["promql"]),
        "handler": query_metrics,
    },
    {
        "name": "get_recent_deploys",
        "description": "List deploy history (version, time, minutes ago, change note), newest first. "
                       "Omit service to see all services.",
        "inputSchema": _schema({"service": {"type": "string", "description": "Optional service name"}}),
        "handler": get_recent_deploys,
    },
]
_BY_NAME = {t["name"]: t for t in TOOLS}


def call_tool(name, args):
    """Returns (text, is_error). Never raises: errors go back to the model as data."""
    tool = _BY_NAME.get(name)
    if tool is None:
        return "unknown tool: %s" % name, True
    if not isinstance(args, dict):
        return "arguments must be an object", True
    try:
        text = json.dumps(tool["handler"](args), separators=(",", ":"))
    except ToolError as e:
        return "error: %s" % e, True
    except Exception as e:  # defensive: a tool bug must not kill the server
        return "internal tool error: %s" % type(e).__name__, True
    if len(text) > MAX_OUTPUT_CHARS:
        text = text[:MAX_OUTPUT_CHARS] + "...[truncated]"
    return text, False
