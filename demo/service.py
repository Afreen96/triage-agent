#!/usr/bin/env python3
"""One demo microservice (stdlib only).

Run four copies chained  gateway -> orders -> payments -> ledger.
Each request does a simulated DB query, then calls the next service.
Every service emits Zipkin-format spans (ingested by Jaeger) and Prometheus
metrics, and exposes /admin endpoints so the eval harness can inject faults.

The "database" is simulated with sleeps: this is a test bed, not a real system.
"""
import json
import os
import queue
import random
import secrets
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

SERVICE = os.environ.get("SERVICE", "gateway")
PORT = int(os.environ.get("PORT", "8080"))
DOWNSTREAM = os.environ.get("DOWNSTREAM", "").rstrip("/")
DOWNSTREAM_NAME = os.environ.get("DOWNSTREAM_NAME") or (urlparse(DOWNSTREAM).hostname or "")
ZIPKIN_URL = os.environ.get("ZIPKIN_URL", "")

POOL_MAX = 10
BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
FAULTS = {"slow_dependency", "pool_exhaustion"}   # runtime faults, no deploy involved
BUGS = {"errors", "latency"}                      # bugs shipped by a bad deploy


def _iso(t):
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_BASELINE_DEPLOY = {
    "version": "v1",
    "deployed_at": _iso(datetime.now(timezone.utc) - timedelta(hours=random.randint(30, 90))),
    "author": "ci-bot",
    "change": "baseline release",
}


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.requests = {"200": 0}
        self.bucket_counts = [0] * (len(BUCKETS) + 1)
        self.dur_sum = 0.0
        self.dur_count = 0
        self.reset()

    def reset(self):
        with self.lock:
            self.fault = None
            self.bug = None
            self.version = "v1"
            self.deploys = [dict(_BASELINE_DEPLOY)]
            self.pool_in_use = 2


S = State()

# ---------------------------------------------------------------- tracing --
_spans = queue.Queue(maxsize=10000)


def new_id(nbytes):
    return secrets.token_hex(nbytes)


def report_span(trace_id, span_id, parent_id, name, start, duration, tags, kind=None):
    span = {
        "traceId": trace_id,
        "id": span_id,
        "name": name,
        "timestamp": int(start * 1e6),
        "duration": max(1, int(duration * 1e6)),
        "localEndpoint": {"serviceName": SERVICE},
        "tags": tags,
    }
    if parent_id:
        span["parentId"] = parent_id
    if kind:
        span["kind"] = kind
    try:
        _spans.put_nowait(span)
    except queue.Full:
        pass  # never block request handling on telemetry


def _flusher():
    while True:
        batch = [_spans.get()]
        time.sleep(0.3)
        while len(batch) < 500:
            try:
                batch.append(_spans.get_nowait())
            except queue.Empty:
                break
        if not ZIPKIN_URL:
            continue
        try:
            req = urllib.request.Request(
                ZIPKIN_URL, data=json.dumps(batch).encode(),
                headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=3).read()
        except Exception:
            pass  # collector down: drop spans


# ---------------------------------------------------------------- metrics --
def observe(status, seconds):
    with S.lock:
        S.requests[str(status)] = S.requests.get(str(status), 0) + 1
        for i, le in enumerate(BUCKETS):
            if seconds <= le:
                S.bucket_counts[i] += 1
                break
        else:
            S.bucket_counts[-1] += 1
        S.dur_sum += seconds
        S.dur_count += 1


def render_metrics():
    with S.lock:
        requests = dict(S.requests)
        counts = list(S.bucket_counts)
        dur_sum, dur_count = S.dur_sum, S.dur_count
        pool, version = S.pool_in_use, S.version
    svc = 'service="%s"' % SERVICE
    out = ["# TYPE http_requests_total counter"]
    for status, n in sorted(requests.items()):
        out.append('http_requests_total{%s,status="%s"} %d' % (svc, status, n))
    out.append("# TYPE http_request_duration_seconds histogram")
    cum = 0
    for le, n in zip(BUCKETS, counts):
        cum += n
        out.append('http_request_duration_seconds_bucket{%s,le="%s"} %d' % (svc, le, cum))
    out.append('http_request_duration_seconds_bucket{%s,le="+Inf"} %d' % (svc, cum + counts[-1]))
    out.append("http_request_duration_seconds_sum{%s} %f" % (svc, dur_sum))
    out.append("http_request_duration_seconds_count{%s} %d" % (svc, dur_count))
    out.append("# TYPE db_pool_in_use gauge")
    out.append("db_pool_in_use{%s} %d" % (svc, pool))
    out.append("# TYPE db_pool_max gauge")
    out.append("db_pool_max{%s} %d" % (svc, POOL_MAX))
    out.append("# TYPE build_info gauge")
    out.append('build_info{%s,version="%s"} 1' % (svc, version))
    return "\n".join(out) + "\n"


# ------------------------------------------------------------ request path --
def db_query(trace_id, parent_id, fault):
    """Simulated DB call. Returns an error string or None."""
    start = time.time()
    err = None
    if fault == "pool_exhaustion":
        pool = POOL_MAX
        if random.random() < 0.8:
            time.sleep(0.05)
            err = "db: connection pool exhausted (%d/%d in use), acquire timed out after 50ms" % (POOL_MAX, POOL_MAX)
        else:
            time.sleep(random.uniform(0.002, 0.006))
    elif fault == "slow_dependency":
        pool = random.randint(4, 7)
        time.sleep(random.uniform(0.4, 0.7))
    else:
        pool = random.randint(1, 3)
        time.sleep(random.uniform(0.002, 0.006))
    with S.lock:
        S.pool_in_use = pool
    tags = {"db.system": "postgresql", "db.statement": "SELECT * FROM %s_records WHERE id = ?" % SERVICE}
    if err:
        tags["error"] = "true"
        tags["error.message"] = err
    report_span(trace_id, new_id(8), parent_id, "db.query", start, time.time() - start, tags)
    return err


def call_downstream(trace_id, span_id):
    req = urllib.request.Request(DOWNSTREAM + "/work", headers={
        "X-B3-TraceId": trace_id, "X-B3-SpanId": span_id, "X-B3-Sampled": "1"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 599


def work(headers):
    start = time.time()
    trace_id = headers.get("X-B3-TraceId") or new_id(16)
    parent_id = headers.get("X-B3-SpanId")
    span_id = new_id(8)
    with S.lock:
        fault, bug, version = S.fault, S.bug, S.version

    status, err = 200, None
    if bug == "errors" and random.random() < 0.7:
        status, err = 500, "panic: runtime error: invalid memory address or nil pointer dereference"
    elif bug == "latency":
        time.sleep(random.uniform(0.4, 0.7))   # slow handler code, not the DB

    if status == 200:
        err = db_query(trace_id, span_id, fault)
        if err:
            status = 500
    if status == 200 and DOWNSTREAM:
        code = call_downstream(trace_id, span_id)
        if code >= 500:
            status, err = 502, "downstream %s returned %d" % (DOWNSTREAM_NAME, code)

    duration = time.time() - start
    tags = {"http.method": "GET", "http.path": "/work",
            "http.status_code": str(status), "service.version": version}
    if err:
        tags["error"] = "true"
        tags["error.message"] = err
    report_span(trace_id, span_id, parent_id, "GET /work", start, duration, tags, kind="SERVER")
    observe(status, duration)
    return status, err


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 4096:
            raise ValueError("body too large")
        body = json.loads(self.rfile.read(n) or b"{}")
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
        return body

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/work":
            status, err = work(self.headers)
            self._send(status, {"service": SERVICE, "ok": status == 200, "error": err})
        elif path == "/metrics":
            self._send(200, render_metrics(), "text/plain; version=0.0.4")
        elif path == "/deploys":
            with S.lock:
                deploys = list(S.deploys)
            self._send(200, {"service": SERVICE, "deploys": deploys})
        elif path == "/healthz":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        # Fault injection. Unauthenticated on purpose (local test bed), which is
        # why docker-compose binds these ports to 127.0.0.1 only.
        path = urlparse(self.path).path
        try:
            body = self._body()
            if path == "/admin/fault":
                kind = body.get("type") or None
                if kind is not None and kind not in FAULTS:
                    return self._send(400, {"error": "type must be one of %s" % sorted(FAULTS)})
                with S.lock:
                    S.fault = kind
            elif path == "/admin/deploy":
                bug = body.get("bug") or None
                if bug is not None and bug not in BUGS:
                    return self._send(400, {"error": "bug must be one of %s" % sorted(BUGS)})
                with S.lock:
                    S.version = str(body.get("version") or "v2")[:20]
                    S.bug = bug
                    S.deploys.append({
                        "version": S.version, "deployed_at": _iso(datetime.now(timezone.utc)),
                        "author": str(body.get("author") or "ci-bot")[:40],
                        "change": str(body.get("change") or "routine release")[:200]})
            elif path == "/admin/reset":
                S.reset()
            else:
                return self._send(404, {"error": "not found"})
            self._send(200, {"ok": True})
        except ValueError as e:
            self._send(400, {"error": str(e)})


if __name__ == "__main__":
    threading.Thread(target=_flusher, daemon=True).start()
    print("%s listening on :%d downstream=%s" % (SERVICE, PORT, DOWNSTREAM or "-"), flush=True)
    ThreadingHTTPServer.request_queue_size = 128   # default backlog of 5 causes latency spikes under load
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
