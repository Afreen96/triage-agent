"""In-process fakes for Jaeger, Prometheus and the Claude API, so the whole
pipeline can be tested offline. Run standalone:  python3 -m tests.fakes"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SPANS = []          # zipkin spans received
LOCK = threading.Lock()
LLM_REQUESTS = []   # request bodies seen by the fake Claude API


def _jaeger_trace(trace_id, spans):
    procs, pid = {}, {}
    out = []
    for s in spans:
        svc = s["localEndpoint"]["serviceName"]
        if svc not in pid:
            pid[svc] = "p%d" % (len(pid) + 1)
            procs[pid[svc]] = {"serviceName": svc, "tags": []}
        out.append({
            "traceID": trace_id, "spanID": s["id"], "operationName": s["name"],
            "references": ([{"refType": "CHILD_OF", "traceID": trace_id, "spanID": s["parentId"]}]
                           if s.get("parentId") else []),
            "startTime": s["timestamp"], "duration": s["duration"],
            "tags": [{"key": k, "type": "string", "value": v} for k, v in s.get("tags", {}).items()],
            "processID": pid[svc]})
    return {"traceID": trace_id, "spans": out, "processes": procs}


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"null")
        path = urlparse(self.path).path
        if path == "/api/v2/spans":
            with LOCK:
                SPANS.extend(body)
            return self._json({}, 202)
        if path == "/v1/messages":      # fake Claude: gather two pieces of evidence, then submit
            assert self.headers.get("x-api-key") and self.headers.get("anthropic-version")
            LLM_REQUESTS.append(body)
            last = body["messages"][-1]["content"]
            if isinstance(last, str):
                content = [
                    {"type": "text", "text": "Checking traces and deploys."},
                    {"type": "tool_use", "id": "tu_1", "name": "search_traces",
                     "input": {"service": "gateway", "errors_only": True}},
                    {"type": "tool_use", "id": "tu_2", "name": "get_recent_deploys", "input": {}}]
            else:
                content = [{"type": "tool_use", "id": "tu_3", "name": "submit_diagnosis", "input": {
                    "root_cause_service": "ledger", "category": "pool_exhaustion",
                    "summary": "scripted answer", "evidence": [{"id": "E1", "claim": "errors seen"}]}}]
            return self._json({"content": content, "stop_reason": "tool_use",
                               "usage": {"input_tokens": 100, "output_tokens": 20}})
        self._json({"error": "not found"}, 404)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/-/ready":
            return self._json({"ready": True})
        if u.path == "/api/services":
            with LOCK:
                return self._json({"data": sorted({s["localEndpoint"]["serviceName"] for s in SPANS})})
        if u.path == "/api/v1/query":
            if "bad(" in q.get("query", [""])[0]:
                return self._json({"status": "error", "errorType": "bad_data", "error": "parse error"}, 400)
            return self._json({"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {"service": "ledger", "status": "500", "job": "demo", "instance": "ledger:8080"},
                 "value": [1700000000, "11.5"]}]}})
        if u.path.startswith("/api/traces"):
            with LOCK:
                by_trace = {}
                for s in SPANS:
                    by_trace.setdefault(s["traceId"], []).append(s)
            tid = u.path[len("/api/traces"):].strip("/")
            if tid:
                return self._json({"data": [_jaeger_trace(tid, by_trace[tid])] if tid in by_trace else []})
            svc, start, end = q["service"][0], int(q["start"][0]), int(q["end"][0])
            min_us = int(q["minDuration"][0].rstrip("ms")) * 1000 if "minDuration" in q else 0
            hits = [_jaeger_trace(t, sp) for t, sp in by_trace.items()
                    if any(s["localEndpoint"]["serviceName"] == svc and s["duration"] >= min_us for s in sp)
                    and start <= min(s["timestamp"] for s in sp) <= end]
            return self._json({"data": hits[-int(q.get("limit", ["20"])[0]):]})
        self._json({"error": "not found"}, 404)


def start(port=0):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d" % srv.server_address[1]


if __name__ == "__main__":
    import sys, time
    _, url = start(int(sys.argv[1]) if len(sys.argv) > 1 else 19000)
    print("fakes on", url, flush=True)
    while True:
        time.sleep(3600)
