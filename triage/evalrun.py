"""Fault-injection eval harness.

For each scenario: inject a known fault -> let it soak under load -> ask the agent
for a root cause -> score against ground truth -> reset -> cool down.
Because we injected the fault ourselves, we know the right answer.

  python3 -m triage.evalrun --smoke      # check the stack is wired up (no LLM, no cost)
  python3 -m triage.evalrun              # full run, writes results.json
  python3 -m triage.evalrun --only ledger --runs 3
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

from . import agent, tools
from .mcp_client import MCPClient

SCENARIOS = [
    {"name": "payments-slow-db",        "service": "payments", "category": "slow_dependency", "kind": "fault",  "mode": "slow_dependency"},
    {"name": "ledger-slow-db",          "service": "ledger",   "category": "slow_dependency", "kind": "fault",  "mode": "slow_dependency"},
    {"name": "orders-bad-deploy-errors", "service": "orders",  "category": "bad_deploy",      "kind": "deploy", "mode": "errors"},
    {"name": "ledger-bad-deploy-errors", "service": "ledger",  "category": "bad_deploy",      "kind": "deploy", "mode": "errors"},
    {"name": "payments-bad-deploy-slow", "service": "payments", "category": "bad_deploy",     "kind": "deploy", "mode": "latency"},
    {"name": "gateway-bad-deploy-slow", "service": "gateway",  "category": "bad_deploy",      "kind": "deploy", "mode": "latency"},
    {"name": "ledger-pool-exhaustion",  "service": "ledger",   "category": "pool_exhaustion", "kind": "fault",  "mode": "pool_exhaustion"},
    {"name": "orders-pool-exhaustion",  "service": "orders",   "category": "pool_exhaustion", "kind": "fault",  "mode": "pool_exhaustion"},
    {"name": "false-alarm",             "service": "none",     "category": "none",            "kind": "none",   "mode": None},
]

ALERT = ("ALERT gateway-slo-burn fired at {now} UTC. The SLO burn-rate monitor for gateway GET /work "
         "tripped (possible elevated errors or latency). This monitor is sometimes noisy. "
         "If there is an incident, it began within the last {window} seconds; telemetry older than that "
         "predates it. Request path: gateway -> orders -> payments -> ledger. Find the root cause.")


def say(msg=""):
    print(msg, flush=True)


def _post(url, body=None):
    req = urllib.request.Request(url, data=json.dumps(body or {}).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=5).read()


def _reachable(url):
    try:
        urllib.request.urlopen(url, timeout=3).read()
        return True
    except Exception:
        return False


def reset_all(svcs):
    for url in svcs.values():
        _post(url + "/admin/reset")


def inject(svcs, sc):
    if sc["kind"] == "fault":
        _post(svcs[sc["service"]] + "/admin/fault", {"type": sc["mode"]})
    elif sc["kind"] == "deploy":
        _post(svcs[sc["service"]] + "/admin/deploy",
              {"version": "v2", "bug": sc["mode"], "change": "dependency bump and request handler refactor"})


class Load:
    """Background traffic against the gateway so telemetry is always live."""

    def __init__(self, url, workers=6, rps=15):
        self.url, self.interval, self.stop_flag, self.sent = url, workers / rps, threading.Event(), 0
        for _ in range(workers):
            threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while not self.stop_flag.is_set():
            t = time.time()
            try:
                urllib.request.urlopen(self.url, timeout=6).read()
            except Exception:
                pass  # 5xx during a fault is expected
            self.sent += 1
            self.stop_flag.wait(max(0.0, self.interval - (time.time() - t)))

    def stop(self):
        self.stop_flag.set()


def preflight(svcs, need_llm):
    problems = []
    for name, url in svcs.items():
        if not _reachable(url + "/healthz"):
            problems.append("service %s not reachable at %s" % (name, url))
    if not _reachable(tools.jaeger_url() + "/api/services"):
        problems.append("Jaeger not reachable at %s" % tools.jaeger_url())
    if not _reachable(tools.prom_url() + "/-/ready"):
        problems.append("Prometheus not reachable at %s" % tools.prom_url())
    if need_llm and not os.environ.get("ANTHROPIC_API_KEY"):
        problems.append("ANTHROPIC_API_KEY is not set")
    if "gateway" not in svcs:
        problems.append("TRIAGE_SERVICES must include a 'gateway' entry")
    if problems:
        say("Preflight failed:\n  - " + "\n  - ".join(problems))
        say("Stack not up?  docker compose up -d     Key missing?  export ANTHROPIC_API_KEY=...")
        sys.exit(1)


def smoke(svcs):
    """Inject one fault and print what each tool returns. No LLM involved."""
    load = Load(svcs["gateway"] + "/work")
    ok = True
    try:
        reset_all(svcs)
        _post(svcs["ledger"] + "/admin/fault", {"type": "pool_exhaustion"})
        soak = int(os.environ.get("SMOKE_SOAK", "20"))
        say("Injected pool_exhaustion at ledger; sending load for %ds..." % soak)
        time.sleep(soak)
        with MCPClient() as mcp:
            say("MCP server: %s, tools: %s" % (mcp.server_info, [t["name"] for t in mcp.list_tools()]))
            calls = [
                ("search_traces", {"service": "ledger", "errors_only": True, "limit": 2}),
                ("query_metrics", {"promql": "sum by (service,status) (rate(http_requests_total[20s]))"}),
                ("query_metrics", {"promql": "db_pool_in_use"}),
                ("get_recent_deploys", {}),
            ]
            for name, args in calls:
                text, is_error = mcp.call_tool(name, args)
                say("\n--- %s %s%s\n%s" % (name, json.dumps(args), "  [ERROR]" if is_error else "", text[:900]))
                ok = ok and not is_error
                if name == "search_traces" and not is_error:
                    traces = json.loads(text).get("traces", [])
                    if not traces:
                        ok = False
                        say("!! No error traces found. Check that Jaeger is receiving spans "
                            "(open http://localhost:16686, look for the 'ledger' service).")
                    else:
                        t2, e2 = mcp.call_tool("get_trace", {"trace_id": traces[0]["trace_id"]})
                        say("\n--- get_trace%s\n%s" % ("  [ERROR]" if e2 else "", t2[:1200]))
                        ok = ok and not e2
    finally:
        reset_all(svcs)
        load.stop()
    say("\nSMOKE %s" % ("PASSED: the stack is wired correctly." if ok else "FAILED: see messages above."))
    return 0 if ok else 1


def run_eval(a, svcs):
    from .llm import Anthropic
    llm = Anthropic()
    scenarios = [s for s in SCENARIOS if not a.only or a.only in s["name"]]
    if not scenarios:
        say("no scenario matches --only %s" % a.only)
        return 1
    total = len(scenarios) * a.runs
    say("Model %s | %d runs | ~%d min\n" % (llm.model, total, round(total * (a.soak + a.cooldown + 45) / 60 + a.warmup / 60)))
    load = Load(svcs["gateway"] + "/work")
    rows = []
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        reset_all(svcs)
        say("Warming up %ds..." % a.warmup)
        time.sleep(a.warmup)
        i = 0
        for _ in range(a.runs):
            for sc in scenarios:
                i += 1
                say("[%d/%d] %s: inject, soak %ds" % (i, total, sc["name"], a.soak))
                row = {"scenario": sc["name"], "expected_service": sc["service"], "expected_category": sc["category"]}
                try:
                    inject(svcs, sc)
                    time.sleep(a.soak)
                    alert = ALERT.format(now=datetime.now(timezone.utc).strftime("%H:%M:%S"), window=a.soak + 15)
                    with MCPClient() as mcp:
                        res = agent.run(llm, mcp, alert, max_tool_calls=a.max_tool_calls,
                                        enforce_citations=not a.no_enforce,
                                        log=say if a.verbose else None)
                    d = res["diagnosis"] or {}
                    row.update(res)
                    row["got_service"] = str(d.get("root_cause_service", "-")).strip().lower()
                    row["got_category"] = d.get("category", "-")
                    row["service_correct"] = row["got_service"] == sc["service"]
                    row["correct"] = row["service_correct"] and row["got_category"] == sc["category"]
                except Exception as e:  # one failed run must not lose the whole eval
                    row.update({"error": "%s: %s" % (type(e).__name__, e), "correct": False,
                                "service_correct": False, "valid": False, "first_submission_valid": False})
                finally:
                    reset_all(svcs)
                rows.append(row)
                say("      -> %s  got %s/%s  (%s tool calls, %ss)%s" % (
                    "PASS" if row["correct"] else "FAIL", row.get("got_service", "-"), row.get("got_category", "-"),
                    row.get("tool_calls", "-"), row.get("seconds", "-"),
                    "  ERROR " + row["error"] if "error" in row else ""))
                if i < total:
                    time.sleep(a.cooldown)
    finally:
        load.stop()

    n = len(rows)
    done = [r for r in rows if "error" not in r]
    pct = lambda k: round(100.0 * sum(1 for r in rows if r.get(k)) / n, 1)
    summary = {
        "runs": n,
        "distinct_scenarios": len(scenarios),
        "root_cause_accuracy_pct": pct("correct"),
        "service_only_accuracy_pct": pct("service_correct"),
        "median_triage_seconds": statistics.median(r["seconds"] for r in done) if done else None,
        "mean_tool_calls": round(statistics.mean(r["tool_calls"] for r in done), 1) if done else None,
        "unsupported_first_submission_pct": round(100 - pct("first_submission_valid"), 1),
        "unsupported_final_pct": round(100 - pct("valid"), 1),
        "total_input_tokens": sum(r.get("input_tokens", 0) for r in rows),
        "total_output_tokens": sum(r.get("output_tokens", 0) for r in rows),
        "errored_runs": n - len(done),
    }
    say("\n%-28s %-26s %-26s %s" % ("scenario", "expected", "got", "result"))
    for r in rows:
        say("%-28s %-26s %-26s %s" % (
            r["scenario"], "%s/%s" % (r["expected_service"], r["expected_category"]),
            "%s/%s" % (r.get("got_service", "-"), r.get("got_category", "-")), "PASS" if r["correct"] else "FAIL"))
    say("\n" + json.dumps(summary, indent=2))
    with open(a.out, "w") as f:
        json.dump({"model": llm.model, "started_at": started_at,
                   "config": {"soak": a.soak, "cooldown": a.cooldown, "max_tool_calls": a.max_tool_calls,
                              "enforce_citations": not a.no_enforce},
                   "summary": summary, "runs": rows}, f, indent=2)
    say("\nWrote %s" % a.out)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--smoke", action="store_true", help="verify wiring without calling the LLM")
    p.add_argument("--runs", type=int, default=1, help="repeat the scenario set this many times")
    p.add_argument("--only", default="", help="only scenarios whose name contains this text")
    p.add_argument("--soak", type=int, default=35, help="seconds a fault runs before the agent starts")
    p.add_argument("--cooldown", type=int, default=45, help="healthy seconds between scenarios")
    p.add_argument("--warmup", type=int, default=20)
    p.add_argument("--max-tool-calls", type=int, default=12)
    p.add_argument("--no-enforce", action="store_true", help="ablation: accept diagnoses without citation checks")
    p.add_argument("--verbose", "-v", action="store_true", help="print each tool call")
    p.add_argument("--out", default="results.json")
    a = p.parse_args()
    svcs = tools.services()
    preflight(svcs, need_llm=not a.smoke)
    return smoke(svcs) if a.smoke else run_eval(a, svcs)


if __name__ == "__main__":
    sys.exit(main())
