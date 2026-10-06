"""Offline tests (stdlib unittest).  Run:  python3 -m unittest -v"""
import json
import os
import subprocess
import sys
import time
import unittest
import urllib.request

from tests import fakes
from triage import agent, mcp_server, tools
from triage.mcp_client import MCPClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHAIN = [("gateway", 18081), ("orders", 18082), ("payments", 18083), ("ledger", 18084)]


def _http(url, body=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


class EndToEnd(unittest.TestCase):
    """Real demo services + real MCP subprocess, with fake Jaeger/Prometheus/Claude."""

    @classmethod
    def setUpClass(cls):
        cls.srv, cls.base = fakes.start()
        os.environ.update({
            "JAEGER_URL": cls.base, "PROM_URL": cls.base, "ANTHROPIC_BASE_URL": cls.base,
            "ANTHROPIC_API_KEY": "test-key",
            "TRIAGE_SERVICES": ",".join("%s=http://127.0.0.1:%d" % c for c in CHAIN)})
        cls.procs = []
        for i, (name, port) in enumerate(CHAIN):
            env = dict(os.environ, SERVICE=name, PORT=str(port), ZIPKIN_URL=cls.base + "/api/v2/spans")
            if i + 1 < len(CHAIN):
                env.update(DOWNSTREAM="http://127.0.0.1:%d" % CHAIN[i + 1][1], DOWNSTREAM_NAME=CHAIN[i + 1][0])
            cls.procs.append(subprocess.Popen([sys.executable, os.path.join(ROOT, "demo", "service.py")],
                                              env=env, stdout=subprocess.DEVNULL))
        for _, port in CHAIN:
            for _ in range(50):
                try:
                    if _http("http://127.0.0.1:%d/healthz" % port)[0] == 200:
                        break
                except Exception:
                    time.sleep(0.1)
        cls.gw = "http://127.0.0.1:%d" % CHAIN[0][1]
        cls.ledger = "http://127.0.0.1:%d" % CHAIN[3][1]

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs:
            p.terminate()
        for p in cls.procs:
            p.wait(timeout=5)
        cls.srv.shutdown()
        cls.srv.server_close()

    def setUp(self):
        for _, port in CHAIN:
            _http("http://127.0.0.1:%d/admin/reset" % port, {})

    def _traffic(self, n=12):
        codes = [_http(self.gw + "/work")[0] for _ in range(n)]
        time.sleep(1.0)  # let span flushers run
        return codes

    def test_healthy_chain_produces_full_trace(self):
        self.assertEqual(set(self._traffic(5)), {200})
        text, err = tools.call_tool("search_traces", {"service": "gateway"})
        self.assertFalse(err, text)
        out = json.loads(text)
        self.assertGreaterEqual(out["traces_sampled"], 5)
        t = out["traces"][0]
        self.assertEqual(t["services"], ["gateway", "ledger", "orders", "payments"])
        self.assertEqual(t["span_count"], 8)   # 4 server spans + 4 db spans
        detail = json.loads(tools.call_tool("get_trace", {"trace_id": t["trace_id"]})[0])
        self.assertEqual(detail["spans"][0]["parent"], None)
        self.assertIn("gateway: GET /work", [s["parent"] for s in detail["spans"]])

    def test_pool_exhaustion_propagates_and_is_visible(self):
        _http(self.ledger + "/admin/fault", {"type": "pool_exhaustion"})
        self.assertIn(502, self._traffic(15))
        out = json.loads(tools.call_tool("search_traces", {"service": "gateway", "errors_only": True})[0])
        self.assertTrue(out["traces"])
        self.assertIn("ledger", out["traces"][0]["services_with_error_spans"])
        detail = json.loads(tools.call_tool("get_trace", {"trace_id": out["traces"][0]["trace_id"]})[0])
        msgs = [s["tags"].get("error.message", "") for s in detail["spans"] if s["service"] == "ledger"]
        self.assertTrue(any("pool exhausted" in m for m in msgs))
        self.assertIn('db_pool_in_use{service="ledger"} 10', _http(self.ledger + "/metrics")[1])

    def test_slow_db_shows_up_as_self_time(self):
        _http(self.ledger + "/admin/fault", {"type": "slow_dependency"})
        self._traffic(3)
        out = json.loads(tools.call_tool("search_traces", {"service": "gateway", "min_duration_ms": 300})[0])
        detail = json.loads(tools.call_tool("get_trace", {"trace_id": out["traces"][0]["trace_id"]})[0])
        slowest = max(detail["spans"], key=lambda s: s["self_time_ms"])
        self.assertEqual((slowest["service"], slowest["operation"]), ("ledger", "db.query"))

    def test_bad_deploy_recorded_and_reset_clears_it(self):
        _http(self.ledger + "/admin/deploy", {"version": "v2", "bug": "errors"})
        out = json.loads(tools.call_tool("get_recent_deploys", {})[0])
        self.assertEqual((out["deploys"][0]["service"], out["deploys"][0]["version"]), ("ledger", "v2"))
        self.assertLess(out["deploys"][0]["minutes_ago"], 1)
        _http(self.ledger + "/admin/reset", {})
        out = json.loads(tools.call_tool("get_recent_deploys", {"service": "ledger"})[0])
        self.assertEqual([d["version"] for d in out["deploys"]], ["v1"])

    def test_admin_rejects_bad_input(self):
        self.assertEqual(_http(self.ledger + "/admin/fault", {"type": "rm -rf"})[0], 400)
        self.assertEqual(_http(self.ledger + "/admin/fault", [1])[0], 400)

    def test_tool_input_validation(self):
        for name, args in [("search_traces", {"service": "nope"}), ("get_trace", {"trace_id": "../admin"}),
                           ("query_metrics", {"promql": ""}), ("query_metrics", {"promql": "bad("}),
                           ("no_such_tool", {})]:
            text, err = tools.call_tool(name, args)
            self.assertTrue(err, "%s should fail: %s" % (name, text))

    def test_metrics_tool_strips_noise_labels(self):
        out = json.loads(tools.call_tool("query_metrics", {"promql": "up"})[0])
        self.assertEqual(out["series"], [{"labels": {"service": "ledger", "status": "500"}, "value": 11.5}])

    def test_mcp_subprocess_roundtrip(self):
        with MCPClient() as mcp:
            self.assertEqual(mcp.server_info["name"], "triage-tools")
            listed = mcp.list_tools()
            self.assertEqual([t["name"] for t in listed],
                             ["search_traces", "get_trace", "query_metrics", "get_recent_deploys"])
            self.assertTrue(all(t["annotations"]["readOnlyHint"] for t in listed))
            text, err = mcp.call_tool("get_recent_deploys", {})
            self.assertFalse(err)
            self.assertEqual(len(json.loads(text)["deploys"]), 4)
            self.assertTrue(mcp.call_tool("get_trace", {"trace_id": "XYZ"})[1])

    def test_agent_against_fake_claude_api(self):
        from triage.llm import Anthropic
        _http(self.ledger + "/admin/fault", {"type": "pool_exhaustion"})
        self._traffic(10)
        with MCPClient() as mcp:
            res = agent.run(Anthropic(), mcp, "ALERT: test")
        self.assertEqual(res["stop"], "submitted")
        self.assertTrue(res["valid"])
        self.assertEqual((res["tool_calls"], res["input_tokens"]), (2, 200))
        # second request must carry both tool results, stamped with evidence ids
        results = fakes.LLM_REQUESTS[-1]["messages"][-1]["content"]
        self.assertEqual([r["tool_use_id"] for r in results], ["tu_1", "tu_2"])
        self.assertTrue(results[0]["content"].startswith("[evidence_id: E1]"))


class ProtocolUnit(unittest.TestCase):
    def test_notifications_get_no_reply_and_errors_are_jsonrpc(self):
        self.assertIsNone(mcp_server.handle_line('{"jsonrpc":"2.0","method":"notifications/initialized"}'))
        self.assertEqual(mcp_server.handle_line("not json")["error"]["code"], -32700)
        self.assertEqual(mcp_server.handle_line('{"jsonrpc":"2.0","id":1,"method":"nope"}')["error"]["code"], -32601)
        r = mcp_server.handle_line('{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}')
        self.assertEqual(r["result"]["protocolVersion"], "2024-11-05")


class ScriptedLLM:
    def __init__(self, turns):
        self.turns, self.seen = list(turns), []

    def complete(self, system, messages, tools_, max_tokens=2000):
        self.seen.append(json.loads(json.dumps(messages)))
        return {"content": self.turns.pop(0), "usage": {"input_tokens": 10, "output_tokens": 5}}


class StubMCP:
    def list_tools(self):
        return [{"name": "search_traces", "description": "d", "inputSchema": {"type": "object"}}]

    def call_tool(self, name, args):
        return '{"traces":[{"trace_id":"aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb"}]}', False


def use(i, name, args):
    return {"type": "tool_use", "id": "t%d" % i, "name": name, "input": args}


def diag(evidence, summary="s", service="ledger", category="pool_exhaustion"):
    return {"root_cause_service": service, "category": category, "summary": summary, "evidence": evidence}


class Guardrails(unittest.TestCase):
    def test_invented_evidence_is_rejected_then_fixed(self):
        llm = ScriptedLLM([
            [use(1, "search_traces", {"service": "ledger"})],
            [use(2, "submit_diagnosis", diag([{"id": "E9", "claim": "made up"}]))],
            [use(3, "submit_diagnosis", diag([{"id": "E1", "claim": "real"}]))]])
        res = agent.run(llm, StubMCP(), "alert")
        self.assertTrue(res["valid"])
        self.assertFalse(res["first_submission_valid"])
        self.assertEqual(res["citation_rejections"], 1)
        self.assertIn("never issued", llm.seen[2][-1]["content"][0]["content"])

    def test_hallucinated_trace_id_is_caught(self):
        real, fake = "aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb", "deadbeefdeadbeefdeadbeefdeadbeef"
        issued = {"E1": '{"trace_id":"%s"}' % real}
        self.assertEqual(agent.validate(diag([{"id": "E1", "claim": "trace %s" % real}]), issued), [])
        probs = agent.validate(diag([{"id": "E1", "claim": "trace %s" % fake}]), issued)
        self.assertTrue(any(fake in p for p in probs))

    def test_no_enforcement_accepts_but_flags(self):
        llm = ScriptedLLM([[use(1, "submit_diagnosis", diag([]))]])
        res = agent.run(llm, StubMCP(), "alert", enforce_citations=False)
        self.assertEqual(res["stop"], "submitted")
        self.assertFalse(res["valid"])

    def test_tool_budget_is_enforced(self):
        llm = ScriptedLLM([
            [use(1, "search_traces", {}), use(2, "search_traces", {})],
            [use(3, "submit_diagnosis", diag([{"id": "E1", "claim": "c"}]))]])
        res = agent.run(llm, StubMCP(), "alert", max_tool_calls=1)
        self.assertEqual(res["tool_calls"], 1)
        refused = llm.seen[1][-1]["content"][1]
        self.assertTrue(refused["is_error"])
        self.assertIn("Budget exhausted", refused["content"])

    def test_token_budget_is_enforced(self):
        llm = ScriptedLLM([[use(1, "search_traces", {})], [use(2, "submit_diagnosis", diag([]))],
                           [use(3, "submit_diagnosis", diag([]))], [use(4, "submit_diagnosis", diag([]))]])
        res = agent.run(llm, StubMCP(), "alert", max_tokens=10)
        self.assertEqual(res["tool_calls"], 0)
        self.assertFalse(res["valid"])           # nothing to cite, so still unsupported after retries
        self.assertEqual(res["citation_rejections"], agent.MAX_REJECTIONS)

    def test_model_that_never_submits_terminates(self):
        llm = ScriptedLLM([[{"type": "text", "text": "hmm"}]] * 5)
        res = agent.run(llm, StubMCP(), "alert")
        self.assertEqual((res["stop"], res["diagnosis"]), ("no_submission", None))

    def test_none_must_be_consistent(self):
        self.assertTrue(agent.validate(diag([{"id": "E1", "claim": "c"}], service="none"), {"E1": ""}))
        self.assertEqual(agent.validate(diag([{"id": "E1", "claim": "c"}], service="none", category="none"), {"E1": ""}), [])


if __name__ == "__main__":
    unittest.main()
