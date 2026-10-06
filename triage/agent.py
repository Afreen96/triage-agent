"""The investigation loop.

    alert -> [model picks a tool -> we run it via MCP -> result goes back] x N -> submit_diagnosis

Three guardrails live here (the fourth, read-only tools, lives in tools.py):
  1. Tool-call budget: after max_tool_calls, further calls are refused.
  2. Token budget: same, once cumulative tokens pass max_tokens.
  3. Evidence citations: each tool result is stamped with an id (E1, E2, ...).
     The final diagnosis must cite ids that were really issued, and any trace id
     it mentions must appear verbatim in a tool result. Otherwise it is rejected
     and the model must fix it.

Run:  python3 -m triage.agent "ALERT: elevated errors on gateway /work"
"""
import json
import re
import sys
import time

CATEGORIES = ("bad_deploy", "slow_dependency", "pool_exhaustion", "none")
MAX_REJECTIONS = 2

SUBMIT_TOOL = {
    "name": "submit_diagnosis",
    "description": "Submit the final root-cause diagnosis. Call exactly once, when the investigation is done.",
    "input_schema": {
        "type": "object",
        "properties": {
            "root_cause_service": {
                "type": "string",
                "description": "The service where the problem originates, or 'none' if telemetry is healthy."},
            "category": {
                "type": "string", "enum": list(CATEGORIES),
                "description": ("bad_deploy: a recent deploy of the root-cause service lines up with onset. "
                                "slow_dependency: that service's database calls are slow, with no recent deploy. "
                                "pool_exhaustion: that service's DB connection pool is saturated. "
                                "none: no service is unhealthy (false alarm).")},
            "summary": {"type": "string", "description": "2-3 sentences: what is wrong and why you believe it."},
            "evidence": {
                "type": "array",
                "description": "The tool results that support the diagnosis.",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Evidence id of a tool result, e.g. E3"},
                        "claim": {"type": "string", "description": "What that result shows"}},
                    "required": ["id", "claim"]}},
        },
        "required": ["root_cause_service", "category", "summary", "evidence"],
    },
}

SYSTEM = """You are an on-call triage assistant. An alert has fired for a microservice system. \
Find the root cause using the read-only tools provided.

How to investigate:
- Requests flow through a chain of services. A failure deep in the chain shows up as errors or latency \
in every service above it. The root cause is the deepest service whose OWN work (its self time, its own \
error, its own database call) explains the symptom, not the services that merely propagate it.
- Start broad with metrics across services, then confirm with one or two concrete traces.
- Always check recent deploys before concluding. A deploy that lines up with onset is strong evidence.
- Respect the time window stated in the alert. Older telemetry may belong to unrelated earlier incidents.
- Alerts can be false alarms. If telemetry is healthy, the correct diagnosis is 'none'.

Rules:
- You have a budget of {budget} tool calls. Be efficient.
- Every tool result begins with an evidence id such as [evidence_id: E3]. Your diagnosis must cite the \
ids that support it. Cite only what you actually saw. Never invent trace ids or numbers.
- Finish by calling submit_diagnosis exactly once."""


def validate(diagnosis, issued):
    """Check a diagnosis against the evidence actually issued. Returns a list of problems."""
    problems = []
    if not isinstance(diagnosis, dict):
        return ["diagnosis must be an object"]
    service = str(diagnosis.get("root_cause_service", "")).strip().lower()
    category = diagnosis.get("category")
    if not service:
        problems.append("root_cause_service is missing")
    if category not in CATEGORIES:
        problems.append("category must be one of %s" % (CATEGORIES,))
    if (service == "none") != (category == "none"):
        problems.append("root_cause_service and category must both be 'none' or neither")
    evidence = diagnosis.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        problems.append("no evidence cited; cite at least one evidence id")
        evidence = []
    claims = [str(diagnosis.get("summary", ""))]
    for item in evidence:
        eid = item.get("id") if isinstance(item, dict) else None
        if eid not in issued:
            problems.append("evidence id %r was never issued; valid ids: %s" % (eid, sorted(issued) or "none"))
        if isinstance(item, dict):
            claims.append(str(item.get("claim", "")))
    seen = "\n".join(issued.values())
    for token in sorted(set(re.findall(r"\b[0-9a-f]{16,32}\b", " ".join(claims)))):
        if token not in seen:
            problems.append("trace id %s does not appear in any tool result" % token)
    return problems


def _tool_result(use, text, is_error=False):
    return {"type": "tool_result", "tool_use_id": use["id"], "content": text, "is_error": is_error}


def run(llm, mcp, alert, max_tool_calls=12, max_tokens=150_000, enforce_citations=True, log=None):
    """Investigate one alert. Returns a result dict (see bottom of function)."""
    log = log or (lambda msg: None)
    started = time.time()
    tool_defs = [{"name": t["name"], "description": t["description"], "input_schema": t["inputSchema"]}
                 for t in mcp.list_tools()] + [SUBMIT_TOOL]
    system = SYSTEM.format(budget=max_tool_calls)
    messages = [{"role": "user", "content": alert}]
    issued = {}            # evidence id -> tool result text
    steps = []
    tokens_in = tokens_out = tool_calls = rejections = nudges = 0
    first_valid = None     # was the FIRST submission fully supported?
    diagnosis, valid, stop = None, False, "no_submission"

    for _ in range(max_tool_calls + 8):   # hard cap on model turns
        resp = llm.complete(system, messages, tool_defs)
        usage = resp.get("usage") or {}
        tokens_in += usage.get("input_tokens", 0)
        tokens_out += usage.get("output_tokens", 0)
        content = resp.get("content") or [{"type": "text", "text": "(no output)"}]
        messages.append({"role": "assistant", "content": content})
        uses = [b for b in content if b.get("type") == "tool_use"]

        if not uses:
            nudges += 1
            if nudges > 2:
                break
            messages.append({"role": "user", "content": "Call submit_diagnosis now with your conclusion."})
            continue

        results, done = [], False
        for use in uses:
            args = use.get("input") or {}
            if use["name"] == "submit_diagnosis":
                problems = validate(args, issued)
                if first_valid is None:
                    first_valid = not problems
                if problems and enforce_citations and rejections < MAX_REJECTIONS:
                    rejections += 1
                    log("  submit rejected: %s" % "; ".join(problems))
                    results.append(_tool_result(use, "Rejected. Fix and resubmit: " + "; ".join(problems), True))
                else:
                    diagnosis, valid, stop, done = args, not problems, "submitted", True
                    results.append(_tool_result(use, "accepted"))
            elif tool_calls >= max_tool_calls or tokens_in + tokens_out >= max_tokens:
                results.append(_tool_result(
                    use, "Budget exhausted. Call submit_diagnosis now using the evidence you already have.", True))
            else:
                tool_calls += 1
                eid = "E%d" % tool_calls
                text, is_error = mcp.call_tool(use["name"], args)
                issued[eid] = text
                steps.append({"evidence_id": eid, "tool": use["name"], "args": args,
                              "is_error": is_error, "chars": len(text)})
                log("  %s %s(%s)%s" % (eid, use["name"], json.dumps(args), "  [error]" if is_error else ""))
                results.append(_tool_result(use, "[evidence_id: %s]\n%s" % (eid, text), is_error))
        if done:
            break
        messages.append({"role": "user", "content": results})

    return {
        "diagnosis": diagnosis,                 # dict or None
        "valid": valid,                         # final answer passed the citation check
        "first_submission_valid": bool(first_valid),
        "citation_rejections": rejections,
        "stop": stop,
        "tool_calls": tool_calls,
        "input_tokens": tokens_in,
        "output_tokens": tokens_out,
        "seconds": round(time.time() - started, 1),
        "steps": steps,
    }


def main(argv):
    from .llm import Anthropic
    from .mcp_client import MCPClient
    if len(argv) < 2:
        print('usage: python3 -m triage.agent "ALERT text"', file=sys.stderr)
        return 2
    with MCPClient() as mcp:
        result = run(Anthropic(), mcp, argv[1], log=lambda m: print(m, file=sys.stderr, flush=True))
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
