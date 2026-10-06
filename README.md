# triage-agent

An AI on-call assistant. When an alert fires, it investigates traces, metrics and deploy history
through read-only tools and returns a root-cause diagnosis with cited evidence. A fault-injection
harness measures how often it is right.

Python 3.9+, standard library only. No `pip install`.

```
 alert ──> agent loop ──(Claude Messages API, tool use)──> Claude
              │  ▲
   JSON-RPC   │  │ tool results, each stamped E1, E2, ...
   over stdio ▼  │
           MCP server ──GET──> Jaeger      (traces)
           (4 read-only ──GET──> Prometheus (metrics)
            tools)      ──GET──> services   (deploy history)

 eval harness: inject fault ─> soak ─> run agent ─> score vs ground truth ─> reset
 test bed:     gateway ─> orders ─> payments ─> ledger   (each with a simulated DB)
```

## Quickstart

You need Python 3.9+, Docker, and an Anthropic API key (only for the last two commands).

```bash
git clone https://github.com/<your-username>/triage-agent.git && cd triage-agent
python3 -m unittest                    # 17 offline tests, ~10s, needs nothing else
docker compose up -d                   # test bed + Jaeger + Prometheus
python3 -m triage.evalrun --smoke      # verifies wiring, no LLM calls
export ANTHROPIC_API_KEY=...           # keep it out of the repo
python3 -m triage.evalrun --only ledger-pool -v    # one scenario, watch the tool calls
python3 -m triage.evalrun              # all 9 scenarios, ~20 min, writes results.json
```

Useful UIs: Jaeger at http://localhost:16686, Prometheus at http://localhost:9090.
Set `TRIAGE_MODEL` to change the model (default is in `triage/llm.py`).

Use the tools interactively from Claude Code (check `claude mcp add --help` if the syntax differs):

```bash
claude mcp add triage -- /absolute/path/to/triage-agent/bin/mcp-server
```

Then inject a fault by hand and ask Claude why the gateway is failing:

```bash
curl -X POST localhost:8084/admin/fault -d '{"type":"pool_exhaustion"}'
while true; do curl -s -o /dev/null localhost:8081/work; sleep 0.1; done
curl -X POST localhost:8084/admin/reset        # when done
```

## How it works, file by file

Read in this order. The core is about 450 lines.

| File | What it teaches |
|---|---|
| `triage/tools.py` | Tool design. One `_get()` function is the only network path, so the agent cannot mutate anything. Tools return small summaries (p50/p95, self time per span) instead of raw payloads, because raw Jaeger JSON wastes tokens and hides the signal. |
| `triage/mcp_server.py` | The Model Context Protocol. A tools-only server is four JSON-RPC methods: `initialize`, `tools/list`, `tools/call`, `ping`. Messages are one JSON object per line on stdin/stdout. |
| `triage/mcp_client.py` | The other side: spawn the server, handshake, call tools. |
| `triage/llm.py` | One HTTP POST to the Messages API, with retry on 429/5xx. |
| `triage/agent.py` | The agent loop. The model replies with `tool_use` blocks; we run each tool and send back `tool_result` blocks; repeat until it calls `submit_diagnosis`. Budgets and citation checks live here. |
| `triage/evalrun.py` | The eval. Ground truth exists because we injected the fault. |
| `demo/service.py` | The test bed: B3 trace propagation, Zipkin spans, hand-rolled Prometheus metrics, fault injection. |

### The agent loop in one paragraph

An LLM cannot call anything itself. You send it the conversation plus a list of tool schemas. It
answers either with text or with a request such as `search_traces({"service":"ledger"})`. Your code
executes that request and appends the output to the conversation. The "agent" is that `for` loop in
`agent.run()`. Everything interesting is in what you put around it.

### Guardrails

1. **Read-only by construction.** Tools can only issue GET. Fault injection lives on `/admin`
   endpoints that no tool touches.
2. **Tool-call budget** (default 12) and **token budget** (default 150k). Past either, tool calls are
   refused and the model is told to conclude.
3. **Evidence citations.** Every tool result is stamped `E1`, `E2`, ... The diagnosis must cite ids
   that were issued, and any trace id it mentions must appear verbatim in a tool result. A failing
   diagnosis is rejected and the model must correct it (two retries).
4. **Structured output.** The answer arrives through the `submit_diagnosis` tool schema, so it is
   always parseable.

### Fault scenarios

| Fault | What it looks like | What separates it from the others |
|---|---|---|
| `slow_dependency` | `db.query` span takes 400-700 ms | No recent deploy; self time is in the DB span |
| `pool_exhaustion` | DB span errors, `db_pool_in_use` pinned at max | Error message and the gauge |
| `bad_deploy` (errors) | Handler returns 500 on 70% of requests | Deploy seconds before onset, new `service.version` |
| `bad_deploy` (latency) | Handler itself is slow, DB is fast | Looks like slow_dependency until you check deploys and self time |
| `false-alarm` | Nothing | Correct answer is `none` |

A fault deep in the chain makes every upstream service look broken too. Finding the deepest service
whose own work explains the symptom is the actual task.

## Results

_Not yet measured. Replace this line with the summary block from `results.json` after running
`python3 -m triage.evalrun --runs 3`, along with the model name and date._

## Reading the results

`results.json` summary fields map to the numbers you would quote:

- `root_cause_accuracy_pct`: service and category both correct.
- `median_triage_seconds`, `mean_tool_calls`.
- `unsupported_first_submission_pct` vs `unsupported_final_pct`: how often the first answer failed
  the citation check, and how often the final one did. That pair is the before/after for the guardrail.

One pass over 9 scenarios is a small sample. Use `--runs 3` before quoting a percentage, and read the
failures: `-v` shows every tool call.

## Honest limitations

- The test bed is synthetic: four Python HTTP services with a simulated database. No Kafka, no gRPC.
- The category is chosen from a closed set of four, which is far easier than open-ended diagnosis.
- Scenarios are single-fault. Real incidents overlap.
- Time scoping relies on the alert stating a window and a cooldown between scenarios. Stale telemetry
  from a previous incident can mislead the agent, which is also true in production.
- The citation check proves the evidence exists, not that it supports the claim.

## Troubleshooting

- **Smoke reports no traces:** open http://localhost:16686 and check the service dropdown. If it is
  empty, run `docker compose logs jaeger gateway`.
- **Port already in use:** something else holds 8081-8084, 9090 or 16686. Change the left-hand port in
  `docker-compose.yml` and set `TRIAGE_SERVICES`, `PROM_URL` or `JAEGER_URL` to match.
- **Stop everything:** `docker compose down`.

## Next steps

- Port the MCP server to Go; put a real queue in the path and add consumer-lag and poison-message scenarios.
- Multi-fault and partial-failure scenarios; open-ended diagnosis graded by a rubric.
- A second model as a judge for whether cited evidence supports each claim.

## License

MIT
