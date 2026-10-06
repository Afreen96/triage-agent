"""Minimal MCP server over stdio (newline-delimited JSON-RPC 2.0).

MCP (Model Context Protocol) is how an AI client discovers and calls tools.
The whole protocol surface needed for a tools-only server is four methods:
  initialize   -> handshake: agree a protocol version, advertise capabilities
  tools/list   -> names, descriptions and JSON Schemas of the tools
  tools/call   -> run one tool, return its text result
  ping         -> liveness
stdout carries protocol messages only; all logging goes to stderr.

Run:  python3 -m triage.mcp_server
"""
import json
import sys

from . import tools

SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
SERVER_INFO = {"name": "triage-tools", "version": "0.1.0"}


def _log(msg):
    print("[mcp-server] %s" % msg, file=sys.stderr, flush=True)


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def dispatch(method, params):
    if method == "initialize":
        wanted = params.get("protocolVersion")
        return {
            "protocolVersion": wanted if wanted in SUPPORTED_VERSIONS else SUPPORTED_VERSIONS[0],
            "capabilities": {"tools": {}},
            "serverInfo": SERVER_INFO,
        }
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": [
            {"name": t["name"], "description": t["description"], "inputSchema": t["inputSchema"],
             "annotations": {"readOnlyHint": True}}
            for t in tools.TOOLS]}
    if method == "tools/call":
        name = params.get("name")
        text, is_error = tools.call_tool(name, params.get("arguments") or {})
        _log("tools/call %s -> %d chars%s" % (name, len(text), " (error)" if is_error else ""))
        return {"content": [{"type": "text", "text": text}], "isError": is_error}
    raise RpcError(-32601, "method not found: %s" % method)


def handle_line(line):
    """Returns a response dict, or None for notifications."""
    try:
        msg = json.loads(line)
    except ValueError:
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}
    if not isinstance(msg, dict) or "method" not in msg:
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "invalid request"}}
    if "id" not in msg:
        return None  # notification (e.g. notifications/initialized): no reply
    try:
        params = msg.get("params") or {}
        if not isinstance(params, dict):
            raise RpcError(-32602, "params must be an object")
        return {"jsonrpc": "2.0", "id": msg["id"], "result": dispatch(msg["method"], params)}
    except RpcError as e:
        return {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": e.code, "message": str(e)}}


def serve(stdin=sys.stdin, stdout=sys.stdout):
    _log("ready (%d read-only tools)" % len(tools.TOOLS))
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        resp = handle_line(line)
        if resp is not None:
            stdout.write(json.dumps(resp) + "\n")
            stdout.flush()


if __name__ == "__main__":
    serve()
