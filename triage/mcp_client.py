"""Minimal MCP client: spawns a server subprocess and talks JSON-RPC over its stdio."""
import json
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class MCPError(Exception):
    pass


class MCPClient:
    def __init__(self, cmd=None, quiet=True):
        cmd = cmd or [sys.executable, "-m", "triage.mcp_server"]
        self._proc = subprocess.Popen(
            cmd, cwd=REPO_ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL if quiet else None, text=True, bufsize=1)
        self._next_id = 0
        info = self._request("initialize", {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "triage-agent", "version": "0.1.0"}})
        self.server_info = info.get("serverInfo", {})
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _send(self, msg):
        try:
            self._proc.stdin.write(json.dumps(msg) + "\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError):
            raise MCPError("MCP server process is not running")

    def _request(self, method, params):
        self._next_id += 1
        rid = self._next_id
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        while True:
            line = self._proc.stdout.readline()
            if not line:
                raise MCPError("MCP server closed the connection")
            try:
                msg = json.loads(line)
            except ValueError:
                continue  # ignore stray non-protocol output
            if msg.get("id") != rid:
                continue  # server notification or unrelated message
            if "error" in msg:
                raise MCPError("%s failed: %s" % (method, msg["error"].get("message")))
            return msg.get("result") or {}

    def list_tools(self):
        return self._request("tools/list", {}).get("tools", [])

    def call_tool(self, name, arguments):
        """Returns (text, is_error)."""
        res = self._request("tools/call", {"name": name, "arguments": arguments})
        text = "\n".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
        return text, bool(res.get("isError"))

    def close(self):
        try:
            self._proc.stdin.close()
            self._proc.wait(timeout=3)
            self._proc.stdout.close()
        except Exception:
            self._proc.kill()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
