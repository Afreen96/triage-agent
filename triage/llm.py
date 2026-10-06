"""Thin Claude Messages API client (stdlib only). Reads ANTHROPIC_API_KEY from the environment."""
import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_MODEL = "claude-sonnet-5-5"   # override with TRIAGE_MODEL
RETRYABLE = (429, 500, 502, 503, 504, 529)


class LLMError(Exception):
    pass


class Anthropic:
    def __init__(self, api_key=None, model=None, base_url=None):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if not self.api_key:
            raise LLMError("ANTHROPIC_API_KEY is not set")
        self.model = model or os.environ.get("TRIAGE_MODEL", DEFAULT_MODEL)
        self.base_url = (base_url or os.environ.get("ANTHROPIC_BASE_URL") or "https://api.anthropic.com").rstrip("/")

    def complete(self, system, messages, tools, max_tokens=2000):
        """One model turn. Returns the raw response dict: content[], stop_reason, usage."""
        body = json.dumps({"model": self.model, "max_tokens": max_tokens, "system": system,
                           "tools": tools, "messages": messages}).encode()
        last = None
        for attempt in range(5):
            req = urllib.request.Request(self.base_url + "/v1/messages", data=body, method="POST", headers={
                "content-type": "application/json", "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01"})
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:400]
                last = "HTTP %d: %s" % (e.code, detail)
                if e.code not in RETRYABLE:
                    raise LLMError(last)
                wait = float(e.headers.get("retry-after") or 2 ** attempt)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last, wait = "network error: %s" % e, 2 ** attempt
            time.sleep(min(wait, 30))
        raise LLMError("giving up after retries: %s" % last)
