from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from urllib.parse import urlparse

from ..contracts import ContractError, encoded


@dataclass(frozen=True)
class Response:
    status: int
    data: dict


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HttpTransport:
    """Endpoints come from trusted host setup, never from model or document text."""
    def __init__(self, *, allow_localhost: bool = False):
        self.allow_localhost = allow_localhost

    async def post(self, url: str, headers: dict, body: bytes, *, timeout: float) -> Response:
        parsed = urlparse(url)
        if parsed.username or parsed.password or not parsed.hostname or parsed.fragment:
            raise ContractError("invalid_endpoint")
        if parsed.scheme != "https" and not (self.allow_localhost and parsed.scheme == "http"
                and parsed.hostname in {"localhost", "127.0.0.1", "::1"}):
            raise ContractError("https_required")
        def request():
            req = urllib.request.Request(url, body, headers, method="POST")
            opener = urllib.request.build_opener(NoRedirect)
            try:
                response = opener.open(req, timeout=timeout)
            except urllib.error.HTTPError as error:
                # Do not expose response bodies or credential-bearing URLs in failures.
                return Response(error.code, {})
            with response:
                raw = response.read(262145)
                if len(raw) > 262144:
                    raise ContractError("provider_response_too_large")
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ContractError("invalid_provider_response")
                return Response(response.status, data)
        return await asyncio.to_thread(request)


def json_bytes(value: dict) -> bytes:
    return encoded(value).encode()
