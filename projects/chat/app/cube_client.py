import time
from typing import Any

import httpx
import jwt

from .config import settings


def _token() -> str:
    payload = {"iat": int(time.time()), "exp": int(time.time()) + 3600}
    return jwt.encode(payload, settings.cube_api_secret, algorithm="HS256")


def _headers() -> dict[str, str]:
    return {"Authorization": _token(), "Content-Type": "application/json"}


def fetch_meta() -> dict[str, Any]:
    url = f"{settings.cube_api_url}/cubejs-api/v1/meta"
    with httpx.Client(timeout=10) as c:
        r = c.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


def _sanitize(query: dict[str, Any]) -> dict[str, Any]:
    """Strip fields that Cube rejects: null granularity, empty lists, null values."""
    q = {k: v for k, v in query.items() if v is not None and v != [] and v != {}}

    # granularity: null is invalid — omit the key entirely
    if "timeDimensions" in q:
        cleaned = []
        for td in q["timeDimensions"]:
            td = {k: v for k, v in td.items() if v is not None}
            if td:
                cleaned.append(td)
        q["timeDimensions"] = cleaned or None
        if not q["timeDimensions"]:
            del q["timeDimensions"]

    return q


def run_query(query: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
    """Execute a Cube REST query. Returns the parsed response.

    Cube /load can return 'Continue wait' (HTTP 200 with `error` set) while a
    pre-aggregation builds — poll briefly before giving up.
    """
    url = f"{settings.cube_api_url}/cubejs-api/v1/load"
    clean = _sanitize(query)
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=timeout) as c:
        while True:
            r = c.post(url, headers=_headers(), json={"query": clean})
            if r.status_code == 400:
                raise httpx.HTTPStatusError(
                    f"{r.status_code}: {r.text}", request=r.request, response=r
                )
            r.raise_for_status()
            data = r.json()
            if data.get("error") == "Continue wait" and time.monotonic() < deadline:
                time.sleep(1)
                continue
            return data
