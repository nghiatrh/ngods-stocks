"""Pre- and post-execution validation.

The most valuable guardrail is *schema-grounded*: every measure/dimension/filter
name the LLM produced must exist in Cube /meta. This catches the bulk of
hallucinations before we ever hit Trino.
"""
from __future__ import annotations

from typing import Any

from .config import settings


class GuardrailError(ValueError):
    pass


def _all_members(cubes: list[dict[str, Any]]) -> tuple[set[str], set[str], set[str]]:
    measures, dims, time_dims = set(), set(), set()
    for c in cubes:
        for m in c.get("measures", []):
            measures.add(m["name"])
        for d in c.get("dimensions", []):
            dims.add(d["name"])
            if d.get("type") == "time":
                time_dims.add(d["name"])
    return measures, dims, time_dims


def validate_query(query: dict[str, Any], cubes: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate and normalize a Cube query. Raises GuardrailError on hard failures.

    Returns the (possibly modified) query — we inject a default `limit` if missing
    rather than rejecting, which would frustrate the user for a fixable issue.
    """
    if not isinstance(query, dict):
        raise GuardrailError("cube_query must be a JSON object")

    measures, dims, time_dims = _all_members(cubes)
    valid = measures | dims

    for m in query.get("measures", []) or []:
        if m not in measures:
            raise GuardrailError(f"unknown measure: {m}")

    for d in query.get("dimensions", []) or []:
        if d not in dims:
            raise GuardrailError(f"unknown dimension: {d}")

    for td in query.get("timeDimensions", []) or []:
        name = td.get("dimension")
        if name not in time_dims:
            raise GuardrailError(f"unknown time dimension: {name}")

    for f in query.get("filters", []) or []:
        member = f.get("member") or f.get("dimension")
        if member not in valid:
            raise GuardrailError(f"unknown filter member: {member}")

    for k in (query.get("order") or {}).keys():
        if k not in valid:
            raise GuardrailError(f"unknown order key: {k}")

    if not (query.get("measures") or query.get("dimensions")):
        raise GuardrailError("query has neither measures nor dimensions")

    limit = query.get("limit")
    if not isinstance(limit, int) or limit <= 0:
        query["limit"] = 50
    elif limit > settings.cube_max_rows:
        query["limit"] = settings.cube_max_rows

    return query
