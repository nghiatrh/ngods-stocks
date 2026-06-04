"""Run the eval suite against /ask and report pass/fail.

We don't compare exact JSON — only that the generated query contains the
expected semantic moves (right cube, expected measures present, etc.). Exact
matching breaks any time the LLM picks a different but equally-correct slice.

Usage (from inside the chat-api container, or with API up):
    python -m eval.run                       # against http://localhost:8001
    CHAT_API_URL=http://chat-api:8001 python -m eval.run
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
import yaml

API = os.environ.get("CHAT_API_URL", "http://localhost:8001")
CASES = Path(__file__).parent / "cases.yaml"


def _names(query: dict, key: str) -> set[str]:
    return {x for x in (query.get(key) or [])}


def _filter_members(query: dict) -> set[str]:
    return {f.get("member") or f.get("dimension")
            for f in (query.get("filters") or [])}


def _time_dim_names(query: dict) -> set[str]:
    return {td.get("dimension") for td in (query.get("timeDimensions") or [])}


def check(case: dict, resp: dict) -> list[str]:
    fails: list[str] = []
    if case.get("expect_error"):
        if not resp.get("error"):
            fails.append("expected an error, got a query")
        return fails

    if resp.get("error"):
        fails.append(f"unexpected error: {resp['error']}")
        return fails

    q = resp.get("cube_query") or {}
    measures = _names(q, "measures")
    dims = _names(q, "dimensions")
    filters = _filter_members(q)
    tdims = _time_dim_names(q)

    if (cube := case.get("expect_cube")):
        all_members = measures | dims | filters | tdims
        if not any(m.startswith(f"{cube}.") for m in all_members):
            fails.append(f"expected cube {cube} not used")

    for m in case.get("must_include_measures") or []:
        if m not in measures:
            fails.append(f"missing measure {m}")

    for d in case.get("must_include_dimensions") or []:
        if d not in dims:
            fails.append(f"missing dimension {d}")

    if (any_m := case.get("must_include_measures_any")):
        if not (set(any_m) & measures):
            fails.append(f"none of {any_m} present")

    for group in case.get("must_include_measures_any_all_of") or []:
        if not (set(group) & measures):
            fails.append(f"none of {group} present")

    if (any_d := case.get("must_include_dimensions_any")):
        if not (set(any_d) & dims):
            fails.append(f"none of {any_d} present")

    if (td := case.get("must_include_time_dim")):
        if td not in tdims:
            fails.append(f"missing time dimension {td}")

    if (member := case.get("must_include_filter_member")):
        if member not in filters:
            fails.append(f"missing filter on {member}")

    return fails


def main() -> int:
    cases = yaml.safe_load(CASES.read_text())
    passed = failed = 0
    with httpx.Client(timeout=120) as c:
        for case in cases:
            try:
                r = c.post(f"{API}/ask", json={"question": case["question"]})
                r.raise_for_status()
                resp = r.json()
            except Exception as e:
                print(f"[ERROR] {case['id']}: {e}")
                failed += 1
                continue

            fails = check(case, resp)
            if fails:
                failed += 1
                print(f"[FAIL] {case['id']}: {case['question']}")
                for f in fails:
                    print(f"   - {f}")
                if resp.get("cube_query"):
                    print(f"   query: {resp['cube_query']}")
            else:
                passed += 1
                print(f"[PASS] {case['id']}")

    total = passed + failed
    print(f"\n{passed}/{total} passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
