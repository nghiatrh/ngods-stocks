from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import ingest as ingest_mod
from .config import settings
from .cube_client import run_query, sanitize
from .db import get_conn
from .guardrails import GuardrailError, validate_query
from .retriever import assemble_context
from .router import generate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ngods chat-with-data", version="0.1.0")


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    query_id: int
    cube_query: dict[str, Any] | None
    result: dict[str, Any] | None
    rationale: str | None = None
    error: str | None = None
    retrieved: list[dict[str, Any]]
    latency_ms: int


class FeedbackRequest(BaseModel):
    query_id: int
    rating: int           # +1 / -1
    note: str | None = None


def _log_query(question: str, retrieved_ids: list[int], cube_query: dict | None,
               result_rows: int | None, error: str | None, latency_ms: int,
               model: str | None) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO log.queries
              (question, retrieved_ids, cube_query, result_rows, error, latency_ms, model)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING id
            """,
            (question, retrieved_ids,
             json.dumps(cube_query) if cube_query else None,
             result_rows, error, latency_ms, model),
        )
        return cur.fetchone()[0]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest")
def ingest_endpoint() -> dict[str, Any]:
    return ingest_mod.run()


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    t0 = time.monotonic()
    ctx = assemble_context(req.question)

    parsed, model = generate(req.question, ctx["cubes"])
    if "error" in parsed:
        latency = int((time.monotonic() - t0) * 1000)
        qid = _log_query(req.question, ctx["retrieved_ids"], None, None,
                         parsed["error"], latency, model)
        return AskResponse(query_id=qid, cube_query=None, result=None,
                           error=parsed["error"], retrieved=ctx["hits"],
                           latency_ms=latency)

    raw_query = parsed.get("cube_query") or {}
    rationale = parsed.get("rationale")

    try:
        # Sanitize after validation so the query we log, return and display is
        # byte-for-byte the one Cube actually executes (no null granularity,
        # no empty lists) — otherwise users copy a query that won't run.
        cube_query = sanitize(validate_query(raw_query, ctx["cubes"]))
    except GuardrailError as e:
        latency = int((time.monotonic() - t0) * 1000)
        qid = _log_query(req.question, ctx["retrieved_ids"], raw_query, None,
                         f"guardrail: {e}", latency, model)
        return AskResponse(query_id=qid, cube_query=raw_query, result=None,
                           error=f"guardrail: {e}", retrieved=ctx["hits"],
                           latency_ms=latency)

    try:
        result = run_query(cube_query)
    except Exception as e:
        err_msg = str(e)
        latency = int((time.monotonic() - t0) * 1000)
        qid = _log_query(req.question, ctx["retrieved_ids"], cube_query, None,
                         f"cube: {err_msg}", latency, model)
        return AskResponse(query_id=qid, cube_query=cube_query, result=None,
                           error=f"cube error: {err_msg}", retrieved=ctx["hits"],
                           latency_ms=latency)

    rows = result.get("data") or []
    latency = int((time.monotonic() - t0) * 1000)
    qid = _log_query(req.question, ctx["retrieved_ids"], cube_query, len(rows),
                     result.get("error"), latency, model)

    return AskResponse(
        query_id=qid,
        cube_query=cube_query,
        result={"rows": rows, "annotation": result.get("annotation", {})},
        rationale=rationale,
        retrieved=ctx["hits"],
        latency_ms=latency,
    )


@app.post("/feedback")
def feedback(req: FeedbackRequest) -> dict[str, Any]:
    if req.rating not in (-1, 1):
        raise HTTPException(400, "rating must be +1 or -1")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO log.feedback (query_id, rating, note) VALUES (%s, %s, %s) RETURNING id",
            (req.query_id, req.rating, req.note),
        )
        fid = cur.fetchone()[0]
    return {"id": fid}
