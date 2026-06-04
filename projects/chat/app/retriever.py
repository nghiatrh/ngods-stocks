"""Vector retrieval over rag.chunks.

Returns the matched chunks plus the *full* schema for any cube that appeared
in the top-k. The LLM then has the complete cube to work with rather than a
random subset of its measures — much fewer hallucinated columns.
"""
from __future__ import annotations

from typing import Any

from .config import settings
from .cube_client import fetch_meta
from .db import get_conn
from .embeddings import embed_one


def search(question: str, k: int | None = None) -> list[dict[str, Any]]:
    k = k or settings.retrieval_top_k
    q_vec = embed_one(question)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_type, source_id, cube_name, title, content,
                   1 - (embedding <=> %s::vector) AS score
            FROM rag.chunks
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (q_vec, q_vec, k),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def cubes_in_meta(meta: dict[str, Any], names: set[str]) -> list[dict[str, Any]]:
    return [c for c in meta.get("cubes", []) if c["name"] in names]


def assemble_context(question: str) -> dict[str, Any]:
    """Return retrieval hits + the full cube definitions they pointed at."""
    hits = search(question)
    cube_names = {h["cube_name"] for h in hits if h["cube_name"]}
    meta = fetch_meta()
    cubes = cubes_in_meta(meta, cube_names)
    return {"hits": hits, "cubes": cubes, "retrieved_ids": [h["id"] for h in hits]}
