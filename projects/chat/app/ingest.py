"""Build the RAG index from Cube /meta + dbt manifest.

Run via: POST /ingest  (or `python -m app.ingest` inside the container).

We embed at three granularities:
  - cube  : overview chunk per cube
  - measure / dimension : one chunk per leaf, enriched with dbt column docs
This lets retrieval pull just the relevant leaves for narrow questions while
still surfacing whole cubes for broad ones.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

from .config import settings
from .cube_client import fetch_meta
from .db import get_conn
from .embeddings import embed

logger = logging.getLogger(__name__)


def _load_dbt_columns() -> dict[str, dict[str, str]]:
    """Return {model_name: {column_name: description}} from dbt manifest."""
    path = Path(settings.dbt_manifest_path)
    if not path.exists():
        logger.warning("dbt manifest not found at %s — skipping column enrichment", path)
        return {}
    manifest = json.loads(path.read_text())
    out: dict[str, dict[str, str]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        cols = {c: (info.get("description") or "") for c, info in node.get("columns", {}).items()}
        out[node["name"]] = cols
    return out


_BARE_COLUMN_RE = re.compile(r"^[A-Za-z_]\w*$")
_CUBE_COLUMN_RE = re.compile(r"\{CUBE\}\.([A-Za-z_]\w*)")


def _underlying_column(leaf: dict[str, Any]) -> str:
    """Resolve a measure/dimension to its underlying dbt column name.

    Cube fields are often renamed/aggregated (e.g. measure `avg_close` over
    `sql: close`), so we map back to the raw column via the `sql:` expression:
      - bare identifier  (`close`)                  → that column
      - single-column expr (`CAST({CUBE}.x AS …)`)  → that one column
      - multi-column expr (`a / NULLIF(b, 0)`, CONCAT(…)) or no sql
                                                    → fall back to field name
    """
    sql = (leaf.get("sql") or "").strip()
    if sql and _BARE_COLUMN_RE.match(sql):
        return sql
    cols = set(_CUBE_COLUMN_RE.findall(sql))
    if len(cols) == 1:
        return cols.pop()
    return leaf["name"].split(".")[-1]


def _load_cube_field_docs(dbt_models: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    """Return {cube_name: {field_name: dbt_description}} by parsing Cube YAML.

    The Cube /meta API exposes neither sql_table nor a field's underlying sql
    column, so we read the YAML files directly: resolve each cube's dbt model
    from `sql_table`, then map every measure/dimension to its raw column and
    pull that column's dbt description.
    """
    conf_dir = Path(settings.cube_conf_path)
    if not conf_dir.exists():
        logger.warning("Cube conf path not found at %s — dbt enrichment disabled", conf_dir)
        return {}
    out: dict[str, dict[str, str]] = {}
    for yml_file in conf_dir.glob("*.yml"):
        try:
            doc = yaml.safe_load(yml_file.read_text())
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", yml_file, exc)
            continue
        for cube in doc.get("cubes", []) or []:
            name = cube.get("name")
            sql_table = cube.get("sql_table")
            if not name or not sql_table:
                continue
            table = sql_table.split(".")[-1]      # gold.rpt_stock_performance → rpt_stock_performance
            dbt_cols = dbt_models.get(table, {})
            if not dbt_cols:
                logger.warning("No dbt model %r for cube %r — enrichment skipped", table, name)
                continue
            field_docs: dict[str, str] = {}
            for leaf in (cube.get("measures") or []) + (cube.get("dimensions") or []):
                field = (leaf.get("name") or "").split(".")[-1]
                col = _underlying_column(leaf)
                desc = dbt_cols.get(col, "")
                if field and desc:
                    field_docs[field] = desc
            out[name] = field_docs
            logger.info("Cube %s → table %s: enriched %d/%d fields from dbt",
                        name, table,
                        len(field_docs),
                        len(cube.get("measures") or []) + len(cube.get("dimensions") or []))
    return out


def _cube_chunk(cube: dict[str, Any]) -> dict[str, Any]:
    name = cube["name"]
    measures = ", ".join(m["name"].split(".")[-1] for m in cube.get("measures", []))
    dimensions = ", ".join(d["name"].split(".")[-1] for d in cube.get("dimensions", []))
    body = (
        f"Cube: {name}\n"
        f"Title: {cube.get('title', '')}\n"
        f"Description: {cube.get('description', '')}\n"
        f"Measures: {measures}\n"
        f"Dimensions: {dimensions}"
    )
    return {
        "source_type": "cube",
        "source_id": name,
        "cube_name": name,
        "title": cube.get("title") or name,
        "content": body,
        "metadata": {"measures": [m["name"] for m in cube.get("measures", [])],
                     "dimensions": [d["name"] for d in cube.get("dimensions", [])]},
    }


def _leaf_chunk(cube: dict[str, Any], leaf: dict[str, Any], kind: str,
                dbt_cols: dict[str, str]) -> dict[str, Any]:
    qualified = leaf["name"]                    # e.g. EquityPerformance.avg_close
    short = qualified.split(".")[-1]
    dbt_doc = dbt_cols.get(short, "")
    body = (
        f"{kind.capitalize()}: {qualified}\n"
        f"Cube: {cube['name']} ({cube.get('title', '')})\n"
        f"Title: {leaf.get('title', '')}\n"
        f"Type: {leaf.get('type', '')}\n"
        f"Description: {leaf.get('description', '') or dbt_doc}\n"
    )
    return {
        "source_type": kind,
        "source_id": qualified,
        "cube_name": cube["name"],
        "title": leaf.get("title") or qualified,
        "content": body,
        "metadata": {"format": leaf.get("format"), "type": leaf.get("type")},
    }


def build_chunks() -> list[dict[str, Any]]:
    meta = fetch_meta()
    dbt_models = _load_dbt_columns()
    cube_field_docs = _load_cube_field_docs(dbt_models)
    chunks: list[dict[str, Any]] = []
    for cube in meta.get("cubes", []):
        if cube.get("isVisible") is False:
            continue
        chunks.append(_cube_chunk(cube))
        # {field_name: dbt_description} already resolved through each field's sql column
        field_docs = cube_field_docs.get(cube["name"], {})
        for m in cube.get("measures", []):
            chunks.append(_leaf_chunk(cube, m, "measure", field_docs))
        for d in cube.get("dimensions", []):
            chunks.append(_leaf_chunk(cube, d, "dimension", field_docs))
    return chunks


def upsert(chunks: list[dict[str, Any]]) -> int:
    if not chunks:
        return 0
    vectors = embed([c["content"] for c in chunks])
    rows = [
        (c["source_type"], c["source_id"], c["cube_name"], c["title"],
         c["content"], json.dumps(c["metadata"]), v)
        for c, v in zip(chunks, vectors)
    ]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE rag.chunks RESTART IDENTITY")
        cur.executemany(
            """
            INSERT INTO rag.chunks
                (source_type, source_id, cube_name, title, content, metadata, embedding)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::vector)
            """,
            rows,
        )
    return len(rows)


def run() -> dict[str, Any]:
    chunks = build_chunks()
    n = upsert(chunks)
    return {"chunks": n, "cubes": sorted({c["cube_name"] for c in chunks if c["cube_name"]})}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run(), indent=2))
