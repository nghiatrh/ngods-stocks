# chat-with-data

A Cube.js-grounded chat agent for the ngods Vietnamese-equities warehouse.
Architecture is **semantic-layer-first**: the LLM never sees Trino SQL — it
sees Cube measures + dimensions and emits Cube REST queries. This dramatically
shrinks the hallucination surface and keeps every metric definition governed.

## Pieces

| File | Role |
| --- | --- |
| `app/ingest.py` | Pulls Cube `/meta` + dbt `manifest.json`, chunks at cube/measure/dimension granularity, embeds with **fastembed** (`bge-small-en-v1.5`, 384-dim) and writes to `chat.rag.chunks` (pgvector). |
| `app/retriever.py` | Vector search; returns the *full* cube schemas behind the top-k hits so the LLM has every measure/dimension to choose from. |
| `app/router.py` | Single Claude call (default `claude-sonnet-4-6`) → JSON Cube query. |
| `app/guardrails.py` | Schema-grounded: every measure / dimension / filter / order key must exist in `/meta`, otherwise reject before execution. |
| `app/cube_client.py` | Calls `/cubejs-api/v1/load`; mints HS256 dev JWT. |
| `app/main.py` | FastAPI: `/ingest`, `/ask`, `/feedback`, `/health`. |
| `ui/chainlit_app.py` | Co-pilot UI: question → query → table, with thumbs up/down. |
| `eval/cases.yaml` | Hand-curated semantic checks (right cube, right measures). Add a case every time the model gets something wrong. |

## One-time setup

The `postgres` service was switched to `pgvector/pgvector:pg16`. If you have
existing data in `./data/postgres`, the new image will pick it up — but
`postgres/init/create_chat_db.sql` only runs on a fresh data dir. To pick it
up on an existing installation:

```bash
docker compose exec postgres psql -U ngods -f /docker-entrypoint-initdb.d/create_chat_db.sql
```

Set your Anthropic key:

```bash
echo 'ANTHROPIC_API_KEY=sk-ant-...' >> .env
```

## Run

```bash
docker compose up -d chat-api chat-ui

# Build the RAG index (run again whenever you add/change a cube or dbt schema.yml)
curl -X POST http://localhost:8001/ingest

# Ask a question
curl -X POST http://localhost:8001/ask \
  -H 'content-type: application/json' \
  -d '{"question":"Which stocks had the biggest gain today?"}' | jq

# Use the UI
open http://localhost:8501
```

## Eval

```bash
docker compose exec chat-api python -m eval.run
```

Each case asserts the *semantic moves* (right cube, expected measures present),
not exact JSON. Add a case any time you find a regression.

## What's *not* in v1 (intentional)

- **No prose summaries** — co-pilot mode shows the table directly. Adding a
  natural-language summary is a one-prompt change but it makes errors much
  harder to spot.
- **No text-to-SQL fallback** — if no cube fits, you get an error. Fallback
  is a much bigger guardrail surface; tackle it after the cube path is solid.
- **No OpenMetadata ingestion into the RAG store** — Cube + dbt covers the
  visible-to-LLM surface. Pulling the OpenMetadata catalog (glossary, lineage)
  into the retriever makes sense once you have a glossary worth retrieving.
- **Logging is in Postgres** (`chat.log.queries`, `chat.log.feedback`), not
  Iceberg. Stream to Iceberg when you actually want to analyze it.

## When to re-ingest

After any of these:
- editing a cube YAML in `conf/cube/model/`
- editing a `schema.yml` in `projects/dbt/*/models/`
- running `dbt compile` to refresh `manifest.json`

`/ingest` truncates and rebuilds — it's idempotent and takes seconds.
