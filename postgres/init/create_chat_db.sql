-- Chat service: pgvector RAG store + query/feedback log.
-- Runs on first postgres container startup.
CREATE DATABASE chat;
GRANT ALL PRIVILEGES ON DATABASE chat TO ngods;

\connect chat

CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS rag;
CREATE SCHEMA IF NOT EXISTS log;

-- One row per indexed knowledge chunk (a cube, a measure, a dimension, a dbt column…).
-- source_type lets the retriever boost / filter by kind.
CREATE TABLE rag.chunks (
    id              BIGSERIAL PRIMARY KEY,
    source_type     TEXT NOT NULL,           -- cube | measure | dimension | dbt_model | dbt_column | example
    source_id       TEXT NOT NULL,           -- e.g. EquityPerformance.avg_close
    cube_name       TEXT,                    -- denormalized for fast filter
    title           TEXT NOT NULL,
    content         TEXT NOT NULL,           -- text fed to the embedder
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding       vector(384) NOT NULL,    -- bge-small-en-v1.5
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_type, source_id)
);

CREATE INDEX chunks_embedding_idx
    ON rag.chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX chunks_cube_idx ON rag.chunks (cube_name);

-- Q -> generated cube query -> result. Feeds the eval set and few-shot examples.
CREATE TABLE log.queries (
    id              BIGSERIAL PRIMARY KEY,
    question        TEXT NOT NULL,
    retrieved_ids   BIGINT[] NOT NULL DEFAULT '{}',
    cube_query      JSONB,                   -- the JSON we sent to /cubejs-api/v1/load
    result_rows     INT,
    error           TEXT,
    latency_ms      INT,
    model           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE log.feedback (
    id              BIGSERIAL PRIMARY KEY,
    query_id        BIGINT NOT NULL REFERENCES log.queries(id) ON DELETE CASCADE,
    rating          SMALLINT NOT NULL,       -- 1 good, -1 bad
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT ALL ON ALL TABLES IN SCHEMA rag TO ngods;
GRANT ALL ON ALL TABLES IN SCHEMA log TO ngods;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA rag TO ngods;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA log TO ngods;
