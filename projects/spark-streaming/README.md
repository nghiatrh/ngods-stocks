# spark-streaming

Steps 2–3 of the realtime layer: two Spark Structured Streaming jobs off the
same Kafka topic.

- **Cold path** ([ticks_to_bronze.py](./ticks_to_bronze.py), service
  `tick-stream`): appends raw ticks into the Iceberg table
  `warehouse.bronze.ticks_stream`, so intraday tick history becomes queryable
  in Trino/dbt the same way every other bronze table is, and any hot-path
  metric can be rebuilt by replaying this table.
- **Hot path** ([ticks_to_realtime.py](./ticks_to_realtime.py), service
  `tick-metrics`): aggregates 1-minute bars per symbol (OHLC, volume,
  notional, VWAP, trade count, buy/sell volume) and upserts them into
  Postgres `realtime.bars_1m` (primary key `symbol, bar_start`), also
  publishing each updated bar to the `market.metrics.1m` topic. Update output
  mode + 5-minute watermark: late ticks re-emit their bar and the upsert
  overwrites it. `startingOffsets=earliest` means a fresh checkpoint rebuilds
  the hot store from whatever Kafka still retains (7 days) — it heals itself.

Both apps run with `spark.cores.max=2` — a standalone-mode app otherwise
grabs every worker core, which would starve the other app and Kyuubi/dbt
(8-core worker: 2 + 2 streaming, 4 left for dbt runs).

## How it runs

This does **not** build a new image. The `tick-stream` service in
`docker-compose.yml` reuses the `aio` image (same Dockerfile, same Iceberg
JDBC catalog + S3A config in `/opt/spark/conf/spark-defaults.conf`, same
`/opt/spark/jars`), but overrides the entrypoint to skip `aio`'s normal startup
(Spark master/worker, Kyuubi, Airflow) and instead runs `spark-submit` in
client mode against the standalone master already running in the `aio`
container (`spark://spark:7077`). Only the job script is mounted in; the Kafka
connector jars are not baked into the shared `aio` image, so they're pulled at
submit time via `--packages` (cached in the container's ivy dir across
restarts thanks to the named volume).

`aio` must be up first (it hosts the Spark master/worker and creates the
`warehouse.bronze` namespace on boot). Start both:

```bash
docker compose up -d aio
docker compose up -d --build tick-stream
```

## Configuration (env vars)

Shared: `KAFKA_BOOTSTRAP_SERVERS` (`kafka:9092`), `TICKS_TOPIC`
(`market.ticks`), `TRIGGER_INTERVAL` (`30 seconds`), and a per-job
`CHECKPOINT_PATH` on MinIO (`s3a://warehouse/checkpoints/ticks_stream` and
`.../bars_1m`).

| Variable | Job | Default | Purpose |
|---|---|---|---|
| `BRONZE_TABLE` | cold | `warehouse.bronze.ticks_stream` | Destination Iceberg table (created if missing) |
| `PG_DSN` | hot | `postgresql://ngods:ngods@postgres:5432/ngods` | Postgres for `realtime.bars_1m` (schema/table created if missing) |
| `METRICS_TOPIC` | hot | `market.metrics.1m` | Kafka topic for updated bars |
| `WATERMARK` | hot | `5 minutes` | Late-tick tolerance on event time |

## Verify

```bash
docker compose logs -f tick-stream    # cold path micro-batch progress
docker compose logs -f tick-metrics   # hot path: "Batch N: upserted M bars"

# cold path: row count via spark-sql in the aio container
docker compose exec aio spark-sql -e "SELECT count(*), max(event_time) FROM warehouse.bronze.ticks_stream;"

# hot path: latest bars in Postgres
docker compose exec postgres psql -U ngods -d ngods \
  -c "SELECT * FROM realtime.bars_1m ORDER BY bar_start DESC LIMIT 10;"
```

Or query `warehouse.bronze.ticks_stream` from Trino once rows have landed.

## Known follow-ups (not yet done)

- **Small files**: a 30s trigger against a low-traffic topic produces many
  small Iceberg data files. Add a nightly Airflow DAG running
  `CALL warehouse.system.rewrite_data_files(...)` and snapshot expiry, as
  already noted in the realtime rollout plan.
- **Checkpoint vs. topic retention**: if `tick-stream` is stopped for longer
  than `market.ticks`'s 7-day retention, the checkpoint's offsets may fall
  outside the retained range. `failOnDataLoss=false` lets the job skip ahead
  rather than fail, but that means silent gaps — fine for a lab setup, worth
  hardening (alerting on offset gaps) before anything closer to production.
- Step 4 wires the serving layer: a Cube data source over
  `realtime.bars_1m` + a Metabase live dashboard + the OpenMetadata Kafka
  connector.
