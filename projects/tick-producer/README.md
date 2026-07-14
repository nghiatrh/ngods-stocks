# tick-producer

Polls the vnstock intraday API (source `KBS`) for the VN30 basket and publishes
new ticks to the Kafka topic `market.ticks`, keyed by symbol. First component of
the realtime streaming layer (producer → Kafka → Spark Structured Streaming).

See the module docstring in [producer.py](./producer.py) for the dedupe and
session-guard design.

## Configuration (env vars)

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers |
| `TICKS_TOPIC` | `market.ticks` | Output topic |
| `POLL_INTERVAL_SECONDS` | `30` | Minimum seconds between poll cycles |
| `SYMBOLS` | *(VN30)* | Comma-separated symbol override |
| `TRADING_HOURS_ONLY` | `true` | Set `false` to poll outside HOSE sessions (smoke tests) |
| `VNSTOCK_API_KEY` | *(empty)* | vnstock key; Guest tier without it |
| `VNSTOCK_MAX_CALLS_PER_MINUTE` | `55` | API budget; use `18` on Guest tier |

## Smoke test

```bash
docker compose up -d kafka kafka-init
docker compose up -d --build tick-producer
docker compose logs -f tick-producer   # expect "Cycle done ... N new ticks published"

# consume a few messages from the host
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks \
  --from-beginning --max-messages 5 --property print.key=true
```

Outside trading hours set `TRADING_HOURS_ONLY=false` in the environment — the
producer then publishes the last session's full tape once and goes quiet
(dedupe suppresses repeats).
