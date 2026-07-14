"""
Realtime tick producer: polls the vnstock intraday API and publishes new
trades to Kafka.

vnstock is a polling REST API (no push feed), so "realtime" means micro-polling:
every POLL_INTERVAL_SECONDS the producer fetches the current session's tick
tape per symbol, deduplicates against what it already published, and sends only
the new ticks to the TICKS_TOPIC keyed by symbol.

Key design points:
  - source=KBS: the VCI intraday endpoint currently raises TypeError('page').
  - Dedupe: intraday() returns the cumulative session tape on every poll. Each
    tick gets a stable key = id (when the API provides one) or
    (time, price, volume, occurrence-index) otherwise; keys already published
    this session are skipped. The seen-set resets when the trading date rolls.
  - Session guard: polls only Mon-Fri 08:55-11:35 and 12:55-15:10 ICT
    (HOSE/HNX hours plus a small buffer). Disable with TRADING_HOURS_ONLY=false
    to backfill the current session's tape once, e.g. for smoke tests.
  - Rate limit: in-process sliding window (single process, unlike the Airflow
    cross-process limiter). Stays below the vnstock per-minute cap.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd
from confluent_kafka import Producer
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("tick-producer")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SOURCE = "KBS"
LISTING_SOURCE = "KBS"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TICKS_TOPIC = os.environ.get("TICKS_TOPIC", "market.ticks")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
TRADING_HOURS_ONLY = os.environ.get("TRADING_HOURS_ONLY", "true").lower() != "false"
MAX_CALLS_PER_MINUTE = int(os.environ.get("VNSTOCK_MAX_CALLS_PER_MINUTE", "55"))
INTRADAY_PAGE_SIZE = int(os.environ.get("INTRADAY_PAGE_SIZE", "50000"))

# Trading windows in ICT minutes-of-day: ATO through morning close, and
# afternoon open through ATC + put-through tail.
_SESSION_WINDOWS = [(8 * 60 + 55, 11 * 60 + 35), (12 * 60 + 55, 15 * 60 + 10)]

_running = True


def _handle_signal(signum, frame):
    global _running
    log.info("Received signal %s, shutting down after current cycle", signum)
    _running = False


class RateLimiter:
    """In-process sliding-window limiter for vnstock API calls."""

    def __init__(self, max_calls: int, window_seconds: float = 60.0):
        self.max_calls = max_calls
        self.window = window_seconds
        self.timestamps: List[float] = []

    def throttle(self) -> None:
        now = time.time()
        self.timestamps = [t for t in self.timestamps if now - t < self.window]
        if len(self.timestamps) >= self.max_calls:
            wait = self.window - (now - min(self.timestamps)) + 0.05
            if wait > 0:
                log.debug("Rate limit reached, sleeping %.2fs", wait)
                time.sleep(wait)
                now = time.time()
                self.timestamps = [t for t in self.timestamps if now - t < self.window]
        self.timestamps.append(time.time())


def setup_vnstock_auth() -> None:
    """Register the vnstock API key if VNSTOCK_API_KEY is set (same as Airflow ingestion)."""
    api_key = os.environ.get("VNSTOCK_API_KEY", "").strip()
    if not api_key:
        log.warning(
            "VNSTOCK_API_KEY is not set — running as Guest tier (20 req/min). "
            "Consider lowering VNSTOCK_MAX_CALLS_PER_MINUTE to 18."
        )
        return
    try:
        from vnstock import register_user
        register_user(api_key=api_key)
        log.info("vnstock authenticated (key: %s...%s)", api_key[:4], api_key[-4:])
    except Exception as e:
        log.warning("vnstock auth failed, continuing as Guest: %s", e)


def in_trading_session(now_ict: datetime) -> bool:
    if now_ict.weekday() >= 5:
        return False
    minute = now_ict.hour * 60 + now_ict.minute
    return any(start <= minute <= end for start, end in _SESSION_WINDOWS)


def load_symbols(limiter: RateLimiter) -> List[str]:
    """SYMBOLS env override (comma-separated), otherwise the VN30 basket."""
    override = os.environ.get("SYMBOLS", "").strip()
    if override:
        symbols = [s.strip().upper() for s in override.split(",") if s.strip()]
        log.info("Using %d symbols from SYMBOLS env", len(symbols))
        return symbols
    limiter.throttle()
    from vnstock import Listing
    listing = Listing(source=LISTING_SOURCE)
    symbols = list(listing.symbols_by_group(group_name="VN30"))
    log.info("Fetched %d VN30 symbols", len(symbols))
    return symbols


def tick_records(df: pd.DataFrame, symbol: str) -> List[dict]:
    """
    Normalize an intraday frame into tick dicts with a stable dedupe key.

    The session tape is cumulative and append-only, so when the API has no id
    column, (time, price, volume, occurrence-index) is stable across polls:
    the Nth identical (t, p, v) tick keeps the same occurrence index next poll.
    """
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    if "time" not in df.columns or "price" not in df.columns:
        log.warning("%s: unexpected intraday columns %s", symbol, list(df.columns))
        return []
    vol_col = "volume" if "volume" in df.columns else "vol"

    records = []
    if "id" in df.columns and df["id"].notna().all():
        keys = df["id"].astype(str)
    else:
        occurrence = df.groupby(["time", "price", vol_col]).cumcount()
        keys = (
            df["time"].astype(str) + "|" + df["price"].astype(str)
            + "|" + df[vol_col].astype(str) + "|" + occurrence.astype(str)
        )

    times = pd.to_datetime(df["time"], errors="coerce")
    ingested_at = datetime.now(timezone.utc).isoformat()
    for i, (_, row) in enumerate(df.iterrows()):
        ts = times.iloc[i]
        if pd.isna(ts):
            continue
        if ts.tzinfo is None:
            ts = ts.tz_localize(VN_TZ)
        records.append({
            "key": f"{symbol}|{keys.iloc[i]}",
            "value": {
                "symbol": symbol,
                "event_time": ts.isoformat(),
                "price": float(row["price"]),
                "volume": int(row[vol_col]) if pd.notna(row[vol_col]) else None,
                "match_type": str(row["match_type"]) if "match_type" in df.columns else None,
                "source": SOURCE,
                "ingested_at": ingested_at,
            },
        })
    return records


def _delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    setup_vnstock_auth()
    limiter = RateLimiter(MAX_CALLS_PER_MINUTE)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    log.info(
        "Producing to %s on %s every %ds (trading hours only: %s)",
        TICKS_TOPIC, KAFKA_BOOTSTRAP_SERVERS, POLL_INTERVAL_SECONDS, TRADING_HOURS_ONLY,
    )

    from vnstock import Quote

    symbols: List[str] = []
    seen: Dict[str, Set[str]] = {}
    session_date: Optional[str] = None
    published_today = 0

    while _running:
        now_ict = datetime.now(VN_TZ)

        if TRADING_HOURS_ONLY and not in_trading_session(now_ict):
            log.info("Outside trading session (%s ICT), sleeping 60s", now_ict.strftime("%a %H:%M"))
            time.sleep(60)
            continue

        today = now_ict.strftime("%Y-%m-%d")
        if today != session_date:
            session_date = today
            seen = {}
            published_today = 0
            try:
                symbols = load_symbols(limiter)
            except Exception as e:
                if not symbols:
                    log.error("Cannot load symbol list, retrying in 60s: %s", e)
                    time.sleep(60)
                    session_date = None
                    continue
                log.warning("Symbol refresh failed, keeping previous list: %s", e)
            log.info("New session %s: tracking %d symbols", session_date, len(symbols))

        cycle_start = time.time()
        cycle_new = 0
        for symbol in symbols:
            if not _running:
                break
            limiter.throttle()
            try:
                df = Quote(symbol=symbol, source=SOURCE).intraday(page_size=INTRADAY_PAGE_SIZE)
            except Exception as e:
                log.warning("Intraday fetch failed for %s: %s", symbol, e)
                continue
            if df is None or df.empty:
                continue

            symbol_seen = seen.setdefault(symbol, set())
            for record in tick_records(df, symbol):
                if record["key"] in symbol_seen:
                    continue
                symbol_seen.add(record["key"])
                producer.produce(
                    TICKS_TOPIC,
                    key=record["value"]["symbol"],
                    value=json.dumps(record["value"]),
                    on_delivery=_delivery_report,
                )
                cycle_new += 1
            producer.poll(0)

        producer.flush(30)
        published_today += cycle_new
        log.info(
            "Cycle done in %.1fs: %d new ticks published (%d total for %s)",
            time.time() - cycle_start, cycle_new, published_today, session_date,
        )

        remaining = POLL_INTERVAL_SECONDS - (time.time() - cycle_start)
        if remaining > 0 and _running:
            time.sleep(remaining)

    producer.flush(10)
    log.info("Shutdown complete")


if __name__ == "__main__":
    main()
