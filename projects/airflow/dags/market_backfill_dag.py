"""
One-shot market backfill DAG.

Manually triggered to backfill OHLCV and index OHLCV for the last 7 calendar days.
Does not schedule automatically — run once via the Airflow UI or CLI.
"""

from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _last_7_dates() -> list:
    """Return the last 7 calendar days as YYYY-MM-DD strings (most recent last)."""
    today = pendulum.today("Asia/Ho_Chi_Minh")
    return [(today - timedelta(days=i)).to_date_string() for i in range(7, 0, -1)]


@dag(
    dag_id="market_backfill",
    description="One-shot backfill of VN30 equity OHLCV and index OHLCV for the last 7 days",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["market", "backfill", "bronze"],
)
def market_backfill_dag():

    @task
    def get_vn30_symbols() -> list:
        from ingestion.market import get_vn30_symbols
        return get_vn30_symbols()

    @task
    def backfill_equity_ohlcv(symbols: list) -> None:
        from ingestion.market import fetch_equity_ohlcv
        from ingestion.storage import get_fs, market_path, write_parquet

        fs = get_fs()
        for date in _last_7_dates():
            df = fetch_equity_ohlcv(symbols, date)
            write_parquet(df, market_path("equity_ohlcv", date), fs)

    @task
    def backfill_index_ohlcv() -> None:
        from ingestion.market import fetch_index_ohlcv
        from ingestion.storage import get_fs, market_path, write_parquet

        fs = get_fs()
        for date in _last_7_dates():
            df = fetch_index_ohlcv(date)
            write_parquet(df, market_path("index_ohlcv", date), fs)

    # Fetch symbols first, then run both backfills in parallel
    symbols = get_vn30_symbols()
    backfill_equity_ohlcv(symbols)
    backfill_index_ohlcv()


market_backfill_dag()
