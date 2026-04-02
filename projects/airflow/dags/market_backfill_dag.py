"""
One-shot market backfill DAG.

Manually triggered via the Airflow UI or CLI.
Accepts a `from_date` parameter (YYYY-MM-DD) and backfills all calendar days
from that date up to and including today (Asia/Ho_Chi_Minh).

If data already exists for a given date it is deleted before re-ingesting,
ensuring the backfill is idempotent and safe to re-run.
"""

from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _date_range(from_date: str) -> list:
    """Return calendar days from from_date to today (ICT) as YYYY-MM-DD strings."""
    today = pendulum.today("Asia/Ho_Chi_Minh").date()
    start = pendulum.parse(from_date).date()
    if start > today:
        return []
    days = (today - start).days + 1
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


@dag(
    dag_id="market_backfill",
    description="Backfill VN30 equity OHLCV and index OHLCV from a given date to today",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "from_date": Param(
            default=str(pendulum.today("Asia/Ho_Chi_Minh").subtract(days=7).date()),
            type="string",
            format="date",
            description="Start date for backfill (YYYY-MM-DD). Backfills up to today inclusive.",
        )
    },
    tags=["market", "backfill", "bronze"],
)
def market_backfill_dag():

    @task
    def get_vn30_symbols() -> list:
        from ingestion.market import get_vn30_symbols
        return get_vn30_symbols()

    @task
    def backfill_equity_ohlcv(symbols: list, **context) -> None:
        from ingestion.market import fetch_equity_ohlcv
        from ingestion.storage import delete_market, get_fs, market_path, write_parquet

        from_date = context["params"]["from_date"]
        fs = get_fs()
        for date in _date_range(from_date):
            delete_market("equity_ohlcv", date, fs)
            df = fetch_equity_ohlcv(symbols, date)
            write_parquet(df, market_path("equity_ohlcv", date), fs)

    @task
    def backfill_index_ohlcv(**context) -> None:
        from ingestion.market import fetch_index_ohlcv
        from ingestion.storage import delete_market, get_fs, market_path, write_parquet

        from_date = context["params"]["from_date"]
        fs = get_fs()
        for date in _date_range(from_date):
            delete_market("index_ohlcv", date, fs)
            df = fetch_index_ohlcv(date)
            write_parquet(df, market_path("index_ohlcv", date), fs)

    symbols = get_vn30_symbols()
    backfill_equity_ohlcv(symbols)
    backfill_index_ohlcv()


market_backfill_dag()
