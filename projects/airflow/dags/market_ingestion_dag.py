"""
Daily market ingestion DAG.

Runs on weekdays at 17:00 ICT (10:00 UTC), after Vietnam market closes (15:15 ICT).
Ingests yesterday's data for:
  - Equity OHLCV       → bronze/market/equity_ohlcv/date=YYYY-MM-DD/
  - Equity trades      → bronze/market/equity_trades/date=YYYY-MM-DD/
  - Equity order book  → bronze/market/equity_order_book/date=YYYY-MM-DD/
  - Index OHLCV        → bronze/market/index_ohlcv/date=YYYY-MM-DD/
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="market_ingestion",
    description="Daily ingestion of VN30 equity and index market data into bronze zone",
    schedule="0 10 * * 1-5",  # 10:00 UTC = 17:00 ICT, Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["market", "ingestion", "bronze"],
)
def market_ingestion_dag():

    @task
    def get_vn30_symbols() -> list:
        from ingestion.market import get_vn30_symbols
        return get_vn30_symbols()

    @task
    def ingest_equity_ohlcv(symbols: list) -> None:
        from ingestion.market import fetch_equity_ohlcv
        from ingestion.storage import get_fs, market_path, write_parquet
        import pendulum

        date = pendulum.yesterday("Asia/Ho_Chi_Minh").to_date_string()
        df = fetch_equity_ohlcv(symbols, date)
        fs = get_fs()
        write_parquet(df, market_path("equity_ohlcv", date), fs)

    @task
    def ingest_equity_trades(symbols: list) -> None:
        from ingestion.market import fetch_equity_trades
        from ingestion.storage import get_fs, market_path, write_parquet
        import pendulum

        date = pendulum.yesterday("Asia/Ho_Chi_Minh").to_date_string()
        df = fetch_equity_trades(symbols, date)
        fs = get_fs()
        write_parquet(df, market_path("equity_trades", date), fs)

    @task
    def ingest_equity_order_book(symbols: list) -> None:
        from ingestion.market import fetch_equity_order_book
        from ingestion.storage import get_fs, market_path, write_parquet
        import pendulum

        date = pendulum.yesterday("Asia/Ho_Chi_Minh").to_date_string()
        df = fetch_equity_order_book(symbols, date)
        fs = get_fs()
        write_parquet(df, market_path("equity_order_book", date), fs)

    @task
    def ingest_index_ohlcv() -> None:
        from ingestion.market import fetch_index_ohlcv
        from ingestion.storage import get_fs, market_path, write_parquet
        import pendulum

        date = pendulum.yesterday("Asia/Ho_Chi_Minh").to_date_string()
        df = fetch_index_ohlcv(date)
        fs = get_fs()
        write_parquet(df, market_path("index_ohlcv", date), fs)

    # DAG topology: fetch symbols first, then run all ingestion tasks in parallel
    symbols = get_vn30_symbols()
    ingest_equity_ohlcv(symbols)
    ingest_equity_trades(symbols)
    ingest_equity_order_book(symbols)
    ingest_index_ohlcv()


market_ingestion_dag()
