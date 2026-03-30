"""
Weekly reference data ingestion DAG.

Runs every Sunday at 02:00 ICT (19:00 UTC Saturday).
Truncates and reloads all reference datasets:
  - Equity listing    → bronze/reference/equity_listing/data.parquet
  - Index listing     → bronze/reference/index_listing/data.parquet
  - Industry (ICB)    → bronze/reference/industry/data.parquet
  - Corporate events  → bronze/reference/events/data.parquet
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="reference_ingestion",
    description="Weekly truncate-and-reload of VN equity reference data into bronze zone",
    schedule="0 19 * * 6",  # 19:00 UTC Saturday = 02:00 ICT Sunday
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reference", "ingestion", "bronze"],
)
def reference_ingestion_dag():

    @task
    def get_vn30_symbols() -> list:
        from ingestion.market import get_vn30_symbols
        return get_vn30_symbols()

    @task
    def ingest_equity_listing() -> None:
        from ingestion.reference import fetch_equity_listing
        from ingestion.storage import delete_reference, get_fs, reference_path, write_parquet

        fs = get_fs()
        delete_reference("equity_listing", fs)
        df = fetch_equity_listing()
        write_parquet(df, reference_path("equity_listing"), fs)

    @task
    def ingest_index_listing() -> None:
        from ingestion.reference import fetch_index_listing
        from ingestion.storage import delete_reference, get_fs, reference_path, write_parquet

        fs = get_fs()
        delete_reference("index_listing", fs)
        df = fetch_index_listing()
        write_parquet(df, reference_path("index_listing"), fs)

    @task
    def ingest_industry() -> None:
        from ingestion.reference import fetch_industry
        from ingestion.storage import delete_reference, get_fs, reference_path, write_parquet

        fs = get_fs()
        delete_reference("industry", fs)
        df = fetch_industry()
        write_parquet(df, reference_path("industry"), fs)

    @task
    def ingest_events(symbols: list) -> None:
        from ingestion.reference import fetch_events
        from ingestion.storage import delete_reference, get_fs, reference_path, write_parquet

        fs = get_fs()
        delete_reference("events", fs)
        df = fetch_events(symbols)
        write_parquet(df, reference_path("events"), fs)

    # Fetch symbols first, then run all ingestion tasks in parallel
    symbols = get_vn30_symbols()
    ingest_equity_listing()
    ingest_index_listing()
    ingest_industry()
    ingest_events(symbols)


reference_ingestion_dag()
