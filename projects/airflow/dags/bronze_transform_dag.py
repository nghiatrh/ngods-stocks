"""
Bronze transformation DAG.

Runs the bronze_vnstock dbt project via Spark (Kyuubi Thrift).

Market models   → incremental append into warehouse.bronze.stg_equity_* / stg_index_*
Reference models → full table refresh into warehouse.bronze.stg_equity_listing etc.

Schedule: 30 minutes after each market ingestion run (10:30 UTC Mon-Fri)
          and after the weekly reference ingestion (19:30 UTC Saturday).
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

DBT_PROJECT_DIR  = "/var/lib/ngods/dbt/bronze_vnstock"
DBT_PROFILES_DIR = "/var/lib/ngods/dbt"
DBT_TARGET       = "dev"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _run_dbt(select: str | None = None) -> None:
    """Run dbt inside the container using subprocess."""
    import subprocess

    cmd = [
        "dbt", "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--target", DBT_TARGET,
    ]
    if select:
        cmd += ["--select", select]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed (exit {result.returncode})")


@dag(
    dag_id="bronze_transform",
    description=(
        "dbt bronze_vnstock — standardise raw parquet → typed Iceberg tables "
        "in warehouse.bronze (incremental for market, full-refresh for reference)"
    ),
    schedule="30 10 * * 1-5",   # Mon-Fri 10:30 UTC (30 min after market ingestion)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "transform"],
)
def bronze_transform_dag():

    @task
    def transform_market() -> None:
        """Incremental append for equity/index OHLCV, trades and order book."""
        _run_dbt(select="bronze_vnstock.market")

    @task
    def transform_reference() -> None:
        """Full-refresh for listing, industry and events tables."""
        _run_dbt(select="bronze_vnstock.reference")

    # Both model groups are independent — run in parallel
    transform_market()
    transform_reference()


bronze_transform_dag()
