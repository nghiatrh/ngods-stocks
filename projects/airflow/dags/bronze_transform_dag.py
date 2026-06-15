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


def _run_dbt_tests() -> None:
    """Run dbt tests so OpenMetadata can ingest test-case pass/fail results.

    A failing data test (exit 1) is logged but does NOT fail the task — the
    result is written to run_results.json and surfaces in OpenMetadata. Only a
    real execution error (exit >= 2: compile/connection failure) raises.
    """
    import subprocess

    result = subprocess.run(
        ["dbt", "test",
         "--project-dir", DBT_PROJECT_DIR,
         "--profiles-dir", DBT_PROFILES_DIR,
         "--target", DBT_TARGET],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode >= 2:
        print(result.stderr)
        raise RuntimeError(f"dbt test errored (exit {result.returncode})")
    if result.returncode == 1:
        print("WARNING: one or more dbt tests failed — see results in OpenMetadata.")


def _generate_dbt_docs() -> None:
    """Generate dbt docs (produces catalog.json for OpenMetadata dbt ingestion)."""
    import subprocess

    result = subprocess.run(
        ["dbt", "docs", "generate",
         "--project-dir", DBT_PROJECT_DIR,
         "--profiles-dir", DBT_PROFILES_DIR,
         "--target", DBT_TARGET],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt docs generate failed (exit {result.returncode})")


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

    @task
    def run_tests() -> None:
        """Run dbt tests; results flow to OpenMetadata via run_results.json."""
        _run_dbt_tests()

    @task
    def generate_docs() -> None:
        """Produce catalog.json for OpenMetadata dbt ingestion."""
        _generate_dbt_docs()

    # market + reference run in parallel → test → docs generate.
    # test writes run_results.json last (docs generate doesn't touch it),
    # so test outcomes reach OpenMetadata.
    [transform_market(), transform_reference()] >> run_tests() >> generate_docs()


bronze_transform_dag()
