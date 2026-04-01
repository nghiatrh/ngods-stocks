"""
Silver + Gold transformation DAG.

Waits for bronze_transform to complete via ExternalTaskSensor before
executing dbt projects in dependency order:
  1. silver_vnstock  (Spark → warehouse.silver)  — enriched facts + dimensions
  2. gold_vnstock    (Spark → warehouse.gold)     — report-ready aggregations

Schedule: 10:30 UTC Mon-Fri (same as bronze_transform).
          ExternalTaskSensor blocks until bronze_transform succeeds for the
          same logical date before proceeding.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

PROFILES_DIR = "/var/lib/ngods/dbt"
SILVER_DIR   = "/var/lib/ngods/dbt/silver_vnstock"
GOLD_DIR     = "/var/lib/ngods/dbt/gold_vnstock"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _run_dbt(project_dir: str, select: str | None = None) -> None:
    import subprocess

    cmd = [
        "dbt", "run",
        "--project-dir", project_dir,
        "--profiles-dir", PROFILES_DIR,
        "--target", "dev",
    ]
    if select:
        cmd += ["--select", select]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed for {project_dir} (exit {result.returncode})")


@dag(
    dag_id="silver_gold_transform",
    description="dbt silver_vnstock → gold_vnstock daily metric refresh",
    schedule="30 10 * * 1-5",   # same schedule as bronze_transform; sensor gates execution
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["silver", "gold", "dbt", "transform"],
)
def silver_gold_transform_dag():

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_transform",
        external_dag_id="bronze_transform",
        external_task_id=None,          # wait for the whole DAG run to succeed
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        execution_delta=timedelta(0),   # same logical date / execution time
        timeout=60 * 60 * 2,           # give bronze up to 2 h before failing
        poke_interval=60,              # check every 60 s
        mode="reschedule",             # release the worker slot while waiting
    )

    @task
    def run_silver() -> None:
        """Build dim_equity, fct_equity_daily, fct_index_daily."""
        _run_dbt(SILVER_DIR)

    @task
    def run_gold() -> None:
        """Build all rpt_* report tables that depend on silver."""
        _run_dbt(GOLD_DIR)

    wait_for_bronze >> run_silver() >> run_gold()


silver_gold_transform_dag()
