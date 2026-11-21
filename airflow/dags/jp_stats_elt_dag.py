"""
jp-stats-elt: monthly ELT DAG that ingests the Consumer Price Index from e-Stat
and transforms it with dbt.

Task flow:
  start → extract_estat → validate_load → dbt_deps → dbt_seed → dbt_run → dbt_test → end

  extract_estat : fetch CPI from the e-Stat API and UPSERT it into raw.cpi
  validate_load : row-count contract gate that protects the downstream dbt run
  dbt_*         : invoke the co-located dbt project through BashOperator

Design decisions:
  - Idempotency: raw.cpi is keyed on e-Stat's own identifiers
    (tab_code, area_code, category_code, time_code) and loaded with
    ON CONFLICT DO UPDATE. The extraction window is a rolling 60 months
    resolved at runtime, so the logical date is deliberately not passed in;
    a re-run, a backfill and a scheduled run all converge on the same state.
  - Retries: retries=2 with a 5-minute delay absorbs outages longer than the
    in-client exponential backoff (see docs/architecture.md).
  - Schedule: '@monthly' (1st of each month) to match e-Stat's monthly release.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

# Make scripts/ importable inside the container (mounted at /opt/airflow/scripts)
sys.path.append("/opt/airflow/scripts")

DBT_PROJECT_DIR = "/opt/airflow/dbt"


def _run_extract() -> int:
    """Fetch CPI from e-Stat and load it idempotently into raw.cpi. Returns rows loaded."""
    import extract_estat

    return extract_estat.main()


def _validate_load(min_rows: int = 20_000) -> None:
    """Data-contract gate: assert raw.cpi holds at least ``min_rows`` rows.

    Rows with tab_code='TEST' are excluded so that only production data is counted.
    """
    import extract_estat

    with extract_estat._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.cpi WHERE tab_code <> 'TEST'")
            (rows,) = cur.fetchone()

    if rows < min_rows:
        raise ValueError(f"raw.cpi has fewer rows than expected: {rows} < {min_rows}")
    logging.info("validate_load OK: raw.cpi holds %d rows", rows)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="jp_stats_elt",
    description="Monthly ELT pipeline for the e-Stat Consumer Price Index",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["elt", "estat", "dbt", "portfolio"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Fetch CPI from the e-Stat API and load it idempotently into raw.cpi
    extract = PythonOperator(
        task_id="extract_estat",
        python_callable=_run_extract,
    )

    # Row-count contract gate on raw.cpi, ahead of anything dbt reads
    load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load,
    )

    # Install dbt packages (dbt_utils). The marts tests depend on it, so it is part
    # of the pipeline to keep a clean environment self-contained. Idempotent and fast
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )
    # Load the area_master and category_master seeds referenced by staging
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR}",
    )
    # Build all models: staging → intermediate → marts (a dbt failure fails the task)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Run every source, seed and model test (a dbt failure fails the task)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> load >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> end
