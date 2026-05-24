"""
jp-stats-elt の最小ELT DAG（Week 1-2 スケルトン）

このDAGはWeek 1-2の動作確認用に「extract → load → dbt_run → dbt_test」の
タスク骨格を示すもの。各タスクは現時点では echo のプレースホルダで、
Week 3 以降で順次実装に置き換える:

  Week 3-4: extract / load を e-Stat API + PostgreSQL INSERT に差し替え
  Week 5-6: dbt_run / dbt_test を実モデルに差し替え
  Week 7-8: GitHub Actions CI から dbt test を呼ぶ

設計判断:
  - 冪等性: data_interval_start を渡し、同じ実行日で再実行しても重複しないこと
  - リトライ: API・DB の一時障害を吸収するため retries=2
  - schedule: e-Stat の月次更新に合わせ '@monthly'（毎月1日）
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"

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
    description="e-Stat 政府統計 ELT パイプライン（CPI + 労働力調査）",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["elt", "estat", "dbt", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Week 3-4 で実装: e-Stat API → ローカルJSON保存
    extract = BashOperator(
        task_id="extract_estat",
        bash_command=(
            'echo "[extract] target month: {{ data_interval_start | ds }}"; '
            'echo "TODO(Week 3-4): call e-Stat API for CPI and Labour Force Survey"'
        ),
    )

    # Week 3-4 で実装: JSON → PostgreSQL raw スキーマへ INSERT
    load = BashOperator(
        task_id="load_to_raw",
        bash_command=(
            'echo "[load] insert into raw.cpi / raw.labour_force"; '
            'echo "TODO(Week 3-4): idempotent upsert using execution_date as partition key"'
        ),
    )

    # Week 5-6 で実モデルに置換
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} || echo 'TODO(Week 5-6): no models yet'",
    )

    # Week 5-6 で実モデルに置換
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR} || echo 'TODO(Week 5-6): no tests yet'",
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> load >> dbt_run >> dbt_test >> end
