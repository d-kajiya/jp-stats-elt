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

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# scripts/ をコンテナ内 import パスに追加（/opt/airflow/scripts にマウント済み）
sys.path.append("/opt/airflow/scripts")

DBT_PROJECT_DIR = "/opt/airflow/dbt"

def _run_extract() -> int:
    """e-Stat から CPI を取得し raw.cpi へ冪等ロード。ロード行数を返す。"""
    import extract_estat

    return extract_estat.main()


def _validate_load(min_rows: int = 20_000) -> None:
    """raw.cpi の行数がしきい値以上かを検証するデータ契約ゲート。

    tab_code='TEST' のテスト行は除外して本番相当のみ数える。
    """
    import extract_estat

    with extract_estat._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.cpi WHERE tab_code <> 'TEST'")
            (rows,) = cur.fetchone()

    if rows < min_rows:
        raise ValueError(
            f"raw.cpi の行数が想定を下回っています: {rows} < {min_rows}"
        )
    logging.info("validate_load OK: raw.cpi に %d 行", rows)


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

    # Week 4: e-Stat API から CPI を取得し raw.cpi へ冪等ロード
    extract = PythonOperator(
        task_id="extract_estat",
        python_callable=_run_extract,
    )

    # Week 4: raw.cpi の行数を検証するデータ契約ゲート（dbt が読む前段）
    load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load,
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
