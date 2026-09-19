"""
jp-stats-elt: e-Stat の消費者物価指数を取り込み、dbt で変換する月次 ELT DAG。

タスク構成:
  start → extract_estat → validate_load → dbt_deps → dbt_seed → dbt_run → dbt_test → end

  extract_estat : e-Stat API から CPI を取得し raw.cpi へ UPSERT
  validate_load : 取り込み行数の契約を検証し、下流の dbt を守るゲート
  dbt_*         : 同居する dbt プロジェクトを BashOperator で起動

設計判断:
  - 冪等性: data_interval_start を渡し、同じ実行日で再実行しても重複しないこと
  - リトライ: API・DB の一時障害を吸収するため retries=2
  - schedule: e-Stat の月次更新に合わせ '@monthly'（毎月1日）
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

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
        raise ValueError(f"raw.cpi の行数が想定を下回っています: {rows} < {min_rows}")
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
    description="e-Stat 消費者物価指数の月次 ELT パイプライン",
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

    # Week 6: dbt パッケージ（dbt_utils）を取得。marts のテストが依存するため、
    # クリーンな環境でも自己完結するようパイプラインに組み込む。冪等かつ高速
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )
    # Week 5: area_master seed をロード（stg_area_master が ref する）
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR}",
    )
    # Week 5: staging モデルを構築（失敗はタスク失敗として扱う）
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Week 5: source/seed/model の全テストを実行（失敗はタスク失敗）
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> load >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> end
