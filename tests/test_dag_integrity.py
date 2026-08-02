"""DAGがImportErrorなく読み込めることを検証する最小スモークテスト。

Week 1-2 時点ではタスクの中身は echo プレースホルダなので、
ここでは「DAGが構文エラーなくパースされる」「想定タスクが揃っている」
ことだけを確認する。Week 3 以降、抽出ロジック本体のテストを追加していく。
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def dagbag():
    # Airflow がインストールされていない CI ステージではスキップ
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    dags_folder = Path(__file__).resolve().parent.parent / "airflow" / "dags"
    return DagBag(dag_folder=str(dags_folder), include_examples=False)


def test_dag_imports_without_error(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_jp_stats_elt_dag_present(dagbag):
    assert "jp_stats_elt" in dagbag.dags


def test_expected_tasks_exist(dagbag):
    dag = dagbag.dags["jp_stats_elt"]
    expected = {"start", "extract_estat", "validate_load", "dbt_seed", "dbt_run", "dbt_test", "end"}
    assert expected.issubset(set(dag.task_ids))

def test_extract_and_validate_are_python_operators(dagbag):
    """Week 4: extract/validate は PythonOperator であること（Bash 逆戻り防止）。"""
    from airflow.operators.python import PythonOperator

    dag = dagbag.dags["jp_stats_elt"]
    assert isinstance(dag.get_task("extract_estat"), PythonOperator)
    assert isinstance(dag.get_task("validate_load"), PythonOperator)

def test_dbt_task_ordering(dagbag):
    """Week 5: seed -> run -> test の依存順を固定（seed は run の前に必須）。"""
    dag = dagbag.dags["jp_stats_elt"]

    def downstream_ids(task_id):
        return {t.task_id for t in dag.get_task(task_id).downstream_list}

    assert "dbt_seed" in downstream_ids("validate_load")
    assert "dbt_run" in downstream_ids("dbt_seed")
    assert "dbt_test" in downstream_ids("dbt_run")


def test_dbt_tasks_have_no_echo_fallback(dagbag):
    """Week 5: dbt タスクが `|| echo` で失敗を握り潰していないこと。
    握り潰しを再導入すると dbt の失敗がタスク成功に化けるため、回帰を防ぐ。
    """
    dag = dagbag.dags["jp_stats_elt"]
    for task_id in ("dbt_seed", "dbt_run", "dbt_test"):
        cmd = dag.get_task(task_id).bash_command
        assert "|| echo" not in cmd, f"{task_id} still swallows failures: {cmd}"
