"""Structural tests for the jp_stats_elt DAG.

These run without a database or the e-Stat API. They check that the DAG parses
without import errors, that its task set and ordering are exactly as designed, and
that regressions fixed earlier (BashOperator extraction, `|| echo` fallbacks) cannot
return unnoticed. The extraction logic itself is tested in test_extract_estat.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def dagbag():
    # Skip in environments where Airflow is not installed
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
    expected = {
        "start",
        "extract_estat",
        "validate_load",
        "dbt_deps",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "end",
    }
    # Equality rather than issubset, so that both added and removed tasks are caught
    assert set(dag.task_ids) == expected


def test_extract_and_validate_are_python_operators(dagbag):
    """extract/validate must be PythonOperators (guards against reverting to Bash)."""
    from airflow.operators.python import PythonOperator

    dag = dagbag.dags["jp_stats_elt"]
    assert isinstance(dag.get_task("extract_estat"), PythonOperator)
    assert isinstance(dag.get_task("validate_load"), PythonOperator)


def test_dbt_task_ordering(dagbag):
    """Fix the dbt order: deps -> seed -> run -> test.

    deps installs dbt_utils, which the marts tests depend on. In a clean environment,
    run/test fail unless deps has run before seed.
    """
    dag = dagbag.dags["jp_stats_elt"]

    def downstream_ids(task_id):
        return {t.task_id for t in dag.get_task(task_id).downstream_list}

    assert "dbt_deps" in downstream_ids("validate_load")
    assert "dbt_seed" in downstream_ids("dbt_deps")
    assert "dbt_run" in downstream_ids("dbt_seed")
    assert "dbt_test" in downstream_ids("dbt_run")


def test_dbt_tasks_have_no_echo_fallback(dagbag):
    """dbt tasks must not swallow failures with `|| echo`.

    Reintroducing that fallback would turn a dbt failure into a task success.
    """
    dag = dagbag.dags["jp_stats_elt"]
    for task_id in ("dbt_deps", "dbt_seed", "dbt_run", "dbt_test"):
        cmd = dag.get_task(task_id).bash_command
        assert "|| echo" not in cmd, f"{task_id} still swallows failures: {cmd}"
