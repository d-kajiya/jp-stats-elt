"""Unit tests for scripts/extract_estat.py.

The principle is no dependency on the real API or a real database:
  - The parser (parse_values) is a pure function, tested with pandas alone
  - The client (_get_with_retry) is tested against HTTP mocked with `responses`
  - The UPSERT needs a database, so it is skipped where psycopg2/the DB is absent
These guard the extraction logic against regressions.
"""

from __future__ import annotations

import math

import extract_estat as e
import pandas as pd
import pytest
import responses


# --- parse_values: happy path ---
def test_parse_values_basic_shape():
    values = [
        {
            "@tab": "1",
            "@cat01": "0002",
            "@area": "13A01",
            "@time": "2026000505",
            "@unit": "",
            "$": "128.2",
        },
        {
            "@tab": "1",
            "@cat01": "0045",
            "@area": "00000",
            "@time": "2026000505",
            "@unit": "",
            "$": "104.6",
        },
    ]
    df = e.parse_values(values)

    # Columns follow the raw.cpi schema order
    assert list(df.columns) == [
        "tab_code",
        "area_code",
        "category_code",
        "time_code",
        "value",
        "value_raw",
        "unit",
    ]
    assert len(df) == 2
    # Code columns are mapped correctly
    assert df.loc[0, "area_code"] == "13A01"
    assert df.loc[0, "category_code"] == "0002"
    # Values are numeric (value_raw keeps the original text)
    assert df.loc[0, "value"] == 128.2
    assert df.loc[0, "value_raw"] == "128.2"


# --- parse_values: missing marker *** -> value=NULL, value_raw=original ---
def test_parse_values_missing_marker_becomes_null():
    values = [
        {
            "@tab": "1",
            "@cat01": "0002",
            "@area": "47A01",
            "@time": "2021000606",
            "@unit": "",
            "$": "***",
        },
    ]
    df = e.parse_values(values)

    # value cannot be parsed, so NaN (-> NULL in the DB)
    assert math.isnan(df.loc[0, "value"])
    # The original '***' is preserved in value_raw
    assert df.loc[0, "value_raw"] == "***"


# --- parse_values: missing @unit key -> unit column is NA ---
def test_parse_values_missing_unit_key():
    values = [
        {
            "@tab": "1",
            "@cat01": "0002",
            "@area": "13A01",
            "@time": "2026000505",
            "$": "128.2",
        },  # no @unit (the actual shape of CPI responses)
    ]
    df = e.parse_values(values)

    assert "unit" in df.columns
    assert pd.isna(df.loc[0, "unit"])


# --- _get_with_retry: succeeds after repeated HTTP 502 (retries work) ---
@responses.activate
def test_get_with_retry_recovers_after_502(monkeypatch):
    # Stub out the backoff sleep so the test runs instantly
    monkeypatch.setattr(e.time, "sleep", lambda *_: None)

    url = f"{e.ESTAT_BASE_URL}/getStatsData"
    # 502 on the first and second calls, valid JSON on the third
    responses.add(responses.GET, url, status=502)
    responses.add(responses.GET, url, status=502)
    responses.add(
        responses.GET,
        url,
        json={"GET_STATS_DATA": {"RESULT": {"STATUS": 0, "ERROR_MSG": "正常"}}},
        status=200,
    )

    session = e._build_session("dummy")
    body = e._get_with_retry(session, "getStatsData", {})

    assert body["GET_STATS_DATA"]["RESULT"]["STATUS"] == 0
    assert len(responses.calls) == 3  # two retries, success on the third call


# --- _get_with_retry: in-body STATUS=100 (auth failure) errors immediately (no retry) ---
@responses.activate
def test_get_with_retry_auth_error_is_not_retried(monkeypatch):
    monkeypatch.setattr(e.time, "sleep", lambda *_: None)

    url = f"{e.ESTAT_BASE_URL}/getStatsList"
    # HTTP 200, but in-body STATUS=100 (a permanent error)
    responses.add(
        responses.GET,
        url,
        json={"GET_STATS_LIST": {"RESULT": {"STATUS": 100, "ERROR_MSG": "認証に失敗しました。"}}},
        status=200,
    )

    session = e._build_session("bad_app_id")
    with pytest.raises(e.EStatError):
        e._get_with_retry(session, "getStatsList", {})

    assert len(responses.calls) == 1  # gives up after one call, without retrying


# --- UPSERT: idempotency integration test (skipped without a DB) ---
@pytest.fixture
def warehouse_conn():
    """Connect to the warehouse; skip the test if unreachable, and close on teardown."""
    import os

    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=os.environ.get("WAREHOUSE_HOST", "localhost"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
            dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
            user=os.environ.get("WAREHOUSE_USER", "airflow"),
            password=os.environ.get("WAREHOUSE_PASSWORD", "airflow"),
            connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"skipped: cannot connect to the warehouse: {exc}")

    yield conn
    conn.close()


def _make_test_df():
    """An isolated DataFrame that never mixes with production data (tab_code='TEST')."""
    return pd.DataFrame(
        [
            {
                "tab_code": "TEST",
                "area_code": "00000",
                "category_code": "0002",
                "time_code": "9999000101",
                "value": 100.0,
                "value_raw": "100.0",
                "unit": None,
            },
            {
                "tab_code": "TEST",
                "area_code": "00000",
                "category_code": "0045",
                "time_code": "9999000101",
                "value": 200.0,
                "value_raw": "200.0",
                "unit": None,
            },
        ]
    )


def test_upsert_is_idempotent(warehouse_conn):
    conn = warehouse_conn
    df = _make_test_df()

    def count_test_rows():
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.cpi WHERE tab_code = 'TEST';")
            return cur.fetchone()[0]

    def max_loaded_at():
        with conn.cursor() as cur:
            cur.execute("SELECT max(loaded_at) FROM raw.cpi WHERE tab_code = 'TEST';")
            return cur.fetchone()[0]

    try:
        # First run: two rows are inserted
        e.upsert_cpi(df, conn=conn)
        assert count_test_rows() == 2
        first_load = max_loaded_at()

        # Second run: same keys, so the row count does not grow (idempotent); loaded_at advances
        e.upsert_cpi(df, conn=conn)
        assert count_test_rows() == 2, "re-UPSERT must not duplicate rows"
        assert max_loaded_at() > first_load, "re-load should advance loaded_at"

        # Value update: UPSERT with a changed value overwrites the existing row
        df2 = df.copy()
        df2.loc[0, "value"] = 999.0
        df2.loc[0, "value_raw"] = "999.0"
        e.upsert_cpi(df2, conn=conn)
        assert count_test_rows() == 2  # still two rows
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM raw.cpi "
                "WHERE tab_code='TEST' AND category_code='0002' AND time_code='9999000101';"
            )
            assert cur.fetchone()[0] == 999.0  # overwritten by DO UPDATE
    finally:
        # Cleanup: always delete the test rows
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.cpi WHERE tab_code = 'TEST';")
        conn.commit()
