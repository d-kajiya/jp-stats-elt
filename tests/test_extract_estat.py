"""scripts/extract_estat.py の単体テスト。

実 API・実 DB に依存しないことを原則とする:
  - パーサ(parse_values)は純粋関数なので pandas のみで検証
  - クライアント(_get_with_retry)は responses で HTTP をモック
  - UPSERT は DB 接続を伴うため、psycopg2/DB が無い環境では skip
Week 3 抽出フェーズのロジックを回帰テストで保護する。
"""
from __future__ import annotations

import math

import pandas as pd
import pytest
import responses

import extract_estat as e


# --- parse_values: 正常系 ---
def test_parse_values_basic_shape():
    values = [
        {"@tab": "1", "@cat01": "0002", "@area": "13A01",
         "@time": "2026000505", "@unit": "", "$": "128.2"},
        {"@tab": "1", "@cat01": "0045", "@area": "00000",
         "@time": "2026000505", "@unit": "", "$": "104.6"},
    ]
    df = e.parse_values(values)

    # 列が raw.cpi のスキーマ順で揃っている
    assert list(df.columns) == [
        "tab_code", "area_code", "category_code",
        "time_code", "value", "value_raw", "unit",
    ]
    assert len(df) == 2
    # コード列が正しくマッピングされている
    assert df.loc[0, "area_code"] == "13A01"
    assert df.loc[0, "category_code"] == "0002"
    # 数値化されている（value_raw は原文保持）
    assert df.loc[0, "value"] == 128.2
    assert df.loc[0, "value_raw"] == "128.2"


# --- parse_values: 欠損マーカー *** → value=NULL, value_raw=原文 ---
def test_parse_values_missing_marker_becomes_null():
    values = [
        {"@tab": "1", "@cat01": "0002", "@area": "47A01",
         "@time": "2021000606", "@unit": "", "$": "***"},
    ]
    df = e.parse_values(values)

    # value は数値化できず NaN（→ DB では NULL）
    assert math.isnan(df.loc[0, "value"])
    # 原文 '***' は value_raw に温存される
    assert df.loc[0, "value_raw"] == "***"


# --- parse_values: @unit キー欠如 → unit 列は NA ---
def test_parse_values_missing_unit_key():
    values = [
        {"@tab": "1", "@cat01": "0002", "@area": "13A01",
         "@time": "2026000505", "$": "128.2"},  # @unit が無い（CPI の実形状）
    ]
    df = e.parse_values(values)

    assert "unit" in df.columns
    assert pd.isna(df.loc[0, "unit"])
    
# --- _get_with_retry: HTTP 502 が続いた後に成功（リトライが効く）---
@responses.activate
def test_get_with_retry_recovers_after_502(monkeypatch):
    # バックオフの sleep を潰して即時化
    monkeypatch.setattr(e.time, "sleep", lambda *_: None)

    url = f"{e.ESTAT_BASE_URL}/getStatsData"
    # 1回目・2回目は 502、3回目で正常 JSON
    responses.add(responses.GET, url, status=502)
    responses.add(responses.GET, url, status=502)
    responses.add(
        responses.GET, url,
        json={"GET_STATS_DATA": {"RESULT": {"STATUS": 0, "ERROR_MSG": "正常"}}},
        status=200,
    )

    session = e._build_session("dummy")
    body = e._get_with_retry(session, "getStatsData", {})

    assert body["GET_STATS_DATA"]["RESULT"]["STATUS"] == 0
    assert len(responses.calls) == 3  # 2回リトライして3回目で成功


# --- _get_with_retry: 本文 STATUS=100（認証失敗）は即エラー（リトライしない）---
@responses.activate
def test_get_with_retry_auth_error_is_not_retried(monkeypatch):
    monkeypatch.setattr(e.time, "sleep", lambda *_: None)

    url = f"{e.ESTAT_BASE_URL}/getStatsList"
    # HTTP は 200 だが本文 STATUS=100（恒久的エラー）
    responses.add(
        responses.GET, url,
        json={"GET_STATS_LIST": {"RESULT": {"STATUS": 100,
              "ERROR_MSG": "認証に失敗しました。"}}},
        status=200,
    )

    session = e._build_session("bad_app_id")
    with pytest.raises(e.EStatError):
        e._get_with_retry(session, "getStatsList", {})

    assert len(responses.calls) == 1  # リトライせず1回で諦める    
    
    # --- UPSERT: 冪等性の統合テスト（DB が無ければ skip）---
@pytest.fixture
def warehouse_conn():
    """warehouse へ接続。繋がらなければテストを skip し、終了時に閉じる。"""
    import os

    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=os.environ.get("WAREHOUSE_HOST", "localhost"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
            dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
            user=os.environ.get("POSTGRES_USER", "airflow"),
            password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
            connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"warehouse に接続できないため skip: {exc}")

    yield conn
    conn.close()


def _make_test_df():
    """本番データと混ざらない隔離用 DataFrame（tab_code='TEST'）。"""
    return pd.DataFrame([
        {"tab_code": "TEST", "area_code": "00000", "category_code": "0002",
         "time_code": "9999000101", "value": 100.0, "value_raw": "100.0", "unit": None},
        {"tab_code": "TEST", "area_code": "00000", "category_code": "0045",
         "time_code": "9999000101", "value": 200.0, "value_raw": "200.0", "unit": None},
    ])


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
        # 1回目: 2行挿入される
        e.upsert_cpi(df, conn=conn)
        assert count_test_rows() == 2
        first_load = max_loaded_at()

        # 2回目: 同じキーなので行数は増えない（冪等）。loaded_at は進む
        e.upsert_cpi(df, conn=conn)
        assert count_test_rows() == 2, "再 UPSERT で行が重複してはいけない"
        assert max_loaded_at() > first_load, "再ロードで loaded_at が更新されるべき"

        # 値更新の確認: value を変えて UPSERT → 既存行が上書きされる
        df2 = df.copy()
        df2.loc[0, "value"] = 999.0
        df2.loc[0, "value_raw"] = "999.0"
        e.upsert_cpi(df2, conn=conn)
        assert count_test_rows() == 2  # 依然 2 行
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM raw.cpi "
                "WHERE tab_code='TEST' AND category_code='0002' AND time_code='9999000101';"
            )
            assert cur.fetchone()[0] == 999.0  # DO UPDATE で上書きされた
    finally:
        # 後始末: テスト行を必ず削除
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.cpi WHERE tab_code = 'TEST';")
        conn.commit()