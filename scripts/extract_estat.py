"""e-Stat CPI 抽出スクリプト

2020年基準消費者物価指数（statsDataId=0003427113）から、
10大費目 × 全国＋47県庁所在市 × 直近60ヶ月の「指数」を取得し、
PostgreSQL の raw.cpi に冪等ロードする。
"""
from __future__ import annotations

import logging
import os
import time

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values


# --- API ---
ESTAT_BASE_URL = "https://api.e-stat.go.jp/rest/3.0/app/json"
STATS_DATA_ID = "0003427113"  # 2020年基準消費者物価指数

# 識別可能な User-Agent（素性を示す。※WAF通過は実リクエスト時に要確認、
# 弾かれたら既知で通るブラウザ風UAにフォールバック）
USER_AGENT = "jp-stats-elt/0.1 (+https://github.com/d-kajiya/jp-stats-elt)"

# --- 静的フィルタ（不変なのでハードコード）---
TAB_CODE = "1"  # 表章項目: 指数（2=前月比/前年比, 3=前年同月比 は除外）

# 10大費目（cat01 の level1 に総合・再掲と混在するため明示列挙）
CATEGORY_CODES = [
    "0002",  # 食料
    "0045",  # 住居
    "0054",  # 光熱・水道
    "0060",  # 家具・家事用品
    "0082",  # 被服及び履物
    "0107",  # 保健医療
    "0111",  # 交通・通信
    "0118",  # 教育
    "0122",  # 教養娯楽
    "0145",  # 諸雑費
]

# 全国 + 47県庁所在市（CPIは都市調査のため都道府県データは無い。
# 福岡の県庁所在市は福岡市=40A02。重複政令市 40A01北九州市 等は除外）
AREA_CODES = [
    "00000",  # 全国
    "01A01", "02A01", "03A01", "04A01", "05A01", "06A01", "07A01",
    "08A01", "09A01", "10A01", "11A01", "12A01", "13A01", "14A01",
    "15A01", "16A01", "17A01", "18A01", "19A01", "20A01", "21A01",
    "22A01", "23A01", "24A01", "25A01", "26A01", "27A01", "28A01",
    "29A01", "30A01", "31A01", "32A01", "33A01", "34A01", "35A01",
    "36A01", "37A01", "38A01", "39A01", "40A02", "41A01", "42A01",
    "43A01", "44A01", "45A01", "46A01", "47A01",
]

# --- 動的フィルタ ---
# 時間軸は実行時に getMetaInfo から「月次の最新60件」を取得する（ローリング窓）。
# 月が増えても自動追従させるため、ここではハードコードしない。
RECENT_MONTHS = 60

# --- リトライ設定 ---
MAX_RETRIES = 5
BACKOFF_BASE = 1.0 # 秒 1-2-4-8-16と待つ
TIMEOUT = 30.0 # 秒

# 一時的エラーとみなす本文STATUS（DBアクセス系の内部エラ）
RETRYABLE_BODY_STATUS = {200, 201, 202, 203, 299}

logger = logging.getLogger(__name__)

class EStatError(RuntimeError):
    """リトライしても回復しなかった、または恒久的な API エラー。"""


def _build_session(app_id: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.params = {"appId": app_id}  # 全リクエスト共通で付与
    return session


def _get_with_retry(
    session: requests.Session, endpoint: str, params: dict
) -> dict:
    """e-Stat の1エンドポイントを叩き、正常な JSON 本文(dict)を返す。

    一時的エラー(HTTP 429/5xx・本文 STATUS 200番台)は指数バックオフで再試行。
    恒久的エラー(HTTP 403・本文 STATUS 100/101/102 等)は即 EStatError。
    """
    url = f"{ESTAT_BASE_URL}/{endpoint}"

    for attempt in range(1, MAX_RETRIES + 1):
        wait = BACKOFF_BASE * (2 ** (attempt - 1))
        try:
            resp = session.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            # ネットワーク層の例外（接続断・タイムアウト等）は一時的とみなす
            if attempt == MAX_RETRIES:
                raise EStatError(f"network error after {attempt} tries: {exc}") from exc
            logger.warning("network error (try %d/%d): %s — retrying in %.0fs",
                           attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
            continue

        # --- 1層目: HTTP ステータス ---
        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            if attempt == MAX_RETRIES:
                raise EStatError(f"HTTP {resp.status_code} after {attempt} tries")
            logger.warning("HTTP %d (try %d/%d) — retrying in %.0fs",
                           resp.status_code, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            # 403(WAF) や 400 など、再試行しても直らないもの
            raise EStatError(f"HTTP {resp.status_code} (non-retryable): {resp.text[:200]}")

        # --- 2層目: 本文 RESULT.STATUS ---
        try:
            body = resp.json()
        except ValueError as exc:
            # JSONでない（WAFのHTML等）。先頭を残して諦める
            raise EStatError(f"non-JSON response: {resp.text[:200]}") from exc

        result = next(iter(body.values())).get("RESULT", {})
        status = result.get("STATUS")
        if status in (0, 1, 2):
            return body  # 正常（1=該当0件 も正常扱いで呼び出し側に委ねる）
        if status in RETRYABLE_BODY_STATUS:
            if attempt == MAX_RETRIES:
                raise EStatError(f"body STATUS {status} after {attempt} tries")
            logger.warning("body STATUS %d (try %d/%d) — retrying in %.0fs",
                           status, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
            continue
        # STATUS 100(認証)/101(必須欠落)/102(値不正)/300(データ無) 等は恒久的
        raise EStatError(f"body STATUS {status}: {result.get('ERROR_MSG')}")

    raise EStatError("exhausted retries")  # 到達しない保険


def fetch_recent_month_codes(
    session: requests.Session, n: int = RECENT_MONTHS
) -> list[str]:
    """getMetaInfo から月次の時間軸コードを取得し、新しい順で上位 n 件を返す。

    月は毎月増えるため実行時に取り直す（ローリング窓）。
    """
    body = _get_with_retry(session, "getMetaInfo", {"statsDataId": STATS_DATA_ID})
    class_objs = body["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
    if isinstance(class_objs, dict):
        class_objs = [class_objs]

    time_obj = next(o for o in class_objs if o["@id"] == "time")
    classes = time_obj["CLASS"]
    if isinstance(classes, dict):
        classes = [classes]

    # 名称に「月」を含む＝月次エントリのみ。コード降順＝新しい順。
    months = [c["@code"] for c in classes if "月" in c["@name"]]
    months.sort(reverse=True)
    if len(months) < n:
        logger.warning("月次エントリが %d 件しかない（要求 %d）", len(months), n)
    return months[:n]


def fetch_cpi_values(
    session: requests.Session, time_codes: list[str]
) -> list[dict]:
    """getStatsData で対象スコープの VALUE 配列を取得して返す。

    現スコープ(28,800行)は limit 10万未満で1リクエスト完結だが、
    継続データ(NEXT_KEY)があれば辿る汎用実装にしておく。
    """
    params = {
        "statsDataId": STATS_DATA_ID,
        "cdTab": TAB_CODE,
        "cdCat01": ",".join(CATEGORY_CODES),
        "cdArea": ",".join(AREA_CODES),
        "cdTime": ",".join(time_codes),
        "metaGetFlg": "N",
    }

    all_values: list[dict] = []
    start_position = None
    while True:
        page_params = dict(params)
        if start_position is not None:
            page_params["startPosition"] = start_position

        body = _get_with_retry(session, "getStatsData", page_params)
        stat = body["GET_STATS_DATA"]["STATISTICAL_DATA"]

        values = stat["DATA_INF"]["VALUE"]
        if isinstance(values, dict):  # 1件のみだと配列でなくdictで来る
            values = [values]
        all_values.extend(values)

        next_key = stat.get("RESULT_INF", {}).get("NEXT_KEY")
        if not next_key:
            break
        start_position = next_key
        logger.info("継続データあり。次の開始位置 %s から取得", next_key)

    return all_values


def parse_values(values: list[dict]) -> pd.DataFrame:
    """VALUE 配列を raw.cpi のスキーマに合わせた DataFrame に変換する。

    - キー @tab/@cat01/@area/@time を各コード列へ。@unit は無ければ NULL。
    - 値 $ は value_raw に原文保持、数値化できなければ value=NULL。
      ('***' = 該当データなし、'-' 等の記号もここで NULL に落ちる)
    """
    if not values:
        return pd.DataFrame(
            columns=["tab_code", "area_code", "category_code",
                     "time_code", "value", "value_raw", "unit"]
        )

    df = pd.DataFrame(values)
    out = pd.DataFrame({
        "tab_code": df["@tab"],
        "area_code": df["@area"],
        "category_code": df["@cat01"],
        "time_code": df["@time"],
        "value_raw": df["$"],
        "unit": df["@unit"] if "@unit" in df.columns else pd.NA,
    })
    # 数値化（失敗は NaN→後でNULL）。value_raw には原文が残る。
    out["value"] = pd.to_numeric(out["value_raw"], errors="coerce")
    return out[["tab_code", "area_code", "category_code",
                "time_code", "value", "value_raw", "unit"]]


# --- DB ---
UPSERT_SQL = """
    INSERT INTO raw.cpi
        (tab_code, area_code, category_code, time_code, value, value_raw, unit, loaded_at)
    VALUES %s
    ON CONFLICT (tab_code, area_code, category_code, time_code)
    DO UPDATE SET
        value     = EXCLUDED.value,
        value_raw = EXCLUDED.value_raw,
        unit      = EXCLUDED.unit,
        loaded_at = now()
"""


def _connect():
    """warehouse への接続を返す。接続情報は環境変数から取得。"""
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def upsert_cpi(df: pd.DataFrame, conn=None) -> int:
    """DataFrame を raw.cpi に冪等 UPSERT する。書き込んだ行数を返す。

    主キー(tab,area,category,time)で衝突した既存行は値を更新し loaded_at を
    現在時刻へ。新規行は挿入。再実行しても重複せず、最新月の後日反映にも追従する。
    """
    if df.empty:
        logger.warning("空の DataFrame。UPSERT をスキップ")
        return 0

    # NaN/NA を psycopg2 が NULL として渡せるよう Python の None に変換
    safe = df.astype(object).where(pd.notna(df), None)
    rows = [
        (r.tab_code, r.area_code, r.category_code, r.time_code,
         r.value, r.value_raw, r.unit)
        for r in safe.itertuples(index=False)
    ]

    own_conn = conn is None
    if own_conn:
        conn = _connect()
    try:
        with conn.cursor() as cur:
            # loaded_at は DEFAULT now() に任せるため VALUES 側では渡さない
            template = "(%s, %s, %s, %s, %s, %s, %s, now())"
            execute_values(cur, UPSERT_SQL, rows, template=template, page_size=1000)
        conn.commit()
        logger.info("UPSERT 完了: %d 行", len(rows))
        return len(rows)
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    app_id = os.environ["ESTAT_APP_ID"]
    session = _build_session(app_id)

    months = fetch_recent_month_codes(session)
    print(f"取得した月数: {len(months)}  最新={months[0]}  最古={months[-1]}")

    values = fetch_cpi_values(session, months)
    print(f"VALUE 件数: {len(values)}")

    df = parse_values(values)
    print(f"DataFrame shape: {df.shape}")
    print(f"value が NULL の行数（***等）: {df['value'].isna().sum()}")
    print(df.head())
    
    written = upsert_cpi(df)
    print(f"UPSERT 行数: {written}")