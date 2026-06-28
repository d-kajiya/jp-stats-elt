"""e-Stat CPI 抽出スクリプト

2020年基準消費者物価指数（statsDataId=0003427113）から、
10大費目 × 全国＋47県庁所在市 × 直近60ヶ月の「指数」を取得し、
PostgreSQL の raw.cpi に冪等ロードする。
"""
from __future__ import annotations

import logging
import time

import requests

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


if __name__ == "__main__":
    print(f"statsDataId   : {STATS_DATA_ID}")
    print(f"categories    : {len(CATEGORY_CODES)}")
    print(f"areas         : {len(AREA_CODES)}")
    print(f"recent months : {RECENT_MONTHS}")