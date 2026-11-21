"""e-Stat CPI extraction.

Fetches the index values of the 2020-base Consumer Price Index
(statsDataId=0003427113) for the 10 major expenditure categories x nationwide + 47
prefectural capital cities x the latest 60 months, and loads them idempotently into
raw.cpi in PostgreSQL.
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
STATS_DATA_ID = "0003427113"  # 2020-base Consumer Price Index

# An identifiable User-Agent. e-Stat sits behind a WAF that blocks curl's default UA;
# this one identifies the client and its source repository.
USER_AGENT = "jp-stats-elt/0.1 (+https://github.com/d-kajiya/jp-stats-elt)"

# --- Static filters (fixed, so hardcoded) ---
TAB_CODE = "1"  # tabulation item: index (2 = month-on-month/year-on-year, 3 = YoY are excluded)

# The 10 major categories. Listed explicitly because cat01 level 1 also mixes in
# "all items" and re-tabulated aggregates.
CATEGORY_CODES = [
    "0002",  # Food
    "0045",  # Housing
    "0054",  # Fuel, light & water charges
    "0060",  # Furniture & household utensils
    "0082",  # Clothes & footwear
    "0107",  # Medical care
    "0111",  # Transportation & communication
    "0118",  # Education
    "0122",  # Culture & recreation
    "0145",  # Miscellaneous
]

# Nationwide + 47 prefectural capitals. CPI is a city survey, so there is no
# prefecture-level data. Fukuoka's capital is Fukuoka City (40A02); other designated
# cities in the same prefecture, such as Kitakyushu (40A01), are excluded.
# fmt: off
AREA_CODES = [
    "00000",  # Nationwide
    "01A01", "02A01", "03A01", "04A01", "05A01", "06A01", "07A01",
    "08A01", "09A01", "10A01", "11A01", "12A01", "13A01", "14A01",
    "15A01", "16A01", "17A01", "18A01", "19A01", "20A01", "21A01",
    "22A01", "23A01", "24A01", "25A01", "26A01", "27A01", "28A01",
    "29A01", "30A01", "31A01", "32A01", "33A01", "34A01", "35A01",
    "36A01", "37A01", "38A01", "39A01", "40A02", "41A01", "42A01",
    "43A01", "44A01", "45A01", "46A01", "47A01",
]
# fmt: on

# --- Dynamic filter ---
# The time axis is resolved at runtime from getMetaInfo as "the latest 60 monthly
# entries" (a rolling window). It is not hardcoded so that new months are picked up.
RECENT_MONTHS = 60

# --- Retry settings ---
MAX_RETRIES = 5
BACKOFF_BASE = 1.0  # seconds; waits 1-2-4-8-16
TIMEOUT = 30.0  # seconds

# In-body STATUS codes treated as transient (internal database-access errors)
RETRYABLE_BODY_STATUS = {200, 201, 202, 203, 299}

logger = logging.getLogger(__name__)


class EStatError(RuntimeError):
    """A permanent API error, or a transient one that persisted through every retry."""


def _build_session(app_id: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.params = {"appId": app_id}  # sent with every request
    return session


def _get_with_retry(session: requests.Session, endpoint: str, params: dict) -> dict:
    """Call one e-Stat endpoint and return the successful JSON body as a dict.

    Transient errors (HTTP 429/5xx, in-body STATUS in the 200s) are retried with
    exponential backoff. Permanent errors (HTTP 403, in-body STATUS 100/101/102, ...)
    raise EStatError immediately.
    """
    url = f"{ESTAT_BASE_URL}/{endpoint}"

    for attempt in range(1, MAX_RETRIES + 1):
        wait = BACKOFF_BASE * (2 ** (attempt - 1))
        try:
            resp = session.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            # Network-level exceptions (dropped connections, timeouts, ...) are transient
            if attempt == MAX_RETRIES:
                raise EStatError(f"network error after {attempt} tries: {exc}") from exc
            logger.warning(
                "network error (try %d/%d): %s — retrying in %.0fs", attempt, MAX_RETRIES, exc, wait
            )
            time.sleep(wait)
            continue

        # --- Layer 1: HTTP status ---
        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            if attempt == MAX_RETRIES:
                raise EStatError(f"HTTP {resp.status_code} after {attempt} tries")
            logger.warning(
                "HTTP %d (try %d/%d) — retrying in %.0fs",
                resp.status_code,
                attempt,
                MAX_RETRIES,
                wait,
            )
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            # 403 (WAF), 400 and the like: retrying will not fix them
            raise EStatError(f"HTTP {resp.status_code} (non-retryable): {resp.text[:200]}")

        # --- Layer 2: in-body RESULT.STATUS ---
        try:
            body = resp.json()
        except ValueError as exc:
            # Not JSON (e.g. a WAF HTML page). Give up, keeping the start of the body
            raise EStatError(f"non-JSON response: {resp.text[:200]}") from exc

        result = next(iter(body.values())).get("RESULT", {})
        status = result.get("STATUS")
        if status in (0, 1, 2):
            return body  # success (STATUS 1 = zero matches is also success; the caller decides)
        if status in RETRYABLE_BODY_STATUS:
            if attempt == MAX_RETRIES:
                raise EStatError(f"body STATUS {status} after {attempt} tries")
            logger.warning(
                "body STATUS %d (try %d/%d) — retrying in %.0fs", status, attempt, MAX_RETRIES, wait
            )
            time.sleep(wait)
            continue
        # STATUS 100 (auth), 101 (missing parameter), 102 (invalid value), 300 (no data) etc. are permanent
        raise EStatError(f"body STATUS {status}: {result.get('ERROR_MSG')}")

    raise EStatError("exhausted retries")  # unreachable; defensive


def fetch_recent_month_codes(session: requests.Session, n: int = RECENT_MONTHS) -> list[str]:
    """Fetch monthly time-axis codes from getMetaInfo and return the newest n.

    Re-fetched on every run because a new month appears each month (rolling window).
    """
    body = _get_with_retry(session, "getMetaInfo", {"statsDataId": STATS_DATA_ID})
    class_objs = body["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
    if isinstance(class_objs, dict):
        class_objs = [class_objs]

    time_obj = next(o for o in class_objs if o["@id"] == "time")
    classes = time_obj["CLASS"]
    if isinstance(classes, dict):
        classes = [classes]

    # Monthly entries are those whose name contains "月" (month); descending code = newest first.
    months = [c["@code"] for c in classes if "月" in c["@name"]]
    months.sort(reverse=True)
    if len(months) < n:
        logger.warning("Only %d monthly entries available (requested %d)", len(months), n)
    return months[:n]


def fetch_cpi_values(session: requests.Session, time_codes: list[str]) -> list[dict]:
    """Fetch the VALUE array for the target scope from getStatsData.

    The current scope (28,800 rows) fits in one request under the 100,000-row limit,
    but continuation pages (NEXT_KEY) are followed anyway in case the scope grows.
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
        if isinstance(values, dict):  # a single value arrives as a dict, not a list
            values = [values]
        all_values.extend(values)

        next_key = stat.get("RESULT_INF", {}).get("NEXT_KEY")
        if not next_key:
            break
        start_position = next_key
        logger.info("More data available; continuing from position %s", next_key)

    return all_values


def parse_values(values: list[dict]) -> pd.DataFrame:
    """Convert the VALUE array into a DataFrame matching the raw.cpi schema.

    - Keys @tab/@cat01/@area/@time map to the code columns; @unit is NULL when absent.
    - The value `$` is kept verbatim in value_raw; value is NULL when not numeric
      ('***' = no data, and symbols such as '-' also become NULL here).
    """
    if not values:
        return pd.DataFrame(
            columns=[
                "tab_code",
                "area_code",
                "category_code",
                "time_code",
                "value",
                "value_raw",
                "unit",
            ]
        )

    df = pd.DataFrame(values)
    out = pd.DataFrame(
        {
            "tab_code": df["@tab"],
            "area_code": df["@area"],
            "category_code": df["@cat01"],
            "time_code": df["@time"],
            "value_raw": df["$"],
            "unit": df["@unit"] if "@unit" in df.columns else pd.NA,
        }
    )
    # Convert to numbers (failures become NaN, later NULL). value_raw keeps the original.
    out["value"] = pd.to_numeric(out["value_raw"], errors="coerce")
    return out[
        ["tab_code", "area_code", "category_code", "time_code", "value", "value_raw", "unit"]
    ]


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
    """Return a connection to the warehouse, configured from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


def upsert_cpi(df: pd.DataFrame, conn=None) -> int:
    """UPSERT the DataFrame into raw.cpi idempotently. Returns the number of rows written.

    Rows that collide on the primary key (tab, area, category, time) have their values
    updated and loaded_at set to now; new rows are inserted. Re-runs never duplicate
    rows, and late revisions to recent months are picked up.
    """
    if df.empty:
        logger.warning("Empty DataFrame; skipping UPSERT")
        return 0

    # Convert NaN/NA to None so psycopg2 sends them as NULL
    safe = df.astype(object).where(pd.notna(df), None)
    rows = [
        (r.tab_code, r.area_code, r.category_code, r.time_code, r.value, r.value_raw, r.unit)
        for r in safe.itertuples(index=False)
    ]

    own_conn = conn is None
    if own_conn:
        conn = _connect()
    try:
        with conn.cursor() as cur:
            # loaded_at is left to DEFAULT now(), so it is not in VALUES
            template = "(%s, %s, %s, %s, %s, %s, %s, now())"
            execute_values(cur, UPSERT_SQL, rows, template=template, page_size=1000)
        conn.commit()
        logger.info("UPSERT complete: %d rows", len(rows))
        return len(rows)
    finally:
        if own_conn:
            conn.close()


def main() -> int:
    """Fetch CPI and load it idempotently into raw.cpi. Returns the number of rows loaded.

    Shared entry point for the CLI, CI and the Airflow PythonOperator. Kept as plain
    Python with no Airflow dependency, so it stays decoupled from the orchestrator.
    """
    app_id = os.environ["ESTAT_APP_ID"]
    session = _build_session(app_id)

    months = fetch_recent_month_codes(session)
    logger.info("Months fetched: %d  newest=%s  oldest=%s", len(months), months[0], months[-1])

    values = fetch_cpi_values(session, months)
    logger.info("VALUE count: %d", len(values))

    df = parse_values(values)
    logger.info(
        "DataFrame shape: %s  rows with NULL value (*** etc.): %d",
        df.shape,
        df["value"].isna().sum(),
    )

    written = upsert_cpi(df)
    logger.info("UPSERT rows: %d", written)
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
