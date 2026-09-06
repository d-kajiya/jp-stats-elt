# Project Status

> このドキュメントはプロジェクトの**進捗状況と意思決定の履歴**を残す内部ドキュメントです。
> README が「使い方」を、architecture.md が「設計」を語るのに対し、
> こちらは「いつ・何を・なぜ決めたか」を時系列で残します。

>This is an internal document that records the project's **progress and decision-making history**.
>while the README covers "usage" and architecture.md details the "design,"
>this document chronicles "what was decided, when, and why" in chronological order.
## Roadmap
| Week | Goal | Status |
|---|---|---|
| 1-2 | Repository scaffold, Docker Compose, minimal DAG | **Done** (stack healthy, DAG parses, pushed to GitHub) |
| 3-4 | e-Stat extraction + idempotent load into `raw.*` | **Done** (statsDataId=0003427113, 28,330 rows loaded; wired into the DAG as PythonOperator) |
| 5-6 | dbt staging / intermediate / marts + tests | **Done** (2 seeds, 7 models, 17 data tests; `dbt build` = 26 PASS) |
| 7-8 | GitHub Actions CI, architecture docs, README polish | **In progress** (CI green: lint / pytest / dbt; docs pending) |
---

## Week 1-2 progress detail

### Completed

- Repository structure (`airflow/`, `dbt/`, `scripts/`, `tests/`, `docs/`, `.github/workflows/`)
- `Dockerfile` — Airflow 2.9.3 + Python 3.11 + dbt-core 1.8 + Great Expectations 0.18
- `docker-compose.yml` — postgres + airflow-init / webserver / scheduler
- `scripts/init-warehouse.sql` — auto-creates warehouse DB and `raw / staging / intermediate / marts` schemas
- Minimal DAG `airflow/dags/jp_stats_elt_dag.py` — placeholder tasks: `start → extract → load → dbt_run → dbt_test → end`
- dbt project skeleton (`dbt_project.yml`, `profiles.yml` reading from env vars)
- `pyproject.toml` + `requirements.lock` — 247 packages pinned via Airflow constraints
- `Makefile` — `setup`, `lint`, `test`, `up`, `down`, `logs`, `clean`
- `tests/test_dag_integrity.py` — passes locally (3 passed in 2.03s)

### Resolved during Week 1-2: Airflow log UID mismatch

The stack restart-looped after `make up`. The container runs as UID `50000`, but
the bind-mounted `airflow/logs/` was owned by the host user (UID 1000), so Airflow
could not write and raised `PermissionError`. Fixed with
`sudo chown -R 50000:0 airflow/logs`, made permanent as the `init-dirs` target in
the Makefile so a fresh clone does not hit the same wall.

---

## Week 3-4 progress detail

### Week 3 — e-Stat extraction

- `scripts/extract_estat.py` — API client with retry/backoff, dynamic time-axis
  fetch, and idempotent UPSERT into `raw.cpi`
- Verified against the live API: **28,330 rows** loaded; idempotency proven at the
  database level (re-running leaves row count unchanged, only `loaded_at` moves)
- Extraction scope: `cdTab=1` (index), 10 explicit `cdCat01` codes, 48 areas
  (nationwide + 47 prefectural capitals), rolling 60-month window

Two domain corrections came out of this week. CPI is a **city** survey, so there is
no prefecture-level data — the unit is the prefectural capital, and Fukuoka's
representative city is Fukuoka City (`40A02`), not Kitakyushu (`40A01`). The ten
major categories also cannot be selected via `lvCat01`; the codes must be listed
explicitly.

### Week 4 — DAG integration

- `extract_estat.main()` extracted as a shared entry point for CLI, CI and Airflow
- DB credentials unified under the `WAREHOUSE_*` namespace
- `extract` became a `PythonOperator`; `load` was **repurposed** as `validate_load`,
  a contract gate asserting `raw.cpi` holds at least 20,000 rows
- End-to-end run: all 6 tasks succeeded

Extract and load were deliberately **not** split into separate tasks. They are
technically inseparable here — the DataFrame goes straight from memory into the
UPSERT — and passing a large DataFrame through XCom would be fragile. Turning
`load` into a validation gate keeps the task count honest and sets up the later
Great Expectations work.

---

## Week 5-6 progress detail

### Week 5 — dbt staging

- `_estat__sources.yml` declares the `estat` source; `not_null` on the four PK
  columns plus `loaded_at`. `value` / `value_raw` / `unit` are left untested on
  purpose, because `***` legitimately becomes NULL
- `stg_cpi_raw.sql` — typed 1:1 view; `period DATE` derived from `time_code`
  (format `YYYY00MMMM`, verified across all 60 months in the real data)
- `area_master` seed (48 rows) plus `stg_area_master`
- `generate_schema_name` macro override
- `dbt_seed` added to the DAG, and the `|| echo` fallbacks removed from
  `dbt_run` / `dbt_test`

Two things are worth recording. The **protobuf conflict** that had been open for
weeks was closed here: dbt-core 1.8 needs `protobuf>=5.0`, but Airflow 2.9.3's
constraints pin it to 4.25.3, and the constrained install in the Dockerfile was
silently downgrading it after dbt was installed. The fix is ordering — an
independent `RUN pip install "protobuf==5.29.6"` **after** the constrained
install, so nothing can pull it back down.

The `|| echo` fallbacks were **swallowing dbt failures into task success**. They
could only be removed once protobuf was fixed and exit code 0 was guaranteed;
removing them earlier would have turned passing dbt runs into failing tasks.

### Week 6 — intermediate and marts

- `category_master` seed (10 rows) + `stg_category_master`
- `int_cpi_monthly` (view) — complete-month filter, joins area and category names
- `fct_cpi_monthly` → `agg_cpi_yoy_change` → `agg_cpi_category_rank` (tables)
- `dbt_utils` pinned to 1.3.0; both `packages.yml` and `package-lock.yml` committed
- 6 marts tests (grain, value range, join integrity); `dbt build` = 26 PASS
- `dbt_deps` added to the DAG ahead of `dbt_seed`; full run = 8 tasks succeeded

**dbt seed's type inference is quietly destructive.** It read `category_code` as
integer and stripped the leading zero from `0002`, turning it into `2` and breaking
every join against `stg_cpi_raw` — while exiting 0 and looking successful. Fixed by
declaring `column_types: text` in `_seeds.yml`, which only takes effect with
`--full-refresh` because an ordinary seed is truncate-and-insert and keeps the
existing column types. `area_master` escaped this only by luck: codes like `13A01`
contain letters, so they were never inferred as integers.

Year-over-year uses a **date self-join** rather than `LAG(index_value, 12)`.
LAG counts twelve *rows* back, not twelve *months*; the two agree only when the
series has no gaps, and when a gap appears LAG silently compares the wrong month
instead of raising anything. A date join simply yields NULL when no matching row
exists.

---
## Week 7 progress detail

### GitHub Actions CI

Three jobs run in parallel on push to `main` and on PRs, in `.github/workflows/ci.yml`:

| Job | What it runs | Duration |
|---|---|---|
| `lint` | `ruff check` + `ruff format --check` | ~10s |
| `test` | `pytest -rs` (11 passed, 1 skipped) | ~1m |
| `dbt` | `dbt deps` + `dbt build` against a postgres service | ~1m30s |

**ruff was unpinned.** `pyproject.toml` listed it with no version, so local and CI
could resolve to different builds and disagree about what counts as a lint error.
It is now pinned to `0.16.4` — the same reasoning already applied to
`requirements.lock` and to `dbt_utils`. This mattered immediately: the lock file
still carried ruff 0.3.3 from Week 1-2, and without the pin CI would have linted
with a version 13 minors behind the local one.

Upgrading 0.3.3 → 0.16.4 surfaced only one new violation (import ordering), and
`ruff format` was then applied across all Python sources as a single `style:`
commit. `AREA_CODES` is wrapped in `fmt: off` / `fmt: on` so the 47-prefecture
grid stays readable; a formatter is worth deferring to everywhere except where
the layout itself carries information.

### Seeding `raw.cpi` in CI

The open question from Week 6 was how `dbt build` could pass when CI has no
extracted data. Excluding the source and staging models would have been simplest,
but everything downstream depends on `stg_cpi_raw`, so CI would have verified
almost nothing. Calling the e-Stat API from CI would make every run depend on an
external service that is known to return 502 during maintenance windows.

The chosen approach is a committed fixture: `tests/fixtures/raw_cpi_sample.csv`,
a 14-month slice of the real data (6,250 rows). CI runs the production
`scripts/init-warehouse.sql` — the same DDL the Docker stack uses, so the CI
schema cannot drift from production — and copies the fixture into `raw.cpi`.

The fixture deliberately includes **2026-07, which covers only one area**, because
that is what makes the complete-month filter in `int_cpi_monthly` observably do
something. Raw holds 6,250 rows and marts hold 6,240; if a regression dropped the
`HAVING` clause, that difference would disappear and CI would fail rather than
pass silently.

`profiles.yml` needed no CI-specific target: every field already reads from
`env_var`, so exporting `WAREHOUSE_*` is enough to point the `dev` target at the
CI postgres service. That was a Week 1-2 decision paying off six weeks later.

### Fixed along the way

- `warehouse_conn` in `tests/test_extract_estat.py` still read
  `POSTGRES_USER` / `POSTGRES_PASSWORD` while host, port and dbname had already
  moved to `WAREHOUSE_*` in Week 4. Harmless locally, since both default to
  `airflow`, but it would have ignored credentials injected by CI
- `dbt_project.yml` still described the intermediate layer as doing pivots, which
  stopped being true when `int_cpi_pivoted` was dropped

---

## Key technical decisions

These were settled in earlier discussions and should not be re-opened without explicit reason.

### Airflow runtime: official image + custom Dockerfile

**Decided**: Use `apache/airflow:2.9.3-python3.11` as the base, layer dbt and Great Expectations on top in a custom Dockerfile.

**Why**: Bare official image requires `pip install` on every container start, hurting reproducibility. Astronomer Runtime adds a vendor dependency that is unnecessary for a portfolio project. Cosmos was considered but deferred — the goal is to demonstrate explicit DAG dependency authoring, which is more valuable in interviews than the abstraction Cosmos provides.

### dbt execution: same container as Airflow, invoked via BashOperator

**Decided**: dbt runs inside the Airflow container; tasks call `dbt run --profiles-dir ...` via `BashOperator`.

**Why**: Demonstrates manual orchestration of dbt within Airflow, which is a more explicit and portable skill than relying on Cosmos. Cosmos is mentioned in `README.md` as a "future improvement".

### Local development: uv + .venv + pyproject.toml + requirements.lock

**Decided**: All local Python dependencies are managed by uv. The `.venv` mirrors the container's package set so that DAG integrity tests and `dbt parse` can run on the host.

**Why**: Consistency with the developer's other portfolio repos (`data-cleaning-project`, `messy-data-cleaning-pyspark`). uv is faster than pip and produces deterministic lockfiles.

### Container internal: pip (NOT uv)

**Decided**: Inside the Docker image, dependencies are installed with `pip` using Airflow's official constraints file.

**Why**: The Airflow official image is built around the assumption "container = the venv". Adding uv inside the container would create double management without clear benefit, and Airflow's constraints file is best applied via standard pip.

### Project itself is not packaged

**Decided**: `pyproject.toml` declares dependencies but `[tool.setuptools] packages = []` prevents the project from being installed as a Python package.

**Why**: This is an application (DAGs + dbt + Docker), not a distributable library. Setuptools' default auto-discovery would pick up `airflow/` and `dbt/` as candidate packages and fail with "multiple top-level packages". Explicitly setting `packages = []` is the cleanest fix.

### Development OS: WSL2 (Ubuntu) on Windows 11

**Decided**: All development happens inside WSL2, accessed via VSCode's WSL extension. Windows-side files (`/mnt/c/...`) are avoided.

**Why**: Linux-native filesystem performance, parity with production environments, alignment with AWS/GCP CLI workflows, and Docker Desktop's WSL2 backend integration.

---

## Environment pitfalls

Notes for future reference and for portfolio talking points:

- Windows display paths (`ドキュメント`) vs. real filesystem paths can diverge after OneDrive migrations. Always verify with `find /mnt/c/Users/<user> -name "..."`.
- `setuptools` automatic package discovery breaks when a project has multiple top-level directories that look like Python packages. Solution: declare `[tool.setuptools] packages = []` for application projects.
- Makefile's default shell is `/bin/sh` (dash on Ubuntu), not bash. Avoid bash-specific syntax like process substitution `<(...)`.
- WSL2 first-time pytest startup can take 10–30 seconds due to plugin loading and file system cold cache. Subsequent runs are fast.
- `C:\ProgramData` ownership can become broken after manual deletions, blocking Docker Desktop installation. Diagnostic: `Get-Acl C:\ProgramData | fl Owner`.
- Airflow's official Docker image runs as UID 50000. On bind-mounted log directories, the host directory must be owned by 50000:0 or the container fails with PermissionError when creating per-DAG log subdirectories. Solved with make init-dirs target.
- The e-Stat API sits behind a WAF (Zenedge); curl's default User-Agent is blocked. Send an identifiable User-Agent.
- Scripts in `docker-entrypoint-initdb.d` run only on first startup with an empty data directory. Adding a table to an existing DB requires manual application (made idempotent with `CREATE TABLE IF NOT EXISTS`).
- CPI is a city-based survey with no prefecture-level data. The area unit is nationwide + 47 prefectural capital cities. Fukuoka City = 40A02.
---

**Last updated**: 2026-06-28 (Week 3, e-Stat extraction phase complete)
