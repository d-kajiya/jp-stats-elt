# Project Status

> このドキュメントはプロジェクトの**進捗状況と意思決定の履歴**を残す内部ドキュメントです。
> README が「使い方」を、architecture.md が「設計」を語るのに対し、
> こちらは「いつ・何を・なぜ決めたか」を時系列で残します。

## Roadmap

| Week | Goal | Status |
|---|---|---|
| 1-2 | Repository scaffold, Docker Compose, minimal DAG | **In progress** (local env done, Docker startup pending) |
| 3-4 | e-Stat extraction + idempotent load into `raw.*` | Not started |
| 5-6 | dbt staging / intermediate / marts + tests | Not started |
| 7-8 | GitHub Actions CI, architecture docs, README polish | Not started |

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

### Pending

- `make build` + `make up` — verify Docker stack starts and Airflow UI is reachable at `http://localhost:8080`
- Trigger DAG `jp_stats_elt` manually and confirm all 6 tasks succeed
- `git init` → first commit → push to GitHub

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

## Outstanding questions before Week 3

The following must be resolved before starting Week 3:

1. **e-Stat API application ID**: Needs to be obtained from <https://www.e-stat.go.jp/api/>.
2. **CPI ingestion granularity**: Recommended starting scope — national + 47 prefectures, top-10 item categories. Full item hierarchy can be added later.
3. **Time range**: Recommended starting scope — most recent 5 years (60 months). Backfill can extend later.

---

## Lessons learned (Week 1-2 setup pitfalls)

Notes for future reference and for portfolio talking points:

- Windows display paths (`ドキュメント`) vs. real filesystem paths can diverge after OneDrive migrations. Always verify with `find /mnt/c/Users/<user> -name "..."`.
- `setuptools` automatic package discovery breaks when a project has multiple top-level directories that look like Python packages. Solution: declare `[tool.setuptools] packages = []` for application projects.
- Makefile's default shell is `/bin/sh` (dash on Ubuntu), not bash. Avoid bash-specific syntax like process substitution `<(...)`.
- WSL2 first-time pytest startup can take 10–30 seconds due to plugin loading and file system cold cache. Subsequent runs are fast.
- `C:\ProgramData` ownership can become broken after manual deletions, blocking Docker Desktop installation. Diagnostic: `Get-Acl C:\ProgramData | fl Owner`.
- Airflow's official Docker image runs as UID 50000. On bind-mounted log directories, the host directory must be owned by 50000:0 or the container fails with PermissionError when creating per-DAG log subdirectories. Solved with make init-dirs target.
---

**Last updated**: 2026-05-06 (Week 1-2, local environment complete)
