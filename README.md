# jp-stats-elt

[![CI](https://github.com/d-kajiya/jp-stats-elt/actions/workflows/ci.yml/badge.svg)](https://github.com/d-kajiya/jp-stats-elt/actions/workflows/ci.yml)

A locally-runnable ELT pipeline that ingests Japanese government statistics from the
[e-Stat API](https://www.e-stat.go.jp/api/), transforms them with **dbt**, orchestrates
the workflow with **Apache Airflow**, and validates data quality on every pull request
through **GitHub Actions CI**.

The project is built to run end-to-end on a laptop with Docker — **zero cloud cost** —
while still demonstrating the engineering practices used in production data platforms.

---

## Why this project / なぜこのプロジェクトか

Most "data engineering portfolio" repositories stop at a notebook or a single Python
script. This one is deliberately scoped to show the parts that matter on the job:

- **Orchestration** — Airflow DAG with explicit retries and idempotent execution
- **Transformation as code** — dbt models layered as staging → intermediate → marts
- **Testing in CI** — `dbt test` and Python unit tests run on every PR
- **Reproducibility** — single `docker compose up` brings up the full stack

公開されている「データエンジニアリング・ポートフォリオ」の多くは Notebook や単発
スクリプトで止まりがちですが、このプロジェクトは現場で実際に問われる要素
（オーケストレーション・冪等性・dbt の責務分離・CI でのテスト自動化）に
意図的にフォーカスしています。

---

## Architecture / アーキテクチャ

```
┌─────────────┐    ┌──────────┐    ┌────────────────┐    ┌──────────┐
│  e-Stat API │ →  │  Airflow │ →  │   PostgreSQL   │ ←  │   dbt    │
│ (CPI/Labour)│    │  (ELT)   │    │ raw → marts    │    │ (models) │
└─────────────┘    └──────────┘    └────────────────┘    └──────────┘
                                          ↑
                                 GitHub Actions CI
                              (dbt test + pytest on PR)
```

Detailed component diagram and data lineage: [`docs/architecture.md`](docs/architecture.md)
*(populated in Week 7-8)*

---

## Data sources / データソース

| Source | What it is | Why I picked it |
| --- | --- | --- |
| **Consumer Price Index (CPI)** | Monthly index by item × region | Three-axis time-series suits dbt's incremental and pivot patterns |
| **Labour Force Survey** | Monthly employment / unemployment figures | Joins with CPI to demonstrate multi-source integration |

Both are published by the Statistics Bureau of Japan and accessed via the official
e-Stat REST API.

---

## Tech stack

| Layer | Tool |
| --- | --- |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Storage | PostgreSQL 15 (single instance, separate logical DBs for metadata and warehouse) |
| Transformation | dbt-core 1.8 + dbt-postgres |
| Data quality | Great Expectations + dbt tests |
| CI | GitHub Actions |
| Runtime | Docker Compose |

---

## Quick start

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- An e-Stat API application ID — [register here](https://www.e-stat.go.jp/api/) (free)

### Setup

```bash
# 1. Clone & enter the repo
git clone https://github.com/d-kajiya/jp-stats-elt.git
cd jp-stats-elt

# 2. Configure environment
cp .env.example .env
# → edit .env and set ESTAT_APP_ID

# 3. Build & start the stack
docker compose build
docker compose up -d

# 4. Open the Airflow UI
open http://localhost:8080      # user: admin / pass: admin
```

### Trigger the pipeline

In the Airflow UI, un-pause `jp_stats_elt` and click **▶ Trigger DAG**.

Or from the CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger jp_stats_elt
```

### Tear down

```bash
docker compose down       # stop containers, keep data
docker compose down -v    # also wipe the Postgres volume
```

---

## Local development / ローカル開発環境

Two Python environments coexist by design:

| Environment | Purpose | Managed by |
| --- | --- | --- |
| **Container** | Run Airflow & dbt for real ELT execution | `pip` inside the Docker image |
| **Host (`.venv`)** | IDE completion, `pytest`, `dbt parse`, lint | [`uv`](https://github.com/astral-sh/uv) |

The host venv mirrors the container's package set so that tests and DAG integrity
checks can run without spinning up Docker.

### Setup with uv

```bash
# 1. Install uv (one-time, if you don't have it)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Create .venv and install all dependencies (uses requirements.lock for reproducibility)
make setup

# 3. Activate
source .venv/bin/activate

# 4. Verify
pytest                  # DAG integrity tests
ruff check .            # Lint
```

`uv` automatically picks up Python 3.11 from `.python-version` and provisions it
if it is not already installed locally.

### Updating dependencies

When you change `pyproject.toml`, regenerate the lock file:

```bash
make lock                # rewrites requirements.lock
git add pyproject.toml requirements.lock
```

### Why pinned to Airflow constraints?

Airflow has a wide dependency tree that is unstable to resolve from scratch. The
`Makefile` and `requirements.lock` both reference the official Airflow constraint
file (`constraints-2.9.3/constraints-3.11.txt`) so that local, container, and CI
environments resolve to the same versions.

---

## Repository layout

```
.
├── airflow/
│   ├── dags/                # Airflow DAGs (jp_stats_elt_dag.py)
│   └── plugins/
├── dbt/
│   ├── models/
│   │   ├── staging/         # 1:1 cleanup of raw tables
│   │   ├── intermediate/    # Joins / pivots
│   │   └── marts/           # Analytical fact & aggregate tables
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   └── init-warehouse.sql   # Creates the warehouse DB & schemas on first boot
├── tests/                   # Python unit tests (pytest)
├── docs/
│   └── architecture.md
├── .github/workflows/       # CI definitions
├── Dockerfile               # Airflow + dbt image
├── docker-compose.yml
├── pyproject.toml           # Local dev dependencies (uv)
├── requirements.lock        # Pinned versions for reproducible installs
├── .python-version          # Pin Python 3.11 for uv
├── Makefile                 # setup / lint / test / docker shortcuts
└── README.md
```

---

## Roadmap

- [x] **Week 1-2** — Repository scaffold, Docker Compose, minimal DAG
- [ ] **Week 3-4** — e-Stat extraction + idempotent load into `raw.*`
- [ ] **Week 5-6** — dbt staging / intermediate / marts + tests
- [ ] **Week 7-8** — GitHub Actions CI, architecture docs, English README polish

### Future improvements (out of scope for the initial release)

- Replace BashOperator-driven dbt with [Cosmos](https://github.com/astronomer/astronomer-cosmos)
  to expose dbt model dependencies as native Airflow tasks
- Migrate the warehouse to DuckDB or Snowflake for partition-aware incremental builds
- Add a Streamlit / Metabase dashboard layer on top of `marts`

---

## License

MIT
