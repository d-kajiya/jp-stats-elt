# Architecture

> This document is populated in **Week 7-8** of the roadmap.
> The current scaffold contains only a high-level overview; data lineage diagrams,
> retry/idempotency reasoning, and CI flow will be filled in once the dbt models and
> CI workflow are in place.

## Sections to write

- Component diagram (e-Stat → Airflow → Postgres → dbt → marts)
- Data lineage (raw → staging → intermediate → marts) with dbt's auto-generated DAG
- Idempotency strategy (execution_date as partition key, upsert semantics)
- Retry policy and failure modes
- CI pipeline flow (PR trigger → dbt test → pytest → status check)
- Trade-offs and "what I would do differently in production"
