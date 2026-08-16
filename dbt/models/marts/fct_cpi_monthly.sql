-- Mart: the monthly CPI fact table analysts and BI tools query directly.
--
-- Why this exists separately from int_cpi_monthly:
--   The intermediate layer is a view, so every downstream query re-executes
--   its joins and the complete-month scan. This model persists the same
--   grain as a table (see marts +materialized: table in dbt_project.yml) so
--   the aggregate models below it, and any ad-hoc analysis, read from
--   materialised storage instead of recomputing the join each time.
--
-- Grain: one row per (period, area_code, category_code) -- 60 months x 48
-- areas x 10 major expenditure groups.
with intermediate as (
    select * from {{ ref('int_cpi_monthly') }}
)
select
    period,
    area_code,
    city_name_ja,
    city_name_en,
    prefecture_name_ja,
    prefecture_name_en,
    category_code,
    category_name_ja,
    category_name_en,
    category_display_order,
    index_value
from intermediate
