-- Mart: year-over-year change of the CPI index, by area and category.
--
-- Why a self join on dates rather than LAG(index_value, 12):
--   LAG counts *rows*, not months. It only equals "twelve months earlier"
--   while the series has no gaps, and if a month were ever missing it would
--   silently compare against the wrong period instead of failing. Joining on
--   period = period - interval '1 year' states the intent directly: a missing
--   prior year yields NULL rather than a wrong number.
--
-- The first twelve months of the window have no prior year by definition, so
-- yoy_change_pct is NULL there (5,760 of 28,800 rows at the current scope).
--
-- Grain: one row per (period, area_code, category_code), same as the fact.
with fct as (
    select * from {{ ref('fct_cpi_monthly') }}
),
with_prior_year as (
    select
        curr.period,
        curr.area_code,
        curr.city_name_ja,
        curr.city_name_en,
        curr.category_code,
        curr.category_name_ja,
        curr.category_name_en,
        curr.category_display_order,
        curr.index_value,
        prev.index_value as index_value_prev_year
    from fct as curr
    left join fct as prev
        on  prev.area_code     = curr.area_code
        and prev.category_code = curr.category_code
        and prev.period        = curr.period - interval '1 year'
)
select
    *,
    round(
        (index_value - index_value_prev_year) / nullif(index_value_prev_year, 0) * 100,
        2
    ) as yoy_change_pct
from with_prior_year
