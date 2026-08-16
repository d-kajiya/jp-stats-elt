-- Mart: for each month and expenditure category, rank the 47 prefectural
-- capital cities by year-over-year CPI change, alongside the national figure.
--
-- Answers "which cities saw food prices rise fastest last month, and how far
-- above the national average were they" -- the geographic comparison this
-- dataset exists to support.
--
-- Why the national aggregate is excluded from the ranking:
--   area_code 00000 is not a city; it is the weighted average of the survey.
--   Ranking it against its own components puts it mid-table (its mean YoY of
--   2.23% sits close to the overall 2.15%), which is meaningless. It is
--   carried as national_yoy_change_pct instead, so each city can be read as a
--   spread against the national figure rather than just an ordinal position.
--
-- RANK() rather than ROW_NUMBER(): the index is published to one decimal, so
-- ties are real. Tied cities should share a rank, not be separated arbitrarily.
--
-- Rows where yoy_change_pct is NULL (the first twelve months, which have no
-- prior year) are excluded -- there is nothing to rank.
--
-- Grain: one row per (period, category_code, area_code), cities only.
with yoy as (
    select * from {{ ref('agg_cpi_yoy_change') }}
),
national as (
    select
        period,
        category_code,
        yoy_change_pct as national_yoy_change_pct
    from yoy
    where area_code = '00000'
),
cities as (
    select *
    from yoy
    where area_code <> '00000'
      and yoy_change_pct is not null
)
select
    c.period,
    c.category_code,
    c.category_name_ja,
    c.category_name_en,
    c.category_display_order,
    c.area_code,
    c.city_name_ja,
    c.city_name_en,
    c.index_value,
    c.yoy_change_pct,
    n.national_yoy_change_pct,
    round(c.yoy_change_pct - n.national_yoy_change_pct, 2) as spread_vs_national_pt,
    rank() over (
        partition by c.period, c.category_code
        order by c.yoy_change_pct desc
    ) as yoy_rank_desc
from cities as c
left join national as n
    on  n.period        = c.period
    and n.category_code = c.category_code
