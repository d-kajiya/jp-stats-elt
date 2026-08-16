-- Intermediate: analysis-ready CPI facts, restricted to complete months
-- and enriched with human-readable area / category names.
--
-- Why this layer exists:
--   Staging is a 1:1 typed view of the source and deliberately keeps every
--   row. Deciding *which* rows are fit for analysis is a business rule, so
--   it lives here rather than in staging.
--
-- Complete-month filter:
--   e-Stat publishes the newest month city by city (Tokyo 23 wards first),
--   so the most recent period is routinely partial -- at the time of writing
--   2026-07 held Tokyo only. Ranking areas or averaging nationally across a
--   partial month produces silently wrong results, so a period is included
--   only when every area defined in the area master is present.
--   The threshold is read from stg_area_master rather than hardcoded to 48,
--   so changing the extraction scope does not require editing this model.
--
-- Grain: one row per (period, area_code, category_code).
with cpi as (
    select * from {{ ref('stg_cpi_raw') }}
),
areas as (
    select * from {{ ref('stg_area_master') }}
),
categories as (
    select * from {{ ref('stg_category_master') }}
),
complete_periods as (
    select period
    from cpi
    group by period
    having count(distinct area_code) = (select count(*) from areas)
),
joined as (
    select
        c.period,
        c.area_code,
        a.city_name_ja,
        a.city_name_en,
        a.prefecture_name_ja,
        a.prefecture_name_en,
        c.category_code,
        cat.category_name_ja,
        cat.category_name_en,
        cat.display_order as category_display_order,
        c.index_value
    from cpi as c
    inner join complete_periods as p on c.period = p.period
    inner join areas as a on c.area_code = a.area_code
    inner join categories as cat on c.category_code = cat.category_code
)
select * from joined
