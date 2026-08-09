-- Staging: typed 1:1 view over the category_master seed.
-- The seed (dbt/seeds/category_master.csv) is the raw load; this view is the
-- typed access layer downstream models should join against, mirroring the
-- source -> staging separation used for stg_cpi_raw and stg_area_master.
--
-- Grain: one row per category_code (10 rows = the 10 major expenditure
-- groups of the Japanese CPI). Japanese names come from the e-Stat
-- getMetaInfo API (cat01, level 1); there is no deeper hierarchy in the
-- extracted scope, so no flattening is required downstream.
with source as (
    select * from {{ ref('category_master') }}
),
renamed as (
    select
        category_code,
        category_name_ja,
        category_name_en,
        display_order
    from source
)
select * from renamed
