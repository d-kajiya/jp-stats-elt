-- Staging: typed 1:1 view over the area_master seed.
-- The seed (dbt/seeds/area_master.csv) is the raw load; this view is the
-- typed access layer downstream models should join against, mirroring the
-- source -> staging separation used for stg_cpi_raw.
--
-- Grain: one row per area_code (48 rows = national + 47 prefectural-capital
-- cities). Note area_code 40 uses 40A02 (Fukuoka City), not 40A01.

with source as (

    select * from {{ ref('area_master') }}

),

renamed as (

    select
        area_code,
        city_name_ja,
        city_name_en,
        prefecture_name_ja,
        prefecture_name_en
    from source

)

select * from renamed
