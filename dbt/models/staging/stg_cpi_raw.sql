-- Staging: 1:1 typed view over raw.cpi.
-- Responsibilities are limited to renaming, typing, and deriving `period` (DATE).
-- No business logic, no joins, no filtering of legitimate data.
-- The *** -> NULL handling already happened at extraction (Week 3), so `value`
-- is trusted here; we only re-assert its numeric type.

with source as (

    select * from {{ source('estat', 'cpi') }}

),

renamed as (

    select
        -- grain / dimensions
        tab_code,
        area_code,
        category_code,
        time_code,

        -- derived: YYYY00MMMM -> first day of month.
        -- Verified across all 60 months: chars 5-6 are always '00',
        -- chars 7-8 and 9-10 are the (identical) month.
        make_date(
            substr(time_code, 1, 4)::int,   -- year
            substr(time_code, 7, 2)::int,   -- month
            1                               -- day (month grain -> 1st)
        ) as period,

        -- measures
        value        as index_value,
        value_raw,
        unit,

        -- lineage / audit
        loaded_at

    from source

)

select * from renamed
