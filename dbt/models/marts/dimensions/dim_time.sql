-- Time dimension table
-- Generates time of day attributes for each minute

{{ config(materialized='table') }}

with hour_generator as (
    {{ dbt_utils.generate_series(24) }}
),

minute_generator as (
    {{ dbt_utils.generate_series(60) }}
),

times as (
    select
        h.generated_number as hour_24,
        m.generated_number as minute
    from hour_generator h
    cross join minute_generator m
    where h.generated_number < 24 and m.generated_number < 60
),

enriched_times as (
    select
        -- Surrogate key (HHMM format)
        hour_24 * 100 + minute as time_key,
        
        -- Full time
        time_from_parts(hour_24, minute, 0) as full_time,
        
        -- Hour attributes
        hour_24,
        case 
            when hour_24 = 0 then 12
            when hour_24 > 12 then hour_24 - 12
            else hour_24
        end as hour_12,
        minute,
        case when hour_24 < 12 then 'AM' else 'PM' end as am_pm,
        
        -- Time period
        case
            when hour_24 between 6 and 10 then 'Breakfast'
            when hour_24 between 11 and 14 then 'Lunch'
            when hour_24 between 15 and 17 then 'Afternoon'
            when hour_24 between 18 and 21 then 'Dinner'
            else 'Late Night'
        end as time_period,
        
        -- Peak hour flag (lunch and dinner rush)
        hour_24 between 11 and 13 or hour_24 between 17 and 19 as is_peak_hour,
        
        -- Metadata
        current_timestamp() as _created_at
        
    from times
)

select * from enriched_times
