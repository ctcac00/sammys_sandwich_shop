-- Report: Location Daily Comparison
-- Compares daily performance across locations

{{ config(materialized='view') }}

with fact_daily_summary as (
    select * from {{ ref('fct_daily_summary') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
    where is_current = true
)

select 
    dd.full_date as order_date,
    dd.day_name,
    dl.location_name,
    fds.total_orders,
    fds.total_revenue,
    fds.avg_order_value,
    
    -- Compare to location average
    avg(fds.total_revenue) over (partition by dl.location_sk) as location_avg_revenue,
    fds.total_revenue - avg(fds.total_revenue) over (partition by dl.location_sk) as variance_from_avg,
    
    -- Compare to same day last week
    lag(fds.total_revenue, 7) over (
        partition by dl.location_sk 
        order by dd.full_date
    ) as same_day_last_week,
    
    -- Day of week average for this location
    avg(fds.total_revenue) over (
        partition by dl.location_sk, dd.day_of_week
    ) as day_of_week_avg
    
from fact_daily_summary fds
join dim_date dd on fds.date_key = dd.date_key
join dim_location dl on fds.location_sk = dl.location_sk
order by dd.full_date desc, dl.location_name
