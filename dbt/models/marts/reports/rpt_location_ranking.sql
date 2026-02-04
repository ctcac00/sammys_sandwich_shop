-- Report: Location Ranking
-- Ranks locations by various performance metrics

{{ config(materialized='view') }}

with fact_daily_summary as (
    select * from {{ ref('fct_daily_summary') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
    where is_current = true
),

location_metrics as (
    select 
        dl.location_name,
        dl.region,
        sum(fds.total_revenue) as total_revenue,
        sum(fds.total_orders) as total_orders,
        round(avg(fds.avg_order_value), 2) as avg_order_value,
        sum(fds.unique_customers) as total_customers
        
    from fact_daily_summary fds
    join dim_location dl on fds.location_sk = dl.location_sk
    group by dl.location_name, dl.region
)

select 
    location_name,
    region,
    total_revenue,
    total_orders,
    avg_order_value,
    total_customers,
    
    -- Rankings
    row_number() over (order by total_revenue desc) as revenue_rank,
    row_number() over (order by total_orders desc) as orders_rank,
    row_number() over (order by avg_order_value desc) as avg_order_value_rank,
    row_number() over (order by total_customers desc) as customers_rank
    
from location_metrics
order by revenue_rank
