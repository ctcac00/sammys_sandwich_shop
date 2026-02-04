-- Report: Weekly Sales Summary
-- Aggregates daily sales to weekly level

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
    dd.year,
    dd.week_of_year,
    min(dd.full_date) as week_start_date,
    max(dd.full_date) as week_end_date,
    dl.location_name,
    dl.region,
    
    -- Volume metrics
    sum(fds.total_orders) as total_orders,
    sum(fds.total_items_sold) as total_items_sold,
    sum(fds.unique_customers) as total_customer_visits,
    
    -- Revenue metrics
    sum(fds.gross_sales) as gross_sales,
    sum(fds.discounts_given) as discounts_given,
    sum(fds.net_sales) as net_sales,
    sum(fds.total_revenue) as total_revenue,
    
    -- Averages
    round(avg(fds.avg_order_value), 2) as avg_order_value,
    count(distinct fds.date_key) as days_of_data,
    round(sum(fds.total_revenue) / nullif(count(distinct fds.date_key), 0), 2) as avg_daily_revenue
    
from fact_daily_summary fds
join dim_date dd on fds.date_key = dd.date_key
join dim_location dl on fds.location_sk = dl.location_sk
group by dd.year, dd.week_of_year, dl.location_name, dl.region
order by dd.year desc, dd.week_of_year desc, dl.location_name
