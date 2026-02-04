-- Report: Daily Sales Summary
-- Provides daily sales metrics across all locations with comparisons

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
    dd.is_weekend,
    dl.location_name,
    dl.region,
    
    -- Volume metrics
    fds.total_orders,
    fds.total_items_sold,
    fds.unique_customers,
    fds.guest_orders,
    
    -- Revenue metrics
    fds.gross_sales,
    fds.discounts_given,
    fds.net_sales,
    fds.tax_collected,
    fds.tips_received,
    fds.total_revenue,
    
    -- Performance metrics
    fds.avg_order_value,
    fds.avg_items_per_order,
    fds.discount_rate,
    
    -- Order type breakdown
    fds.dine_in_orders,
    fds.takeout_orders,
    fds.drive_thru_orders,
    
    -- Period breakdown
    fds.breakfast_orders,
    fds.lunch_orders,
    fds.dinner_orders,
    
    -- Comparison metrics (vs previous day)
    lag(fds.total_revenue) over (
        partition by dl.location_id 
        order by dd.full_date
    ) as prev_day_revenue,
    fds.total_revenue - lag(fds.total_revenue) over (
        partition by dl.location_id 
        order by dd.full_date
    ) as revenue_change,
    
    -- Week over week comparison
    lag(fds.total_revenue, 7) over (
        partition by dl.location_id 
        order by dd.full_date
    ) as same_day_last_week_revenue
    
from fact_daily_summary fds
join dim_date dd on fds.date_key = dd.date_key
join dim_location dl on fds.location_sk = dl.location_sk
order by dd.full_date desc, dl.location_name
