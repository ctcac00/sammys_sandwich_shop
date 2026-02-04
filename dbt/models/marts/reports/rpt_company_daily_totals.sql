-- Report: Company Daily Totals
-- Company-wide daily aggregated sales

{{ config(materialized='view') }}

with fact_daily_summary as (
    select * from {{ ref('fct_daily_summary') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select 
    dd.full_date as order_date,
    dd.day_name,
    dd.is_weekend,
    
    -- Volume metrics
    sum(fds.total_orders) as total_orders,
    sum(fds.total_items_sold) as total_items_sold,
    sum(fds.unique_customers) as unique_customers,
    
    -- Revenue metrics
    sum(fds.gross_sales) as gross_sales,
    sum(fds.net_sales) as net_sales,
    sum(fds.total_revenue) as total_revenue,
    
    -- Calculated metrics
    round(sum(fds.total_revenue) / nullif(sum(fds.total_orders), 0), 2) as company_avg_order_value,
    count(distinct fds.location_sk) as locations_reporting
    
from fact_daily_summary fds
join dim_date dd on fds.date_key = dd.date_key
group by dd.full_date, dd.day_name, dd.is_weekend
order by dd.full_date desc
