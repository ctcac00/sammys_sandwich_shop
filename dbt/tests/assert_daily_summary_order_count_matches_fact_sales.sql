-- Aggregate consistency: daily summary total orders should match fact_sales
-- This test fails if the aggregate counts differ by more than 5%
-- Matches Snowflake data quality check: "Fact daily summary order count should match fact sales"

with daily_summary_total as (
    select sum(total_orders) as total_orders
    from {{ ref('fct_daily_summary') }}
),

fact_sales_count as (
    select count(*) as order_count
    from {{ ref('fct_sales') }}
),

comparison as (
    select
        ds.total_orders as daily_summary_orders,
        fs.order_count as fact_sales_orders,
        abs(ds.total_orders - fs.order_count) as order_diff,
        case 
            when fs.order_count = 0 then 0
            else round(abs(ds.total_orders - fs.order_count) / fs.order_count * 100, 2)
        end as diff_pct
    from daily_summary_total ds
    cross join fact_sales_count fs
)

select *
from comparison
where diff_pct > 5
