-- Report: Drive-Thru Analysis
-- Analyzes performance of drive-thru locations



with fact_daily_summary as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_daily_summary
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
)

select 
    dl.location_name,
    dl.region,
    dl.has_drive_thru,
    
    -- Total metrics
    sum(fds.total_orders) as total_orders,
    sum(fds.drive_thru_orders) as drive_thru_orders,
    sum(fds.total_revenue) as total_revenue,
    
    -- Drive-thru percentage
    round(sum(fds.drive_thru_orders) * 100.0 / nullif(sum(fds.total_orders), 0), 2) as drive_thru_pct,
    
    -- Compare drive-thru vs non-drive-thru locations
    round(avg(fds.avg_order_value), 2) as avg_order_value,
    round(sum(fds.total_revenue) / nullif(count(distinct fds.date_key), 0), 2) as avg_daily_revenue
    
from fact_daily_summary fds
join dim_location dl on fds.location_sk = dl.location_sk
group by dl.location_name, dl.region, dl.has_drive_thru
order by drive_thru_orders desc