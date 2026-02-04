-- Report: Location Peak Hours
-- Analyzes order patterns by hour for each location



with fct_sales as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_sales
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
),

dim_time as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_time
)

select 
    dl.location_name,
    dt.hour_24 as hour,
    dt.am_pm,
    dt.time_period,
    
    -- Metrics
    count(distinct fs.order_id) as order_count,
    sum(fs.total_amount) as total_revenue,
    round(avg(fs.total_amount), 2) as avg_order_value,
    
    -- Percentage of daily orders
    round(count(distinct fs.order_id) * 100.0 / 
          nullif(sum(count(distinct fs.order_id)) over (partition by dl.location_sk), 0), 2) as pct_of_orders
    
from fct_sales fs
join dim_location dl on fs.location_sk = dl.location_sk
join dim_time dt on fs.time_key = dt.time_key
group by dl.location_name, dl.location_sk, dt.hour_24, dt.am_pm, dt.time_period
order by dl.location_name, dt.hour_24