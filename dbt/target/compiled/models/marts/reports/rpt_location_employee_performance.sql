-- Report: Location Employee Performance
-- Analyzes employee performance by location



with fct_sales as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_sales
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
),

dim_employee as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_employee
    where is_current = true
)

select 
    dl.location_name,
    de.full_name as employee_name,
    de.job_title,
    de.is_manager,
    
    -- Metrics
    count(distinct fs.order_id) as orders_processed,
    sum(fs.total_amount) as total_sales,
    round(avg(fs.total_amount), 2) as avg_order_value,
    sum(fs.item_count) as items_sold,
    
    -- Performance vs location average
    round(avg(fs.total_amount) - 
          avg(avg(fs.total_amount)) over (partition by dl.location_sk), 2) as avg_ticket_vs_location,
          
    -- Tip performance
    sum(fs.tip_amount) as total_tips,
    round(sum(fs.tip_amount) / nullif(sum(fs.subtotal), 0) * 100, 2) as tip_rate
    
from fct_sales fs
join dim_location dl on fs.location_sk = dl.location_sk
join dim_employee de on fs.employee_sk = de.employee_sk
where de.employee_id != 'UNKNOWN'
group by dl.location_name, dl.location_sk, de.full_name, de.job_title, de.is_manager
order by dl.location_name, total_sales desc