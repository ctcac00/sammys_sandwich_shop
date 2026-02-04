-- Report: Location Performance
-- Compares performance metrics across store locations



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
    dl.city,
    dl.years_in_operation,
    dl.seating_capacity,
    dl.capacity_tier,
    dl.has_drive_thru,
    dl.manager_name,
    
    -- Sales metrics
    sum(fds.total_orders) as total_orders,
    sum(fds.total_items_sold) as total_items_sold,
    sum(fds.gross_sales) as gross_sales,
    sum(fds.net_sales) as net_sales,
    sum(fds.total_revenue) as total_revenue,
    sum(fds.tips_received) as tips_received,
    
    -- Performance metrics
    round(avg(fds.avg_order_value), 2) as avg_order_value,
    round(avg(fds.avg_items_per_order), 2) as avg_items_per_order,
    round(avg(fds.discount_rate), 2) as avg_discount_rate,
    
    -- Customer metrics
    sum(fds.unique_customers) as total_customer_visits,
    sum(fds.guest_orders) as guest_orders,
    round(sum(fds.guest_orders) * 100.0 / nullif(sum(fds.total_orders), 0), 2) as guest_order_pct,
    
    -- Order type breakdown
    sum(fds.dine_in_orders) as dine_in_orders,
    sum(fds.takeout_orders) as takeout_orders,
    sum(fds.drive_thru_orders) as drive_thru_orders,
    
    -- Period breakdown
    sum(fds.breakfast_orders) as breakfast_orders,
    sum(fds.lunch_orders) as lunch_orders,
    sum(fds.dinner_orders) as dinner_orders,
    
    -- Days of data
    count(distinct fds.date_key) as days_of_data,
    
    -- Daily averages
    round(sum(fds.total_revenue) / nullif(count(distinct fds.date_key), 0), 2) as avg_daily_revenue,
    round(sum(fds.total_orders) / nullif(count(distinct fds.date_key), 0), 1) as avg_daily_orders
    
from fact_daily_summary fds
join dim_location dl on fds.location_sk = dl.location_sk
group by 
    dl.location_name, dl.region, dl.city, dl.years_in_operation,
    dl.seating_capacity, dl.capacity_tier, dl.has_drive_thru, dl.manager_name
order by total_revenue desc