-- Report: Loyalty Tier Analysis
-- Customer loyalty tier performance analysis with spend distribution



with fact_customer_activity as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_customer_activity
),

dim_customer as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_customer
    where is_current = true
)

select 
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    count(distinct dc.customer_sk) as customer_count,
    sum(fca.total_orders) as total_orders,
    sum(fca.total_spend) as total_spend,
    round(avg(fca.total_spend), 2) as avg_customer_spend,
    round(avg(fca.total_orders), 1) as avg_orders_per_customer,
    round(avg(fca.avg_order_value), 2) as avg_order_value,
    round(avg(fca.days_since_last_order), 0) as avg_days_since_order,
    
    -- Spend distribution
    min(fca.total_spend) as min_spend,
    percentile_cont(0.25) within group (order by fca.total_spend) as spend_25th_pct,
    percentile_cont(0.50) within group (order by fca.total_spend) as spend_median,
    percentile_cont(0.75) within group (order by fca.total_spend) as spend_75th_pct,
    max(fca.total_spend) as max_spend
    
from fact_customer_activity fca
join dim_customer dc on fca.customer_sk = dc.customer_sk
group by dc.loyalty_tier, dc.loyalty_tier_rank
order by dc.loyalty_tier_rank desc