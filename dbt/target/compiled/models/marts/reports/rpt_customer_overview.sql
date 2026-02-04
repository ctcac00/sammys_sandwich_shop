-- Report: Customer Overview
-- Customer overview with lifetime metrics, RFM scores, and favorites



with fact_customer_activity as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_customer_activity
),

dim_customer as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_customer
    where is_current = true
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
),

dim_menu_item as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_menu_item
    where is_current = true
)

select 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    dc.age_group,
    dc.tenure_group,
    dc.marketing_opt_in,
    
    -- Activity metrics
    fca.total_orders,
    fca.total_items_purchased,
    fca.total_spend,
    fca.total_tips,
    fca.avg_order_value,
    fca.avg_days_between_orders,
    fca.customer_lifetime_days,
    fca.days_since_last_order,
    
    -- RFM
    fca.recency_score,
    fca.frequency_score,
    fca.monetary_score,
    fca.rfm_segment,
    
    -- Favorites
    dl.location_name as favorite_location,
    dm.item_name as favorite_item,
    fca.favorite_order_type,
    
    -- First/Last order dates
    to_date(fca.first_order_date_key::varchar, 'YYYYMMDD') as first_order_date,
    to_date(fca.last_order_date_key::varchar, 'YYYYMMDD') as last_order_date
    
from fact_customer_activity fca
join dim_customer dc on fca.customer_sk = dc.customer_sk
left join dim_location dl on fca.favorite_location_sk = dl.location_sk
left join dim_menu_item dm on fca.favorite_menu_item_sk = dm.menu_item_sk
order by fca.total_spend desc