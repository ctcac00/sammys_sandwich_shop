-- Report: Top Customers
-- Top customers by total spend

{{ config(materialized='view') }}

with fact_customer_activity as (
    select * from {{ ref('fct_customer_activity') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
    where is_current = true
)

select 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    dc.city,
    dc.state,
    fca.total_orders,
    fca.total_spend,
    fca.total_items_purchased,
    fca.avg_order_value,
    fca.days_since_last_order,
    fca.rfm_segment,
    to_date(fca.first_order_date_key::varchar, 'YYYYMMDD') as first_order_date,
    to_date(fca.last_order_date_key::varchar, 'YYYYMMDD') as last_order_date,
    row_number() over (order by fca.total_spend desc) as spend_rank
    
from fact_customer_activity fca
join dim_customer dc on fca.customer_sk = dc.customer_sk
order by fca.total_spend desc
limit 100
