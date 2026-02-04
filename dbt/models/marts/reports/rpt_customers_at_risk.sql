-- Report: Customers at Risk
-- Identifies customers who may be churning based on RFM analysis

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
    dc.phone,
    dc.loyalty_tier,
    dc.city,
    fca.total_orders,
    fca.total_spend,
    fca.avg_order_value,
    fca.days_since_last_order,
    fca.rfm_segment,
    fca.recency_score,
    fca.frequency_score,
    fca.monetary_score,
    to_date(fca.last_order_date_key::varchar, 'YYYYMMDD') as last_order_date
    
from fact_customer_activity fca
join dim_customer dc on fca.customer_sk = dc.customer_sk
where fca.rfm_segment in ('At Risk', 'Need Attention', 'Lost')
   or (fca.recency_score <= 2 and fca.monetary_score >= 3)
order by fca.total_spend desc
