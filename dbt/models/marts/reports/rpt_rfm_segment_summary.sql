-- Report: RFM Segment Summary
-- RFM (Recency, Frequency, Monetary) segment analysis

{{ config(materialized='view') }}

with fact_customer_activity as (
    select * from {{ ref('fct_customer_activity') }}
)

select 
    rfm_segment,
    count(*) as customer_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as pct_of_customers,
    sum(total_spend) as segment_total_spend,
    round(sum(total_spend) * 100.0 / sum(sum(total_spend)) over (), 2) as pct_of_revenue,
    round(avg(total_spend), 2) as avg_customer_spend,
    round(avg(total_orders), 1) as avg_orders,
    round(avg(days_since_last_order), 0) as avg_days_since_order,
    round(avg(avg_order_value), 2) as avg_order_value
    
from fact_customer_activity
group by rfm_segment
order by segment_total_spend desc
