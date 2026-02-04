-- Report: Customer Cohort Analysis
-- Analyzes customer cohorts based on signup month

{{ config(materialized='view') }}

with fact_customer_activity as (
    select * from {{ ref('fct_customer_activity') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
    where is_current = true
)

select 
    date_trunc('month', dc.signup_date) as cohort_month,
    count(distinct dc.customer_sk) as cohort_size,
    sum(fca.total_orders) as total_orders,
    sum(fca.total_spend) as total_revenue,
    round(avg(fca.total_orders), 1) as avg_orders_per_customer,
    round(avg(fca.total_spend), 2) as avg_spend_per_customer,
    round(sum(fca.total_spend) / nullif(count(distinct dc.customer_sk), 0), 2) as cohort_ltv,
    round(avg(fca.customer_lifetime_days), 0) as avg_customer_lifetime_days
    
from fact_customer_activity fca
join dim_customer dc on fca.customer_sk = dc.customer_sk
where dc.signup_date is not null
group by date_trunc('month', dc.signup_date)
order by cohort_month desc
