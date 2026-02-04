-- Fact table for customer activity (accumulating snapshot)
-- Grain: one row per customer

{{ config(materialized='table') }}

with fct_sales as (
    select * from {{ ref('fct_sales') }}
),

fct_sales_line_item as (
    select * from {{ ref('fct_sales_line_item') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
    where is_current = true
      and customer_id != '{{ var("unknown_customer_id") }}'
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Customer order aggregations
customer_orders as (
    select
        fs.customer_sk,
        min(fs.date_key) as first_order_date_key,
        max(fs.date_key) as last_order_date_key,
        count(distinct fs.order_id) as total_orders,
        sum(fs.total_amount) as total_spend,
        sum(fs.discount_amount) as total_discounts,
        sum(fs.tip_amount) as total_tips,
        round(avg(fs.total_amount), 2) as avg_order_value
    from fct_sales fs
    group by fs.customer_sk
),

-- Total items purchased
customer_items as (
    select
        customer_sk,
        sum(quantity) as total_items_purchased
    from fct_sales_line_item
    group by customer_sk
),

-- Favorite location (most orders)
favorite_location as (
    select
        customer_sk,
        location_sk,
        row_number() over (partition by customer_sk order by count(*) desc) as rn
    from fct_sales
    group by customer_sk, location_sk
),

-- Favorite menu item (most purchased)
favorite_item as (
    select
        customer_sk,
        menu_item_sk,
        row_number() over (partition by customer_sk order by sum(quantity) desc) as rn
    from fct_sales_line_item
    group by customer_sk, menu_item_sk
),

-- Favorite order type
favorite_order_type as (
    select
        fs.customer_sk,
        ot.order_type,
        row_number() over (partition by fs.customer_sk order by count(*) desc) as rn
    from fct_sales fs
    join {{ ref('dim_order_type') }} ot on fs.order_type_sk = ot.order_type_sk
    group by fs.customer_sk, ot.order_type
),

-- Calculate days between orders
order_gaps as (
    select
        customer_sk,
        round(avg(days_to_next), 2) as avg_days_between_orders
    from (
        select
            customer_sk,
            date_key,
            datediff('day', 
                to_date(date_key::varchar, 'YYYYMMDD'),
                lead(to_date(date_key::varchar, 'YYYYMMDD')) over (partition by customer_sk order by date_key)
            ) as days_to_next
        from fct_sales
    )
    where days_to_next is not null
    group by customer_sk
),

-- RFM scoring
rfm_scores as (
    select
        co.customer_sk,
        datediff('day', to_date(co.last_order_date_key::varchar, 'YYYYMMDD'), current_date()) as days_since_last_order,
        datediff('day', to_date(co.first_order_date_key::varchar, 'YYYYMMDD'), current_date()) as customer_lifetime_days,
        
        -- Recency score (1-5, 5 is best = most recent)
        ntile(5) over (order by co.last_order_date_key) as recency_score,
        
        -- Frequency score (1-5, 5 is best = most orders)
        ntile(5) over (order by co.total_orders) as frequency_score,
        
        -- Monetary score (1-5, 5 is best = highest spend)
        ntile(5) over (order by co.total_spend) as monetary_score
        
    from customer_orders co
),

-- RFM segment assignment
rfm_segments as (
    select
        customer_sk,
        days_since_last_order,
        customer_lifetime_days,
        recency_score,
        frequency_score,
        monetary_score,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 4 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_score >= 3 then 'Potential Loyalists'
            when recency_score <= 2 and frequency_score >= 4 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2 then 'Lost'
            when recency_score <= 2 and monetary_score >= 3 then 'Need Attention'
            else 'Other'
        end as rfm_segment
    from rfm_scores
)

select
    dc.customer_sk,
    dc.customer_id,
    
    -- Activity metrics (lifetime)
    co.first_order_date_key,
    co.last_order_date_key,
    co.total_orders,
    coalesce(ci.total_items_purchased, 0) as total_items_purchased,
    co.total_spend,
    co.total_discounts,
    co.total_tips,
    co.avg_order_value,
    coalesce(og.avg_days_between_orders, 0) as avg_days_between_orders,
    rs.customer_lifetime_days,
    
    -- Favorites
    fl.location_sk as favorite_location_sk,
    fi.menu_item_sk as favorite_menu_item_sk,
    fot.order_type as favorite_order_type,
    
    -- Recency metrics
    rs.days_since_last_order,
    
    -- RFM scores
    rs.recency_score,
    rs.frequency_score,
    rs.monetary_score,
    rs.rfm_segment,
    
    -- Metadata
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
    
from dim_customer dc
join customer_orders co on dc.customer_sk = co.customer_sk
left join customer_items ci on dc.customer_sk = ci.customer_sk
left join favorite_location fl on dc.customer_sk = fl.customer_sk and fl.rn = 1
left join favorite_item fi on dc.customer_sk = fi.customer_sk and fi.rn = 1
left join favorite_order_type fot on dc.customer_sk = fot.customer_sk and fot.rn = 1
left join order_gaps og on dc.customer_sk = og.customer_sk
join rfm_segments rs on dc.customer_sk = rs.customer_sk
