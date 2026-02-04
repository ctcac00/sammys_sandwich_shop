-- Intermediate model for customers
-- Adds derived fields and business logic enrichments

with staged_customers as (
    select * from {{ ref('stg_customers') }}
),

enriched as (
    select
        -- Original fields
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        birth_date,
        signup_date,
        loyalty_tier,
        loyalty_points,
        preferred_location_id,
        marketing_opt_in,
        
        -- Derived fields
        datediff('year', birth_date, current_date()) as age,
        
        -- Loyalty tier rank for sorting
        case upper(loyalty_tier)
            when 'PLATINUM' then 4
            when 'GOLD' then 3
            when 'SILVER' then 2
            when 'BRONZE' then 1
            else 0
        end as loyalty_tier_rank,
        
        -- Customer tenure
        datediff('day', signup_date, current_date()) as customer_tenure_days,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_customers
)

select * from enriched
