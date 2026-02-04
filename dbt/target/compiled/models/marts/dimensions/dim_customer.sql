-- Customer dimension table
-- SCD Type 1 implementation (current state only)



with enriched_customers as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_customers
),

unknown_customer as (
    -- Unknown/Guest customer record for handling null customer references
    select
        'UNKNOWN' as customer_id,
        'Guest' as first_name,
        'Customer' as last_name,
        'Guest Customer' as full_name,
        null as email,
        null as phone,
        null as city,
        null as state,
        null as zip_code,
        'Unknown' as age_group,
        'None' as loyalty_tier,
        0 as loyalty_tier_rank,
        null as signup_date,
        0 as tenure_months,
        'Unknown' as tenure_group,
        false as marketing_opt_in,
        null as preferred_location_id,
        cast('1900-01-01' as date) as effective_date,
        true as is_current
),

customer_dimension as (
    select
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        city,
        state,
        zip_code,
        
        -- Age grouping
        case 
            when age < 18 then 'Under 18'
            when age between 18 and 24 then '18-24'
            when age between 25 and 34 then '25-34'
            when age between 35 and 44 then '35-44'
            when age between 45 and 54 then '45-54'
            when age between 55 and 64 then '55-64'
            else '65+'
        end as age_group,
        
        loyalty_tier,
        loyalty_tier_rank,
        signup_date,
        
        -- Tenure in months
        floor(customer_tenure_days / 30.0) as tenure_months,
        
        -- Tenure grouping
        case 
            when customer_tenure_days < 90 then 'New (0-3 mo)'
            when customer_tenure_days < 365 then 'Growing (3-12 mo)'
            when customer_tenure_days < 730 then 'Established (1-2 yr)'
            else 'Loyal (2+ yr)'
        end as tenure_group,
        
        marketing_opt_in,
        preferred_location_id,
        
        -- SCD Type 1 fields (for future enhancement to SCD Type 2)
        current_date() as effective_date,
        true as is_current
        
    from enriched_customers
),

final as (
    select * from customer_dimension
    union all
    select * from unknown_customer
)

select
    md5(cast(coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_sk,
    *,
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
from final