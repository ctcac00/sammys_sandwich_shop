-- Location dimension table
-- SCD Type 1 implementation (current state only)

{{ config(materialized='table') }}

with enriched_locations as (
    select * from {{ ref('int_locations') }}
),

unknown_location as (
    -- Unknown location record for handling null references
    select
        '{{ var("unknown_location_id") }}' as location_id,
        'Unknown Location' as location_name,
        null as address,
        'Unknown' as city,
        'XX' as state,
        null as zip_code,
        'Unknown' as region,
        null as open_date,
        0 as years_in_operation,
        0 as seating_capacity,
        'Unknown' as capacity_tier,
        false as has_drive_thru,
        null as manager_name,
        cast('1900-01-01' as date) as effective_date,
        true as is_current
),

location_dimension as (
    select
        location_id,
        location_name,
        address,
        city,
        state,
        zip_code,
        
        -- Region based on state (simplified)
        case
            when state in ('CA', 'OR', 'WA', 'NV', 'AZ') then 'West'
            when state in ('TX', 'OK', 'NM', 'CO', 'UT') then 'Southwest'
            when state in ('IL', 'OH', 'MI', 'IN', 'WI', 'MN') then 'Midwest'
            when state in ('NY', 'PA', 'NJ', 'MA', 'CT') then 'Northeast'
            when state in ('FL', 'GA', 'NC', 'SC', 'VA', 'TN') then 'Southeast'
            else 'Other'
        end as region,
        
        open_date,
        years_in_operation,
        seating_capacity,
        
        -- Capacity tier
        case
            when seating_capacity < 30 then 'Small'
            when seating_capacity < 60 then 'Medium'
            when seating_capacity < 100 then 'Large'
            else 'Extra Large'
        end as capacity_tier,
        
        has_drive_thru,
        manager_name,
        
        -- SCD Type 1 fields
        current_date() as effective_date,
        true as is_current
        
    from enriched_locations
),

final as (
    select * from location_dimension
    union all
    select * from unknown_location
)

select
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk,
    *,
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
from final
