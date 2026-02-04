-- Order type dimension table (static/seed)

{{ config(materialized='table') }}

with order_types as (
    select 
        'Dine-In' as order_type,
        true as is_in_store,
        5 as avg_service_minutes
    union all
    select 'Takeout', true, 3
    union all
    select 'Drive-Thru', false, 4
    union all
    select 'Delivery', false, 30
    union all
    select 'Catering', false, 60
)

select
    {{ dbt_utils.generate_surrogate_key(['order_type']) }} as order_type_sk,
    order_type,
    is_in_store,
    avg_service_minutes,
    current_timestamp() as _created_at
from order_types
