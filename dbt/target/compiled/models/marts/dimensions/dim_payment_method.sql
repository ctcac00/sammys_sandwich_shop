-- Payment method dimension table (static/seed)



with payment_methods as (
    select 
        'Credit Card' as payment_method,
        'Card' as payment_type,
        true as is_digital,
        0.0295 as processing_fee_pct
    union all
    select 'Debit Card', 'Card', true, 0.0150
    union all
    select 'Cash', 'Cash', false, 0.0000
    union all
    select 'Mobile Pay', 'Digital', true, 0.0250
    union all
    select 'Gift Card', 'Prepaid', false, 0.0000
)

select
    md5(cast(coalesce(cast(payment_method as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as payment_method_sk,
    payment_method,
    payment_type,
    is_digital,
    processing_fee_pct,
    current_timestamp() as _created_at
from payment_methods