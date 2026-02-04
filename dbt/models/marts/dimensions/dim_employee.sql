-- Employee dimension table
-- SCD Type 1 implementation (current state only)

{{ config(materialized='table') }}

with enriched_employees as (
    select * from {{ ref('int_employees') }}
),

unknown_employee as (
    -- Unknown employee record for handling null references
    select
        '{{ var("unknown_employee_id") }}' as employee_id,
        'Unknown' as first_name,
        'Employee' as last_name,
        'Unknown Employee' as full_name,
        'Unknown' as job_title,
        'Unknown' as department,
        0 as hourly_rate,
        'Unknown' as rate_band,
        null as location_id,
        false as is_manager,
        null as hire_date,
        0 as tenure_months,
        'Unknown' as employment_status,
        cast('1900-01-01' as date) as effective_date,
        true as is_current
),

employee_dimension as (
    select
        employee_id,
        first_name,
        last_name,
        full_name,
        job_title,
        department,
        hourly_rate,
        
        -- Rate band
        case
            when hourly_rate < 12 then 'Entry Level'
            when hourly_rate < 15 then 'Standard'
            when hourly_rate < 20 then 'Senior'
            else 'Management'
        end as rate_band,
        
        location_id,
        is_manager,
        hire_date,
        
        -- Tenure in months
        floor(tenure_days / 30.0) as tenure_months,
        
        employment_status,
        
        -- SCD Type 1 fields
        current_date() as effective_date,
        true as is_current
        
    from enriched_employees
),

final as (
    select * from employee_dimension
    union all
    select * from unknown_employee
)

select
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk,
    *,
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
from final
