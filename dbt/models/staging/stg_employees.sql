-- Staging model for employees
-- Performs data type casting and basic cleaning

with source as (
    select * from {{ source('raw', 'employees') }}
),

staged as (
    select
        -- Primary key
        employee_id,
        
        -- Name fields
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        
        -- Contact info
        lower(trim(email)) as email,
        trim(phone) as phone,
        
        -- Employment info
        try_to_date(hire_date, 'YYYY-MM-DD') as hire_date,
        trim(job_title) as job_title,
        trim(department) as department,
        try_to_number(hourly_rate, 10, 2) as hourly_rate,
        trim(location_id) as location_id,
        trim(manager_id) as manager_id,
        trim(employment_status) as employment_status,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where employee_id is not null
)

select * from staged
