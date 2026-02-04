-- Intermediate model for employees
-- Adds derived fields and business logic enrichments

with staged_employees as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_employees
),

enriched as (
    select
        -- Original fields
        employee_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        phone,
        hire_date,
        job_title,
        department,
        hourly_rate,
        location_id,
        manager_id,
        employment_status,
        
        -- Derived fields
        datediff('day', hire_date, current_date()) as tenure_days,
        
        -- Is this employee a manager?
        job_title ilike '%manager%' 
            or job_title ilike '%director%' 
            or job_title ilike '%supervisor%' as is_manager,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_employees
)

select * from enriched