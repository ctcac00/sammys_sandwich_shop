-- Intermediate model for locations
-- Adds derived fields and business logic enrichments

with staged_locations as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_locations
),

employees as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_employees
),

enriched as (
    select
        -- Original fields
        l.location_id,
        l.location_name,
        l.address,
        l.city,
        l.state,
        l.zip_code,
        l.phone,
        l.manager_employee_id,
        l.open_date,
        l.seating_capacity,
        l.has_drive_thru,
        
        -- Derived fields
        round(datediff('day', l.open_date, current_date()) / 365.25, 2) as years_in_operation,
        
        -- Manager name from employees
        e.full_name as manager_name,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_locations l
    left join employees e on l.manager_employee_id = e.employee_id
)

select * from enriched