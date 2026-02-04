-- Date dimension table
-- Generates a date spine with various date attributes

{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

dates as (
    select
        cast(date_day as date) as full_date
    from date_spine
),

enriched_dates as (
    select
        -- Surrogate key (YYYYMMDD format)
        to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
        
        full_date,
        
        -- Year attributes
        year(full_date) as year,
        quarter(full_date) as quarter,
        'Q' || quarter(full_date) as quarter_name,
        
        -- Month attributes
        month(full_date) as month,
        monthname(full_date) as month_name,
        left(monthname(full_date), 3) as month_abbr,
        
        -- Week attributes
        weekofyear(full_date) as week_of_year,
        
        -- Day attributes
        dayofmonth(full_date) as day_of_month,
        dayofweek(full_date) as day_of_week,
        dayname(full_date) as day_name,
        left(dayname(full_date), 3) as day_abbr,
        
        -- Flags
        dayofweek(full_date) in (0, 6) as is_weekend,
        false as is_holiday,  -- Can be enhanced with actual holiday data
        null as holiday_name,
        
        -- Fiscal year (assuming calendar year = fiscal year)
        year(full_date) as fiscal_year,
        quarter(full_date) as fiscal_quarter,
        
        -- Metadata
        current_timestamp() as _created_at
        
    from dates
)

select * from enriched_dates
