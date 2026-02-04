-- Date dimension table
-- Generates a date spine with various date attributes



with date_spine as (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 4017
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by generated_number) - 1,
        cast('2020-01-01' as date)
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2030-12-31' as date)

)

select * from filtered


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