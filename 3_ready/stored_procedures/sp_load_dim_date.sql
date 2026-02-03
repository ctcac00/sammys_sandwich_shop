/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_DATE
================================================================================
  Populates the date dimension with a range of dates.
  Includes fiscal calendar and holiday flags.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_date(
    p_start_date DATE DEFAULT '2020-01-01',
    p_end_date DATE DEFAULT '2026-12-31'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Clear existing data
    TRUNCATE TABLE dim_date;
    
    -- Generate date dimension using recursive CTE
    INSERT INTO dim_date (
        date_key,
        full_date,
        year,
        quarter,
        quarter_name,
        month,
        month_name,
        month_abbr,
        week_of_year,
        day_of_month,
        day_of_week,
        day_name,
        day_abbr,
        is_weekend,
        is_holiday,
        holiday_name,
        fiscal_year,
        fiscal_quarter,
        _created_at
    )
    WITH RECURSIVE date_spine AS (
        SELECT :p_start_date AS gen_date
        UNION ALL
        SELECT DATEADD('day', 1, gen_date)
        FROM date_spine
        WHERE gen_date < :p_end_date
    )
    SELECT 
        -- Date key in YYYYMMDD format
        TO_NUMBER(TO_CHAR(gen_date, 'YYYYMMDD')) AS date_key,
        gen_date AS full_date,
        YEAR(gen_date) AS year,
        QUARTER(gen_date) AS quarter,
        'Q' || QUARTER(gen_date) AS quarter_name,
        MONTH(gen_date) AS month,
        MONTHNAME(gen_date) AS month_name,
        LEFT(MONTHNAME(gen_date), 3) AS month_abbr,
        WEEKOFYEAR(gen_date) AS week_of_year,
        DAY(gen_date) AS day_of_month,
        DAYOFWEEK(gen_date) AS day_of_week,
        DAYNAME(gen_date) AS day_name,
        LEFT(DAYNAME(gen_date), 3) AS day_abbr,
        CASE WHEN DAYOFWEEK(gen_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        -- US Federal Holidays (simplified)
        CASE 
            WHEN MONTH(gen_date) = 1 AND DAY(gen_date) = 1 THEN TRUE
            WHEN MONTH(gen_date) = 7 AND DAY(gen_date) = 4 THEN TRUE
            WHEN MONTH(gen_date) = 12 AND DAY(gen_date) = 25 THEN TRUE
            WHEN MONTH(gen_date) = 11 AND DAYOFWEEK(gen_date) = 4 
                 AND DAY(gen_date) BETWEEN 22 AND 28 THEN TRUE -- Thanksgiving
            ELSE FALSE
        END AS is_holiday,
        CASE 
            WHEN MONTH(gen_date) = 1 AND DAY(gen_date) = 1 THEN 'New Years Day'
            WHEN MONTH(gen_date) = 7 AND DAY(gen_date) = 4 THEN 'Independence Day'
            WHEN MONTH(gen_date) = 12 AND DAY(gen_date) = 25 THEN 'Christmas Day'
            WHEN MONTH(gen_date) = 11 AND DAYOFWEEK(gen_date) = 4 
                 AND DAY(gen_date) BETWEEN 22 AND 28 THEN 'Thanksgiving'
            ELSE NULL
        END AS holiday_name,
        -- Fiscal year (assuming Feb 1 start for restaurant)
        CASE WHEN MONTH(gen_date) >= 2 THEN YEAR(gen_date) ELSE YEAR(gen_date) - 1 END AS fiscal_year,
        CASE 
            WHEN MONTH(gen_date) IN (2, 3, 4) THEN 1
            WHEN MONTH(gen_date) IN (5, 6, 7) THEN 2
            WHEN MONTH(gen_date) IN (8, 9, 10) THEN 3
            ELSE 4
        END AS fiscal_quarter,
        CURRENT_TIMESTAMP() AS _created_at
    FROM date_spine;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' date records';
END;
$$;
