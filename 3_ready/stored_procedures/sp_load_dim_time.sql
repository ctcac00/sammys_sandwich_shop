/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_TIME
================================================================================
  Populates the time dimension with each minute of the day.
  Includes time periods and peak hour classification.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_time()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE dim_time;
    
    -- Generate time dimension for each minute of the day
    INSERT INTO dim_time (
        time_key,
        full_time,
        hour_24,
        hour_12,
        minute,
        am_pm,
        time_period,
        is_peak_hour,
        _created_at
    )
    WITH RECURSIVE time_spine AS (
        SELECT 0 AS minutes_since_midnight
        UNION ALL
        SELECT minutes_since_midnight + 1
        FROM time_spine
        WHERE minutes_since_midnight < 1439  -- 24*60 - 1
    )
    SELECT 
        -- Time key in HHMM format
        (minutes_since_midnight / 60) * 100 + (minutes_since_midnight % 60) AS time_key,
        TIME_FROM_PARTS(minutes_since_midnight / 60, minutes_since_midnight % 60, 0) AS full_time,
        minutes_since_midnight / 60 AS hour_24,
        CASE 
            WHEN minutes_since_midnight / 60 = 0 THEN 12
            WHEN minutes_since_midnight / 60 > 12 THEN minutes_since_midnight / 60 - 12
            ELSE minutes_since_midnight / 60
        END AS hour_12,
        minutes_since_midnight % 60 AS minute,
        CASE WHEN minutes_since_midnight / 60 < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
        CASE 
            WHEN minutes_since_midnight / 60 BETWEEN 6 AND 10 THEN 'Breakfast'
            WHEN minutes_since_midnight / 60 BETWEEN 11 AND 14 THEN 'Lunch'
            WHEN minutes_since_midnight / 60 BETWEEN 15 AND 17 THEN 'Afternoon'
            WHEN minutes_since_midnight / 60 BETWEEN 18 AND 21 THEN 'Dinner'
            ELSE 'Closed'
        END AS time_period,
        -- Peak hours for sandwich shop: 11-13 (lunch rush)
        CASE 
            WHEN minutes_since_midnight / 60 BETWEEN 11 AND 13 THEN TRUE 
            ELSE FALSE 
        END AS is_peak_hour,
        CURRENT_TIMESTAMP() AS _created_at
    FROM time_spine;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' time records';
END;
$$;
