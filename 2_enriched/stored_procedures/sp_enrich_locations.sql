/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_LOCATIONS
================================================================================
  Transforms raw location data into enriched format with:
  - Proper data type casting
  - Derived fields (years_in_operation)
  - Boolean parsing for drive_thru flag
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_locations()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_locations;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_locations (
        location_id,
        location_name,
        address,
        city,
        state,
        zip_code,
        phone,
        manager_employee_id,
        open_date,
        seating_capacity,
        has_drive_thru,
        years_in_operation,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        location_id,
        location_name,
        address,
        city,
        state,
        zip_code,
        phone,
        manager_employee_id,
        TRY_TO_DATE(open_date, 'YYYY-MM-DD') AS open_date,
        TRY_TO_NUMBER(seating_capacity) AS seating_capacity,
        CASE UPPER(has_drive_thru)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS has_drive_thru,
        ROUND(DATEDIFF('day', TRY_TO_DATE(open_date, 'YYYY-MM-DD'), CURRENT_DATE()) / 365.25, 2) AS years_in_operation,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.locations
    WHERE location_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' location records';
END;
$$;
