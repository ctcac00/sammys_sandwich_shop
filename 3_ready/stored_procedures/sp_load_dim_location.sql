/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_LOCATION
================================================================================
  Loads the location dimension from enriched layer.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_location()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    MERGE INTO dim_location tgt
    USING (
        SELECT 
            l.location_id,
            l.location_name,
            l.address,
            l.city,
            l.state,
            l.zip_code,
            -- Region based on zip code (simplified)
            CASE 
                WHEN l.zip_code LIKE '787%' THEN 'Central Austin'
                WHEN l.zip_code LIKE '786%' THEN 'West Austin'
                ELSE 'Greater Austin'
            END AS region,
            l.open_date,
            l.years_in_operation,
            l.seating_capacity,
            -- Capacity tier
            CASE 
                WHEN l.seating_capacity < 40 THEN 'Small'
                WHEN l.seating_capacity < 50 THEN 'Medium'
                ELSE 'Large'
            END AS capacity_tier,
            l.has_drive_thru,
            e.full_name AS manager_name
        FROM SAMMYS_ENRICHED.enriched_locations l
        LEFT JOIN SAMMYS_ENRICHED.enriched_employees e 
            ON l.manager_employee_id = e.employee_id
    ) src
    ON tgt.location_id = src.location_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        location_name = src.location_name,
        address = src.address,
        city = src.city,
        state = src.state,
        zip_code = src.zip_code,
        region = src.region,
        years_in_operation = src.years_in_operation,
        seating_capacity = src.seating_capacity,
        capacity_tier = src.capacity_tier,
        has_drive_thru = src.has_drive_thru,
        manager_name = src.manager_name,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        location_id, location_name, address, city, state, zip_code, region,
        open_date, years_in_operation, seating_capacity, capacity_tier,
        has_drive_thru, manager_name, effective_date, is_current
    ) VALUES (
        src.location_id, src.location_name, src.address, src.city, src.state,
        src.zip_code, src.region, src.open_date, src.years_in_operation,
        src.seating_capacity, src.capacity_tier, src.has_drive_thru,
        src.manager_name, CURRENT_DATE(), TRUE
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM dim_location;
    
    RETURN 'SUCCESS: dim_location now has ' || v_rows_inserted || ' records';
END;
$$;
