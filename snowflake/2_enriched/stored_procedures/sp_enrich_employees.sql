/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_EMPLOYEES
================================================================================
  Transforms raw employee data into enriched format with:
  - Proper data type casting
  - Derived fields (full_name, tenure_days, is_manager)
  - Name formatting
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_employees()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_employees;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_employees (
        employee_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        hire_date,
        job_title,
        department,
        hourly_rate,
        location_id,
        manager_id,
        employment_status,
        tenure_days,
        is_manager,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        employee_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        INITCAP(TRIM(first_name)) || ' ' || INITCAP(TRIM(last_name)) AS full_name,
        LOWER(TRIM(email)) AS email,
        phone,
        TRY_TO_DATE(hire_date, 'YYYY-MM-DD') AS hire_date,
        job_title,
        department,
        TRY_TO_NUMBER(hourly_rate, 10, 2) AS hourly_rate,
        location_id,
        NULLIF(manager_id, 'NULL') AS manager_id,
        COALESCE(employment_status, 'Active') AS employment_status,
        DATEDIFF('day', TRY_TO_DATE(hire_date, 'YYYY-MM-DD'), CURRENT_DATE()) AS tenure_days,
        CASE 
            WHEN job_title LIKE '%Manager%' OR manager_id IS NULL OR manager_id = 'NULL'
            THEN TRUE 
            ELSE FALSE 
        END AS is_manager,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.employees
    WHERE employee_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' employee records';
END;
$$;
