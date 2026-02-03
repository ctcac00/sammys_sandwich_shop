/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_CUSTOMERS
================================================================================
  Transforms raw customer data into enriched format with:
  - Proper data type casting
  - Derived fields (full_name, age, loyalty_tier_rank, tenure_days)
  - Null handling
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_customers()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
    v_start_time TIMESTAMP_NTZ;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();
    
    -- Truncate and reload (full refresh)
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_customers;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_customers (
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        birth_date,
        age,
        signup_date,
        loyalty_tier,
        loyalty_tier_rank,
        loyalty_points,
        preferred_location_id,
        marketing_opt_in,
        customer_tenure_days,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        customer_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        INITCAP(TRIM(first_name)) || ' ' || INITCAP(TRIM(last_name)) AS full_name,
        LOWER(TRIM(email)) AS email,
        phone,
        address,
        city,
        state,
        zip_code,
        TRY_TO_DATE(birth_date, 'YYYY-MM-DD') AS birth_date,
        DATEDIFF('year', TRY_TO_DATE(birth_date, 'YYYY-MM-DD'), CURRENT_DATE()) AS age,
        TRY_TO_DATE(signup_date, 'YYYY-MM-DD') AS signup_date,
        COALESCE(loyalty_tier, 'Bronze') AS loyalty_tier,
        CASE UPPER(loyalty_tier)
            WHEN 'PLATINUM' THEN 4
            WHEN 'GOLD' THEN 3
            WHEN 'SILVER' THEN 2
            WHEN 'BRONZE' THEN 1
            ELSE 0
        END AS loyalty_tier_rank,
        COALESCE(TRY_TO_NUMBER(loyalty_points), 0) AS loyalty_points,
        preferred_location AS preferred_location_id,
        CASE UPPER(marketing_opt_in)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS marketing_opt_in,
        DATEDIFF('day', TRY_TO_DATE(signup_date, 'YYYY-MM-DD'), CURRENT_DATE()) AS customer_tenure_days,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.customers
    WHERE customer_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' customer records in ' || 
           DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || ' seconds';
END;
$$;

-- Also enrich locations
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

-- Enrich employees
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

-- Enrich suppliers
CREATE OR REPLACE PROCEDURE sp_enrich_suppliers()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_suppliers;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_suppliers (
        supplier_id,
        supplier_name,
        contact_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        payment_terms,
        payment_days,
        lead_time_days,
        is_active,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        supplier_id,
        supplier_name,
        contact_name,
        LOWER(TRIM(email)) AS email,
        phone,
        address,
        city,
        state,
        zip_code,
        payment_terms,
        -- Extract days from payment terms (e.g., "Net 30" -> 30)
        COALESCE(TRY_TO_NUMBER(REGEXP_SUBSTR(payment_terms, '\\d+')), 30) AS payment_days,
        TRY_TO_NUMBER(lead_time_days) AS lead_time_days,
        CASE UPPER(is_active)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_active,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.suppliers
    WHERE supplier_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' supplier records';
END;
$$;
