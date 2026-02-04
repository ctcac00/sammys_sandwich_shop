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
