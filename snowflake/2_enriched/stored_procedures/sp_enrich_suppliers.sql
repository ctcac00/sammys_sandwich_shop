/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_SUPPLIERS
================================================================================
  Transforms raw supplier data into enriched format with:
  - Proper data type casting
  - Derived fields (payment_days from payment_terms)
  - Boolean parsing for is_active flag
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

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
