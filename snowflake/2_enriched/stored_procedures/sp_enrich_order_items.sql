/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_ORDER_ITEMS
================================================================================
  Transforms raw order item data into enriched format with:
  - Proper data type casting
  - Derived fields (has_customization flag)
  - Line total calculation
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_order_items()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_order_items;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_order_items (
        order_item_id,
        order_id,
        item_id,
        quantity,
        unit_price,
        customizations,
        line_total,
        has_customization,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        order_item_id,
        order_id,
        item_id,
        TRY_TO_NUMBER(quantity) AS quantity,
        TRY_TO_NUMBER(unit_price, 10, 2) AS unit_price,
        NULLIF(TRIM(customizations), '') AS customizations,
        TRY_TO_NUMBER(line_total, 10, 2) AS line_total,
        CASE 
            WHEN customizations IS NOT NULL AND TRIM(customizations) != '' THEN TRUE 
            ELSE FALSE 
        END AS has_customization,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.order_items
    WHERE order_item_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' order item records';
END;
$$;
