/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_INVENTORY
================================================================================
  Transforms raw inventory data into enriched format with:
  - Proper data type casting
  - Calculated fields (available qty, days until expiration)
  - Stock status classification
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_inventory()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_inventory;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_inventory (
        inventory_id,
        location_id,
        ingredient_id,
        snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        last_restock_date,
        expiration_date,
        days_until_expiration,
        needs_reorder,
        stock_status,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        inventory_id,
        location_id,
        ingredient_id,
        TRY_TO_DATE(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
        TRY_TO_NUMBER(quantity_on_hand) AS quantity_on_hand,
        COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) AS quantity_reserved,
        -- Available = on_hand - reserved
        TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) AS quantity_available,
        TRY_TO_NUMBER(reorder_point) AS reorder_point,
        TRY_TO_NUMBER(reorder_quantity) AS reorder_quantity,
        TRY_TO_DATE(last_restock_date, 'YYYY-MM-DD') AS last_restock_date,
        TRY_TO_DATE(expiration_date, 'YYYY-MM-DD') AS expiration_date,
        -- Days until expiration
        DATEDIFF('day', CURRENT_DATE(), TRY_TO_DATE(expiration_date, 'YYYY-MM-DD')) AS days_until_expiration,
        -- Needs reorder flag
        CASE 
            WHEN TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) 
                 <= TRY_TO_NUMBER(reorder_point) 
            THEN TRUE 
            ELSE FALSE 
        END AS needs_reorder,
        -- Stock status classification
        CASE 
            WHEN TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) <= 0 
                THEN 'Out of Stock'
            WHEN TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) 
                 <= TRY_TO_NUMBER(reorder_point) * 0.5 
                THEN 'Critical Low'
            WHEN TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) 
                 <= TRY_TO_NUMBER(reorder_point) 
                THEN 'Low Stock'
            WHEN TRY_TO_NUMBER(quantity_on_hand) - COALESCE(TRY_TO_NUMBER(quantity_reserved), 0) 
                 <= TRY_TO_NUMBER(reorder_point) * 2 
                THEN 'Adequate'
            ELSE 'Well Stocked'
        END AS stock_status,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.inventory
    WHERE inventory_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' inventory records';
END;
$$;
