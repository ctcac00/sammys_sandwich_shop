/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_MENU_ITEM_INGREDIENTS
================================================================================
  Transforms raw menu item ingredients (recipes) into enriched format with:
  - Proper data type casting
  - Derived fields (ingredient_cost calculated from quantity and cost_per_unit)
  - Boolean parsing for is_optional flag
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_menu_item_ingredients()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_menu_item_ingredients;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_menu_item_ingredients (
        item_id,
        ingredient_id,
        quantity_required,
        is_optional,
        extra_charge,
        ingredient_cost,
        _enriched_at
    )
    SELECT 
        mii.item_id,
        mii.ingredient_id,
        TRY_TO_NUMBER(mii.quantity_required, 10, 4) AS quantity_required,
        CASE UPPER(mii.is_optional)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_optional,
        TRY_TO_NUMBER(mii.extra_charge, 10, 2) AS extra_charge,
        -- Calculate ingredient cost = quantity * cost_per_unit
        ROUND(TRY_TO_NUMBER(mii.quantity_required, 10, 4) * ei.cost_per_unit, 4) AS ingredient_cost,
        CURRENT_TIMESTAMP() AS _enriched_at
    FROM SAMMYS_RAW.menu_item_ingredients mii
    LEFT JOIN SAMMYS_ENRICHED.enriched_ingredients ei 
        ON mii.ingredient_id = ei.ingredient_id
    WHERE mii.item_id IS NOT NULL 
      AND mii.ingredient_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' recipe records';
END;
$$;
