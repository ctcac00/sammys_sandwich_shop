/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_INGREDIENTS
================================================================================
  Transforms raw ingredient data into enriched format with:
  - Proper data type casting
  - Derived fields (cost_tier)
  - Boolean parsing for is_allergen flag
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_ingredients()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_ingredients;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_ingredients (
        ingredient_id,
        ingredient_name,
        category,
        unit_of_measure,
        cost_per_unit,
        supplier_id,
        is_allergen,
        allergen_type,
        shelf_life_days,
        storage_type,
        cost_tier,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        ingredient_id,
        TRIM(ingredient_name) AS ingredient_name,
        category,
        unit_of_measure,
        TRY_TO_NUMBER(cost_per_unit, 10, 2) AS cost_per_unit,
        supplier_id,
        CASE UPPER(is_allergen)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_allergen,
        NULLIF(allergen_type, 'NULL') AS allergen_type,
        TRY_TO_NUMBER(shelf_life_days) AS shelf_life_days,
        storage_type,
        -- Cost tier based on cost_per_unit
        CASE 
            WHEN TRY_TO_NUMBER(cost_per_unit, 10, 2) < 2.00 THEN 'Low Cost'
            WHEN TRY_TO_NUMBER(cost_per_unit, 10, 2) < 5.00 THEN 'Medium Cost'
            WHEN TRY_TO_NUMBER(cost_per_unit, 10, 2) < 10.00 THEN 'High Cost'
            ELSE 'Premium Cost'
        END AS cost_tier,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.ingredients
    WHERE ingredient_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' ingredient records';
END;
$$;
