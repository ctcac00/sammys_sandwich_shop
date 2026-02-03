/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_INGREDIENT
================================================================================
  Loads the ingredient dimension from enriched layer.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_ingredient()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    MERGE INTO dim_ingredient tgt
    USING (
        SELECT 
            i.ingredient_id,
            i.ingredient_name,
            i.category,
            i.unit_of_measure,
            i.cost_per_unit,
            i.cost_tier,
            i.supplier_id,
            s.supplier_name,
            i.is_allergen,
            i.allergen_type,
            i.shelf_life_days,
            i.storage_type
        FROM SAMMYS_ENRICHED.enriched_ingredients i
        LEFT JOIN SAMMYS_ENRICHED.enriched_suppliers s 
            ON i.supplier_id = s.supplier_id
    ) src
    ON tgt.ingredient_id = src.ingredient_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        ingredient_name = src.ingredient_name,
        category = src.category,
        unit_of_measure = src.unit_of_measure,
        cost_per_unit = src.cost_per_unit,
        cost_tier = src.cost_tier,
        supplier_id = src.supplier_id,
        supplier_name = src.supplier_name,
        is_allergen = src.is_allergen,
        allergen_type = src.allergen_type,
        shelf_life_days = src.shelf_life_days,
        storage_type = src.storage_type,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        ingredient_id, ingredient_name, category, unit_of_measure, cost_per_unit,
        cost_tier, supplier_id, supplier_name, is_allergen, allergen_type,
        shelf_life_days, storage_type, effective_date, is_current
    ) VALUES (
        src.ingredient_id, src.ingredient_name, src.category, src.unit_of_measure,
        src.cost_per_unit, src.cost_tier, src.supplier_id, src.supplier_name,
        src.is_allergen, src.allergen_type, src.shelf_life_days, src.storage_type,
        CURRENT_DATE(), TRUE
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM dim_ingredient;
    
    RETURN 'SUCCESS: dim_ingredient now has ' || v_rows_inserted || ' records';
END;
$$;
