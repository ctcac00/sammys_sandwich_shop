/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_MENU_ITEM
================================================================================
  Loads the menu item dimension from enriched layer.
  Calculates food cost and margin from recipe data.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_menu_item()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    MERGE INTO dim_menu_item tgt
    USING (
        SELECT 
            m.item_id,
            m.item_name,
            m.category,
            m.subcategory,
            m.description,
            m.base_price,
            m.price_tier,
            m.calories,
            m.calorie_category,
            m.prep_time_minutes,
            m.is_active,
            m.is_seasonal,
            m.introduced_date,
            -- Food cost from recipe
            COALESCE(r.food_cost, 0) AS food_cost,
            -- Food cost percentage
            ROUND(COALESCE(r.food_cost, 0) / NULLIF(m.base_price, 0) * 100, 2) AS food_cost_pct,
            -- Gross margin
            m.base_price - COALESCE(r.food_cost, 0) AS gross_margin,
            -- Gross margin percentage
            ROUND((m.base_price - COALESCE(r.food_cost, 0)) / NULLIF(m.base_price, 0) * 100, 2) AS gross_margin_pct
        FROM SAMMYS_ENRICHED.enriched_menu_items m
        LEFT JOIN (
            -- Calculate total food cost per menu item from recipe
            SELECT 
                item_id,
                SUM(ingredient_cost) AS food_cost
            FROM SAMMYS_ENRICHED.enriched_menu_item_ingredients
            WHERE is_optional = FALSE
            GROUP BY item_id
        ) r ON m.item_id = r.item_id
    ) src
    ON tgt.item_id = src.item_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        item_name = src.item_name,
        category = src.category,
        subcategory = src.subcategory,
        description = src.description,
        base_price = src.base_price,
        price_tier = src.price_tier,
        calories = src.calories,
        calorie_category = src.calorie_category,
        prep_time_minutes = src.prep_time_minutes,
        is_active = src.is_active,
        is_seasonal = src.is_seasonal,
        food_cost = src.food_cost,
        food_cost_pct = src.food_cost_pct,
        gross_margin = src.gross_margin,
        gross_margin_pct = src.gross_margin_pct,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        item_id, item_name, category, subcategory, description, base_price,
        price_tier, calories, calorie_category, prep_time_minutes, is_active,
        is_seasonal, introduced_date, food_cost, food_cost_pct, gross_margin,
        gross_margin_pct, effective_date, is_current
    ) VALUES (
        src.item_id, src.item_name, src.category, src.subcategory, src.description,
        src.base_price, src.price_tier, src.calories, src.calorie_category,
        src.prep_time_minutes, src.is_active, src.is_seasonal, src.introduced_date,
        src.food_cost, src.food_cost_pct, src.gross_margin, src.gross_margin_pct,
        CURRENT_DATE(), TRUE
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM dim_menu_item;
    
    RETURN 'SUCCESS: dim_menu_item now has ' || v_rows_inserted || ' records';
END;
$$;

-- Load ingredient dimension
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
