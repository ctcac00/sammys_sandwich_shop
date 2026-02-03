/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_MENU
================================================================================
  Transforms raw menu items and ingredients into enriched format with:
  - Proper data type casting
  - Derived fields (price_tier, calorie_category, days_on_menu)
  - Recipe cost calculations
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_menu_items()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_menu_items;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_menu_items (
        item_id,
        item_name,
        category,
        subcategory,
        description,
        base_price,
        is_active,
        is_seasonal,
        calories,
        prep_time_minutes,
        introduced_date,
        days_on_menu,
        price_tier,
        calorie_category,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        item_id,
        TRIM(item_name) AS item_name,
        category,
        subcategory,
        description,
        TRY_TO_NUMBER(base_price, 10, 2) AS base_price,
        CASE UPPER(is_active)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_active,
        CASE UPPER(is_seasonal)
            WHEN 'TRUE' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN '1' THEN TRUE
            ELSE FALSE
        END AS is_seasonal,
        TRY_TO_NUMBER(calories) AS calories,
        TRY_TO_NUMBER(prep_time_minutes) AS prep_time_minutes,
        TRY_TO_DATE(introduced_date, 'YYYY-MM-DD') AS introduced_date,
        DATEDIFF('day', TRY_TO_DATE(introduced_date, 'YYYY-MM-DD'), CURRENT_DATE()) AS days_on_menu,
        -- Price tier based on base_price
        CASE 
            WHEN TRY_TO_NUMBER(base_price, 10, 2) < 5.00 THEN 'Budget'
            WHEN TRY_TO_NUMBER(base_price, 10, 2) < 10.00 THEN 'Standard'
            WHEN TRY_TO_NUMBER(base_price, 10, 2) < 13.00 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier,
        -- Calorie category
        CASE 
            WHEN TRY_TO_NUMBER(calories) < 300 THEN 'Light'
            WHEN TRY_TO_NUMBER(calories) < 500 THEN 'Moderate'
            WHEN TRY_TO_NUMBER(calories) < 700 THEN 'Hearty'
            ELSE 'Indulgent'
        END AS calorie_category,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.menu_items
    WHERE item_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' menu item records';
END;
$$;

-- Enrich ingredients
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

-- Enrich menu item ingredients (recipes) with cost calculations
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
