/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_MENU_ITEMS
================================================================================
  Transforms raw menu items into enriched format with:
  - Proper data type casting
  - Derived fields (price_tier, calorie_category, days_on_menu)
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
