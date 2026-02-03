/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_CUSTOMER
================================================================================
  Loads the customer dimension from enriched layer.
  Implements SCD Type 1 (overwrite) with option for Type 2.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_customer()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
    v_rows_updated INTEGER;
BEGIN
    -- Insert unknown customer record if not exists
    MERGE INTO dim_customer tgt
    USING (
        SELECT 
            'UNKNOWN' AS customer_id,
            'Guest' AS first_name,
            'Customer' AS last_name,
            'Guest Customer' AS full_name,
            NULL AS email,
            NULL AS phone,
            NULL AS city,
            NULL AS state,
            NULL AS zip_code,
            'Unknown' AS age_group,
            'None' AS loyalty_tier,
            0 AS loyalty_tier_rank,
            NULL AS signup_date,
            0 AS tenure_months,
            'Unknown' AS tenure_group,
            FALSE AS marketing_opt_in,
            NULL AS preferred_location_id,
            '1900-01-01'::DATE AS effective_date
    ) src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN INSERT (
        customer_id, first_name, last_name, full_name, email, phone,
        city, state, zip_code, age_group, loyalty_tier, loyalty_tier_rank,
        signup_date, tenure_months, tenure_group, marketing_opt_in,
        preferred_location_id, effective_date, is_current
    ) VALUES (
        src.customer_id, src.first_name, src.last_name, src.full_name, 
        src.email, src.phone, src.city, src.state, src.zip_code, src.age_group,
        src.loyalty_tier, src.loyalty_tier_rank, src.signup_date, src.tenure_months,
        src.tenure_group, src.marketing_opt_in, src.preferred_location_id,
        src.effective_date, TRUE
    );
    
    -- Merge customer dimension from enriched layer (SCD Type 1)
    MERGE INTO dim_customer tgt
    USING (
        SELECT 
            customer_id,
            first_name,
            last_name,
            full_name,
            email,
            phone,
            city,
            state,
            zip_code,
            -- Age grouping
            CASE 
                WHEN age < 18 THEN 'Under 18'
                WHEN age BETWEEN 18 AND 24 THEN '18-24'
                WHEN age BETWEEN 25 AND 34 THEN '25-34'
                WHEN age BETWEEN 35 AND 44 THEN '35-44'
                WHEN age BETWEEN 45 AND 54 THEN '45-54'
                WHEN age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END AS age_group,
            loyalty_tier,
            loyalty_tier_rank,
            signup_date,
            -- Tenure in months
            DATEDIFF('month', signup_date, CURRENT_DATE()) AS tenure_months,
            -- Tenure grouping
            CASE 
                WHEN DATEDIFF('month', signup_date, CURRENT_DATE()) < 3 THEN 'New (0-3 mo)'
                WHEN DATEDIFF('month', signup_date, CURRENT_DATE()) < 12 THEN 'Growing (3-12 mo)'
                WHEN DATEDIFF('month', signup_date, CURRENT_DATE()) < 24 THEN 'Established (1-2 yr)'
                ELSE 'Loyal (2+ yr)'
            END AS tenure_group,
            marketing_opt_in,
            preferred_location_id
        FROM SAMMYS_ENRICHED.enriched_customers
    ) src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        first_name = src.first_name,
        last_name = src.last_name,
        full_name = src.full_name,
        email = src.email,
        phone = src.phone,
        city = src.city,
        state = src.state,
        zip_code = src.zip_code,
        age_group = src.age_group,
        loyalty_tier = src.loyalty_tier,
        loyalty_tier_rank = src.loyalty_tier_rank,
        tenure_months = src.tenure_months,
        tenure_group = src.tenure_group,
        marketing_opt_in = src.marketing_opt_in,
        preferred_location_id = src.preferred_location_id,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, first_name, last_name, full_name, email, phone,
        city, state, zip_code, age_group, loyalty_tier, loyalty_tier_rank,
        signup_date, tenure_months, tenure_group, marketing_opt_in,
        preferred_location_id, effective_date, is_current
    ) VALUES (
        src.customer_id, src.first_name, src.last_name, src.full_name, 
        src.email, src.phone, src.city, src.state, src.zip_code, src.age_group,
        src.loyalty_tier, src.loyalty_tier_rank, src.signup_date, src.tenure_months,
        src.tenure_group, src.marketing_opt_in, src.preferred_location_id,
        CURRENT_DATE(), TRUE
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM dim_customer;
    
    RETURN 'SUCCESS: dim_customer now has ' || v_rows_inserted || ' records';
END;
$$;
