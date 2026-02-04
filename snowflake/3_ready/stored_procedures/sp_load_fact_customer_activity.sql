/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_CUSTOMER_ACTIVITY
================================================================================
  Loads the fact_customer_activity accumulating snapshot.
  Calculates lifetime metrics and RFM segmentation.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_customer_activity()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE fact_customer_activity;
    
    INSERT INTO fact_customer_activity (
        customer_sk,
        customer_id,
        first_order_date_key,
        last_order_date_key,
        total_orders,
        total_items_purchased,
        total_spend,
        total_discounts,
        total_tips,
        avg_order_value,
        avg_days_between_orders,
        customer_lifetime_days,
        favorite_location_sk,
        favorite_menu_item_sk,
        favorite_order_type,
        days_since_last_order,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_segment,
        _created_at,
        _updated_at
    )
    WITH customer_orders AS (
        SELECT 
            fs.customer_sk,
            dc.customer_id,
            fs.order_id,
            fs.date_key,
            fs.location_sk,
            fs.item_count,
            fs.total_amount,
            fs.discount_amount,
            fs.tip_amount,
            ot.order_type
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
        JOIN dim_order_type ot ON fs.order_type_sk = ot.order_type_sk
        WHERE dc.customer_id != 'UNKNOWN'
    ),
    customer_metrics AS (
        SELECT 
            customer_sk,
            customer_id,
            MIN(date_key) AS first_order_date_key,
            MAX(date_key) AS last_order_date_key,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(item_count) AS total_items_purchased,
            SUM(total_amount) AS total_spend,
            SUM(discount_amount) AS total_discounts,
            SUM(tip_amount) AS total_tips,
            ROUND(AVG(total_amount), 2) AS avg_order_value
        FROM customer_orders
        GROUP BY customer_sk, customer_id
    ),
    customer_favorites AS (
        SELECT DISTINCT
            customer_sk,
            FIRST_VALUE(location_sk) OVER (
                PARTITION BY customer_sk 
                ORDER BY location_count DESC
            ) AS favorite_location_sk,
            FIRST_VALUE(order_type) OVER (
                PARTITION BY customer_sk 
                ORDER BY type_count DESC
            ) AS favorite_order_type
        FROM (
            SELECT 
                customer_sk,
                location_sk,
                order_type,
                COUNT(*) OVER (PARTITION BY customer_sk, location_sk) AS location_count,
                COUNT(*) OVER (PARTITION BY customer_sk, order_type) AS type_count
            FROM customer_orders
        )
    ),
    customer_favorite_items AS (
        SELECT DISTINCT
            fsl.customer_sk,
            FIRST_VALUE(fsl.menu_item_sk) OVER (
                PARTITION BY fsl.customer_sk 
                ORDER BY item_qty DESC
            ) AS favorite_menu_item_sk
        FROM (
            SELECT 
                customer_sk,
                menu_item_sk,
                SUM(quantity) AS item_qty
            FROM fact_sales_line_item
            GROUP BY customer_sk, menu_item_sk
        ) fsl
    ),
    rfm_scores AS (
        SELECT 
            customer_sk,
            -- Recency: days since last order (lower is better)
            DATEDIFF('day', 
                TO_DATE(last_order_date_key::VARCHAR, 'YYYYMMDD'), 
                CURRENT_DATE()) AS days_since_last,
            total_orders,
            total_spend,
            -- RFM scoring (1-5 scale using NTILE)
            NTILE(5) OVER (ORDER BY DATEDIFF('day', 
                TO_DATE(last_order_date_key::VARCHAR, 'YYYYMMDD'), 
                CURRENT_DATE()) DESC) AS recency_score,
            NTILE(5) OVER (ORDER BY total_orders) AS frequency_score,
            NTILE(5) OVER (ORDER BY total_spend) AS monetary_score
        FROM customer_metrics
    )
    SELECT 
        cm.customer_sk,
        cm.customer_id,
        cm.first_order_date_key,
        cm.last_order_date_key,
        cm.total_orders,
        cm.total_items_purchased,
        cm.total_spend,
        cm.total_discounts,
        cm.total_tips,
        cm.avg_order_value,
        -- Average days between orders
        CASE 
            WHEN cm.total_orders > 1 THEN 
                ROUND(DATEDIFF('day', 
                    TO_DATE(cm.first_order_date_key::VARCHAR, 'YYYYMMDD'),
                    TO_DATE(cm.last_order_date_key::VARCHAR, 'YYYYMMDD')
                ) / (cm.total_orders - 1.0), 2)
            ELSE NULL
        END AS avg_days_between_orders,
        -- Customer lifetime in days
        DATEDIFF('day', 
            TO_DATE(cm.first_order_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) AS customer_lifetime_days,
        cf.favorite_location_sk,
        cfi.favorite_menu_item_sk,
        cf.favorite_order_type,
        rs.days_since_last AS days_since_last_order,
        rs.recency_score,
        rs.frequency_score,
        rs.monetary_score,
        -- RFM Segment
        CASE 
            WHEN rs.recency_score >= 4 AND rs.frequency_score >= 4 AND rs.monetary_score >= 4 
                THEN 'Champions'
            WHEN rs.recency_score >= 4 AND rs.frequency_score >= 3 
                THEN 'Loyal Customers'
            WHEN rs.recency_score >= 4 AND rs.frequency_score <= 2 
                THEN 'New Customers'
            WHEN rs.recency_score >= 3 AND rs.frequency_score >= 3 
                THEN 'Potential Loyalists'
            WHEN rs.recency_score <= 2 AND rs.frequency_score >= 4 
                THEN 'At Risk'
            WHEN rs.recency_score <= 2 AND rs.frequency_score >= 2 
                THEN 'Need Attention'
            WHEN rs.recency_score <= 2 AND rs.frequency_score <= 2 
                THEN 'Hibernating'
            ELSE 'Other'
        END AS rfm_segment,
        CURRENT_TIMESTAMP() AS _created_at,
        CURRENT_TIMESTAMP() AS _updated_at
    FROM customer_metrics cm
    LEFT JOIN customer_favorites cf ON cm.customer_sk = cf.customer_sk
    LEFT JOIN customer_favorite_items cfi ON cm.customer_sk = cfi.customer_sk
    LEFT JOIN rfm_scores rs ON cm.customer_sk = rs.customer_sk;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' records into fact_customer_activity';
END;
$$;
