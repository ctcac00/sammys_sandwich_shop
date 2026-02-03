/*
================================================================================
  REPORT: RFM SEGMENT SUMMARY
================================================================================
  RFM (Recency, Frequency, Monetary) segment analysis.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_rfm_segment_summary AS
SELECT 
    rfm_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
    SUM(total_spend) AS segment_total_spend,
    ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER (), 2) AS pct_of_revenue,
    ROUND(AVG(total_spend), 2) AS avg_customer_spend,
    ROUND(AVG(total_orders), 1) AS avg_orders,
    ROUND(AVG(days_since_last_order), 0) AS avg_days_since_order,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM fact_customer_activity
GROUP BY rfm_segment
ORDER BY segment_total_spend DESC;
