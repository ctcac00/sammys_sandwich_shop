# Reports

This folder contains report views for the Sammy's Sandwich Shop data warehouse. Each file contains a single view for better organization and maintainability.

## Report Inventory

### Customer Reports

| File | View | Description |
|------|------|-------------|
| `rpt_customer_overview.sql` | `v_rpt_customer_overview` | Customer overview with lifetime metrics |
| `rpt_rfm_segment_summary.sql` | `v_rpt_rfm_segment_summary` | RFM segment analysis |
| `rpt_loyalty_tier_analysis.sql` | `v_rpt_loyalty_tier_analysis` | Loyalty tier performance |
| `rpt_customer_cohort_analysis.sql` | `v_rpt_customer_cohort_analysis` | Cohort retention analysis |
| `rpt_top_customers.sql` | `v_rpt_top_customers` | Top customers by spend |
| `rpt_customers_at_risk.sql` | `v_rpt_customers_at_risk` | Churn risk customers |

### Sales Reports

| File | View | Description |
|------|------|-------------|
| `rpt_daily_sales_summary.sql` | `v_rpt_daily_sales_summary` | Daily sales by location |
| `rpt_weekly_sales_summary.sql` | `v_rpt_weekly_sales_summary` | Weekly aggregated sales |
| `rpt_company_daily_totals.sql` | `v_rpt_company_daily_totals` | Company-wide daily totals |

### Inventory Reports

| File | View | Description |
|------|------|-------------|
| `rpt_ingredient_cost_breakdown.sql` | `v_rpt_ingredient_cost_breakdown` | Ingredient costs per item |
| `rpt_estimated_ingredient_usage.sql` | `v_rpt_estimated_ingredient_usage` | Estimated usage from sales |
| `rpt_inventory_status.sql` | `v_rpt_inventory_status` | Current inventory status |
| `rpt_inventory_alerts.sql` | `v_rpt_inventory_alerts` | Low stock/expiring alerts |
| `rpt_inventory_value_by_location.sql` | `v_rpt_inventory_value_by_location` | Inventory value summary |
| `rpt_supplier_spend.sql` | `v_rpt_supplier_spend` | Supplier spend summary |

### Location Reports

| File | View | Description |
|------|------|-------------|
| `rpt_location_performance.sql` | `v_rpt_location_performance` | Location performance metrics |
| `rpt_location_ranking.sql` | `v_rpt_location_ranking` | Location rankings |
| `rpt_location_daily_comparison.sql` | `v_rpt_location_daily_comparison` | Daily comparison pivot |
| `rpt_location_peak_hours.sql` | `v_rpt_location_peak_hours` | Peak hours analysis |
| `rpt_location_employee_performance.sql` | `v_rpt_location_employee_performance` | Employee performance |
| `rpt_drive_thru_analysis.sql` | `v_rpt_drive_thru_analysis` | Drive-thru analysis |

### Menu Item Reports

| File | View | Description |
|------|------|-------------|
| `rpt_top_selling_items.sql` | `v_rpt_top_selling_items` | Top selling items |
| `rpt_top_items_by_category.sql` | `v_rpt_top_items_by_category` | Top items per category |
| `rpt_menu_item_profitability.sql` | `v_rpt_menu_item_profitability` | Profitability quadrants |
| `rpt_daily_item_sales.sql` | `v_rpt_daily_item_sales` | Daily item sales |
| `rpt_category_performance.sql` | `v_rpt_category_performance` | Category performance |
