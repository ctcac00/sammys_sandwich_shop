# Sammy's Sandwich Shop - Improvement Backlog

Gaps and inconsistencies identified during a cross-platform review of the project.

---

## High Severity

### 1. ~~Payment Method / Order Type Schema Drift Across Platforms~~ RESOLVED

Updated `databricks/dlt/gold/dimensions.py` to align both static dimensions with the canonical Snowflake/dbt schema:

- `dim_payment_method`: renamed `payment_category` to `payment_type`, replaced `is_cash` with `is_digital`, added `processing_fee_pct`, removed `"Unknown"` row, corrected `"Gift Card"` type from `"Other"` to `"Prepaid"`.
- `dim_order_type`: renamed `is_dine_in` to `is_in_store`, replaced `is_drive_thru` with `avg_service_minutes`, removed `"Unknown"` row.

### 2. ~~`"Mobile Pay"` vs `"Mobile Payment"` Value Mismatch in DLT~~ RESOLVED

Fixed in the same change as #1. DLT `dim_payment_method` now seeds `"Mobile Pay"` (matching the source CSV and other platforms) instead of `"Mobile Payment"`.

---

## Medium Severity

### 3. ~~SCD Type 1 vs Type 2 Mismatch (Snowflake vs Others)~~ RESOLVED

Documented as an intentional design difference in the README. Added an "SCD Strategy" row to the Implementation Comparison table and a new "Cross-Platform Design Notes" section explaining that Snowflake uses `AUTOINCREMENT` surrogate keys with SCD Type 2 columns while the other platforms use hash-based SCD Type 1.

### 4. ~~Missing Tests on Intermediate and Report Layers (dbt)~~ RESOLVED

Added column-level tests to `_intermediate.yml` and `_reports.yml`:

- **Intermediate layer**: Primary key `unique`/`not_null` tests on all 10 models plus derived column validation — `loyalty_tier_rank` range check, `price_tier` and `calorie_category` accepted values, `order_period` accepted values, `years_in_operation` range, and `not_null` on key derived fields (`full_name`, `order_date`, `item_count`, etc.).
- **Reports layer**: Sanity tests on one representative report per category — `rpt_daily_sales_summary` (sales), `rpt_customer_overview` (customers), `rpt_top_selling_items` (menu items), `rpt_location_performance` (locations), `rpt_inventory_status` (inventory). Tests include `not_null`, `unique` where applicable, and non-negative value range checks on revenue/quantity columns.

### 5. ~~Dimension Test Coverage Gaps (Databricks Notebooks)~~ RESOLVED

Added the five missing dimensions (`dim_ingredient`, `dim_date`, `dim_time`, `dim_payment_method`, `dim_order_type`) to the `dim_tests` list in `databricks/notebooks/04_tests/test_data_quality.py`. All nine dimensions now have surrogate key `unique`/`not_null` tests, matching dbt's coverage.

### 6. ~~DLT Dimension Columns Diverge From dbt / Snowflake~~ RESOLVED

Documented as an intentional design difference in the README under a new "Cross-Platform Design Notes" section. The DLT pipeline is designed as a self-contained analytical pipeline that exposes additional enriched columns on entity dimensions, while the core join keys and business keys remain identical across all platforms.

---

## Low Severity

### 7. No dbt Source Freshness Configuration

`_sources.yml` does not define `loaded_at_field` or `freshness` blocks. Since the Snowflake raw tables have `_loaded_at`, adding freshness configuration would be straightforward:

```yaml
sources:
  - name: raw
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at
```

### 8. Missing Unknown Records for `dim_menu_item` / `dim_ingredient`

`dim_customer`, `dim_employee`, and `dim_location` all include an "Unknown" record for handling null foreign key lookups from fact tables. However, `dim_menu_item` and `dim_ingredient` do not include unknown records in any platform. If a fact table references a missing `item_id` or `ingredient_id`, the surrogate key will be NULL with no fallback.

**Fix:** Add unknown/default records to `dim_menu_item` and `dim_ingredient` in all four platforms, matching the pattern used by the other dimensions.

### 9. DLT Fiscal Quarter Logic Potentially Incorrect

In `databricks/dlt/gold/dimensions.py`, the fiscal quarter calculation:

```python
when(month(col("full_date")) >= 10, quarter(col("full_date")) - 3)
    .otherwise(quarter(col("full_date")) + 1).alias("fiscal_quarter")
```

For Jan–Mar (calendar Q1), this produces fiscal Q2. For Apr–Jun (calendar Q2), this produces fiscal Q3. The resulting sequence is FQ2/FQ3/FQ4/FQ1 across the calendar year, which may not be the intended fiscal calendar mapping.

**Fix:** Verify intended fiscal year calendar and update the formula if needed. A common Oct-start mapping produces FQ1=Oct-Dec, FQ2=Jan-Mar, FQ3=Apr-Jun, FQ4=Jul-Sep.

### 10. dbt Singular Test Coverage Is Limited

The `dbt/tests/` directory has only 3 singular tests:
- `assert_daily_summary_order_count_matches_fact_sales.sql`
- `assert_stg_customers_row_count_matches_source.sql`
- `assert_stg_orders_row_count_matches_source.sql`

The Snowflake data quality framework covers far more: row counts for all entities, range checks on inventory and employee pay, and referential integrity across the enriched layer.

**Fix:** Add singular tests for:
- Row count parity for all staging models (not just customers and orders).
- Aggregate consistency between `fct_sales_line_item` and `fct_sales`.
- Revenue non-negativity across fact tables.

### 11. README Notebook Count Is Inaccurate

The README states "65+ notebooks organized by layer" for the Databricks notebooks implementation. The actual count is approximately 78 notebooks (83 Python files minus config/helpers).

**Fix:** Update the README to reflect the actual count.

### 12. Databricks Missing `stg_menu_item_ingredients` Composite Key Test

`stg_menu_item_ingredients` is a junction table with a composite key (`item_id` + `ingredient_id`). The Databricks `test_data_quality.py` only tests single-column primary keys. The composite uniqueness constraint is not validated.

**Fix:** Add a composite uniqueness test for `(item_id, ingredient_id)` in the Databricks test suite.

### 13. Metadata Columns Inconsistent Across Platforms

The Snowflake raw DDL adds `_loaded_at` and `_source_file` metadata columns on load. The dbt staging models reference these columns. Neither the Databricks notebooks nor DLT bronze layers add equivalent metadata columns, creating inconsistency in what "raw" means across platforms.

**Fix:** Either add `_loaded_at` / `_source_file` to the Databricks bronze layer or document this as an intentional Snowflake-only feature.

---

## Summary

| # | Issue | Severity | Platforms Affected |
|---|-------|----------|--------------------|
| 1 | ~~Payment method / order type schema drift~~ | ~~High~~ | ~~RESOLVED~~ |
| 2 | ~~`"Mobile Pay"` vs `"Mobile Payment"` value mismatch~~ | ~~High~~ | ~~RESOLVED~~ |
| 3 | ~~SCD Type 1 vs Type 2 structural mismatch~~ | ~~Medium~~ | ~~RESOLVED~~ |
| 4 | ~~Missing tests on intermediate and report layers~~ | ~~Medium~~ | ~~RESOLVED~~ |
| 5 | ~~Dimension test coverage gaps~~ | ~~Medium~~ | ~~RESOLVED~~ |
| 6 | ~~DLT dimension columns diverge from dbt/Snowflake~~ | ~~Medium~~ | ~~RESOLVED~~ |
| 7 | No dbt source freshness configuration | Low | dbt |
| 8 | Missing unknown records for dim_menu_item / dim_ingredient | Low | All platforms |
| 9 | DLT fiscal quarter logic potentially incorrect | Low | DLT |
| 10 | dbt singular test coverage is limited | Low | dbt |
| 11 | README notebook count inaccuracy | Low | README |
| 12 | Missing composite key test for menu_item_ingredients | Low | Databricks notebooks |
| 13 | Metadata columns inconsistent across platforms | Low | Databricks (both) |
