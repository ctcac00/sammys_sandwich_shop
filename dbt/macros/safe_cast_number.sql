/*
================================================================================
  MACRO: safe_cast_number
================================================================================
  Safely casts a string column to a numeric type.
  Returns a default value (0) if the cast fails, avoiding NULL propagation.

  This is the dbt equivalent of the COALESCE(TRY_TO_NUMBER(...), 0) pattern
  used throughout the Snowflake stored procedures.

  Arguments:
    column_name   - the column to cast (string)
    precision     - total number of digits (default: 10)
    scale         - digits after decimal point (default: 2)
    default_value - fallback when cast fails (default: 0)

  Usage:
    {{ safe_cast_number('subtotal') }}
    -- renders: coalesce(try_to_number(subtotal, 10, 2), 0)

    {{ safe_cast_number('loyalty_points', scale=0) }}
    -- renders: coalesce(try_to_number(loyalty_points, 10, 0), 0)

  Snowflake UDF equivalent: snowflake/utils/udf_safe_cast_number.sql
================================================================================
*/

{% macro safe_cast_number(column_name, precision=10, scale=2, default_value=0) %}
    coalesce(try_to_number({{ column_name }}, {{ precision }}, {{ scale }}), {{ default_value }})
{%- endmacro %}
