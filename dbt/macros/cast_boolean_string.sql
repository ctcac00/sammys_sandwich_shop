/*
================================================================================
  MACRO: cast_boolean_string
================================================================================
  Converts a string column containing boolean-like values ('TRUE', 'YES', '1')
  into a proper SQL boolean. Returns false for any unrecognized value.

  This is the dbt equivalent of the CASE UPPER(...) WHEN 'TRUE' THEN TRUE
  pattern used in the Snowflake stored procedures (e.g. sp_enrich_customers).

  Arguments:
    column_name - the string column to convert

  Usage:
    {{ cast_boolean_string('marketing_opt_in') }}
    -- renders:
    --   case upper(trim(marketing_opt_in))
    --       when 'TRUE' then true
    --       when 'YES'  then true
    --       when '1'    then true
    --       else false
    --   end

  Snowflake UDF equivalent: snowflake/utils/udf_cast_boolean_string.sql
================================================================================
*/

{% macro cast_boolean_string(column_name) %}
    case upper(trim({{ column_name }}))
        when 'TRUE' then true
        when 'YES'  then true
        when '1'    then true
        else false
    end
{%- endmacro %}
