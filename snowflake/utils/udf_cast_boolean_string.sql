/*
================================================================================
  UDF: CAST_BOOLEAN_STRING
================================================================================
  Snowflake User-Defined Function equivalent of the dbt macro cast_boolean_string.

  Converts a VARCHAR column containing boolean-like text ('TRUE', 'YES', '1')
  into a proper Snowflake BOOLEAN. Returns FALSE for any unrecognized value.

  This mirrors the Jinja macro in dbt/macros/cast_boolean_string.sql, but runs
  at query time inside Snowflake rather than at dbt compile time.

  Key difference from the dbt macro:
    - The dbt macro inlines SQL at compile time (no runtime overhead, no object)
    - This UDF is a persistent Snowflake object called like a function in SQL

  Arguments:
    value VARCHAR - the string column value to convert

  Returns: BOOLEAN (TRUE or FALSE)

  Usage:
    SELECT cast_boolean_string('TRUE');   -- returns TRUE
    SELECT cast_boolean_string('yes');    -- returns TRUE
    SELECT cast_boolean_string('1');      -- returns TRUE
    SELECT cast_boolean_string('false');  -- returns FALSE
    SELECT cast_boolean_string(NULL);     -- returns FALSE

  Equivalent dbt macro: dbt/macros/cast_boolean_string.sql
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_UTILS;

-- Create the utils schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS SAMMYS_SANDWICH_SHOP.SAMMYS_UTILS;

CREATE OR REPLACE FUNCTION SAMMYS_UTILS.cast_boolean_string(value VARCHAR)
RETURNS BOOLEAN
AS
$$
    CASE UPPER(TRIM(value))
        WHEN 'TRUE' THEN TRUE
        WHEN 'YES'  THEN TRUE
        WHEN '1'    THEN TRUE
        ELSE FALSE
    END
$$;

-- Example calls:
-- SELECT SAMMYS_UTILS.cast_boolean_string('TRUE');   -- TRUE
-- SELECT SAMMYS_UTILS.cast_boolean_string('YES');    -- TRUE
-- SELECT SAMMYS_UTILS.cast_boolean_string('1');      -- TRUE
-- SELECT SAMMYS_UTILS.cast_boolean_string('false');  -- FALSE
-- SELECT SAMMYS_UTILS.cast_boolean_string(NULL);     -- FALSE

-- Example in a query (replaces the raw CASE WHEN pattern in sp_enrich_customers.sql):
--
-- Before:
--   CASE UPPER(marketing_opt_in)
--       WHEN 'TRUE' THEN TRUE
--       WHEN 'YES'  THEN TRUE
--       WHEN '1'    THEN TRUE
--       ELSE FALSE
--   END AS marketing_opt_in
--
-- After (using the UDF):
--   SAMMYS_UTILS.cast_boolean_string(marketing_opt_in) AS marketing_opt_in
