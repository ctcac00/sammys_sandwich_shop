/*
================================================================================
  UDF: SAFE_CAST_NUMBER
================================================================================
  Snowflake User-Defined Function equivalent of the dbt macro safe_cast_number.

  Safely casts a VARCHAR value to a fixed-precision NUMBER.
  Returns 0 if the value cannot be parsed as a number.

  This mirrors the Jinja macro in dbt/macros/safe_cast_number.sql, but runs
  at query time inside Snowflake rather than at dbt compile time.

  Key difference from the dbt macro:
    - The dbt macro inlines SQL at compile time (no runtime overhead, no object)
    - This UDF is a persistent Snowflake object called like a function in SQL

  Arguments:
    value     VARCHAR  - the string value to cast
    precision INT      - total number of digits
    scale     INT      - digits after the decimal point

  Returns: NUMBER(precision, scale), defaulting to 0 on failure

  Usage:
    SELECT safe_cast_number('12.50', 10, 2);    -- returns 12.50
    SELECT safe_cast_number('abc',   10, 2);    -- returns 0
    SELECT safe_cast_number(NULL,    10, 2);    -- returns 0

  Equivalent dbt macro: dbt/macros/safe_cast_number.sql
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_UTILS;

-- Create the utils schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS SAMMYS_SANDWICH_SHOP.SAMMYS_UTILS;

CREATE OR REPLACE FUNCTION SAMMYS_UTILS.safe_cast_number(
    value     VARCHAR,
    precision INT,
    scale     INT
)
RETURNS NUMBER
AS
$$
    COALESCE(TRY_TO_NUMBER(value, precision, scale), 0)
$$;

-- Example calls:
-- SELECT SAMMYS_UTILS.safe_cast_number('12.50', 10, 2);   -- 12.50
-- SELECT SAMMYS_UTILS.safe_cast_number('bad',   10, 2);   -- 0
-- SELECT SAMMYS_UTILS.safe_cast_number(NULL,    10, 2);   -- 0

-- Example in a query (replaces the raw COALESCE(TRY_TO_NUMBER(...)) pattern):
--
-- Before (as written in sp_enrich_orders.sql):
--   COALESCE(TRY_TO_NUMBER(o.subtotal, 10, 2), 0) AS subtotal
--
-- After (using the UDF):
--   SAMMYS_UTILS.safe_cast_number(o.subtotal, 10, 2) AS subtotal
