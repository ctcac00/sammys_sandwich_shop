-- Row count validation: staged orders should match raw source orders
-- This test fails if the row counts differ by more than 5%
-- Matches Snowflake data quality check: "Enriched orders count should match raw orders"

with source_count as (
    select count(*) as cnt
    from {{ source('raw', 'orders') }}
    where order_id is not null  -- Exclude invalid rows
),

staged_count as (
    select count(*) as cnt
    from {{ ref('stg_orders') }}
),

comparison as (
    select
        s.cnt as source_rows,
        t.cnt as staged_rows,
        abs(s.cnt - t.cnt) as row_diff,
        case 
            when s.cnt = 0 then 0
            else round(abs(s.cnt - t.cnt) / s.cnt * 100, 2)
        end as diff_pct
    from source_count s
    cross join staged_count t
)

select *
from comparison
where diff_pct > 5
