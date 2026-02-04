-- Report: Inventory Value by Location
-- Summarizes inventory value by location

{{ config(materialized='view') }}

with fct_inventory_snapshot as (
    select * from {{ ref('fct_inventory_snapshot') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
    where is_current = true
),

dim_ingredient as (
    select * from {{ ref('dim_ingredient') }}
    where is_current = true
)

select 
    dl.location_name,
    dl.region,
    
    -- Inventory metrics
    count(distinct fis.ingredient_sk) as ingredient_count,
    sum(fis.quantity_on_hand) as total_units_on_hand,
    sum(fis.stock_value) as total_inventory_value,
    
    -- Stock status breakdown
    sum(case when fis.stock_status = 'Out of Stock' then 1 else 0 end) as out_of_stock_count,
    sum(case when fis.stock_status = 'Low Stock' then 1 else 0 end) as low_stock_count,
    sum(case when fis.stock_status = 'Adequate' then 1 else 0 end) as adequate_count,
    sum(case when fis.stock_status = 'Well Stocked' then 1 else 0 end) as well_stocked_count,
    
    -- Reorder needs
    sum(case when fis.needs_reorder then 1 else 0 end) as items_need_reorder,
    
    -- Expiration risk
    sum(case when fis.expiration_risk in ('Expired', 'Critical') then 1 else 0 end) as expiration_risk_count
    
from fct_inventory_snapshot fis
join dim_location dl on fis.location_sk = dl.location_sk
join dim_ingredient di on fis.ingredient_sk = di.ingredient_sk
group by dl.location_name, dl.region
order by total_inventory_value desc
