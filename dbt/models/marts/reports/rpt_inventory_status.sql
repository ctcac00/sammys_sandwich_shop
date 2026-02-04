-- Report: Inventory Status
-- Current inventory status by location and ingredient

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
    di.ingredient_name,
    di.category as ingredient_category,
    di.storage_type,
    di.supplier_name,
    
    -- Quantities
    fis.quantity_on_hand,
    fis.quantity_reserved,
    fis.quantity_available,
    fis.reorder_point,
    
    -- Values
    fis.stock_value,
    fis.days_of_supply,
    
    -- Status
    fis.stock_status,
    fis.needs_reorder,
    fis.days_until_expiration,
    fis.expiration_risk,
    
    -- Allergen info
    di.is_allergen,
    di.allergen_type
    
from fct_inventory_snapshot fis
join dim_location dl on fis.location_sk = dl.location_sk
join dim_ingredient di on fis.ingredient_sk = di.ingredient_sk
order by dl.location_name, fis.stock_status, di.ingredient_name
