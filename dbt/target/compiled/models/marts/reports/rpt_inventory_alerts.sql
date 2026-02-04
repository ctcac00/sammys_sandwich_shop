-- Report: Inventory Alerts
-- Low stock and expiring inventory alerts



with fct_inventory_snapshot as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_inventory_snapshot
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
),

dim_ingredient as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_ingredient
    where is_current = true
)

select 
    dl.location_name,
    di.ingredient_name,
    di.category as ingredient_category,
    di.supplier_name,
    
    -- Current status
    fis.quantity_on_hand,
    fis.quantity_available,
    fis.reorder_point,
    fis.stock_status,
    fis.needs_reorder,
    
    -- Expiration info
    fis.days_until_expiration,
    fis.expiration_risk,
    
    -- Alert type
    case
        when fis.stock_status = 'Out of Stock' then 'CRITICAL: Out of Stock'
        when fis.expiration_risk = 'Expired' then 'CRITICAL: Expired'
        when fis.expiration_risk = 'Critical' then 'WARNING: Expiring Soon'
        when fis.needs_reorder then 'ALERT: Reorder Needed'
        when fis.stock_status = 'Low Stock' then 'INFO: Low Stock'
        else 'OK'
    end as alert_type,
    
    -- Priority (1=highest)
    case
        when fis.stock_status = 'Out of Stock' then 1
        when fis.expiration_risk = 'Expired' then 1
        when fis.expiration_risk = 'Critical' then 2
        when fis.needs_reorder then 3
        when fis.stock_status = 'Low Stock' then 4
        else 5
    end as alert_priority
    
from fct_inventory_snapshot fis
join dim_location dl on fis.location_sk = dl.location_sk
join dim_ingredient di on fis.ingredient_sk = di.ingredient_sk
where fis.needs_reorder = true
   or fis.stock_status in ('Out of Stock', 'Low Stock')
   or fis.expiration_risk in ('Expired', 'Critical', 'Warning')
order by alert_priority, dl.location_name, di.ingredient_name