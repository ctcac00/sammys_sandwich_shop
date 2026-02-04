{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="location_id", field="location_id", model=get_where_subquery(ref('stg_orders')), to=ref('stg_locations')) }}