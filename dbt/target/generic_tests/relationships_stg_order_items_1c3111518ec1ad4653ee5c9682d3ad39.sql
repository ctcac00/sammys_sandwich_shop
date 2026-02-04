{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="item_id", field="item_id", model=get_where_subquery(ref('stg_order_items')), to=ref('stg_menu_items')) }}