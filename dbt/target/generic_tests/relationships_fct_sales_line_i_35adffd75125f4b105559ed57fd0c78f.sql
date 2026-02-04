{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="menu_item_sk", field="menu_item_sk", model=get_where_subquery(ref('fct_sales_line_item')), to=ref('dim_menu_item')) }}