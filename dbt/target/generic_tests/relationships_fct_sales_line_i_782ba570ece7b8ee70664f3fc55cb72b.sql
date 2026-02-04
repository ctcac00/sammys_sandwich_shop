{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="customer_sk", field="customer_sk", model=get_where_subquery(ref('fct_sales_line_item')), to=ref('dim_customer')) }}