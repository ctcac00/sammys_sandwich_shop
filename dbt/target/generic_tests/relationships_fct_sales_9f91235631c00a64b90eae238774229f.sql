{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="location_sk", field="location_sk", model=get_where_subquery(ref('fct_sales')), to=ref('dim_location')) }}