{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="employee_sk", field="employee_sk", model=get_where_subquery(ref('fct_sales')), to=ref('dim_employee')) }}