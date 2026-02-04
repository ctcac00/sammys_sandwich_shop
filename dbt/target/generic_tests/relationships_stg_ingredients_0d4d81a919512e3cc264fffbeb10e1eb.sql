{{ config({"severity":"Warn"}) }}
{{ test_relationships(column_name="supplier_id", field="supplier_id", model=get_where_subquery(ref('stg_ingredients')), to=ref('stg_suppliers')) }}