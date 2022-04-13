SELECT
    CASE WHEN count(distinct specimen_id)= count(specimen_id) THEN 'values are unique' ELSE 'values are NOT unique' END AS specimen_id_distinct_check,
    CASE WHEN count(distinct order_id)= count(order_id) THEN 'values are unique' ELSE 'values are NOT unique' END AS order_id_distinct_check,
    CASE WHEN count(distinct external_id_value)= count(external_id_value) THEN 'values are unique' ELSE 'values are NOT unique' END AS external_value_distinct_check
FROM {{params.table}} ; 