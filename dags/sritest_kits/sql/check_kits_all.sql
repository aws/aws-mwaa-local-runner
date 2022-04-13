SELECT
    CASE WHEN count(distinct kit_id)= count(kit_id) THEN 'values are unique' ELSE 'values are NOT unique' END AS kit_id_distinct_check 
FROM {{params.table}} ;