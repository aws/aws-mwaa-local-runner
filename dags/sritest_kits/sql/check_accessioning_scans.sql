SELECT
        CASE WHEN count(distinct kit_id)= count(kit_id) THEN 'column values are unique' ELSE 'column values are NOT unique' END AS distinct_check
FROM {{ params.table }} ;