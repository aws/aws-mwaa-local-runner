SELECT
    CASE WHEN count(distinct user_id)= count(user_id) THEN 'values are unique' ELSE 'values are NOT unique' END AS user_id_distinct_check
FROM {{params.table}}
    