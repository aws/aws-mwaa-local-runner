SELECT *
FROM {{ params.table }} AS staging_table 
            LEFT JOIN sritest.myhealth_workflow_custom USING(kit_id)