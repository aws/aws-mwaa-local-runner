SELECT *
FROM {{ params.table }} AS staging_table
            FULL OUTER JOIN (SELECT user_id,source_id, external_id_value, external_id_type, 
            first_name,last_name, date_of_birth,biological_sex FROM projection_popgen.genomic_patient) USING (user_id)
