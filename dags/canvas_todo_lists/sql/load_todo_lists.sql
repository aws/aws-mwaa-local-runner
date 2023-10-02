{% set date_suffix = ds.replace("-", "_") %}
BEGIN;
    TRUNCATE TABLE {{ params.schema }}.{{ params.table }};
    INSERT INTO {{ params.schema }}.{{ params.table }}
    SELECT 
        pd.sis_user_id learn_uuid
      , staging.*
    FROM staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }} staging
    LEFT JOIN canvas.user_dim ud 
        ON staging.user_id = ud.canvas_id 
    LEFT JOIN canvas.pseudonym_dim pd 
        ON ud.id = pd.user_id;
END;
DROP TABLE staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }};