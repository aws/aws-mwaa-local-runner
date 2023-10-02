{% set date_suffix = ds.replace("-", "_") %}
BEGIN;
    DELETE FROM {{ params.schema }}.{{ params.table }}
    USING staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}
    WHERE staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}.{{ params.swap_key }}={{ params.schema }}.{{ params.table }}.{{ params.swap_key }};

    INSERT INTO {{ params.schema }}.{{ params.table }}
    SELECT {{ ",".join(params.column_list) if params.column_list else "*" }} FROM staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
END;
DROP TABLE staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
