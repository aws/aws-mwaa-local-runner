{% set date_suffix = ds.replace("-", "_") %}
BEGIN;
    DELETE FROM {{ params.schema }}.{{ params.table }}
    USING {{params.staging_schema}}.{{ params.schema_qualifer }}{{ params.schema }}__{{ params.table }}__{{ date_suffix }}
    WHERE {{params.staging_schema}}.{{ params.schema_qualifer }}{{ params.schema }}__{{ params.table }}__{{ date_suffix }}.{{ params.pk }}={{ params.schema }}.{{ params.table }}.{{ params.pk }};

    INSERT INTO {{ params.schema }}.{{ params.table }}
    SELECT {{ ",".join(params.column_list) if params.column_list else "*" }} FROM {{params.staging_schema}}.{{ params.schema_qualifer }}{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
END;
DROP TABLE {{params.staging_schema}}.{{ params.schema_qualifer }}{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
