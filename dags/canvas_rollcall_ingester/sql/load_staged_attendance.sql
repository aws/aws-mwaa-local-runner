{% set date_suffix = ds.replace("-", "_") %}
BEGIN;
    DELETE FROM {{ params.schema }}.{{ params.table }}
    USING staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}
    WHERE
      staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}.{{ params.compound_key1 }}={{ params.schema }}.{{ params.table }}.{{ params.compound_key1 }} AND
      staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}.{{ params.compound_key2 }}={{ params.schema }}.{{ params.table }}.{{ params.compound_key2 }} AND
      staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }}.{{ params.compound_key3 }}={{ params.schema }}.{{ params.table }}.{{ params.compound_key3 }};

    INSERT INTO {{ params.schema }}.{{ params.table }}
    SELECT {{ ",".join(params.column_list) if params.column_list else "*" }} FROM staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
END;
DROP TABLE staging.{{ params.schema }}__{{ params.table }}__{{ date_suffix }};
