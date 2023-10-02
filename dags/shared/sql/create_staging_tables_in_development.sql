{% set date_suffix = ds.replace("-", "_") %}
{% for table in params["tables"] %}
  CREATE TABLE IF NOT EXISTS {{params.staging_schema}}.{{params.schema_qualifier}}{{ params.schema }}__{{ table }}__{{ date_suffix }} (LIKE {{ params.schema }}.{{ table }});
  TRUNCATE {{params.staging_schema}}.{{params.schema_qualifier}}{{ params.schema }}__{{ table }}__{{ date_suffix }};
{% endfor %}
