{% set date_suffix = ds.replace("-", "_") %}
{% for table in params["tables"] %}
  CREATE TABLE IF NOT EXISTS staging.{{ params.schema }}__{{ table }}__{{ date_suffix }} (LIKE {{ params.schema }}.{{ table }} INCLUDING DEFAULTS);
  TRUNCATE staging.{{ params.schema }}__{{ table }}__{{ date_suffix }};
{% endfor %}
