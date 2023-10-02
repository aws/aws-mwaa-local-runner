{% set date_suffix = ds.replace("-", "_") %}

UPDATE
{% if params.table_ds_suffix %}
  {{ params.schema }}.{{ params.table }}__{{ date_suffix }}
{% else %}
  {{ params.schema }}.{{ params.table }}
{% endif %}
SET snapshot_date = '{{ds}}'::DATE
WHERE snapshot_date IS NULL
