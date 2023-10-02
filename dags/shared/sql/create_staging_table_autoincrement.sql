{% set date_suffix = ds.replace("-", "_") if not params["strftime"] else execution_date.strftime(params.strftime).replace('-', '_').replace(' ', '_') %}
{% set table_reference = 'staging.' + params["schema"] + '__' + params["table"] + '__' + date_suffix %}
DROP TABLE IF EXISTS {{ table_reference }};
CREATE TABLE {{ table_reference }}  (LIKE {{ params["schema"] }}.{{ params["table"] }});
TRUNCATE {{ table_reference }};
ALTER TABLE {{ table_reference }} DROP {{ params["pk"] }};;