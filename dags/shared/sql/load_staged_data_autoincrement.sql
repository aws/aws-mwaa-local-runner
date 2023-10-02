{% set date_suffix = ds.replace("-", "_") if not "strftime" in params else execution_date.strftime(params["strftime"]).replace('-', '_').replace(' ', '_') %}
{% set production_reference = params["schema"] + '.' + params["table"] %}
{% set staging_reference = 'staging.' + params["schema"] + '__' + params["table"] + '__' + date_suffix %}
BEGIN;
    INSERT INTO {{ production_reference }}
    SELECT (SELECT NVL(max({{ params["pk"] }}), 0) FROM {{ production_reference }}) + ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) {{ params["pk"] }}
         , {{ ",".join(params["column_list"]) if "column_list" in params else staging_reference + ".*" }} FROM {{ staging_reference }};
END;
DROP TABLE {{ staging_reference }};