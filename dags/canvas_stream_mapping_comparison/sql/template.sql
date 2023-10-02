
{% set date_suffix = ds.replace("-", "_") %}
{% set dest = params["dest_schema"] + '.' + params["dest_table"] %}
{% set source = dest + '__loading' %}

DROP TABLE IF EXISTS {{ source }};

CREATE TABLE {{ source }} AS 
SELECT * FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY 
{% if id is defined %}
    {{ id|join(', ') }}
{% else %}
    id
{% endif %}
ORDER BY event_time desc nulls last) AS RNK
FROM ({{ self.query() }}))
WHERE RNK = 1;

ALTER TABLE {{ source }}
RENAME COLUMN event_time TO updated_at;

ALTER TABLE {{ source }}
DROP COLUMN RNK;

CREATE TABLE IF NOT EXISTS {{ dest }} (LIKE {{ source }});

DELETE FROM {{ dest }}
USING {{ source }}
WHERE 
{% if id is defined %}
    {% for value in id%}
        {{ source }}.{{ "updated_at" if value == "event_time" else value }} = {{ dest }}.{{ "updated_at" if value == "event_time" else value }}
        {{ "and" if not loop.last else "" }}
    {% endfor %}
{% else %}
    {{ source }}.id={{ dest }}.id
{% endif %}
;

INSERT INTO {{ params.dest_schema }}.{{ params.dest_table }} 
SELECT * FROM {{ source }} AS table2;

DROP TABLE {{ source }};