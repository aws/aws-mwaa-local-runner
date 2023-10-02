{% if params.test_env %}
EXPLAIN {% block query %}{% endblock %};
{% else %}
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }}__loading;
CREATE TABLE {{ params.schema }}.{{ params.table }}__loading AS ({{ self.query() }});
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (LIKE {{ params.schema }}.{{ params.table }}__loading);
ALTER TABLE {{ params.schema }}.{{ params.table }} RENAME TO {{ params.table }}__old;
ALTER TABLE {{ params.schema }}.{{ params.table }}__loading RENAME TO {{ params.table }};
DROP TABLE {{ params.schema }}.{{ params.table }}__old;
{% endif %}
