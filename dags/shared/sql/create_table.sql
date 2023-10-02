CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
  {{ ",\n".join(params.cols) }}
  {% if params.pk %}
    {{ ", primary key(" + params.pk + ")"}}
  {% elif params.dist_key %}
    {{ ", distkey(" + params.dist_key + ")"}}
  {% elif params.sort_key %}
    {{ ", sortkey(" + params.sort_key + ")"}}
  {% else %}
    {{ ";"}}
  {% endif %})
