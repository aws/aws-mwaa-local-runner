{% extends "rotate_table.sql" %}
{% set date_suffix = ds.replace("-", "_") %}
{% set staging = "staging." + params["schema"] + "__" + params["table"] + "__" +  date_suffix %}
{% block query %}
select {% for column in params["columns"] %}
     {% if loop.first %}
     prod.{{ column }}
     {% else %}
     , prod.{{ column }}
     {% endif %}
     {% endfor %}
     , staging.id is null is_removed
from {{ params["schema"] }}.{{ params["table"] }} prod
left join {{ staging }} staging
    on prod.id = staging.id
    and prod.guild_id = staging.guild_id
{% endblock %}
