{% set date_suffix = ds.replace("-", "_") %}
BEGIN;
    {% if params.get("pk") %} -- drop rows in prod with primary keys in staging
        DELETE FROM {{ params["schema"] }}.{{ params["table"] }}
        USING staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }}
        {% if params["pk"] is string %} -- single primary key
        WHERE staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }}.{{ params["pk"] }}={{ params["schema"] }}.{{ params["table"] }}.{{ params["pk"] }};
        {% elif params["pk"] is iterable %} -- multiple primary keys
            {% for pk in params["pk"] %}
                {% if loop.index == 1 %} -- first filter begins with WHERE
        WHERE staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }}.{{ pk }}={{ params["schema"] }}.{{ params["table"] }}.{{ pk }}
                {% else %} -- all other filters begin with AND
        AND staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }}.{{ pk }}={{ params["schema"] }}.{{ params["table"] }}.{{ pk }}
                {% endif %}
                {% if loop.index == params["pk"]|length %} -- final AND ends with ;
        ;
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    INSERT INTO {{ params["schema"] }}.{{ params["table"] }} {% if "column_list" in params %} ({{ ",".join(params["column_list"]) }}) {% endif %}
    SELECT {{ ",".join(params["column_list"]) if "column_list" in params else "*" }} FROM staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }};
END;
DROP TABLE staging.{{ params["schema"] }}__{{ params["table"] }}__{{ date_suffix }};
