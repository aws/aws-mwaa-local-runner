
{% set date_suffix = ds.replace("-", "_") %}
{% set dest = params["dest_schema"] + '.' + params["dest_table"] %}
{% set source = dest + '__loading' %}

DROP TABLE IF EXISTS {{ source }};

CREATE TABLE {{ source }} AS 
SELECT * 
FROM (
    -- Identify the most up-to-date record by ID(s)
    SELECT 
        *
        , ROW_NUMBER() OVER (PARTITION BY 
            {% if id is defined %}
                {{ id|join(', ') }}
            {% else %}
                id
            {% endif %}
            ORDER BY updated_at desc nulls last) AS RNK
    FROM (
        {% if ( (derived is defined and ti.get_previous_ti() != None)
                or (derived is not defined) ) %}
        -- Append all created/updated records with existing table
        SELECT * FROM {{ dest }}
        UNION ALL
        {% endif %}
        {{ self.query() }}
    )
)
WHERE RNK = 1;

-- Drop the ID field
ALTER TABLE {{ source }}
DROP COLUMN RNK;

-- Rotate the tables: prod table -> old, loading table -> prod
CREATE TABLE IF NOT EXISTS {{ dest }} ( LIKE {{ source }} );
ALTER TABLE {{ dest }} RENAME TO {{ params["dest_table"] }}__old;
ALTER TABLE {{ source }} RENAME TO {{ params["dest_table"] }};
DROP TABLE {{ dest }}__old;
