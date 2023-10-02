DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE {{ params.schema }}.{{ params.table }} (
{{ ",\n".join(params.cols) }}
);
