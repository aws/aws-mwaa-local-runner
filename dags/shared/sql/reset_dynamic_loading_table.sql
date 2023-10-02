DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }}__loading;
CREATE TABLE {{ params.schema }}.{{ params.table }}__loading (
{{ ",\n".join(params.cols) }}
);
