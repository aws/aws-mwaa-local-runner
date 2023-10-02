SELECT 1 from {{ params.schema }}.{{ params.table }}__loading;
ALTER TABLE {{ params.schema }}.{{ params.table }} RENAME TO {{ params.table }}__old;
ALTER TABLE {{ params.schema }}.{{ params.table }}__loading RENAME TO {{ params.table }};
DROP TABLE {{ params.schema }}.{{ params.table }}__old;
