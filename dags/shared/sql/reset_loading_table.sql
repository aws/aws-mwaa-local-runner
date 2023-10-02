DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }}__loading;
CREATE TABLE {{ params.schema }}.{{ params.table }}__loading (LIKE {{ params.schema }}.{{ params.table }} INCLUDING DEFAULTS);
