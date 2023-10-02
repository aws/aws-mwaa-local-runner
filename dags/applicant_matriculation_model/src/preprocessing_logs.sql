create table if not exists {{ params.schema }}.{{ params.table }} (
    id integer,
    record_id varchar(1000),
    created_at timestamp,
    task_name varchar(1000),
    kind varchar(500),
    details super
)