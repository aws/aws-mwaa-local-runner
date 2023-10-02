create table if not exists {{ params.schema }}.{{ params.table }} (
    id integer,
    record_id varchar(1000),
    record_type varchar(1000),
    model_name varchar(100),
    model_version varchar(10),
    prediction float,
    score integer,
    create_at timestamp
)