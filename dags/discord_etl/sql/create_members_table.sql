CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    id                  VARCHAR(128)
    , name              VARCHAR(128)
    , display_name      VARCHAR(128)
    , nickname          VARCHAR(128)
    , roles             VARCHAR(1028)
    , guild_id          VARCHAR(128)
    , guild_name        VARCHAR(128)
    , created_at        TIMESTAMP
    , joined_at         TIMESTAMP
    , is_removed        BOOLEAN
    , PRIMARY KEY (id, guild_id)
);