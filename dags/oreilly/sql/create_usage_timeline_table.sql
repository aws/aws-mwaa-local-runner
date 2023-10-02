CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id                          VARCHAR(128)
    , date                      DATE
    , account_identifier        VARCHAR(256)
    , user_identifier           VARCHAR(256)
    , email                     VARCHAR(256)
    , title_identifier          VARCHAR(256)
    , title                     VARCHAR(256)
    , content_format            VARCHAR(256)
    , virtual_pages             INT
    , duration_seconds          INT
    , topic                     VARCHAR(256)
    , device_type               VARCHAR(256)
    , platform                  VARCHAR(256)
    , units_viewed              FLOAT
    , min_client_timestamp      VARCHAR(256)
    , max_client_timestamp      VARCHAR(256)
    , PRIMARY KEY (id)
);
