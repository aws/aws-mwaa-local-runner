DROP TABLE IF EXISTS {{ params["schema"] }}.{{ params["table"] }};
CREATE TABLE {{ params["schema"] }}.{{ params["table"] }} (
    id                          VARCHAR(256)
    , email                     VARCHAR(256)
    , first_name                VARCHAR(256)
    , last_name                 VARCHAR(256)
    , content_last_accessed     VARCHAR(256)
    , activation_date           VARCHAR(256)
    , deactivation_date         VARCHAR(256)
    , active                    VARCHAR(256)
    , PRIMARY KEY (id)
);
