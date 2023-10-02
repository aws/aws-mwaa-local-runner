CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
      id          VARCHAR(512)
    , email       VARCHAR(512)
    , name        VARCHAR(512)
    , first_name  VARCHAR(128)
    , last_name   VARCHAR(128)
    , timezone    VARCHAR(512)
    , tracking    VARCHAR(512)
    , PRIMARY KEY(id)
);
