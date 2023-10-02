CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id                      VARCHAR(64)
    , title                 VARCHAR(128)
    , isRemote              BOOLEAN
    , jobType               VARCHAR(64)
    , url                   VARCHAR(MAX)
    , postDate              TIMESTAMP
    , createdAt             TIMESTAMP
    , location              SUPER
    , jobPostStatus         SUPER
    , employer              SUPER
    , ownerMember           SUPER
    , htmlDescription       VARCHAR(MAX)
);
