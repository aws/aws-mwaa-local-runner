CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id                      VARCHAR(64)
    , actiontype            VARCHAR(128)
    , date                  TIMESTAMP
    , creatorMember         SUPER
    , ownerMember           SUPER
    , document              SUPER
);