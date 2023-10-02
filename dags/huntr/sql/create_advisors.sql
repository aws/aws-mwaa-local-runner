CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id              VARCHAR(64)
    , givenName     VARCHAR(64)
    , familyName    VARCHAR(64)
    , fullName      VARCHAR(128)
    , email         VARCHAR(128)
    , createdAt     TIMESTAMP
    , lastSeenAt    TIMESTAMP
);
