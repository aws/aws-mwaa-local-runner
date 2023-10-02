DROP TABLE IF EXISTS {{ params["schema"] }}.{{ params["table"] }};
CREATE TABLE {{ params["schema"] }}.{{ params["table"] }} (
    id              VARCHAR(64)
    , givenName     VARCHAR(64)
    , familyName    VARCHAR(64)
    , fullName      VARCHAR(128)
    , email         VARCHAR(128)
    , createdAt     TIMESTAMP
    , isActive      BOOLEAN
    {% for col in params.cols %}
    , {{ col }} VARCHAR(128)
    {% endfor %}
    , lastSeenAt    TIMESTAMP
    , advisor       SUPER
);
