CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id                      VARCHAR(64)
    , title                 VARCHAR(MAX)
    , note                  VARCHAR(MAX)
    , completed             BOOLEAN
    , completedAt           TIMESTAMP
    , startAt               TIMESTAMP
    , endAt                 TIMESTAMP
    , createdAt             TIMESTAMP
    , activityCategory      SUPER
    , job                   SUPER
    , jobPost               SUPER
    , employer              SUPER
    , ownerMember           SUPER
    , creatorMember         SUPER
    , createdByWorkflow     BOOLEAN
);
