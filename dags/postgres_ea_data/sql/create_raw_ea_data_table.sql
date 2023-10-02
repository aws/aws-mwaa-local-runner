CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    rownum                  VARCHAR(128)
    , tabId                 VARCHAR(128)
    , tabLabel              VARCHAR(128)
    , value                 VARCHAR(128)    
    , compliance_event_id   VARCHAR(128)
    , envelope_id           VARCHAR(128)
    , requirement_uuid      VARCHAR(128)
    , actor_uuid            VARCHAR(128)
    , recipient_uuid        VARCHAR(128)
    , updated_at            TIMESTAMP
    , PRIMARY KEY (tabId)
);