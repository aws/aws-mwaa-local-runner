CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    marketoGUID                 VARCHAR(128)
    , leadId                    VARCHAR(128)
    , activityDate              TIMESTAMP
    , activityTypeId            VARCHAR(128)
    , campaignId                VARCHAR(128)
    , primaryAttributeValueId   VARCHAR(256)
    , primaryAttributeValue     VARCHAR(MAX)
    , attributes                SUPER
    , PRIMARY KEY (marketoGUID)
);