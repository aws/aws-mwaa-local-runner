{% extends "rotate_table.sql" %}
{% block query %}
WITH proofs_of_hs AS (
    SELECT DISTINCT 
        acc.id
        , True AS high_school_graduation_confirmed__c
    FROM service_documents.file_uploads fu 
    JOIN service_documents.file_upload_types fut 
        ON fu.file_upload_type_id = fut.id
    JOIN stitch_salesforce.account acc
        ON fu.student_uuid = acc.learn_uuid__c
        AND NOT acc.isdeleted
    WHERE 
        fut.name = 'Proof of High School Graduation or Equivalent'
        AND fu.completed_at IS NOT NULL
        -- Get any accounts with NULL or FALSE High School graduation confirmations
        AND NVL(acc.high_school_graduation_confirmed__c, FALSE) = FALSE
)
, statuses AS (
    SELECT 
        acc.id
        , statuses.status AS student_status__c
    FROM (
        SELECT
            admittee_id AS student_uuid
            , status
            , ROW_NUMBER() OVER (PARTITION BY admittee_id ORDER BY updated_at DESC) AS ord
        FROM registrar.admissions
    ) statuses
    JOIN stitch_salesforce.account acc
        ON statuses.student_uuid = acc.learn_uuid__c
        AND NOT acc.isdeleted 
        AND statuses.ord = 1
        -- Get any accounts with NULL or outdated status
        AND statuses.status <> NVL(acc.student_status__c, 'na')
)
SELECT
    account.id
    , NVL(proofs_of_hs.high_school_graduation_confirmed__c
        , account.high_school_graduation_confirmed__c) AS high_school_graduation_confirmed__c
    , NVL(statuses.student_status__c
        , account.student_status__c) AS student_status__c
FROM stitch_salesforce.account
LEFT JOIN proofs_of_hs
    ON account.id = proofs_of_hs.id
LEFT JOIN statuses
    ON account.id = statuses.id
WHERE NVL(proofs_of_hs.id, statuses.id) IS NOT NULL
{% endblock %}