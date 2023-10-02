{% extends "rotate_table.sql" %}
{% block query %}
WITH academic_clearances AS (
    SELECT DISTINCT
        a.salesforce_opportunity_id AS id
        , True AS academically_cleared__c
    FROM registrar.admissions a
    JOIN registrar.cohort_registrations cr
        ON a.id = cr.admission_id
    JOIN registrar.cohorts c
        ON cr.cohort_id = c.id
    JOIN registrar.courses cc
        ON c.course_id = cc.id
    JOIN registrar.academic_clearances ac 
        ON cc.uuid = ac.course_uuid
        AND a.admittee_id = ac.student_uuid
    LEFT JOIN stitch_salesforce.opportunity o
        ON a.salesforce_opportunity_id = o.id
        AND NOT o.isdeleted
    WHERE 
        -- Get any opportunities with NULL or FALSE academic clearance 
        NVL(o.academically_cleared__c, FALSE) = FALSE
),
ea_clearances AS (
    SELECT 
        te.student_uuid
        , te.catalog_acknowledgement__c
        , te.enrollment_agreement__c
    FROM (
        SELECT 
            JSON_EXTRACT_PATH_TEXT(te.data, 'student_uuid') AS student_uuid
            , 1 = MAX(CASE WHEN JSON_EXTRACT_PATH_TEXT(te.data, 'kind') ~ 'catalog' THEN 1 ELSE 0 END) AS catalog_acknowledgement__c
            , 1 = MAX(CASE WHEN JSON_EXTRACT_PATH_TEXT(te.data, 'kind') = 'enrollment_agreement' THEN 1 ELSE 0 END) AS enrollment_agreement__c
        FROM registrar.tracking_events te
        WHERE te.action in (
            'Enrollment Agreement Cleared'
            , 'Compliance Document Completed'
        )
        GROUP BY 1
    ) te
    LEFT JOIN stitch_salesforce.opportunity o
        ON te.student_uuid = o.learn_uuid__c
        AND NOT o.isdeleted
    WHERE
        te.catalog_acknowledgement__c <> o.catalog_acknowledgement__c
        OR te.enrollment_agreement__c <> o.enrollment_agreement__c
)
SELECT 
    o.id
    , NVL(ac.academically_cleared__c, o.academically_cleared__c) AS academically_cleared__c
    , NVL(ea.catalog_acknowledgement__c, o.catalog_acknowledgement__c) AS catalog_acknowledgement__c
    , NVL(ea.enrollment_agreement__c, o.enrollment_agreement__c) AS enrollment_agreement__c
FROM stitch_salesforce.opportunity o
LEFT JOIN academic_clearances ac
    ON o.id = ac.id
LEFT JOIN ea_clearances ea
    ON o.learn_uuid__c = ea.student_uuid
-- Update if any one of these doesn't match prod
WHERE NVL(ac.academically_cleared__c, ea.catalog_acknowledgement__c, ea.enrollment_agreement__c) IS NOT NULL
{% endblock %}