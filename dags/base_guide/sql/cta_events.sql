{% extends "rotate_table.sql" %}
{% block query %}
-- Leads with a UUID
SELECT
    l.id AS event_id
    , 'stitch_salesforce.lead.id' AS id_source
    , CONVERT_TIMEZONE('America/New_York', l.createddate) AS event_date
    , 'Lead Created' AS event_type
    , l.email
FROM stitch_salesforce.lead l
WHERE l.learn_uuid__c IS NOT NULL

UNION ALL

-- Application Completed
SELECT
    a.id
    , 'stitch_salesforce.application__c.id'
    , CONVERT_TIMEZONE('America/New_York', a.createddate)
    , 'Application Completed'
    , a.email__c
FROM stitch_salesforce.application__c a

UNION ALL 

-- Taken CCAT
SELECT
    f.id
    , 'stitch_salesforce.form__c.id'
    , CONVERT_TIMEZONE('America/New_York', f.createddate)
    , 'Taken CCAT'
    , f.email__c
FROM stitch_salesforce.form__c f
JOIN stitch_salesforce.recordtype rt 
    ON f.recordtypeid = rt.id
WHERE rt.name =  'Admissions Aptitude Assessment'

UNION ALL 

-- Scheduled Interview
SELECT
    lh.id
    , 'stitch_salesforce.leadhistory.id'
    , CONVERT_TIMEZONE('America/New_York', lh.createddate)
    , 'Scheduled Interview'
    , l.email
FROM stitch_salesforce.lead l 
JOIN stitch_salesforce.leadhistory lh 
    ON l.id = lh.leadid
WHERE 
    lh.field = 'Admissions_Interview_Status__c' 
    AND lh.newvalue__string = 'Scheduled'

UNION ALL

-- Completed Interview
SELECT
    lh.id
    , 'stitch_salesforce.leadhistory.id'
    , CONVERT_TIMEZONE('America/New_York', lh.createddate)
    , 'Completed Interview'
    , l.email
FROM stitch_salesforce.lead l 
JOIN stitch_salesforce.leadhistory lh 
    ON l.id = lh.leadid
WHERE 
    field = 'Admissions_Interview_Status__c' 
    AND newvalue__string = 'Completed'

UNION ALL

-- Paid Deposit
SELECT 
    i.id::VARCHAR
    , 'registrar.invoices.id'
    , CONVERT_TIMEZONE('America/New_York', i.paid_at)
    , 'Paid Deposit'
    , i.invoicee_email
FROM registrar.invoices i
JOIN registrar.invoice_categories ic
    ON i.invoice_category_id = ic.id
WHERE 
    ic.name = 'deposit'
    AND i.paid_at IS NOT NULL

UNION ALL

-- EA Signed
SELECT 
    ricm.uuid
    , 'service_documents.railway_ipc_consumed_messages.uuid'
    , CONVERT_TIMEZONE('America/New_York', ricm.updated_at)
    , 'EA Signed'
    , u.email
FROM service_documents.railway_ipc_consumed_messages ricm 
JOIN learn.users u 
    ON ricm.user_uuid = u.learn_uuid
WHERE 
    JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(ricm.encoded_message, 'encoded_message'), 'data'), 'kind') = 'enrollment_agreement'
    AND ricm.status = 'success'
    AND ricm.message_type = 'LearnIpc::Events::Compliance::DocumentCompleted'

UNION ALL

-- Financially Cleared
SELECT 
    pp.uuid
    , 'registrar.payment_plans.uuid'
    , CONVERT_TIMEZONE('America/New_York', pp.financially_cleared_at)
    , 'Financially Cleared'
    , u.email
FROM registrar.payment_plans pp 
JOIN learn.users u 
    ON pp.customer_uuid = u.learn_uuid
WHERE pp.financially_cleared_at IS NOT NULL

UNION ALL

-- Started Prep
SELECT
    ed.id::VARCHAR
    , 'canvas.enrollment_dim.id'
    , CONVERT_TIMEZONE('America/New_York', ed.created_at)
    , 'Started Prep'
    , u.email
FROM canvas.enrollment_dim ed 
JOIN canvas.course_dim cd 
    ON ed.course_id = cd.id 
JOIN canvas.user_dim ud 
    ON ed.user_id = ud.id
JOIN canvas.pseudonym_dim pd 
    ON ud.id = pd.user_id
	AND pd.sis_user_id IS NOT NULL
JOIN learn.users u
    ON pd.sis_user_id = u.learn_uuid
WHERE 
    cd.name ~* '(Pre.?work)|(Prep)' -- Should capture all Pre-work on Canvas

UNION ALL

-- Academic Clearance
SELECT 
    ac.uuid
    , 'registrar.academic_clearances.uuid'
    , CONVERT_TIMEZONE('America/New_York', ac.completed_at)
    , 'Academic Clearance'
    , u.email
FROM registrar.academic_clearances ac 
JOIN learn.users u
    ON ac.student_uuid = u.learn_uuid

UNION ALL

-- Status Changes (for calculating student status)
SELECT 
    te.id::VARCHAR
    , 'registrar.tracking_events.id'
    , CONVERT_TIMEZONE('America/New_York', te.occurred_at)
    , json_extract_path_text(te.data, 'new_status')
    , u.email
FROM registrar.tracking_events te
JOIN learn.users u
    ON json_extract_path_text(te.data, 'student_uuid') = u.learn_uuid
WHERE 
    action = 'Student Status Changed'
{% endblock %}