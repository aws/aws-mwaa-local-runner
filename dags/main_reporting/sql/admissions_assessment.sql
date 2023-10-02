{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    NVL(admissions.first_name || ' ' || admissions.last_name, students.first_name || ' ' || students.last_name) AS full_name
    , assessment.email
    , NVL(admissions.application_discipline, students.matriculated_cohort_discipline) AS discipline
    , CASE 
        WHEN students.graduation_date IS NOT NULL THEN 'Graduate'
        WHEN students.matriculation_date IS NOT NULL THEN 'Student'
        WHEN admissions.application_submission_date IS NOT NULL THEN 'Applicant'
        ELSE 'Unknown'
        END AS assessment_taker_type
    , assessment.created_date
    , assessment.form_id
    , assessment.ccat_raw_score
    , assessment.ccat_percentile
    , assessment.ccat_spatial_percentile
    , assessment.ccat_verbal_percentile
    , assessment.ccat_math_percentile
    , ROW_NUMBER() OVER (PARTITION BY assessment.email ORDER BY assessment.created_date DESC) = 1 AS is_most_recent
    , ROW_NUMBER() OVER (PARTITION BY assessment.email ORDER BY assessment.ccat_raw_score DESC) = 1 AS is_highest
    , CASE
        WHEN assessment.met_threshold__c IS NOT NULL THEN assessment.met_threshold__c
        WHEN discipline = 'Cybersecurity Analytics' THEN assessment.ccat_raw_score >= 18
        WHEN discipline = 'Cybersecurity Engineering' THEN assessment.ccat_raw_score >= 20
        WHEN discipline = 'Data Science' THEN assessment.ccat_raw_score >= 22
        WHEN discipline = 'Software Engineering' THEN assessment.ccat_raw_score >= 20
        WHEN discipline ~* 'Design' THEN assessment.ccat_raw_score >= 20
        END AS met_score_threshold
FROM
    (
        SELECT 
            NVL(sfdc.email__c, history.email) AS email
            , NVL(sfdc.created_date, history.test_date) AS created_date
            , sfdc.id AS form_id
            , NVL(sfdc.ccat_raw_score__c, history.ccat_raw_score) AS ccat_raw_score
            , NVL(sfdc.ccat_percentile__c, history.ccat_percentile) AS ccat_percentile
            , NVL(sfdc.ccat_spatial_percentile__c, history.ccat_spatial_percentile) AS ccat_spatial_percentile
            , NVL(sfdc.ccat_verbal_percentile__c, history.ccat_verbal_percentile) AS ccat_verbal_percentile
            , NVL(sfdc.ccat_math_percentile__c, history.ccat_math_percentile) AS ccat_math_percentile
            , sfdc.met_threshold__c
        FROM 
            (
                SELECT
                    f.email__c
                    , CONVERT_TIMEZONE('America/New_York', f.createddate) AS created_date
                    , f.id
                    , f.ccat_raw_score__c
                    , f.ccat_percentile__c
                    , f.ccat_spatial_percentile__c
                    , f.ccat_verbal_percentile__c
                    , f.ccat_math_percentile__c
                    , f.met_threshold__c
                FROM {{ params.table_refs["stitch_salesforce.form__c"] }} f
                JOIN {{ params.table_refs["stitch_salesforce.recordtype"] }} rt 
                    ON f.recordtypeid = rt.id
                WHERE rt.name =  'Admissions Aptitude Assessment'
            ) AS sfdc
        FULL OUTER JOIN
            -- Export as of 2021-05-11 11:30am EST - shouldn't need to be updated as new data comes in from SFDC
            data_analytics.admissions_assessment_2021_05_11 AS history
            ON sfdc.email__c = history.email 
            AND sfdc.ccat_spatial_percentile__c = history.ccat_spatial_percentile
            AND sfdc.ccat_verbal_percentile__c = history.ccat_verbal_percentile
            AND sfdc.ccat_math_percentile__c = history.ccat_math_percentile
    ) AS assessment
LEFT JOIN {{ params.table_refs["admissions"] }} admissions
    ON assessment.email = admissions.email
    AND admissions.is_most_recent_app
LEFT JOIN {{ params.table_refs["students"] }} students
    ON assessment.email = students.email
{% endblock %}