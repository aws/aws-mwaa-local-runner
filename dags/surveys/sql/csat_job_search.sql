{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , survey_name
    , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
    , MAX(CASE WHEN label = 'Email' THEN value END) AS email
    , MAX(CASE WHEN label = 'Discipline' THEN value END) AS discipline
    , MAX(CASE WHEN label = 'Pace' THEN value END) AS pace
    , MAX(CASE WHEN label = 'Start Date' THEN value END) AS start_date
    -- Career Prep CSAT
    , MAX(CASE WHEN label = 'I am satisfied with the quality of the Career Prep materials.' THEN value END) AS career_prep_csat
    , MAX(CASE WHEN label = 'How could we improve the Career Prep materials?' THEN value END) AS career_prep_improve_text
    -- Career Workshop CSAT
    , MAX(CASE WHEN label = 'Who was your workshop presenter?' THEN value END) AS workshop_presenter
    , MAX(CASE WHEN label = 'Which workshop did you attend?' THEN value END) AS workshop_attended
    , MAX(
        CASE WHEN label IN ('I am satisfied with today''s Career Workshop.', 
                            'I am satisfied with the usefulness of the concepts and strategies taught in today''s workshop that I can apply in my job search.')
        THEN value END
        ) AS workshop_csat
    , MAX(CASE WHEN label = 'What is one key takeaway from this event?' THEN value END) AS workshop_takeaway_text
    , MAX(CASE WHEN label = 'Is there anything else you’d like to share?' THEN value END) AS workshop_anything_else_text
    -- Day 30, 90, 120 CSATs
    , MAX(
        CASE WHEN label IN ('I am satisfied with the support I''m getting from my Career Coach in my job search.',
                            'I am satisfied with the support I received from my Career Coach in my job search.')
        THEN value END) AS career_coach_support_csat
    , MAX(
        CASE WHEN label IN ('I am satisfied with the quality of feedback provided to me from my Career Coach throughout my job search.',
                            'I am satisfied with the feedback my Career Coach has provided to me throughout my job search.')
        THEN value END) AS career_coach_feedback_csat
    , MAX(CASE WHEN label = 'What, if anything, would you like to see your Career Coach do differently to better support you in your job search?' THEN value END) AS career_coach_do_differently_text
    -- NPS
    , MAX(CASE WHEN label = 'How likely are you to recommend Flatiron School to a friend?' THEN value END) AS nps
    , MAX(CASE WHEN label = 'If we could change one thing about your overall experience, what would it be?' THEN value END) AS change_one_thing_text
    -- Placement or Withdrawal CSAT
    , MAX(CASE WHEN label = 'What part of your Career Services experience did you find most valuable?' THEN value END) AS career_services_most_valuable_text
    , MAX(CASE WHEN label = 'Now that you are at the end of your job search, what advice would you give to yourself on Day 1?' THEN value END) AS job_search_advice_text
    -- Marketing Alumni Feature
    , MAX(
        CASE WHEN label IN ('Flatiron has my permission to share my responses to the above questions (in full or in an abridged format) with new and prospective students via our marketing materials or social media channels.',
                            'Would you allow Marketing to highlight your story on social media and potentially share your answers to this and prior surveys you’ve completed while at Flatiron School with prospective students?')
        THEN value END) AS permission_yn
    , MAX(CASE WHEN label = 'What was your career path before Flatiron school?' THEN value END) AS mkt_career_path
    , MAX(CASE WHEN label = 'What was your favorite part of the program?' THEN value END) AS mkt_favorite_part
    , MAX(CASE WHEN label = 'How would you describe your job search experience and what role did your career coach play in landing your new job?' THEN value END) AS mkt_job_search_exp
    , MAX(CASE WHEN label = 'What was your biggest takeaway from your time at Flatiron School?' THEN value END) AS mkt_biggest_takeaway
    , MAX(CASE WHEN label = 'Lastly, would you also allow marketing to review your initial application to Flatiron School, as part of this feature?' THEN value END) AS mkt_permission_review_app
FROM 
    (
        SELECT form_id, submission_id, 'Career Prep' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.career_prep_merged"] }}
        UNION ALL
        SELECT 
            form_id
            , submission_id
            , 'Career Workshop' AS survey_name
            , submission_timestamp
            , label
            , CASE 
                WHEN value = 'Building Your Online Brand' THEN 'Building Your Online Brand/LinkedIn'
                WHEN value = 'Networking' THEN 'Networking: Start Conversations and Cultivate Connections' 
                ELSE value END AS value
        FROM {{ params.table_refs["formstack.career_workshop_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 'Job Search Day 30' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.job_search_day30_merged"] }}
        UNION ALL
        SELECT form_id, submission_id, 'Job Search Day 90' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.job_search_day90_merged"] }}
        UNION ALL
        SELECT form_id, submission_id, 'Job Search Day 120' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.job_search_day120_merged"] }}
        UNION ALL
        SELECT form_id, submission_id, 'Job Search Withdrawal' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_job_search_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 'Job Placement' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.job_placement_merged"] }} 
    )
GROUP BY
    form_id
    , submission_id
    , submission_timestamp
    , survey_name
{% endblock %}
