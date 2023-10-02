{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , 'End of Phase ' || phase AS survey_name
    , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
    , MAX(CASE WHEN label = 'Email' THEN value END) AS email
    , MAX(CASE WHEN label = 'Discipline' THEN value END) AS discipline
    , MAX(CASE WHEN label = 'Pace' THEN value END) AS pace
    , MAX(CASE WHEN label = 'Start Date' THEN value END) AS start_date
    , phase
    -- Standard CSATs
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The instructional support I received in this phase.') END
        ) AS instructional_support_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The quality of support I received from my Technical Coach in this phase.') END
        ) AS technical_coach_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The support I received from my Advisor in this phase.')
        ELSE REGEXP_SUBSTR(value, '= (.*)\"', 1, 1, 'e') END
        ) AS advisor_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The quality of the curriculum in this phase.') END
        ) AS curriculum_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The quality of the lectures in this phase.') END
        ) AS lecture_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN JSON_EXTRACT_PATH_TEXT(value, 'The software and technology provided to me (e.g. Base, Canvas).') END
        ) AS product_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(value, 'The onboarding experience and the prework required for this phase.'), ''),
            JSON_EXTRACT_PATH_TEXT(value, 'The onboarding experience and the prep required for this phase.')) END
        ) AS onboarding_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with...' AND IS_VALID_JSON(value)
        THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(value, 'My connectedness to the Flatiron School Community.'), ''),
            JSON_EXTRACT_PATH_TEXT(value, 'My Flatiron Community experience.')) END
        ) AS community_connection_csat
    , MAX(CASE WHEN label = 'What could we do differently to provide a better experience?' THEN value END) AS could_do_differently_text
    -- Phase 1, Onboarding Questions
    , MAX(CASE WHEN label = 'Why did you choose Flatiron School?' THEN value END) AS why_fis_text
    , MAX(CASE WHEN label = 'What other options did you consider?' THEN value END) AS other_options_checkbox
    -- NPS and free-text followup
    , MAX(CASE WHEN label = 'How likely are you to recommend Flatiron School to a friend?' THEN value END) AS nps
    , MAX(CASE WHEN label = 'If we could change one thing about your overall experience, what would it be?' THEN value END) AS change_one_thing_text
    -- Instructor-specific CSAT Questions
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' OR label = 'My instructor…'
        THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(value, 'Set clear expectations.'), ''),
            JSON_EXTRACT_PATH_TEXT(value, 'Sets clear expectations.')) END
        ) AS set_expectations_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' 
        THEN JSON_EXTRACT_PATH_TEXT(value, 'Provide feedback.') END
        ) AS provide_feedback_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' 
        THEN JSON_EXTRACT_PATH_TEXT(value, 'Care about my success.') END
        ) AS care_about_success_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' OR label = 'My instructor…'
        THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(value, 'Inspire and motivate me.'), ''),
            JSON_EXTRACT_PATH_TEXT(value, 'Motivates me to succeed.')) END
        ) AS inspire_and_motivate_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' OR label = 'My instructor…'
        THEN COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(value, 'Demonstrate knowledge.'), ''),
            JSON_EXTRACT_PATH_TEXT(value, 'Is knowledgeable and effective.')) END 
        ) AS demonstrate_knowledge_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' 
        THEN JSON_EXTRACT_PATH_TEXT(value, 'Explain concepts clearly.') END
        ) AS explain_clearly_csat
    , MAX(
        CASE WHEN label = 'I am satisfied with my instructor''s ability to...' 
        THEN JSON_EXTRACT_PATH_TEXT(value, 'Strike a balance between supporting and challenging me.') END
        ) AS support_and_challenge_csat
    , MAX(
        CASE WHEN label = 'What part of your experience in learning from your Instructor has been the most valuable?'
        THEN value END
        ) AS instructor_most_valuable_text
    , MAX(
        CASE WHEN label = 'What part of your experience in learning from your Instructor could be improved?'
        THEN value END
        ) AS instructor_improve_text
    , MAX(
        CASE WHEN label = 'What was the main reason for the scores about your instructor?'
        THEN value END
        ) AS instructor_main_reason_text
    -- Phase 5 questions
    , MAX(
        CASE WHEN label = 'As you officially transition from a student to an alum(!), are you interested in staying connected in one or more of the following ways? Please check all that apply.'
        THEN value END
        ) AS staying_connected_checkbox
    , MAX(
        CASE WHEN label = 'What advice would you give to students just beginning their educational journey with Flatiron School?' 
        THEN value END
        ) AS advice_text
    , MAX(
        CASE WHEN label = 'Flatiron has my permission to share my responses to the above questions (in full or in an abridged format) with new and prospective students via our marketing materials or social media channels.'
        THEN value END
        ) AS permission_yn    
FROM 
    (
        SELECT form_id, submission_id, 1 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_phase_1_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 2 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_phase_2_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 3 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_phase_3_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 4 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_phase_4_merged"] }}
        UNION ALL 
        SELECT form_id, submission_id, 5 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.end_of_phase_5_merged"] }}
	)
GROUP BY 
    form_id
    , submission_id
    , submission_timestamp
    , phase
{% endblock %}
