{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , survey_name
    , learn_uuid
    , email
    , discipline
    , pace
    , start_date
    -- NPS
    , nps
    -- Standard CSATs
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The instructional support I received in this phase.') AS instructional_support_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The quality of the curriculum (e.g. lessons, labs, quizzes) in this phase.') AS curriculum_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The quality of the lectures in this phase, if applicable.') AS lecture_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The level of communication I received (e.g. general announcements,  expectation-setting, assignment due dates).') AS communication_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The software and technology provided to me (e.g. Canvas, Slack).') AS product_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with, 'The pace of the program.') AS pacing_csat
    , change_one_thing_text
    -- Instructor-specific CSAT Questions
    , COALESCE(
        NULLIF(JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Set clear expectations.'), ''),
        JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor_v2, 'Sets clear expectations.')
     ) AS set_expectations_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Provide feedback.') AS provide_feedback_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Care about my success.') AS care_about_success_csat
    , COALESCE(
        NULLIF(JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Inspire and motivate me.'), ''),
        JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor_v2, 'Motivates me to succeed.')
     ) AS inspire_and_motivate_csat
    , COALESCE(
        NULLIF(JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Demonstrate knowledge.'), ''),
        JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor_v2, 'Is knowledgeable and effective.')
     ) AS demonstrate_knowledge_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Explain concepts clearly.') AS explain_clearly_csat
    , JSON_EXTRACT_PATH_TEXT(satisfied_with_instructor, 'Strike a balance between supporting and challenging me.') AS support_and_challenge_csat
    , instructor_most_valuable_text
    , instructor_improve_text
    , instructor_main_reason_text
FROM 
    (
        SELECT form_id, submission_id, 'Enterprise - End of Phase' AS survey_name, submission_timestamp, label, value 
        FROM formstack.enterprise_end_of_phase_merged

        UNION ALL 

        SELECT form_id, submission_id, 'Enterprise - End of Phase + NPS' AS survey_name, submission_timestamp, label, value 
        FROM formstack.enterprise_end_of_phase_nps_merged
        
        UNION ALL 

        SELECT form_id, submission_id, 'Enterprise - End of Program' AS survey_name, submission_timestamp, label, value 
        FROM formstack.enterprise_end_of_program_merged
    )
PIVOT (
    MAX(value) for label IN (
        'Learn UUID' AS learn_uuid
        , 'Email' AS email
        , 'Discipline' AS discipline
        , 'Pace' AS pace
        , 'Start Date' AS start_date
        , 'How likely are you to recommend this program to a friend?' AS nps
        , 'I am satisfied with...' AS satisfied_with
        , 'If we could change one thing about your overall experience, what would it be?' AS change_one_thing_text
        , 'I am satisfied with my instructor''s ability to...' AS satisfied_with_instructor
        , 'My instructor…' AS satisfied_with_instructor_v2
        , 'What part of your experience in learning from your Instructor has been the most valuable?' AS instructor_most_valuable_text
        , 'What part of your experience in learning from your Instructor could be improved?' AS instructor_improve_text
        , 'What was the main reason for the scores about your instructor?' AS instructor_main_reason_text
    )
    )
{% endblock %}