{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , survey_name
    , learn_uuid
    , email
    , JSON_EXTRACT_PATH_TEXT(csat_json, 'I am satisfied with my overall experience with Ada') AS ada_csat
    , JSON_EXTRACT_PATH_TEXT(csat_json, 'The interface of Ada was user-friendly and intuitive') AS user_friendly
    , JSON_EXTRACT_PATH_TEXT(csat_json, 'Ada easily understood my questions') AS easily_understood
    , JSON_EXTRACT_PATH_TEXT(csat_json, 'Ada provided clear and concise answers') AS clear_concise
    , JSON_EXTRACT_PATH_TEXT(csat_json, 'Ada provided relevant code examples or resources when needed') AS code_examples
    , useful
    , topics_or_issues
    , other_feedback
    , tech_proficiency
FROM
    (
        SELECT form_id, submission_id, 'Ask Ada' AS survey_name, submission_timestamp, label, value 
        FROM formstack.ask_ada_merged
    ) PIVOT (
        MAX(value) for label IN (
            'Learn UUID' AS learn_uuid
            , 'Email' AS email
            , 'Please rate the following statements:' AS csat_json
            , 'Compared to other resources (lecturers, tutors, textbooks, online forums, etc.), how useful was Ada?' AS useful
            , 'Were there any topics or issues that Ada struggled with?' AS topics_or_issues
            , 'Please provide any other feedback or comments on improving Ada.' AS other_feedback
            , 'How would you rate your tech proficiency?' AS tech_proficiency
        )
    )
{% endblock %}
