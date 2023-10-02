{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , 'End of Phase ' || phase AS survey_name
    , learn_uuid
    , email
    , campus
    , discipline
    , region
    , pace
    , start_date
    , section
    , phase
    , week
    -- NPS
    , nps
    , change_one_thing_text
    -- Onboarding
    , why_fis_text
    -- Curriculum CSAT
    , curriculum_csat
    , net_admin_curriculum_csat
    , sys_admin_curriculum_csat
    , hunt_curriculum_csat
    , siem_curriculum_csat
    , grc_curriculum_csat
    , strat_curriculum_csat
    , threat_curriculum_csat
    , capstone_curriculum_csat
    , crypto_curriculum_csat
    , logs_curriculum_csat
    , culture_curriculum_csat
    , python_curriculum_csat
    , app_sec_curriculum_csat
    -- Lecture CSAT
    , lecture_csat
    -- Instructional Support CSAT
    , NVL(instructional_support_1_csat, instructional_support_2_csat, instructional_support_3_csat) AS instructional_support_csat
    -- Central Lecturer-specific CSAT
    , lecturer1_set_expectations_csat
    , lecturer1_inspire_and_motivate_csat
    , lecturer1_demonstrate_knowledge_csat
    , lecturer1_explain_clearly_csat
    , lecturer1_most_valuable_text
    , lecturer1_improve_text
    -- Instructor-specific CSAT
    , instructor_name
    , instructor1_set_expectations_csat AS set_expectations_csat
    , instructor1_provide_feedback_csat AS provide_feedback_csat
    , instructor1_care_about_success_csat AS care_about_success_csat
    , instructor1_inspire_and_motivate_csat AS inspire_and_motivate_csat
    , instructor1_demonstrate_knowledge_csat AS demonstrate_knowledge_csat
    , instructor1_explain_clearly_csat AS explain_clearly_csat
    , instructor1_support_and_challenge_csat AS support_and_challenge_csat
    , instructor1_most_valuable_text AS instructor_most_valuable_text
    , instructor1_improve_text AS instructor_improve_text
    -- Community CSAT
    , part_of_the_community_csat
    , safe_and_included_csat
    -- Phase 5 questions
    , staying_connected_checkbox
    , advice_text
    , permission_yn
FROM 
    (
        SELECT 
            s.form_id
            , submission_id
            , submission_timestamp
            , CASE 
                WHEN label ~ 'Who is your primary instructor' THEN 'Who is your primary instructor?'
                ELSE label
                END AS label
            , value 
        FROM {{ params.table_refs["formstack.nps_submissions_deprecated"] }} s
        JOIN {{ params.table_refs["formstack.nps_responses_deprecated"] }} r
            ON s.id = r.submission_id
	) PIVOT (
        MAX(value) for label IN (
            -- Identifying questions
            'Learn_UUID' AS learn_uuid
            , 'Email address' AS email
            , 'Which Flatiron campus/program are you a part of?' AS campus
            , 'Discipline' AS discipline
            , 'Which region are you located in?' AS region
            , 'What is your program pace?' AS pace
            , 'What is your cohort start date?' AS start_date
            , 'Which section are you in?' AS section
            , 'Which Module/Phase did you just complete?' AS phase
            , 'Which week (and courses) did you just complete?' AS week

            -- NPS
            , 'How likely are you to recommend Flatiron School to a friend?' AS nps
            , 'If we could change one thing about your overall experience, what would it be?' AS change_one_thing_text

            -- Onboarding
            , 'What do you remember most about why you chose Flatiron School?' AS why_fis_text

            -- Relevant CSAT metrics
            , 'I am satisfied with the overall curriculum in this Module/Phase.' AS curriculum_csat
            , 'I am satisfied with the overall curriculum in the Net Admin course.' AS net_admin_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Sys Admin course.' AS sys_admin_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Hunt course.' AS hunt_curriculum_csat
            , 'I am satisfied with the overall curriculum in the SIEM course.' AS siem_curriculum_csat
            , 'I am satisfied with the overall curriculum in the GRC course.' AS grc_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Strat course.' AS strat_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Threat course.' AS threat_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Capstone course.' AS capstone_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Crypto course.' AS crypto_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Logs course.' AS logs_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Culture course.' AS culture_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Python course.' AS python_curriculum_csat
            , 'I am satisfied with the overall curriculum in the Application Security course.' AS app_sec_curriculum_csat

            , 'I am satisfied with the quality of lecture provided in this Module/Phase/course.' AS lecture_csat
            , 'I am satisfied with the quality of Instruction provided in this Module/Phase.' AS instructional_support_1_csat
            , 'I am satisfied with the quality of instruction provided in this module - both video and live support, as applicable.' AS instructional_support_2_csat
            , 'I am satisfied with the quality of lab instruction provided in this Module/Phase/course.' AS instructional_support_3_csat

            -- Central Lecturer
            , 'Lecturer 1 sets clear lesson objectives and expectations.' AS lecturer1_set_expectations_csat
            , 'Lecturer 1 inspires and motivates me.' AS lecturer1_inspire_and_motivate_csat
            , 'Lecturer 1 is highly knowledgeable.' AS lecturer1_demonstrate_knowledge_csat
            , 'Lecturer 1 is organized and explains concepts clearly.' AS lecturer1_explain_clearly_csat
            , 'What part of your experience in lectures with Lecturer 1 has been the most valuable?' AS lecturer1_most_valuable_text
            , 'What part of your experience in lectures with Lecturer 1 could be improved?' AS lecturer1_improve_text

            -- Instructor-specific
            , 'Who is your primary instructor?' AS instructor_name
            , 'Instructor 1 sets clear expectations.' AS instructor1_set_expectations_csat
            , 'Instructor 1 provides feedback.' AS instructor1_provide_feedback_csat
            , 'Instructor 1 cares about my success.' AS instructor1_care_about_success_csat
            , 'Instructor 1 inspires and motivates me.' AS instructor1_inspire_and_motivate_csat
            , 'Instructor 1 is highly knowledgeable.' AS instructor1_demonstrate_knowledge_csat
            , 'Instructor 1 explains concepts clearly.' AS instructor1_explain_clearly_csat
            , 'Instructor 1 strikes a balance between supporting and challenging me.' AS instructor1_support_and_challenge_csat
            , 'What part of your experience in learning from Instructor 1 has been the most valuable?' AS instructor1_most_valuable_text
            , 'What part of your experience in learning from Instructor 1 could be improved?' AS instructor1_improve_text

            -- Community
            , 'Please consider the following statement: I feel like I am a valued part of the Flatiron community.' AS part_of_the_community_csat
            , 'Please consider the following statement: I feel safe and included at Flatiron School.' AS safe_and_included_csat

            -- Phase 5
            , 'As you officially transition from a student to an alum(!), are you interested in staying connected in one or more of the following ways? Please check all that apply.' AS staying_connected_checkbox
            , 'Now that you have come this far, what advice would you give to a new student who is just getting started?' AS advice_text

            , 'Flatiron has my permission to share my response to the above question (in full or in an abridged format) with new and prospective students via our marketing materials or social media channels.' AS permission_yn
        )
    )
{% endblock %}
