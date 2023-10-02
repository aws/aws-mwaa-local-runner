{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    -- Survey and student identifiers
    form_id
    , submission_id
    , submission_timestamp
    , survey_name
    , surveys.learn_uuid
    , surveys.email
    , surveys.discipline
    , NVL(surveys.pace, rosters.pacing) AS pace
    , CASE 
        WHEN surveys.start_date ~ '^\\d{6}$'
        THEN TO_DATE(surveys.start_date, 'MMDDYY')
        WHEN surveys.start_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
        THEN TO_DATE(surveys.start_date, 'YYYY-MM-DD')
        ELSE NVL(rosters.cohort_start_date, rosters.scheduled_start_date)
        END AS start_date
    , surveys.phase
    , rosters.cohort_name
    , rosters.campus
    , students.institution_name
    , students.institution_type
    , NVL(
        students.first_name || ' ' || students.last_name,
        rosters.first_name || ' ' || rosters.last_name
     ) AS student_name
    , NVL(advisors.educator_name, most_recent_advisor.educator_name) AS advisor_name
    -- Job Seeker Data
    , NVL(coaches.educator_name, gl_cl.coach, most_recent_coach.educator_name) AS coach_name
    -- Standard CSATs
    , instructional_support_csat
    , technical_coach_csat
    , advisor_csat
    , curriculum_csat
    , lecture_csat
    , product_csat
    , onboarding_csat
    , community_connection_csat
    , enterprise_communication_csat
    , enterprise_pacing_csat
    , could_do_differently_text
    -- Phase 1, Onboarding Questions
    , why_fis_text
    , other_options_checkbox
    -- NPS and free-text followup
    , surveys.nps
    , change_one_thing_text
    -- Instructor-specific CSAT Questions
    , NVL(instructors.educator_name, surveys.instructor_name, most_recent_instructor.educator_name) AS instructor_name
    , set_expectations_csat
    , provide_feedback_csat
    , care_about_success_csat
    , inspire_and_motivate_csat
    , demonstrate_knowledge_csat
    , explain_clearly_csat
    , support_and_challenge_csat
    , instructor_most_valuable_text
    , instructor_improve_text
    , instructor_main_reason_text
    -- Phase 5 questions
    , staying_connected_checkbox
    , advice_text
    -- Community Events
    , speaker
    , event_title
    , event_csat
    , change_about_event_text
    , main_takeaway_text

    -- Job Search Surveys
    -- Career Prep CSAT
    , career_prep_csat
    , career_prep_improve_text
    -- Career Workshop
    , workshop_presenter
    , workshop_attended
    , workshop_csat
    , workshop_takeaway_text
    , workshop_anything_else_text
    -- Day 30, 90, 120 CSATs
    , career_coach_support_csat
    , career_coach_feedback_csat
    , career_coach_do_differently_text
    -- Placement or Withdrawal CSAT
    , career_services_most_valuable_text
    , job_search_advice_text
    -- Shared permission question (Phase 5 and End of Job Search)
    , permission_yn
    -- Marketing Feature
    , mkt_career_path
    , mkt_favorite_part
    , mkt_job_search_exp
    , mkt_biggest_takeaway
    , mkt_permission_review_app
FROM 
    (
        SELECT
            -- Survey and student identifiers
            form_id
            , submission_id
            , submission_timestamp
            , survey_name
            , learn_uuid
            , email
            , discipline
            , pace
            , start_date
            , phase::VARCHAR(1)
            -- Standard CSATs
            , instructional_support_csat
            , technical_coach_csat
            , advisor_csat
            , curriculum_csat
            , lecture_csat
            , product_csat
            , onboarding_csat
            , community_connection_csat
            , NULL AS enterprise_communication_csat
            , NULL AS enterprise_pacing_csat
            , could_do_differently_text
            -- Phase 1, Onboarding Questions
            , why_fis_text
            , other_options_checkbox
            -- NPS and free-text followup
            , nps
            , change_one_thing_text
            -- Instructor-specific CSAT Questions
            , NULL AS instructor_name
            , set_expectations_csat
            , provide_feedback_csat
            , care_about_success_csat
            , inspire_and_motivate_csat
            , demonstrate_knowledge_csat
            , explain_clearly_csat
            , support_and_challenge_csat
            , instructor_most_valuable_text
            , instructor_improve_text
            , instructor_main_reason_text
            -- Phase 5 questions
            , staying_connected_checkbox
            , advice_text
            -- Community Events
            , NULL AS speaker
            , NULL AS event_title
            , NULL AS event_csat
            , NULL AS change_about_event_text
            , NULL AS main_takeaway_text

            -- Job Search Surveys
            -- Career Prep CSAT
            , NULL AS career_prep_csat
            , NULL AS career_prep_improve_text
            -- Career Workshop
            , NULL AS workshop_presenter
            , NULL AS workshop_attended
            , NULL AS workshop_csat
            , NULL AS workshop_takeaway_text
            , NULL AS workshop_anything_else_text
            -- Day 30, 90, 120 CSATs
            , NULL AS career_coach_support_csat
            , NULL AS career_coach_feedback_csat
            , NULL AS career_coach_do_differently_text
            -- Placement or Withdrawal CSAT
            , NULL AS career_services_most_valuable_text
            , NULL AS job_search_advice_text
            -- Shared permission question (Phase 5 and End of Job Search)
            , permission_yn
            , NULL AS mkt_career_path
            , NULL AS mkt_favorite_part
            , NULL AS mkt_job_search_exp
            , NULL AS mkt_biggest_takeaway
            , NULL AS mkt_permission_review_app
        FROM {{ params.table_refs["csat_end_of_phase"] }}

        UNION ALL 

        SELECT
            -- Survey and student identifiers
            form_id
            , submission_id
            , submission_timestamp
            , 'Community Event' AS survey_name
            , learn_uuid
            , email
            , discipline
            , pace
            , start_date
            , NULL AS phase
            -- Standard CSATs
            , NULL AS instructional_support_csat
            , NULL AS technical_coach_csat
            , NULL AS advisor_csat
            , NULL AS curriculum_csat
            , NULL AS lecture_csat
            , NULL AS product_csat
            , NULL AS onboarding_csat
            , NULL AS community_connection_csat
            , NULL AS enterprise_communication_csat
            , NULL AS enterprise_pacing_csat
            , NULL AS could_do_differently_text
            -- Phase 1, Onboarding Questions
            , NULL AS why_fis_text
            , NULL AS other_options_checkbox
            -- NPS and free-text followup
            , NULL AS nps
            , NULL AS change_one_thing_text
            -- Instructor-specific CSAT Questions
            , NULL AS instructor_name
            , NULL AS set_expectations_csat
            , NULL AS provide_feedback_csat
            , NULL AS care_about_success_csat
            , NULL AS inspire_and_motivate_csat
            , NULL AS demonstrate_knowledge_csat
            , NULL AS explain_clearly_csat
            , NULL AS support_and_challenge_csat
            , NULL AS instructor_most_valuable_text
            , NULL AS instructor_improve_text
            , NULL AS instructor_main_reason_text
            -- Phase 5 questions
            , NULL AS staying_connected_checkbox
            , NULL AS advice_text
            -- Community Events
            , speaker
            , event_title
            , event_csat
            , change_about_event_text
            , main_takeaway_text

            -- Job Search Surveys
            -- Career Prep CSAT
            , NULL AS career_prep_csat
            , NULL AS career_prep_improve_text
            -- Career Workshop
            , NULL AS workshop_presenter
            , NULL AS workshop_attended
            , NULL AS workshop_csat
            , NULL AS workshop_takeaway_text
            , NULL AS workshop_anything_else_text
            -- Day 30, 90, 120 CSATs
            , NULL AS career_coach_support_csat
            , NULL AS career_coach_feedback_csat
            , NULL AS career_coach_do_differently_text
            -- Placement or Withdrawal CSAT
            , NULL AS career_services_most_valuable_text
            , NULL AS job_search_advice_text
            -- Shared permission question (Phase 5 and End of Job Search)
            , NULL AS permission_yn
            , NULL AS mkt_career_path
            , NULL AS mkt_favorite_part
            , NULL AS mkt_job_search_exp
            , NULL AS mkt_biggest_takeaway
            , NULL AS mkt_permission_review_app
        FROM {{ params.table_refs["csat_community_event"] }}

        UNION ALL 

        SELECT 
        -- Survey and student identifiers
            form_id
            , submission_id
            , submission_timestamp
            , survey_name
            , learn_uuid
            , email
            , discipline
            , pace
            , start_date
            , NULL AS phase
            -- Standard CSATs
            , NULL AS instructional_support_csat
            , NULL AS technical_coach_csat
            , NULL AS advisor_csat
            , NULL AS curriculum_csat
            , NULL AS lecture_csat
            , NULL AS product_csat
            , NULL AS onboarding_csat
            , NULL AS community_connection_csat
            , NULL AS enterprise_communication_csat
            , NULL AS enterprise_pacing_csat
            , NULL AS could_do_differently_text
            -- Phase 1, Onboarding Questions
            , NULL AS why_fis_text
            , NULL AS other_options_checkbox
            -- NPS and free-text followup
            , nps
            , change_one_thing_text
            -- Instructor-specific CSAT Questions
            , NULL AS instructor_name
            , NULL AS set_expectations_csat
            , NULL AS provide_feedback_csat
            , NULL AS care_about_success_csat
            , NULL AS inspire_and_motivate_csat
            , NULL AS demonstrate_knowledge_csat
            , NULL AS explain_clearly_csat
            , NULL AS support_and_challenge_csat
            , NULL AS instructor_most_valuable_text
            , NULL AS instructor_improve_text
            , NULL AS instructor_main_reason_text
            -- Phase 5 questions
            , NULL AS staying_connected_checkbox
            , NULL AS advice_text
            -- Community Events
            , NULL AS speaker
            , NULL AS event_title
            , NULL AS event_csat
            , NULL AS change_about_event_text
            , NULL AS main_takeaway_text

            -- Job Search Surveys
            -- Career Prep CSAT
            , career_prep_csat
            , career_prep_improve_text
            -- Career Workshop
            , workshop_presenter
            , workshop_attended
            , workshop_csat
            , workshop_takeaway_text
            , workshop_anything_else_text
            -- Day 30, 90, 120 CSATs
            , career_coach_support_csat
            , career_coach_feedback_csat
            , career_coach_do_differently_text
            -- Placement or Withdrawal CSAT
            , career_services_most_valuable_text
            , job_search_advice_text
            -- Shared permission question (Phase 5 and End of Job Search)
            , permission_yn
            , mkt_career_path
            , mkt_favorite_part
            , mkt_job_search_exp
            , mkt_biggest_takeaway
            , mkt_permission_review_app
        FROM {{ params.table_refs["csat_job_search"] }} x 

        UNION ALL

        SELECT
            -- Survey and student identifiers
            form_id
            , submission_id
            , submission_timestamp
            , survey_name
            , learn_uuid
            , email
            , discipline
            , pace
            , start_date
            , phase
            -- Standard CSATs
            , instructional_support_csat
            , NULL AS technical_coach_csat
            , NULL AS advisor_csat
            , curriculum_csat
            , lecture_csat
            , NULL AS product_csat
            , NULL AS onboarding_csat
            , NULL AS community_connection_csat
            , NULL AS enterprise_communication_csat
            , NULL AS enterprise_pacing_csat
            , NULL AS could_do_differently_text
            -- Phase 1, Onboarding Questions
            , why_fis_text
            , NULL AS other_options_checkbox
            -- NPS and free-text followup
            , nps
            , change_one_thing_text
            -- Instructor-specific CSAT Questions
            , instructor_name
            , set_expectations_csat
            , provide_feedback_csat
            , care_about_success_csat
            , inspire_and_motivate_csat
            , demonstrate_knowledge_csat
            , explain_clearly_csat
            , support_and_challenge_csat
            , instructor_most_valuable_text
            , instructor_improve_text
            , NULL AS instructor_main_reason_text
            -- Phase 5 questions
            , staying_connected_checkbox
            , advice_text
            -- Community Events
            , NULL AS speaker
            , NULL AS event_title
            , NULL AS event_csat
            , NULL AS change_about_event_text
            , NULL AS main_takeaway_text

            -- Job Search Surveys
            -- Career Prep CSAT
            , NULL AS career_prep_csat
            , NULL AS career_prep_improve_text
            -- Career Workshop
            , NULL AS workshop_presenter
            , NULL AS workshop_attended
            , NULL AS workshop_csat
            , NULL AS workshop_takeaway_text
            , NULL AS workshop_anything_else_text
            -- Day 30, 90, 120 CSATs
            , NULL AS career_coach_support_csat
            , NULL AS career_coach_feedback_csat
            , NULL AS career_coach_do_differently_text
            -- Placement or Withdrawal CSAT
            , NULL AS career_services_most_valuable_text
            , NULL AS job_search_advice_text
            -- Shared permission question (Phase 5 and End of Job Search)
            , permission_yn
            , NULL AS mkt_career_path
            , NULL AS mkt_favorite_part
            , NULL AS mkt_job_search_exp
            , NULL AS mkt_biggest_takeaway
            , NULL AS mkt_permission_review_app
        FROM {{ params.table_refs["historic_nps"] }}

        UNION ALL 

        SELECT
            -- Survey and student identifiers
            form_id
            , submission_id
            , submission_timestamp
            , survey_name
            , learn_uuid
            , email
            , discipline
            , pace
            , start_date
            , NULL AS phase
            -- Standard CSATs
            , instructional_support_csat
            , NULL AS technical_coach_csat
            , NULL AS advisor_csat
            , curriculum_csat
            , lecture_csat
            , product_csat
            , NULL AS onboarding_csat
            , NULL AS community_connection_csat
            , communication_csat AS enterprise_communication_csat
            , pacing_csat AS enterprise_pacing_csat
            , change_one_thing_text AS could_do_differently_text
            -- Phase 1, Onboarding Questions
            , NULL AS why_fis_text
            , NULL AS other_options_checkbox
            -- NPS and free-text followup
            , nps
            , change_one_thing_text
            -- Instructor-specific CSAT Questions
            , NULL AS instructor_name
            , set_expectations_csat
            , provide_feedback_csat
            , care_about_success_csat
            , inspire_and_motivate_csat
            , demonstrate_knowledge_csat
            , explain_clearly_csat
            , support_and_challenge_csat
            , instructor_most_valuable_text
            , instructor_improve_text
            , instructor_main_reason_text
            -- Phase 5 questions
            , NULL AS staying_connected_checkbox
            , NULL AS advice_text
            -- Community Events
            , NULL AS speaker
            , NULL AS event_title
            , NULL AS event_csat
            , NULL AS change_about_event_text
            , NULL AS main_takeaway_text

            -- Job Search Surveys
            -- Career Prep CSAT
            , NULL AS career_prep_csat
            , NULL AS career_prep_improve_text
            -- Career Workshop
            , NULL AS workshop_presenter
            , NULL AS workshop_attended
            , NULL AS workshop_csat
            , NULL AS workshop_takeaway_text
            , NULL AS workshop_anything_else_text
            -- Day 30, 90, 120 CSATs
            , NULL AS career_coach_support_csat
            , NULL AS career_coach_feedback_csat
            , NULL AS career_coach_do_differently_text
            -- Placement or Withdrawal CSAT
            , NULL AS career_services_most_valuable_text
            , NULL AS job_search_advice_text
            -- Shared permission question (Phase 5 and End of Job Search)
            , NULL AS permission_yn
            , NULL AS mkt_career_path
            , NULL AS mkt_favorite_part
            , NULL AS mkt_job_search_exp
            , NULL AS mkt_biggest_takeaway
            , NULL AS mkt_permission_review_app
        FROM {{ params.table_refs["csat_enterprise"] }}
    ) AS surveys
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} instructors
    ON instructors.educator_type = 'instructor'
    AND surveys.learn_uuid = instructors.student_uuid
    AND surveys.submission_timestamp BETWEEN instructors.created_at AND NVL(instructors.removed_at, CURRENT_TIMESTAMP)
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} most_recent_instructor
    ON most_recent_instructor.educator_type = 'instructor'
    AND surveys.learn_uuid = most_recent_instructor.student_uuid
    AND most_recent_instructor.is_most_recent_instructor
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} coaches
    ON coaches.educator_type = 'coach'
    AND surveys.learn_uuid = coaches.student_uuid
    AND surveys.submission_timestamp BETWEEN coaches.created_at AND NVL(coaches.removed_at, CURRENT_TIMESTAMP)
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} most_recent_coach
    ON most_recent_coach.educator_type = 'coach'
    AND surveys.learn_uuid = most_recent_coach.student_uuid
    AND most_recent_coach.is_most_recent_coach
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} advisors
    ON advisors.educator_type = 'advisor'
    AND surveys.learn_uuid = advisors.student_uuid
    AND surveys.submission_timestamp BETWEEN advisors.created_at AND NVL(advisors.removed_at, CURRENT_TIMESTAMP)
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} most_recent_advisor
    ON most_recent_advisor.educator_type = 'advisor'
    AND surveys.learn_uuid = most_recent_advisor.student_uuid
    AND most_recent_advisor.is_most_recent_advisor
LEFT JOIN 
    {{ params.table_refs["fis.rosters"] }} rosters
    ON surveys.learn_uuid = rosters.learn_uuid
    AND surveys.submission_timestamp BETWEEN rosters.added_to_cohort_date AND NVL(rosters.removed_from_cohort_date, CURRENT_TIMESTAMP)
LEFT JOIN 
    {{ params.table_refs["gradleaders.change_log"] }} gl_cl
    ON surveys.learn_uuid = gl_cl.learn_uuid
    AND surveys.submission_timestamp::DATE = gl_cl.snapshot_date::DATE
LEFT JOIN 
    {{ params.table_refs["fis.students"] }} students 
    ON surveys.learn_uuid = students.learn_uuid
WHERE 
    surveys.email !~ 'flatironschool\\.com'
{% endblock %}
