{% extends "rotate_table.sql" %}
{% block query %}
WITH job_searches AS (
    SELECT 
        job_searches.id
        , job_searches.created_at
        , job_searches.student_id
        , student_users.learn_uuid
        , student_users.first_name || ' ' || student_users.last_name AS student_name
        , job_searches.coach_id
        , coach_users.learn_uuid AS coach_learn_uuid
        , coach_users.first_name || ' ' || coach_users.last_name AS coach_name
        , job_searches.start_date 
        , json_extract_path_text(job_searches.survey_response,'goals') AS goals_after_completion
        , json_extract_path_text(job_searches.survey_response,'citizenship_countries') AS citizenship_countries
        , json_extract_path_text(job_searches.survey_response,'authorized_countries') AS authorized_countries
        , json_extract_path_text(job_searches.survey_response,'work_authorization') AS authroized_us_uk
        , json_extract_path_text(job_searches.survey_response,'flatiron_program') AS program
        , json_extract_path_text(job_searches.survey_response,'uiux_focus_areas') AS ui_focus
        , json_extract_path_text(job_searches.survey_response,'education') AS ed_level
        , json_extract_path_text(job_searches.survey_response,'college_graduation_date') AS college_graduation_date
        , json_extract_path_text(job_searches.survey_response,'education_concentration') AS ed_concentration
        , json_extract_path_text(job_searches.survey_response,'programming_skills') AS programming_skills
        , json_extract_path_text(job_searches.survey_response,'work_environment') AS work_size_environ
        , json_extract_path_text(job_searches.survey_response,'job_search_nervousness') AS job_search_nervousness
        , json_extract_path_text(job_searches.survey_response,'job_search_barrier') AS job_search_barriers
        , json_extract_path_text(job_searches.survey_response,'people_connector')  AS people_connector
        , json_extract_path_text(job_searches.survey_response,'interview_feeling') AS interview_feeling    
        , json_extract_path_text(job_searches.survey_response,'current_location') AS current_location
        , json_extract_path_text(job_searches.survey_response,'relocation') AS relocation_cities
        , json_extract_path_text(job_searches.survey_response,'relocate_anywhere') relocation_anywhere
        , json_extract_path_text(job_searches.survey_response,'relocate_non_major_city') AS relocation_non_major_city
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'finance') AS industry_finance
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'nonProfit') AS industry_nonprofit
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'healthcare') AS industry_healthcare
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'creative') AS industry_creative
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'marketing') AS industry_marketing
        , json_extract_path_text(json_extract_path_text(job_searches.survey_response,'career_industry'),'hardware') AS industry_hardware
        , json_extract_path_text(job_searches.survey_response,'other') AS other_info
    FROM  
        (
            SELECT *, COUNT(1) OVER (PARTITION BY student_id) ct
            FROM {{ params.table_refs["learn.job_searches"] }} 
        ) job_searches
    LEFT JOIN 
        {{ params.table_refs["learn.users"] }} student_users
        ON job_searches.student_id = student_users.id
    LEFT JOIN 
        {{ params.table_refs["learn.users"] }} coach_users 
        ON job_searches.coach_id = coach_users.id
    WHERE 
        archived_at IS NULL AND (survey_response <> '{}' OR ct = 1)
)
, job_search_events AS (
    SELECT 
        job_search_id
        , MAX(CASE WHEN type = 'ResumeDraft' THEN url END) AS resume_link
        , MAX(CASE WHEN type = 'JourneyKickoff' THEN status END) AS journey_kickoff_status
        , MAX(CASE WHEN type = 'JourneyKickoff' THEN acknowledged_at END) AS journey_kickoff_acknowledged_at
        , MAX(CASE WHEN type = 'JourneyKickoff' THEN completed_at END) AS journey_kickoff_completed_at
        , MAX(CASE WHEN type = 'CareerSurvey' THEN status END) AS career_survey_status
        , MAX(CASE WHEN type = 'CareerSurvey' THEN acknowledged_at END) AS career_survey_acknowledged_at
        , MAX(CASE WHEN type = 'CareerSurvey' THEN completed_at END) AS career_survey_completed_at
        , MAX(CASE WHEN type = 'KickoffMeeting' THEN status END) AS kickoff_meeting_status
        , MAX(CASE WHEN type = 'KickoffMeeting' THEN acknowledged_at END) AS kickoff_meeting_acknowledged_at
        , MAX(CASE WHEN type = 'KickoffMeeting' THEN completed_at END) AS kickoff_meeting_completed_at
        , MAX(CASE WHEN type = 'ResumeDraft' THEN status END) AS resume_draft_status
        , MAX(CASE WHEN type = 'ResumeDraft' THEN acknowledged_at END) AS resume_draft_acknowledged_at
        , MAX(CASE WHEN type = 'ResumeDraft' THEN completed_at END) AS resume_draft_completed_at
        , MAX(CASE WHEN type = 'ResumeReview' THEN status END) AS resume_review_status
        , MAX(CASE WHEN type = 'ResumeReview' THEN acknowledged_at END) AS resume_review_acknowledged_at
        , MAX(CASE WHEN type = 'ResumeReview' THEN completed_at END) AS resume_review_completed_at
        , MAX(CASE WHEN type = 'PersonalBrandAssessment' THEN status END) AS personal_brand_assessment_status
        , MAX(CASE WHEN type = 'PersonalBrandAssessment' THEN acknowledged_at END) AS personal_brand_assessment_acknowledged_at
        , MAX(CASE WHEN type = 'PersonalBrandAssessment' THEN completed_at END) AS personal_brand_assessment_completed_at
        , MAX(CASE WHEN type = 'MockCulturalInterview' THEN status END) AS mock_cultural_interview_status
        , MAX(CASE WHEN type = 'MockCulturalInterview' THEN acknowledged_at END) AS mock_cultural_interview_acknowledged_at
        , MAX(CASE WHEN type = 'MockCulturalInterview' THEN completed_at END) AS mock_cultural_interview_completed_at
        , MAX(CASE WHEN type = 'MockTechnicalInterview' THEN status END) AS mock_technical_interview_status
        , MAX(CASE WHEN type = 'MockTechnicalInterview' THEN acknowledged_at END) AS mock_technical_interview_acknowledged_at
        , MAX(CASE WHEN type = 'MockTechnicalInterview' THEN completed_at END) AS mock_technical_interview_completed_at
        , MAX(CASE WHEN type = 'PreGraduationMeeting' THEN status END) AS pregraduation_meeting_status
        , MAX(CASE WHEN type = 'PreGraduationMeeting' THEN acknowledged_at END) AS pregraduation_meeting_acknowledged_at
        , MAX(CASE WHEN type = 'PreGraduationMeeting' THEN completed_at END) AS pregraduation_meeting_completed_at
        , MAX(CASE WHEN type = 'GraduateCoachingMeeting' THEN status END) AS graduate_coaching_meeting_status
        , MAX(CASE WHEN type = 'GraduateCoachingMeeting' THEN acknowledged_at END) AS graduate_coaching_meeting_acknowledged_at
        , MAX(CASE WHEN type = 'GraduateCoachingMeeting' THEN completed_at END) AS graduate_coaching_meeting_completed_at
        , MAX(CASE WHEN type = 'NetworkingCoachingMeeting' THEN status END) AS networking_coaching_meeting_status
        , MAX(CASE WHEN type = 'NetworkingCoachingMeeting' THEN acknowledged_at END) AS networking_coaching_meeting_acknowledged_at
        , MAX(CASE WHEN type = 'NetworkingCoachingMeeting' THEN completed_at END) AS networking_coaching_meeting_completed_at
        , MAX(CASE WHEN type = 'JobAcceptanceSurvey' THEN status END) AS job_acceptance_survey_status
        , MAX(CASE WHEN type = 'JobAcceptanceSurvey' THEN acknowledged_at END) AS job_acceptance_survey_acknowledged_at
        , MAX(CASE WHEN type = 'JobAcceptanceSurvey' THEN completed_at END) AS job_acceptance_survey_completed_at
        , MAX(CASE WHEN type = 'ExitInterview' THEN status END) AS exit_interview_status
        , MAX(CASE WHEN type = 'ExitInterview' THEN acknowledged_at END) AS exit_interview_acknowledged_at
        , MAX(CASE WHEN type = 'ExitInterview' THEN completed_at END) AS exit_interview_completed_at
    FROM {{ params.table_refs["learn.career_steps"] }}
    GROUP BY job_search_id
)

SELECT 
    job_searches.id
    , job_searches.created_at
    , job_searches.student_id
    , job_searches.learn_uuid
    , job_searches.student_name
    , job_searches.coach_id
    , job_searches.coach_learn_uuid
    , job_searches.coach_name
    , job_searches.start_date 
    , job_searches.goals_after_completion
    , job_searches.citizenship_countries
    , job_searches.authorized_countries
    , job_searches.authroized_us_uk
    , job_searches.program
    , job_searches.ui_focus
    , job_searches.ed_level
    , job_searches.college_graduation_date
    , job_searches.ed_concentration
    , job_searches.programming_skills
    , job_searches.work_size_environ
    , job_searches.job_search_nervousness
    , job_searches.job_search_barriers
    , job_searches.people_connector
    , job_searches.interview_feeling    
    , job_searches.current_location
    , job_searches.relocation_cities
    , job_searches.relocation_anywhere
    , job_searches.relocation_non_major_city
    , job_searches.industry_finance
    , job_searches.industry_nonprofit
    , job_searches.industry_healthcare
    , job_searches.industry_creative
    , job_searches.industry_marketing
    , job_searches.industry_hardware
    , job_searches.other_info

    -- Job Search Events
    , job_search_events.resume_link
    , job_search_events.journey_kickoff_status
    , job_search_events.journey_kickoff_acknowledged_at
    , job_search_events.journey_kickoff_completed_at
    , job_search_events.career_survey_status
    , job_search_events.career_survey_acknowledged_at
    , job_search_events.career_survey_completed_at
    , job_search_events.kickoff_meeting_status
    , job_search_events.kickoff_meeting_acknowledged_at
    , job_search_events.kickoff_meeting_completed_at
    , job_search_events.resume_draft_status
    , job_search_events.resume_draft_acknowledged_at
    , job_search_events.resume_draft_completed_at
    , job_search_events.resume_review_status
    , job_search_events.resume_review_acknowledged_at
    , job_search_events.resume_review_completed_at
    , job_search_events.personal_brand_assessment_status
    , job_search_events.personal_brand_assessment_acknowledged_at
    , job_search_events.personal_brand_assessment_completed_at
    , job_search_events.mock_cultural_interview_status
    , job_search_events.mock_cultural_interview_acknowledged_at
    , job_search_events.mock_cultural_interview_completed_at
    , job_search_events.mock_technical_interview_status
    , job_search_events.mock_technical_interview_acknowledged_at
    , job_search_events.mock_technical_interview_completed_at
    , job_search_events.pregraduation_meeting_status
    , job_search_events.pregraduation_meeting_acknowledged_at
    , job_search_events.pregraduation_meeting_completed_at
    , job_search_events.graduate_coaching_meeting_status
    , job_search_events.graduate_coaching_meeting_acknowledged_at
    , job_search_events.graduate_coaching_meeting_completed_at
    , job_search_events.networking_coaching_meeting_status
    , job_search_events.networking_coaching_meeting_acknowledged_at
    , job_search_events.networking_coaching_meeting_completed_at
    , job_search_events.job_acceptance_survey_status
    , job_search_events.job_acceptance_survey_acknowledged_at
    , job_search_events.job_acceptance_survey_completed_at
    , job_search_events.exit_interview_status
    , job_search_events.exit_interview_acknowledged_at
    , job_search_events.exit_interview_completed_at

    -- Notes (Blurbs/Opportunities)
    , blurbs.blurbs
    , opportunities.opportunity

    -- Job Acceptance Survey  
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'gender', true) AS job_acceptance_survey_gender
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_location_city', true) AS job_acceptance_survey_job_location_city
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_location_state', true) AS job_acceptance_survey_job_location_state
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'company_size', true) AS job_acceptance_survey_company_size
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_acceptance_date', true) AS job_acceptance_survey_job_acceptance_date
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'company_name', true) AS job_acceptance_survey_company_name
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_title', true) AS job_acceptance_survey_job_title
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_structure', true) AS job_acceptance_survey_job_structure
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_function', true) AS job_acceptance_survey_job_function
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'compensation', true) AS job_acceptance_survey_compensation
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'compensation_unit', true) AS job_acceptance_survey_compensation_unit
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'hours_per_week', true) AS job_acceptance_survey_hours_per_week
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_duration', true) AS job_acceptance_survey_job_duration
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_duration_unit', true) AS job_acceptance_survey_job_duration_unit
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'graduation_date', true) AS job_acceptance_survey_graduation_date
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'job_search_start_date', true) AS job_acceptance_survey_job_search_start_date
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'education', true) AS job_acceptance_survey_education
    , JSON_EXTRACT_PATH_TEXT(job_acceptance_survey.response, 'enrolled_program', true) AS job_acceptance_survey_enrolled_program
FROM 
    job_searches
LEFT JOIN 
    job_search_events 
    ON job_searches.id = job_search_events.job_search_id
LEFT JOIN 
    (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_search_id ORDER BY updated_at DESC) AS row_num 
        FROM {{ params.table_refs["learn.job_acceptance_survey_responses"] }}
    ) AS job_acceptance_survey
    ON job_searches.id = job_acceptance_survey.job_search_id
    AND job_acceptance_survey.row_num = 1
LEFT JOIN 
    (
        SELECT
            student_learn_uuid
            , LISTAGG(body, '\n\n') AS blurbs
        FROM {{ params.table_refs["student_notes"] }}
        WHERE body LIKE '**BLURB**%'
        GROUP BY student_learn_uuid
    ) AS blurbs
    ON job_searches.learn_uuid = blurbs.student_learn_uuid
LEFT JOIN 
    (
        SELECT
            student_learn_uuid
            , body AS opportunity
            , ROW_NUMBER() OVER (PARTITION BY student_learn_uuid ORDER BY created_date DESC) AS row_num
        FROM {{ params.table_refs["student_notes"] }}
        WHERE body LIKE '**OPPORTUNITIES**%'
    ) AS opportunities
    ON job_searches.learn_uuid = opportunities.student_learn_uuid
    AND opportunities.row_num = 1
{% endblock %}