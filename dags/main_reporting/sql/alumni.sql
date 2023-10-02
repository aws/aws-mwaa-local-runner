{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    -- Student Info
    students.learn_uuid
    , students.full_name
    , students.first_name
    , students.last_name
    , students.email
    , students.phone_number
    , students.age
    , students.state_of_residency
    , NULL AS gender
    , NULL AS current_city
    , students.institution_name
    , students.institution_type

    -- Program and Graduation Info
    , students.most_recent_cohort_name AS cohort
    , students.most_recent_cohort_campus AS campus
    , students.most_recent_cohort_discipline AS discipline
    , students.most_recent_cohort_modality AS modality
    , students.most_recent_cohort_pacing AS pacing
    , students.most_recent_cohort_start_date AS cohort_start_date
    , NULL AS mbg_possible
    , NULL AS ready_for_coaching_date
    , students.graduation_date
    , students.graduation_updated_at
    , NULL AS graduation_action_taken_date

    -- Job Seeking Info
    , NULL AS job_tracker_url
    , students.tech_score
    , students.citizenship_score
    , NULL analytical_score
    , NULL AS job_seeking_status
    , NULL AS job_seeking_status_update_date
    , NULL AS alumni_seeking_status
    , NULL AS interview_availability
    , NULL AS money_back_guarantee_status
    , NULL AS money_back_guarantee_status_update_date
    , NULL AS job_search_start_date
    , NULL AS days_from_grad_date_to_jssd
    , NULL AS days_job_seeking
    , NULL AS days_to_placement

    -- Job Offer/Placement Info
    , NULL AS placement_date
    , NULL AS withdrawal_date
    , NULL AS offer_date 
    , NULL AS offer_created_date
    , NULL AS company_name
    , NULL AS job_title
    , NULL AS job_city
    , NULL AS job_state
    , NULL AS job_start_date
    , NULL AS job_function
    , NULL AS job_structure
    , NULL AS raw_salary
    , NULL AS wage_amount
    , NULL AS wage_period
    , NULL AS job_details_survey_received
    , NULL AS offer_letter_received

    -- Career Prep Progress
    , career_prep.total_lessons_complete
    , career_prep.perc_complete

    -- Career Coach Info
    , students.coach_uuid
    , students.coach_name
    , students.coach_email
    , students.coach_assigned_date
    , students.is_active_coach_assignment
    
    -- Links and Accounts
    , NULL AS learn_resume_link

    -- Job Search Feedback
    , jsf.timestamp AS job_search_feedback_timestamp
    , jsf.coach_name AS job_search_feedback_coach
    , jsf.coach_value_add AS job_search_feedback_coach_value_add    
    , jsf.coach_provide_feedback AS job_search_feedback_coach_provide_feedback    
    , jsf.coach_improves_candidate AS job_search_feedback_coach_improves_candidate   
    , jsf.coach_nps AS job_search_feedback_coach_nps    
    , jsf.most_valuable_experience AS job_search_feedback_most_valuable_experience    
    , jsf.day_1_advice AS job_search_feedback_day_1_advice    
    , jsf.change_1_thing AS job_search_feedback_change_1_thing    
    , jsf.speak_to_prospective_students_yesno AS job_search_feedback_speak_to_prospective_students_yesno    
    , jsf.share_on_social_media_yesno AS job_search_feedback_share_on_social_media_yesno
    , jsf.social_handles AS job_search_feedback_social_handles    
    , jsf.flatiron_nps AS job_search_feedback_flatiron_nps    
    , jsf.additional_thoughts AS job_search_feedback_additional_thoughts    
    , jsf.coaching_manager AS job_search_feedback_coaching_manager  
FROM
    {{ params.table_refs["students"] }} students
LEFT JOIN 
    (
        SELECT 
            *, ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY timestamp DESC) AS row_num 
        FROM {{ params.table_refs["career_services.job_search_feedback"] }} 
    ) AS jsf
    ON students.learn_uuid = jsf.learn_uuid
    AND jsf.row_num = 1
LEFT JOIN 
    {{ params.table_refs["career_prep_progress"] }} AS career_prep 
    ON students.learn_uuid = career_prep.learn_uuid
WHERE
    -- Table should only include graduated, placed, or coach-assigned students
    students.graduation_date IS NOT NULL 
    OR EXISTS (
        SELECT 1 FROM career_services.alumni_job_placements ajp 
        WHERE students.learn_uuid = ajp.learn_uuid
    )
    OR students.coach_assigned_date IS NOT NULL
{% endblock %}
