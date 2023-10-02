{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , approval_status
    , learn_uuid
    , email
    , discipline
    , institution_type
    , company_name
    , company_size
    , work_location
    , job_city
    , job_state
    , job_zip_code
    , employer_phone_number
    , job_acceptance_date
    , job_start_date
    , job_title
    , job_function
    , currency
    , compensation
    , compensation_unit
    , predetermined_end_date
    , expected_duration
    , duration_unit
    , job_end_date
    , expected_hrs_per_week
    , employee_or_freelance
    , job_structure
    , first_connected_w_company
    , compensation_change
    , annual_comp_before_flatiron
    , prior_job
    , offer_letter_checkbox
    , validation_checkbox
    , privacy_policy_checkbox
FROM
    (
        SELECT form_id, submission_id, 'Job Details' AS survey_name, submission_timestamp, approval_status, label, value 
        FROM {{ params.table_refs["formstack.job_details_merged"] }}
    ) PIVOT (
        MAX(value) for label IN (
            'Learn UUID' AS learn_uuid
            , 'Email' AS email
            , 'Discipline' AS discipline
            , 'Institution Type' AS institution_type
            , 'Company Name' AS company_name
            , 'Company Size' AS company_size
            , 'Which of these best describes your expected work location?' AS work_location
            , 'Job Location (City)' AS job_city
            , 'Job Location (State)' AS job_state
            , 'Job Location: Zip Code' AS job_zip_code
            , 'Employer''s Telephone Number' AS employer_phone_number
            , 'Job Acceptance Date' AS job_acceptance_date
            , 'Job Start Date' AS job_start_date
            , 'Job Title' AS job_title
            , 'Job Function' AS job_function
            , 'Currency' AS currency
            , 'Compensation' AS compensation
            , 'Compensation Unit' AS compensation_unit
            , 'Does your employment have a predetermined end date?' AS predetermined_end_date
            , 'What is the expected job duration?' AS expected_duration
            , 'Job Duration Unit' AS duration_unit
            , 'What is the expected end date?' AS job_end_date
            , 'What is the expected number of hours per week?' AS expected_hrs_per_week
            , 'Are you an employee or a freelance consultant?' AS employee_or_freelance
            , 'Therefore, my job structure will be reported as:' AS job_structure
            , 'How did you first get connected to this company?' AS first_connected_w_company
            , 'Will your annual individual compensation change with the start of your new position?' AS compensation_change
            , '[Optional] If you are willing to share, what was your annual individual compensation prior to Flatiron School?' AS annual_comp_before_flatiron
            , 'What was your last job title prior to Flatiron School (if you were employed)?' AS prior_job
            , 'Offer Letter' AS offer_letter_checkbox
            , 'Validation' AS validation_checkbox
            , 'Privacy Policy' AS privacy_policy_checkbox
        )
    )
{% endblock %}
