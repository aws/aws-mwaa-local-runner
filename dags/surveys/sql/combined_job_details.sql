{% extends "rotate_table.sql" %}
{% block query %}
SELECT *
FROM (
    SELECT
        submission_id
        , submission_timestamp
        , 'formstack' AS source
        , learn_uuid
        , company_name
        , company_size
        , work_location
        , job_city
        , job_state
        , job_title
        , compensation
        , currency
        , compensation_unit
        , expected_hrs_per_week
        , expected_duration
        , duration_unit
        , job_function
        , compensation_change
        , annual_comp_before_flatiron
        , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY submission_timestamp DESC) as rn
    FROM {{ params.table_refs["formstack_job_details"] }}
    WHERE approval_status = 'Approved' AND job_structure <> 'Non-Qualifying Job'

    UNION ALL

    SELECT
        offer_id
        , offer_offer_created_date::TIMESTAMP
        , 'gradleaders' AS source
        , offer_offer_created_by
        , offer_offer_organization_name
        , offer_company_size
        , offer_which_of_these_best_describes_your_expected_work_location
        , offer_job_location_city
        , offer_job_location_state
        , offer_offer_job_title
        , offer_offer_base_salary
        , offer_currency
        , offer_unit_of_time_for_compensation_amount_you_entered
        , offer_expected_number_of_hours_per_week
        , offer_expected_job_duration
        , offer_unit_of_time_for_expected_job_duration
        , offer_job_function
        , offer_will_your_annual_individual_compensation_change_with_the_start_of_your_new_position
        , pre_fis_compensation
        , ROW_NUMBER() OVER (PARTITION BY offer_offer_created_by ORDER BY offer_offer_date_offer_received::timestamp DESC) as rn
    FROM fis.gradleaders_offer_survey gos
    /* 
    This dataset was imported one time via the 20220608-gl-offers.csv 
    within this compressed folder:
    https://drive.google.com/file/d/1tcTuAivW4oPsjNe8Df3Xo2UJ0LxzU-14/view?usp=sharing

    It now exists in s3://fis-deprecated-data-sources
   
    This CSV was the bulk export when migrating off of Gradleaders, 
    after all data entry in the platform stopped
    */
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY offer_created_date DESC) AS row_num
        FROM gradleaders.student_profiles
    ) sp
    ON gos.offer_offer_created_by = sp.learn_uuid
    AND sp.row_num = 1
)
WHERE rn = 1
{% endblock %}
