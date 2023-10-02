{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    GETDATE() AS run_date
   
    /*** Student IDs and relevant timestamps ***/
    , learn_users.learn_uuid 
    , external_ids.external_id
    , learn_users.created_at AS user_created_at
    , learn_users.last_seen_at AS user_last_active_date
    , NVL(most_recent_cohort.account_id, admissions.account_id) AS account_id
   
    /*** Student PII ***/
    , NVL(learn_users.last_name, base_students.last_name) AS last_name
    , NVL(learn_users.first_name, base_students.first_name) AS first_name
    , NVL(
        NVL(learn_users.first_name, base_students.first_name) || ' ' || NVL(learn_users.last_name, base_students.last_name),
        most_recent_cohort.account_name
    ) AS full_name
    , learn_users.email
    , INITCAP(NVL(NULLIF(institutions.type, 'default'), 'Consumer')) AS institution_type
    , NVL2(institutions.name, institutions.name, 'Flatiron School') AS institution_name
    , admissions.phone_number
    , admissions.date_of_birth
    , EXTRACT(YEARS FROM CURRENT_DATE - admissions.date_of_birth) AS age
    , NULL AS gender
    , admissions.state_of_residency
    , admissions.mailing_street
    , admissions.mailing_city
    , admissions.mailing_state
    , admissions.mailing_postal_code
    , admissions.mailing_country
    , admissions.billing_street
    , admissions.billing_city
    , admissions.billing_state
    , admissions.billing_postal_code
    , admissions.billing_country
    , NULL AS academic_zip_code
    , NULL AS finance_last_name
    , NULL AS finance_first_name
    , learn_users.finance_email
    , NULL AS slack_username
    , github_accounts.username AS github_username
    , NULL AS learn_profile_location
    , NULL AS learn_resume_link
    , admissions.applicant_dma AS student_dma
    , admissions.msa_code
    /*** EA Address ***/
    , ea_addresses.home_address
    , ea_addresses.home_city
    , ea_addresses.home_state
    , ea_addresses.home_zip
   
    /*** Marketing/Admissions data ***/
    , admissions.lead_created_date
    , admissions.lead_source
    , admissions.lead_status
    , admissions.lead_score
    , admissions.clean_marketing_source AS marketing_channel
    , admissions.lead_postal_code AS marketing_postal_code -- or should i use marketo/hubspot location data?
    , admissions.application_submission_date
    , admissions.application_postal_code
    , admissions.application_state
    , admissions.application_score
    , admissions.lead_owner_name
    , admissions.opportunity_owner_name
    , admissions.owner_name
    , admissions.admissions_interview_date_time
    , admissions.technical_interview_date_time
    , admissions.technical_interview_score
    , admissions.technical_interviewer_name
    , admissions.original_admitted_date
    , admissions.admitted_date
    , admissions.committed_date
    , admissions.onboarding_cleared_to_start
    , admissions.most_recent_closed_won_date AS closed_won_date
    
    /*** Registrar Finance/Tuition Data ***/
    , admissions.scholarship_letter_sent_date
    , admissions.scholarship_letter_signed_date
    , admissions.scholarship_code_used
    , admissions.scholarship_amount
    , admissions.scholarship_status
    

    

    , NULL AS finance_start_date -- first payment date?
    , NULL AS finance_end_date -- last payment date?

    /*** Historic Financial Data from Learn/SFDC ***/
    , admissions.deposit_amount AS sf_deposit_amount
    , admissions.deposit_received_date AS sf_deposit_received_date
    , admissions.financing_method AS sf_financing_method
    , admissions.financing_status AS sf_financing_status
    , admissions.financing_partner AS sf_financing_partner
    , admissions.scholarship_status AS sf_scholarship_status
    , admissions.gross_tuition_amount AS sf_gross_tuition_amount
    , admissions.paid_tuition AS sf_paid_tuition
    , admissions.balance_amount AS sf_balance_amount
    , admissions.balance_due_amount AS sf_balance_due_amount
    , admissions.loan_amount AS sf_loan_amount
    , NULL AS sf_monthly_recurring_payment
    , stripe_payments.amount_paid AS stripe_amount_paid
    , stripe_payments.most_recent_payment_date AS stripe_most_recent_payment_date
    
    /*** Academic Data ***/
    , prework.prework_started_at AS prework_start_date
    , prework.last_prework_activity_date
    , prework.completed_prework_lessons / NULLIF(prework.total_prework_lessons::FLOAT, 0) AS prework_completed_pct
    
    -- Originally erolled cohort
    , original_cohort.uuid AS original_cohort_uuid
    , original_cohort.name AS original_cohort_name
    , original_cohort.start_date AS original_cohort_start_date
    , original_cohort.end_date AS original_cohort_end_date
    , original_cohort.campus AS original_cohort_campus
    , original_cohort.modality AS original_cohort_modality
 	, original_cohort.discipline AS original_cohort_discipline
    , original_cohort.pacing AS original_cohort_pacing

    -- Matriculated cohort
    , matriculated.cohort_uuid AS matriculated_cohort_uuid
    , matriculated.cohort_name AS matriculated_cohort_name
    , matriculated.cohort_start_date AS matriculated_cohort_start_date
    , matriculated.cohort_end_date AS matriculated_cohort_end_date
    , matriculated.campus AS matriculated_cohort_campus
    , matriculated.modality AS matriculated_cohort_modality
 	, matriculated.discipline AS matriculated_cohort_discipline
    , matriculated.pacing AS matriculated_cohort_pacing
    , matriculated.day_1_start_date AS matriculation_date
    , matriculated.online_day_1_start AS online_matriculation_date
    , matriculated.on_roster_day_15 AS on_roster_day_15

    -- Most recent cohort
    {% set roster_columns = ti.xcom_pull(task_ids=["query_roster_columns"], key="columns")[0] %}
    {% for column in roster_columns %}
        {% if column in params.non_most_recent %}
            {% if params.non_most_recent[column].get("sql") %}
            , {{ params.non_most_recent[column]["sql"] }}
            {% elif params.non_most_recent[column].get("alias") %}
            , most_recent_cohort.{{ column }} AS {{ params.non_most_recent[column]["alias"] }}
            {% else %}
            , most_recent_cohort.{{ column }}
            {% endif %}
        {% else %}
            {% if 'cohort' == column[:6] %}
            , most_recent_cohort.{{ column }} most_recent_{{ column }}
            {% else %}
            , most_recent_cohort.{{ column }} most_recent_cohort_{{ column }}
            {% endif %}
        {% endif %}
    {% endfor %}

    , NULL AS academic_overall_attendance
    , NULL AS nps
    , matriculated_instructor.educator_uuid AS matriculated_instructor_uuid
    , matriculated_instructor.educator_name AS matriculated_instructor_name
    , matriculated_instructor.educator_email AS matriculated_instructor_email
    , most_recent_instructor.educator_uuid AS instructor_uuid
    , most_recent_instructor.educator_name AS instructor_name
    , most_recent_instructor.educator_email AS instructor_email
    , most_recent_instructor.removed_at IS NULL AS is_active_instructor_assignment
    , most_recent_advisor.educator_uuid AS advisor_uuid
    , most_recent_advisor.educator_name AS advisor_name
    , most_recent_advisor.educator_email AS advisor_email
    , most_recent_advisor.created_at AS advisor_assigned_date
    , most_recent_advisor.removed_at IS NULL AS is_active_advisor_assignment
    , most_recent_coach.educator_uuid AS coach_uuid
    , most_recent_coach.educator_name AS coach_name
    , most_recent_coach.educator_email AS coach_email
    , most_recent_coach.created_at AS coach_assigned_date
    , most_recent_coach.removed_at IS NULL AS is_active_coach_assignment
    
    , CASE
        WHEN grades.phase_1_pass_date IS NOT NULL THEN grades.phase_1_pass_date
        WHEN NVL(
            grades.phase_2_pass_date
            , grades.phase_3_pass_date
            , grades.phase_4_pass_date
            , grades.phase_5_pass_date
            , most_recent_cohort.graduation_date) IS NOT NULL
        THEN '1900-01-01'::DATE
        END AS phase_1_pass_date
    , CASE
        WHEN grades.phase_2_pass_date IS NOT NULL THEN grades.phase_2_pass_date
        WHEN NVL(
            grades.phase_3_pass_date
            , grades.phase_4_pass_date
            , grades.phase_5_pass_date
            , most_recent_cohort.graduation_date) IS NOT NULL
        THEN '1900-01-01'::DATE
        END AS phase_2_pass_date
    , CASE
        WHEN grades.phase_3_pass_date IS NOT NULL THEN grades.phase_3_pass_date
        WHEN NVL(
            grades.phase_4_pass_date
            , grades.phase_5_pass_date
            , most_recent_cohort.graduation_date) IS NOT NULL
        THEN '1900-01-01'::DATE
        END AS phase_3_pass_date
    , CASE
        WHEN grades.phase_4_pass_date IS NOT NULL THEN grades.phase_4_pass_date
        WHEN NVL(
            grades.phase_5_pass_date
            , most_recent_cohort.graduation_date) IS NOT NULL
        THEN '1900-01-01'::DATE
        END AS phase_4_pass_date
    , CASE
        WHEN grades.phase_5_pass_date IS NOT NULL THEN grades.phase_5_pass_date
        WHEN most_recent_cohort.graduation_date IS NOT NULL
        THEN '1900-01-01'::DATE
        END AS phase_5_pass_date
    
    -- Graduate Data
    , scores.tech_score
    , scores.citizenship_score
    , NULL AS marked_ready_for_coaching_date
FROM
    {{ params.table_refs["learn.users"] }} learn_users
LEFT JOIN 
    {{ params.table_refs["student_home.students"] }} base_students
    ON learn_users.learn_uuid = base_students.student_uuid
LEFT JOIN 
    {{ params.table_refs["registrar.external_ids"] }} external_ids
    ON learn_users.learn_uuid = external_ids.learn_uuid
LEFT JOIN 
    {{ params.table_refs["registrar.student_institutions"] }} student_institutions
    ON learn_users.learn_uuid = student_institutions.student_uuid
    AND student_institutions.archived_at IS NULL
LEFT JOIN 
    {{ params.table_refs["service_catalog.institutions"] }} institutions
    ON student_institutions.institution_uuid = institutions.id
LEFT JOIN
    (
        SELECT
            *, ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY close_date DESC NULLS LAST) AS row_num
        FROM {{ params.table_refs["admissions"] }}
    ) AS admissions
    ON learn_users.learn_uuid = admissions.learn_uuid 
    AND admissions.row_num = 1
LEFT JOIN
    (
        SELECT
            user_id
            , username
            , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS is_most_recent
        FROM {{ params.table_refs["learn.identities_github_accounts"] }}
        WHERE 
            archived_at IS NULL
            AND server = 'github.com'
    ) AS github_accounts 
    ON learn_users.id = github_accounts.user_id
    AND github_accounts.is_most_recent = 1
LEFT JOIN 
    {{ params.table_refs["prework_progress"] }} AS prework 
    ON learn_users.learn_uuid = prework.learn_uuid
    AND prework.is_most_recent
LEFT JOIN 
    (
        SELECT
            JSON_EXTRACT_PATH_TEXT(re.properties, 'applicant_uuid') AS learn_uuid 
            , cohorts.uuid
            , cohorts.name
            , cohorts.start_date
            , cohorts.end_date
            , cohorts.campus
            , cohorts.modality
            , cohorts.discipline
            , cohorts.pacing
            , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY re.occurred_at) AS row_num
        FROM {{ params.table_refs["registrar.registration_events"] }} re
        LEFT JOIN {{ params.table_refs["cohorts"] }} cohorts
            ON JSON_EXTRACT_PATH_TEXT(re.properties, 'cohort_uuid') = cohorts.uuid
        WHERE re.action = 'Student Admitted' 
    ) AS original_cohort
    ON learn_users.learn_uuid = original_cohort.learn_uuid 
    AND original_cohort.row_num = 1
LEFT JOIN 
    {{ params.table_refs["rosters"] }} AS most_recent_cohort 
    ON learn_users.learn_uuid = most_recent_cohort.learn_uuid 
    AND most_recent_cohort.is_most_recent_cohort
LEFT JOIN 
    {{ params.table_refs["registrar.historical_graduations"] }} AS historical_graduations
    ON learn_users.learn_uuid = historical_graduations.student_uuid
LEFT JOIN 
    (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY student_uuid, cohort_uuid ORDER BY updated_at DESC) AS row_num
        FROM {{ params.table_refs["service_milestones.student_pace_selections"] }} 
    ) AS pace_selection 
    ON most_recent_cohort.learn_uuid = pace_selection.student_uuid
    AND most_recent_cohort.cohort_uuid = pace_selection.cohort_uuid
    AND pace_selection.row_num = 1
LEFT JOIN 
    {{ params.table_refs["service_milestones.pacing_templates"] }} AS pacing_template 
    ON pace_selection.pacing_template_id = pacing_template.id
LEFT JOIN 
    (
        SELECT
            learn_uuid 
            , cohort_uuid
            , added_to_cohort_date
            , cohort_name
            , cohort_start_date
            , cohort_end_date
            , campus
            , modality
            , discipline
            , pacing
            , scheduled_start_date
            , NVL(cohort_start_date, scheduled_start_date) AS day_1_start_date
            , MIN(CASE WHEN modality = 'Online' THEN day_1_start_date END) OVER (PARTITION BY learn_uuid) AS online_day_1_start
            , on_roster_day_1
            , on_roster_day_15
            , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY added_to_cohort_date) AS cohort_order
        FROM {{ params.table_refs["rosters"] }}
        WHERE on_roster_day_1
    ) AS matriculated
    ON learn_users.learn_uuid = matriculated.learn_uuid
    AND matriculated.cohort_order = 1

LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} AS matriculated_instructor
    ON matriculated.learn_uuid = matriculated_instructor.student_uuid
    AND matriculated_instructor.is_first_instructor
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} AS most_recent_instructor
    ON learn_users.learn_uuid = most_recent_instructor.student_uuid
    AND most_recent_instructor.is_most_recent_instructor
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} AS most_recent_advisor
    ON learn_users.learn_uuid = most_recent_advisor.student_uuid
    AND most_recent_advisor.is_most_recent_advisor
LEFT JOIN 
    {{ params.table_refs["fis.service_educator_versions"] }} AS most_recent_coach
    ON learn_users.learn_uuid = most_recent_coach.student_uuid
    AND most_recent_coach.is_most_recent_coach
LEFT JOIN 
    {{ params.table_refs["end_of_phase_grades"] }} AS grades
    ON learn_users.learn_uuid = grades.learn_uuid
LEFT JOIN 
    (  
        SELECT 
            user_id
            , MAX(NULLIF(REGEXP_SUBSTR(body,'techn?i?c?a?l? score: (\\d*\\.?\\d+)',1,1,'ie'),'')::FLOAT) AS tech_score
            , MAX(NULLIF(REGEXP_SUBSTR(body,'\\bc\\w+ score: (\\d*\\.?\\d+)',1,1,'ie'),'')::FLOAT) AS citizenship_score
        FROM 
            {{ params.table_refs["student_notes"] }} 
        WHERE 
            body ~* '(tech(nical)?|citizenship|cultural) score'
        GROUP BY
            user_id
    ) AS scores 
    ON learn_users.id = scores.user_id
LEFT JOIN
    (
        SELECT 
            NVL(uuids.learn_uuid, ids.learn_uuid) AS learn_uuid
            , SUM(invoices.amount_due / 100) AS amount_paid
            , MAX(invoices.received_at) AS most_recent_payment_date
        FROM {{ params.table_refs["stripe.invoices"] }} invoices
        LEFT JOIN {{ params.table_refs["stripe.customers"] }} customers 
            ON invoices.customer_id = customers.id
        LEFT JOIN {{ params.table_refs["learn.users"] }} uuids
            ON customers.metadata_learn_uuid = uuids.learn_uuid
        LEFT JOIN {{ params.table_refs["learn.users"] }} ids
            ON customers.metadata_learn_id = ids.id
        WHERE invoices.paid
        GROUP BY
            NVL(uuids.learn_uuid, ids.learn_uuid)
    ) AS stripe_payments
    ON learn_users.learn_uuid = stripe_payments.learn_uuid
LEFT JOIN
    (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY recipient_uuid ORDER BY updated_at DESC) AS rn
        FROM postgres_service_documents.ea_addresses
    ) AS ea_addresses
    ON learn_users.learn_uuid = ea_addresses.recipient_uuid
    AND ea_addresses.rn = 1
{% endblock %}