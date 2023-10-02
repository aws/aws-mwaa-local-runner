{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    /*** Student Info ***/
    admissions.admittee_id AS learn_uuid
    , external_ids.external_id
    , admissions.uuid AS admission_uuid
    , cohorts.uuid AS cohort_uuid
    , cohorts.url AS cohort_url
    , users.last_name
    , users.first_name
    , users.email
    , admissions.state
    , admissions.country

    /*** Cohort Info ***/
    , cohorts.name AS cohort_name
    , cohorts.start_date AS cohort_start_date
    , cohorts.end_date AS cohort_end_date
    , cohorts.campus
    , cohorts.modality
    , cohorts.discipline
    , cohorts.pacing
    , admissions.admitted_at AS admitted_date
    , CASE WHEN cohorts.pacing = 'Self Paced' THEN scheduled_starts.start_date END AS scheduled_start_date
    , events.added_to_cohort_date
    , NVL(events.removed_from_cohort_date, admissions.cancelled_at) AS removed_from_cohort_date
    , admissions.cancelled_at AS admission_canceled_date
    , graduation_actions.graduation_date
    , graduation_actions.graduation_updated_at
    , expiration_actions.expiration_date
    , expiration_actions.expiration_updated_at

    /*** Roster Actions ***/
    , events.added_to_roster_action_type
    , events.added_to_roster_action_change
    , events.added_to_roster_action_module
    , events.added_to_roster_action_details
    , events.added_to_roster_action_reasons
    , events.removed_from_roster_action_type
    , events.removed_from_roster_action_change
    , events.removed_from_roster_action_module
    , events.removed_from_roster_action_details
    , events.removed_from_roster_action_reasons

    /*** Payment Info ***/
    , payment_plans.scholarship_name
    , payment_plans.scholarship_amount/100 AS scholarship_amount    
    
    , payments.deposit_amount_due
    , payments.deposit_paid_date
    
    , payment_plans.financially_cleared
    , payment_plans.financially_cleared_at AS financially_cleared_date
    , payment_options.payment_type AS financing_method
    , payment_plans.amount_due/100 AS financing_method_amount
    , NULLIF(REGEXP_SUBSTR(payment_plans.price_quotes, 'gross_tuition_amount\\W+(\\d+)',1,1,'e'),'')::INT/100 AS gross_tuition_amount
    , payments.tuition_amount_paid
    , payment_options.status AS financing_method_status
    , approved_by.first_name || ' ' || approved_by.last_name AS financing_method_status_approved_by
    , approved_by.occurred_at AS financing_method_status_approved_date
    , payment_options.financing_partner
    , payment_options.standing AS financial_standing
    , delinquent_status.marked_delinquent_date

    /*** Status Info ***/
    , compliance_clearances.catalog_completed_at AS catalog_signed_date
    , compliance_clearances.ea_completed_at AS enrollment_agreement_signed_date
    , compliance_clearances.ea_cleared_at IS NOT NULL AS enrollment_agreement_cleared
    , compliance_clearances.ea_cleared_at AS enrollment_agreement_cleared_date
    , academic_clearances.completed_at AS academically_cleared_date
    , admissions.status AS student_status
    , NVL2(NVL(mbga.admission_uuid, mbgcr.admission_id::VARCHAR), 'Yes', 'No') AS mbg_program
    , leaves_of_absence.loa_count
    , leaves_of_absence.effective_date AS most_recent_loa_start_date
    , leaves_of_absence.return_date AS most_recent_loa_end_date
    , CASE 
        WHEN CURRENT_DATE < NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE) THEN NULL
        WHEN 
            events.added_to_cohort_date::DATE <= NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE)
            AND NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE) < NVL(events.removed_from_cohort_date::DATE, admissions.cancelled_at::DATE, CURRENT_TIMESTAMP) 
        THEN TRUE 
        ELSE FALSE 
        END AS on_roster_day_1
    , CASE 
        WHEN CURRENT_DATE < NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE) + 14 THEN NULL
        WHEN 
            events.added_to_cohort_date::DATE <= NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE) + 14
            AND NVL(cohorts.start_date::DATE, scheduled_starts.start_date::DATE) + 14 < NVL(events.removed_from_cohort_date, admissions.cancelled_at, CURRENT_TIMESTAMP) 
        THEN TRUE 
        ELSE FALSE 
        END AS on_roster_day_15
    , ROW_NUMBER() OVER (PARTITION BY admissions.admittee_id ORDER BY events.added_to_cohort_date) AS cohort_order
    , ROW_NUMBER() OVER (PARTITION BY admissions.admittee_id ORDER BY cohorts.start_date) AS start_date_order
    , cohort_order = 1 AS is_original_cohort
    , ROW_NUMBER() OVER (PARTITION BY admissions.admittee_id ORDER BY events.occurred_at DESC) = 1 AS is_most_recent_cohort
    
    /*** Salesforce Opportunity Info ***/
    , opportunities.id AS opportunity_id
    , opportunities.name AS opportunity_name
    , sf_users.name AS opportunity_owner_name
    , onboarding_agents.name AS onboarding_agent_name
    , accounts.id AS account_id
    , accounts.name AS account_name
    , opportunities.application_date__c AS application_submission_date
    , opportunities.committed_date__c AS committed_date
    , opportunities.deferral_reason__c AS deferral_reason
    , opportunities.closedate AS close_date
    , opportunities.closed_lost_reason__c AS closed_lost_reason
    , opportunities.stagename AS stage_name
    , opportunities.pause_start_date__c AS sf_pause_start_date
    , opportunities.pause_end_date__c AS sf_pause_end_date
    , opportunities.days_since_last_activity__c days_since_onboarding_activity
FROM
    {{ params.table_refs["registrar.admissions"] }} admissions
JOIN
    {{ params.table_refs["learn.users"] }} users
    ON admissions.admittee_id = users.learn_uuid
JOIN
    {{ params.table_refs["roster_changes"] }} AS events
    ON admissions.uuid = events.admission_uuid
JOIN
    {{ params.table_refs["cohorts"] }} cohorts
    ON events.cohort_uuid = cohorts.uuid
LEFT JOIN 
    {{ params.table_refs["graduation_actions"] }} graduation_actions
    ON events.admission_uuid = graduation_actions.admission_uuid 
    AND events.cohort_uuid = graduation_actions.source_cohort_uuid
LEFT JOIN 
    {{ params.table_refs["expiration_actions"] }} expiration_actions
    ON events.admission_uuid = expiration_actions.admission_uuid 
    AND events.cohort_uuid = expiration_actions.source_cohort_uuid
LEFT JOIN 
    {{ params.table_refs["registrar.external_ids"] }} external_ids
    ON admissions.admittee_id = external_ids.learn_uuid
LEFT JOIN
    {{ params.table_refs["registrar.scheduled_starts"] }} scheduled_starts
    ON admissions.uuid = scheduled_starts.admission_uuid
LEFT JOIN 
    (
        SELECT 
            admission_uuid
            , cohort_uuid
            , MAX(CASE WHEN document_kind ~ 'catalog' THEN occurred_at END) AS catalog_completed_at
            , MAX(CASE WHEN document_kind = 'enrollment_agreement' THEN occurred_at END) AS ea_completed_at
            , MAX(CASE WHEN action = 'Enrollment Agreement Cleared' THEN occurred_at END) AS ea_cleared_at
        FROM (
            SELECT *
                , JSON_EXTRACT_PATH_TEXT(data, 'student_uuid') AS student_uuid
                , JSON_EXTRACT_PATH_TEXT(data, 'kind') AS document_kind
                , LAST_VALUE(CASE WHEN resource_name = 'Cohort' THEN resource_uuid END IGNORE NULLS) OVER (PARTITION BY student_uuid ORDER BY occurred_at ROWS UNBOUNDED PRECEDING) cohort_uuid
                , LAST_VALUE(CASE WHEN resource_name = 'Admission' THEN resource_uuid END IGNORE NULLS) OVER (PARTITION BY student_uuid ORDER BY occurred_at ROWS UNBOUNDED PRECEDING) admission_uuid
            FROM {{ params.table_refs["registrar.tracking_events"] }}
            WHERE action in (
                'Student Status Changed'
                , 'Student Registered In Cohort'
                , 'Enrollment Agreement Cleared'
                , 'Compliance Document Completed'
            )
        )
        WHERE cohort_uuid IS NOT NULL
        GROUP BY 1, 2
    ) compliance_clearances
    ON events.admission_uuid = compliance_clearances.admission_uuid
    AND events.cohort_uuid = compliance_clearances.cohort_uuid
LEFT JOIN
    {{ params.table_refs["registrar.academic_clearances"] }} academic_clearances
    ON cohorts.discipline_uuid = academic_clearances.course_uuid
    AND admissions.admittee_id = academic_clearances.student_uuid
LEFT JOIN
    {{ params.table_refs["registrar.payment_plans"] }} payment_plans
    ON admissions.admittee_id = payment_plans.customer_uuid
    AND admissions.salesforce_opportunity_id = payment_plans.salesforce_opportunity_id
LEFT JOIN
    {{ params.table_refs["registrar.payment_options"] }} payment_options
    ON payment_plans.id = payment_options.payment_plan_id
    AND payment_options.active = 'true'
LEFT JOIN
    (
        SELECT 
            invoices.invoicee_uuid
            , invoices.sf_opportunity_id
            , SUM(CASE WHEN invoice_categories.name = 'deposit' THEN invoices.amount_due/100 END) AS deposit_amount_due
            , MIN(CASE WHEN invoice_categories.name = 'deposit' THEN invoices.paid_at END) AS deposit_paid_date
            , SUM(CASE WHEN invoices.paid_at IS NOT NULL AND invoice_categories.name = 'tuition' THEN invoices.amount_due/100 END) AS tuition_amount_paid
        FROM {{ params.table_refs["registrar.invoices"] }} invoices
        JOIN {{ params.table_refs["registrar.invoice_categories"] }} invoice_categories
            ON invoices.invoice_category_id = invoice_categories.id
        WHERE invoices.closed_at IS NULL
        GROUP BY 
            invoices.invoicee_uuid
            , invoices.sf_opportunity_id
    ) AS payments 
    ON admissions.admittee_id = payments.invoicee_uuid
    AND admissions.salesforce_opportunity_id = payments.sf_opportunity_id
LEFT JOIN 
    (
        SELECT 
            learn_uuid
            , effective_date
	        , return_date
            , SUM(CASE WHEN status != 'canceled' THEN 1 END) OVER (PARTITION BY learn_uuid) AS loa_count
            , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY effective_date desc) AS loa_order
        FROM {{ params.table_refs["leaves_of_absence"] }} leaves_of_absence
    ) AS leaves_of_absence
    ON admissions.admittee_id = leaves_of_absence.learn_uuid
    AND leaves_of_absence.loa_order = 1
LEFT JOIN 
    {{ params.table_refs["stitch_salesforce.opportunity"] }} opportunities
    ON admissions.salesforce_opportunity_id = opportunities.id
LEFT JOIN 
    {{ params.table_refs["stitch_salesforce.account"] }} accounts
    ON opportunities.accountid = accounts.id
    AND NOT accounts.isdeleted
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} sf_users
    ON opportunities.ownerid = sf_users.id
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS onboarding_agents
    ON opportunities.onboarding_agent__c = onboarding_agents.id
LEFT JOIN 
    (
        SELECT
            JSON_EXTRACT_PATH_TEXT(data, 'student_uuid') AS student_uuid
            , MAX(occurred_at) AS marked_delinquent_date
        FROM {{ params.table_refs["registrar.tracking_events"] }}
        WHERE 
            action = 'Financing Method Standing Changed'
            AND JSON_EXTRACT_PATH_TEXT(data, 'standing') = 'Delinquent'
        GROUP BY
            JSON_EXTRACT_PATH_TEXT(data, 'student_uuid')
    ) AS delinquent_status
    ON admissions.admittee_id = delinquent_status.student_uuid
LEFT JOIN 
    (
        SELECT
            JSON_EXTRACT_PATH_TEXT(te.data, 'student_uuid') AS learn_uuid
            , JSON_EXTRACT_PATH_TEXT(te.data, 'payment_option_uuid') AS payment_option_uuid
            , te.occurred_at
            , u.first_name
            , u.last_name 
            , ROW_NUMBER() OVER (PARTITION BY 
                    JSON_EXTRACT_PATH_TEXT(te.data, 'student_uuid'), 
                    JSON_EXTRACT_PATH_TEXT(te.data, 'payment_option_uuid') ORDER BY te.occurred_at DESC
                ) AS status_order
        FROM {{ params.table_refs["registrar.tracking_events"] }} te 
        LEFT JOIN  {{ params.table_refs["learn.users"] }} u 
            ON te.actor_uuid = u.learn_uuid 
        WHERE 
            te.action = 'Financing Method Status Changed'
            AND JSON_EXTRACT_PATH_TEXT(te.data, 'status') = 'Approved'
    ) AS approved_by
    ON admissions.admittee_id = approved_by.learn_uuid
    AND payment_options.uuid = approved_by.payment_option_uuid
    AND approved_by.status_order = 1
LEFT JOIN 
    {{ params.table_refs["registrar.money_back_guarantee_access"] }} mbga
    ON admissions.uuid = mbga.admission_uuid
    AND mbga.cancelled_at IS NULL
LEFT JOIN 
    (
        SELECT 
            cr.admission_id
            , cr.cohort_id
        FROM {{ params.table_refs["registrar.money_back_guarantee_access"] }} mbga 
        JOIN {{ params.table_refs["registrar.cohort_registrations"] }} cr 
            ON mbga.cohort_registration_id = cr.id
        WHERE mbga.cancelled_at IS NULL
        GROUP BY 
            cr.admission_id
            , cr.cohort_id
    ) AS mbgcr
    ON admissions.id = mbgcr.admission_id
    AND cohorts.id = mbgcr.cohort_id
{% endblock %}