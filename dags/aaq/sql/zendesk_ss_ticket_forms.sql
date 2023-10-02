{% extends "rotate_table.sql" %}
{% block query %}
select
    ticket_id
    , student_full_name
    , student_email
    , discipline
    , location_of_study
    , current_cohort
    , new_cohort
    , phase_to_repeat
    , academic_probation
    , loa_start_date
    , loa_end_date
    , withdrawal_date
    , category
    , DECODE(
        category,
        'admissions_category', admissions_subtype,
        'career_services_category', career_services_subtype,
        'data_privacy_request_category', data_privacy_subtype,
        'document_request_category', document_request_subtype,
        'delinquency_category', delinquency_subtype,
        'education_verification_category', education_verification_subtype,
        'financial_adjustment_category', financial_subtypes,
        'financial_clearance_category', financial_subtypes,
        'financial_dispute_category', financial_subtypes,
        'general_finance_questions_category', financial_subtypes,
        'general_questions_category', general_questions_subtype,
        'laptops_category', laptops_subtype,
        'leave_of_absence_category', loa_subtype,
        'policy_question_category', policy_question_subtype,
        'self-paced_program_category', self_paced_subtype,
        'student_relations_category', student_relations_subtype,
        'swag_category', swag_subtype,
        'tech_support_category', tech_support_subtype,
        'transfer/withdrawal_calculations_category', financial_subtypes,
        'transfer_category', transfer_subtype,
        'withdrawal_category', withdrawal_subtype,
        NULL
    ) AS category_subtype
    , DECODE(
        category,
        'admissions_category', admissions_reason,
        'delinquency_category', delinquency_reason,
        'enrollment_cancellation_category', enrollment_cancellation_reason,
        'financial_adjustment_category', financial_reasons,
        'financial_clearance_category', financial_reasons,
        'general_finance_questions_category', financial_reasons,
        'transfer/withdrawal_calculations_category', financial_reasons,
        'student_relations_category', student_relations_reason,
        'withdrawal_category', rostering_action_reason,
        NULL
    ) AS category_reason
    , rostering_action_reason
FROM 
    (
        SELECT tcf._sdc_source_key_id AS ticket_id, tf2.title, tcf.value__string
        FROM zendesk.ticket_forms tf
        JOIN zendesk.ticket_forms__ticket_field_ids tftfi 
            ON tf.id = tftfi._sdc_source_key_id
        JOIN zendesk.ticket_fields tf2
            ON tftfi.value = tf2.id
        JOIN zendesk.tickets__custom_fields tcf
            ON tf2.id = tcf.id
        WHERE
            tf.name = 'Student Support Form' 
	) PIVOT (
        MAX(value__string) for title IN (
            'Student Full Name' student_full_name
            , 'Student Email' student_email
            , 'Student''s Current Discipline ' discipline
            , 'Student''s Current Location of Study' location_of_study
            , 'Current Cohort' current_cohort
            , 'New Cohort (as a result of transfer/LOA)' new_cohort
            , 'Which module/phase should the student repeat?' phase_to_repeat
            , 'Is the student on Academic Probation?' academic_probation
            , ' Start date for leave of absence' loa_start_date
            , 'End date for leave of absence' loa_end_date
            , 'Official Withdrawal Date' withdrawal_date
            , 'Admissions Reason' admissions_reason
            , 'Admissions Subtype' admissions_subtype
            , 'Career Services Subtype' career_services_subtype
            , 'Category' category
            , 'Data Privacy Request Subtype' data_privacy_subtype
            , 'Delinquency Reason' delinquency_reason
            , 'Delinquency Subtype' delinquency_subtype
            , 'Document Request Subtype' document_request_subtype
            , 'Education Verification Subtype' education_verification_subtype
            , 'Enrollment Cancellation Reason' enrollment_cancellation_reason
            , 'Financial Reasons' financial_reasons
            , 'Financial Subtypes' financial_subtypes
            , 'General Questions Subtype' general_questions_subtype
            , 'Laptops Subtype' laptops_subtype
            , 'Leave of Absence Subtype' loa_subtype
            , 'Policy Question Subtype' policy_question_subtype
            , 'Rostering Action Reason' rostering_action_reason
            , 'Self-Paced Program Subtype' self_paced_subtype
            , 'Student Relations Subtype' student_relations_subtype
            , 'Student Relations reason' student_relations_reason
            , 'Swag Subtype' swag_subtype
            , 'Tech Support Subtype' tech_support_subtype
            , 'Transfer Subtype' transfer_subtype
            , 'Transfer/Withdrawal Calculations Reason' transfer_withdrawal_reason
            , 'Withdrawal Subtype' withdrawal_subtype
        )
    )
{% endblock %}