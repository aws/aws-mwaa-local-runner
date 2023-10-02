{% extends "rotate_table.sql" %}
{% block query %}
WITH leads AS (
    SELECT 
        id 
        , createddate
        , convertedaccountid
        , convertedcontactid
        , ownerid
        , associated_application__c
        , downloaded_outcomes_report_date__c
        , downloaded_syllabus_date__c
        , started_application_date__c
        , learn_uuid__c
        , email 
        , lastname 
        , firstname 
        , phone 
        , state_of_residency__c
        , street
        , city
        , state
        , postalcode
        , country
        , leadsource
        , lead_score_2__c AS lead_score__c
        , interview_priority__c
        , status
        , nurture_reason__c
        , admissions_interview_invite_sent_date__c
        , admissions_interview_date_time__c
        , admissions_interview_status__c
        , x10_minute_chat_date__c
        , x10_minute_chat_status__c
        , x10_minute_chat_owner__c
        , call_center_agent__c
        , application_claimed_by__c
        , career_karma_tier__c
        , prospected_lead__c
        , prospecting_rep__c
    FROM {{ params.table_refs["stitch_salesforce.lead"] }}
    WHERE 
        NOT isdeleted
)   
, applications AS (
    SELECT
        id 
        , lead__c
        , createddate
        , email__c
        , last_name__c
        , first_name__c
        , phone__c
        , date_of_birth__c
        , recordtypeid
        , postal_code__c
        , state__c
        , country__c
        , course__c
        , market_interest__c 
        , campus_interest__c
        , pacing_interest__c
        , interested_in_full_time_cohort__c
        , timeframe__c
        , professional_goal__c
        , education__c
        , area_of_study__c
        , technical_background__c
        , professional_background__c
        , referral_source__c
        , scholarship_code__c
        , application_score__c
        , amazon_fulltime_or_part_time__c
        , program_goal__c
        -- , application_type__c
    FROM {{ params.table_refs["stitch_salesforce.application__c"] }}
    WHERE  
        createddate >= '2019-05-19' -- Post-bedrock
        AND NOT isdeleted
)
, accounts AS (
    SELECT 
        id 
        , createddate 
        , learn_uuid__c
        , email__c
        , last_name__c
        , first_name__c
        , phone
        , billingstreet
        , billingcity
        , billingstate
        , billingpostalcode
        , billingcountry
        , current_cohort__c
    FROM {{ params.table_refs["stitch_salesforce.account"] }}
    WHERE 
        email__c NOT LIKE '%flatironschool.com%' 
        AND NOT isdeleted 
)
, contacts AS (
    SELECT
        id 
        , accountid
        , learn_uuid__c
        , email
        , lastname
        , firstname
        , phone
        , NVL(birthday__c, birthdate) AS date_of_birth
        , mailingstreet
        , mailingcity
        , mailingstate
        , mailingpostalcode
        , mailingcountry
        , ROW_NUMBER() OVER (PARTITION BY accountid ORDER BY createddate DESC) row_num
    FROM {{ params.table_refs["stitch_salesforce.contact"] }}
    WHERE NOT isdeleted
)

SELECT
    GETDATE() AS run_date
    
    /*** Records IDs and creation timestamps ***/
    , leads.id AS lead_id
    , leads.createddate AS lead_created_date
    , contacts.id AS contact_id
    , accounts.id AS account_id
    , accounts.createddate AS account_created_date
    , opportunities.id AS opportunity_id
    , opportunities.name AS opportunity_name
    , opportunities.type AS opportunity_type
    , opportunities.recordtypeid AS opportunity_record_type
    , opportunities.createddate AS opportunity_created_date
    , ROW_NUMBER() OVER (PARTITION BY accounts.learn_uuid__c ORDER BY opportunities.createddate DESC) = 1 AS is_most_recent_opp
    , applications.id AS application_id
    , LEAST(leads.started_application_date__c, applications.createddate) AS application_start_date
    , applications.createddate AS application_submission_date
    , ROW_NUMBER() OVER (PARTITION BY NVL(accounts.email__c, contacts.email, applications.email__c, leads.email) ORDER BY applications.createddate DESC) = 1 AS is_most_recent_app
    
    /*** Time period (Pre-bedrock, Post-bedrock, Post-brickhouse, etc) ***/
    , CASE
        WHEN applications.createddate < '2019-05-19' THEN 'pre_bedrock' 
        WHEN applications.createddate < '2020-06-08' THEN 'pre_brickhouse'
        WHEN applications.createddate >= '2020-06-08' THEN 'post_brickhouse'
        END AS internal_time_period
    , opportunities.pre_brickhouse__c AS pre_brickhouse_flag
    
    /*** PII ***/
    , NVL(accounts.learn_uuid__c, leads.learn_uuid__c) AS learn_uuid 
    , NVL(accounts.email__c, contacts.email, applications.email__c, leads.email) AS email
    , NVL(accounts.last_name__c, contacts.lastname, applications.last_name__c, leads.lastname) AS last_name
    , NVL(accounts.first_name__c, contacts.firstname, applications.first_name__c, leads.firstname) AS first_name
    , NVL(accounts.phone, contacts.phone, applications.phone__c, leads.phone) AS phone_number
    , NVL(applications.date_of_birth__c, contacts.date_of_birth) AS date_of_birth
    
    /*** Location Information ***/
    , NVL(opportunities.state_of_residency__c, leads.state_of_residency__c) AS state_of_residency
    , accounts.billingstreet AS billing_street
    , accounts.billingcity AS billing_city
    , accounts.billingstate AS billing_state
    , accounts.billingpostalcode AS billing_postal_code
    , accounts.billingcountry AS billing_country
    , contacts.mailingstreet AS mailing_street
    , contacts.mailingcity AS mailing_city
    , contacts.mailingstate AS mailing_state
    , contacts.mailingpostalcode AS mailing_postal_code
    , contacts.mailingcountry AS mailing_country
    , leads.street AS lead_street
    , leads.city AS lead_city
    , leads.state AS lead_state
    , leads.postalcode AS lead_postal_code
    , leads.country AS lead_country
    , DECODE(applications.market_interest__c, 'London', 'London', zip_to_msa.msa_name) AS applicant_dma
    , zip_to_msa.msa_num AS msa_code
    , zip_to_msa.msa_name ~* '(Austin)|(Chicago)|(Colorado Springs)|(Denver)|(Houston)|(New York)|(San Francisco)|(Seattle)|(Washington)' AS applicant_in_market

    /*** Marketing/Lead data ***/
    , leads.leadsource AS lead_source
    , leads.lead_score__c AS lead_score
    , leads.status AS lead_status
    , leads.nurture_reason__c AS lead_nurture_reason
    , leads.downloaded_outcomes_report_date__c AS downloaded_outcomes_report_date
	, leads.downloaded_syllabus_date__c AS downloaded_syllabus_date
    , leads.call_center_agent__c AS call_center_agent
    , leads.career_karma_tier__c AS career_karma_tier
    , leads.prospected_lead__c AS prospected_lead
    , marketing.fift_source
    , marketing.fift_medium
    , marketing.fift_campaign
    , marketing.fift_term
    , marketing.alumni_referrer_name
    , marketing.alumni_referrer_email
    , marketing.fimt_source
    , marketing.fimt_medium
    , marketing.fimt_campaign
    , marketing.fimt_term
    , marketing.filt_source
    , marketing.filt_medium
    , marketing.filt_campaign
    , marketing.filt_term
    , marketing.clean_marketing_source
    , marketing.paid_non_paid
    , marketing.campus_interest AS marketo_campus_interest
    , marketing.programassignment AS marketo_program_of_interest
    , marketing.marketo_lead_provided_state
    , marketing.marketo_ip_state
    , marketing.marketo_inferred_state
    , marketing.sms_marketing_opt_in
    , marketing.sms_marketing_opt_in_confirmed
    , marketing.sms_marketing_opt_in_date
    , marketing.sms_marketing_opt_out
    , marketing.sms_marketing_opt_out_date
    /*****  Are the fields below needed? *******/
    , marketing.started_application_page_detail
    , marketing.first_utm_campaign_on_first_page_view
    , marketing.locationassignment
    , marketing.hubspot_updated_source
    , marketing.value2fb
    , marketing.adformat
    , marketing.platform
    , marketing.tactic
    , marketing.registration_source_info
    , marketing.switchupmatch
    , marketing.coursereportmatchexperience

    /*** Application details ***/
    , application_claimer.name application_claimed_by
    , applications.recordtypeid AS application_record_type
    , applications.postal_code__c AS application_postal_code
    , applications.state__c AS application_state
    , applications.country__c AS application_country
    , applications.course__c AS application_discipline -- discipline
    , applications.market_interest__c AS application_market -- Online, NYC, DC, etc
    , applications.campus_interest__c AS application_campus -- Manhattan/Brooklyn if market == NYC
    , applications.pacing_interest__c AS application_pacing
    , applications.interested_in_full_time_cohort__c AS interested_in_full_time_cohort
    , CASE 
        WHEN applications.market_interest__c = 'Online' THEN 'Online'
        WHEN applications.market_interest__c <> 'Online' THEN 'Campus'
        END AS application_modality
    , applications.timeframe__c AS start_timeframe
    , applications.professional_goal__c AS professional_goal
    , applications.education__c AS education
    , applications.area_of_study__c AS area_of_study
    , applications.technical_background__c AS technical_background
    , applications.professional_background__c AS professional_background
    , applications.referral_source__c AS application_referral_source
    , applications.scholarship_code__c AS application_coupon_code
    , NVL(
        applications.application_score__c::VARCHAR, 
        leads.interview_priority__c::VARCHAR
        )::FLOAT AS application_score
    , CASE WHEN application_score >= 3 THEN 'Qualified' ELSE 'Non-Qualified' END AS quality
    , CASE 
        WHEN applications.application_score__c IS NOT NULL
        THEN 'AI_model'
        WHEN leads.interview_priority__c IS NOT NULL
        THEN 'admissions_rep'
        END AS application_score_source
    , applications.amazon_fulltime_or_part_time__c AS amazon_fulltime_or_part_time
    , applications.program_goal__c AS amazon_program_goal
    -- , applications.application_type__c AS application_type

    /*** Admissions Interview data ***/
    , lead_owners.name AS prospecting_rep_name
    , lead_owners.name AS lead_owner_name
    , opp_owners.name AS opportunity_owner_name
    , onboarding_agents.name AS onboarding_agent_name
    , NVL(opp_owners.name, lead_owners.name) AS owner_name
    , leads.admissions_interview_invite_sent_date__c AS admissions_interview_invite_sent_date
    , NVL(
        opportunities.admissions_interview_date_time__c
        , leads.admissions_interview_date_time__c
        ) AS admissions_interview_date_time
 	, NVL(
        opportunities.admissions_interview_status__c
        , leads.admissions_interview_status__c
        ) AS admissions_interview_status
    , leads.x10_minute_chat_date__c AS ten_minute_chat_date
    , leads.x10_minute_chat_status__c AS ten_minute_chat_status
    , leads.x10_minute_chat_owner__c AS ten_minute_chat_owner

    /*** Technical Interview data ***/
    , tech_int.recordtypeid AS technical_interview_type -- tech interview type
    , opportunities.technical_interviewer_name__c AS technical_interviewer_name
 	, opportunities.technical_interview_invite_sent_date__c AS technical_interview_invite_sent_date
    , opportunities.technical_interview_date_time__c AS technical_interview_date_time
 	, opportunities.technical_interview_status__c AS technical_interview_status
    , tech_int.technical_interview_score

    /*** Conversion data ***/
 	, opportunities.admitted_date__c AS admitted_date
 	, opportunities.committed_date__c AS committed_date
 	, opportunities.financially_cleared__c AS financially_cleared_flag
    , most_recent_cohort.academically_cleared_date
    , opportunities.onboarding_cleared_to_start__c AS onboarding_cleared_to_start
 	, opportunities.enrollment_status__c AS enrollment_status
 	, opportunities.enrollment_agreement_signed_date__c AS enrollment_agreement_signed_date
 	, opportunities.closedate AS close_date
 	, opportunities.stagename AS stage_name
    , opportunities.deferral_reason__c AS deferral_reason
    , opportunities.deferralnotes__c AS deferral_notes
    , opportunities.most_recent_date_deferred__c AS most_recent_date_deferred
    , opportunities.times_deferred__c AS times_deferred
 	, opportunities.closed_lost_reason__c AS closed_lost_reason
    , history.original_admitted_date
    , history.committed_timestamp
    , history.most_recent_closed_lost_date
    , history.most_recent_closed_won_date

    /*** Cohort data ***/
    , og_cohort.cohort_name AS original_cohort_name
    , og_cohort.cohort_start_date AS original_cohort_start_date
    , og_cohort.cohort_end_date AS original_cohort_end_date
    , og_cohort.campus AS original_cohort_campus
 	, og_cohort.modality AS original_cohort_modality
 	, og_cohort.discipline AS original_cohort_discipline
    , og_cohort.pacing AS original_cohort_pacing
        
    , matriculated_cohort.cohort_name AS matriculated_cohort_name
    , matriculated_cohort.cohort_start_date AS matriculated_cohort_start_date
    , matriculated_cohort.cohort_end_date AS matriculated_cohort_end_date
    , matriculated_cohort.campus AS matriculated_cohort_campus
    , matriculated_cohort.modality AS matriculated_cohort_modality
 	, matriculated_cohort.discipline AS matriculated_cohort_discipline
    , matriculated_cohort.pacing AS matriculated_cohort_pacing
    , matriculated_cohort.matriculation_date
    , matriculated_cohort.on_roster_day_15

    , most_recent_cohort.cohort_name AS most_recent_cohort_name
    , most_recent_cohort.cohort_start_date AS most_recent_cohort_start_date
    , most_recent_cohort.cohort_end_date AS most_recent_cohort_end_date
    , most_recent_cohort.campus AS most_recent_cohort_campus
    , most_recent_cohort.modality AS most_recent_cohort_modality
    , most_recent_cohort.discipline AS most_recent_cohort_discipline
    , most_recent_cohort.pacing AS most_recent_cohort_pacing
    , most_recent_cohort.scheduled_start_date AS most_recent_scheduled_start_date
    , most_recent_cohort.added_to_cohort_date AS most_recent_added_to_cohort_date
    , most_recent_cohort.removed_from_cohort_date AS most_recent_removed_from_cohort_date
    
    , prework.prework_started_at AS prework_start_date
    , prework.completed_hashketball_date
    , prework.last_prework_activity_date
    , prework.current_course
    , prework.current_module
    , prework.last_lesson_completed
    , prework.completed_prework_lessons
    , prework.total_prework_lessons
    , prework.completed_prework_lessons / NULLIF(prework.total_prework_lessons::FLOAT, 0) AS prework_completed_pct

    /*** Gov't Benefits ***/
    , opportunities.military_benefits_applied__c AS military_benefits_applied
    , opportunities.military_benefits_approved_date__c AS military_benefits_approved_date
    , opportunities.proof_of_military_benefits_intake_form__c AS proof_of_military_benefits_intake_form
    , opportunities.misc_benefits_applied__c AS misc_benefits_applied
    , opportunities.misc_benefits_approved_date__c AS misc_benefits_approved_date
    , opportunities.proof_of_misc_benefits_intake_form__c AS proof_of_misc_benefits_intake_form

    /*** Finance/Tuition data ***/
    , opportunities.currencyisocode AS currency_iso_code
    , opportunities.deposit_amount__c AS deposit_amount
    , opportunities.deposit_received_date__c AS deposit_received_date
    , opportunities.financing_method__c AS financing_method
    , NULL AS financing_amount
    , NULL AS financing_applied_date
    , NULL AS financing_cleared_date
    , opportunities.financing_status__c AS financing_status
    , opportunities.financial_partner__c AS financing_partner
    , scholarships.name__c AS scholarship_code_used
    , opportunities.scholarship_award_letter_sent_date__c AS scholarship_letter_sent_date
    , opportunities.scholarship_letter_signed_date__c AS scholarship_letter_signed_date
    , NVL(scholarships.amount__c, opportunities.scholarship_amount__c) AS scholarship_amount
    , opportunities.scholarship_status__c AS scholarship_status
    , opportunities.gross_tuition__c AS gross_tuition_amount
    , opportunities.paid_tuition__c AS paid_tuition
    , opportunities.balance_amount__c AS balance_amount
    , opportunities.balance_due_amount__c AS balance_due_amount
    , opportunities.loan_amount__c AS loan_amount
FROM 
	leads
FULL OUTER JOIN
    applications
    ON leads.id = applications.lead__c
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.opportunity"] }} opportunities
    ON applications.id = opportunities.application__c
    AND NOT opportunities.isdeleted
    AND opportunities.recordtypeid IN (
        '0121L0000014MmwQAE',
        '0121L0000014Mn1QAE',
        '0121L0000014Mn6QAE',
        '0121L0000014MnBQAU'
    )
LEFT JOIN 
    accounts
    ON opportunities.accountid = accounts.id
LEFT JOIN
    contacts
    ON accounts.id = contacts.accountid
    AND contacts.row_num = 1
LEFT JOIN
    (
        SELECT
            opportunityid
            , MIN(CASE WHEN stagename = 'Admitted' THEN createddate END) AS original_admitted_date
            , MIN(CASE WHEN stagename = 'Committed' THEN createddate END) AS committed_timestamp
            , MAX(CASE WHEN stagename = 'Closed Lost' THEN closedate END) AS most_recent_closed_lost_date
            , MAX(CASE WHEN stagename = 'Closed Won' THEN closedate END) AS most_recent_closed_won_date
        FROM {{ params.table_refs["stitch_salesforce.opportunityhistory"] }}
        WHERE stagename in ('Admitted', 'Committed', 'Closed Lost', 'Closed Won')
        GROUP BY opportunityid
    ) history
    ON opportunities.id = history.opportunityid
LEFT JOIN 
    (
        SELECT
            f.opportunity__c
            , f.recordtypeid
            , f.score__c AS technical_interview_score
            , ROW_NUMBER() OVER (PARTITION BY f.opportunity__c ORDER BY f.createddate DESC) AS row_num
        FROM {{ params.table_refs["stitch_salesforce.form__c"] }} f
        WHERE f.score__c IS NOT NULL
    ) tech_int
    ON opportunities.id = tech_int.opportunity__c 
    AND tech_int.row_num = 1
LEFT JOIN 
    {{ params.table_refs["stitch_salesforce.cohort__c"] }} AS current_cohort
    ON accounts.current_cohort__c = current_cohort.id
LEFT JOIN 
    {{ params.table_refs["rosters"] }} AS og_cohort
    ON accounts.learn_uuid__c = og_cohort.learn_uuid
    AND og_cohort.is_original_cohort
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS prospecting_reps
    ON leads.ownerid = prospecting_reps.id
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS lead_owners
    ON leads.ownerid = lead_owners.id
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS application_claimer
    ON leads.application_claimed_by__c = application_claimer.id
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS opp_owners
    ON opportunities.ownerid = opp_owners.id
LEFT JOIN
    {{ params.table_refs["stitch_salesforce.user"] }} AS onboarding_agents
    ON opportunities.onboarding_agent__c = onboarding_agents.id
LEFT JOIN 
    {{ params.table_refs["marketing_sources"] }} AS marketing
    ON NVL(accounts.email__c, contacts.email, applications.email__c, leads.email) = marketing.email
LEFT JOIN 
    (
        SELECT
            learn_uuid 
            , opportunity_id
            , cohort_name
            , cohort_start_date
            , cohort_end_date
            , campus
            , modality
            , discipline
            , pacing
            , scheduled_start_date
            , NVL(cohort_start_date, scheduled_start_date) AS matriculation_date
            , on_roster_day_1
            , on_roster_day_15
            , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY added_to_cohort_date) AS cohort_order
        FROM {{ params.table_refs["rosters"] }}
        WHERE on_roster_day_1
    ) AS matriculated_cohort
    ON accounts.learn_uuid__c = matriculated_cohort.learn_uuid
    AND matriculated_cohort.cohort_order = 1
LEFT JOIN 
    {{ params.table_refs["rosters"] }} AS most_recent_cohort 
    ON opportunities.id = most_recent_cohort.opportunity_id 
    AND most_recent_cohort.is_most_recent_cohort
LEFT JOIN 
    {{ params.table_refs["data_analytics.campus_zips"] }} AS campus_zips 
    ON applications.market_interest__c = campus_zips.campus
LEFT JOIN
    {{ params.table_refs["data_analytics.zip_to_msa"] }} AS zip_to_msa
    ON NVL(campus_zips.zip_code, applications.postal_code__c) = zip_to_msa.zip_code
LEFT JOIN 
    {{ params.table_refs["prework_progress"] }} AS prework 
    ON accounts.learn_uuid__c = prework.learn_uuid
    AND prework.is_most_recent
LEFT JOIN
    stitch_salesforce.code__c AS scholarships
    ON opportunities.scholarship__c = scholarships.id
{% endblock %}