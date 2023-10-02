{% extends "rotate_table.sql" %}
{% block query %}
select
	a.lead_id
	, a.application_id
	, a.email
	, CONVERT_TIMEZONE('America/New_York', a.application_start_date) AS application_start_date
	, CONVERT_TIMEZONE('America/New_York', a.application_submission_date) AS application_submission_date
    , case 
        when a.committed_date is not null then '8. Committed'
        when a.original_admitted_date is not null then '7. Admitted'
        when a.technical_interview_status = 'Completed' then '6. Tech Interview Complete'
        when a.technical_interview_date_time is not null then '5. Tech Interview Scheduled'
        when a.technical_interview_invite_sent_date is not null then '4. Tech Interview Invite'
        when a.admissions_interview_status = 'Completed' then '3. Admissions Interview Completed'
        when a.admissions_interview_date_time is not null then '2. Admissions Interview Scheduled'
        when a.admissions_interview_invite_sent_date is not null then '1. Admissions Inverview Invite'
        when a.application_submission_date is not null then '0. Applied'
        end as most_recent_completed_step

	, MIN(case when ra.activity_type = 'Call' and ra.activity_date > a.application_start_date then CONVERT_TIMEZONE('America/New_York', ra.activity_date) end) as first_call_after_app_start
	, MIN(case when ra.activity_type = 'Email' and ra.activity_date > a.application_start_date then CONVERT_TIMEZONE('America/New_York', ra.activity_date) end) as first_email_after_app_start

	, MIN(case when ra.activity_type = 'Call' and ra.activity_date > a.application_submission_date then CONVERT_TIMEZONE('America/New_York', ra.activity_date) end) as first_call_after_app
	, MIN(case when ra.activity_type = 'Email' and ra.activity_date > a.application_submission_date then CONVERT_TIMEZONE('America/New_York', ra.activity_date) end) as first_email_after_app

	, SUM(case when ra.activity_type = 'Call' and ra.activity_date > a.application_start_date then 1 end) as calls_after_app_start
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date > a.application_start_date then 1 end) as emails_after_app_start

	, SUM(case when ra.activity_type = 'Call' and ra.activity_date > a.application_submission_date then 1 end) as calls_after_app
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date > a.application_submission_date then 1 end) as emails_after_app

	, SUM(case when ra.activity_type = 'Call' and ra.activity_date::DATE >= a.admissions_interview_invite_sent_date then 1 end) as calls_after_interview_invite
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date::DATE >= a.admissions_interview_invite_sent_date then 1 end) as emails_after_interview_invite
	
	, SUM(case when ra.activity_type = 'Call' and ra.activity_date > a.admissions_interview_date_time then 1 end) as calls_after_interview
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date > a.admissions_interview_date_time then 1 end) as emails_after_interview
	
	, SUM(case when ra.activity_type = 'Call' and ra.activity_date::DATE >= a.technical_interview_invite_sent_date then 1 end) as calls_after_tech_int_invite
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date::DATE >= a.technical_interview_invite_sent_date then 1 end) as emails_after_tech_int_invite
	
	, SUM(case when ra.activity_type = 'Call' and ra.activity_date > a.technical_interview_date_time then 1 end) as calls_after_tech_int
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date > a.technical_interview_date_time then 1 end) as emails_after_tech_int
	
	, SUM(case when ra.activity_type = 'Call' and ra.activity_date >= a.original_admitted_date then 1 end) as calls_after_admit
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date >= a.original_admitted_date then 1 end) as emails_after_admit
	
	, SUM(case when ra.activity_type = 'Call' and ra.activity_date >= a.committed_timestamp then 1 end) as calls_after_commit
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date >= a.committed_timestamp then 1 end) as emails_after_commit
	
    , SUM(case when ra.activity_type = 'Call' and ra.activity_date >= a.most_recent_closed_won_date then 1 end) as calls_after_close_won
	, SUM(case when ra.activity_type = 'Email' and ra.activity_date >= a.most_recent_closed_won_date then 1 end) as emails_after_close_won
	
from {{ params.table_refs["admissions"] }} a 
left join 
	(
		select 
			l.id AS lead_id
			, c.id AS contact_id
			, ra.who_id
			, case 
				when ra.type || ra.task_subtype ~* 'call' then 'Call' 
				when ra.type || ra.task_subtype ~* 'email' then 'Email'
				end as activity_type
			, ra.activity_date
		from {{ params.table_refs["rep_activities"] }} ra
        left join stitch_salesforce.lead l 
            on ra.who_id = l.id
        left join stitch_salesforce.contact c
            on ra.who_id = c.id
		where 
			ra.activity_date > '2020-01-01'
			and ra.status = 'Completed' 
			and (
				ra.call_duration_in_seconds > 0 -- Calls that happened
				OR ra.type = 'Email' -- Emails
			)
		UNION ALL 
		select 
			l.id as lead_id
			, c.id as contact_id
			, e.whoid as who_id
			, 'Call' as activity_type
			, e.startdatetime as activity_date
		from stitch_salesforce.event e
        left join stitch_salesforce.lead l 
            on e.whoid = l.id
        left join stitch_salesforce.contact c
            on e.whoid = c.id
		where 
			e.scheduleonce_event_type__c = 'Flatiron School Interview'
			and e.scheduleonce__event_status__c = 'Completed'
	) ra
    on NVL(a.contact_id, a.lead_id) = ra.who_id
	and a.application_submission_date < ra.activity_date
where
	(
		a.application_submission_date > '2020-01-01' 
		and a.is_most_recent_app
	)
	OR (
		a.application_start_date IS NOT NULL 
		AND a.application_submission_date IS NULL
	)
group by 
	a.lead_id
	, a.application_id
	, a.email
	, a.application_start_date
	, a.application_submission_date
    , case 
        when a.committed_date is not null then '8. Committed'
        when a.original_admitted_date is not null then '7. Admitted'
        when a.technical_interview_status = 'Completed' then '6. Tech Interview Complete'
        when a.technical_interview_date_time is not null then '5. Tech Interview Scheduled'
        when a.technical_interview_invite_sent_date is not null then '4. Tech Interview Invite'
        when a.admissions_interview_status = 'Completed' then '3. Admissions Interview Completed'
        when a.admissions_interview_date_time is not null then '2. Admissions Interview Scheduled'
        when a.admissions_interview_invite_sent_date is not null then '1. Admissions Inverview Invite'
        when a.application_submission_date is not null then '0. Applied'
        end
{% endblock %}
	