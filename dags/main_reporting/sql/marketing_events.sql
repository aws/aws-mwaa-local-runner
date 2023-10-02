{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    events.id
    , events.source
    , events.url
    , events.clean_name AS name
    , events.theme
    , events.description
    , events.medium
    , events.affiliate
    , events.start
    , events.end
    , events.venue_name
    , events.venue_address_1
    , events.venue_address_2
    , events.venue_city
    , events.venue_postal_code
    , events.venue_state
    , events.venue_country
    , events.email
    , events.status
    , events.attended
    , admissions.billing_city
    , admissions.billing_state
    , admissions.billing_country
    , admissions.mailing_city
    , admissions.mailing_state
    , admissions.mailing_country
    , admissions.lead_city
    , admissions.lead_state
    , admissions.lead_country
    , admissions.applicant_dma
    , admissions.applicant_in_market
    , admissions.marketo_campus_interest
    , admissions.marketo_lead_provided_state
    , admissions.marketo_ip_state
    , admissions.marketo_inferred_state
    , admissions.fift_source
    , admissions.application_submission_date 
    , admissions.admitted_date
    , admissions.committed_date
    , admissions.close_date
    , admissions.stage_name
    , admissions.matriculation_date
    -- FIRST EVENT ATTENDED
    , CASE
        WHEN events.attended 
        THEN ROW_NUMBER() OVER(PARTITION BY events.email
                               ORDER BY case
                                            WHEN events.attended 
                                                THEN events.start
                                            ELSE NULL
                                        END) = 1
        ELSE FALSE
      END original_event
    -- FUNNEL STAGE
    , CASE
        WHEN admissions.application_submission_date IS NULL
            THEN 'pre-application'
        WHEN admissions.application_submission_date IS NOT NULL
            AND events.start < admissions.application_submission_date
            THEN 'application'
        WHEN admissions.admitted_date IS NOT NULL
            AND events.start > admissions.application_submission_date
            AND events.start < admissions.admitted_date
            THEN 'admit'
        WHEN admissions.committed_date IS NOT NULL
            AND events.start > admissions.admitted_date
            AND events.start < admissions.committed_date
            THEN 'commit'
        WHEN admissions.matriculation_date IS NOT NULL
            AND events.start > admissions.committed_date
            AND events.start < admissions.matriculation_date
            THEN 'matriculate'
        ELSE NULL
      END funnel_stage
    -- FUNNEL STAGE - EVENT SEQUENCE - FIRST EVENT
    , CASE
        WHEN funnel_stage IS NOT NULL
            AND events.attended 
            THEN ROW_NUMBER() OVER(PARTITION BY events.email, funnel_stage
                                   ORDER by CASE 
                                                WHEN events.attended 
                                                    THEN events.start
                                                ELSE NULL
                                            END) = 1
        WHEN NOT events.attended
            OR events.attended IS NULL
            THEN FALSE
      END event_sequence_first
    -- FUNNEL STAGE - EVENT SEQUENCE - LAST EVENT
    , CASE
        WHEN funnel_stage IS NOT NULL
            AND events.attended 
            then ROW_NUMBER() OVER(PARTITION BY events.email, funnel_stage
                                   ORDER by CASE 
                                                WHEN events.attended 
                                                    THEN events.start
                                                ELSE NULL
                                            END DESC NULLS LAST) = 1
        WHEN NOT events.attended
            OR events.attended IS NULL
            THEN FALSE
      END event_sequence_last
    -- LAST EVENT BEFORE FUNNEL STAGE INDICATORS
    , CASE
        WHEN admissions.application_submission_date IS NOT NULL
            AND events.attended
            AND events.start < admissions.application_submission_date
            THEN RANK() OVER(PARTITION BY events.email
                             ORDER BY CASE
                                        WHEN events.attended
                                            AND events.start < admissions.application_submission_date
                                            THEN events.start
                                        ELSE NULL
                                      END DESC NULLS LAST) = 1
        ELSE FALSE
      END last_event_before_application
    , CASE
        WHEN admissions.admitted_date IS NOT NULL
            AND events.attended
            AND events.start < admissions.admitted_date
            THEN RANK() OVER(PARTITION BY events.email
                             ORDER BY CASE
                                        WHEN events.attended
                                            AND events.start < admissions.admitted_date
                                            THEN events.start
                                        ELSE NULL
                                      END DESC NULLS LAST) = 1
        ELSE FALSE
      END last_event_before_admit
    , CASE
        WHEN admissions.committed_date IS NOT NULL
            AND events.attended
            AND events.start < admissions.committed_date
            THEN RANK() OVER(PARTITION BY events.email
                             ORDER BY CASE
                                          WHEN events.attended
                                            AND events.start < admissions.committed_date
                                            THEN events.start
                                          ELSE NULL
                                      END DESC NULLS LAST) = 1
        ELSE FALSE
      END last_event_before_commit
    , CASE
        WHEN admissions.matriculation_date IS NOT NULL
            AND events.attended
            AND events.start < admissions.matriculation_date
            THEN RANK() OVER(PARTITION BY events.email 
                             ORDER BY CASE
                                        WHEN events.attended
                                            AND events.start < admissions.matriculation_date
                                            THEN events.start
                                        ELSE NULL
                                      END DESC NULLS LAST) = 1
        ELSE FALSE
      END last_event_before_matriculation
           
FROM  
    (
        SELECT 
            c.id
            , 'Marketo/Salesforce' AS source
            , NULL AS url
            -- rename due to YYYY-DD-MM error by Marketing team
            , CASE WHEN c.name = '2022-16-03-Info Session-FIS' THEN '2022-03-16-Info Session-FIS' ELSE c.name END as clean_name
            , NULL AS theme
            , c.description
            , 'Online' AS medium
            , NULL AS affiliate
            , TO_DATE(REGEXP_SUBSTR(clean_name, '(\\d{4}-\\d{1,2}-\\d{1,2})', 1, 1, 'e'), 'YYYY-MM-DD') AT TIME ZONE 'America/New_York' AS start
            , NULL AS end
            , NULL AS venue_name
            , NULL AS venue_address_1
            , NULL AS venue_address_2
            , NULL AS venue_city
            , NULL AS venue_postal_code
            , NULL AS venue_state
            , NULL AS venue_country
            , cm.email
            , cm.status
            , cm.status = 'Attended' AS attended
        FROM 
            {{ params.table_refs["stitch_salesforce.campaign"] }} c 
        LEFT JOIN 
            {{ params.table_refs["stitch_salesforce.campaignmember"] }} cm 
            ON c.id = cm.campaignid
        LEFT JOIN
            stitch_salesforce.lead l 
            ON cm.leadid = l.id
        WHERE 
            c.type IN ('Webinar', 'Live Event') 
            AND cm.status IN ('Registered', 'No Show', 'Attended') -- Don't care about invites
            AND NOT NVL(l.isdeleted, false) -- Exclude deleted leads
            AND cm.email !~ 'qq\\.com' -- Exclude spam

        UNION ALL 

        SELECT
            e.id
            , 'Eventbrite' AS source
            , e.url
            , split_part(e.name, '|', 1) AS name
            , NULL AS theme
            , e.description
            , DECODE(e.online_event, TRUE, 'Online', 'In-person') AS medium
            , a.affiliate
            , e.start
            , e.end
            , e.venue_name
            , e.venue_address_1
            , e.venue_address_2
            , e.venue_city
            , e.venue_postal_code
            , e.venue_region AS venue_state
            , e.venue_country
            , a.profile_email AS email
            , a.status 
            , a.checked_in AS attended
        FROM 
            {{ params.table_refs["eventbrite.events"] }} e
        LEFT JOIN stitch_marketo_production.programs p 
            ON e.id = regexp_substr(p.description, '(\\d+)', 1, 1, 'e')
        LEFT JOIN 
            {{ params.table_refs["eventbrite.attendees"] }} a 
            ON e.id = a.event_id
        WHERE 
            e.start >= '2019-01-01'
            AND p.id IS NULL -- Exclude events already captured in Marketo

        UNION ALL
        
        SELECT 
            e.id
            , 'Splash' AS source
            , e.fq_url AS url
            , e.title AS name
            , e.type AS theme
            , e.description_text AS description
            , DECODE(e.title ~* 'virtual', TRUE, 'Online', 'In-person') AS medium
            , NULL AS affiliate
            , e.event_start AS start
            , e.event_end AS end
            , e.venue_name
            , e.venue_address AS venue_address_1
            , NULL AS venue_address_2
            , e.venue_city
            , e.venue_postal_code
            , e.venue_region AS venue_state
            , e.venue_country
            , c.contact_primary_email AS email
            , c.status
            , c.status = 'checkin_yes' AS attended
        FROM 
            {{ params.table_refs["splash.events"] }} e 
        LEFT JOIN 
            {{ params.table_refs["splash.contacts"] }} c
            ON e.id = c.event_id
        WHERE 
            e.type NOT LIKE '%Recruiting%' 
            AND e.title NOT LIKE '%Recruiting%'

        UNION ALL

        SELECT
            e.webinar_id AS id
            , 'Go_to_Webinar' AS source
            , e.registration_url AS url
            , e.subject AS name
            , 'Webinar' AS theme
            , e.description
            , 'Online' AS medium
            , NULL AS affiliate
            , CAST(json_extract_path_text(TRIM('\\[|\\]' FROM json_extract_array_element_text(times,0)), 'startTime') AS TIMESTAMP) AS start
            , CAST(json_extract_path_text(TRIM('\\[|\\]' FROM json_extract_array_element_text(times,0)), 'endTime') AS TIMESTAMP) AS end
            , 'Online' AS venue_name
            , NULL AS venue_address_1
            , NULL AS venue_address_2
            , 'Online' AS venue_city
            , 'Online' AS venue_state
            , 'Online' AS venue_postal_code
            , 'Online' AS venue_country
            , r.email
            , NULL AS status
            , a.attendance IS NOT NULL AS attended
        FROM 
            {{ params.table_refs["go_to_webinar.webinars"] }} e
        INNER JOIN 
            {{ params.table_refs["go_to_webinar.registrants"] }} r 
            ON e.webinar_key = r.webinar_key
        LEFT JOIN 
            {{ params.table_refs["go_to_webinar.attendees"] }} a 
            ON r.email = a.email
            AND r.webinar_key = a.webinar_key
    ) AS events 
LEFT JOIN 
    (
        SELECT 
            email 
            , billing_city
            , billing_state
            , billing_country
            , mailing_city
            , mailing_state
            , mailing_country
            , lead_city
            , lead_state
            , lead_country
            , applicant_dma
            , applicant_in_market
            , marketo_campus_interest
            , marketo_lead_provided_state
            , marketo_ip_state
            , marketo_inferred_state
            , application_submission_date 
            , admitted_date
            , committed_date
            , close_date
            , stage_name
            , matriculation_date
            , fift_source
            , ROW_NUMBER() OVER (PARTITION BY email ORDER BY application_submission_date DESC) AS app_order
        FROM {{ params.table_refs["admissions"] }} 
    ) AS admissions
    ON events.email = admissions.email 
    AND admissions.app_order = 1
{% endblock %}
