{% extends "rotate_table.sql" %}
{% block query %}
WITH marketo_sources_stg AS (
	SELECT
		email
		, MAX(CASE WHEN asc_order = 1 THEN locationassignment END) AS locationassignment
		, MAX(CASE WHEN asc_order = 1 THEN campus_interest END) AS campus_interest
		, MAX(CASE WHEN asc_order = 1 THEN programassignment END) AS programassignment
		, MAX(CASE WHEN asc_order = 1 THEN fiftsource END) AS fift_source
		, MAX(CASE WHEN asc_order = 1 THEN fiftmedium END) AS fift_medium
		, MAX(CASE WHEN asc_order = 1 THEN fiftcampaign END) AS fift_campaign
		, MAX(CASE WHEN asc_order = 1 THEN fiftterm END) AS fift_term
		, MAX(CASE WHEN asc_order = 1 THEN fimtsource END) AS fimt_source
		, MAX(CASE WHEN asc_order = 1 THEN fimtmedium END) AS fimt_medium
		, MAX(CASE WHEN asc_order = 1 THEN fimtcampaign END) AS fimt_campaign
		, MAX(CASE WHEN asc_order = 1 THEN fimtterm END) AS fimt_term
		, MAX(CASE WHEN desc_order = 1 THEN filtsource END) AS filt_source
		, MAX(CASE WHEN desc_order = 1 THEN filtmedium END) AS filt_medium
		, MAX(CASE WHEN desc_order = 1 THEN filtcampaign END) AS filt_campaign
		, MAX(CASE WHEN desc_order = 1 THEN filtterm END) AS filt_term
		, MAX(CASE WHEN desc_order = 1 THEN startedapplicationpagedetail END) AS started_application_page_detail
		, MAX(CASE WHEN desc_order = 1 THEN state END) AS state
		, MAX(CASE WHEN desc_order = 1 THEN marketo_lead_provided_state END) AS marketo_lead_provided_state
		, MAX(CASE WHEN desc_order = 1 THEN marketo_billing_state END) AS marketo_billing_state
		, MAX(CASE WHEN desc_order = 1 THEN marketo_ip_state END) AS marketo_ip_state
		, MAX(CASE WHEN desc_order = 1 THEN marketo_inferred_state END) AS marketo_inferred_state
		, MAX(CASE WHEN desc_order = 1 THEN city END) AS city
		, MAX(CASE WHEN desc_order = 1 THEN country END) AS country
		, MAX(CASE WHEN desc_order = 1 THEN registrationsourceinfo END) AS registration_source_info
		, MAX(CASE WHEN desc_order = 1 THEN switchupmatch END) AS switchupmatch
		, MAX(CASE WHEN desc_order = 1 THEN coursereportmatchexperience END) AS coursereportmatchexperience
		, MAX(CASE WHEN desc_order = 1 THEN globaleventsource END) AS globaleventsource
		, MAX(CASE WHEN desc_order = 1 THEN marketing_sms_opt_in__c END) AS sms_marketing_opt_in
		, MAX(CASE WHEN desc_order = 1 THEN sms_marketing_opt_in_confirmed__c::INT END) AS sms_marketing_opt_in_confirmed
		, MAX(CASE WHEN desc_order = 1 THEN sms_marketing_opt_in_date__c END) AS sms_marketing_opt_in_date
		, MAX(CASE WHEN desc_order = 1 THEN sms_marketing_opt_out__c::INT END) AS sms_marketing_opt_out
		, MAX(CASE WHEN desc_order = 1 THEN sms_marketing_opt_out_date__c END) AS sms_marketing_opt_out_date
	FROM
		(
			SELECT
				createdat AS created_date
				, email
				, locationassignment
				, where__c AS campus_interest
				, programassignment
				, fiftmedium
				, fiftsource
				, fiftcampaign
				, fiftterm
				, fimtmedium
				, fimtsource
				, fimtcampaign
				, fimtterm
				, filtmedium
				, filtsource
				, filtcampaign
				, filtterm
				, switchupmatch
				, coursereportmatchexperience
				, startedapplicationpagedetail
				, public.clean_state(NVL(state, billingstate, ip_city_location__c, inferredstateregion)) AS state
				, public.clean_state(state) AS marketo_lead_provided_state
				, public.clean_state(billingstate) AS marketo_billing_state
				, public.clean_state(ip_city_location__c) AS marketo_ip_state
				, public.clean_state(inferredstateregion) AS marketo_inferred_state
				, public.clean_city(LOWER(NVL(city, billingcity, inferredcity))) AS city
				, CASE
					WHEN LOWER(NVL(country, billingcountry, idcountry__c, inferredcountry)) = 'usa' 
					THEN 'united states'
					ELSE LOWER(NVL(country, billingcountry, idcountry__c, inferredcountry)) 
					END AS country
				, registrationsourceinfo
				, globaleventsource
				, marketing_sms_opt_in__c
				, sms_marketing_opt_in_confirmed__c
				, sms_marketing_opt_in_date__c
				, sms_marketing_opt_out__c
				, sms_marketing_opt_out_date__c
				, ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_date NULLS LAST) AS asc_order
				, ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_date DESC NULLS LAST) AS desc_order
			FROM
				{{ params.table_refs["stitch_marketo_production.leads"] }}
			WHERE
				email NOT LIKE '%@donotcontact.com%'
				AND email NOT LIKE '%@notthere.com%'
				AND email NOT LIKE '%@none.com%'
				AND email NOT LIKE '%@no.com%'
				AND email NOT LIKE '%flatiron%'
		)
	GROUP BY email
)
, marketing_tactic_ad_format AS (
    SELECT
        email
        , listagg(DISTINCT hubspot_source, '|') AS hubspot_source
        , listagg(DISTINCT hubspot_updated_source, '|') AS hubspot_updated_source
        , listagg(DISTINCT value2fb, '|') AS value2fb
        , listagg(DISTINCT AdFormat, '|') AS AdFormat
        , listagg(DISTINCT platform, '|') AS platform
        , listagg(DISTINCT Tactic, '|') AS tactic
        , listagg(DISTINCT Facebook_national_vs_local, '|') AS facebook_national_vs_local
        , listagg(DISTINCT state, '|') AS state
        , listagg(DISTINCT city, '|') AS city
        , listagg(DISTINCT country, '|') AS country
        , current_date AS run_date
	FROM
		(	
			SELECT
				c.properties__email__value AS email
				, c.properties__hs_analytics_source__value AS hubspot_source
				, NVL(g.ad_network_type, c.properties__hs_analytics_source_data_1__value) AS hubspot_updated_source
				, c.properties__hs_analytics_source_data_2__value AS value2fb
				, CASE 
					WHEN c.properties__hs_analytics_source__value = 'PAID_SEARCH' THEN 'Google'
					WHEN c.properties__hs_analytics_source__value = 'PAID_SOCIAL' AND hubspot_updated_source = 'LinkedIn' THEN 'LinkedIn'
					ELSE 'Facebook'
					END AS platform
				, CASE 
					WHEN platform = 'Facebook' THEN 'Banner'
					WHEN platform = 'LinkedIn' THEN 'InMail'
					WHEN platform = 'Google'
						AND (
							hubspot_updated_source LIKE '%rand%'
							OR hubspot_updated_source = '1783325438'
							OR hubspot_updated_source = '1874157328'
						) 
					THEN 'Branded Search'
					WHEN 
						platform = 'Google'
						AND hubspot_updated_source LIKE '%isp%'
						OR hubspot_updated_source LIKE 'gmail' 
					THEN 'Display'
					WHEN platform = 'Google' AND hubspot_updated_source LIKE 'youtube' THEN 'Youtube'
					ELSE 'Unbranded Search'
					END AS adformat
				, CASE 
					WHEN 
						platform = 'Facebook'
						AND (
							value2fb LIKE '%ret%'
							OR value2fb LIKE '%rt%'
						) 
					THEN 'Retargeting'
					WHEN 
						platform = 'Facebook'
						AND (
							value2fb LIKE '%acq%'
							OR value2fb LIKE 'nyc%'
							OR value2fb LIKE '%usa%'
							OR value2fb LIKE '%dc%'
							OR value2fb LIKE '%atx%'
							OR value2fb LIKE '%sea%'
							OR value2fb LIKE '%dal%'
							OR value2fb LIKE '%nyc%'
							OR value2fb LIKE '%alt%'
							OR value2fb LIKE '%lnd%'
							OR value2fb LIKE '%hou%'
							OR value2fb LIKE '%usa%'
							OR value2fb LIKE '%atl%'
							OR value2fb LIKE '%ldn%'
							OR value2fb NOT LIKE '%ret'
						) 
					THEN 'Acquisition'
					WHEN 
						Platform = 'Google'
						AND (
							hubspot_updated_source LIKE '%rt%'
							OR hubspot_updated_source LIKE '%RET%'
							OR hubspot_updated_source LIKE '%ret%'
						) 
					THEN 'Retargeting'
					WHEN 
						platform = 'Google'
						AND (
							hubspot_updated_source LIKE '%acq%'
							OR hubspot_updated_source LIKE '%usa%'
							OR hubspot_updated_source LIKE '%dc%'
							OR hubspot_updated_source LIKE '%sea%'
							OR hubspot_updated_source LIKE '%atl%'
							OR hubspot_updated_source LIKE '%ldn%'
							OR hubspot_updated_source LIKE '%nyc%'
							OR hubspot_updated_source LIKE '%chi%'
							OR hubspot_updated_source LIKE '%hou%'
							OR hubspot_updated_source LIKE '%mia%'
							OR hubspot_updated_source LIKE '%den%'
							OR hubspot_updated_source LIKE '%la%'
							OR hubspot_updated_source LIKE '%den%'
							OR hubspot_updated_source LIKE '%dfw%'
							OR hubspot_updated_source LIKE '%atx%'
							OR hubspot_updated_source LIKE '%sf%'
							OR hubspot_updated_source LIKE '%usa%'
							OR hubspot_updated_source LIKE '%glb%'
							OR hubspot_updated_source LIKE '%Search%'
						) 
					THEN 'Acquisition'
					WHEN platform = 'LinkedIn' AND value2fb NOT LIKE '%acq%' THEN 'Retargeting'
					WHEN platform = 'LinkedIn' AND value2fb LIKE '%acq%' THEN 'Acquisition'
					ELSE 'N/A'
					END AS tactic
				, CASE 
					WHEN platform = 'Facebook' AND hubspot_updated_source LIKE '%usa%' THEN 'National'
					WHEN platform = 'Facebook' AND hubspot_updated_source NOT LIKE '%usa%' THEN 'Local'
					WHEN platform <> 'Facebook' THEN 'N/A'
					END AS facebook_national_vs_local
				, c.properties__ip_city__value AS city
				, c.properties__ip_state__value AS state
				, c.properties__ip_country__value AS country
			FROM
				{{ params.table_refs["stitch__hubspot.contacts"] }} c
			LEFT JOIN
				{{ params.table_refs["stitch_new_google_ads.ad_performance_report"] }} g 
				ON c.properties__hs_analytics_source__value = g.campaign_id
			WHERE
				c.properties__email__value IS NOT NULL
				AND c.properties__hs_analytics_source__value IS NOT NULL
				AND c.properties__email__value NOT LIKE '%@donotcontact.com%'
				AND c.properties__email__value NOT LIKE '%@notthere.com%'
				AND c.properties__email__value NOT LIKE '%@none.com%'
				AND c.properties__email__value NOT LIKE '%@no.com%'
		)
  	GROUP BY email
)
, segment_sources_stg AS (
	SELECT
		email
		, listagg(all_utm_sources, '|') AS segment_source
		, listagg(all_utm_mediums, '|') AS segment_medium
		, listagg(first_utm_campaign_on_first_page_view, '|') AS first_utm_campaign_on_first_page_view
	FROM {{ params.table_refs["personas_default.users"] }}
	WHERE
		email IS NOT NULL
		AND (
			all_utm_sources IS NOT NULL 
			OR all_utm_sources IS NOT NULL
		)
	GROUP BY email
)
SELECT
	base.email
	, leads.started_application_page_detail
	, leads.locationassignment
	, leads.campus_interest
	, leads.programassignment
	, leads.fift_source
	, leads.fift_medium
	, leads.fift_campaign
	, leads.fift_term
	, u.first_name || ' ' || u.last_name AS alumni_referrer_name
	, u.email AS alumni_referrer_email
	, leads.fimt_source
	, leads.fimt_medium
	, leads.fimt_campaign
	, leads.fimt_term
	, leads.filt_source
	, leads.filt_medium
	, leads.filt_campaign
	, leads.filt_term
	, leads.globaleventsource
	, leads.switchupmatch
	, leads.coursereportmatchexperience
	, leads.sms_marketing_opt_in
	, leads.sms_marketing_opt_in_confirmed
	, leads.sms_marketing_opt_in_date
	, leads.sms_marketing_opt_out
	, leads.sms_marketing_opt_out_date
	, NVL(leads.state, mf.state) AS state
	, leads.marketo_lead_provided_state
	, leads.marketo_billing_state
	, leads.marketo_ip_state
	, leads.marketo_inferred_state
	, mf.state AS hubspot_ip_state
	, NVL(leads.country, mf.country) AS country
	, NVL(leads.city, mf.city) AS city
	, leads.registration_source_info
	, mf.hubspot_source
	, mf.hubspot_updated_source
	, mf.value2fb
	, mf.adformat
	, mf.platform
	, mf.tactic
	, segment.segment_source
	, segment.segment_medium
	, segment.first_utm_campaign_on_first_page_view
	, CASE
		WHEN leads.fift_medium = 'affiliate' THEN 'PAID_AFFILIATE'
		WHEN LOWER(leads.fift_source) ~ '(course.*report)|(lbm_)|(cygnus)|(switchup)|(career.*karma)'
			OR LOWER(leads.fift_source) IN ('certainsource', 'bestcolleges', 'uxbeginner', 'chronicallycapable', 'essentialworkerscom', 'computerscience\\.org', 'degreeme')
			OR NVL(leads.switchupmatch, leads.coursereportmatchexperience) IS NOT NULL THEN 'PAID_AFFILIATE'
		WHEN leads.fift_medium ilike '%paidsocial%' THEN 'PAID_SOCIAL'
		WHEN leads.fift_medium ilike '%ppc%' THEN 'PAID_SEARCH'
		WHEN leads.fift_medium = 'social' THEN 'SOCIAL_MEDIA'
		WHEN leads.fift_medium = 'organic' THEN 'ORGANIC'
		WHEN leads.fift_medium = 'programmatic' THEN 'PAID_PROGRAMMATIC'
		WHEN leads.fift_medium = 'nativeads' THEN 'PAID_NATIVE'
		WHEN leads.fift_medium ilike '%coursereport%' OR leads.fift_medium ilike '%switchup%' THEN 'NON_PAID_AFFILIATE'
		WHEN mf.hubspot_source IN ('DIRECT_TRAFFIC', 'ORGANIC_SEARCH', 'PAID_SOCIAL', 'PAID_SEARCH') THEN mf.hubspot_source
		WHEN mf.hubspot_source IS NOT NULL OR leads.fift_medium IS NOT NULL THEN 'OTHER_CAMPAIGNS'
		ELSE 'UNKNOWN' 
		END AS clean_marketing_source
	, CASE WHEN clean_marketing_source ilike 'paid%' THEN 'PAID' ELSE 'NON_PAID' END AS paid_non_paid
FROM (
		SELECT email
		FROM (
				SELECT email FROM marketo_sources_stg
				UNION ALL
				SELECT email FROM marketing_tactic_ad_format
				UNION ALL
				SELECT email FROM segment_sources_stg
			)
		GROUP BY email
	) base
LEFT JOIN
	marketo_sources_stg leads 
	ON base.email = leads.email
LEFT  JOIN
	marketing_tactic_ad_format mf 
	ON base.email = mf.email
LEFT  JOIN
	segment_sources_stg segment 
	ON base.email = segment.email
LEFT JOIN 
	learn.users u 
	ON leads.fift_campaign = u.learn_uuid
{% endblock %}
