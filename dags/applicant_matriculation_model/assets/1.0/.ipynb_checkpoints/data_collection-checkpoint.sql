select
      applications.id 
    , applications.createddate
    , applications.referral_source__c application_referral_source
    , applications.interested_in_full_time_cohort__c interested_in_full_time_cohort
    , applications.course__c application_discipline
    , nvl(regexp_count(applications.professional_background__c, ''\\w+''), 0) professional_background_word_count
    , case when applications.pacing_interest__c ~* ''sure'' then ''Not Sure'' 
           else applications.pacing_interest__c end application_pacing
    , case when applications.scholarship_code__c is not null then 1 else 0 end applied_for_scholarship
    , nvl(regexp_count(applications.technical_background__c, ''\\w+''), 0) technical_background_word_count
    , applications.education__c education
    , decode(applications.market_interest__c, ''London'', ''London'', zip_to_msa.msa_name) as applicant_location
    , nvl(regexp_count(applications.professional_background__c || '' '' || applications.technical_background__c, '' i '')) lower_i
    , case when leads.x10_minute_chat_date__c is not null then 1 else 0 end ten_minute_chat
    , marketing.paid_non_paid
    , applications.professional_goal__c AS professional_goal
    , case when leads.downloaded_outcomes_report_date__c is not null then 1 else 0 end as downloaded_outcomes
    , applications.technical_background__c technical_background
    , applications.professional_background__c professional_background
from stitch_salesforce.application__c applications
left join stitch_salesforce.lead leads
    on leads.id = applications.lead__c
    and applications.createddate::date >= ''2019-05-19''
    and not applications.isdeleted
    and not leads.isdeleted
left join data_analytics.campus_zips as campus_zips 
    on applications.market_interest__c = campus_zips.campus
left join data_analytics.zip_to_msa as zip_to_msa
    on nvl(campus_zips.zip_code, applications.postal_code__c) = zip_to_msa.zip_code
left join staging.marketing_sources as marketing
    on applications.email__c = marketing.email
where  applications.createddate::timestamp > ''{{ start_time }}''::timestamp
       and applications.createddate::timestamp <= ''{{ end_time }}''::timestamp
order by applications.createddate::timestamp desc;