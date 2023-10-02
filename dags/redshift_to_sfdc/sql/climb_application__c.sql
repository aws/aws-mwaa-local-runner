{% extends "rotate_table.sql" %}
{% block query %}
with apps as (
    select 
        app_id as climb_id__c
        , 'Ascent' as application_type__c
        , borrower_email as email__c
        , borrower_first_name as first_name__c
        , borrower_last_name as last_name__c
        , requested_amount as finance_amount__c
        , requested_living_amount as living_stipend__c
        , status as status__c
        , last_updated::timestamp as lastupdateddate__c
    from fis.ascent_sheet
    union all 
    select 
        ref as climb_id__c
        , 'Climb' as application_type__c
        , student_email as email__c
        , student_first_name as first_name__c
        , student_last_name as last_name__c
        , amount_requested as finance_amount__c
        , living_expenses as living_stipend__c
        , status as status__c
        , last_updated::timestamp as lastupdateddate__c
    from fis.climb_sheet
    union all
    select 
        application_id as climb_id__c
        , 'EdAid' as application_type__c
        , email as email__c
        , first_name as first_name__c
        , last_name as last_name__c
        , regexp_replace(deferred_amount, '\\$|\\,') finance_amount__c
        , null as living_stipend__c
        , status as status__c
        , null::timestamp as lastupdateddate__c
    from fis.edaid_sheet
)
select 
    apps.*
from apps
left join stitch_salesforce.climb_application__c as sfdc
    on apps.climb_id__c = sfdc.climb_id__c
where 
    -- Scenarios for updating
    -- 1. Doesn't exist in SFDC
    -- 2. Sheet was updated more recently than SFDC
    sfdc.climb_id__c is null
    or apps.lastupdateddate__c > sfdc.lastupdateddate__c::timestamp
{% endblock %}