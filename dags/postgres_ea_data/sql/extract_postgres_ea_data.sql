with ce as (
    select *
    from public.compliance_events
    where type = 'Events::CompletedEnvelopeParsed'
    and updated_at between '{{ params.data_interval_start }}' and '{{ params.data_interval_end }}'
)
select
    "tabId"
    , "tabLabel"
    , "value"
    , ce.id as compliance_event_id
    , ce.data ->> 'envelope_id' as envelope_id
    , ce.requirement_uuid
    , ce.actor_uuid
    , ce.recipient_uuid
    , ce.updated_at
from 
    ce, 
    json_to_recordset(ce.data->'envelope_data'->'textTabs') as x("tabId" text, "tabLabel" text, "value" text)
where 
    "tabLabel" in ('Home Street Address', 'Home City', 'Home Zip')
union all
select
    "tabId"
    , 'Home State' as tabLabel
    , "value"
    , ce.id as compliance_event_id
    , ce.data ->> 'envelope_id' as envelope_id
    , ce.requirement_uuid
    , ce.actor_uuid
    , ce.recipient_uuid
    , ce.updated_at
from 
    ce, 
    json_to_recordset(ce.data->'envelope_data'->'listTabs') as x("tabId" text, "tabLabel" text, "value" text)
where 
    "tabLabel" = 'Dropdown 179b4194-4473-442c-9787-f74060799c1c'