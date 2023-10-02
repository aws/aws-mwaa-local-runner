{% extends "rotate_table.sql" %}
{% block query %}
select   t.id --join field

        --subtype fields for deprecated tc ticket form

        , MAX(case when title='Group' then value__string else NULL end) as group
        , MAX(case when title='Assignee' then value__string else NULL end) as assignee
        , MAX(case when title='Subject' then value__string else NULL end) as subject
        , MAX(case when title='Description' then value__string else NULL end) as description
        , MAX(case when title='Question Type ' then value__string else NULL end) as question_type_
        , MAX(case when title='Curriculum/Lab Work Related Subtype ' then value__string else NULL end) as curriculum_lab_work_related_subtype_
        , MAX(case when title='Platforms Subtype ' then value__string else NULL end) as platforms_subtype_
        , MAX(case when title='Applied Learning Tool Subtype ' then value__string else NULL end) as applied_learning_tool_subtype_
        , MAX(case when title='Curriculum Issue Subtype ' then value__string else NULL end) as curriculum_issue_subtype_
        , MAX(case when title='Other Subtype ' then value__string else NULL end) as other_subtype_
        
        --subtype field for tc ticket form 12/15/21
        , MAX(case when title='Chat Type' then value__string else NULL end) as chat_type
        , MAX(case when title='Environment Subtype ' then value__string else NULL end) as environment_subtype
        , MAX(case when title='Curriculum Lesson/Lab Broken ' then value__string else NULL end) as curriculum_lesson_lab_broken
        , MAX(case when title='Service not provided' then value__string else NULL end) as service_not_provided
        , MAX(case when title='Curriculum Comprehension' then value__string else NULL end) as curriculum_comprehension
        , MAX(case when title='Environment' then value__string else NULL end) as environment
        , MAX(case when title='Unresponsive student' then value__string else NULL end) as unresponsive_student
        
from zendesk.tickets t 
left join zendesk.ticket_forms tf
    on t.ticket_form_id = tf.id 
        
left join zendesk.ticket_forms__ticket_field_ids tftfi 
    on tf.id = tftfi._sdc_source_key_id
    
left join zendesk.ticket_fields tf2 --where the `title` variable is stored
    on tftfi.value = tf2.id
    
left join zendesk.tickets__custom_fields tcf --where the `value__string` variable is stored
    on t.id = tcf._sdc_source_key_id
    and tf2.id = tcf.id 
    
where tf.name = 'PTC' --form name for tc tickets form 03/29/22
or tf.name = 'AAQ' --form name for tc tickets form 12/15/21
or ticket_form_id = 1500000397661 --form id for deprecated tc ticket form prior to 12/15/21
or tf.name = 'AAQ [Deprecated Version] ' --edge case for collecting other deprecated forms

GROUP BY 1, t.created_at
order by t.created_at desc
{% endblock %}