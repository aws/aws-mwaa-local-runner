{% extends "rotate_table.sql" %}
{% block query %}
select s.learn_uuid
     , max(case when r.removed_from_roster_action_module = 'Module 1' and r.removed_from_roster_action_type = 'Required Repeat' then 1 else 0 end) = 1 phase_1_repeat
     , max(case when r.removed_from_roster_action_module = 'Module 2' and r.removed_from_roster_action_type = 'Required Repeat' then 1 else 0 end) = 1 phase_2_repeat
     , max(case when r.removed_from_roster_action_module = 'Module 3' and r.removed_from_roster_action_type = 'Required Repeat' then 1 else 0 end) = 1 phase_3_repeat
     , max(case when r.removed_from_roster_action_module = 'Module 4' and r.removed_from_roster_action_type = 'Required Repeat' then 1 else 0 end) = 1 phase_4_repeat
     , max(case when r.removed_from_roster_action_module = 'Module 5' and r.removed_from_roster_action_type = 'Required Repeat' then 1 else 0 end) = 1 phase_5_repeat
from {{ params.table_refs["students"] }} s
left join {{ params.table_refs["rosters"] }} r 
on s.learn_uuid = r.learn_uuid 
where s.matriculated_cohort_start_date is not null
group by 1
{% endblock %}