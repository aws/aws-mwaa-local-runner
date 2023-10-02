{% extends "rotate_table.sql" %}
{% block query %}
select date
     , cohort_start_date::date
     , days_till_start
     , count(case when enrollment_agreement_cleared
                       and financially_cleared
                       and not academically_cleared then 1 end) todo_acad
    , count(case when enrollment_agreement_cleared
                      and not financially_cleared
                      and academically_cleared then 1 end) todo_fin
    , count(case when not enrollment_agreement_cleared
                      and financially_cleared
                      and academically_cleared then 1 end) todo_ea
    , count(case when enrollment_agreement_cleared
                      and not financially_cleared
                      and not academically_cleared then 1 end) todo_acad_fin
    , count(case when not enrollment_agreement_cleared
                      and financially_cleared
                      and not academically_cleared then 1 end) todo_acad_ea
    , count(case when not enrollment_agreement_cleared
                      and not financially_cleared
                      and academically_cleared then 1 end) todo_ea_fin
    , count(case when not enrollment_agreement_cleared
                      and not financially_cleared
                      and not academically_cleared then 1 end) todo_acad_ea_fin
     , count(case when (not enrolled and cohort_order > 1) then 1 end)::float deferrals
     , count(*)::float as committed
     , count(case when enrolled then 1 end)::float enrolled
     , count(case when not enrolled then 1 end)::float not_enrolled
     , count(case when not enrolled and on_roster_day_1 then 1 end)::float on_roster_day_1
     , count(case when on_roster_day_1 then 1 end) matriculation_count
from (
select c.date
     , r.cohort_start_date
     , r.cohort_name
     , datediff(day, c.date, r.cohort_start_date) days_till_start
     , nvl(r.financially_cleared_date <= c.date, false) 
       and nvl(r.enrollment_agreement_cleared_date <= c.date, false) 
       and nvl(r.academically_cleared_date <= c.date, false)  enrolled
     , nvl(r.enrollment_agreement_cleared_date <= c.date, false) enrollment_agreement_cleared
     , nvl(r.academically_cleared_date <= c.date, false)  academically_cleared
     , nvl(r.financially_cleared_date <= c.date, false) financially_cleared
     , r.on_roster_day_1
     , r.cohort_order
from fis.calendar c 
join fis.rosters r 
  on c.date between r.added_to_cohort_date and nvl(r.removed_from_cohort_date, r.cohort_start_date)
  and datediff(day, c.date, r.cohort_start_date) between 0 and 21
  and nvl(r.committed_date <= c.date, false)
  and r.cohort_start_date::date >= ''2022-07-01''::date
join fis.students s 
  on r.learn_uuid = s.learn_uuid
  and r.cohort_start_date <= nvl(s.matriculation_date, dateadd(day, 21, current_date))
  )
group by date, cohort_start_date, days_till_start
{% endblock %}