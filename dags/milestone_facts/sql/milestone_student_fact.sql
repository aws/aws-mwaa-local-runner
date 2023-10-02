{% extends "rotate_table.sql" %}
{% block query %}
/* STUDENT PACINGS
 * this table grabs the most recent student pacing
 * for each student
 */
with student_pacings as (
              select student_uuid
                   , total_time_seconds/60/60/24/7 pacing_in_weeks
                   , datediff(day, pacings.updated_at, current_date) days_at_current_pace
                from (-- Subquery with duplicate rnk field
                      select student_uuid
                           , pacing_template_id
                           , updated_at
                           , row_number() over(partition by student_uuid order by updated_at desc) rnk
                     from service_milestones.student_pace_selections sps
                     ) pacings
            left join service_milestones.pacing_templates pt 
            on pacings.pacing_template_id = pt.id
            where rnk = 1
)
select ss.student_uuid
     , max(discipline) discipline
     , max(case when current_milestone  then milestone_uuid end) current_milestone_uuid
     , max(case when current_milestone  then milestone_name end) current_milestone_name
     , max(case when current_milestone  then due_on         end) current_milestone_deadline
     , max(case when current_milestone  then course_name    end) current_milestone_course
     , datediff(day, current_date, current_milestone_deadline) pacing_to_current_milestone
     , max(case when previous_milestone then milestone_uuid end) previous_milestone_uuid
     , max(case when previous_milestone then milestone_name end) previous_milestone_name
     , max(case when previous_milestone then due_on         end) previous_milestone_deadline
     , max(case when previous_milestone then course_name    end) previous_milestone_course
     , max(case when previous_milestone then completed_at   end) previous_milestone_completion_date
     , datediff(day, previous_milestone_completion_date, previous_milestone_deadline) pacing_to_previous_milestone
     , max(case when next_milestone     then milestone_uuid end) next_milestone_uuid
     , max(case when next_milestone     then milestone_name end) next_milestone_name
     , max(case when next_milestone     then due_on         end) next_milestone_deadline
     , max(case when next_milestone     then course_name    end) next_milestone_course
     , datediff(day, current_date, next_milestone_deadline) pacing_to_next_milestone
     , count(milestone_uuid) total_milestones
     , count(completed_at)::float/total_milestones::float percent_complete
     , count(skipped or null) skipped_milestones
     , count(completed_at) completed_milestones
     , max(program_end_date) program_end_date
     , sum(case when remaining then milestone_allotted_days end) remaining_allotted_days
     , max(sp.pacing_in_weeks) pacing_in_weeks
     , max(sp.days_at_current_pace) days_at_current_pace
     , max(total_allotted_days_completed) total_allotted_days_completed
     , max(days_in_program) days_in_program
     , max(program_remaining_days) program_remaining_days
from {{ params.table_refs["milestone_student_schedules"] }} ss
join student_pacings sp
on ss.student_uuid = sp.student_uuid
group by 1
{% endblock %}
