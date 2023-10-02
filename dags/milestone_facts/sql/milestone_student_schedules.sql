{% extends "rotate_table.sql" %}
{% block query %}
/* STUDENT TEMPLATES
 * 1. Collect the most recent pacing selection for each student
 * 2. Join to pacing template and collect the milestone_template_id
 */
with student_templates as (
                           select student_uuid
                                , milestone_template_id 
                           from (
                                select student_uuid
                                     , pacing_template_id
                                     , row_number() over(partition by student_uuid order by updated_at desc) rnk
                                from service_milestones.student_pace_selections sps
                                ) pacings
                           left join service_milestones.pacing_templates pt
                             on pacings.pacing_template_id = pt.id
                             and rnk = 1 -- Remove all rows except for the most recent pacing selection
)
/* LESSON COMPLETION
 * 1. Collect the earliest completion date per student and milestone
 * 2. Identify the highest ordinality milestone a student has completed
 * 3. Collect milestone details from fis.milestone_fact
 */
, lesson_completion as (
                  select *
                       , sum(case when rnk = 1 and completed_at is not null then 1 else 0 end) 
                           over(partition by student_uuid) = 0 no_milestones_completed
                         -- Find the highest ordinality lesson that has been completed
                       , case when no_milestones_completed
                              then false
                              else row_number() 
                                   over(partition by student_uuid 
                                        order by (case when rnk = 1 and completed_at is not null
                                          			   then 1 else 0 end * nvl("ordinality", 0)) desc) = 1
                           end previous_milestone
                  from    
				  (select st.student_uuid
                         , mf.*
                         , case when json_extract_path_text(e.data, 'completed') = 'true'
                                then e.occurred_at 
                                else null end completed_at
                        -- rank columns to filter down to first completion date
                         , row_number() over(partition by st.student_uuid, mf.milestone_uuid order by e.occurred_at desc) rnk
                    from student_templates st 
                    left join {{ params.table_refs["milestone_fact"] }} mf  -- DAG DEPENDENCY
                        on st.milestone_template_id = mf.template_id 
                        and mf.discarded_at is null
                    left join service_milestones.events e
                        on e.resource_uuid = mf.milestone_uuid
                        and e.actor_uuid = st.student_uuid
                        and e."type" = 'MilestoneCompletionEvent'
                    where milestone_uuid is not null))
select student_uuid
     , sum(allotted_days) over(partition by student_uuid) total_allotted_days
     , sum(case when completed_at is not null then allotted_days else 0 end)
       over(partition by student_uuid) total_allotted_days_completed
     , datediff(day, s.most_recent_cohort_start_date, current_date) days_in_program
     , datediff(day, current_date, s.most_recent_cohort_end_date) program_remaining_days
     , s.most_recent_cohort_end_date program_end_date
     , milestone_uuid
     , milestone_name 
     , completed_at
     , "ordinality" 
     , allotted_days milestone_allotted_days
     , description
     , discipline
     , course_name
     , no_milestones_completed
     , previous_milestone
     , rnk
     -- Current Milestone Indicator
     , case 
        when no_milestones_completed
        then row_number() over(partition by student_uuid order by "ordinality") = 1
        else lag(previous_milestone) over(partition by student_uuid order by "ordinality") is not null
         and lag(previous_milestone) over(partition by student_uuid order by "ordinality")
       end current_milestone
    -- Next Milestone Indicator
    , case 
        when no_milestones_completed
        then row_number() over(partition by student_uuid order by "ordinality") = 2
        else lag(previous_milestone, 2) over(partition by student_uuid order by "ordinality") is not null
         and lag(previous_milestone, 2) over(partition by student_uuid order by "ordinality")
      end next_milestone
    -- Milestone is incomplete and following the current_milestone
    , case 
        when no_milestones_completed
        then true
        else "ordinality" > max(case when previous_milestone then "ordinality" end) over(partition by student_uuid) 
      end remaining
    /* 
     * Calculate the total number of allotted assignment days for
     * all milestones following a given milestone
     * and subtract the total from the program end date
     * 
     * 
     * ie...you must complete x milestone in order to have enough time to complete y and z milestones
     * where the z milestone's due date is the same as the program end date
     */
    , dateadd(day, -1 * nvl(sum(allotted_days) over(partition by student_uuid 
                                                    order by "ordinality" 
                                                    rows between 1 following 
                                                    and unbounded following)::integer, 0), 
                                                    s.most_recent_cohort_end_date) due_on
    , completed_at is null and sum(case when completed_at is null then 0 else 1 end) 
                               over(partition by student_uuid 
                                    order by "ordinality"
                                    rows between current row 
                                    and unbounded following) > 0  skipped
from lesson_completion lc
left join fis.students s
    on lc.student_uuid = s.learn_uuid
where lc.rnk = 1
order by student_uuid, "ordinality"
{% endblock %}