select course_id, user_id from (
select ed.type
     , ed.workflow_state
     , c.name
     , cd2.*
     , cd.name course_name
     , cd.canvas_id course_id
     , ud.canvas_id user_id
     , row_number() over(partition by cd.canvas_id) rnk
FROM 
    canvas.course_dim cd
JOIN 
    canvas.enrollment_dim ed
    ON ed.course_id = cd.id
JOIN 
    registrar.canvas_data cd2
    on (cd.account_id - 158020000000000000)::int = REGEXP_SUBSTR(cd2.url, 'accounts\\/(\\d+)', 1, 1, 'e')::int
join 
    registrar.cohorts c 
    on cd2.resource_uuid = c.uuid
join 
    canvas.user_dim ud 
    on ed.user_id = ud.id
where 
    ed.type = 'TeacherEnrollment' and ed.workflow_state = 'active' -- active teacher enrollments
    and current_date between c.start_date::date and c.end_date::date  -- active cohorts
)
where rnk = 1;