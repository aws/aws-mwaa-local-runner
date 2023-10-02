with expanded as (
    select student_uuid 
         , c.date
    from fis.calendar c 
    left join (
    select student_uuid
         , min(updated_at::date) min_date
    from canvas_consumer.assets a
    group by 1) min_date
        on c.date between min_date.min_date and current_date
    )
select student_uuid
     , "date"
     , list_views
     , content_views
     , submissions
     , participations
     , case when streak_id = 0 then 0 else row_number() over(partition by student_uuid, streak_id order by "date") end streak
     , case when streak_id = 0 then False else row_number() over(partition by student_uuid, streak_id order by "date" desc) = 1 end streak_end
     , last_content_view_date   
     , last_activity_date
from (
        select *
            , case when content_views > 0 
              then sum(streak_start) over(partition by student_uuid order by "date" rows between unbounded preceding and current row)
              else 0 end streak_id
        from (
            select *
                , last_value(case when content_views > 0 then "date" end ignore nulls) 
                                 over(partition by student_uuid 
                                      order by "date" asc 
                                      rows between unbounded preceding and current row) last_content_view_date
               , last_value(case when content_views > 0 or list_views > 0 then "date" end ignore nulls)
                                                over(partition by student_uuid 
                                      order by "date" asc 
                                      rows between unbounded preceding and current row)  last_activity_date
               , case when datediff(day, last_content_view_date, "date") >= 1 then 1
                      when content_views >= 1 and lag("date") over(partition by student_uuid order by "date") is null 
                      then 1
                      else 0 end streak_start
            from (
                  select e.student_uuid
                         , e.date
                         , count(case when nvl(a.content_list, False) then 1 end) list_views
                         , count(case when not nvl(a.content_list, True) then 1 end) content_views
                         , count(case when nvl("level", '') = 'submit' then 1 end) submissions
                         , count(case when nvl("level", '') = 'participate' then 1 end) participations
                    from expanded e
                    left join canvas_consumer.assets a
                        on e.student_uuid = a.student_uuid 
                        and e.date = a.updated_at::date
                    left join fis.students s
                        on e.student_uuid = s.learn_uuid
                    where s.email !~ 'flatironschool'
                    group by 1, 2
            )
            )
       )