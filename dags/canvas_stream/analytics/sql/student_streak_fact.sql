-- Set upstream sql files by the file stem
{% set upstream=["daily_streak"] %}
-- Table query
select student_uuid
     , getdate() run_time
     , max(case when "date" = current_date then streak
                when "date" = dateadd(day, -1, current_date) then streak end) current_streak
     , sum(case when datediff(day, "date", current_date) < 14 then content_views end) content_views_last_14_days
     , sum(case when datediff(day, "date", current_date) < 14 then submissions end) submissions_last_14_days
     , count(case when streak = 1 then 1 end) total_streaks
     , avg(case when streak_end then streak end) avg_streak_length
     , max(case when "date" = current_date then last_content_view_date end) last_content_view_date
     , max(case when "date" = current_date then last_activity_date end) last_activity_date
from canvas_consumer.daily_streak
group by student_uuid, current_time
