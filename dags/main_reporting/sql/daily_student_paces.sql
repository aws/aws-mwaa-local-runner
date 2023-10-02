{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    cal.reporting_date
    , cal.reporting_month
    , e.actor_uuid AS learn_uuid
    , e.pacing_started_at
    , e.pacing_ended_at
    , NVL(pt.total_time_seconds/60/60/24/7, 60) AS pacing_in_weeks
FROM
    (
        SELECT 
            calendar.date::DATE AS reporting_date
            , TO_CHAR(calendar.date::DATE, 'Mon YYYY') AS reporting_month
		FROM fis.calendar
    ) cal
JOIN
    (
    	SELECT 
            *
            , e.occurred_at AS pacing_started_at
            , LEAD(e.occurred_at) OVER (PARTITION BY e.actor_uuid ORDER BY e.occurred_at) AS pacing_ended_at
    	FROM {{ params.table_refs["service_milestones.events"] }} e
    	WHERE e.type = 'StudentPaceSelectionEvent'
    ) e
    ON e.pacing_started_at::DATE <= cal.reporting_date AND cal.reporting_date < NVL(e.pacing_ended_at::DATE, CURRENT_TIMESTAMP)
LEFT JOIN 
    {{ params.table_refs["service_milestones.pacing_templates"] }} pt 
    ON e.resource_uuid = pt.uuid
{% endblock %}