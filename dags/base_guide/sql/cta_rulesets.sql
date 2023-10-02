{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    cta.*
    , DECODE(crs.student_status
        , 0, 'prospective'
        , 1, 'committed'
        , 3, 'alumni') AS student_status
    , to2.name
    , rto.complete
FROM 
    student_home.call_to_actions cta
JOIN 
    student_home.cta_rule_sets crs 
    ON cta.id = crs.call_to_action_id 
JOIN 
    student_home.ruleset_task_options rto 
    ON crs.id = rto.cta_rule_set_id 
JOIN 
    student_home.task_options to2 
    ON rto.task_option_id = to2.id
{% endblock %}