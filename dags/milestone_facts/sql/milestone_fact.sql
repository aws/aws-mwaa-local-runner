{% extends "rotate_table.sql" %}
{% block query %}
select m.id milestone_id
     , m.uuid  milestone_uuid
     , m.name milestone_name
     , m.created_at
     , m.updated_at
     , m.discarded_at
     , m.description
     , mt.id template_id
     , me.allotted_seconds::float/60/60/24 allotted_days
     , sum(case when m.discarded_at is null then allotted_days else 0 end) 
       over(partition by mt.id order by m."ordinality" rows between unbounded preceding and current row) cumulative_days
     , m."ordinality"
     , c.name discipline
     , p.name program_name
     , cc.name course_name
     , cm.name module_name
     , cm.position module_position
     , cmi.title module_item_name
     , cmi.position module_item_position
     , lag(m.id, 1) over(partition by template_id order by m."ordinality") previous_milestone_id
     , lag(m.uuid, 1) over(partition by template_id order by m."ordinality") previous_milestone_uuid
     , lag(m.id, 1) over(partition by template_id order by m."ordinality" desc) next_milestone_id
     , lag(m.uuid, 1) over(partition by template_id order by m."ordinality" desc) next_milestone_uuid
from service_milestones.milestones m
left join service_milestones.milestone_templates mt
on m.milestone_template_id = mt.id
left join service_milestones.programs p 
on mt.program_id = p.id
left join registrar.courses c 
on p.discipline_uuid = c.uuid 
left join service_milestones.pacing_templates pt
on mt.id = pt.milestone_template_id 
and pt.name ~* '60 Weeks'
left join service_milestones.milestone_estimates me
on pt.id = me.pacing_template_id
and m.id = me.milestone_id
left join service_milestones.milestones_canvas_milestones mcm 
on m.id = mcm.milestone_id 
left join service_milestones.canvas_module_items cmi 
on mcm.canvas_module_item_id = cmi.id 
left join service_milestones.canvas_modules cm 
on cmi.external_module_id = cm.external_id 
left join service_milestones.canvas_courses cc 
on cm.external_course_id = cc.external_id 
{% endblock %}