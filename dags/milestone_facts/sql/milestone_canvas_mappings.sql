{% extends "rotate_table.sql" %}
{% block query %}
select -- blueprint
       bp.name blueprint_name
       -- course
     , c.name course_name
       -- module
     , cm.name module_name
     , cm.position module_position
     , cm.published module_published
       -- module items
     , cmi.title module_item_name
     , cmi.position module_item_position
     , cmi.content_type module_item_type
     , cmi.published module_item_published
     , cmi.html_url module_item_url
       -- milestones
     , m.name milestone_name
     , m.ordinality milestone_ordinality
     , m.description milestone_description
       -- ids
     , bp.external_id blueprint_canvas_id
     , bp.id blueprint_service_id
     , c.external_id course_canvas_id
     , c.id course_service_id
     , cm.external_id module_canvas_id
     , cm.id module_service_id
     , cmi.external_id module_item_canvas_id
     , cmi.id module_item_service_id
     , m.id milestone_id
     , m.uuid milestone_uuid
     , mimaps.source_module_item_id blueprint_module_item_id
     , mcm.id module_item_milestones_mapping_id
-- blueprint --> courses
from service_milestones.canvas_courses bp
left join service_milestones.canvas_course_mappings cmaps 
    on bp.id = cmaps.source_course_id
left join service_milestones.canvas_courses c
    on cmaps.student_course_id = c.id
-- courses --> modules
left join service_milestones.canvas_modules cm
    on c.external_id = cm.external_course_id
-- modules --> items
left join service_milestones.canvas_module_items cmi
    on c.external_id = cmi.external_course_id
    and cm.external_id = cmi.external_module_id 
-- items to blue print items
left join service_milestones.canvas_module_item_mappings mimaps 
    on cmi.id = mimaps.student_module_item_id 
-- blue print items to milestones
left join service_milestones.milestones_canvas_milestones mcm
    on mimaps.source_module_item_id = mcm.canvas_module_item_id 
left join service_milestones.milestones m 
    on mcm.milestone_id = m.id
where bp.blueprint = 'true'
{% endblock %}