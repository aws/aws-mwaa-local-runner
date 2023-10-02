{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    c.id
    , c.title
    , c.url 
    , c.description
    , c.created_at
    , c.updated_at
    , c.type
    , c.format
    , i.name AS institution
    , LISTAGG(DISTINCT ss.status, ', ') AS student_status
    , LISTAGG(DISTINCT pd.name, ', ') AS discipline
    , pp.name AS phase
    , pm.name AS module
    , LISTAGG(DISTINCT t.name, ', ') AS tags
FROM 
    {{ params.table_refs["service_content.contents"] }} c
LEFT JOIN 
    {{ params.table_refs["service_content.contents_institutions"] }} ci 
    ON c.id = ci.content_id 
LEFT JOIN 
    {{ params.table_refs["service_content.institutions"] }} i
    ON ci.institution_id = i.id 
LEFT JOIN 
    {{ params.table_refs["service_content.contents_student_statuses"] }} css
    ON c.id = css.content_id 
LEFT JOIN 
    {{ params.table_refs["service_content.student_statuses"] }} ss
    ON css.student_status_id = ss.id 
LEFT JOIN 
    {{ params.table_refs["service_content.contents_program_disciplines"] }} cpd 
    ON c.id = cpd.content_id
LEFT JOIN 
    {{ params.table_refs["service_content.program_disciplines"] }} pd
    ON cpd.program_discipline_id = pd.id 
LEFT JOIN 
    {{ params.table_refs["service_content.contents_program_phases"] }} cpp 
    ON c.id = cpp.content_id
LEFT JOIN 
    {{ params.table_refs["service_content.program_phases"] }} pp
    ON cpp.program_phase_id = pp.id 
LEFT JOIN 
    {{ params.table_refs["service_content.contents_program_modules"] }} cpm
    ON c.id = cpm.content_id
LEFT JOIN 
    {{ params.table_refs["service_content.program_modules"] }} pm
    ON cpm.program_module_id = pd.id 
LEFT JOIN 
    {{ params.table_refs["service_content.contents_tags"] }} ct2 
    ON c.id = ct2.content_id 
LEFT JOIN 
    {{ params.table_refs["service_content.tags"] }} t
    ON ct2.tag_id = t.id
GROUP BY 
    c.id
    , c.title
    , c.url 
    , c.description
    , c.created_at
    , c.updated_at
    , c.type
    , c.format
    , i.name
    , pp.name
    , pm.name
{% endblock %}
