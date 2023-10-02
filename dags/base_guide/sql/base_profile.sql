{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    -- Basic Info
    s.student_uuid
    , s.first_name
    , s.last_name
    , s.preferred_first_name
    , s.preferred_pronouns
    -- Addresses
    , ha.line_1 as home_address_line_1
    , ha.line_2 as home_address_line_2
    , ha.city as home_address_city
    , NULLIF(ha.state, '') as home_address_state
    , ha.zipcode as home_address_zipcode
    , ha.country as home_address_country
    , sa.line_1 as shipping_address_line_1
    , sa.line_2 as shipping_address_line_2
    , sa.city as shipping_address_city
    , NULLIF(sa.state, '') as shipping_address_state
    , sa.zipcode as shipping_address_zipcode
    , sa.country as shipping_address_country
    , s.timezone
    , s.daylight_savings_time
    , s.phone_number
    , s.avatar
    -- Emergency Contact
    , s.emergency_contact_email
    , s.emergency_contact_name
    , s.emergency_contact_phone
    , s.emergency_contact_relationship
    -- Coaching Fields
    , s.linked_in_profile_url
    , s.blog_url
    , s.portfolio_site_url
    , s.skilled_interview_feedback_url
    , s.skilled_mentor_feedback_url
    , s.resume_url
    -- Diversity Info
    , NULLIF(s.race, '') AS race
    , NULLIF(s.ethnicity, '') AS ethnicity
    , NULLIF(s.gender, '') AS gender
    , i.parent
    , i.disabled
    , i.lgbtq
    , i.transgender
    , INITCAP(s.military_status) AS military_status
    , NULLIF(INITCAP(s.educational_background), '') AS educational_background
    , s.tshirt_size
from student_home.students s
left join student_home.home_addresses ha
    on s.student_uuid = ha.student_uuid
left join student_home.shipping_addresses sa
    on s.student_uuid = sa.student_uuid
left join 
    (
        select
            sit.student_uuid
            , max(case when it.category = 'Parent' then 1 end) as parent
            , max(case when it.category = 'Disabled' then 1 end) as disabled
            , max(case when it.category = 'LGBTQ+' then 1 end) as lgbtq
            , max(case when it.category = 'Transgender' then 1 end) as transgender
        from student_home.student_identity_terms sit
        join student_home.identity_terms it
        on sit.identity_term_id = it.id
        group by 1
    ) i
    on s.student_uuid  = i.student_uuid
{% endblock %}