{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    m.*
    , ct.owner_uuid
    , ct.created_at AS token_created_at
    , ct.updated_at AS token_updated_at
    , ct.expires_at AS token_expires_at
FROM {{ params.members_table.full_name }} m
LEFT JOIN easely.chat_tokens ct
    ON m.id = ct.discord_user_id
{% endblock %}