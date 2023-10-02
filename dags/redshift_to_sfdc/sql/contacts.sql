{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    c.id
    , i.hosted_invoice_url AS student_facing_enroll_link__c
FROM 
    (
        SELECT 
            i.invoicee_uuid
            , i.hosted_invoice_url 
            , ROW_NUMBER() OVER (PARTITION BY i.invoicee_uuid ORDER BY i.updated_at DESC) AS ord
        FROM registrar.invoices i
        JOIN registrar.invoice_categories ic 
            ON i.invoice_category_id = ic.id
        WHERE
            ic.name = 'deposit'
            AND i.closed_at IS NULL
    ) i 
JOIN stitch_salesforce.contact c
    ON i.invoicee_uuid = c.learn_uuid__c
    AND NOT c.isdeleted
    AND i.ord = 1
WHERE
    -- Get any contacts with NULL or different links
    i.hosted_invoice_url <> NVL(c.student_facing_enroll_link__c, 'NA')
{% endblock %}