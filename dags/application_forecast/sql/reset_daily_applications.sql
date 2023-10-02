{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    CONVERT_TIMEZONE('America/New_York', createddate)::DATE AS ds
    , COUNT(1)::FLOAT AS y
FROM stitch_salesforce.application__c
WHERE 
    ds BETWEEN '2020-01-01' AND '{{ ds }}' - 1
    AND (
        email__c !~* '(mrkt)|(flatironschool)|(qq\\.com)'
        AND NVL(scholarship_code__c, 'null') != 'DELOITTE'
        AND NVL(scholarship_code__c, 'null') != 'BLKROCK'
        AND NVL(scholarship_code__c, 'null') != 'DELOITTECLOUD'
        AND NVL(scholarship_code__c, 'null') != '2023AMZNCC'
    )
GROUP BY 1
{% endblock %}