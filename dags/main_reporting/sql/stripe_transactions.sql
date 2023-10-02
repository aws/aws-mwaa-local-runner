{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    TIMESTAMP 'epoch' + balance_transactions.available_on * INTERVAL '1 second' AS deposit_date
    , external_ids.external_id
    , charges.receipt_email AS email
    , charges.customer AS stripe_customer_id
    , NVL(invoice_lines.plan__name, invoice_lines.description) AS invoice_description
    , invoice_lines.plan__id AS stripe_course
    , invoices.discount__coupon__name AS scholarship_detail
    , invoices.subtotal / 100.0 AS gross_tuition
    , invoices.discount__coupon__amount_off / 100.0 AS scholarship
    , invoices.starting_balance / 100.0 AS starting_balance
    , charges.amount / 100.0 AS net_balance
    , charges.amount_refunded / 100.0 AS amount_refunded
    , DATE(charges.created) AS payment_date
    , charges.currency AS currency
    , charges.description AS payment_description
    , rosters.financing_partner AS financial_partner
    , charges.paid AS is_paid
    , NVL(card.address_state, charges.billing_details__address__state) AS sales_state
    , NVL(card.address_country, charges.billing_details__address__country) AS sales_country
    , NVL(card.address_zip, charges.billing_details__address__postal_code) AS sales_zip_code
    , NVL(customers.metadata__first_name, rosters.first_name, users_id.first_name, users_uuid.first_name, users_email.first_name) AS first_name
    , NVL(customers.metadata__last_name, rosters.last_name, users_id.last_name, users_uuid.last_name, users_email.last_name) AS last_name
    , CASE
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%online%' THEN 'Online'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%community-powered%' THEN 'Online - CPB'
        ELSE rosters.campus
        END AS campus
    , CASE
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%self-paced%' THEN 'Self-Paced'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%part-time%' THEN 'Part Time'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%full-time%' THEN 'Full Time'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%community-powered%' THEN 'CPB'
        ELSE rosters.pacing
        END AS pacing
    , CASE
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%data science%' THEN 'Data Science'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%software engineering%' THEN 'Software Engineering'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%cyber%engineering%' THEN 'Cybersecurity Engineering'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%cyber%analytics%' THEN 'Cybersecurity Analytics'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%design%' THEN 'Design'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%ux%ui%' THEN 'Design'
        WHEN LOWER(NVL(invoice_lines.plan__name, invoice_lines.description)) LIKE '%community-powered%' THEN 'CPB'
        ELSE rosters.discipline
        END AS course
    , NVL(
        TO_DATE(NULLIF(REGEXP_SUBSTR(NVL(invoice_lines.plan__name, invoice_lines.description), '\\((\\w{3} \\d{2}, \\d{4})', 1, 1, 'e'), ''), 'Mon DD, YYYY')
        , rosters.cohort_start_date) AS cohort_start_date
    , NVL(
        TO_DATE(NULLIF(REGEXP_SUBSTR(NVL(invoice_lines.plan__name, invoice_lines.description), ' (\\w{3} \\d{2}, \\d{4})\\)', 1, 1, 'e'), ''), 'Mon DD, YYYY')
        , rosters.cohort_end_date) AS cohort_end_date
    , NULL AS current_cohort_c
    , rosters.first_name || ' ' || rosters.last_name AS sf_full_user_name
    , rosters.cohort_name AS salesforce_cohort_name
    , rosters.account_id AS sf_account_id
    , NVL(users_id.learn_uuid, users_uuid.learn_uuid, users_email.learn_uuid) AS learn_user_uuid
    , NVL(users_id.id, users_uuid.id, users_email.id) AS learn_user_id
    , invoices.id AS stripe_invoice_id
    , invoices.hosted_invoice_url
    , invoices.subscription AS stripe_subscription_id
    , charges.id AS stripe_charge_id
    , NULL AS learn_co_status
    , balance_transactions.status AS stripe_status
    , CASE WHEN registrar.id IS NOT NULL THEN 1 ELSE 0 END AS has_registrar_invoice_created
    , current_date AS run_date
FROM
    {{ params["table_refs"]["stitch_stripe.charges"] }} charges
LEFT JOIN 
    {{ params["table_refs"]["stitch_stripe.invoices"] }} invoices 
    ON charges.invoice = invoices.id
LEFT JOIN 
    {{ params["table_refs"]["stitch_stripe.customers__cards"] }} card 
    ON charges.source__id = card.id
LEFT JOIN 
    {{ params["table_refs"]["stitch_stripe.subscriptions"] }} subscriptions 
    ON invoices.subscription = subscriptions.id
LEFT JOIN 
    {{ params.table_refs["stitch_stripe.invoice_line_items"] }} invoice_lines
    ON invoices.id = invoice_lines.invoice
    AND invoice_lines.subscription IS NULL
LEFT JOIN 
    {{ params["table_refs"]["stitch_stripe.balance_transactions"] }} balance_transactions 
    ON charges.id = balance_transactions.source
LEFT JOIN 
    {{ params["table_refs"]["stitch_stripe.customers"] }} customers 
    ON charges.customer = customers.id
LEFT JOIN 
    {{ params["table_refs"]["learn.users"] }} users_id 
    ON customers.metadata__learn_id = users_id.id
LEFT JOIN 
    {{ params["table_refs"]["learn.users"] }} users_uuid 
    ON customers.metadata__learn_uuid = users_uuid.learn_uuid
LEFT JOIN 
    {{ params["table_refs"]["learn.users"] }} users_email 
    ON charges.receipt_email = users_email.email
LEFT JOIN 
    {{ params["table_refs"]["registrar_events.invoice_created"] }} registrar 
    ON invoices.id = registrar.stripe_invoice_id
LEFT JOIN 
    {{ params.table_refs["rosters"] }} rosters 
    ON NVL(users_id.learn_uuid, users_uuid.learn_uuid, users_email.learn_uuid) = rosters.learn_uuid 
    AND TIMESTAMP 'epoch' + balance_transactions.available_on * INTERVAL '1 second' BETWEEN rosters.added_to_cohort_date AND NVL(rosters.removed_from_cohort_date, CURRENT_DATE)
LEFT JOIN 
    {{ params["table_refs"]["registrar.external_ids"] }} external_ids 
    ON NVL(users_id.learn_uuid, users_uuid.learn_uuid, users_email.learn_uuid) = external_ids.learn_uuid
WHERE
    charges.captured = TRUE
{% endblock %}
