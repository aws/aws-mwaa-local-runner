SELECT
    CASE WHEN count(distinct kit_id)= count(kit_id) THEN 'column values are unique' ELSE 'column values are NOT unique' END AS distinct_check
  /*  CASE WHEN interp_ordered_at::date < interp_received_at::date THEN TRUE ELSE FALSE END AS interp_order_check,
    CASE WHEN confirmation_triggered_at::date < confirmation_ordered_at::date THEN TRUE ELSE FALSE END AS confimation_trigger_check,
    CASE WHEN confirmation_ordered_at::date < confirmation_received_at::date THEN TRUE ELSE FALSE END AS confimation_ordered_check,
    CASE WHEN clinical_rev_requested_at::date < clinical_rev_approved_at::date THEN TRUE ELSE FALSE END AS clinical_review_check,
    CASE WHEN interp_returned_at::date < results_published_at::date THEN TRUE ELSE FALSE END AS interp_resturned_check */
FROM {{ params.table }} ;