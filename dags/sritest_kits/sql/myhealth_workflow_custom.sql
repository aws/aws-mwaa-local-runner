SELECT DISTINCT kit_id, 
    MIN(CASE WHEN event = 'interpretation-ordered' THEN created_at ELSE NULL END) AS interp_ordered_at,
    MIN(CASE WHEN event IN ('interpretation-recieved','interpretation-received') THEN created_at ELSE NULL END) AS interp_received_at,
    MIN(CASE WHEN event IN ('result-published','result_published','results-published','results_published') THEN created_at ELSE NULL END) AS results_published_at,
    MIN(CASE WHEN event = 'interpretation-returned' THEN created_at ELSE NULL END) AS interp_returned_at,
    MIN(CASE WHEN event = 'confirmation-triggered' THEN created_at ELSE NULL END) AS confirmation_triggered_at, 
    MIN(CASE WHEN event = 'confirmation-tracked' THEN created_at ELSE NULL END) AS confirmation_tracked_at,
    MIN(CASE WHEN event = 'confirmation-rejected' THEN created_at ELSE NULL END) AS confirmation_rejected_at,
    MIN(CASE WHEN event = 'confirmation-ordered' THEN created_at ELSE NULL END) AS confirmation_ordered_at,
    MIN(CASE WHEN event = 'confirmation-received' THEN created_at ELSE NULL END) AS confirmation_received_at,
    MIN(CASE WHEN event = 'clinical-review-requested' THEN created_at ELSE NULL END) AS clinical_rev_requested_at,
    MIN(CASE WHEN event = 'clinical-review-approved' THEN created_at ELSE NULL END) AS clinical_rev_approved_at
FROM projection_popgen.myhealth_workflow
GROUP BY 1