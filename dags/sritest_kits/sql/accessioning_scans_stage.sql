SELECT *
FROM {{ params.table }} AS staging_table 
LEFT JOIN (SELECT kit_id,status, failure_reason, hold_reason,created_by_user_id, created_at as created_at_scans,
    reviewed_by_user_id, reviewed_at, site, box, well, manifest_uploaded_modified_at, manifest_upload_status, scan_id, modified_at, 
    workflow_type, cancel_reason FROM projection.accessioning_scans) 
USING (kit_id)