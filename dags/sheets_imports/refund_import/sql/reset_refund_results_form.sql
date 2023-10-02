DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    external_id                         VARCHAR(64)
    , form_submitted_at                 TIMESTAMP
    , student_name	                    VARCHAR(128)
    , course	                        VARCHAR(64)
    , pacing	                        VARCHAR(32)
    , start_date	                    DATE
    , withdrawal_date	                DATE
    , duration_of_enrollment	        FLOAT
    , finance_method	                VARCHAR(64)
    , exit_type	                        VARCHAR(32)
    , withdrawal_reason	                VARCHAR(32)
    , additional_withdrawal_details	    VARCHAR(256)
    , reason_for_dismissal	            VARCHAR(64)
    , refund_amount	                    FLOAT
    , refund_results_form_submitted	    DATE
    , withdrawal_executed	            VARCHAR(16)
    , date_withdrawal_executed	        DATE
    , zendesk_ticket  	                VARCHAR(32)
    , last_lesson_completed             VARCHAR(64)
)
