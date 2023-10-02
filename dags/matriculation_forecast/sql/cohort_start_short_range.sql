create table if not exists {{ params["schema"] }}.{{ params["table"] }} (
    id bigint identity(1,1)
  , created_at timestamp
  , cohort_start_date date
  , days_till_start integer
  , static_prediction integer
  , auto_train_prediction integer
)