create table if not exists {{ params["schema"] }}.{{ params["table"] }} (
    student_id varchar(280)
  , student_uuid varchar(280)
  , course_name varchar(280)
  , course_id  varchar(280)
  , module_item_id varchar(280)
  , assignment_title varchar(280)
  , url varchar(280)
  , kind varchar(280)
  , action varchar(280)
  , interaction_seconds float
  , created_at timestamp
  , participated boolean
)