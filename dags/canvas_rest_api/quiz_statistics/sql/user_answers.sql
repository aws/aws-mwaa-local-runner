create table if not exists {{ params["schema"] }}.{{ params["table"] }} (
    question_id     integer
  , quiz_id         integer
  , course_id       integer
  , user_id         integer
  , user_name       varchar(440)
  , answer_text     varchar(4000)
  , correct         boolean
);