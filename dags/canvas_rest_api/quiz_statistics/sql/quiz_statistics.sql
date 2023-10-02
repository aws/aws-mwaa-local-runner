create table if not exists {{ params["schema"] }}.{{ params["table"] }} (
    id                        integer
  , course_id                 integer
  , quiz_id                   integer
  , quiz_title                varchar(140)
  , html_url                  varchar(624)
  , multiple_attempts_exist   boolean
  , generated_at              timestamp
  , included_all_versions     boolean
  , points_possible           float
  , anonymous_survey          boolean
  , speed_grader_url          varchar(624)
  , quiz_submissions_zip_url  varchar(624)
);