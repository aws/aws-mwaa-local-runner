create table if not exists {{ params["schema"] }}.{{ params["table"] }} (
    id integer
  , quiz_statistics_id integer
  , quiz_id integer
  , course_id integer
  , question_type varchar(80)
  , question_text varchar(8000)
  , position integer
  , responses integer
  , top_student_count float
  , middle_student_count float
  , bottom_student_count float
  , answered_student_count float
  , correct_student_count float
  , incorrect_student_count float
  , correct_student_ratio float
  , incorrect_student_ratio float
  , correct_top_student_count float
  , correct_middle_student_count float
  , correct_bottom_student_count float
  , variance float
  , stdev float
  , difficulty_index float
  , alpha float
  , discrimination_index float
);