BEGIN;
  -- Create student_instructor_history_table if it doesn't already exist
  CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
      educator_uuid     VARCHAR(128)
      , student_uuid      VARCHAR(128)
      , updated_at        TIMESTAMP
      , removed_at        TIMESTAMP
  );

  -- Insert student-educator links that do not exist in the current table
  INSERT INTO {{ params.schema }}.{{ params.table }}
  SELECT
    educator_uuid
    , student_uuid
    , updated_at
    , NULL AS removed_at
  FROM service_educator.educator_students
  WHERE NOT EXISTS (
    SELECT 1
    FROM {{ params.schema }}.{{ params.table }}
    WHERE 
      {{ params.schema }}.{{ params.table }}.educator_uuid = service_educator.educator_students.educator_uuid
      AND {{ params.schema }}.{{ params.table }}.student_uuid = service_educator.educator_students.student_uuid
      AND {{ params.schema }}.{{ params.table }}.updated_at = service_educator.educator_students.updated_at
  )
  ;

  -- Add a removed at date for student-educator links no longer in the service table
  UPDATE {{ params.schema }}.{{ params.table }} 
  SET removed_at = CASE WHEN removals.removed THEN NVL(removals.removed_at, GETDATE()) END
  FROM 
      (
          SELECT 
            {{ params.schema }}.{{ params.table }}.educator_uuid
            , {{ params.schema }}.{{ params.table }}.student_uuid
            , {{ params.schema }}.{{ params.table }}.updated_at
            , LEAD({{ params.schema }}.{{ params.table }}.updated_at) OVER (
              PARTITION BY {{ params.schema }}.{{ params.table }}.student_uuid ORDER BY {{ params.schema }}.{{ params.table }}.updated_at
              ) AS removed_at
            , service_educator.educator_students.student_uuid IS NULL AS removed
          FROM {{ params.schema }}.{{ params.table }}
          LEFT JOIN service_educator.educator_students
            ON service_educator.educator_students.educator_uuid = {{ params.schema }}.{{ params.table }}.educator_uuid
              AND service_educator.educator_students.student_uuid = {{ params.schema }}.{{ params.table }}.student_uuid
              AND service_educator.educator_students.updated_at = {{ params.schema }}.{{ params.table }}.updated_at
      ) removals 
  WHERE 
    {{ params.schema }}.{{ params.table }}.educator_uuid = removals.educator_uuid
    AND {{ params.schema }}.{{ params.table }}.student_uuid = removals.student_uuid
    AND {{ params.schema }}.{{ params.table }}.updated_at = removals.updated_at;

END;
