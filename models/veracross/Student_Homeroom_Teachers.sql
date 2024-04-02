{{ config(materialized='table') }}

SELECT
    id as student_id,
    last_name,
    first_name,
    REPLACE(JSON_EXTRACT(homeroom_teacher, '$.name'),'"', '') as homeroom_teacher_name,
FROM `atlis-data-workshop.Veracross_AB.Get_Students`