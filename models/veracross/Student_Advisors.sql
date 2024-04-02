{{ config(materialized='table') }}

SELECT
    id as student_id,
    last_name,
    first_name,
    json_extract(advisor, '$.id') as advisor_id,
    json_extract(advisor, '$.name') as advisor_name,
FROM `atlis-data-workshop.Veracross_AB.Get_Students`
