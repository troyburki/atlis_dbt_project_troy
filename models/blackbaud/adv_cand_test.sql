{{ config(materialized='table') }}

WITH base_data AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    JSON_EXTRACT_ARRAY(columns) AS columns_array
  FROM
    `atlis-data-workshop.Blackbaud_AB.adv_cand_test`
),
unnested_data AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    JSON_EXTRACT_SCALAR(column_value, '$.name') AS key_name,
    JSON_EXTRACT_SCALAR(column_value, '$.value') AS key_value
  FROM
    base_data
  CROSS JOIN
    UNNEST(columns_array) AS column_value
)
SELECT
    MAX(IF(key_name = 'First Name', key_value, NULL)) AS First_Name,
    MAX(IF(key_name = 'Last Name', key_value, NULL)) AS Last_Name,
    MAX(IF(key_name = 'Entering Grade', key_value, NULL)) AS Entering_Grade,
    MAX(IF(key_name = 'Entering Year', key_value, NULL)) AS Entering_Year,
FROM
  unnested_data
GROUP BY
  _airbyte_raw_id,
  _airbyte_extracted_at
