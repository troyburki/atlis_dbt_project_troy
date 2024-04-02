{{ config(materialized='table') }}

WITH base_data AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    JSON_EXTRACT_ARRAY(columns) AS columns_array
  FROM
    `dtsdatastore.Airbyte_Syncs.OfficialNotes`
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
  MAX(IF(key_name = 'Date Processed', key_value, NULL)) AS Date_Processed,
    MAX(IF(key_name = 'Decision Date', key_value, NULL)) AS Decision_Date,
    MAX(IF(key_name = 'School Decision', key_value, NULL)) AS School_Decision,
    MAX(IF(key_name = 'Candidate Decision', key_value, NULL)) AS Candidate_Decision,
FROM
  unnested_data
GROUP BY
  _airbyte_raw_id,
  _airbyte_extracted_at