-- has_condition must be TRUE exactly when condition_code is present.
SELECT encounter_id, condition_code, has_condition
FROM {{ ref('encounter_summary') }}
WHERE has_condition != (condition_code IS NOT NULL)