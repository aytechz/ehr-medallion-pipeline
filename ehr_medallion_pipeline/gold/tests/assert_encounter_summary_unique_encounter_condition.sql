-- Grain is encounter × condition: each (encounter_id, condition_code) pair must appear once.
SELECT encounter_id, condition_code, COUNT(*) AS row_count
FROM {{ ref('encounter_summary') }}
GROUP BY encounter_id, condition_code
HAVING COUNT(*) > 1