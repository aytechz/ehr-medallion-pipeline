-- An encounter cannot end before it starts.
SELECT encounter_id, encounter_start, encounter_end, duration_minutes
FROM {{ ref('encounter_summary') }}
WHERE encounter_end < encounter_start
   OR duration_minutes < 0