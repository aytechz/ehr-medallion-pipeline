-- A provider cannot see more distinct patients than encounters; values cannot be negative.
SELECT provider_id, total_encounters, count_patients, avg_duration_minutes, total_cost
FROM {{ ref('provider_metrics') }}
WHERE count_patients > total_encounters
   OR total_encounters <= 0
   OR avg_duration_minutes < 0
   OR total_cost < 0