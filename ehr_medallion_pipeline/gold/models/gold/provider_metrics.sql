SELECT
  COALESCE(provider_id, 'UNKNOWN') AS provider_id,
  COUNT(id) AS total_encounters,
  COUNT(DISTINCT patient_id) AS count_patients,
  ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
  ROUND(SUM(cost), 2) AS total_cost
FROM
  {{ source('silver', 'synthea_encounters') }}
GROUP BY
  COALESCE(provider_id, 'UNKNOWN')