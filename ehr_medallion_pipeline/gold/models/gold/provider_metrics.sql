SELECT 
  provider_id,
  COUNT(id) as total_encounters,
  COUNT(DISTINCT patient_id) as count_patients,
  ROUND(AVG(duration_minutes), 2) as avg_duration_minutes,
  ROUND(SUM(cost), 2) as total_cost
FROM 
  {{ source('silver', 'synthea_encounters')}}
GROUP BY 
  provider_id;