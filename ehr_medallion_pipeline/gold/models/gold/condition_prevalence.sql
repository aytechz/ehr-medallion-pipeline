SELECT
  c.condition_code,
  c.condition_description,
  COUNT(DISTINCT c.patient_id) as patient_count,
  (SELECT COUNT(*) FROM {{ source('silver', 'synthea_patients') }}) as total_patients,
  ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / 
    (SELECT COUNT(*) FROM {{ source('silver', 'synthea_patients') }}), 2) as prevalence_pct
FROM 
  {{ source('silver','synthea_conditions') }} c
GROUP BY 
  c.condition_code,
  c.condition_description