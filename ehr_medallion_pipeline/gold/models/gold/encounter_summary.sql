SELECT 
  e.id as encounter_id,
  e.patient_id,
  e.start as encounter_start,
  e.stop as encounter_end,
  e.duration_minutes,
  e.cost as encounter_cost,
  e.encounterclass as encounter_class,
  c.condition_code,
  c.condition_description,
  CASE WHEN c.condition_code IS NOT NULL THEN TRUE ELSE FALSE END as has_condition,
  p.patient_age,
  p.gender,
  e.encounter_year,
  e.encounter_month
FROM {{ source('silver', 'synthea_encounters') }} e
LEFT JOIN {{ source('silver', 'synthea_conditions') }} c
  ON e.id = c.encounter_id
JOIN {{ source('silver', 'synthea_patients') }} p
  ON p.id = e.patient_id