WITH encounter_ordered AS (
  SELECT 
    id as encounter_id,
    patient_id,
    start as encounter_start,
    LEAD(start) OVER(PARTITION BY patient_id ORDER BY start) as next_encounter_date
  FROM {{ source('silver','synthea_encounters') }}
)
SELECT 
  encounter_id,
  patient_id,
  encounter_start,
  next_encounter_date,
  DATEDIFF(next_encounter_date, encounter_start) as days_to_next_encounter,
  CASE 
    WHEN DATEDIFF(next_encounter_date, encounter_start) <= 30 THEN TRUE 
    ELSE FALSE 
  END as is_readmission_risk
FROM encounter_ordered