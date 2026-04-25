
WITH encounter_agg AS(
  SELECT 
    patient_id,
    count(e.id) total_encounters,
    ROUND(SUM(e.cost),2) AS total_spend,
    MIN(DATE(e.start)) AS first_encounter_date,
    MAX(DATE(e.start)) AS last_encounter_date
  FROM {{ source('silver', 'synthea_encounters') }} e
  GROUP BY patient_id
),
condition_agg AS (
    SELECT 
      patient_id,
      COUNT(DISTINCT condition_code) as unique_conditions
    FROM 
      {{ source('silver', 'synthea_conditions') }}
    GROUP BY
      patient_id
)

SELECT 
  pt.id, 
    e.total_encounters,
    c.unique_conditions as unique_conditions,
    e.total_spend,
    e.first_encounter_date,
    e.last_encounter_date,
    pt.patient_age,
    pt.gender,
    pt.is_deceased,
    pt.marital_status
FROM 
        {{ source('silver', 'synthea_patients') }} pt
LEFT JOIN encounter_agg e ON e.patient_id = pt.id
LEFT JOIN condition_agg c ON c.patient_id = pt.id
