-- Prevalence must be a valid percentage, and a subset cannot exceed the total.
SELECT condition_code, condition_description, patient_count, total_patients, prevalence_pct
FROM {{ ref('condition_prevalence') }}
WHERE prevalence_pct < 0
   OR prevalence_pct > 100
   OR patient_count > total_patients