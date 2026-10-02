-- Days and next date must be NULL together, days cannot be negative,
-- and the flag must match the 30-day rule (unknown next visit → FALSE).
SELECT encounter_id, next_encounter_date, days_to_next_encounter, is_readmission_risk
FROM {{ ref('readmission_risk') }}
WHERE days_to_next_encounter < 0
   OR (next_encounter_date IS NULL) != (days_to_next_encounter IS NULL)
   OR is_readmission_risk != COALESCE(days_to_next_encounter <= 30, FALSE)