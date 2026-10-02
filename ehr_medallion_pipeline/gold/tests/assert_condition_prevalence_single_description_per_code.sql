{{ config(severity='warn') }}
-- Data quality monitor: a code should map to one condition.
-- Known Synthea collision on 427089005; WARN (not fail) so new collisions are visible.
SELECT condition_code, COUNT(*) AS description_count
FROM {{ ref('condition_prevalence') }}
GROUP BY condition_code
HAVING COUNT(*) > 1