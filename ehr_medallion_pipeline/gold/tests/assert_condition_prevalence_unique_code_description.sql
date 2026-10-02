-- Grain: each (condition_code, condition_description) pair appears once.
SELECT condition_code, condition_description, COUNT(*) AS row_count
FROM {{ ref('condition_prevalence') }}
GROUP BY condition_code, condition_description
HAVING COUNT(*) > 1