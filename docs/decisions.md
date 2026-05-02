# Architecture decisions

## Data sources
- Synthea RWE: `dbfs:/databricks-datasets/rwe/ehr/csv/`  
  Clinical data — patients, encounters, conditions, medications, observations, procedures
- HealthVerity: `samples.healthverity.claims_sample_synthetic`  
  Claims/payer data — ICD-10, NPI, payer type, billed vs allowed costs

## Why both sources
Synthea provides clinical depth. HealthVerity provides financial context.
Join point is ICD-10 diagnosis code shared between both systems.
Note: Synthea uses SNOMED internally — requires SNOMED→ICD-10 mapping in Silver.

## Catalog strategy
Single catalog `ehr_pipeline` with three schemas: bronze, silver, gold.
In production would use separate catalogs per environment (dev/stg/prod).
Free edition limitation + solo project made single catalog the pragmatic choice.

## Bundle default schema
Set to `bronze` in etl.pipeline.yml as the DLT fallback landing zone.
Each table explicitly declares its target schema in Python code.

## Branch naming
feat/     → new feature
fix/      → bug fix  
docs/     → documentation only
chore/    → maintenance and config
test/     → tests only

## Pipeline style
Building both PySpark notebooks (for learning) and DLT (for showcase).
Learn with notebooks first, graduate to DLT.

## Repo structure
Bundle lives inside repo subfolder due to bundle init order.
Would flatten to repo root in a greenfield project.

## Progress log

### Bronze layer (complete)
- 12 Synthea CSV tables ingested into ehr_pipeline.bronze
- HealthVerity claims ingested into ehr_pipeline.bronze.hv_claims_raw (409,825 rows)
- Config-driven ingestion via load_config() + dev.yml
- Audit columns: _ingested_at, _source_file on every row
- Append mode — full history preserved

### Silver layer (in progress)
- transform_patients: 11,737 rows
  - PII dropped (ssn, drivers, passport)
  - Dates cast, patient_age derived, is_deceased flag
  - Marital status decoded
  - Deduplication via window function
- transform_encounters: 393,234 rows
  - Timestamps cast, duration_minutes derived
  - patient_id/provider_id renamed
  - Partitioned by encounter_year/encounter_month
  - Deduplication via window function

### Pending
- Silver: conditions, medications, observations, procedures
- Gold layer
- Schema drift detection
- Dashboard
- AI agent

### Silver layer (complete — Synthea)
- transform_patients:     11,737 rows
- transform_encounters:   393,234 rows  
- transform_conditions:   84,421 rows
- transform_medications:  109,121 rows
- transform_observations: 2,181,850 rows
- transform_procedures:   327,171 rows

## Silver layer scope
Phase 1 (complete): patients, encounters, conditions, 
                    medications, observations, procedures
Phase 2 (future):   allergies, immunizations, 
                    careplans, imaging_studies

organizations and providers added as dimension tables 
needed for Gold provider metrics.

## Incremental processing
Current implementation uses full load — reads entire 
source on every run. 

Production improvement: implement watermark pattern
using pipeline_control table to track last successful
run per table. Only process records with 
_ingested_at > last_watermark.

### Silver HealthVerity enrichments (Issue #21)
- diagnosis_category via broadcast join on ICD-10 first character
  - Broadcast join chosen over F.when() chain — Spark sends
    25-row lookup to all executors, no shuffle
  - Left join to preserve rows with null diagnosis_code
- cost_reduction: line_charge - line_allowed
- claim_type_decoded: P → Professional, I → Institutional
- is_valid_npi: regex validation (^\\d{10}$), not just length check
- icd10_letter retained for Gold layer filtering
- DecimalType(10,2) used for monetary columns — not float/double
  to avoid floating-point precision drift in aggregations

### Gold layer (complete — dbt)
- Built with dbt-databricks, materialized as Delta tables in ehr_pipeline.gold
- PySpark for Bronze/Silver, dbt for Gold — two engines, one repo
- dbt chosen for Gold because: SQL models, lineage tracking, built-in documentation and testing

#### Gold models
- patient_summary: 11,737 rows (1 row per patient)
  - CTE pattern to avoid fan-out from multi-fact joins
  - Left joins to preserve patients with zero encounters/conditions
  - Aggregates: total_encounters, unique_conditions, total_spend, first/last encounter dates
- encounter_summary: 400,873 rows (1 row per encounter-condition pair)
  - Grain changed from per-encounter to per-encounter-condition to preserve diagnostic detail
  - LEFT JOIN conditions — 80% of encounters had no diagnosis (routine visits preserved)
  - has_condition boolean flag for downstream filtering
- condition_prevalence: 1 row per condition code
  - Prevalence percentage via subquery for total patient count
  - SNOMED codes (Synthea), not ICD-10 — noted for future mapping
- provider_metrics: 1 row per provider
  - COUNT(DISTINCT patient_id) for unique patients — not COUNT(patient_id)
  - AVG duration preferred over SUM for provider comparison
- readmission_risk: 393,234 rows (1 row per encounter)
  - LEAD() window function to find next encounter per patient
  - 30-day threshold for readmission flag
  - CTE for portability — column alias referencing varies across SQL engines

#### dbt configuration
- Profile: gold (in ~/.dbt/profiles.yml)
- Catalog: ehr_pipeline, schema: gold
- Materialization: table (not view — Gold serves dashboards and AI agents)
- Sources defined in _sources.yml pointing to all 7 Silver tables
- dbt run --select <model> for targeted builds (avoids unnecessary rebuilds)

### Schema drift detection (Issue #10)
- Baseline stored in ehr_pipeline.bronze.schema_registry (Delta table)
- Delta chosen over config file — queryable, automatable, auditable
- detect_drift() uses left_anti joins for set comparison
- Drift log is append-only — preserves full history
- Registry uses delete + append per schema to avoid cross-schema overwrites
- Checks both Bronze (188 columns, 15 tables) and Silver (137 columns, 7 tables)