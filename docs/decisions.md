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