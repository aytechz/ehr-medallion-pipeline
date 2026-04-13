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

