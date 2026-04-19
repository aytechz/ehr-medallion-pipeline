from ehr_medallion_pipeline.bronze.ingest_synthea import run_synthea_ingestion
from ehr_medallion_pipeline.bronze.ingest_healthverity import ingest_healthverity
from ehr_medallion_pipeline.silver.transform_patients import transform_patients
from ehr_medallion_pipeline.silver.transform_encounters import transform_encounters
from ehr_medallion_pipeline.silver.transform_conditions import transform_conditions
from ehr_medallion_pipeline.silver.transform_medications import transform_medications
from ehr_medallion_pipeline.silver.transform_observations import transform_observations
from ehr_medallion_pipeline.silver.transform_procedures import transform_procedures


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    env = "dev"

    print("=== BRONZE LAYER ===")
    run_synthea_ingestion(spark, env=env)
    ingest_healthverity(spark, env=env)

    print("=== SILVER LAYER ===")
    transform_patients(spark, env=env)
    transform_encounters(spark, env=env)
    transform_conditions(spark, env=env)
    transform_medications(spark, env=env)
    transform_observations(spark, env=env)
    transform_procedures(spark, env=env)

    print("=== PIPELINE COMPLETE ===")


if __name__ == "__main__":
    main()