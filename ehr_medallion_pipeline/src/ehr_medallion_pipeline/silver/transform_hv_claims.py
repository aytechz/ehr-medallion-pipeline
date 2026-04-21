from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType
from ehr_medallion_pipeline.utils import load_config


def transform_hv_claims(spark, env: str = "dev"):
    """
    Transform Bronze HealthVerity claims into Silver.

    Transformations:
    - Cast date_service to date
    - Cast line_charge, line_allowed to DecimalType(10,2)
    - Cast patient_year_of_birth to integer
    - Cast service_line_number to integer
    - Derive patient_age from birth year
    - Generate surrogate key (claim_line_id) via MD5 hash
    - Deduplicate by claim_line_id keeping latest ingestion
    - Add _silver_processed_at audit column

    Args:
        spark: Active SparkSession
        env:   Environment name e.g. 'dev' or 'prod'
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.hv_claims_raw"
    target = f"{cfg['catalog']}.silver.hv_claims_clean"

    df = spark.table(source)

    # 1. cast date columns
    df = df.withColumn("date_service", F.col("date_service").cast("date"))

    # 2. cast monetary columns — DecimalType for precision, not float/double
    df = df.withColumn("line_charge", F.col("line_charge").cast(DecimalType(10, 2))) \
           .withColumn("line_allowed", F.col("line_allowed").cast(DecimalType(10, 2)))

    # 3. cast numeric columns
    df = df.withColumn("patient_year_of_birth", F.col("patient_year_of_birth").cast("integer")) \
           .withColumn("service_line_number", F.col("service_line_number").cast("integer"))

    # 4. derive patient age from birth year
    df = df.withColumn(
        "patient_age",
        F.year(F.current_date()) - F.col("patient_year_of_birth")
    )

    # 5. surrogate key — composite of claim line identity
    df = df.withColumn(
        "claim_line_id",
        F.md5(F.concat_ws("_",
            F.col("claim_id"),
            F.col("service_line_number"),
            F.col("diagnosis_code")
        ))
    )

    # 6. dedup — keep latest ingestion per claim line
    window = Window.partitionBy("claim_line_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_rank", F.row_number().over(window)) \
           .filter(F.col("_row_rank") == 1) \
           .drop("_row_rank")

    # 7. audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    # write to silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target)

    print(f"transform_hv_claims complete — {df.count()} rows written to {target}")