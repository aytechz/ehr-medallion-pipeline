from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config


def transform_patients(spark, env: str = "dev"):
    """
    Transform Bronze patients table into Silver.

    Transformations:
    - Lowercase column names
    - Cast birthdate and deathdate to date
    - Derive patient_age and is_deceased
    - Decode marital status
    - Drop PII columns (ssn, drivers, passport)
    - Deduplicate by id keeping latest ingestion
    - Add _silver_processed_at audit column

    Args:
        spark: Active SparkSession
        env:   Environment name e.g. 'dev' or 'prod'
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_patients"
    target = f"{cfg['catalog']}.silver.synthea_patients"

    # read from bronze
    df = spark.table(source)

    # lowercase column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # cast date columns
    df = df.withColumn("birthdate", F.col("birthdate").cast("date"))
    df = df.withColumn("deathdate", F.col("deathdate").cast("date"))

    # derive age and deceased flag
    df = df.withColumn(
        "patient_age",
        (F.datediff(F.current_date(), F.col("birthdate")) / 365).cast("integer")
    )
    df = df.withColumn(
        "is_deceased",
        F.when(F.col("deathdate").isNotNull(), True).otherwise(False)
    )

    # decode marital status
    df = df.withColumn(
        "marital_status",
        F.when(F.col("marital") == "M", "Married")
         .when(F.col("marital") == "S", "Single")
         .otherwise("Unknown")
    ).drop("marital")

    # drop PII columns
    df = df.drop("ssn", "drivers", "passport")

    # deduplicate — keep latest ingestion per patient
    window = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_rank", F.row_number().over(window)) \
           .filter(F.col("_row_rank") == 1) \
           .drop("_row_rank")

    # add silver audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    # write to silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target)

    print(f"transform_patients complete — {df.count()} rows written to {target}")