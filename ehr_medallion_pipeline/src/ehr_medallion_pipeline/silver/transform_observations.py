from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config


def transform_observations(spark, env: str = "dev"):
    """
    Transform Bronze observations table into Silver.

    Transformations:
    - Lowercase column names
    - Cast date to date type
    - Rename patient → patient_id, encounter → encounter_id
    - Rename code → observation_code, description → observation_description
    - Split value → numeric_value (double) + text_value (string)
    - Generate surrogate key (observation_id) via MD5 hash
    - Deduplicate by observation_id
    - Partition by observation_year + observation_month
    - Add _silver_processed_at audit column
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_observations"
    target = f"{cfg['catalog']}.silver.synthea_observations"

    df = spark.table(source)

    # lowercase column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # cast date
    df = df.withColumn("date", F.col("date").cast("date"))

    # rename columns for clarity and join safety
    df = df.withColumnRenamed("code", "observation_code") \
           .withColumnRenamed("description", "observation_description") \
           .withColumnRenamed("patient", "patient_id") \
           .withColumnRenamed("encounter", "encounter_id")

    # split polymorphic value column into typed columns
    df = df.withColumn(
        "numeric_value",
        F.when(F.col("type") == "numeric", F.col("value").cast("double"))
         .otherwise(None)
    )
    df = df.withColumn(
        "text_value",
        F.when(F.col("type") == "text", F.col("value"))
         .otherwise(None)
    )
    df = df.drop("value")

    # extract year and month for partitioning
    df = df.withColumn("observation_year", F.year("date"))
    df = df.withColumn("observation_month", F.month("date"))

    # generate surrogate key — no natural PK in observations
    df = df.withColumn(
        "observation_id",
        F.md5(F.concat_ws("_",
            F.col("patient_id"),
            F.col("encounter_id"),
            F.col("observation_code"),
            F.col("date")
        ))
    )

    # deduplicate — keep latest ingestion per observation
    window = Window.partitionBy("observation_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_rank", F.row_number().over(window)) \
           .filter(F.col("_row_rank") == 1) \
           .drop("_row_rank")

    # add silver audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    # write to silver — partitioned for query performance on 2.1M rows
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("observation_year", "observation_month") \
        .saveAsTable(target)

    print(f"transform_observations complete — {df.count()} rows written to {target}")