from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config


def transform_conditions(spark, env: str = "dev"):
    """
    Transform Bronze conditions table into Silver.

    Transformations:
    - Lowercase column names
    - Cast start and stop to date
    - Rename patient → patient_id, encounter → encounter_id
    - Rename code → condition_code, description → condition_description
    - Generate surrogate key (condition_id) via MD5 hash
      because conditions table has no natural primary key
    - Deduplicate by condition_id keeping latest ingestion
    - Add _silver_processed_at audit column
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_conditions"
    target = f"{cfg['catalog']}.silver.synthea_conditions"

    df = spark.table(source)

    # lowercase all column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # cast date columns
    df = df.withColumn("start", F.col("start").cast("date"))
    df = df.withColumn("stop", F.col("stop").cast("date"))

    # rename columns for clarity and join safety
    df = df.withColumnRenamed("patient", "patient_id") \
           .withColumnRenamed("encounter", "encounter_id") \
           .withColumnRenamed("code", "condition_code") \
           .withColumnRenamed("description", "condition_description")

    # generate surrogate key — no natural PK in conditions table
    # MD5 hash of composite business key gives stable unique identifier
    df = df.withColumn(
        "condition_id",
        F.md5(F.concat_ws("_",
            F.col("patient_id"),
            F.col("encounter_id"),
            F.col("condition_code"),
            F.col("start")
        ))
    )

    # deduplicate — keep latest ingestion per condition
    window = Window.partitionBy("condition_id").orderBy(F.col("_ingested_at").desc())
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

    print(f"transform_conditions complete — {df.count()} rows written to {target}")