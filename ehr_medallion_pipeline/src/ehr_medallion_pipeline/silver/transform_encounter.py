from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config


def transform_encounters(spark, env: str = "dev"):
    """
    Transform Bronze encounters table into Silver.

    Transformations:
    - Lowercase column names
    - Cast start and stop to timestamp
    - Derive duration_minutes
    - Cast cost to float
    - Rename patient → patient_id, provider → provider_id
    - Extract encounter_year and encounter_month
    - Deduplicate by id keeping latest ingestion
    - Add _silver_processed_at audit column

    Args:
        spark: Active SparkSession
        env:   Environment name e.g. 'dev' or 'prod'
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_encounters"
    target = f"{cfg['catalog']}.silver.synthea_encounters"

    df = spark.table(source)

    # 1. lowercase column names
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())

    # 2. cast start and stop to timestamp
    df = df.withColumn("start", F.to_timestamp(F.col('start'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    df = df.withColumn("stop", F.to_timestamp(F.col('stop'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # 3. derive duration_minutes
    df = df.withColumn(
        "duration_minutes",
        ((F.unix_timestamp("stop") - F.unix_timestamp("start")) / 60).cast("integer")
    )

    # 4. cast cost to float
    df = df.withColumn("cost", F.col("cost").cast("double"))     

    # 5. rename patient and provider
    df = df \
        .withColumnRenamed('patient', 'patient_id') \
        .withColumnRenamed('provider', 'provider_id')

    # 6. extract year and month
    df = df.withColumn("encounter_year", F.year("start"))
    df = df.withColumn("encounter_month", F.month("start"))

    # 7. deduplicate by id
    window = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())

    df = df.withColumn("_row_rank", F.row_number().over(window)) \
           .filter(F.col("_row_rank") == 1) \
           .drop("_row_rank")

    # 8. add silver audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())


    # write to silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("encounter_year", "encounter_month") \
        .saveAsTable(target)

    print(f"transform_encounters complete — {df.count()} rows written to {target}")