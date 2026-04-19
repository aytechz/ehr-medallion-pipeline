from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config


def transform_medications(spark, env: str = "dev"):
    """
    Transform Bronze medications table into Silver.

    Transformations:
    - Lowercase column names
    - Cast start and stop to date
    - Cast cost, totalcost to double, dispenses to integer
    - Rename patient → patient_id, encounter → encounter_id
    - Rename code → medication_code, description → medication_description
    - Generate surrogate key (medication_id) via MD5 hash
    - Deduplicate by medication_id keeping latest ingestion
    - Add _silver_processed_at audit column
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_medications"
    target = f"{cfg['catalog']}.silver.synthea_medications"

    df = spark.table(source)

    # 1. lowercase
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())

    # 2. cast dates
    df = df.withColumn("start", F.col("start").cast("date")) \
           .withColumn("stop", F.col("stop").cast("date"))

    # 3. cast numeric columns
    df = df.withColumn("cost", F.col("cost").cast("double")) \
           .withColumn("dispenses", F.col("dispenses").cast("integer")) \
           .withColumn("totalcost", F.col("totalcost").cast("double"))

    # 4. rename columns
    df = df.withColumnRenamed("code", "medication_code") \
           .withColumnRenamed("description", "medication_description") \
           .withColumnRenamed("patient", "patient_id") \
           .withColumnRenamed("encounter", "encounter_id")

    # 5. surrogate key — fix this section
    df = df.withColumn(
        "medication_id",
        F.md5(F.concat_ws("_",
            F.col("patient_id"),
            F.col("encounter_id"),
            F.col("medication_code"),
            F.col("start")
        ))
    )

    # 6. dedup
    window = Window.partitionBy("medication_id").orderBy(F.col("_ingested_at").desc())
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

    print(f"transform_medications complete — {df.count()} rows written to {target}")