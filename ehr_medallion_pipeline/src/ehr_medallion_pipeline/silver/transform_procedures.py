from pyspark.sql import functions as F
from pyspark.sql import Window
from ehr_medallion_pipeline.utils import load_config

def transform_procedures(spark, env: str = "dev"):
    """
    Transform Bronze procedures table into Silver.

    Transformations:
    - Lowercase column names
    - Cast date to date type
    - Rename patient → patient_id, encounter → encounter_id
    - Rename code → procedure_code, description → procedure_description
    - Generate surrogate key (procedure_id) via MD5 hash
      because procedures table has no natural primary key
    - Deduplicate by procedure_id keeping latest ingestion
    - Add _silver_processed_at audit column
    """
    cfg = load_config(env)
    source = f"{cfg['catalog']}.bronze.synthea_procedures"
    target = f"{cfg['catalog']}.silver.synthea_procedures"

    df = spark.table(source)

    # lowercase column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # cast date
    df = df.withColumn("date", F.col("date").cast("date"))

    # rename columns for clarity and join safety
    df = df.withColumnRenamed('patient', 'patient_id') \
            .withColumnRenamed('encounter', 'encounter_id') \
            .withColumnRenamed('code','procedure_code') \
            .withColumnRenamed('description', 'procedure_description')

    df = df.withColumn('cost', F.col('cost').cast('double'))

    df = df.withColumn('procedure_id', 
                    F.md5(F.concat_ws("_",
                    F.col("patient_id"),
                    F.col("encounter_id"),
                    F.col("procedure_code"),
                    F.col("date")
                    )))

    df = df.withColumn('procedure_year', F.year(F.col('date')))

    # deduplicate — keep latest ingestion per procedure
    window = Window.partitionBy("procedure_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_rank", F.row_number().over(window)) \
        .filter(F.col("_row_rank") == 1) \
        .drop("_row_rank")

    # add silver audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())


    df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("procedure_year") \
    .saveAsTable(target)

    print(f"transform_procedures complete — {df.count()} rows written to {target}")
