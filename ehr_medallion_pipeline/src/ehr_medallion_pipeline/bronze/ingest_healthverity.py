from pyspark.sql import functions as F
from ehr_medallion_pipeline.utils import load_config

def ingest_healthverity(spark, env: str = "dev"):
    """
    Ingest HealthVerity claims table into Bronze Delta table.

    Args:
        spark: Active SparkSession
        env:   Environment name e.g. 'dev' or 'prod'
    """
    cfg = load_config(env)
    source_table = cfg["healthverity"]["source_table"]
    source_prefix = cfg["healthverity"]["source_prefix"]
    target = f"{cfg['catalog']}.bronze"
    target_table = cfg["healthverity"]["target_table"]

    df = spark.read.table(source_table)

    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source_file", F.lit(source_table))

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{target}.{source_prefix}_{target_table}")
        
    print(f"{source_prefix}_{target_table} ingested successfully.")
