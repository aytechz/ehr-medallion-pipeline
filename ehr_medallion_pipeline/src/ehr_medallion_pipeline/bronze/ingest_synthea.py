from pyspark.sql import functions as F
from ehr_medallion_pipeline.utils import load_config



def ingest_table(spark, table_name, source_base_path, target_catalog_schema, source_prefix):
    """
    Ingest a single CSV file into a Bronze Delta table.

    Args:
        spark:                  Active SparkSession
        table_name:             Name of the CSV file without extension e.g. 'patients'
        source_base_path:       DBFS path to the CSV folder
        target_catalog_schema:  Unity Catalog target e.g. 'ehr_pipeline.bronze'
        source_prefix:          Source system prefix e.g. 'synthea' or 'hv'
    """

    source_file = f"{source_base_path}{table_name}.csv"

    df = spark.read \
        .option("header", "true") \
        .csv(source_file)
    
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source_file", F.lit(source_file))

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{target_catalog_schema}.{source_prefix}_{table_name}")
    
    print(f"{source_prefix}_{table_name} ingested successfully.")


def run_synthea_ingestion(spark, env: str = "dev"):
    """
    Run full Synthea Bronze Ingestion for all CSV tables
    """

    cfg = load_config(env)

    base_path = cfg["synthea"]["base_path"]
    source_prefix = cfg["synthea"]["source_prefix"]
    target = f"{cfg['catalog']}.bronze"
    tables = cfg["synthea"]["tables"]


    for table in tables:
        try:
            print(f"Ingesting {table}...")
            ingest_table(spark, table, base_path, target, source_prefix)
        except Exception as e:
            print(f"Error ingesting {table}: {e}")