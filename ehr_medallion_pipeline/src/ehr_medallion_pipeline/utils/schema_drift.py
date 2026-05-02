from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from ehr_medallion_pipeline.utils import load_config


def capture_schema(spark, catalog: str, schema_name: str, env: str = "dev"):
    """
    Capture the current schema of all tables in a given schema
    and write to the schema_registry table.

    This establishes the baseline that detect_drift() compares against.
    Run once to create the baseline, re-run to refresh after approved changes.

    Args:
        spark:       Active SparkSession
        catalog:     Catalog name e.g. 'ehr_pipeline'
        schema_name: Schema to scan e.g. 'bronze'
        env:         Environment name
    """
    registry_table = f"{catalog}.bronze.schema_registry"

    # list all tables in the schema
    tables = spark.catalog.listTables(f"{catalog}.{schema_name}")

    rows = []
    for table in tables:
        full_name = f"{catalog}.{schema_name}.{table.name}"
        try:
            df = spark.table(full_name)
            for field in df.schema.fields:
                rows.append((
                    schema_name,
                    table.name,
                    field.name,
                    str(field.dataType).replace("Type", "").lower(),
                    field.nullable
                ))
        except Exception as e:
            print(f"Warning: could not read schema for {full_name}: {e}")

    # create dataframe from collected rows
    schema = StructType([
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("is_nullable", BooleanType(), False),
    ])

    registry_df = spark.createDataFrame(rows, schema)

    # add audit timestamps
    registry_df = registry_df.withColumn("_registered_at", F.current_timestamp()) \
                             .withColumn("updated_at", F.current_timestamp())

    # write to registry — overwrite to refresh baseline
    registry_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(registry_table)

    print(f"Schema registry captured: {len(rows)} columns across {len(tables)} tables in {schema_name}")
    return registry_df


def detect_drift(spark, catalog: str, schema_name: str, env: str = "dev"):
    """
    Compare current schema of all tables against the baseline
    stored in schema_registry. Log any differences to schema_drift_log.

    Detects three types of drift:
    - COLUMN_ADDED:   column exists now but not in baseline
    - COLUMN_REMOVED: column in baseline but missing now
    - TYPE_CHANGED:   column exists in both but data type differs

    Args:
        spark:       Active SparkSession
        catalog:     Catalog name e.g. 'ehr_pipeline'
        schema_name: Schema to check e.g. 'bronze'
        env:         Environment name
    """
    registry_table = f"{catalog}.bronze.schema_registry"
    drift_log_table = f"{catalog}.bronze.schema_drift_log"

    # load baseline
    baseline_df = spark.table(registry_table) \
        .filter(F.col("schema_name") == schema_name) \
        .select("schema_name", "table_name", "column_name", "data_type", "is_nullable")

    # capture current schema
    tables = spark.catalog.listTables(f"{catalog}.{schema_name}")

    current_rows = []
    for table in tables:
        full_name = f"{catalog}.{schema_name}.{table.name}"
        try:
            df = spark.table(full_name)
            for field in df.schema.fields:
                current_rows.append((
                    schema_name,
                    table.name,
                    field.name,
                    str(field.dataType).replace("Type", "").lower(),
                    field.nullable
                ))
        except Exception as e:
            print(f"Warning: could not read schema for {full_name}: {e}")

    current_schema = StructType([
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("is_nullable", BooleanType(), False),
    ])

    current_df = spark.createDataFrame(current_rows, current_schema)

    # --- detect drift ---

    join_keys = ["schema_name", "table_name", "column_name"]

    # columns added (in current but not in baseline)
    added_df = current_df.join(baseline_df, on=join_keys, how="left_anti") \
        .withColumn("drift_type", F.lit("COLUMN_ADDED")) \
        .withColumn("old_value", F.lit(None).cast("string")) \
        .withColumn("new_value", F.col("data_type")) \
        .select("schema_name", "table_name", "column_name", "drift_type", "old_value", "new_value")

    # columns removed (in baseline but not in current)
    removed_df = baseline_df.join(current_df, on=join_keys, how="left_anti") \
        .withColumn("drift_type", F.lit("COLUMN_REMOVED")) \
        .withColumn("old_value", F.col("data_type")) \
        .withColumn("new_value", F.lit(None).cast("string")) \
        .select("schema_name", "table_name", "column_name", "drift_type", "old_value", "new_value")

    # type changes (column exists in both but data_type differs)
    type_changed_df = current_df.alias("curr").join(
        baseline_df.alias("base"),
        on=join_keys,
        how="inner"
    ).filter(
        F.col("curr.data_type") != F.col("base.data_type")
    ).select(
        F.col("curr.schema_name"),
        F.col("curr.table_name"),
        F.col("curr.column_name"),
        F.lit("TYPE_CHANGED").alias("drift_type"),
        F.col("base.data_type").alias("old_value"),
        F.col("curr.data_type").alias("new_value"),
    )

    # combine all drift events
    drift_df = added_df.union(removed_df).union(type_changed_df)

    # add detection timestamp
    drift_df = drift_df.withColumn("_detected_at", F.current_timestamp())

    drift_count = drift_df.count()

    if drift_count > 0:
        # append to drift log (preserve history)
        drift_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(drift_log_table)

        print(f"⚠️  Schema drift detected: {drift_count} changes in {schema_name}")
        drift_df.show(truncate=False)
    else:
        print(f"✅ No schema drift detected in {schema_name}")

    return drift_df


def run_drift_check(spark, env: str = "dev"):
    """
    Run schema drift detection across all pipeline schemas.

    Args:
        spark: Active SparkSession
        env:   Environment name
    """
    cfg = load_config(env)
    catalog = cfg["catalog"]

    for schema_name in ["bronze", "silver"]:
        print(f"\n--- Checking {schema_name} ---")
        detect_drift(spark, catalog, schema_name, env)