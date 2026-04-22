from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql import Window
from pyspark.sql.types import DecimalType
from ehr_medallion_pipeline.utils import load_config


# ICD-10 first-character → diagnosis category lookup
ICD10_CATEGORY_MAP = {
    "A": "Infectious Diseases", "B": "Infectious Diseases",
    "C": "Neoplasms / Blood Diseases", "D": "Neoplasms / Blood Diseases",
    "E": "Endocrine / Metabolic", "F": "Mental / Behavioral",
    "G": "Nervous System", "H": "Eye / Ear",
    "I": "Circulatory System", "J": "Respiratory",
    "K": "Digestive", "L": "Skin",
    "M": "Musculoskeletal", "N": "Genitourinary",
    "O": "Pregnancy", "P": "Perinatal",
    "Q": "Congenital", "R": "Abnormal Findings",
    "S": "Injury / Poisoning", "T": "Injury / Poisoning",
    "V": "External Causes", "W": "External Causes",
    "X": "External Causes", "Y": "External Causes",
    "Z": "Health Services / Factors",
}


def transform_hv_claims(spark, env: str = "dev"):
    """
    Transform Bronze HealthVerity claims into Silver.

    Transformations:
    - Cast date_service to date
    - Cast line_charge, line_allowed to DecimalType(10,2)
    - Cast patient_year_of_birth to integer
    - Cast service_line_number to integer
    - Derive patient_age from birth year
    - Decode ICD-10 first character to diagnosis_category (broadcast join)
    - Derive cost_reduction (line_charge - line_allowed)
    - Decode claim_type (P → Professional, I → Institutional)
    - Validate NPI format (is_valid_npi flag)
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

    # 5. diagnosis_category — broadcast join on ICD-10 first character
    icd10_items = [(k, v) for k, v in ICD10_CATEGORY_MAP.items()]
    icd10_df = spark.createDataFrame(icd10_items, ["icd10_letter", "diagnosis_category"])

    df = df.withColumn("icd10_letter", F.substring(F.col("diagnosis_code"), 1, 1))
    df = df.join(broadcast(icd10_df), on="icd10_letter", how="left")

    # 6. cost reduction — difference between billed and allowed
    df = df.withColumn("cost_reduction", F.col("line_charge") - F.col("line_allowed"))

    # 7. decode claim type
    df = df.withColumn("claim_type_decoded",
        F.when(F.col("claim_type") == "P", "Professional")
         .when(F.col("claim_type") == "I", "Institutional")
         .otherwise(None)
    )

    # 8. validate NPI format — must be exactly 10 digits
    df = df.withColumn("is_valid_npi",
        F.when(F.col("prov_rendering_npi").rlike("^\\d{10}$"), True)
         .otherwise(False)
    )

    # 9. surrogate key — composite of claim line identity
    df = df.withColumn(
        "claim_line_id",
        F.md5(F.concat_ws("_",
            F.col("claim_id"),
            F.col("service_line_number"),
            F.col("diagnosis_code")
        ))
    )

    # 10. dedup — keep latest ingestion per claim line
    window = Window.partitionBy("claim_line_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_rank", F.row_number().over(window)) \
           .filter(F.col("_row_rank") == 1) \
           .drop("_row_rank")

    # 11. audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    # write to silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target)

    print(f"transform_hv_claims complete — {df.count()} rows written to {target}")