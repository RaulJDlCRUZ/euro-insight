from pyspark.sql import SparkSession
from delta import DeltaTable

# ============================
# Spark Session
# ============================
spark = (SparkSession.builder
    .appName("CreatePipelineControl")
    .master("spark://spark-master:7077")
    .config("spark.submit.deployMode", "client")

    # Delta
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Silver warehouse
    .config("spark.sql.warehouse.dir", "s3a://silver/warehouse")

    # MinIO
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # JARs
    .config(
        "spark.jars",
        "/opt/spark/jars/hadoop-aws-3.4.0.jar,"
        "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar,"
        "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
        "/opt/spark/jars/delta-storage-4.0.0.jar"
    )

    .getOrCreate()
)

print("[OK] SparkSession creada")

spark.sql("CREATE DATABASE IF NOT EXISTS silver")


# ============================
# Helper
# ============================
def create_table_if_not_exists(path, create_sql):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[..] Creando tabla en {path}")
        spark.sql(create_sql)
    else:
        print(f"[OK] Tabla ya existe en {path}")


# ============================
# Tabla pipeline_control
# ============================

path_pipeline_control = "s3a://silver/pipeline_control"

create_sql_pipeline_control = f"""
CREATE TABLE IF NOT EXISTS silver.pipeline_control (
    pipeline_name          STRING,
    last_success_ts        TIMESTAMP,
    last_pipeline_run_id   STRING,
    rows_processed         BIGINT,
    rows_valid             BIGINT,
    rows_invalid           BIGINT,
    updated_at             TIMESTAMP
)
USING DELTA
LOCATION '{path_pipeline_control}'
"""

create_table_if_not_exists(
    path_pipeline_control,
    create_sql_pipeline_control
)


# ============================
# Inicialización de pipelines
# ============================

spark.sql("""
MERGE INTO silver.pipeline_control AS target
USING (
    SELECT
        'bronze_to_silver_sorteos' AS pipeline_name,
        CAST(NULL AS TIMESTAMP)    AS last_success_ts,
        CAST(NULL AS STRING)       AS last_pipeline_run_id,
        CAST(0 AS BIGINT)          AS rows_processed,
        CAST(0 AS BIGINT)          AS rows_valid,
        CAST(0 AS BIGINT)          AS rows_invalid,
        current_timestamp()        AS updated_at
) AS source
ON target.pipeline_name = source.pipeline_name
WHEN NOT MATCHED THEN
INSERT (
    pipeline_name,
    last_success_ts,
    last_pipeline_run_id,
    rows_processed,
    rows_valid,
    rows_invalid,
    updated_at
)
VALUES (
    source.pipeline_name,
    source.last_success_ts,
    source.last_pipeline_run_id,
    source.rows_processed,
    source.rows_valid,
    source.rows_invalid,
    source.updated_at
)
""")

print("[OK] Pipeline bronze_to_silver_sorteos inicializado")

print("\n[!!] Estado actual de pipeline_control:")
spark.sql(
    "SELECT * FROM silver.pipeline_control"
).show(truncate=False)

spark.stop()
