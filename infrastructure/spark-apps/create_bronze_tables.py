from pyspark.sql import SparkSession
from delta import DeltaTable

# ----------------------------
# Crear sesión Spark (basado en tu script)
# ----------------------------
spark = (SparkSession.builder
    .appName("CreateBronzeTables")
    .master("spark://spark-master:7077")
    .config("spark.submit.deployMode", "client")

    # Delta
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Usamos bucket bronze directamente
    .config("spark.sql.warehouse.dir", "s3a://bronze/warehouse")

    # Config MinIO
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

    # JARS (los reutilizas de tu entorno)
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

# ----------------------------
# Crear DB lógica
# ----------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# ----------------------------
# Función util: crear tabla si no existe
# ----------------------------
def create_table_if_not_exists(path, create_sql):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[..] Creando tabla en {path}")
        spark.sql(create_sql)
    else:
        print(f"[OK] Tabla ya existe en {path}")


# ============================
# 1. bronze.sorteos_raw
# ============================

path_sorteos = "s3a://bronze/sorteos_raw"

create_sql_sorteos = f"""
CREATE TABLE IF NOT EXISTS bronze.sorteos_raw (
    raw_content     STRING,
    source          STRING,
    ingestion_ts    TIMESTAMP,
    pipeline_run_id STRING,
    anio            INT,
    mes             INT
)
USING DELTA
LOCATION '{path_sorteos}'
PARTITIONED BY (source)
"""

create_table_if_not_exists(path_sorteos, create_sql_sorteos)


# ============================
# 2. bronze.puntos_venta_raw
# ============================

path_puntos = "s3a://bronze/puntos_venta_raw"

create_sql_puntos = f"""
CREATE TABLE IF NOT EXISTS bronze.puntos_venta_raw (
    raw_content     STRING,
    source          STRING,
    ingestion_ts    TIMESTAMP,
    pipeline_run_id STRING
)
USING DELTA
LOCATION '{path_puntos}'
PARTITIONED BY (source)
"""

create_table_if_not_exists(path_puntos, create_sql_puntos)


# ============================
# 3. bronze.scraping_selae_raw
# ============================

path_scraping = "s3a://bronze/scraping_selae_raw"

create_sql_scraping = f"""
CREATE TABLE IF NOT EXISTS bronze.scraping_selae_raw (
    raw_json        STRING,
    kafka_offset    BIGINT,
    kafka_partition INT,
    ingestion_ts    TIMESTAMP,
    ingestion_date  DATE,
    pipeline_run_id STRING
)
USING DELTA
LOCATION '{path_scraping}'
PARTITIONED BY (ingestion_date)
"""

create_table_if_not_exists(path_scraping, create_sql_scraping)


# ============================
# 4. bronze.apuestas_raw
# ============================

path_apuestas = "s3a://bronze/apuestas_raw"

create_sql_apuestas = f"""
CREATE TABLE IF NOT EXISTS bronze.apuestas_raw (
    raw_json        STRING,
    kafka_offset    BIGINT,
    kafka_partition INT,
    ingestion_ts    TIMESTAMP,
    ingestion_date  DATE,
    pipeline_run_id STRING
)
USING DELTA
LOCATION '{path_apuestas}'
PARTITIONED BY (ingestion_date)
"""

create_table_if_not_exists(path_apuestas, create_sql_apuestas)


# ----------------------------
# Verificación
# ----------------------------
print("\n[!!] Tablas en bronze:")
spark.sql("SHOW TABLES IN bronze").show(truncate=False)

print("[OK] Tablas Bronze creadas correctamente")

spark.stop()
