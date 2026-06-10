from pyspark.sql import SparkSession
from delta import DeltaTable

# ============================
# Spark Session (Gold Layer)
# ============================
spark = (SparkSession.builder
    .appName("CreateGoldTables")
    .master("spark://spark-master:7077")
    .config("spark.submit.deployMode", "client")

    # Delta Lake
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Warehouse Gold
    .config("spark.sql.warehouse.dir", "s3a://gold/warehouse")

    # MinIO
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # JARS
    .config(
        "spark.jars",
        "/opt/spark/jars/hadoop-aws-3.4.0.jar,"
        "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar,"
        "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
        "/opt/spark/jars/delta-storage-4.0.0.jar"
    )
    .getOrCreate()
)

print("[OK] SparkSession Gold creada")

spark.sql("CREATE DATABASE IF NOT EXISTS gold")


# ============================
# Helper
# ============================
def create_table_if_not_exists(path, sql):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[..] Creando tabla en {path}")
        spark.sql(sql)
    else:
        print(f"[OK] Tabla ya existe en {path}")


# ============================
# 1. gold.tendencias_numericas
# ============================
path_tendencias = "s3a://gold/tendencias_numericas"

sql_tendencias = f"""
CREATE TABLE IF NOT EXISTS gold.tendencias_numericas (
    periodo             DATE,
    numero              INT,
    frecuencia_absoluta INT,
    frecuencia_relativa FLOAT,
    racha_ausencia      INT,
    updated_at          TIMESTAMP
)
USING DELTA
LOCATION '{path_tendencias}'
"""

create_table_if_not_exists(path_tendencias, sql_tendencias)


# ============================
# 2. gold.agregaciones_provinciales
# ============================
path_prov = "s3a://gold/agregaciones_provinciales"

sql_prov = f"""
CREATE TABLE IF NOT EXISTS gold.agregaciones_provinciales (
    window_start            TIMESTAMP,
    window_end              TIMESTAMP,
    window_partition_date   DATE,
    codigo_provincia        STRING,
    gasto_acumulado         DECIMAL(14,2),
    num_transacciones       INT,
    canal_predominante      STRING,
    updated_at              TIMESTAMP
)
USING DELTA
LOCATION '{path_prov}'
PARTITIONED BY (window_partition_date)
"""

create_table_if_not_exists(path_prov, sql_prov)


print("\n[OK] Tablas Gold creadas correctamente")
spark.stop()
