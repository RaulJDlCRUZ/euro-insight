from pyspark.sql import SparkSession
from delta import DeltaTable

# ============================
# Spark Session (Silver Layer)
# ============================
spark = (SparkSession.builder
    .appName("CreateSilverTables")
    .master("spark://spark-master:7077")
    .config("spark.submit.deployMode", "client")

    # Delta
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Warehouse Silver
    .config("spark.sql.warehouse.dir", "s3a://silver/warehouse")

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

print("[OK] SparkSession Silver creada")

spark.sql("CREATE DATABASE IF NOT EXISTS silver")


# ============================
# Helper
# ============================
def create_table_if_not_exists(path, sql):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[..] Creando tabla en {path}")
        spark.sql(sql)
    else:
        print(f"[OK] Existe tabla en {path}")


# ============================
# 1. sorteos_validados
# ============================
path_sorteos_validados = "s3a://silver/sorteos_validados"

sql_sorteos_validados = f"""
CREATE TABLE IF NOT EXISTS silver.sorteos_validados (
    fecha_sorteo    DATE,
    dia_semana      STRING,
    numero_1        INT,
    numero_2        INT,
    numero_3        INT,
    numero_4        INT,
    numero_5        INT,
    estrella_1      INT,
    estrella_2      INT,
    recaudacion     DECIMAL(14,2),
    pais_origen     STRING,
    pipeline_run_id   STRING,
    validated_at    TIMESTAMP,
    anio            INT,
    mes             INT
)
USING DELTA
LOCATION '{path_sorteos_validados}'
PARTITIONED BY (anio, mes)
"""

create_table_if_not_exists(path_sorteos_validados, sql_sorteos_validados)


# ============================
# 2. apuestas_enriquecidas
# ============================
path_apuestas = "s3a://silver/apuestas_enriquecidas"

sql_apuestas = f"""
CREATE TABLE IF NOT EXISTS silver.apuestas_enriquecidas (
    timestamp           TIMESTAMP,
    fecha_particion     DATE,
    id_transaccion      STRING,
    codigo_provincia    STRING,
    id_administracion   STRING,
    numeros             ARRAY<INT>,
    estrellas           ARRAY<INT>,
    importe             DECIMAL(8,2),
    tipo_apuesta        STRING,
    canal               STRING,
    nombre_provincia    STRING,
    poblacion_total     INT,
    renta_media_hogar   DECIMAL(10,2),
    tasa_paro           FLOAT
)
USING DELTA
LOCATION '{path_apuestas}'
PARTITIONED BY (fecha_particion)
"""

create_table_if_not_exists(path_apuestas, sql_apuestas)


# ============================
# 3. sorteos_quarantine
# ============================
path_q1 = "s3a://silver/sorteos_quarantine"

sql_q1 = f"""
CREATE TABLE IF NOT EXISTS silver.sorteos_quarantine (
    raw_record         STRING,
    source             STRING,
    validation_error   STRING,
    pipeline_run_id    STRING,
    ingestion_ts       TIMESTAMP,
    quarantined_at     TIMESTAMP
)
USING DELTA
LOCATION '{path_q1}'
PARTITIONED BY (source)
"""

create_table_if_not_exists(path_q1, sql_q1)


# ============================
# 4. apuestas_quarantine
# ============================
path_q2 = "s3a://silver/apuestas_quarantine"

sql_q2 = f"""
CREATE TABLE IF NOT EXISTS silver.apuestas_quarantine (
    raw_record      STRING,
    reason          STRING,
    pipeline_run_id   STRING,
    ingestion_ts    TIMESTAMP
)
USING DELTA
LOCATION '{path_q2}'
"""

create_table_if_not_exists(path_q2, sql_q2)


print("\n[OK] Tablas Silver creadas correctamente")
spark.stop()
