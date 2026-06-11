"""
================================================================================
PIPELINE SILVER — Paso 1/2: Transformación (Spark)
Origen  : s3a://bronze/ine_raw       (Delta Lake)
Destino : s3a://silver/ine_provincias_tmp  (CSV, una partición)
Lógica  :
  - Población : suma de Total donde Sexo = 'Total', año más reciente por provincia
  - Renta      : media de Total donde Indicador = 'Renta neta media por hogar',
                 año más reciente por provincia
  - Cruce      : JOIN por codigo_provincia (2 primeros dígitos del código INE)
Ejecutar antes de: silver_ine_load.py
================================================================================
"""

import re
import gc
import csv
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

S3_BRONZE_PATH   = "s3a://bronze/ine_raw"
LOCAL_SILVER_TMP = "/opt/spark-data/ine_provincias_tmp"

SOURCE_MUNICIPAL = "ine_municipal"
SOURCE_RENTA     = "ine_renta_hogares"

# ──────────────────────────────────────────────────────────────────────────────
# SESIÓN SPARK
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# UDFs DE PARSING
# ──────────────────────────────────────────────────────────────────────────────

@F.udf(StringType())
def extract_codigo_provincia(municipios_field: str) -> str:
    """Extrae los 2 primeros dígitos del código INE. '01051 Agurain' → '01'"""
    if not municipios_field:
        return None
    match = re.match(r"^(\d{2})", municipios_field.strip())
    return match.group(1) if match else None


@F.udf(DoubleType())
def parse_numeric_es(value: str) -> float:
    """Normaliza formato numérico INE (punto=miles, coma=decimal) a float."""
    if not value or value.strip() in ("", '""', "-"):
        return None
    cleaned = value.strip().strip('"').replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# LECTURA BRONZE
# ──────────────────────────────────────────────────────────────────────────────

print("\n[1/4] Leyendo Bronze...")
df_bronze = spark.read.format("delta").load(S3_BRONZE_PATH)


# ──────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN — POBLACIÓN MUNICIPAL
# raw: Municipios;Sexo;Periodo;Total
# ──────────────────────────────────────────────────────────────────────────────

print("[2/4] Transformando población municipal...")

df_mun = (
    df_bronze
    .filter(F.col("source") == SOURCE_MUNICIPAL)
    .select("raw_content")
    .filter(~F.col("raw_content").startswith("Municipios;"))
    .withColumn("cols",       F.split("raw_content", ";"))
    .withColumn("municipios", F.col("cols")[0])
    .withColumn("sexo",       F.col("cols")[1])
    .withColumn("periodo",    F.col("cols")[2].cast(IntegerType()))
    .withColumn("total_raw",  F.col("cols")[3])
    .drop("cols", "raw_content")
    .withColumn("codigo_provincia", extract_codigo_provincia("municipios"))
    .withColumn("total",            parse_numeric_es("total_raw"))
    .filter(
        (F.col("sexo") == "Total")
        & F.col("codigo_provincia").isNotNull()
        & F.col("total").isNotNull()
        & F.col("periodo").isNotNull()
    )
)

df_mun_anio = (
    df_mun.groupBy("codigo_provincia")
          .agg(F.max("periodo").alias("anio_max"))
)

df_poblacion = (
    df_mun
    .join(df_mun_anio, on="codigo_provincia")
    .filter(F.col("periodo") == F.col("anio_max"))
    .groupBy("codigo_provincia")
    .agg(
        F.sum("total").cast(IntegerType()).alias("poblacion_total"),
        F.first("anio_max").alias("anio_poblacion")
    )
)

print(f"  [OK] Provincias con población: {df_poblacion.count()}")


# ──────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN — RENTA POR HOGAR
# raw: Municipios;Distritos;Secciones;Indicadores de renta media;Periodo;Total
# ──────────────────────────────────────────────────────────────────────────────

print("[3/4] Transformando renta por hogar...")

df_renta = (
    df_bronze
    .filter(F.col("source") == SOURCE_RENTA)
    .select("raw_content")
    .filter(~F.col("raw_content").startswith("Municipios;"))
    .withColumn("cols",       F.split("raw_content", ";"))
    .withColumn("municipios", F.col("cols")[0])
    .withColumn("distritos",  F.col("cols")[1])
    .withColumn("secciones",  F.col("cols")[2])
    .withColumn("indicador",  F.col("cols")[3])
    .withColumn("periodo",    F.col("cols")[4].cast(IntegerType()))
    .withColumn("total_raw",  F.col("cols")[5])
    .drop("cols", "raw_content")
    .withColumn("codigo_provincia", extract_codigo_provincia("municipios"))
    .withColumn("total",            parse_numeric_es("total_raw"))
    .filter(
        (F.col("distritos").isNull() | (F.col("distritos") == ""))
        & (F.col("secciones").isNull() | (F.col("secciones") == ""))
        & F.col("indicador").contains("Renta neta media por hogar")
        & F.col("codigo_provincia").isNotNull()
        & F.col("total").isNotNull()
        & F.col("periodo").isNotNull()
    )
)

df_renta_anio = (
    df_renta.groupBy("codigo_provincia")
            .agg(F.max("periodo").alias("anio_max"))
)

df_renta_agg = (
    df_renta
    .join(df_renta_anio, on="codigo_provincia")
    .filter(F.col("periodo") == F.col("anio_max"))
    .groupBy("codigo_provincia")
    .agg(
        F.round(F.avg("total"), 2).alias("renta_media_hogar"),
        F.first("anio_max").alias("anio_renta")
    )
)

print(f"  [OK] Provincias con renta: {df_renta_agg.count()}")


# ──────────────────────────────────────────────────────────────────────────────
# CRUCE Y ESCRITURA CSV DESDE EL DRIVER
# collect() es seguro (~54 filas). Escribimos con csv stdlib para evitar el
# FileOutputCommitter de Spark y los problemas de rename en volúmenes Docker.
# Idempotencia: escritura atómica tmp → final con os.replace(), sin borrar
# ficheros ajenos que pudieran tener permisos del worker.
# ──────────────────────────────────────────────────────────────────────────────

print("[4/4] Cruzando y escribiendo CSV en spark-data...")

df_silver = (
    df_poblacion
    .join(df_renta_agg, on="codigo_provincia", how="full")
    .select(
        "codigo_provincia",
        "poblacion_total",
        "renta_media_hogar",
        "anio_poblacion",
        "anio_renta"
    )
    .orderBy("codigo_provincia")
)

silver_rows = df_silver.collect()

output_dir = Path(LOCAL_SILVER_TMP)
output_dir.mkdir(parents=True, exist_ok=True)

csv_path = output_dir / "part-00000.csv"
tmp_path = output_dir / "part-00000.tmp"

with open(tmp_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "codigo_provincia", "poblacion_total",
        "renta_media_hogar", "anio_poblacion", "anio_renta"
    ])
    writer.writeheader()
    for row in silver_rows:
        writer.writerow(row.asDict())

# Rename atómico: sobreescribe el fichero final sin tocar otros ficheros del directorio
tmp_path.replace(csv_path)

print(f"  [OK] {len(silver_rows)} filas escritas en {csv_path}")

# Liberar antes de salir
df_silver.unpersist()
del df_silver, df_poblacion, df_renta_agg, df_mun, df_renta
spark.catalog.clearCache()
gc.collect()

print("\n[OK] silver_ine_transform completado")
spark.stop()
