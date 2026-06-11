import re
import gc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2
from psycopg2.extras import execute_values


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

S3_BRONZE_PATH = "s3a://bronze/ine_raw"

DB_CONFIG = {
    "host": "postgres",      # nombre del servicio Docker
    "port": 5432,
    "database": "airflow",   # BD existente para administración
    "user": "airflow",
    "password": "airflow",
}

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
# UTILIDADES DE PARSING
# ──────────────────────────────────────────────────────────────────────────────

# UDF: extrae el código de provincia (2 primeros dígitos del código municipal INE)
# Entrada esperada: "01051 Agurain/Salvatierra" → "01"
@F.udf(StringType())
def extract_codigo_provincia(municipios_field: str) -> str:
    if not municipios_field:
        return None
    match = re.match(r"^(\d{2})", municipios_field.strip())
    return match.group(1) if match else None


# UDF: normaliza números con formato español (1.234,56 → 1234.56)
# Los CSVs del INE usan punto como separador de miles y coma como decimal.
# En los datos municipales observados usan punto como separador de miles sin decimales.
@F.udf(DoubleType())
def parse_numeric_es(value: str) -> float:
    if not value or value.strip() in ("", '""', "-"):
        return None
    # Eliminar comillas, espacios y separador de miles (punto)
    cleaned = value.strip().strip('"').replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# LECTURA DESDE BRONZE
# ──────────────────────────────────────────────────────────────────────────────

print("\n[1/4] Leyendo Bronze...")
df_bronze = spark.read.format("delta").load(S3_BRONZE_PATH)


# ──────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN — POBLACIÓN MUNICIPAL
# Columnas raw: Municipios;Sexo;Periodo;Total
# Filtramos Sexo = 'Total' para evitar doble conteo (Hombres + Mujeres + Total)
# ──────────────────────────────────────────────────────────────────────────────

print("[2/4] Transformando población municipal...")

df_mun_raw = (
    df_bronze
    .filter(F.col("source") == SOURCE_MUNICIPAL)
    .select("raw_content")
    # Ignorar la línea de cabecera
    .filter(~F.col("raw_content").startswith("Municipios;"))
    # Partir el raw_content por el separador
    .withColumn("cols", F.split(F.col("raw_content"), ";"))
    .withColumn("municipios", F.col("cols")[0])
    .withColumn("sexo",       F.col("cols")[1])
    .withColumn("periodo",    F.col("cols")[2].cast(IntegerType()))
    .withColumn("total_raw",  F.col("cols")[3])
    .drop("cols", "raw_content")
)

# Extraer código de provincia y parsear valor numérico
df_mun = (
    df_mun_raw
    .withColumn("codigo_provincia", extract_codigo_provincia(F.col("municipios")))
    .withColumn("total",            parse_numeric_es(F.col("total_raw")))
    .filter(
        F.col("sexo") == "Total"           # evitar doble conteo
      & F.col("codigo_provincia").isNotNull()
      & F.col("total").isNotNull()
      & F.col("periodo").isNotNull()
    )
)

# Año más reciente disponible por provincia
df_mun_anio = (
    df_mun
    .groupBy("codigo_provincia")
    .agg(F.max("periodo").alias("anio_max"))
)

# Población total en el año más reciente
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
# Columnas raw: Municipios;Distritos;Secciones;Indicadores de renta media;Periodo;Total
# Filtramos solo nivel municipal (Distritos y Secciones vacíos) y el indicador
# 'Renta neta media por hogar' para agregar a nivel provincia.
# ──────────────────────────────────────────────────────────────────────────────

print("[3/4] Transformando renta por hogar...")

df_renta_raw = (
    df_bronze
    .filter(F.col("source") == SOURCE_RENTA)
    .select("raw_content")
    .filter(~F.col("raw_content").startswith("Municipios;"))
    .withColumn("cols",       F.split(F.col("raw_content"), ";"))
    .withColumn("municipios", F.col("cols")[0])
    .withColumn("distritos",  F.col("cols")[1])
    .withColumn("secciones",  F.col("cols")[2])
    .withColumn("indicador",  F.col("cols")[3])
    .withColumn("periodo",    F.col("cols")[4].cast(IntegerType()))
    .withColumn("total_raw",  F.col("cols")[5])
    .drop("cols", "raw_content")
)

df_renta = (
    df_renta_raw
    .withColumn("codigo_provincia", extract_codigo_provincia(F.col("municipios")))
    .withColumn("total",            parse_numeric_es(F.col("total_raw")))
    # Solo nivel municipal (sin desglose por distrito ni sección)
    .filter(
        (F.col("distritos").isNull() | (F.col("distritos") == ""))
      & (F.col("secciones").isNull() | (F.col("secciones") == ""))
      & F.col("indicador").contains("Renta neta media por hogar")
      & F.col("codigo_provincia").isNotNull()
      & F.col("total").isNotNull()
      & F.col("periodo").isNotNull()
    )
)

# Año más reciente disponible por provincia
df_renta_anio = (
    df_renta
    .groupBy("codigo_provincia")
    .agg(F.max("periodo").alias("anio_max"))
)

# Media de renta por hogar en el año más reciente
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
# CRUCE POR codigo_provincia
# ──────────────────────────────────────────────────────────────────────────────

print("[4/4] Cruzando y cargando en PostgreSQL...")

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

# Recoger en el driver (52 provincias + Ceuta + Melilla → volumen mínimo)
rows = df_silver.collect()
print(f"  [..] Filas a cargar en PostgreSQL: {len(rows)}")

# Liberar memoria Spark antes de la escritura en PG
df_silver.unpersist()
del df_silver, df_poblacion, df_renta_agg, df_mun, df_renta
spark.catalog.clearCache()
gc.collect()


# ──────────────────────────────────────────────────────────────────────────────
# ESCRITURA EN POSTGRESQL — upsert por codigo_provincia
# Los campos nombre_provincia, densidad_poblacion, tasa_paro y ccaa
# no provienen de esta fuente; se dejan a NULL para poblarse en otro pipeline.
# ──────────────────────────────────────────────────────────────────────────────

UPSERT_SQL = """
    INSERT INTO provincias_demograficas (
        codigo_provincia,
        poblacion_total,
        renta_media_hogar
    )
    VALUES %s
    ON CONFLICT (codigo_provincia) DO UPDATE SET
        poblacion_total   = EXCLUDED.poblacion_total,
        renta_media_hogar = EXCLUDED.renta_media_hogar
"""

pg_rows = [
    (
        row["codigo_provincia"],
        row["poblacion_total"],
        float(row["renta_media_hogar"]) if row["renta_media_hogar"] is not None else None,
    )
    for row in rows
]

with psycopg2.connect(**PG_CONFIG) as conn:
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, pg_rows)
    conn.commit()

print(f"  [OK] {len(pg_rows)} provincias cargadas en provincias_demograficas")


# ──────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN
# ──────────────────────────────────────────────────────────────────────────────

VERIFY_SQL = """
    SELECT
        COUNT(*)                                          AS total_provincias,
        COUNT(poblacion_total)                            AS con_poblacion,
        COUNT(renta_media_hogar)                          AS con_renta,
        SUM(poblacion_total)                              AS poblacion_espana,
        ROUND(AVG(renta_media_hogar)::numeric, 2)         AS renta_media_nacional
    FROM provincias_demograficas
"""

with psycopg2.connect(**PG_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute(VERIFY_SQL)
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, cur.fetchone()))

print("\n  Resumen provincias_demograficas:")
for k, v in result.items():
    print(f"    {k}: {v}")

print("\n[OK] Pipeline Silver completado")
spark.stop()
