import gc
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta import DeltaTable


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

DATA_ROOT      = Path("/opt/project/data")
PATH_MUNICIPAL = DATA_ROOT / "ine_detalle_municipal_csv"
PATH_RENTA     = DATA_ROOT / "ine_atlas_distrib_renta_hogares_csv" / "30824.csv"

S3_BRONZE_PATH = "s3a://bronze/ine_raw"

# Identificador único de esta ejecución del pipeline
PIPELINE_RUN_ID = str(uuid.uuid4())
INGESTION_TS    = datetime.now(timezone.utc)

SOURCE_MUNICIPAL = "ine_municipal"
SOURCE_RENTA     = "ine_renta_hogares"

# Nº de líneas por escritura parcial a Delta.
# Con 2 GB por worker, 5.000 líneas es conservador:
# el driver nunca acumula más de ~5 MB de strings en heap antes de liberar.
CHUNK_SIZE = 5_000


# ──────────────────────────────────────────────────────────────────────────────
# SESIÓN SPARK
# ──────────────────────────────────────────────────────────────────────────────
def create_spark():
    return (
        SparkSession.builder
        .appName("IngestPOIRawBronze")
        .master("spark://spark-master:7077")
        .config("spark.submit.deployMode", "client")

        # Delta
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.4.0.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar,"
            "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
            "/opt/spark/jars/delta-storage-4.0.0.jar"
        )

        .getOrCreate()
    )

spark = create_spark()

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# ──────────────────────────────────────────────────────────────────────────────
# SCHEMA DE LA TABLA BRONZE
# ──────────────────────────────────────────────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("raw_content",      StringType(),    nullable=False),  # línea original del CSV
    StructField("source",           StringType(),    nullable=False),  # ine_municipal | ine_renta_hogares
    StructField("file_name",        StringType(),    nullable=False),  # nombre del fichero de origen
    StructField("file_md5",         StringType(),    nullable=False),  # hash MD5 del fichero completo
    StructField("file_encoding",    StringType(),    nullable=False),  # utf-8-sig (UTF-8 BOM — estándar INE)
    StructField("ingestion_ts",     TimestampType(), nullable=False),  # timestamp UTC de ingesta
    StructField("pipeline_run_id",  StringType(),    nullable=False),  # UUID de la ejecución
])


# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────

# Encoding hardcodeado: los CSVs del INE se publican en UTF-8 con BOM.
# Python lo gestiona automáticamente con el alias 'utf-8-sig'
# (lee el fichero correctamente y descarta el BOM en la primera línea).
FILE_ENCODING = "utf-8-sig"


def compute_md5(path: Path) -> str:
    """Calcula el hash MD5 del fichero completo."""
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def iter_chunks(path: Path, chunk_size: int):
    """
    Generador: lee el fichero línea a línea y cede listas de hasta `chunk_size`
    líneas. Nunca materializa el fichero completo en memoria.
    """
    chunk = []
    with open(path, encoding=FILE_ENCODING, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.strip():
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
    if chunk:
        yield chunk


def build_spark_rows(lines: list[str], source: str, file_path: Path,
                     md5: str) -> list[dict]:
    """Construye la lista de filas con todos los metadatos de trazabilidad."""
    return [
        {
            "raw_content":     line,
            "source":          source,
            "file_name":       file_path.name,
            "file_md5":        md5,
            "file_encoding":   FILE_ENCODING,
            "ingestion_ts":    INGESTION_TS,
            "pipeline_run_id": PIPELINE_RUN_ID,
        }
        for line in lines
    ]


def ingest_file(file_path: Path, source: str) -> int:
    """
    Procesa un único CSV del INE en chunks de CHUNK_SIZE líneas.
    Cada chunk se escribe en Delta y se descarta antes del siguiente,
    manteniendo el heap del driver bajo control.
    Devuelve el número total de filas ingestadas.
    """
    print(f"  [>>] {file_path.name}")

    md5         = compute_md5(file_path)
    total_lines = 0

    for i, chunk in enumerate(iter_chunks(file_path, CHUNK_SIZE)):
        rows = build_spark_rows(chunk, source, file_path, md5)
        df   = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

        (
            df.write
              .format("delta")
              .mode("append")
              .partitionBy("source")
              .option("mergeSchema", "true")
              .save(S3_BRONZE_PATH)
        )

        total_lines += len(chunk)

        # Liberar el DataFrame y forzar GC del driver tras cada chunk
        df.unpersist()
        del df, rows, chunk
        gc.collect()

    print(f"       encoding={FILE_ENCODING}  md5={md5}  líneas={total_lines}")
    return total_lines


def create_table_if_not_exists(path: str, create_sql: str) -> None:
    """Registra la tabla en el catálogo de Spark si aún no existe."""
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[..] Creando tabla en {path}")
        spark.sql(create_sql)
    else:
        print(f"[OK] Tabla ya existe en {path}")


# ──────────────────────────────────────────────────────────────────────────────
# CREACIÓN DE LA TABLA BRONZE (catálogo)
# ──────────────────────────────────────────────────────────────────────────────

create_sql_ine = f"""
CREATE TABLE IF NOT EXISTS bronze.ine_raw (
    raw_content     STRING      COMMENT 'Línea original del fichero CSV sin transformar',
    source          STRING      COMMENT 'Identificador de la fuente: ine_municipal | ine_renta_hogares',
    file_name       STRING      COMMENT 'Nombre del fichero CSV de origen',
    file_md5        STRING      COMMENT 'Hash MD5 del fichero completo para detección de cambios',
    file_encoding   STRING      COMMENT 'Encoding del fichero (utf-8-sig, UTF-8 con BOM — estándar INE)',
    ingestion_ts    TIMESTAMP   COMMENT 'Timestamp UTC del momento de ingesta',
    pipeline_run_id STRING      COMMENT 'UUID único por ejecución del pipeline'
)
USING DELTA
LOCATION '{S3_BRONZE_PATH}'
PARTITIONED BY (source)
COMMENT 'Capa Bronze — datos INE en crudo (población municipal + renta por hogar)'
"""

create_table_if_not_exists(S3_BRONZE_PATH, create_sql_ine)


# ──────────────────────────────────────────────────────────────────────────────
# INGESTA — POBLACIÓN MUNICIPAL (todos los CSV provinciales)
# ──────────────────────────────────────────────────────────────────────────────

print("\n[1/2] Ingestando datos de población municipal...")
provincial_files = sorted(PATH_MUNICIPAL.glob("*.csv"))

if not provincial_files:
    print(f"  [!!] No se encontraron CSVs en {PATH_MUNICIPAL}")
else:
    total_municipal = 0
    for csv_file in provincial_files:
        total_municipal += ingest_file(csv_file, SOURCE_MUNICIPAL)
    print(f"  [OK] {len(provincial_files)} ficheros | {total_municipal:,} filas ingestadas")

# Limpiar caché Spark y forzar GC del driver antes del siguiente bloque
spark.catalog.clearCache()
gc.collect()


# ──────────────────────────────────────────────────────────────────────────────
# INGESTA — ATLAS DE RENTA POR HOGAR (fichero único 30824.csv)
# ──────────────────────────────────────────────────────────────────────────────

print("\n[2/2] Ingestando datos de renta por hogar...")
if not PATH_RENTA.exists():
    print(f"  [!!] Fichero no encontrado: {PATH_RENTA}")
else:
    total_renta = ingest_file(PATH_RENTA, SOURCE_RENTA)
    print(f"  [OK] 1 fichero | {total_renta:,} filas ingestadas")


# ──────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN FINAL
# ──────────────────────────────────────────────────────────────────────────────

print("\n[!!] Tablas en bronze:")
spark.sql("SHOW TABLES IN bronze").show(truncate=False)

print("[!!] Conteo por source y pipeline_run_id:")
spark.sql(f"""
    SELECT
        source,
        pipeline_run_id,
        COUNT(*)        AS filas,
        COUNT(DISTINCT file_name) AS ficheros,
        MIN(ingestion_ts) AS ingestion_ts
    FROM delta.`{S3_BRONZE_PATH}`
    WHERE pipeline_run_id = '{PIPELINE_RUN_ID}'
    GROUP BY source, pipeline_run_id
    ORDER BY source
""").show(truncate=False)

print(f"\n[OK] Ingesta Bronze completada — pipeline_run_id: {PIPELINE_RUN_ID}")
spark.stop()
