"""
pipeline_apuestas_streaming.py
──────────────────────────────
Pipeline Spark Structured Streaming que:

  1. Consume `apuestas-raw` desde Kafka (event-time sobre campo `timestamp`).
  2. Persiste la capa BRONZE: cada mensaje raw → Delta en MinIO (s3a://bronze/apuestas_raw).
  3. Calcula agregaciones por ventana de 1 min / provincia → capa GOLD.

Diseño de event-time:
  • watermark  = 2 minutos  → tolera eventos tardíos hasta 2 min.
  • micro-batch = 30 segundos (ProcessingTime trigger).
  • ventana     = 1 minuto con deslizamiento implícito (tumbling).

Ejecución:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 \
      pipeline_apuestas_streaming.py
"""

import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)
from pyspark.sql.streaming import StreamingQuery

# ═══════════════════════════════════════════════════════════
# 1. CONFIGURACIÓN SPARK
# ═══════════════════════════════════════════════════════════

PIPELINE_RUN_ID = str(uuid.uuid4())   # identificador único de esta ejecución

# Rutas Delta en MinIO
PATH_BRONZE_APUESTAS        = "s3a://bronze/apuestas_raw"
PATH_GOLD_AGREGACIONES      = "s3a://gold/agregaciones_provinciales"
PATH_CHECKPOINT_BRONZE      = "s3a://bronze/_checkpoints/apuestas_raw"
PATH_CHECKPOINT_GOLD        = "s3a://gold/_checkpoints/agregaciones_provinciales"

# Kafka
KAFKA_BOOTSTRAP   = "localhost:9094"
KAFKA_TOPIC_INPUT = "apuestas-raw"

# Parámetros de streaming
WATERMARK_DELAY  = "2 minutes"
TRIGGER_INTERVAL = "30 seconds"
WINDOW_DURATION  = "1 minute"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("Pipeline-Apuestas-Streaming")
        .master("spark://spark-master:7077")
        .config("spark.submit.deployMode", "client")

        # ── Delta Lake ──────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # ── Warehouse apunta a bucket bronze ───────────────────
        .config("spark.sql.warehouse.dir", "s3a://bronze/warehouse")

        # ── MinIO (S3A) ─────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.access.key",              "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",              "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint",                "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access",       "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",  "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # ── JARs locales ────────────────────────────────────────
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/hadoop-aws-3.4.0.jar",
                "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar",
                "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
                "/opt/spark/jars/delta-storage-4.0.0.jar",
                # El conector Kafka debe estar disponible en el classpath o
                # descargarse con --packages spark-sql-kafka-0-10
                "/opt/spark/jars/spark-sql-kafka-0-10_2.13-3.5.0.jar",
                "/opt/spark/jars/kafka-clients-3.4.0.jar",
            ]),
        )

        # ── Optimizaciones Delta Streaming ──────────────────────
        # Limita cuántos ficheros nuevos procesa por micro-batch (evita picos)
        .config("spark.databricks.delta.maxBytesPerTrigger", "128m")
        # Permite a Delta hacer OPTIMIZE+ZORDER en background
        .config("spark.databricks.delta.optimizeWrite.enabled",    "true")
        .config("spark.databricks.delta.autoCompact.enabled",      "true")
    )

    return builder.getOrCreate()


# ═══════════════════════════════════════════════════════════
# 2. SCHEMA DEL MENSAJE JSON
# ═══════════════════════════════════════════════════════════

SCHEMA_APUESTA = StructType([
    StructField("timestamp",         TimestampType(), nullable=False),
    StructField("id_transaccion",    StringType(),    nullable=False),
    StructField("codigo_provincia",  StringType(),    nullable=False),
    StructField("id_administracion", StringType(),    nullable=True),
    StructField("tipo_apuesta",      StringType(),    nullable=True),
    StructField("numeros",           ArrayType(IntegerType()), nullable=True),
    StructField("estrellas",         ArrayType(IntegerType()), nullable=True),
    StructField("importe",           DoubleType(),    nullable=True),
    StructField("canal",             StringType(),    nullable=True),
])


# ═══════════════════════════════════════════════════════════
# 3. INGESTA KAFKA → DATAFRAME BASE
# ═══════════════════════════════════════════════════════════

def read_kafka(spark: SparkSession):
    """
    Lee del tópico apuestas-raw con semántica at-least-once.
    Devuelve un DataFrame con las columnas del mensaje parseadas + metadatos Kafka.
    """
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP)
        .option("subscribe",                KAFKA_TOPIC_INPUT)
        # Punto de inicio: latest en producción, earliest en replay/backfill
        .option("startingOffsets",          "latest")
        # Cuántos offsets procesar por micro-batch (control de throughput)
        .option("maxOffsetsPerTrigger",     50_000)
        # Tolerancia a particiones ausentes al inicio (útil en arranque en frío)
        .option("failOnDataLoss",           "false")
        .load()
    )

    # El payload llega en `value` como bytes; lo casteamos a STRING y parseamos
    parsed = (
        raw_kafka
        .select(
            # ── Metadatos Kafka ─────────────────────────
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            # ── Cuerpo del mensaje ───────────────────────
            F.col("value").cast(StringType()).alias("raw_json"),
            # Parseamos el JSON en columnas estructuradas
            F.from_json(
                F.col("value").cast(StringType()),
                SCHEMA_APUESTA,
            ).alias("data"),
        )
        # Elevamos campos del struct al nivel raíz
        .select(
            "raw_json",
            "kafka_offset",
            "kafka_partition",
            "data.*",   # timestamp, id_transaccion, codigo_provincia, …
        )
        # Añadimos metadatos de ingesta
        .withColumn("ingestion_ts",    F.current_timestamp())
        .withColumn("ingestion_date",  F.to_date(F.current_timestamp()))
        .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        # Descartamos mensajes con timestamp nulo (malformados)
        .filter(F.col("timestamp").isNotNull())
    )

    return parsed


# ═══════════════════════════════════════════════════════════
# 4. ESCRITURA BRONZE  (raw → Delta)
# ═══════════════════════════════════════════════════════════

def write_bronze(df) -> StreamingQuery:
    """
    Persiste el stream raw en la tabla Delta bronze.apuestas_raw.
    Sólo guarda las columnas definidas en el contrato de la tabla.

    CREATE TABLE bronze.apuestas_raw:
        raw_json, kafka_offset, kafka_partition,
        ingestion_ts, ingestion_date, pipeline_run_id
    PARTITIONED BY (ingestion_date)
    """
    bronze_df = df.select(
        "raw_json",
        "kafka_offset",
        "kafka_partition",
        "ingestion_ts",
        "ingestion_date",
        "pipeline_run_id",
    )

    return (
        bronze_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", PATH_CHECKPOINT_BRONZE)
        # Escribimos directamente a la ruta S3A (sin catálogo persistente)
        .option("path", PATH_BRONZE_APUESTAS)
        # Partition pruning: cada micro-batch sólo toca la partición del día
        .partitionBy("ingestion_date")
        .start()
    )


# ═══════════════════════════════════════════════════════════
# 5. AGREGACIONES GOLD  (event-time + watermark)
# ═══════════════════════════════════════════════════════════

def build_gold_aggregations(df):
    """
    Aplica watermark y ventana tumbling de 1 minuto agrupada por provincia.

    Semántica:
      • watermark("timestamp", "2 minutes"):
            Spark espera hasta 2 min de retraso antes de cerrar una ventana.
            Eventos con retraso > 2 min son descartados (late data drop).
      • window("timestamp", "1 minute"):
            Ventana tumbling de 60 s anclada al event-time del mensaje.
      • outputMode("append"):
            Sólo emite filas de ventanas CERRADAS (no actualiza rows previos).
            Correcto con watermark; evita re-escrituras en Delta.
    """
    return (
        df
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("timestamp"), WINDOW_DURATION),
            F.col("codigo_provincia"),
        )
        .agg(
            F.sum("importe")
             .cast("decimal(14,2)")
             .alias("gasto_acumulado"),

            F.count("id_transaccion")
             .cast("int")
             .alias("num_transacciones"),

            F.first("canal").alias("canal_predominante"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")

        .withColumn("window_partition_date", F.to_date("window_start"))

        .withColumn("updated_at", F.current_timestamp())

        .select(
            "window_start",
            "window_end",
            "window_partition_date",
            "codigo_provincia",
            "gasto_acumulado",
            "num_transacciones",
            "canal_predominante",
            "updated_at",
        )
    )

def write_gold(gold_df) -> StreamingQuery:
    """
    Escribe las agregaciones en s3a://gold/agregaciones_provinciales (Delta).

    outputMode=append: sólo emite ventanas cerradas → sin riesgo de upserts.
    """
    return (
        gold_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", PATH_CHECKPOINT_GOLD)
        .option("path", PATH_GOLD_AGREGACIONES)
        .start()
    )


# ═══════════════════════════════════════════════════════════
# 6. CREACIÓN DE TABLAS DELTA (idempotente, sin catálogo)
# ═══════════════════════════════════════════════════════════

DDL_BRONZE = f"""
    CREATE TABLE IF NOT EXISTS bronze.apuestas_raw (
        raw_json        STRING,
        kafka_offset    BIGINT,
        kafka_partition INT,
        ingestion_ts    TIMESTAMP,
        ingestion_date  DATE,
        pipeline_run_id STRING
    )
    USING DELTA
    LOCATION '{PATH_BRONZE_APUESTAS}'
    PARTITIONED BY (ingestion_date)
"""

DDL_GOLD = f"""
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
LOCATION '{PATH_GOLD_AGREGACIONES}'
PARTITIONED BY (window_partition_date)
"""


def ensure_tables(spark: SparkSession):
    """
    Crea las tablas Delta si no existen (DDL idempotente).
    Al no usar catálogo persistente, el LOCATION ancla la tabla a MinIO.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    # SOLO crear si no existe físicamente (no validar schema)
    if not spark.catalog.tableExists("gold.agregaciones_provinciales"):
        spark.sql(DDL_GOLD)

    print("✓ Tablas Delta verificadas")


# ═══════════════════════════════════════════════════════════
# 7. ENTRYPOINT
# ═══════════════════════════════════════════════════════════

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'═'*60}")
    print(f"  Pipeline Apuestas Streaming  |  run={PIPELINE_RUN_ID[:8]}")
    print(f"{'═'*60}\n")

    # 7.1  Garantizamos que las tablas Delta existen en MinIO
    ensure_tables(spark)

    # 7.2  Leemos Kafka y parseamos el JSON
    base_df = read_kafka(spark)

    # 7.3  Stream BRONZE: persiste raw sin transformar
    q_bronze: StreamingQuery = write_bronze(base_df)
    print(f"✓ Query Bronze iniciada  [id={q_bronze.id}]")

    # 7.4  Stream GOLD: agrega por ventana + provincia
    gold_df            = build_gold_aggregations(base_df)
    q_gold: StreamingQuery = write_gold(gold_df)
    print(f"✓ Query Gold   iniciada  [id={q_gold.id}]")

    print("\nStreaming activo. Ctrl+C para detener.\n")

    try:
        # awaitAnyTermination bloquea hasta que alguna query falle o se cancele
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nSeñal de interrupción recibida. Deteniendo queries…")
    finally:
        q_bronze.stop()
        q_gold.stop()
        spark.stop()
        print("Pipeline detenido correctamente.")


if __name__ == "__main__":
    main()
