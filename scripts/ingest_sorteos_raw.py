import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    col,
    concat_ws
)

TABLE_PATH = "s3a://bronze/sorteos_raw"

VALID_SOURCES = {
    "kaggle_es",
    "kaggle_uk"
}


# ----------------------------
# Spark Session
# ----------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("IngestSorteosRawBronze")
        .master("spark://spark-master:7077")
        .config("spark.submit.deployMode", "client")

        # Delta
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # Warehouse
        .config(
            "spark.sql.warehouse.dir",
            "s3a://bronze/warehouse"
        )

        # MinIO
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "minioadmin"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "minioadmin123"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
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


def _clean_uk_multiline(input_path: str) -> str:
    """
    Elimina los saltos de línea espurios ('\\nRolled') del CSV de UK
    que Spark no absorbe por estar fuera de campos quoted.
    Escribe el fichero limpio junto al original y devuelve su path.
    """
    clean_path = input_path.replace(".csv", "_cleaned.csv")

    with open(input_path, "r", encoding="utf-8") as f:
        raw = f.read()

    cleaned = re.sub(r'\n[ \t]*Rolled', '', raw, flags=re.IGNORECASE)

    with open(clean_path, "w", encoding="utf-8") as f:
        f.write(cleaned)

    return clean_path


# ----------------------------
# Ingestión
# ----------------------------
def ingest_sorteos_raw(
    spark,
    input_path,
    source,
    table_path,
    pipeline_run_id
):

    if source not in VALID_SOURCES:
        raise ValueError(
            f"Source inválido: {source}"
        )

    if source == "kaggle_uk":
        input_path = _clean_uk_multiline(input_path)

    # -----------------------------------
    # Leer CSV respetando registros multilínea
    # -----------------------------------
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(input_path)
    )

    if df.rdd.isEmpty():
        raise ValueError(
            f"El fichero está vacío: {input_path}"
        )

    # -----------------------------------
    # Reconstruir el registro lógico
    # -----------------------------------
    # Se concatenan las columnas originales en el orden
    # en que aparecen en el CSV para mantener un payload
    # "raw" homogéneo.
    df_raw = (
        df
        .select(
            concat_ws(
                ",",
                *[col(c).cast("string") for c in df.columns]
            ).alias("raw_content")
        )
    )

    # Metadatos Bronze
    df_final = (
        df_raw
        .withColumn(
            "source",
            lit(source)
        )
        .withColumn(
            "ingestion_ts",
            current_timestamp()
        )
        .withColumn(
            "pipeline_run_id",
            lit(pipeline_run_id)
        )
        .select(
            "raw_content",
            "source",
            "ingestion_ts",
            "pipeline_run_id",
        )
    )

    print("[INFO] Vista previa:")

    df_final.show(
        10,
        truncate=False
    )

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("source")
        .save(table_path)
    )

    print(
        f"[OK] Ingesta completada -> {table_path}"
    )


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_path",
        required=True
    )

    parser.add_argument(
        "--source",
        required=True,
        choices=[
            "kaggle_es",
            "kaggle_uk"
        ]
    )

    parser.add_argument(
        "--pipeline_run_id",
        required=True
    )

    args = parser.parse_args()

    spark = create_spark()

    try:

        ingest_sorteos_raw(
            spark=spark,
            input_path=args.input_path,
            source=args.source,
            table_path=TABLE_PATH,
            pipeline_run_id=args.pipeline_run_id
        )

    finally:
        spark.stop()
