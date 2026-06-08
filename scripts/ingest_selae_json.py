import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date
)

TABLE_PATH = "s3a://bronze/scraping_selae_raw"


# ----------------------------
# Spark Session
# ----------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("IngestScrapingSelaeRawBronze")
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


# ----------------------------
# Ingestión JSON raw
# ----------------------------
def ingest_json_raw(spark, input_path, table_path, pipeline_run_id):

    # 1. leer JSONL como RAW TEXT (NO JSON PARSE)
    df = spark.read.text(input_path)

    # 2. renombrar a mi atributo definido para bronze
    df = df.select(
        col("value").alias("raw_json")
    )

    # 3. metadata bronze (sin tocar JSON)
    df_final = (
        df
        .withColumn("kafka_offset", lit(None).cast("long"))
        .withColumn("kafka_partition", lit(None).cast("int"))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_date", to_date(current_timestamp()))
        .withColumn("pipeline_run_id", lit(pipeline_run_id))
    )

    # 4. escritura delta (SIN mergeSchema, SIN cambios de tabla)
    (
        df_final
        .write
        .format("delta")
        .mode("append")
        .save(table_path)
    )

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--pipeline_run_id", required=True)

    args = parser.parse_args()

    spark = create_spark()

    ingest_json_raw(
        spark=spark,
        input_path=args.input_path,
        table_path=TABLE_PATH,
        pipeline_run_id=args.pipeline_run_id
    )

    spark.stop()
