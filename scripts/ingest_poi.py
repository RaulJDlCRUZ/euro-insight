import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col

TABLE_PATH = "s3a://bronze/puntos_venta_raw"


# ----------------------------
# Spark Session
# ----------------------------
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


# ----------------------------
# Ingestión POI
# ----------------------------
def ingest_poi_raw(spark, input_path, source, table_path, pipeline_run_id):

    # lectura completamente RAW (línea a línea)
    df = spark.read.text(input_path)

    # opcional: eliminar header si existiera accidentalmente
    header = df.first()["value"]

    df_raw = (
        df
        .filter(col("value") != header)
        .select(
            col("value").alias("raw_content")
        )
    )

    df_final = (
        df_raw
        .withColumn("source", lit(source))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("pipeline_run_id", lit(pipeline_run_id))
    )

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("source")
        .save(table_path)
    )

    print(f"[OK] Ingesta POI completada -> {table_path}")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--source", required=True, default="selae_poi")
    parser.add_argument("--pipeline_run_id", required=True)

    args = parser.parse_args()

    spark = create_spark()

    ingest_poi_raw(
        spark=spark,
        input_path=args.input_path,
        source=args.source,
        table_path=TABLE_PATH,
        pipeline_run_id=args.pipeline_run_id
    )

    spark.stop()