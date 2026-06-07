from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_medallion",
    description="Pipeline Medallion con Spark + Delta + MinIO",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "delta", "minio"],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job",

        conn_id="spark_default",

        application="/opt/spark-apps/spark_job.py",

        name="crear_arquitectura_medallion",

        packages="org.apache.hadoop:hadoop-aws:3.4.0,"
                 "software.amazon.awssdk:bundle:2.23.19,"
                 "io.delta:delta-spark_2.13:4.0.0",

        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
        },

        verbose=True,
    )

    submit_spark_job
