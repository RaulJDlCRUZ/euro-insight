from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "raul",
    "start_date": datetime(2026, 6, 7),
}


# ============================
# Spark submit command
# ============================
spark_submit_cmd = """
spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--packages io.delta:delta-spark_2.13:4.0.0 \
--jars /opt/spark/jars/hadoop-aws-3.4.0.jar,/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
--conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
--conf "spark.hadoop.fs.s3a.secret.key=minioadmin123" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
/opt/spark-apps/bronze_to_silver_sorteos.py \
--bronze_path s3a://bronze/sorteos_raw \
--silver_valid s3a://silver/sorteos_validados \
--silver_quarantine s3a://silver/sorteos_quarantine \
--pipeline_run_id {{ ts_nodash }}
"""


# ============================
# DAG
# ============================
with DAG(
    dag_id="bronze_to_silver_sorteos",
    default_args=default_args,
    schedule=None,   # manual trigger
    catchup=False,
    description="Pipeline Bronze → Silver (sorteos ES + UK)",
) as dag:

    bronze_to_silver = BashOperator(
        task_id="run_bronze_to_silver",
        bash_command=spark_submit_cmd
    )
