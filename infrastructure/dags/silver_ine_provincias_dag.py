from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "raul",
    "start_date": datetime(2026, 6, 7),
}

transform_command = """
spark-submit \
--master spark://spark-master:7077 \
--packages io.delta:delta-spark_2.13:4.0.0 \
--jars /opt/spark/jars/hadoop-aws-3.4.0.jar,/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar \
--conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
--conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
--conf "spark.hadoop.fs.s3a.secret.key=minioadmin123" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
/opt/spark-apps/silver_ine_transform.py
"""


with DAG(
    dag_id="silver_ine_provincias",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # Paso 1: Spark lee Bronze, transforma y escribe CSV en MinIO
    transform = BashOperator(
        task_id="silver_ine_transform",
        # bash_command="python /opt/spark-apps/silver_ine_transform.py",
        bash_command=transform_command,
    )

    # Paso 2: Pandas + psycopg2 lee el CSV de MinIO y hace upsert en PostgreSQL.
    # Solo se ejecuta si el paso anterior ha terminado correctamente.
    load = BashOperator(
        task_id="silver_ine_load",
        bash_command="python /opt/spark-apps/silver_ine_load.py",
    )

    transform >> load
