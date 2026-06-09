from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "raul",
    "start_date": datetime(2026, 6, 7),
}

with DAG(
    dag_id="ingest_kaggle_sorteos",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    ingest_es = BashOperator(
        task_id="ingest_kaggle_es",
        bash_command="""
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
        /opt/project/scripts/ingest_sorteos_raw.py \
        --input_path /opt/project/data/miguelminambres/Euromillones-Draw-Data.csv \
        --source kaggle_es \
        --pipeline_run_id {{ dag.dag_id }}_{{ ts_nodash }}
        """
    )

    ingest_uk = BashOperator(
        task_id="ingest_kaggle_uk",
        bash_command="""
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
        /opt/project/scripts/ingest_sorteos_raw.py \
        --input_path /opt/project/data/sihantao0626/euromillions.csv \
        --source kaggle_uk \
        --pipeline_run_id {{ dag.dag_id }}_{{ ts_nodash }}
        """
    )

    ingest_es >> ingest_uk