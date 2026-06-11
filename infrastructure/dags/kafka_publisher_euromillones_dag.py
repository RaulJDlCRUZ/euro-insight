from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "raul",
    "start_date": datetime(2026, 6, 16),  # Primer lunes tras la validación
    "retries": 2,
    "retry_delay": 300,  # 5 min entre reintentos (margen si la web tarda)
}

with DAG(
    dag_id="kafka_publisher_euromillones",
    default_args=default_args,
    schedule="0 8 * * 1",  # Lunes 08:00 — da margen al sorteo del domingo y queden publicados resultados por la noche
    catchup=False,
    tags=["kafka", "euromillones", "ingesta"],
) as dag:

    publicar_resultados_semana = BashOperator(
        task_id="publicar_resultados_semana",
        bash_command="python /opt/project/spark-apps/kafka_publisher_euromillones.py",
    )
