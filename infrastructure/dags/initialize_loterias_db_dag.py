from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "raul",
    "start_date": datetime(2026, 6, 7),
}

with DAG(
    dag_id="initialize_loterias_database",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    initialize_database = BashOperator(
        task_id="initialize_loterias_database_task",
        bash_command="""
        python /opt/project/scripts/init_loterias_db.py
        """
    )
