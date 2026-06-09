from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from pathlib import Path


def convert_json_to_jsonl():
    input_path = "/opt/project/data/raw/euromillones_historico_completo.json"
    output_path = "/opt/project/data/out/euromillones_historico_completo.jsonl"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("El input JSON no es una lista")

    with open(output_path, "w", encoding="utf-8") as f:
        for i, row in enumerate(data):
            if not isinstance(row, dict):
                continue
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"OK: {len(data)} registros convertidos")


default_args = {
    "owner": "data-eng",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="convert_scraping_json_to_jsonl",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    convert_task = PythonOperator(
        task_id="convert_json_to_jsonl",
        python_callable=convert_json_to_jsonl
    )

    convert_task
