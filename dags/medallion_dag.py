from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Agregar el path del proyecto al PYTHONPATH
sys.path.append("/opt/airflow/project")

# Importar tus funciones reales
from ingesta.run_bronze import run_bronze
from transformacion.run_silver import run_silver
from datos_gold.run_gold import run_gold


with DAG(
    dag_id="bc_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["banco_central", "medallion"]
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze
    )

    silver_task = PythonOperator(
        task_id="silver",
        python_callable=run_silver
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=run_gold
    )

    bronze_task >> silver_task >> gold_task