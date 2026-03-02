from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/project")

# Imports
from ingesta.deso_na import run_bronze_deso_na
from ingesta.deso_re import run_bronze_deso_re
from ingesta.fuerza_trabajo import run_bronze_fuerza_trabajo
from ingesta.ipc import run_bronze_ipc
from ingesta.pbi import run_bronze_pbi
from ingesta.imacec import run_bronze_imacec
from ingesta.uf import run_bronze_uf

from transformacion.tr_deso_na import run_silver_deso_na
from transformacion.tr_deso_re import run_silver_deso_re
from transformacion.tr_fuerza_trabajo import run_silver_fuerza_trabajo
from transformacion.tr_ipc import run_silver_ipc
from transformacion.tr_pbi import run_silver_pbi
from transformacion.tr_imacec import run_silver_imacec
from transformacion.tr_uf import run_silver_uf

from datos_gold.gold_deso_na import run_gold_deso_na
from datos_gold.gold_deso_re import run_gold_deso_re
from datos_gold.gold_fuerza_trabajo import run_gold_fuerza_trabajo
from datos_gold.gold_ipc import run_gold_ipc
from datos_gold.gold_pbi import run_gold_pbi
from datos_gold.gold_imacec import run_gold_imacec
from datos_gold.gold_uf import run_gold_uf

from power_bi.powerbi_refresh import refresh_powerbi_dataset

default_args = {
    "owner": "julian",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bc_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    default_args=default_args,
    tags=["banco_central", "medallion"],
) as dag:

    # ================= BRONZE =================
    with TaskGroup("bronze_layer") as bronze_group:

        bronze_deso_na = PythonOperator(
            task_id="bronze_deso_na",
            python_callable=run_bronze_deso_na,
        )

        bronze_deso_re = PythonOperator(
            task_id="bronze_deso_re",
            python_callable=run_bronze_deso_re,
        )

        bronze_fuerza_trabajo = PythonOperator(
            task_id="bronze_fuerza_trabajo",
            python_callable=run_bronze_fuerza_trabajo,
        )

        bronze_ipc = PythonOperator(
            task_id="bronze_ipc",
            python_callable=run_bronze_ipc,
        )

        bronze_pbi = PythonOperator(
            task_id="bronze_pbi",
            python_callable=run_bronze_pbi,
        )

        bronze_imacec = PythonOperator(
            task_id="bronze_imacec",
            python_callable=run_bronze_imacec,
        )

        bronze_uf = PythonOperator(
            task_id="bronze_uf",
            python_callable=run_bronze_uf,
        )

    # ================= SILVER =================
    with TaskGroup("silver_layer") as silver_group:

        silver_deso_na = PythonOperator(
            task_id="silver_deso_na",
            python_callable=run_silver_deso_na,
        )

        silver_deso_re = PythonOperator(
            task_id="silver_deso_re",
            python_callable=run_silver_deso_re,
        )

        silver_fuerza_trabajo = PythonOperator(
            task_id="silver_fuerza_trabajo",
            python_callable=run_silver_fuerza_trabajo,
        )

        silver_ipc = PythonOperator(
            task_id="silver_ipc",
            python_callable=run_silver_ipc,
        )

        silver_pbi = PythonOperator(
            task_id="silver_pbi",
            python_callable=run_silver_pbi,
        )

        silver_imacec = PythonOperator(
            task_id="silver_imacec",
            python_callable=run_silver_imacec,
        )

        silver_uf = PythonOperator(
            task_id="silver_uf",
            python_callable=run_silver_uf,
        )

    # ================= GOLD =================
    with TaskGroup("gold_layer") as gold_group:

        gold_deso_na = PythonOperator(
            task_id="gold_deso_na",
            python_callable=run_gold_deso_na,
        )

        gold_deso_re = PythonOperator(
            task_id="gold_deso_re",
            python_callable=run_gold_deso_re,
        )

        gold_fuerza_trabajo = PythonOperator(
            task_id="gold_fuerza_trabajo",
            python_callable=run_gold_fuerza_trabajo,
        )

        gold_ipc = PythonOperator(
            task_id="gold_ipc",
            python_callable=run_gold_ipc,
        )

        gold_pbi = PythonOperator(
            task_id="gold_pbi",
            python_callable=run_gold_pbi,
        )

        gold_imacec = PythonOperator(
            task_id="gold_imacec",
            python_callable=run_gold_imacec,
        )

        gold_uf = PythonOperator(
            task_id="gold_uf",
            python_callable=run_gold_uf,
        )

    # ================= POWER BI REFRESH =================
    with TaskGroup("power_bi_layer") as power_bi_group:
        refresh_powerbi = PythonOperator(
            task_id="refresh_powerbi",
            python_callable=refresh_powerbi_dataset,
        )

    # ================= PIPELINE =================
    bronze_group >> silver_group >> gold_group >> power_bi_group
