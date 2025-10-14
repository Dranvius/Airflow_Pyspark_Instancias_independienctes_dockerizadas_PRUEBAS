from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="rabbit_to_db_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",  # o @daily o None
    catchup=False,
    description="Consume la cola de procesamiento y luego la guarda en una base de datos"

) as dag:

    load_rabbit_to_db = SparkSubmitOperator(
        task_id="carga_a_base_de_datos",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_5.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True
    )

    load_rabbit_to_db
