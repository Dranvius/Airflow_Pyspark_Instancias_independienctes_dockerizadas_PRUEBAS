from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="kafka_test_api",
    start_date=datetime(2025, 9, 22),
    schedule=None,   # manual
    catchup=False, # No recurar fechas pasadas
) as dag:

        # T1 
    t1 = SparkSubmitOperator(
        task_id="kafka_test",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_4.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"},
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.7.0",
    )

    t1