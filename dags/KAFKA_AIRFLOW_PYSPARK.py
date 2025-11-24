from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# Ejecucion cada minuto
with DAG(
    dag_id="kafka_test_api",
    start_date=datetime(2025, 9, 22),
    #La ejecucion se tiene que realizar manual 
    schedule=None,   # manual
    #Define si tiene que ejecutar tambien todos los dias anteriores al definido hasta la fecha de inicio
    catchup=False, # No recurar fechas pasadas
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    # T1 
    kafka_test = SparkSubmitOperator(
        task_id="kafka_test",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_4.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "local[*]",  # equivalente a tu --master local[*]
            "spark.hadoop.hadoop.user.name": "airflow"
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.5.1"
        ),
    )