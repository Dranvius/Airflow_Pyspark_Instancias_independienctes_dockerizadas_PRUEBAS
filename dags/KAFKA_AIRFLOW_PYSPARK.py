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
    schedule="*/2 * * * *",
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    # T1 
    t1 = SparkSubmitOperator(
        task_id="kafka_test",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_4.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.hadoop.hadoop.user.name": "airflow"
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
            "org.apache.kafka:kafka-clients:3.7.0,"
            "org.postgresql:postgresql:42.7.3"
        ),
    )