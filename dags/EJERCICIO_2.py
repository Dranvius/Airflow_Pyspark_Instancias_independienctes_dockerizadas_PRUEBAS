from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# ----------------------
# DAG definition
# ----------------------

with DAG(
    dag_id="spark_join_con_mucha_informacion",
    start_date=datetime(2025, 9, 22),
    schedule=None,   # manual
    catchup=False, # No recurar fechas pasadas
) as dag:


    # T1 - Ejecutar
    t1 = SparkSubmitOperator(
        task_id="broadcast_salting",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_2.py",
        conn_id="spark_default",
        verbose=True,
        conf={
        "spark.master": "spark://spark-master:7077",   
        "spark.executor.memory": "1g",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1",
        "spark.executor.instances": "2"},

    )



    # Orquestación: extract → transform → load
    t1
