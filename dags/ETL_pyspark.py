from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# ----------------------
# DAG definition
# ----------------------

with DAG(
    dag_id="spark_etl_pipeline",
    start_date=datetime(2025, 9, 22),
    schedule=None,   # manual
    catchup=False, # No recurar fechas pasadas
) as dag:


    # T1 - Ejecutar bronce
    t1 = SparkSubmitOperator(
        task_id="crear_bronce",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicios_0.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"},
    )

    # T1 - Ejecutar silver
    t2 = SparkSubmitOperator(
        task_id="limpieza_datos",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicio_1.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"},
    )



    # Orquestación: extract → transform → load
    t1 >> t2
