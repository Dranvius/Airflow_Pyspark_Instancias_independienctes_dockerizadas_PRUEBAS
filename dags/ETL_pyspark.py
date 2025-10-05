from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# ----------------------
# ! DAG definition
# ----------------------

with DAG(

    #Nombre del dag
    dag_id="spark_etl_pipeline",
    #Fecha de inicio de la tarea
    start_date=datetime(2025, 9, 22),
    #La ejecucion se tiene que realizar manual 
    schedule=None,   # manual
    #Define si tiene que ejecutar tambien todos los dias anteriores al definido hasta la fecha de inicio
    catchup=False, # No recurar fechas pasadas

) as dag:


    # ? ----------------------
    # ? Tarea definition
    # ? ----------------------


    # T1 - Ejecutar bronce
    t1 = SparkSubmitOperator(

        # ! ID único de la tarea dentro del DAG
        task_id="crear_bronce",

        # ! Ruta al script PySpark que se ejecutará
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/Ejercicios_0.py",

        # ! Conexión definida en Airflow (se configura en la UI en "Admin > Connections")
        conn_id="spark_default",

        # ! Si se muestran logs detallados de la ejecución
        verbose=True,

        # ! Configuración adicional para Spark (en este caso, especificar el master del cluster)
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
