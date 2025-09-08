from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_test_operator",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="run_spark_test",
        application="/opt/airflow/pyspark_instancia_pruebas/pyspark_instancia_pruebas/test_pyspark.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"}
    )
