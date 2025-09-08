from pyspark.sql import SparkSession
import logging

# Configuración de logs en Python
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WordCountApp")

def main():
    logger.info("Iniciando la aplicación PySpark...")

    # Crear la sesión de Spark conectada al cluster
    spark = SparkSession.builder \
        .appName("WordCountApp") \
        .getOrCreate()

    # Crear un RDD con texto de prueba
    data = ["hola mundo", "hola spark", "spark es genial", "hola mundo otra vez"]
    rdd = spark.sparkContext.parallelize(data)

    # Dividir en palabras y contar ocurrencias
    word_counts = (
        rdd.flatMap(lambda line: line.split(" "))
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    )

    # Mostrar resultados en logs
    for word, count in word_counts.collect():
        logger.info(f"Palabra: {word} - Conteo: {count}")

    logger.info("Finalizando la aplicación PySpark...")
    spark.stop()

if __name__ == "__main__":
    main()
