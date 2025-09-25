from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast # Funciones clave

#INICIAR PROCESO DE CREACION 
# Genera dataset grande sintético (p. ej. 5M filas) con columnas: id_event, product, category, price, event_date.
# Bronze: guardar raw (Parquet particionado por event_date).


spark = SparkSession.builder \
    .appName("SparkOpsEjemplo") \
    .master("local[*]") \
    .getOrCreate()

# crear df sintético
    # Crear una instancia de proyecto
df = spark.range(0, 5_000_000) \
    .withColumn("product", (F.col("id") % 10).cast("string")) \
    .withColumn("price", (F.col("id") % 1000) * 1.0) \
    .withColumn("event_date", F.expr("date_sub(current_date(), cast(id % 30 as int))")) \
    .withColumn("id_event", F.monotonically_increasing_id()) \
    .withColumn(
        "category",
        F.when(F.col("product").isin("0", "1", "2"), "Electronics")
         .when(F.col("product").isin("3", "4"), "Books")
         .when(F.col("product").isin("5", "6"), "Clothing")
         .otherwise("Other")
    )
# ejemplo de cache y particionado al escribir

    #Importante el guardado por medio de partitionBy guarda los archivos en particiones de event date -> Sub conjuntos de del dataframe
    # Definidos por la columa event_date 

df.write.mode("overwrite").partitionBy("event_date").parquet("./ejercicio_1_data/bronce/")

spark.stop()
    



