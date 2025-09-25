# Tareas:


# Silver: limpiar nulls, normalizar nombres (lower/trim), deduplicar por id_event (última marca por event_date).

# Gold: agregar ventas por category, event_date (sum, count, avg) y guardar con partición por event_date.


# Silver

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast # Funciones clave
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType # Tipos para los archivos


spark = SparkSession.builder \
    .appName("ejemplo_1") \
    .master("local[*]") \
    .getOrCreate()

# Leer parquet - No importa si es solo una parquet lee toda la carpeta
df = spark.read.parquet("./ejercicio_1_data/")

#Limpieza de datos
    # Nulos
    # Cuando exite un nulo en alguna de la fila seleccionada
    # Consideraciones
        # id_event = debe tener un id si o si
        # price = debe tener un precio si o si
        # event_date = debe contar con una fecha si o si
df.na.drop(how=("any"),subset=["id_event","price"]).show()


# Eliminar duplicados
    #Definicion de funcion ventana y como se aplica con spark
window = Window.partitionBy("id_event").orderBy(F.col("event_date").desc())

df = (
    df.withColumn("row_num", F.row_number().over(window))
      .filter(F.col("row_num") == 1)
      .drop("row_num")
)

# 🔹 3. Eliminar duplicados con una regla adicional (ej. quedarte con el último por fecha)

# Aquí usas funciones ventana:
# from pyspark.sql import Window
# import pyspark.sql.functions as F
# window = Window.partitionBy("id_event").orderBy(F.col("event_date").desc())
# df_sin_dups = (
#     df.withColumn("row_num", F.row_number().over(window))
#       .filter(F.col("row_num") == 1)
#       .drop("row_num")
# )

# 👉 En este caso:
# Agrupamos por id_event
# Ordenamos por event_date descendente
# Nos quedamos solo con el último registro por cada id_event.


#Limpieza de nombres 
    #Minusculas y sin espacios
df = df.withColumn(
    "product",
    F.regexp_replace(F.trim(F.lower(F.col("product"))), "[^a-zA-Z0-9áéíóúÁÉÍÓÚ ]", "")
)

    #Limpieza de fechas
        #Tu filtro "event_date != 00/00/0000" nunca funciona bien porque event_date es tipo date o string, no lo comparas contra formato real.
        #·Mejor asegurar con isNotNull() y si es string, validarlo con un regexp.
df = df.withColumn("event_date", F.to_date("event_date", "yyyy-MM-dd"))
df = df.filter(F.col("event_date").isNotNull())

    #Limpieza de valores de price no sean ngativos

df = df.filter(F.col("price") >= 0)

#Guardar datos
    # Dia, Mes , Año
df = df.withColumn("year", F.year("event_date")) \
       .withColumn("month", F.month("event_date")) \
       .withColumn("day", F.dayofmonth("event_date"))

df.repartition("year", "month", "day") \
  .write.mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .parquet("./ejercicio_1_data/silver/")

spark.stop()
