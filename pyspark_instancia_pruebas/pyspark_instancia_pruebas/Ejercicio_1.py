from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast # Funciones clave
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType # Tipos para los archivos


#! Creacion de instancia spark
    #? Local[*] Ejecion en stand alone (No cluster)
    #? definicion de nombres

spark = SparkSession.builder \
    .appName("ejemplo_1") \
    .master("local[*]") \
    .getOrCreate()


#! lECTURA DE PARQUET
    #? Definicion un directorio o un archivo en especifico.
    #? Leer parquet - No importa si es solo una parquet lee toda la carpeta.
df = spark.read.parquet("./ejercicio_1_data/")

#! Limpieza de datos
    #? Nulos
    #TODO Consideraciones
        #* id_event = debe tener un id si o si
        #* price = debe tener un precio si o si
        #* event_date = debe contar con una fecha si o si
df.na.drop(how=("any"),subset=["id_event","price"]).show()


#! Limpieza de nombres 
    #* Minusculas y sin espacios

df = df.withColumn(
    "product",
    F.regexp_replace(F.trim(F.lower(F.col("product"))), "[^a-zA-Z0-9áéíóúÁÉÍÓÚ ]", "")
)

#! Limpieza de fechas
    # *isNotNull() y si es string, validarlo con un regexp.

df = df.withColumn("event_date", F.to_date("event_date", "yyyy-MM-dd"))
df = df.filter(F.col("event_date").isNotNull())

#! Limpieza de valores de price no sean ngativos.
    #* Los valores no deberian ser negativos.

df = df.filter(F.col("price") >= 0)


#! Eliminar duplicados por medio de una funcion ventana
    #* Definicion de funcion ventana (Construccion de la funcion)

window = Window.partitionBy("id_event").orderBy(F.col("event_date").desc())

    #* Aplicar funcion ventana y elimina los valores que no son 1.
    #* 👉 En este caso:
    #* Agrupamos por id_event
    #* Ordenamos por event_date descendente
    #* Nos quedamos solo con el último registro por cada id_event.

df = (
    df.withColumn("row_num", F.row_number().over(window))
      .filter(F.col("row_num") == 1)
      .drop("row_num")
)

#! Guarda los datos
    #TODO prepara los datos para ser guardaso en parque
        #TODO El objetivo es guardar los datos en conjuntos de datos agrupados por año, mes y día; Cada conjunto estara guardado en una carpeta correspondiente

    #? Definir nuevas columnas de agrupamiento
        #TODO constuida por medio de Dia,Mes,Año

df = df.withColumn("year", F.year("event_date")) \
       .withColumn("month", F.month("event_date")) \
       .withColumn("day", F.dayofmonth("event_date"))

    #TODO guarda los datos de forma tal que se crean un directorio por cada año, mes y día (Como un historico)

df.repartition("year", "month", "day") \
  .write.mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .parquet("./ejercicio_1_data/silver/")

    #TODO termina la ejecucion

spark.stop()
