from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Definicion de funciones ventana
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("TopNByGroup").getOrCreate()

data = [
    ("Electronics", "Laptop", 1200),
    ("Electronics", "Phone", 800),
    ("Electronics", "Camera", 800),
    ("Electronics", "Tablet", 500),
    ("Clothing", "Jacket", 300),
    ("Clothing", "Shirt", 300),
    ("Clothing", "Pants", 200),
    ("Clothing", "Socks", 100),
]

# Crear el dataframe spark
df = spark.createDataFrame(data, ["category", "product", "price"])
df.show()


# ventana: particionar por categoría, ordenar por precio descendente
windowSpec = Window.partitionBy("category").orderBy(F.desc("price"))


# Definicion de cada una de las funciones
df = df.withColumn("row_number", F.row_number().over(windowSpec)) \
       .withColumn("rank", F.rank().over(windowSpec)) \
       .withColumn("dense_rank", F.dense_rank().over(windowSpec)) \
       .withColumn("prev_price", F.lag("price").over(windowSpec)) \
       .withColumn("diff", F.col("price") - F.col("prev_price"))

# Ejecutar la definicion
df.show()
