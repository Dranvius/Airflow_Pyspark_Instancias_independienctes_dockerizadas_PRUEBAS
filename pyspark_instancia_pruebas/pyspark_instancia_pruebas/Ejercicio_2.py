from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, lit, concat, monotonically_increasing_id, broadcast, when
import time


#! ========================
#! 1. JOINS CON BIG DATA
#! ========================

spark = SparkSession.builder.appName("SkewCartesianExample").getOrCreate()

# ========================
# 1. CREACIÓN DE DATOS
# ========================
start_1 = time.time()

num_rows = 10_000_000

# Generamos product con skew (50% = 0, 50% = 1)
facts = spark.range(num_rows).withColumn(
    "product", (rand() < 0.5).cast("int")  # mitad 0, mitad 1
).withColumn("value", (rand()*100).cast("int"))

# Ajustar filas con product=0 para que sean skewed en el rango 1..999
facts = facts.withColumn(
    "product",
    when(col("product") == 0, (rand()*999).cast("int")).otherwise(1)
)

# Dimensión pequeña (catálogo de productos)
dim_products = spark.range(1000).withColumnRenamed("id", "product") \
    .withColumn("product_name", concat(lit("Product_"), col("product")))

end_1 = time.time()
print(f"Tiempo creación de datos: {end_1 - start_1:.2f} segundos")


# ========================
# 2. JOIN NORMAL
# ========================
start_2 = time.time()

join_normal = facts.join(dim_products, on="product", how="inner")
join_normal.count()  # forzar ejecución

end_2 = time.time()
print(f"Tiempo JOIN normal: {end_2 - start_2:.2f} segundos")


# ========================
# 3. JOIN CON BROADCAST
# ========================
start_3 = time.time()

join_broadcast = facts.join(broadcast(dim_products), on="product", how="inner")
join_broadcast.count()  # forzar ejecución

end_3 = time.time()
print(f"Tiempo JOIN con broadcast: {end_3 - start_3:.2f} segundos")


# ========================
# 4. JOIN CON SALTING
# ========================
start_4 = time.time()

# Agregamos "salt" para redistribuir filas skew
facts_salted = facts.withColumn("salt", (monotonically_increasing_id() % 10))
dim_salted = dim_products.crossJoin(spark.range(10).withColumnRenamed("id", "salt"))

join_salted = facts_salted.join(dim_salted, on=["product", "salt"], how="inner")
join_salted.count()  # forzar ejecución

end_4 = time.time()
print(f"Tiempo JOIN con salting: {end_4 - start_4:.2f} segundos")
