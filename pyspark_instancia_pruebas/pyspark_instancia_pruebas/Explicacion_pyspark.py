from pyspark.sql import SparkSession

# Data simulando
data = [
    {"id": 1, "product": "Laptop", "category": "Electronics", "price": 1200},
    {"id": 2, "product": "Phone", "category": "Electronics", "price": 800},
    {"id": 3, "product": "Shirt", "category": "Clothing", "price": 40},
    {"id": 4, "product": "Book", "category": "Books", "price": 15},
    {"id": 5, "product": "Tablet", "category": "Electronics", "price": 500},
    {"id": 6, "product": "Jacket", "category": "Clothing", "price": 90},
]

# 1. Crear SparkSession
spark = SparkSession.builder \
    .appName("Ejercicio_1") \
    .master("local[*]") \
    .getOrCreate()

# 2. Crear DataFrame directamente desde la lista de diccionarios
df = spark.createDataFrame(data)

# 3. Transformaciones (lazy evaluation)
    # No se ejecuta.
    # Por denajo define evaluacion estandarizada.    
grouped = df.groupBy("category").count()

# 4. Acción (ejecuta el job)
    # Aquí es donde se ejecuta
    # Es aca donde se deben realizar evaluaciones de rendimiento 
grouped.show()

# 5. Detener Sparkz
spark.stop()

# ! ------------------------------------------------------------------------- Funciones --------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast # Funciones clave

data = [
    {"id": 1, "product": "Laptop",   "category": "Electronics", "price": 1200},
    {"id": 2, "product": "Phone",    "category": "Electronics", "price": 800},
    {"id": 3, "product": "Shirt",    "category": "Clothing",    "price": 40},
    {"id": 4, "product": "Book",     "category": "Books",       "price": 15},
    {"id": 5, "product": "Tablet",   "category": "Electronics", "price": 500},
    {"id": 6, "product": "Jacket",   "category": "Clothing",    "price": 90},
]


spark = SparkSession.builder \
    .appName("SparkOpsEjemplo") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(data)


# 1) Select y proyecciones: select, withColumn, drop, alias

#Qué hacen

# select(...): elige columnas (proyección).
# withColumn("nueva", expr): añade o reemplaza una columna.
# drop("col"): elimina una columna.
# alias("nuevoNombre") / col("x").alias(...): renombra para la consulta.

#! Ejemplo

# seleccionar columnas
df.select("id", "product", "price").show()

# agregar columna price_with_tax (10%)
    # TRabaja con columna y crea una columna donde  le aplica 10% a cada una de la clumna price
df_tax = df.withColumn("price_with_tax", F.round(F.col("price") * 1.10, 2))
df_tax.show()

# eliminar id
df_noid = df_tax.drop("id")
df_noid.show()

# alias (renombrar product a producto)
df_alias = df.select(F.col("product").alias("producto"), "category", "price")
df_alias.show()

#! 2) Filtros y condiciones: filter, where --Limpiezas 

# Qué hacen

# Filtran filas según condiciones booleanas. filter y where son equivalentes.

# precio mayor a 500
df.filter(F.col("price") > 500).show()

# categoría Electronics y precio <= 1000
df.where((F.col("category") == "Electronics") & (F.col("price") <= 1000)).show()

# buscar por patrón
df.filter(F.col("product").like("%Phone%")).show()

#! 3) Agregaciones: groupBy, agg, funciones (sum, avg, countDistinct,...)

# Qué hacen

# groupBy agrupa por una o varias columnas.
# agg aplica funciones agregadas por grupo.

# Ejemplo

agg = df.groupBy("category").agg(
    F.count("*").alias("n_items"),
    F.sum("price").alias("total_price"),
    F.avg("price").alias("avg_price"),
    F.countDistinct("product").alias("distinct_products")
)
agg.show()

# 4) Joins: inner, left, right, full, left_semi, left_anti

# Preparación (tabla auxiliar para demostrar joins)


product_info = [
    {"product":"Laptop", "supplier":"A", "stock":10},
    {"product":"Phone",  "supplier":"B", "stock":5},
    {"product":"Tablet", "supplier":"A", "stock":3},
    {"product":"Clock",  "supplier":"C", "stock":7},  # no existe en df original
]
df_info = spark.createDataFrame(product_info)


# inner join: solo filas con match en ambas tablas
inner = df.join(df_info, on="product", how="inner")
inner.show()

# left join: todas las filas del left (df), con info de df_info cuando exista
left = df.join(df_info, on="product", how="left")
left.show()

# right join: todas las filas del right (df_info), con info del left cuando exista
right = df.join(df_info, on="product", how="right")
right.show()

# full outer: unión completa, null donde no hay match
full = df.join(df_info, on="product", how="full")
full.show()

# left_semi: devuelve filas del left que tienen un match en right (solo columnas del left)
semi = df.join(df_info, on="product", how="left_semi")
semi.show()

# left_anti: filas del left que NO tienen match en right
anti = df.join(df_info, on="product", how="left_anti")
anti.show()


# Cuándo usar cada una
# inner cuando solo quieres coincidencias completas.
# left para enriquecer datos principales sin perder filas.
# right ocasional (depende de qué tabla es la principal).
# full para un merge completo.
# left_semi/left_anti son muy útiles para filtros basados en existencia (subconjunto / exclusión) sin traer columnas del right.


# 5) Funciones de ventana (window functions): row_number, rank, lag, lead

# Qué son
# Permiten cálculos por partición (p. ej. por category) y ordenados por alguna columna (p. ej. price).
# No colapsan filas (a diferencia de groupBy); devuelven resultados a nivel fila.
# Diferencia entre row_number, rank, dense_rank
# row_number: asigna un número secuencial único por fila (sin considerar empates: cada fila recibe distinto número).
# rank: asigna mismo rank a empates, pero deja huecos. Ej: precios [100,100,50] → ranks [1,1,3].
# dense_rank: similar a rank pero sin huecos. Ej: [1,1,2].
# Ejemplo — Ranking por precio dentro de cada categoría


w = Window.partitionBy("category").orderBy(F.desc("price"))

df_ranked = df.withColumn("row_number", F.row_number().over(w)) \
              .withColumn("rank", F.rank().over(w)) \
              .withColumn("dense_rank", F.dense_rank().over(w)) \
              .withColumn("prev_price", F.lag("price", 1).over(w)) \
              .withColumn("next_price", F.lead("price", 1).over(w))

# Mostrar ordenado por categoría y rank
df_ranked.orderBy("category", "row_number").show()

