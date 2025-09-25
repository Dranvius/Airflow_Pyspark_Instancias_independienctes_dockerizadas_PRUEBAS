from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("KafkaETL").getOrCreate()

# Definir esquema del JSON
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("order_date", StringType())
])

# Leer de Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sales") \
    .load()

# Parsear JSON
json_df = df.selectExpr("CAST(value AS STRING) as json")
sales_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Bronze: guardar crudo
bronze_query = sales_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/output/bronze/sales") \
    .option("checkpointLocation", "/tmp/checkpoints/bronze") \
    .outputMode("append") \
    .start()

# Gold: métricas agregadas en tiempo real (con watermark y ventana de 1 minuto)
agg_df = sales_df.withColumn("order_ts", to_timestamp("order_date")) \
    .withWatermark("order_ts", "2 minutes") \
    .groupBy(
        window(col("order_ts"), "1 minute"),
        col("product_id")
    ).agg(
        spark_sum(col("quantity") * col("price")).alias("total_sales")
    )

gold_query = agg_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/output/gold/sales_metrics") \
    .option("checkpointLocation", "/tmp/checkpoints/gold") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
