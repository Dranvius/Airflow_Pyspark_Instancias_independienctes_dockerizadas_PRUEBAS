import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2

# ========================
# Configuración de logging
# ========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaETL")

# ========================
# Inicializar SparkSession
# ========================

try:
    logger.info("=== Iniciando SparkSession ===")
    spark = SparkSession.builder.appName("KafkaETL").getOrCreate()
    logger.info("SparkSession creada correctamente")
except Exception as e:
    logger.error(f"Error creando SparkSession: {e}")
    raise

# ========================
# Definir esquemas de JSON
# ========================
schema_product = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("order_date", StringType())  # sigue siendo string en Kafka
])

schema_clientes = StructType([
    StructField("id_user", IntegerType()),
    StructField("nombre", StringType()),
    StructField("apellido", StringType()),
    StructField("edad", IntegerType()),
])

# ========================
# Lectura de Kafka
# ========================

# ! IMPORTANTE Lectura de kafka en stream : Se esta leyendo pero deja el hilo abierto esperando cambios (para ETL esta mal porque deberia volver a ejecutar todo)
def read_kafka_stream(topic_name):
    try:
        logger.info(f"=== Leyendo desde Kafka topic: {topic_name} ===")
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info(f"Kafka conectado correctamente al topic {topic_name}")
        return df
    except Exception as e:
        logger.error(f"Error leyendo de Kafka: {e}")
        raise

df_sales = read_kafka_stream("sales")
df_clientes = read_kafka_stream("cliente")

# ========================
# Parsear JSON
# ========================
def parse_json(df, schema, name):
    try:
        logger.info(f"=== Parseando JSON {name} ===")
        json_df = df.selectExpr("CAST(value AS STRING) as json")
        parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
        logger.info(f"JSON {name} parseado correctamente")
        return parsed_df
    except Exception as e:
        logger.error(f"Error parseando JSON {name}: {e}")
        raise

sales_df = parse_json(df_sales, schema_product, "productos_vendidos")
cliente_df = parse_json(df_clientes, schema_clientes, "clientes_registrados")

# ========================
# Convertir order_date a timestamp
# ========================
sales_df = sales_df.withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))

# ========================
# Mostrar primeros 5 registros (verificación de Kafka)
# ========================
def show_first_rows(df, batch_id, name):
    logger.info(f"=== Mostrando primeros 5 registros de {name} ===")
    df.show(5, truncate=False)

sales_df.writeStream \
    .foreachBatch(lambda df, batch_id: show_first_rows(df, batch_id, "sales")) \
    .outputMode("append") \
    .start()

cliente_df.writeStream \
    .foreachBatch(lambda df, batch_id: show_first_rows(df, batch_id, "clientes")) \
    .outputMode("append") \
    .start()

# ========================
# Escritura Bronze (Parquet)
# ========================
def write_bronze(df, path, checkpoint, name):
    try:
        logger.info(f"=== Configurando escritura Bronze {name} ===")

        #! IMPORTANTE : EL write stream escribe sobre un hilo de procesamiento contioonuo, deja abierto el hilo para posibles modificaciones
        query = df.writeStream \
            .format("parquet") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint) \
            .outputMode("append") \
            .start()
        logger.info(f"Bronze {name} iniciado correctamente")
        return query
    except Exception as e:
        logger.error(f"Error en Bronze {name}: {e}")
        raise

bronze_query_sales = write_bronze(
    sales_df,
    "/opt/airflow/data/output/bronze/sales",
    "/tmp/checkpoints/bronze_sales",
    "sales"
)

bronze_query_cliente = write_bronze(
    cliente_df,
    "/opt/airflow/data/output/bronze/clientes",
    "/tmp/checkpoints/bronze_clientes",
    "clientes"
)

# ========================
# Transformaciones Gold
# ========================
try:
    logger.info("=== Configurando agregaciones Gold ===")
    agg_df = sales_df.withColumn("order_ts", col("order_date")) \
        .withWatermark("order_ts", "2 minutes") \
        .groupBy(
            window(col("order_ts"), "1 minute"),
            col("product_id")
        ).agg(
            spark_sum(col("quantity") * col("price")).alias("total_sales")
        )

    # Separar columnas window.start y window.end para PostgreSQL
    agg_df_postgres = agg_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("total_sales")
    )

    logger.info("Transformaciones Gold aplicadas correctamente")
except Exception as e:
    logger.error(f"Error preparando Gold: {e}")
    raise

gold_query_sales = write_bronze(
    agg_df_postgres,
    "/opt/airflow/data/output/gold/sales_metrics",
    "/tmp/checkpoints/gold_sales",
    "sales_gold"
)

# ========================
# Conexión y escritura a PostgreSQL
# ========================
def write_to_postgres(df, batch_id, table_name, schema="datos"):
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="sergio",
            password="123"
        )
        cur = conn.cursor()
        
        if table_name == "bronze_sales":
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    order_id INT,
                    customer_id INT,
                    product_id INT,
                    quantity INT,
                    price DOUBLE PRECISION,
                    order_date TIMESTAMP
                )
            """)
        elif table_name == "gold_sales_metrics":
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    product_id INT,
                    total_sales DOUBLE PRECISION
                )
            """)
        elif table_name == "bronze_clientes":
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    id_user INT,
                    nombre VARCHAR(100),
                    apellido VARCHAR(100),
                    edad INT
                )
            """)
        conn.commit()
        cur.close()
        conn.close()

        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", table_name) \
            .option("user", "sergio") \
            .option("password", "123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id} guardado en {table_name}")
    
    except Exception as e:
        logger.error(f"Error guardando en {table_name}: {e}")

# ========================
# Escritura de streams a PostgreSQL
# ========================
bronze_query_sales_postgres = sales_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "bronze_sales")) \
    .outputMode("append") \
    .start()

gold_query_sales_postgres = agg_df_postgres.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "gold_sales_metrics")) \
    .outputMode("append") \
    .start()

bronze_query_cliente_postgres = cliente_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "bronze_clientes")) \
    .outputMode("append") \
    .start()

# ========================
# Esperar terminación de streams
# ========================
logger.info("=== Esperando terminación de streams ===")
spark.streams.awaitAnyTermination()
