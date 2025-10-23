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
    StructField("cliente_id", IntegerType()),
    StructField("nombre", StringType()),
    StructField("email", StringType()),
    StructField("telefono", StringType()),
    StructField("fecha_registro", StringType())
])

# ========================
# Lectura batch desde Kafka
# ========================
def read_kafka_batch(topic_name):
    try:
        logger.info(f"=== Leyendo desde Kafka topic: {topic_name} (modo batch) ===")
        df = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info(f"Kafka conectado correctamente al topic {topic_name}")
        return df
    except Exception as e:
        logger.error(f"Error leyendo de Kafka: {e}")
        raise

df_sales = read_kafka_batch("sales")
df_clientes = read_kafka_batch("cliente")

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
# Mostrar primeros 5 registros
# ========================
logger.info("=== Primeros registros de ventas ===")
sales_df.show(5, truncate=False)
logger.info("=== Primeros registros de clientes ===")
cliente_df.show(5, truncate=False)

# ========================
# Escritura Bronze (Parquet)
# ========================
sales_df.write.mode("overwrite").parquet("/opt/airflow/data/output/bronze/sales")
cliente_df.write.mode("overwrite").parquet("/opt/airflow/data/output/bronze/clientes")

# ========================
# Transformaciones Gold
# ========================
logger.info("=== Aplicando transformaciones Gold ===")
agg_df = sales_df.groupBy("product_id") \
    .agg(spark_sum(col("quantity") * col("price")).alias("total_sales"))

agg_df_postgres = agg_df.select(
    col("product_id"),
    col("total_sales")
)

agg_df_postgres.write.mode("overwrite").parquet("/opt/airflow/data/output/gold/sales_metrics")

# ========================
# Función para escribir a PostgreSQL
# ========================
def write_to_postgres(df, table_name, schema="datos"):
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
        logger.info(f"Datos guardados en PostgreSQL: {table_name}")
    except Exception as e:
        logger.error(f"Error guardando en {table_name}: {e}")

# ========================
# Escritura a PostgreSQL
# ========================
write_to_postgres(sales_df, "bronze_sales")
write_to_postgres(cliente_df, "bronze_clientes")
write_to_postgres(agg_df_postgres, "gold_sales_metrics")

logger.info("=== ETL batch completado ===")
