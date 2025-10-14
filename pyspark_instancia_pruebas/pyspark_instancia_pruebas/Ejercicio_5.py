from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# ======================
# CONFIGURACIÓN BASE
# ======================
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
RABBITMQ_VHOST = "/app_vhost"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_PROPERTIES = {
    "user": "sergio",
    "password": "123",
    "driver": "org.postgresql.Driver"
}

# ======================
# SCHEMA BASE
# ======================
cliente_schema = StructType([
    StructField("cliente_id", IntegerType()),
    StructField("nombre", StringType()),
    StructField("email", StringType()),
    StructField("telefono", StringType()),
    StructField("fecha_registro", StringType())
])

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", IntegerType()),
    StructField("order_date", StringType())
])

# ======================
# SESIÓN SPARK
# ======================
spark = SparkSession.builder \
    .appName("RabbitMQ_to_Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# ======================
# FUNCIÓN: CONSUMIR DE COLAS
# ======================
def read_from_rabbit(queue_name):
    # Simulación básica (en un entorno real usarías spark-streaming-rabbitmq)
    import pika, json
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host=RABBITMQ_VHOST,
            credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    messages = []
    for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout=2):
        if body:
            messages.append(json.loads(body))
        else:
            break
    connection.close()
    return messages

# ======================
# PROCESAR Y GUARDAR
# ======================
for queue_name, schema, table in [
    ("queue_clientes", cliente_schema, "clientes"),
    ("queue_orders", order_schema, "orders")
]:
    messages = read_from_rabbit(queue_name)
    if messages:
        df = spark.createDataFrame(messages, schema=schema)
        df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPERTIES)
        print(f"✅ Guardados {len(messages)} registros en {table}")

spark.stop()
