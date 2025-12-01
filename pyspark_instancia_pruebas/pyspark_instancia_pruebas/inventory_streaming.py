from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import json
import pika

# -----------------------------
# Configuración Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("InventoryStreaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Esquema de eventos desde Kafka
# -----------------------------
schema = StructType([
    StructField("action", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("extra_info", StringType(), True)
])

# -----------------------------
# Leer eventos desde Kafka
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "inventory_sales") \
    .option("startingOffsets", "latest") \
    .load()

events = df.selectExpr("CAST(value AS STRING) as json_str") \
           .select(from_json(col("json_str"), schema).alias("data")) \
           .select("data.*")

# -----------------------------
# Configuración RabbitMQ
# -----------------------------
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
RABBITMQ_VHOST = "/app_vhost"
LOW_STOCK_THRESHOLD = 5

EXCHANGE_NAME = "inventory_alerts"
ALERT_QUEUE = "low_stock_alerts"

def send_low_stock_alert(product_id, quantity):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=ALERT_QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=ALERT_QUEUE)
    
    payload = {
        "product_id": product_id,
        "current_quantity": quantity,
        "source": "spark_streaming",
    }
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="",
        body=json.dumps(payload),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    connection.close()

# -----------------------------
# Función para actualizar inventario
# -----------------------------
def update_inventory(batch_df, batch_id):
    import psycopg2
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="sergio",
        password="123"
    )
    cur = conn.cursor()
    
    for row in batch_df.collect():
        # Actualizar inventario
        if row.action in ["venta", "salida"]:
            cur.execute(
                "UPDATE inventario SET cantidad = cantidad - %s WHERE product_id = %s",
                (row.quantity, row.product_id)
            )
        elif row.action in ["entrada", "nuevo_producto", "ajuste"]:
            cur.execute(
                "INSERT INTO inventario (product_id, cantidad) VALUES (%s, %s) "
                "ON CONFLICT (product_id) DO UPDATE SET cantidad = inventario.cantidad + EXCLUDED.cantidad",
                (row.product_id, row.quantity)
            )

        # Revisar stock bajo y enviar alerta
        cur.execute("SELECT cantidad FROM inventario WHERE product_id = %s", (row.product_id,))
        stock = cur.fetchone()[0]
        if stock < LOW_STOCK_THRESHOLD:
            send_low_stock_alert(row.product_id, stock)

    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# Ejecutar streaming
# -----------------------------
query = events.writeStream \
    .foreachBatch(update_inventory) \
    .start()

query.awaitTermination()
