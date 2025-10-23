from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pika
import json
import psycopg2
from urllib.parse import urlparse

# ======================
# CONFIGURACIÓN GENERAL
# ======================

RABBITMQ_URL = "amqp://airflow:airflow123@rabbitmq:5672/%2Fapp_vhost"

POSTGRES_DB = "airflow"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_USER = "sergio"
POSTGRES_PASS = "123"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver"
}

# ======================
# SCHEMAS
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
    .appName("ETL_RabbitMQ_to_Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# ======================
# CREACIÓN DE TABLAS SI NO EXISTEN
# ======================

def ensure_tables_exist():
    """Crea las tablas en PostgreSQL si no existen."""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    create_clientes = """
    CREATE TABLE IF NOT EXISTS clientes (
        cliente_id INT PRIMARY KEY,
        nombre VARCHAR(100),
        email VARCHAR(100),
        telefono VARCHAR(50),
        fecha_registro VARCHAR(50)
    );
    """

    create_orders = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        quantity INT,
        price INT,
        order_date VARCHAR(50)
    );
    """

    cur.execute(create_clientes)
    cur.execute(create_orders)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tablas verificadas o creadas correctamente en PostgreSQL")

# ======================
# FUNCIÓN: LECTURA DE COLAS
# ======================

def read_messages_from_queue(queue_name, max_messages=100):
    """Lee mensajes desde RabbitMQ usando la URL AMQP y los retorna como lista de diccionarios"""
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
    except Exception as e:
        print(f"❌ Error al conectar con RabbitMQ: {e}")
        return []

    messages = []
    for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout=3):
        if body:
            try:
                data = json.loads(body)
                if isinstance(data, dict):
                    messages.append(data)
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                else:
                    print(f"⚠️ Mensaje inválido (no es diccionario): {data}")
            except json.JSONDecodeError:
                print(f"❌ Error al decodificar mensaje en {queue_name}")
        else:
            break
        if len(messages) >= max_messages:
            break

    channel.cancel()
    connection.close()
    return messages

# ======================
# FUNCIÓN: PROCESAR Y CARGAR
# ======================

def process_and_load(queue_name, schema, table):
    """Procesa los mensajes de una cola y los carga en PostgreSQL"""
    data = read_messages_from_queue(queue_name)
    if not data:
        print(f"⚠️ No hay mensajes nuevos en {queue_name}")
        return

    try:
        df = spark.createDataFrame(data, schema=schema)
        df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPERTIES)
        print(f"✅ {len(data)} registros cargados desde {queue_name} → tabla {table}")
    except Exception as e:
        print(f"❌ Error al procesar/cargar datos de {queue_name}: {e}")

