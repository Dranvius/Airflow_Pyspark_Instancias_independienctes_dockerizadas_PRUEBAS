import os

# -----------------------------
# PostgreSQL
# -----------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sergio")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# -----------------------------
# Kafka
# -----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPICS = {
    "venta": os.getenv("KAFKA_TOPIC_VENTAS", "inventory_sales"),
    "entrada": os.getenv("KAFKA_TOPIC_ENTRADA", "inventory_entries"),
    "salida": os.getenv("KAFKA_TOPIC_SALIDA", "inventory_out"),
    "nuevo_producto": os.getenv("KAFKA_TOPIC_NUEVO_PRODUCTO", "inventory_new_products"),
    "ajuste": os.getenv("KAFKA_TOPIC_AJUSTE", "inventory_adjustments")
}

# -----------------------------
# RabbitMQ
# -----------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE_ALERTS = os.getenv("RABBITMQ_QUEUE_ALERTS", "alerts")

# -----------------------------
# Otros parámetros
# -----------------------------
DEBUG = os.getenv("DEBUG", "True").lower() in ("true", "1", "t")
