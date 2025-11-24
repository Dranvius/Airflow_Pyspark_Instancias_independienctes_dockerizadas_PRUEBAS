import pika
import json
import os
import time
import logging
from typing import Optional

# --------------------------
# Configuración del logger
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------
# Configuración RabbitMQ
# --------------------------
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")  # Nombre del servicio en Docker Compose
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/app_vhost")
ALERT_QUEUE_NAME_DEFAULT = os.environ.get("ALERT_QUEUE_NAME", "low_stock_alerts")

MAX_RETRIES = 5
RETRY_DELAY = 5  # segundos

# --------------------------
# Funciones
# --------------------------
def get_rabbitmq_connection() -> Optional[pika.BlockingConnection]:
    """
    Establece y devuelve una conexión con RabbitMQ, con reintentos.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials
    )
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connection = pika.BlockingConnection(parameters)
            logger.info("✅ Conexión a RabbitMQ establecida.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Intento {attempt}/{MAX_RETRIES} fallido: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                logger.error("❌ No se pudo conectar a RabbitMQ después de varios intentos.")
                return None

def publish_low_stock_alert(
    product_id: int,
    current_quantity: int,
    warning_level: int,
    queue_name: str = ALERT_QUEUE_NAME_DEFAULT
):
    """
    Publica una alerta de bajo stock en la cola de RabbitMQ.
    """
    connection = get_rabbitmq_connection()
    if not connection:
        logger.error("No se pudo publicar la alerta porque no hay conexión a RabbitMQ.")
        return

    try:
        channel = connection.channel()
        # Declarar la cola (durable)
        channel.queue_declare(queue=queue_name, durable=True)

        message = {
            "product_id": product_id,
            "current_quantity": current_quantity,
            "warning_level": warning_level,
            "alert_type": "LOW_STOCK"
        }

        # Publicar el mensaje
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2  # Persistente
            )
        )
        logger.info(f"✅ Alerta de bajo stock para el producto {product_id} publicada en '{queue_name}'.")

    except pika.exceptions.AMQPChannelError as e:
        logger.error(f"❌ Error en el canal al publicar en RabbitMQ: {e}")
    except Exception as e:
        logger.error(f"❌ Error inesperado al publicar en RabbitMQ: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("🔒 Conexión a RabbitMQ cerrada.")

# --------------------------
# Ejemplo de uso
# --------------------------
if __name__ == '__main__':
    logger.info("Ejecutando prueba de publicación en RabbitMQ...")
    publish_low_stock_alert(product_id=123, current_quantity=8, warning_level=10)
