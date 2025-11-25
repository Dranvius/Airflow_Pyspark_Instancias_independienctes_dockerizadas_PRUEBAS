import os
import json
import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/app_vhost")

EXCHANGE_NAME = "inventory_alerts"
ALERT_QUEUE = "low_stock_alerts"


def _get_channel():
  """Crea canal de RabbitMQ y asegura exchange/queue fanout para alertas."""
  credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
  params = pika.ConnectionParameters(
      host=RABBITMQ_HOST,
      port=RABBITMQ_PORT,
      virtual_host=RABBITMQ_VHOST,
      credentials=credentials,
      heartbeat=30,
      blocked_connection_timeout=30,
  )
  connection = pika.BlockingConnection(params)
  channel = connection.channel()
  channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
  channel.queue_declare(queue=ALERT_QUEUE, durable=True)
  channel.queue_bind(exchange=EXCHANGE_NAME, queue=ALERT_QUEUE)
  return connection, channel


def publish_low_stock_alert(product_id: int, current_quantity: int, source: str = "api"):
  """Envía una alerta de bajo stock a la cola de RabbitMQ (fanout)."""
  try:
    connection, channel = _get_channel()
    payload = {
        "product_id": product_id,
        "current_quantity": current_quantity,
        "source": source,
    }
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="",
        body=json.dumps(payload),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    connection.close()
    return True
  except Exception as exc:  # pragma: no cover - solo logging
    print(f"RabbitMQ publish failed: {exc}")
    return False
